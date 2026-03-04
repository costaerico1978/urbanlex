"""
Navegador Universal — IA navega como um humano, passo a passo.

Filosofia:
- A IA recebe o estado visual da página
- Pondera as consequências de cada opção
- Decide UMA ação por vez
- O código executa
- Repete até encontrar ou desistir

O resultado é o LINK (URL ou PDF), não o texto extraído.
A IA lê para confirmar que é a legislação certa, mas não copia/extrai.
"""

import json
import re
import time
import os
import tempfile
from typing import Optional


def _capturar_estado_pagina(page, frame) -> dict:
    """Captura o estado completo e visual da página para a IA analisar."""
    
    estado = frame.evaluate(r'''() => {
        const state = {
            url: window.location.href,
            title: document.title || '',
            texto_visivel: '',
            formularios: [],
            tabelas: [],
            links: [],
            botoes: [],
            paginacao: []
        };
        
        // ── Texto visível (primeiros 8000 chars) ──
        const main = document.querySelector('main, article, .content, .resultado, #conteudo, #resultado, .panel') || document.body;
        state.texto_visivel = (main?.innerText || '').substring(0, 8000);
        
        // ── Formulários com campos ──
        document.querySelectorAll('form, [role="form"]').forEach((form, fi) => {
            if (fi > 2) return;
            const f = { campos: [], botoes: [] };
            
            // Inputs
            form.querySelectorAll('input:not([type="hidden"])').forEach(inp => {
                const type = inp.type || 'text';
                if (['submit', 'button', 'reset', 'image'].includes(type)) {
                    f.botoes.push({
                        tipo: type,
                        texto: inp.value || inp.textContent || '',
                        id: inp.id || '',
                        name: inp.name || ''
                    });
                    return;
                }
                f.campos.push({
                    tipo: type,
                    id: inp.id || '',
                    name: inp.name || '',
                    placeholder: inp.placeholder || '',
                    label: _findLabel(inp),
                    valor_atual: inp.value || '',
                    obrigatorio: inp.required
                });
            });
            
            // Selects
            form.querySelectorAll('select').forEach(sel => {
                const options = [];
                sel.querySelectorAll('option').forEach(opt => {
                    options.push({
                        valor: opt.value,
                        texto: opt.textContent.trim(),
                        selecionado: opt.selected
                    });
                });
                f.campos.push({
                    tipo: 'select',
                    id: sel.id || '',
                    name: sel.name || '',
                    label: _findLabel(sel),
                    opcoes: options,
                    valor_atual: sel.value
                });
            });
            
            // Textareas
            form.querySelectorAll('textarea').forEach(ta => {
                f.campos.push({
                    tipo: 'textarea',
                    id: ta.id || '',
                    name: ta.name || '',
                    placeholder: ta.placeholder || '',
                    label: _findLabel(ta),
                    valor_atual: ta.value || ''
                });
            });
            
            // Botões do form
            form.querySelectorAll('button, input[type="submit"], input[type="button"]').forEach(btn => {
                f.botoes.push({
                    tipo: btn.type || 'button',
                    texto: (btn.textContent || btn.value || '').trim(),
                    id: btn.id || '',
                    name: btn.name || ''
                });
            });
            
            if (f.campos.length > 0) state.formularios.push(f);
        });
        
        // Se não encontrou forms, buscar campos soltos
        if (state.formularios.length === 0) {
            const f = { campos: [], botoes: [] };
            document.querySelectorAll('input:not([type="hidden"]), select, textarea').forEach(el => {
                if (el.closest('nav') || el.closest('header')) return;
                const tag = el.tagName.toLowerCase();
                if (tag === 'select') {
                    const options = [];
                    el.querySelectorAll('option').forEach(opt => {
                        options.push({ valor: opt.value, texto: opt.textContent.trim(), selecionado: opt.selected });
                    });
                    f.campos.push({ tipo: 'select', id: el.id || '', name: el.name || '', label: _findLabel(el), opcoes: options, valor_atual: el.value });
                } else if (tag === 'textarea') {
                    f.campos.push({ tipo: 'textarea', id: el.id || '', name: el.name || '', placeholder: el.placeholder || '', label: _findLabel(el), valor_atual: el.value || '' });
                } else {
                    const type = el.type || 'text';
                    if (['submit', 'button'].includes(type)) {
                        f.botoes.push({ tipo: type, texto: el.value || '', id: el.id || '', name: el.name || '' });
                    } else {
                        f.campos.push({ tipo: type, id: el.id || '', name: el.name || '', placeholder: el.placeholder || '', label: _findLabel(el), valor_atual: el.value || '', obrigatorio: el.required });
                    }
                }
            });
            document.querySelectorAll('button').forEach(btn => {
                if (btn.closest('nav') || btn.closest('header')) return;
                f.botoes.push({ tipo: btn.type || 'button', texto: (btn.textContent || '').trim(), id: btn.id || '', name: btn.name || '' });
            });
            if (f.campos.length > 0) state.formularios.push(f);
        }
        
        // ── Tabelas com conteúdo e links ──
        document.querySelectorAll('table').forEach((table, ti) => {
            if (ti > 3) return;
            const tbl = { headers: [], linhas: [] };
            
            table.querySelectorAll('th').forEach(th => {
                tbl.headers.push(th.textContent.trim().substring(0, 50));
            });
            
            const trs = table.querySelectorAll('tr');
            let count = 0;
            for (const tr of trs) {
                if (count >= 15) break;
                const tds = tr.querySelectorAll('td');
                if (tds.length === 0) continue;
                
                const linha = { celulas: [], links: [] };
                tds.forEach((td, ci) => {
                    linha.celulas.push({
                        coluna: tbl.headers[ci] || 'col_' + ci,
                        texto: td.textContent.trim().substring(0, 200)
                    });
                });
                
                // Links na linha (incluindo ícones sem texto, links javascript, onclick)
                tr.querySelectorAll('a[href], a[onclick], [onclick]').forEach(a => {
                    const href = a.getAttribute('href') || '';
                    const onclick = a.getAttribute('onclick') || '';
                    // Pular se não tem href nem onclick
                    if (!href && !onclick) return;
                    // Pular links ancora vazia
                    if (href === '#' && !onclick) return;
                    
                    const text = (a.textContent || '').trim();
                    const hasImg = !!a.querySelector('img, svg, i, [class*="icon"]');
                    const imgAlt = a.querySelector('img') ? (a.querySelector('img').alt || '') : '';
                    const imgSrc = a.querySelector('img') ? a.querySelector('img').getAttribute('src') || '' : '';
                    const title = a.getAttribute('title') || '';
                    
                    // Em qual coluna está
                    const td = a.closest('td');
                    const ci = td ? Array.from(td.parentElement.children).indexOf(td) : -1;
                    const colName = ci >= 0 && tbl.headers[ci] ? tbl.headers[ci] : '';
                    
                    // Gerar descrição visual
                    let descVisual = text.substring(0, 100);
                    if (!descVisual && hasImg) descVisual = '(ícone: ' + (imgAlt || imgSrc.split('/').pop() || 'imagem') + ')';
                    if (!descVisual) descVisual = '(elemento clicável)';
                    
                    linha.links.push({
                        href: href.substring(0, 200) || ('javascript: ' + onclick.substring(0, 100)),
                        texto: descVisual,
                        coluna: colName,
                        tem_icone: hasImg,
                        alt_imagem: imgAlt,
                        img_src: imgSrc.split('/').pop() || '',
                        title: title,
                        tem_onclick: !!onclick
                    });
                });
                
                linha.texto_completo = tr.textContent.trim().substring(0, 300);
                tbl.linhas.push(linha);
                count++;
            }
            
            if (tbl.linhas.length > 0) state.tabelas.push(tbl);
        });
        
        // ── Links gerais (fora de tabelas) ──
        document.querySelectorAll('a[href]').forEach(a => {
            if (a.closest('table') || a.closest('nav') || a.closest('header') || a.closest('footer')) return;
            const href = a.getAttribute('href') || '';
            if (!href || href === '#' || href.startsWith('javascript:void') || href.startsWith('mailto:')) return;
            
            const text = (a.textContent || '').trim();
            const hasImg = !!a.querySelector('img, svg, i');
            const title = a.getAttribute('title') || '';
            
            // Filtrar lixo
            const combined = (href + text).toLowerCase();
            const lixo = ['facebook', 'twitter', 'whatsapp', 'linkedin', 'instagram', 'youtube', 'telegram', 'login', 'cadastr', 'minha conta'];
            if (lixo.some(x => combined.includes(x))) return;
            
            state.links.push({
                href: href.substring(0, 200),
                texto: text.substring(0, 100) || '(sem texto)',
                tem_icone: hasImg,
                title: title
            });
        });
        state.links = state.links.slice(0, 30);
        
        // ── Paginação ──
        document.querySelectorAll('a[href]').forEach(a => {
            const text = (a.textContent || '').trim().toLowerCase();
            const href = a.getAttribute('href') || '';
            if (['próxima', 'proxima', 'próximo', 'proximo', 'next', '»', '→', 'próxima página'].some(x => text.includes(x)) ||
                ['anterior', 'previous', 'prev', '«', '←', 'página anterior'].some(x => text.includes(x)) ||
                /^\d+$/.test(text.trim())) {
                state.paginacao.push({ texto: text.substring(0, 30), href: href.substring(0, 200) });
            }
        });
        state.paginacao = state.paginacao.slice(0, 10);
        
        // Helper: encontrar label de um campo
        function _findLabel(el) {
            // label[for]
            if (el.id) {
                const lbl = document.querySelector('label[for="' + el.id + '"]');
                if (lbl) return lbl.textContent.trim().substring(0, 80);
            }
            // label pai
            const parent = el.closest('label');
            if (parent) return parent.textContent.replace(el.value || '', '').trim().substring(0, 80);
            // texto anterior
            const prev = el.previousElementSibling;
            if (prev && ['LABEL', 'SPAN', 'B', 'STRONG'].includes(prev.tagName)) {
                return prev.textContent.trim().substring(0, 80);
            }
            // td anterior na mesma row
            const td = el.closest('td');
            if (td && td.previousElementSibling) {
                return td.previousElementSibling.textContent.trim().substring(0, 80);
            }
            return '';
        }
        
        return state;
    }''')
    
    # Adicionar URL real (pode ser diferente do que o JS retorna em iframes)
    estado['url_real'] = page.url
    
    return estado


def _formatar_estado_para_prompt(estado: dict) -> str:
    """Formata o estado da página de forma legível para a IA."""
    
    partes = []
    partes.append(f"URL: {estado.get('url_real', estado.get('url', ''))}")
    partes.append(f"TÍTULO: {estado.get('title', '')}")
    partes.append(f"\n{'='*60}")
    partes.append(f"TEXTO VISÍVEL:")
    partes.append(estado.get('texto_visivel', '')[:6000])
    
    # Formulários
    forms = estado.get('formularios', [])
    if forms:
        partes.append(f"\n{'='*60}")
        partes.append("FORMULÁRIOS:")
        for fi, form in enumerate(forms):
            partes.append(f"\n  Formulário {fi+1}:")
            for campo in form.get('campos', []):
                tipo = campo.get('tipo', '')
                label = campo.get('label', '') or campo.get('placeholder', '') or campo.get('name', '')
                id_campo = campo.get('id', '') or campo.get('name', '')
                valor = campo.get('valor_atual', '')
                
                if tipo == 'select':
                    opcoes = campo.get('opcoes', [])
                    opcoes_txt = ', '.join([f'"{o["texto"]}"' for o in opcoes[:15]])
                    partes.append(f"    [{tipo}] #{id_campo} ({label}): valor_atual=\"{valor}\" → opções: [{opcoes_txt}]")
                else:
                    partes.append(f"    [{tipo}] #{id_campo} ({label}): valor_atual=\"{valor}\"")
            
            for btn in form.get('botoes', []):
                partes.append(f"    [BOTÃO] #{btn.get('id','')} \"{btn.get('texto','')}\"")
    
    # Tabelas
    tabelas = estado.get('tabelas', [])
    if tabelas:
        partes.append(f"\n{'='*60}")
        partes.append("TABELAS:")
        for ti, tbl in enumerate(tabelas):
            headers = tbl.get('headers', [])
            partes.append(f"\n  Tabela {ti+1} — colunas: {' | '.join(headers) if headers else '(sem cabeçalho)'}")
            for li, linha in enumerate(tbl.get('linhas', [])[:10]):
                cells = ' | '.join([f"{c['coluna']}: {c['texto'][:50]}" for c in linha.get('celulas', [])])
                partes.append(f"    Linha {li+1}: {cells}")
                for link in linha.get('links', []):
                    icone = " [TEM ÍCONE/IMAGEM]" if link.get('tem_icone') else ""
                    col = f" (coluna: {link['coluna']})" if link.get('coluna') else ""
                    partes.append(f"      → Link{col}: \"{link['texto']}\"{icone} href=\"{link['href'][:80]}\"")
    
    # Links gerais
    links = estado.get('links', [])
    if links:
        partes.append(f"\n{'='*60}")
        partes.append(f"LINKS NA PÁGINA ({len(links)}):")
        for link in links[:20]:
            icone = " [ÍCONE]" if link.get('tem_icone') else ""
            partes.append(f"  \"{link['texto']}\"{icone} → {link['href'][:80]}")
    
    # Paginação
    pag = estado.get('paginacao', [])
    if pag:
        partes.append(f"\nPAGINAÇÃO: {', '.join([p['texto'] for p in pag])}")
    
    return '\n'.join(partes)


def _montar_prompt(estado_formatado: str, legislacao: dict, historico: list, passo: int) -> str:
    """Monta o prompt universal para a IA decidir a próxima ação."""
    
    tipo = legislacao.get('tipo', 'Lei Complementar')
    numero = legislacao.get('numero', '')
    ano = legislacao.get('ano', '')
    municipio = legislacao.get('municipio', '')
    data_pub = legislacao.get('data_publicacao', '')
    
    desc = f"{tipo} nº {numero}/{ano} — {municipio}"
    
    historico_txt = ""
    if historico:
        historico_txt = "\n\nHISTÓRICO DAS AÇÕES ANTERIORES:\n"
        for h in historico[-8:]:  # últimas 8 ações
            historico_txt += f"  Passo {h['passo']}: {h['acao']} → {h['resultado']}\n"
    
    return f"""Você é um pesquisador navegando a web para encontrar uma legislação brasileira.
Seu objetivo é encontrar o LINK (URL ou PDF) da legislação abaixo.

LEGISLAÇÃO BUSCADA:
- Tipo: {tipo}
- Número: {numero}
- Ano: {ano}
- Município: {municipio}
- Data de publicação: {data_pub or 'desconhecida'}

PASSO ATUAL: {passo}
{historico_txt}

{'='*60}
ESTADO ATUAL DA PÁGINA:
{'='*60}
{estado_formatado}
{'='*60}

INSTRUÇÕES:

Você está olhando para a página acima. Decida o que fazer.

ANTES DE AGIR, VOCÊ DEVE OBRIGATORIAMENTE PONDERAR AS CONSEQUÊNCIAS.

Para CADA opção disponível na página, analise:
- "Se eu fizer X, o que provavelmente vai acontecer?"
- "Isso me aproxima ou me afasta da legislação?"
- "Posso perder algo fazendo isso? (ex: sair da página de resultados, perder a sessão)"
- "Existe uma opção mais segura ou direta?"

Exemplos de ponderação:
- "Posso clicar em 'Consultar' agora, mas ainda não preenchi o tipo de ato. Se eu submeter sem filtrar, vou receber centenas de resultados e vai ser difícil encontrar. Melhor preencher primeiro."
- "Vejo 99 resultados em 4 páginas. Posso refinar a busca, mas antes devo olhar as páginas — talvez a legislação já esteja listada e eu só preciso achá-la."
- "Há um ícone na coluna 'Arquivo' — provavelmente é o download do texto completo. Mas antes de clicar, preciso verificar se esta linha é realmente a LC 198/2019 (conferir número, data, ementa)."
- "O PDF do dia 14/01/2019 não continha a lei. Faz sentido tentar o dia 15 — leis frequentemente são publicadas no DO um ou dois dias após a assinatura."

SUA ANÁLISE DEVE SEGUIR ESTA ORDEM:
1. O que eu vejo nesta página? (descreva objetivamente)
2. Quais são TODAS as minhas opções? (liste cada uma)
3. Para CADA opção: qual a consequência provável? (positiva e negativa)
4. Qual opção escolho e POR QUÊ? (justifique comparando com as alternativas)

TIPOS DE AÇÃO DISPONÍVEIS:

- "preencher_e_submeter": preencher TODOS os campos necessários do formulário E clicar no botão de submissão. Use quando vê um formulário com múltiplos campos.
- "clicar": clicar num botão ou link
- "baixar": clicar num link/ícone que provavelmente baixa um arquivo (PDF)
- "navegar": ir para uma URL específica
- "proximo": ir para a próxima página de resultados
- "voltar": voltar à página anterior
- "concluido": encontrei a legislação! Aqui está o link.
- "desistir": não consigo encontrar a legislação neste site

REGRAS:
- Para "preencher_e_submeter": preencha TODOS os campos de uma vez. Informe uma lista de campos.
- Para campos de data: use formato dd/mm/aaaa
- Para selects: informe o TEXTO VISÍVEL da opção (ex: "Municipal"), não o value numérico
- Se vê uma tabela de resultados, PRIMEIRO verifique se a legislação buscada está na lista
- Se encontrou a legislação numa tabela, clique no link/ícone que leva ao TEXTO COMPLETO
- **Ícones/imagens em colunas como "Arquivo" são BOTÕES DE DOWNLOAD** — clique neles!
- Se a coluna "Arquivo" mostra um ícone (img, svg), esse é o link para o documento
- Se um resultado tem status "Revogado", "Sem efeito" etc, registre isso
- NÃO clique em "Voltar", "Imprimir", links de redes sociais
- **NUNCA saia do site atual** se já encontrou a legislação na listagem. Insista tentando clicar de outras formas.
- Se um clique falhou (ex: "Clicou sem download"), TENTE NOVAMENTE com outra estratégia (ex: usar "clicar" em vez de "baixar", ou tentar outro link da mesma linha). NÃO desista nem vá para outro site.

RESPONDA COM JSON:
{{
    "o_que_vejo": "descrição objetiva do que a página mostra",
    "opcoes_e_consequencias": [
        {{
            "opcao": "o que posso fazer",
            "consequencia_provavel": "o que provavelmente acontece se eu fizer isso",
            "risco": "o que posso perder ou o que pode dar errado",
            "me_aproxima": true/false
        }}
    ],
    "decisao": "qual opção escolhi e POR QUÊ (comparando com as alternativas)",
    "acao": {{
        "tipo": "preencher_e_submeter|clicar|baixar|navegar|proximo|voltar|concluido|desistir",
        "campos": [
            {{"seletor": "#id", "tipo": "input|select|date", "valor": "valor a preencher"}}
        ],
        "botao_submit": "#id_do_botao (para preencher_e_submeter)",
        "alvo": "#id_do_elemento ou descrição (para clicar/baixar)",
        "href": "URL/href do link (se clicar/baixar/navegar)"
    }},
    "resultado_esperado": "o que espero que aconteça após esta ação",
    "legislacao_encontrada": {{
        "encontrada": true/false,
        "confirmacao": "como confirmei que é a legislação certa (cabeçalho, número, data, ementa)",
        "url": "URL ou link do PDF da legislação",
        "status": "Válido|Revogado|etc (se visível)",
        "pagina_no_pdf": "número da página no PDF (se aplicável)"
    }}
}}

IMPORTANTE: O campo "legislacao_encontrada" só deve ter "encontrada": true quando você:
1. Já CLICOU no link/ícone e obteve a URL real (começando com http)
2. NÃO marque como encontrada apenas por ver o resultado na tabela — primeiro clique para abrir
3. Se a URL é "#" ou vazia, NÃO é encontrada — execute a ação de clicar/baixar primeiro
4. Só marque encontrada DEPOIS de ter a URL do documento aberto"""


def navegar_como_humano(
    page,
    frame,
    legislacao: dict,
    chamar_llm,
    logs: list,
    label: str = '',
    max_passos: int = 15
) -> dict:
    """
    Navega uma página web como um humano para encontrar uma legislação.
    
    Args:
        page: Playwright page
        frame: Frame ou page para interagir (pode ser iframe)
        legislacao: dict com tipo, numero, ano, municipio, data_publicacao
        chamar_llm: função(prompt, logs, label) -> str
        logs: lista de logs
        label: prefixo para logs
        max_passos: máximo de ações antes de desistir
    
    Returns:
        dict com:
            - encontrada: bool
            - url: str (URL ou caminho do PDF)
            - status: str (Válido, Revogado, etc)
            - confirmacao: str (como confirmou)
            - pdf_path: str (se baixou PDF)
            - pagina_pdf: int (página no PDF, se aplicável)
    """
    
    historico = []
    resultado = {
        'encontrada': False,
        'url': '',
        'status': '',
        'confirmacao': '',
        'pdf_path': None,
        'pagina_pdf': None
    }
    
    for passo in range(1, max_passos + 1):
        try:
            # 1. Capturar estado da página
            estado = _capturar_estado_pagina(page, frame)
            estado_fmt = _formatar_estado_para_prompt(estado)
            
            # 2. Pedir decisão à IA
            prompt = _montar_prompt(estado_fmt, legislacao, historico, passo)
            resp = chamar_llm(prompt, logs, f'{label} passo {passo}')
            
            if not resp:
                logs.append({'nivel': 'aviso', 'msg': f'{label}: Passo {passo} — IA não respondeu'})
                break
            
            # 3. Parsear resposta
            try:
                resp_clean = re.sub(r'^```(?:json)?\s*|\s*```$', '', resp.strip())
                decisao = json.loads(resp_clean)
            except (json.JSONDecodeError, ValueError):
                logs.append({'nivel': 'aviso', 'msg': f'{label}: Passo {passo} — resposta inválida'})
                historico.append({'passo': passo, 'acao': 'erro_parse', 'resultado': 'resposta da IA não é JSON válido'})
                continue
            
            pensamento = decisao.get('decisao', '') or decisao.get('pensamento', '') or ''
            o_que_vejo = (decisao.get('o_que_vejo', '') or '')[:100]
            opcoes = decisao.get('opcoes_e_consequencias', []) or []
            acao = decisao.get('acao', {}) or {}
            tipo_acao = acao.get('tipo', '') or ''
            alvo = acao.get('alvo', '') or ''
            valor = acao.get('valor', '') or ''
            href = acao.get('href', '') or ''
            resultado_esperado = (decisao.get('resultado_esperado', '') or '')[:100]
            
            # Log detalhado do raciocínio
            logs.append({'nivel': 'info', 'msg': f'{label}: 👁️ Passo {passo}: {o_que_vejo[:80]}'})
            if opcoes:
                for op in opcoes[:4]:
                    aprox = '✅' if op.get('me_aproxima') else '❌'
                    logs.append({'nivel': 'info', 'msg': f'{label}:   {aprox} {(op.get("opcao","") or "")[:50]} → {(op.get("consequencia_provavel","") or "")[:50]}'})
            logs.append({'nivel': 'info', 'msg': f'{label}: 🧠 Decisão: {tipo_acao} — {pensamento[:80]}'})
            
            # 4. Verificar se encontrou
            leg_encontrada = decisao.get('legislacao_encontrada', {}) or {}
            leg_url = (leg_encontrada.get('url', '') or '').strip()
            
            # Só aceitar como "encontrada" se tem URL real (não null, não "#", não vazio)
            if leg_encontrada.get('encontrada') and leg_url and leg_url != '#' and leg_url.startswith('http'):
                resultado['encontrada'] = True
                resultado['url'] = leg_url
                resultado['status'] = (leg_encontrada.get('status', '') or '')
                resultado['confirmacao'] = (leg_encontrada.get('confirmacao', '') or '')
                if leg_encontrada.get('pagina_no_pdf'):
                    try:
                        resultado['pagina_pdf'] = int(leg_encontrada['pagina_no_pdf'])
                    except (ValueError, TypeError):
                        pass
                
                logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ Legislação encontrada! {leg_url[:80]}'})
                logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ Confirmação: {resultado["confirmacao"][:100]}'})
                if resultado['status']:
                    logs.append({'nivel': 'info', 'msg': f'{label}: 📌 Status: {resultado["status"]}'})
                
                historico.append({'passo': passo, 'acao': 'concluido', 'resultado': f'Encontrada: {leg_url[:60]}'})
                break
            
            # 5. Executar ação
            if tipo_acao == 'desistir':
                logs.append({'nivel': 'aviso', 'msg': f'{label}: ❌ Passo {passo}: IA desistiu — {pensamento[:80]}'})
                historico.append({'passo': passo, 'acao': 'desistir', 'resultado': pensamento[:100]})
                break
            
            exec_resultado = _executar_acao(page, frame, acao, logs, label, passo) or 'sem resultado'
            historico.append({'passo': passo, 'acao': f'{tipo_acao}: {alvo or href or valor}'[:60], 'resultado': exec_resultado[:100]})
            
            # Se abriu popup, mudar contexto para a popup no próximo passo
            if exec_resultado.startswith('Popup aberto:'):
                # Pegar a última popup aberta
                try:
                    all_pages = page.context.pages
                    if len(all_pages) > 1:
                        popup_pg = all_pages[-1]  # última aba
                        # Ler conteúdo da popup para verificar se é a legislação
                        try:
                            popup_text = popup_pg.evaluate('() => document.body?.innerText || ""') or ''
                            popup_url = popup_pg.url
                            logs.append({'nivel': 'info', 'msg': f'{label}: 📄 Popup: {len(popup_text)} chars em {popup_url[:50]}'})
                            
                            # Mudar frame para popup no próximo passo
                            frame = popup_pg
                            page = popup_pg
                        except Exception:
                            pass
                except Exception:
                    pass
            
            # Se baixou PDF, verificar e guardar caminho
            if exec_resultado.startswith('PDF_BAIXADO:'):
                pdf_path = exec_resultado.split(':', 1)[1]
                resultado['pdf_path'] = pdf_path
                # O próximo passo da IA vai receber info sobre o PDF
            
        except Exception as e:
            err_msg = str(e)[:80]
            if 'closed' in err_msg.lower() or 'disposed' in err_msg.lower():
                logs.append({'nivel': 'aviso', 'msg': f'{label}: Passo {passo}: Página fechou — encerrando navegação'})
                break
            logs.append({'nivel': 'aviso', 'msg': f'{label}: Passo {passo} erro: {err_msg}'})
            historico.append({'passo': passo, 'acao': 'erro', 'resultado': err_msg})
    
    if not resultado['encontrada']:
        logs.append({'nivel': 'aviso', 'msg': f'{label}: Navegação encerrada após {len(historico)} passos sem encontrar'})
    
    return resultado


def _executar_acao(page, frame, acao: dict, logs: list, label: str, passo: int) -> str:
    """Executa uma ação decidida pela IA. Retorna descrição do resultado."""
    
    tipo = acao.get('tipo', '')
    alvo = acao.get('alvo', '')
    valor = acao.get('valor', '')
    href = acao.get('href', '')
    
    try:
        if tipo == 'preencher_e_submeter':
            # Preencher múltiplos campos e submeter
            campos = acao.get('campos', [])
            botao = acao.get('botao_submit', '')
            
            if not campos:
                return 'Nenhum campo informado para preencher'
            
            preenchidos = []
            for campo in campos:
                sel = campo.get('seletor', '')
                tipo_campo = campo.get('tipo', 'input')
                valor = campo.get('valor', '')
                
                if not sel or not valor:
                    continue
                
                el = _encontrar_elemento(frame, sel)
                if not el:
                    logs.append({'nivel': 'info', 'msg': f'{label}: ⚠️ Campo não encontrado: {sel}'})
                    continue
                
                if tipo_campo == 'select':
                    try:
                        el.select_option(label=valor)
                    except Exception:
                        try:
                            el.select_option(value=valor)
                        except Exception:
                            options = el.evaluate('el => Array.from(el.options).map(o => ({v: o.value, t: o.textContent.trim()}))')
                            for opt in options:
                                if valor.lower() in opt['t'].lower():
                                    el.select_option(value=opt['v'])
                                    break
                    el.evaluate('el => el.dispatchEvent(new Event("change", {bubbles: true}))')
                    time.sleep(2)
                    try:
                        frame.wait_for_load_state('networkidle', timeout=5000)
                    except Exception:
                        pass
                    val_sel = el.evaluate('el => el.options[el.selectedIndex]?.text || ""')
                    preenchidos.append(f'{sel}="{val_sel}"')
                    logs.append({'nivel': 'info', 'msg': f'{label}: ✏️ {sel} = "{val_sel}"'})
                
                elif tipo_campo == 'date':
                    el_type = (el.get_attribute('type') or '').lower()
                    if el_type in ('date', 'datetime-local'):
                        match = re.match(r'^(\d{2})/(\d{2})/(\d{4})$', valor)
                        if match:
                            valor_iso = f'{match.group(3)}-{match.group(2)}-{match.group(1)}'
                            el.fill(valor_iso)
                            preenchidos.append(f'{sel}="{valor}" (ISO: {valor_iso})')
                            logs.append({'nivel': 'info', 'msg': f'{label}: 📅 {sel} = "{valor}" (ISO: {valor_iso})'})
                        else:
                            el.fill(valor)
                            preenchidos.append(f'{sel}="{valor}"')
                    else:
                        el.click()
                        time.sleep(0.1)
                        el.fill('')
                        el.type(valor, delay=30)
                        preenchidos.append(f'{sel}="{valor}"')
                        logs.append({'nivel': 'info', 'msg': f'{label}: ✏️ {sel} = "{valor}"'})
                
                else:  # input, textarea
                    el.click()
                    time.sleep(0.1)
                    el.fill('')
                    el.type(valor, delay=30)
                    preenchidos.append(f'{sel}="{valor}"')
                    logs.append({'nivel': 'info', 'msg': f'{label}: ✏️ {sel} = "{valor}"'})
            
            # Submeter
            if botao:
                time.sleep(1)
                btn = _encontrar_elemento(frame, botao)
                if btn:
                    btn.click()
                    logs.append({'nivel': 'info', 'msg': f'{label}: 🖱️ Clicou: {botao}'})
                    time.sleep(3)
                    try:
                        page.wait_for_load_state('networkidle', timeout=15000)
                    except Exception:
                        pass
                else:
                    logs.append({'nivel': 'aviso', 'msg': f'{label}: Botão não encontrado: {botao}'})
            
            return f'Preenchidos: {", ".join(preenchidos)}'
        
        elif tipo == 'preencher':
            el = _encontrar_elemento(frame, alvo)
            if not el:
                return f'Elemento não encontrado: {alvo}'
            
            # Verificar se é campo de data (type=date)
            el_type = (el.get_attribute('type') or '').lower()
            if el_type in ('date', 'datetime-local'):
                # Converter dd/mm/aaaa para yyyy-mm-dd
                match = re.match(r'^(\d{2})/(\d{2})/(\d{4})$', valor)
                if match:
                    valor_iso = f'{match.group(3)}-{match.group(2)}-{match.group(1)}'
                    el.fill(valor_iso)
                    logs.append({'nivel': 'info', 'msg': f'{label}: ✏️ #{alvo} = "{valor}" (ISO: {valor_iso})'})
                else:
                    el.fill(valor)
                    logs.append({'nivel': 'info', 'msg': f'{label}: ✏️ #{alvo} = "{valor}"'})
            else:
                el.click()
                time.sleep(0.2)
                el.fill('')
                el.type(valor, delay=30)
                logs.append({'nivel': 'info', 'msg': f'{label}: ✏️ #{alvo} = "{valor}"'})
            
            return f'Preenchido: {alvo} = "{valor}"'
        
        elif tipo == 'selecionar':
            el = _encontrar_elemento(frame, alvo)
            if not el:
                return f'Elemento não encontrado: {alvo}'
            
            # Tentar selecionar pelo texto visível
            try:
                el.select_option(label=valor)
            except Exception:
                try:
                    el.select_option(value=valor)
                except Exception:
                    # Tentar match parcial
                    options = el.evaluate('''el => Array.from(el.options).map(o => ({v: o.value, t: o.textContent.trim()}))''')
                    for opt in options:
                        if valor.lower() in opt['t'].lower():
                            el.select_option(value=opt['v'])
                            break
            
            # Disparar change event (importante para ASP.NET)
            el.evaluate('el => el.dispatchEvent(new Event("change", {bubbles: true}))')
            time.sleep(2)
            
            # Esperar possível postback
            try:
                frame.wait_for_load_state('networkidle', timeout=5000)
            except Exception:
                pass
            
            valor_selecionado = el.evaluate('el => el.options[el.selectedIndex]?.text || ""')
            logs.append({'nivel': 'info', 'msg': f'{label}: ✏️ #{alvo} = "{valor_selecionado}"'})
            return f'Selecionado: {alvo} = "{valor_selecionado}"'
        
        elif tipo == 'clicar':
            el = _encontrar_elemento(frame, alvo, href)
            if not el:
                if frame != page:
                    el = _encontrar_elemento(page, alvo, href)
                if not el:
                    return f'Elemento não encontrado: {alvo or href}'
            
            # Contar páginas antes do clique
            n_pages_antes = len(page.context.pages)
            url_antes = page.url
            
            el.click()
            time.sleep(4)
            
            # Verificar se abriu nova aba (comparar contagem)
            all_pages = page.context.pages
            if len(all_pages) > n_pages_antes:
                nova_aba = all_pages[-1]
                try:
                    nova_aba.wait_for_load_state('networkidle', timeout=15000)
                except Exception:
                    try:
                        nova_aba.wait_for_load_state('domcontentloaded', timeout=10000)
                    except Exception:
                        pass
                time.sleep(1)
                logs.append({'nivel': 'ok', 'msg': f'{label}: 🪟 Nova aba: {nova_aba.url[:60]}'})
                return f'Popup aberto: {nova_aba.url}'
            
            # URL mudou?
            try:
                page.wait_for_load_state('networkidle', timeout=5000)
            except Exception:
                pass
            
            if page.url != url_antes:
                logs.append({'nivel': 'info', 'msg': f'{label}: 🖱️ Navegou para: {page.url[:60]}'})
                return f'Navegou para: {page.url}'
            
            logs.append({'nivel': 'info', 'msg': f'{label}: 🖱️ Clicou: {alvo or href}'[:60]})
            return f'Clicou: {alvo or href}'
        
        elif tipo == 'baixar':
            el = _encontrar_elemento(frame, alvo, href)
            if not el and frame != page:
                el = _encontrar_elemento(page, alvo, href)
            
            if not el and href:
                for a in frame.query_selector_all('a[href]'):
                    a_href = a.get_attribute('href') or ''
                    if href in a_href or a_href.endswith(href.split('/')[-1]):
                        el = a
                        break
            
            if not el:
                # Último recurso: buscar qualquer a com img dentro de td (ícone em tabela)
                for td in frame.query_selector_all('td'):
                    a_in_td = td.query_selector('a')
                    if a_in_td and a_in_td.query_selector('img'):
                        el = a_in_td
                        logs.append({'nivel': 'info', 'msg': f'{label}: 🔧 Ícone encontrado por busca em td>a>img'})
                        break
            
            if not el:
                return f'Elemento de download não encontrado: {alvo or href}'
            
            # Log do elemento encontrado
            try:
                el_info = el.evaluate('''el => {
                    return {
                        tag: el.tagName,
                        href: el.getAttribute('href') || '',
                        onclick: el.getAttribute('onclick') || '',
                        target: el.getAttribute('target') || '',
                        outerHTML: el.outerHTML.substring(0, 200),
                        hasImg: !!el.querySelector('img'),
                        parentOnclick: el.parentElement ? (el.parentElement.getAttribute('onclick') || '') : ''
                    }
                }''')
                logs.append({'nivel': 'info', 'msg': f'{label}: 🔧 Elemento: {el_info.get("outerHTML","")[:80]}'})
                logs.append({'nivel': 'info', 'msg': f'{label}: 🔧 onclick="{el_info.get("onclick","")[:50]}" target="{el_info.get("target","")}" parentOnclick="{el_info.get("parentOnclick","")[:50]}"'})
            except Exception:
                pass
            
            # Registrar estado ANTES do clique
            url_antes = page.url
            n_pages_antes = len(page.context.pages)
            _download_obj = None
            
            def _on_download(d):
                nonlocal _download_obj
                _download_obj = d
            
            page.on('download', _on_download)
            
            el.click()
            time.sleep(5)  # Dar tempo para popup/download/navegação
            
            try:
                page.remove_listener('download', _on_download)
            except Exception:
                pass
            
            # Caso 1: Download
            if _download_obj:
                try:
                    dl_path = tempfile.mktemp(suffix='.pdf')
                    _download_obj.save_as(dl_path)
                    dl_size = os.path.getsize(dl_path) // 1024
                    logs.append({'nivel': 'ok', 'msg': f'{label}: 📥 Download: {_download_obj.suggested_filename} ({dl_size}KB)'})
                    
                    try:
                        import fitz
                        doc = fitz.open(dl_path)
                        n_pages = doc.page_count
                        doc.close()
                        logs.append({'nivel': 'info', 'msg': f'{label}: 📄 PDF: {n_pages} páginas'})
                    except Exception:
                        pass
                    
                    return f'PDF_BAIXADO:{dl_path}'
                except Exception as e_save:
                    logs.append({'nivel': 'info', 'msg': f'{label}: ⚠️ Download falhou: {str(e_save)[:60]}'})
            
            # Caso 2: Nova aba/popup (comparar contagem)
            all_pages = page.context.pages
            if len(all_pages) > n_pages_antes:
                nova_aba = all_pages[-1]
                try:
                    nova_aba.wait_for_load_state('networkidle', timeout=15000)
                except Exception:
                    try:
                        nova_aba.wait_for_load_state('domcontentloaded', timeout=10000)
                    except Exception:
                        pass
                time.sleep(1)
                logs.append({'nivel': 'ok', 'msg': f'{label}: 🪟 Nova aba: {nova_aba.url[:60]}'})
                return f'Popup aberto: {nova_aba.url}'
            
            # Caso 3: URL mudou
            try:
                page.wait_for_load_state('networkidle', timeout=5000)
            except Exception:
                pass
            
            if page.url != url_antes:
                logs.append({'nivel': 'info', 'msg': f'{label}: 🖱️ Navegou para: {page.url[:60]}'})
                return f'Navegou para: {page.url}'
            
            # Caso 4: Nada visível — extrair onclick e executar manualmente
            try:
                el_onclick = el.get_attribute('onclick') or ''
                el_href_attr = el.get_attribute('href') or ''
                el_target = el.get_attribute('target') or ''
                
                logs.append({'nivel': 'info', 'msg': f'{label}: 🔧 Elemento: href="{el_href_attr[:30]}" onclick="{el_onclick[:50]}" target="{el_target}"'})
                
                # Extrair URL do onclick (ex: window.open('url',...))
                url_from_onclick = ''
                if el_onclick:
                    import re as _re
                    _url_match = _re.search(r"(?:window\.open|location\.href|window\.location)\s*[\(=]\s*['\"]([^'\"]+)", el_onclick)
                    if _url_match:
                        url_from_onclick = _url_match.group(1)
                    else:
                        # Executar onclick diretamente
                        logs.append({'nivel': 'info', 'msg': f'{label}: 🔧 Executando onclick via JS...'})
                        n_before = len(page.context.pages)
                        try:
                            el.evaluate('el => el.onclick ? el.onclick() : null')
                            time.sleep(4)
                        except Exception:
                            try:
                                frame.evaluate(f'() => {{ {el_onclick} }}')
                                time.sleep(4)
                            except Exception as e_eval:
                                logs.append({'nivel': 'info', 'msg': f'{label}: ⚠️ onclick eval: {str(e_eval)[:50]}'})
                        
                        pages_now = page.context.pages
                        if len(pages_now) > n_before:
                            nova = pages_now[-1]
                            try:
                                nova.wait_for_load_state('networkidle', timeout=15000)
                            except Exception:
                                pass
                            logs.append({'nivel': 'ok', 'msg': f'{label}: 🪟 Nova aba via onclick: {nova.url[:60]}'})
                            return f'Popup aberto: {nova.url}'
                
                if url_from_onclick:
                    logs.append({'nivel': 'info', 'msg': f'{label}: 🔧 URL do onclick: {url_from_onclick[:60]}'})
                    if url_from_onclick.startswith('/') or not url_from_onclick.startswith('http'):
                        base = frame.url or page.url
                        from urllib.parse import urljoin
                        url_from_onclick = urljoin(base, url_from_onclick)
                    new_page = page.context.new_page()
                    try:
                        new_page.goto(url_from_onclick, wait_until='networkidle', timeout=20000)
                    except Exception:
                        try:
                            new_page.goto(url_from_onclick, wait_until='domcontentloaded', timeout=15000)
                        except Exception:
                            pass
                    time.sleep(2)
                    logs.append({'nivel': 'ok', 'msg': f'{label}: 🪟 Abriu URL do onclick: {new_page.url[:60]}'})
                    return f'Popup aberto: {new_page.url}'
                
                # Se href é real (não # nem javascript:), abrir diretamente
                if el_href_attr and el_href_attr != '#' and not el_href_attr.startswith('javascript:'):
                    from urllib.parse import urljoin
                    full_url = urljoin(frame.url or page.url, el_href_attr)
                    new_page = page.context.new_page()
                    try:
                        new_page.goto(full_url, wait_until='networkidle', timeout=20000)
                    except Exception:
                        pass
                    time.sleep(2)
                    logs.append({'nivel': 'ok', 'msg': f'{label}: 🪟 Abriu href: {new_page.url[:60]}'})
                    return f'Popup aberto: {new_page.url}'
                    
            except Exception as e_js:
                logs.append({'nivel': 'info', 'msg': f'{label}: ⚠️ Fallback JS: {str(e_js)[:60]}'})
            
            logs.append({'nivel': 'info', 'msg': f'{label}: 🖱️ Clicou mas nada mudou — pode ter sido iframe/JS'})
            return f'Clicou: {alvo or href} (sem mudança visível)'
        elif tipo == 'navegar':
            url = href or valor
            if url:
                try:
                    page.goto(url, wait_until='networkidle', timeout=20000)
                except Exception:
                    try:
                        page.goto(url, wait_until='domcontentloaded', timeout=15000)
                    except Exception:
                        return f'Não conseguiu navegar para: {url[:60]}'
                time.sleep(2)
                logs.append({'nivel': 'info', 'msg': f'{label}: 🌐 Navegou para: {url[:60]}'})
                return f'Navegou para: {url}'
            return 'URL não informada'
        
        elif tipo == 'proximo':
            # Buscar link de próxima página
            for a in frame.query_selector_all('a[href]'):
                text = (a.text_content() or '').strip().lower()
                if any(x in text for x in ['próxima', 'proxima', 'next', '»', 'próxima página']):
                    a.click()
                    time.sleep(2)
                    try:
                        page.wait_for_load_state('networkidle', timeout=10000)
                    except Exception:
                        pass
                    logs.append({'nivel': 'info', 'msg': f'{label}: ➡️ Próxima página'})
                    return 'Foi para próxima página'
            return 'Link de próxima página não encontrado'
        
        elif tipo == 'voltar':
            page.go_back(wait_until='networkidle', timeout=10000)
            time.sleep(2)
            logs.append({'nivel': 'info', 'msg': f'{label}: ⬅️ Voltou'})
            return 'Voltou à página anterior'
        
        elif tipo == 'refinar':
            logs.append({'nivel': 'info', 'msg': f'{label}: 🔄 Refinando busca...'})
            return 'Refinando busca (próximo passo definirá como)'
        
        elif tipo == 'concluido':
            return 'Legislação encontrada'
        
        else:
            return f'Ação desconhecida: {tipo}'
    
    except Exception as e:
        return f'Erro ao executar {tipo}: {str(e)[:80]}'


def _encontrar_elemento(frame, alvo: str, href: str = None):
    """Encontra um elemento na página por ID, seletor CSS, href, ou descrição."""
    
    if not alvo and not href:
        return None
    
    el = None
    
    # Tentar pelo alvo (ID ou seletor)
    if alvo:
        # Se parece com seletor CSS
        if alvo.startswith('#') or alvo.startswith('.') or alvo.startswith('['):
            try:
                el = frame.query_selector(alvo)
            except Exception:
                pass
        
        # Tentar como ID (sem #)
        if not el:
            try:
                el = frame.query_selector(f'#{alvo}')
            except Exception:
                pass
        
        # Tentar como name
        if not el:
            try:
                el = frame.query_selector(f'[name="{alvo}"]')
            except Exception:
                pass
        
        # Tentar por texto do botão
        if not el:
            for btn in frame.query_selector_all('button, input[type="submit"], input[type="button"]'):
                btn_text = (btn.text_content() or btn.get_attribute('value') or '').strip()
                if alvo.lower() in btn_text.lower():
                    el = btn
                    break
        
        # Tentar por texto do link
        if not el:
            for a in frame.query_selector_all('a'):
                a_text = (a.text_content() or '').strip()
                if a_text and alvo.lower() in a_text.lower():
                    el = a
                    break
    
    # Tentar pelo href
    if not el and href:
        for a in frame.query_selector_all('a[href]'):
            a_href = a.get_attribute('href') or ''
            if href == a_href or a_href.endswith(href) or href.endswith(a_href.split('/')[-1]):
                el = a
                break
    
    # Estratégia especial: buscar ícone/link clicável na coluna "Arquivo" de tabelas
    if not el and alvo:
        alvo_lower = alvo.lower()
        # Se menciona "arquivo", "coluna", "ícone", "tabela", "download"
        if any(k in alvo_lower for k in ['arquivo', 'coluna', 'ícone', 'icone', 'tabela', 'download', 'imagem', 'icon']):
            # Buscar em todas as linhas da tabela
            for tr in frame.query_selector_all('tr'):
                # Verificar se a linha tem links com ícones
                links = tr.query_selector_all('a[href], a[onclick], [onclick]')
                for link in links:
                    has_icon = link.query_selector('img, svg, i, [class*="icon"]')
                    if has_icon:
                        link_href = (link.get_attribute('href') or '')
                        # Pular links ancora vazia
                        if link_href == '#' and not link.get_attribute('onclick'):
                            continue
                        el = link
                        break
                if el:
                    break
    
    # Último recurso: buscar qualquer link com ícone na página
    if not el and alvo and any(k in (alvo or '').lower() for k in ['ícone', 'icone', 'icon', 'imagem', 'img']):
        for a in frame.query_selector_all('a'):
            if a.query_selector('img, svg, i'):
                a_href = (a.get_attribute('href') or '')
                if a_href and a_href != '#':
                    el = a
                    break
    
    return el
