"""
navegador_universal.py — Navegação visual por screenshot
=========================================================

A IA recebe um screenshot da página e decide o que fazer,
como um humano: olha, entende, clica.

Sem extração de DOM, sem heurísticas, sem _encontrar_elemento.
"""

import os
import time
import json
import tempfile
import base64


# Viewport fixo (deve ser igual ao definido no buscador_legislacoes.py)
VIEWPORT_W = 1280
VIEWPORT_H = 900


def _screenshot_base64(page) -> str:
    """Tira screenshot da página e retorna como base64."""
    img_bytes = page.screenshot(type='png', full_page=False)
    return base64.b64encode(img_bytes).decode('utf-8')


def _chamar_gemini_visao(prompt: str, screenshot_b64: str, logs: list, label: str) -> str:
    """Chama Gemini com imagem (visão) e retorna texto."""
    import google.generativeai as genai

    api_key = os.getenv('GEMINI_API_KEY', '')
    if not api_key:
        logs.append({'nivel': 'erro', 'msg': f'{label}: GEMINI_API_KEY não configurada'})
        return ''

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel('gemini-2.5-flash')

    image_part = {
        'mime_type': 'image/png',
        'data': screenshot_b64
    }

    for tentativa in range(3):
        if tentativa > 0:
            wait = tentativa * 3
            logs.append({'nivel': 'info', 'msg': f'{label}: aguardando {wait}s antes de retry #{tentativa}...'})
            time.sleep(wait)

        try:
            response = model.generate_content([prompt, image_part])
            texto = response.text.strip()
            logs.append({'nivel': 'ok', 'msg': f'{label}: Gemini respondeu ({len(texto)} chars)'})
            return texto
        except Exception as e:
            err = str(e)[:120]
            is_rate_limit = '429' in err or 'quota' in err.lower() or 'rate' in err.lower()
            logs.append({'nivel': 'aviso', 'msg': f'{label}: Gemini visão falhou: {err}'})
            if not is_rate_limit:
                break

    return ''


def _montar_prompt(legislacao: dict, historico: list, passo: int, url_atual: str) -> str:
    """Monta o prompt para a IA decidir a próxima ação baseada no screenshot."""

    tipo = legislacao.get('tipo', 'Lei Complementar')
    numero = legislacao.get('numero', '')
    ano = legislacao.get('ano', '')
    municipio = legislacao.get('municipio', '')
    data_pub = legislacao.get('data_publicacao', '')
    assunto = legislacao.get('assunto', '')

    historico_txt = ''
    if historico:
        historico_txt = '\n'.join([
            f"  Passo {h['passo']}: {h['acao']} -> {h['resultado']}"
            for h in historico[-5:]
        ])
        historico_txt = f"\n\nHISTORICO DOS ULTIMOS PASSOS:\n{historico_txt}"

    return f"""Voce esta navegando uma pagina web para encontrar a legislacao: {tipo} nº {numero}/{ano} — {municipio}.
Data de publicacao: {data_pub}. Assunto: {assunto}

URL ATUAL: {url_atual} | PASSO: {passo}
{historico_txt}

Olhe o screenshot. Decida a proxima acao.

ACOES DISPONIVEIS:
- "clicar": clicar em algo (x,y de 0 a 1000)
- "digitar": clicar num campo e digitar texto (x,y + texto)
- "selecionar": abrir um dropdown (x,y)
- "preencher_formulario": preencher TODOS os campos de um formulario de uma vez e submeter. Use quando vir um formulario com dropdowns e campos de texto. Informe os campos como lista.
- "scroll": rolar pagina ("baixo" ou "cima")
- "concluido": encontrei a legislacao (informar URL)
- "desistir": nao consigo encontrar

DICAS:
- Coordenadas normalizadas: 0,0 = canto superior esquerdo, 1000,1000 = canto inferior direito
- FORMULARIOS: use "preencher_formulario" para preencher tudo de uma vez. Nao preencha campo por campo — sites ASP.NET resetam campos.
- Icone na coluna "Arquivo" = link para o documento. Clique nele.
- So marque "encontrada" quando tiver URL real (http...), nao apenas por ver na tabela.
- Se Cloudflare/CAPTCHA, desista.

JSON (sem markdown):
{{
    "o_que_vejo": "...",
    "decisao": "o que vou fazer e por que",
    "acao": {{
        "tipo": "clicar|digitar|selecionar|preencher_formulario|scroll|concluido|desistir",
        "x": 500,
        "y": 450,
        "texto": "",
        "direcao": "",
        "campos": [
            {{"label": "Esfera", "valor": "Municipal", "tipo_campo": "select"}},
            {{"label": "Tipo de Ato", "valor": "Lei Complementar", "tipo_campo": "select"}},
            {{"label": "Nº do Ato", "valor": "198", "tipo_campo": "input"}},
            {{"label": "Data Inicial", "valor": "14/01/2019", "tipo_campo": "date"}}
        ],
        "botao_submit": "Consultar"
    }},
    "legislacao_encontrada": {{
        "encontrada": false,
        "url": "",
        "status": "",
        "confirmacao": ""
    }}
}}"""


def _executar_acao(page, acao: dict, logs: list, label: str) -> str:
    """Executa uma acao na pagina via mouse/teclado."""

    tipo = acao.get('tipo', '')
    x_norm = int(acao.get('x', 0) or 0)
    y_norm = int(acao.get('y', 0) or 0)
    texto = acao.get('texto', '') or ''
    direcao = acao.get('direcao', 'baixo') or 'baixo'

    # Converter coordenadas normalizadas (0-1000) para pixels reais
    x = int(x_norm * VIEWPORT_W / 1000)
    y = int(y_norm * VIEWPORT_H / 1000)

    try:
        if tipo == 'clicar':
            try:
                url_antes = page.url
            except Exception:
                url_antes = ''
            n_pages_antes = len(page.context.pages)

            try:
                page.mouse.click(x, y)
            except Exception as e_click:
                # Clique pode falhar se a página já fechou — verificar novas abas
                logs.append({'nivel': 'info', 'msg': f'{label}: ⚠️ Clique em ({x},{y}): {str(e_click)[:40]}'})
            
            time.sleep(3)

            # Nova aba? (verificar mesmo se houve erro)
            try:
                all_pages = page.context.pages
                if len(all_pages) > n_pages_antes:
                    nova = all_pages[-1]
                    try:
                        nova.wait_for_load_state('networkidle', timeout=15000)
                    except Exception:
                        try:
                            nova.wait_for_load_state('domcontentloaded', timeout=10000)
                        except Exception:
                            pass
                    time.sleep(2)
                    logs.append({'nivel': 'ok', 'msg': f'{label}: 🪟 Nova aba: {nova.url[:60]}'})
                    return f'Nova aba: {nova.url}'
            except Exception:
                pass

            # URL mudou?
            try:
                page.wait_for_load_state('networkidle', timeout=5000)
                if page.url != url_antes:
                    logs.append({'nivel': 'info', 'msg': f'{label}: 🖱️ Navegou para: {page.url[:60]}'})
                    return f'Navegou: {page.url}'
            except Exception:
                pass

            logs.append({'nivel': 'info', 'msg': f'{label}: 🖱️ Clicou em ({x}, {y}) [norm: {x_norm},{y_norm}]'})
            return f'Clicou em ({x}, {y})'

        elif tipo == 'digitar':
            page.mouse.click(x, y)
            time.sleep(0.5)

            # Limpar campo
            page.keyboard.press('Control+a')
            time.sleep(0.2)
            page.keyboard.press('Backspace')
            time.sleep(0.2)

            # Digitar
            page.keyboard.type(texto, delay=50)
            time.sleep(0.5)

            logs.append({'nivel': 'info', 'msg': f'{label}: ✏️ Digitou "{texto[:30]}" em ({x}, {y})'})
            return f'Digitou "{texto[:30]}" em ({x}, {y})'

        elif tipo == 'selecionar':
            page.mouse.click(x, y)
            time.sleep(1)

            logs.append({'nivel': 'info', 'msg': f'{label}: 📋 Abriu dropdown em ({x}, {y})'})
            return f'Abriu dropdown em ({x}, {y})'

        elif tipo == 'preencher_formulario':
            campos = acao.get('campos', []) or []
            botao = acao.get('botao_submit', '') or 'Consultar'
            frame = page.main_frame
            
            # Tentar usar frames internos se existirem
            for f in page.frames:
                if f != page.main_frame and f.query_selector('form, input, select'):
                    frame = f
                    break
            
            preenchidos = []
            for campo in campos:
                label_campo = (campo.get('label', '') or '').strip()
                valor = (campo.get('valor', '') or '').strip()
                tipo_campo = (campo.get('tipo_campo', '') or 'input').strip()
                
                if not label_campo or not valor:
                    continue
                
                # Encontrar o elemento pelo label
                el = None
                
                # 1. Buscar por label[for] -> id
                try:
                    for lbl in frame.query_selector_all('label'):
                        lbl_text = (lbl.text_content() or '').strip().lower()
                        if label_campo.lower() in lbl_text:
                            for_id = lbl.get_attribute('for')
                            if for_id:
                                el = frame.query_selector(f'#{for_id}')
                            break
                except Exception:
                    pass
                
                # 2. Buscar por placeholder, name, ou id contendo o label
                if not el:
                    label_lower = label_campo.lower()
                    selectors = [
                        f'input[placeholder*="{label_campo}" i]',
                        f'select[name*="{label_campo}" i]',
                        f'input[name*="{label_campo}" i]',
                    ]
                    # Tentar variações comuns
                    key_map = {
                        'esfera': ['esfera', 'Esfera'],
                        'tipo de ato': ['tipoato', 'TipoAto', 'tipo_ato'],
                        'tipo': ['tipo', 'Tipo'],
                        'número': ['num', 'Num', 'numero'],
                        'nº do ato': ['numato', 'NumAto', 'num_ato', 'txtNumAto'],
                        'nº': ['num', 'Num'],
                        'data inicial': ['dataato', 'DataAto', 'data_ato', 'txtDataAto'],
                        'data final': ['dataatofinal', 'DataAtoFinal', 'txtDataAtoFinal'],
                        'data': ['data', 'Data'],
                        'status': ['status', 'Status'],
                    }
                    for key, ids in key_map.items():
                        if key in label_lower:
                            for try_id in ids:
                                try:
                                    candidate = frame.query_selector(f'#{try_id}') or frame.query_selector(f'[id*="{try_id}" i]') or frame.query_selector(f'[name*="{try_id}" i]')
                                    if candidate:
                                        el = candidate
                                        break
                                except Exception:
                                    pass
                            if el:
                                break
                
                # 3. Buscar select/input perto de texto visível
                if not el:
                    try:
                        all_els = frame.query_selector_all('select, input:not([type="hidden"]):not([type="submit"])')
                        for candidate in all_els:
                            # Verificar texto anterior (td, label, span)
                            nearby_text = candidate.evaluate('''el => {
                                let t = '';
                                if (el.previousElementSibling) t += el.previousElementSibling.textContent || '';
                                let td = el.closest('td');
                                if (td && td.previousElementSibling) t += td.previousElementSibling.textContent || '';
                                let label = el.closest('label');
                                if (label) t += label.textContent || '';
                                return t.trim().toLowerCase();
                            }''')
                            if label_campo.lower() in (nearby_text or ''):
                                el = candidate
                                break
                    except Exception:
                        pass
                
                if not el:
                    logs.append({'nivel': 'info', 'msg': f'{label}: ⚠️ Campo não encontrado: {label_campo}'})
                    continue
                
                # Preencher
                try:
                    tag = el.evaluate('el => el.tagName.toLowerCase()')
                    
                    if tag == 'select':
                        # Selecionar por texto da opção
                        options = el.query_selector_all('option')
                        selected = False
                        for opt in options:
                            opt_text = (opt.text_content() or '').strip()
                            if valor.lower() in opt_text.lower():
                                opt_val = opt.get_attribute('value')
                                el.select_option(value=opt_val)
                                selected = True
                                logs.append({'nivel': 'info', 'msg': f'{label}: ✏️ {label_campo} = "{opt_text}"'})
                                preenchidos.append(label_campo)
                                break
                        if not selected:
                            logs.append({'nivel': 'info', 'msg': f'{label}: ⚠️ Opção "{valor}" não encontrada em {label_campo}'})
                        
                        # Disparar change event (ASP.NET postback)
                        el.dispatch_event('change')
                        time.sleep(1)
                    
                    else:  # input, textarea
                        # Verificar se é campo de data HTML5
                        input_type = el.get_attribute('type') or 'text'
                        
                        if input_type == 'date':
                            # Converter dd/mm/yyyy para yyyy-mm-dd
                            import re as _re
                            m = _re.match(r'(\d{2})/(\d{2})/(\d{4})', valor)
                            if m:
                                iso_val = f'{m.group(3)}-{m.group(2)}-{m.group(1)}'
                                el.fill(iso_val)
                                logs.append({'nivel': 'info', 'msg': f'{label}: 📅 {label_campo} = "{valor}" (ISO: {iso_val})'})
                            else:
                                el.fill(valor)
                                logs.append({'nivel': 'info', 'msg': f'{label}: ✏️ {label_campo} = "{valor}"'})
                        else:
                            el.fill(valor)
                            logs.append({'nivel': 'info', 'msg': f'{label}: ✏️ {label_campo} = "{valor}"'})
                        
                        preenchidos.append(label_campo)
                
                except Exception as e_fill:
                    logs.append({'nivel': 'info', 'msg': f'{label}: ⚠️ Erro preenchendo {label_campo}: {str(e_fill)[:40]}'})
            
            # Clicar no botão de submit
            btn = None
            try:
                # Buscar por texto do botão
                for b in frame.query_selector_all('button, input[type="submit"], input[type="button"]'):
                    b_text = (b.text_content() or b.get_attribute('value') or '').strip()
                    if botao.lower() in b_text.lower():
                        btn = b
                        break
                # Buscar por id/name
                if not btn:
                    btn = frame.query_selector(f'#{botao}') or frame.query_selector(f'[name="{botao}"]') or frame.query_selector(f'[value*="{botao}" i]')
            except Exception:
                pass
            
            if btn:
                try:
                    url_antes = page.url
                except Exception:
                    url_antes = ''
                
                btn.click()
                time.sleep(3)
                
                try:
                    page.wait_for_load_state('networkidle', timeout=15000)
                except Exception:
                    pass
                
                logs.append({'nivel': 'info', 'msg': f'{label}: 🖱️ Clicou: {botao}'})
            else:
                logs.append({'nivel': 'info', 'msg': f'{label}: ⚠️ Botão "{botao}" não encontrado'})
            
            return f'Formulário preenchido ({len(preenchidos)} campos) e submetido'

        elif tipo == 'scroll':
            amount = 500 if direcao == 'baixo' else -500
            page.mouse.wheel(0, amount)
            time.sleep(1)

            logs.append({'nivel': 'info', 'msg': f'{label}: 📜 Scroll {direcao}'})
            return f'Scroll {direcao}'

        elif tipo == 'concluido':
            return 'Legislacao encontrada'

        elif tipo == 'desistir':
            return 'Desistiu'

        else:
            return f'Acao desconhecida: {tipo}'

    except Exception as e:
        err = str(e)[:80]
        logs.append({'nivel': 'info', 'msg': f'{label}: ⚠️ Erro: {err}'})
        return f'Erro: {err}'


def navegar_como_humano(
    page,
    frame,
    legislacao: dict,
    chamar_llm,  # Mantido por compatibilidade, mas usamos Gemini visao diretamente
    logs: list,
    label: str = '',
    max_passos: int = 20
) -> dict:
    """
    Navega uma pagina web como um humano: olha o screenshot, decide, age.

    Args:
        page: Playwright page
        frame: Playwright frame (nao usado — screenshot captura tudo)
        legislacao: dict com tipo, numero, ano, municipio, etc.
        chamar_llm: funcao LLM (mantida por compatibilidade)
        logs: lista de logs
        label: prefixo para logs
        max_passos: maximo de passos

    Retorna:
        {'encontrada': bool, 'url': str, 'status': str, 'confirmacao': str, ...}
    """

    resultado = {
        'encontrada': False,
        'url': '',
        'status': '',
        'confirmacao': '',
        'pdf_path': None,
        'pagina_pdf': None
    }

    historico = []
    pagina_ativa = page

    for passo in range(1, max_passos + 1):
        try:
            # 1. Screenshot
            screenshot_b64 = _screenshot_base64(pagina_ativa)
            url_atual = pagina_ativa.url

            # 2. Prompt
            prompt = _montar_prompt(legislacao, historico, passo, url_atual)

            # 3. Gemini com visao
            resp = _chamar_gemini_visao(prompt, screenshot_b64, logs, f'{label} passo {passo}')

            if not resp:
                logs.append({'nivel': 'aviso', 'msg': f'{label}: Passo {passo}: sem resposta da IA'})
                historico.append({'passo': passo, 'acao': 'sem resposta', 'resultado': 'IA nao respondeu'})
                continue

            # 4. Parsear JSON
            try:
                resp_clean = resp.strip()
                if resp_clean.startswith('```'):
                    resp_clean = resp_clean.split('\n', 1)[-1]
                if resp_clean.endswith('```'):
                    resp_clean = resp_clean.rsplit('```', 1)[0]
                resp_clean = resp_clean.strip()

                decisao = json.loads(resp_clean)
            except json.JSONDecodeError:
                import re
                json_match = re.search(r'\{[\s\S]*\}', resp)
                if json_match:
                    try:
                        decisao = json.loads(json_match.group())
                    except json.JSONDecodeError:
                        logs.append({'nivel': 'aviso', 'msg': f'{label}: Passo {passo}: JSON invalido'})
                        historico.append({'passo': passo, 'acao': 'erro parse', 'resultado': 'JSON invalido'})
                        continue
                else:
                    logs.append({'nivel': 'aviso', 'msg': f'{label}: Passo {passo}: sem JSON'})
                    historico.append({'passo': passo, 'acao': 'erro parse', 'resultado': 'sem JSON'})
                    continue

            # 5. Extrair campos
            o_que_vejo = (decisao.get('o_que_vejo', '') or '')[:100]
            pensamento = decisao.get('decisao', '') or ''
            acao = decisao.get('acao', {}) or {}
            tipo_acao = acao.get('tipo', '') or ''

            # Log
            logs.append({'nivel': 'info', 'msg': f'{label}: 👁️ Passo {passo}: {o_que_vejo}'})
            logs.append({'nivel': 'info', 'msg': f'{label}: 🧠 Decisão: {tipo_acao} — {pensamento[:80]}'})

            # 6. Legislacao encontrada?
            leg = decisao.get('legislacao_encontrada', {}) or {}
            leg_url = (leg.get('url', '') or '').strip()

            if leg.get('encontrada') and leg_url and leg_url != '#' and leg_url.startswith('http'):
                resultado['encontrada'] = True
                resultado['url'] = leg_url
                resultado['status'] = (leg.get('status', '') or '')
                resultado['confirmacao'] = (leg.get('confirmacao', '') or '')

                logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ Legislação encontrada! {leg_url[:80]}'})
                logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ Confirmação: {resultado["confirmacao"][:100]}'})
                if resultado['status']:
                    logs.append({'nivel': 'info', 'msg': f'{label}: 📌 Status: {resultado["status"]}'})

                historico.append({'passo': passo, 'acao': 'concluido', 'resultado': f'Encontrada: {leg_url[:60]}'})
                break

            # 7. Desistiu?
            if tipo_acao == 'desistir':
                logs.append({'nivel': 'aviso', 'msg': f'{label}: ❌ Passo {passo}: IA desistiu — {pensamento[:80]}'})
                historico.append({'passo': passo, 'acao': 'desistir', 'resultado': pensamento[:100]})
                break

            # 8. Executar
            exec_resultado = _executar_acao(pagina_ativa, acao, logs, label) or 'sem resultado'

            coord_info = ''
            if tipo_acao in ('clicar', 'digitar', 'selecionar'):
                coord_info = f' ({acao.get("x",0)},{acao.get("y",0)})'

            historico.append({
                'passo': passo,
                'acao': f'{tipo_acao}{coord_info}',
                'resultado': exec_resultado[:100]
            })

            # 9. Nova aba? Mudar contexto
            if exec_resultado.startswith('Nova aba:'):
                all_pages = page.context.pages
                if len(all_pages) > 1:
                    pagina_ativa = all_pages[-1]
                    logs.append({'nivel': 'info', 'msg': f'{label}: 📄 Mudou para nova aba: {pagina_ativa.url[:50]}'})

        except Exception as e:
            err_msg = str(e)[:80]
            if 'closed' in err_msg.lower() or 'disposed' in err_msg.lower():
                # Página original fechou — mas pode ter aberto nova aba
                try:
                    all_pages = page.context.pages
                    if len(all_pages) > 1:
                        pagina_ativa = all_pages[-1]
                        try:
                            pagina_ativa.wait_for_load_state('networkidle', timeout=10000)
                        except Exception:
                            pass
                        logs.append({'nivel': 'ok', 'msg': f'{label}: 🪟 Página fechou mas nova aba existe: {pagina_ativa.url[:50]}'})
                        historico.append({'passo': passo, 'acao': 'nova aba', 'resultado': f'Mudou para: {pagina_ativa.url[:60]}'})
                        continue  # Continuar navegação na nova aba
                except Exception:
                    pass
                logs.append({'nivel': 'aviso', 'msg': f'{label}: Pagina fechou — encerrando'})
                break

            logs.append({'nivel': 'aviso', 'msg': f'{label}: Passo {passo} erro: {err_msg}'})
            historico.append({'passo': passo, 'acao': 'erro', 'resultado': err_msg})

    if not resultado['encontrada']:
        logs.append({'nivel': 'aviso', 'msg': f'{label}: Navegação encerrada após {len(historico)} passos sem encontrar'})

    return resultado
