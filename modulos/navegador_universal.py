"""
navegador_universal.py — Navegação visual + clique por texto
==============================================================

A IA recebe screenshot, decide o que fazer, e indica elementos pelo TEXTO.
O Playwright encontra o elemento pelo texto e clica — sem coordenadas.
"""

import os
import time
import json
import tempfile
import base64


VIEWPORT_W = 1280
VIEWPORT_H = 900


def _get_proxy_requests():
    """Retorna dict de proxies para requests Python, lendo as mesmas env vars do Playwright.
    Retorna None se proxy não configurado."""
    import re as _re_px
    proxy_url = os.getenv('PROXY_URL', '').strip()
    if proxy_url:
        # Formato Evomi: socks5://host:port:user:pass → converter para user:pass@host:port
        _m = _re_px.match(r'^(socks5|socks4|http|https)://([^:@]+):(\d+):([^:]+):(.+)$', proxy_url)
        if _m:
            _scheme, _host, _port, _user, _pw = _m.groups()
            proxy_url = f'{_scheme}://{_user}:{_pw}@{_host}:{_port}'
        return {'http': proxy_url, 'https': proxy_url}
    server = os.getenv('PROXY_SERVER', '').strip()
    if server:
        user = os.getenv('PROXY_USER', '').strip()
        pw   = os.getenv('PROXY_PASS', '').strip()
        if user and pw:
            proxy_url = f'http://{user}:{pw}@{server}'
        else:
            proxy_url = f'http://{server}'
        return {'http': proxy_url, 'https': proxy_url}
    return None


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
    """Prompt: IA decide o que fazer e indica elementos pelo TEXTO visivel."""

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

    return f"""Voce esta navegando uma pagina web para encontrar a legislacao: {tipo} no {numero}/{ano} — {municipio}.
Data de publicacao: {data_pub}. Assunto: {assunto}

URL ATUAL: {url_atual} | PASSO: {passo}
{historico_txt}

Olhe o screenshot. Decida a proxima acao.

ACOES:
- "clicar": informe o TEXTO EXATO do elemento a clicar (botao, link, icone). Copie o texto como aparece na tela.
- "digitar": informe o LABEL do campo + texto a digitar.
- "preencher_formulario": liste campos e valores para preencher de uma vez.
- "scroll": "baixo" ou "cima"
- "concluido": encontrei (informar URL)
- "desistir": nao consigo encontrar

REGRAS:
1. NUNCA use busca por palavra-chave. Prefira busca por data/edicao/numero.
2. DIARIOS OFICIAIS: busque por data de UMA edicao. Comece pela data de assinatura. IMPORTANTE: apos digitar a data, voce DEVE clicar o botao "OK" para carregar aquela edicao. So depois que a edicao carregar (verifique se o numero da edicao mudou), clique em "PDF". Apos baixar o PDF, o sistema verifica automaticamente se a legislacao esta naquela edicao. Se NAO estiver, volte a pagina inicial e tente o dia seguinte. Repita ate 5 dias.
3. Esfera: sempre "Municipal".
4. Numero da legislacao: apenas o numeral (ex: "198").
5. Icone na coluna "Arquivo" = link para o documento.
6. So marque "encontrada" com URL real (http...).
7. Cloudflare/CAPTCHA: desista.
8. Formularios com dropdowns simples (tag <select> padrao): use "preencher_formulario". Datepickers: use "clicar"/"digitar". Dropdowns CUSTOMIZADOS (botoes que abrem listas, checkboxes flutuantes, componentes JS): use "clicar" no botao do dropdown e depois "clicar" na opcao desejada — NUNCA use preencher_formulario nesses casos.
9. Se a pagina ja mostra o conteudo (edicao carregada, preview visivel), clique em DOWNLOAD (PDF), nao em buscar de novo.
10. Sempre prefira baixar PDF.
11. SITES DE BUSCA DE LEGISLACAO (NAO diarios oficiais): preencha APENAS Esfera, Tipo de Ato e Numero. NAO preencha campos de data — deixe-os vazios. A busca retornara resultados e voce identifica a legislacao correta pela descricao (tipo, numero, ano, ementa). Se houver varias paginas de resultados, navegue ate encontrar. EXCECAO: se o formulario EXIGIR data (campo obrigatorio, erro ao submeter sem data), use a data de ASSINATURA da legislacao (a data informada no prompt) como Data Inicial, e +5 dias como Data Final. A data de assinatura e diferente da data de publicacao no diario oficial. MUITO IMPORTANTE: a data exibida ao lado do resultado num site de busca e a data de ASSINATURA da lei, nao de publicacao. A data no prompt pode estar errada. NUNCA rejeite um resultado correto (tipo + numero + ano batem) por causa de diferenca de data — clique no icone Arquivo imediatamente.
13. LEISMUNICIPAIS.COM.BR — siga EXATAMENTE esta sequencia, sem pular passos:
    (a) Clicar em "Mais opcoes" para expandir o formulario avancado.
    (b) Digitar o nome do municipio no campo de cidade e aguardar autocomplete — selecionar da lista (regra 12).
    (c) Clicar no dropdown "Todos os Atos" e selecionar o tipo correto (ex: "Leis Complementares").
    (d) Digitar apenas o NUMERO da lei no campo de numero/palavra-chave.
    (e) Clicar em "Pesquisar".
    (f) Nos resultados, clicar SOMENTE no TITULO da legislacao (texto em vermelho). NUNCA clique em links encurtados como "http://leis.org/..." ou "bit.ly/..." — ignorar completamente.
    (g) Apos abrir a pagina da legislacao e ela carregar completamente (div com o texto da lei visivel), procure o botao de download do PDF original (pode aparecer como "PDF", "Baixar PDF", "Download PDF", icone de PDF, ou similar) e clique nele. So marque "concluido" DEPOIS de clicar no botao de download.
    (g) Apos abrir a pagina da legislacao e ela carregar completamente (div com o texto da lei visivel), procure o botao de download do PDF original (pode aparecer como "PDF", "Baixar PDF", "Download PDF", icone de PDF, ou similar) e clique nele. So marque "concluido" DEPOIS de clicar no botao de download.
12. CAMPOS COM AUTOCOMPLETE (campo de cidade/municipio): NUNCA use preencher_formulario nesses campos. Use a sequencia: (a) "digitar" no campo; (b) aguarde a lista de sugestoes aparecer (ela aparece abaixo do campo); (c) "clicar" na primeira sugestao da lista. Se nao aparecer lista imediatamente e a pagina mostrar "Pesquisando..." isso significa que a lista AINDA ESTA CARREGANDO — tire outro screenshot para ver se ela ja apareceu. Tente tambem digitar so as primeiras 3 letras para forcar o autocomplete. O sistema REJEITA o municipio se voce nao SELECIONAR da lista — digitar sem clicar na sugestao nunca funciona. Somente desista se apos multiplas tentativas a lista realmente nao aparecer.
12. TEXTO DO ELEMENTO: copie o texto EXATO como aparece na tela. Exemplos:
    - Botao "OK" -> texto_elemento: "OK"
    - Link "PDF" -> texto_elemento: "PDF"  
    - Link "HTML" -> texto_elemento: "HTML"
    - Link "Consultar" -> texto_elemento: "Consultar"
    - Link "Download da Edição nº 200" -> texto_elemento: "Download da Edição nº 200"
    - Se o elemento nao tem texto (icone puro), descreva: "icone coluna Arquivo linha 1"

14. PAGINA CARREGANDO (leismunicipais.com.br): Se a pagina mostrar mensagem "Por favor, aguarde", spinner de carregamento, ou "A norma requisitada esta sendo carregada" — NAO faca scroll, NAO clique em nada. Use acao "screenshot" para aguardar o carregamento completar. Somente desista se aparecer CAPTCHA.
CRITICO: Responda SOMENTE com o objeto JSON abaixo. Nenhum texto antes, nenhum texto depois, nenhum markdown, nenhuma explicacao. Se a pagina estiver carregando ou em transicao, ainda assim responda com JSON — use decisao "screenshot" para aguardar.
JSON:
{{{{
    "o_que_vejo": "...",
    "decisao": "o que vou fazer e por que",
    "acao": {{{{
        "tipo": "clicar|digitar|preencher_formulario|scroll|concluido|desistir",
        "texto_elemento": "texto exato do botao/link (so para clicar)",
        "label_campo": "label do campo (so para digitar)",
        "texto": "texto a digitar (so para digitar)",
        "direcao": "baixo|cima (so para scroll)",
        "campos": [
            {{{{"label": "Esfera", "valor": "Municipal", "tipo_campo": "select"}}}},
            {{{{"label": "Tipo de Ato", "valor": "Lei Complementar", "tipo_campo": "select"}}}},
            {{{{"label": "Nº do Ato", "valor": "198", "tipo_campo": "input"}}}}
        ],
        "botao_submit": "Consultar"
    }}}},
    "legislacao_encontrada": {{{{
        "encontrada": false,
        "url": "",
        "status": "",
        "confirmacao": ""
    }}}}
}}}}

IMPORTANTE para preencher_formulario:
- Preencha o array "campos" com os campos relevantes do formulario.
- Use tipo_campo "select" para dropdowns, "input" para texto, "date" para datas.
- Datas SEMPRE no formato DD/MM/AAAA (ex: "14/01/2019").
- Em sites de busca de legislacao (NAO diarios oficiais), NAO preencha campos de data.
- O botao_submit deve ser o texto exato do botao de envio."""


def _clicar_por_texto(page, texto_elemento: str, logs: list, label: str) -> str:
    """Tenta clicar num elemento pelo texto visivel. Multiplas estrategias."""
    
    texto = texto_elemento.strip()
    if not texto:
        return 'Erro: texto_elemento vazio'
    
    try:
        url_antes = page.url
    except Exception:
        url_antes = ''
    n_pages_antes = len(page.context.pages)
    
    # Capturar downloads
    _download_obj = None
    def _on_download(d):
        nonlocal _download_obj
        _download_obj = d
    try:
        page.on('download', _on_download)
    except Exception:
        pass
    
    # Tentar multiplas estrategias
    estrategias = [
        ('exact', lambda p: p.get_by_text(texto, exact=True).first),
        ('partial', lambda p: p.get_by_text(texto).first),
        ('button', lambda p: p.get_by_role('button', name=texto).first),
        ('link', lambda p: p.get_by_role('link', name=texto).first),
        ('locator', lambda p: p.locator(f'text="{texto}"').first),
        ('value', lambda p: p.locator(f'[value="{texto}"]').first),
        ('title', lambda p: p.locator(f'[title*="{texto}" i]').first),
        ('alt', lambda p: p.locator(f'[alt*="{texto}" i]').first),
        ('aria', lambda p: p.locator(f'[aria-label*="{texto}" i]').first),
    ]
    
    el = None
    estrategia_usada = ''
    
    # Tentar na pagina principal
    for nome, finder in estrategias:
        try:
            candidate = finder(page)
            if candidate and candidate.is_visible():
                el = candidate
                estrategia_usada = nome
                break
        except Exception:
            continue
    
    # Tentar em frames internos
    if not el:
        for frame in page.frames:
            if frame == page.main_frame:
                continue
            for nome, finder in estrategias[:3]:  # exact, partial, button
                try:
                    candidate = finder(frame)
                    if candidate and candidate.is_visible():
                        el = candidate
                        estrategia_usada = f'frame:{nome}'
                        break
                except Exception:
                    continue
            if el:
                break
    
    # Fallback: ícones sem texto em tabelas (ex: coluna "Arquivo" do BuscaFácil)
    if not el:
        texto_lower = texto.lower()
        is_icon_request = any(kw in texto_lower for kw in ['icone', 'icon', 'arquivo', 'download', 'imagem'])
        if is_icon_request:
            try:
                # Buscar links/imagens clicáveis na última coluna de tabelas
                for target in [page] + [f for f in page.frames if f != page.main_frame]:
                    try:
                        # Estratégia 1: links com imagem dentro de tabelas
                        links_img = target.locator('table td a:has(img)').all()
                        if links_img:
                            for link in links_img:
                                try:
                                    if link.is_visible():
                                        el = link
                                        estrategia_usada = 'table-img-link'
                                        break
                                except Exception:
                                    continue
                        
                        # Estratégia 2: qualquer link na última coluna de cada row
                        if not el:
                            rows = target.locator('table tr').all()
                            for row in rows[1:]:  # pular header
                                try:
                                    cells = row.locator('td').all()
                                    if cells:
                                        # Verificar últimas 3 células por links
                                        for cell in reversed(cells[-3:] if len(cells) >= 3 else cells):
                                            link = cell.locator('a').first
                                            try:
                                                if link and link.is_visible():
                                                    el = link
                                                    estrategia_usada = 'table-last-col-link'
                                                    break
                                            except Exception:
                                                continue
                                    if el:
                                        break
                                except Exception:
                                    continue
                    except Exception:
                        continue
                    if el:
                        break
            except Exception:
                pass
    
    if not el:
        logs.append({'nivel': 'info', 'msg': f'{label}: ⚠️ "{texto[:30]}" não encontrado'})
        try:
            page.remove_listener('download', _on_download)
        except Exception:
            pass
        return f'Texto não encontrado: "{texto[:30]}"'
    
    # Extrair href do elemento ANTES de clicar (opção 1: evita browser morrer)
    href_extraido = None
    try:
        href_extraido = el.evaluate('''el => {
            // Se é um link, retornar href
            if (el.tagName === 'A') return el.href;
            // Se é imagem dentro de link
            let parent = el.closest('a');
            if (parent) return parent.href;
            // Se tem onclick com window.open ou location
            let onclick = el.getAttribute('onclick') || '';
            let m = onclick.match(/window\\.open\\(['"]([^'"]+)['"]/);
            if (m) return m[1];
            m = onclick.match(/location\\.href\\s*=\\s*['"]([^'"]+)['"]/);
            if (m) return m[1];
            return null;
        }''')
        if href_extraido:
            # Resolver URLs relativas
            if href_extraido.startswith('/'):
                try:
                    from urllib.parse import urljoin
                    href_extraido = urljoin(page.url, href_extraido)
                except Exception:
                    pass
            logs.append({'nivel': 'info', 'msg': f'{label}: 🔗 Href extraído: {(href_extraido or "")[:120]}'})
    except Exception:
        pass
    
    # Detectar onclick (funções como MostraDocumento que abrem popups)
    _onclick_fn = None
    try:
        _onclick_fn = el.evaluate('''el => {
            let a = el.closest('a') || el;
            return a.getAttribute('onclick') || null;
        }''')
        if _onclick_fn:
            logs.append({'nivel': 'info', 'msg': f'{label}: 🔧 onclick detectado: {_onclick_fn[:60]}'})
    except Exception:
        pass
    
    # Screenshot de debug com highlight
    try:
        el.evaluate('el => { el.style.outline = "3px solid red"; }')
        time.sleep(0.2)
        debug_dir = '/tmp/nav_screenshots'
        os.makedirs(debug_dir, exist_ok=True)
        import re as _re
        label_clean = _re.sub(r'[^\w\-]', '_', label)
        texto_clean = _re.sub(r'[^\w]', '_', texto[:15])
        ss_path = f'{debug_dir}/click_{label_clean}_{texto_clean}.png'
        ss_bytes = page.screenshot(type='png', full_page=False)
        with open(ss_path, 'wb') as f_ss:
            f_ss.write(ss_bytes)
        screenshot_url = f'/debug/screenshots/img/{os.path.basename(ss_path)}'
        logs.append({'nivel': 'info', 'msg': f'{label}: 📸 <a href="{screenshot_url}" target="_blank" style="color:#4fc3f7">Elemento encontrado: "{texto[:20]}"</a>'})
        el.evaluate('el => { el.style.outline = ""; }')
    except Exception:
        pass
    
    # Interceptar navegação real e respostas PDF
    _nav_url_real = [None]
    _pdf_response_url = [None]
    
    try:
        def _on_response(response):
            try:
                ct = response.headers.get('content-type', '')
                url = response.url
                if 'pdf' in ct.lower() or url.lower().endswith('.pdf'):
                    _pdf_response_url[0] = url
                elif response.status == 200 and url != url_antes:
                    if 'text/html' in ct.lower() or '.asp' in url.lower():
                        _nav_url_real[0] = url
            except Exception:
                pass
        
        page.on('response', _on_response)
    except Exception:
        pass
    
    # Helper para limpar listeners
    def _cleanup_listeners():
        try:
            page.remove_listener('download', _on_download)
        except Exception:
            pass
        try:
            page.remove_listener('response', _on_response)
        except Exception:
            pass
    
    # Clicar
    try:
        el.scroll_into_view_if_needed()
        time.sleep(0.3)
        
        # Se tem onclick (ex: MostraDocumento), capturar documento com cascade de fallbacks
        if _onclick_fn:
            logs.append({'nivel': 'info', 'msg': f'{label}: 🔧 onclick detectado — iniciando captura em cascata'})

            # ═══════════════════════════════════════════════════════════════════
            # ESTRATÉGIA A½ — Extrair URL real do JS antes de qualquer clique
            # Executa a função onclick em modo "spy": intercepta window.open e
            # location.href para capturar a URL sem abrir popup de verdade.
            # Se funcionar, faz requests direto com proxy — contexto nem entra.
            # ═══════════════════════════════════════════════════════════════════
            _direct_url = None
            try:
                _direct_url = page.evaluate('''(onclickStr) => {
                    let captured = null;
                    // Salvar originals
                    const _origOpen = window.open;
                    const _origAssign = window.location && window.location.assign
                        ? window.location.assign.bind(window.location) : null;
                    // Monkey-patch window.open
                    window.open = function(url) { captured = url; return null; };
                    // Tentar extrair URL diretamente do string do onclick
                    // Padrão: MostraDocumento(id1, id2) — montar URL do ContadorAcessoAto
                    let m = onclickStr.match(/MostraDocumento\\s*\\(\\s*(\\d+)\\s*,\\s*(\\d+)\\s*\\)/);
                    if (m) {
                        captured = '/smu/buscafacil/ContadorAcessoAto.asp?codato=' + m[2]
                                 + '&origem=RelacaoDocumentos&codigo=' + m[1];
                    }
                    // Tentar executar e capturar window.open
                    if (!captured) {
                        try { eval(onclickStr); } catch(e) {}
                    }
                    // Restaurar
                    window.open = _origOpen;
                    return captured;
                }''', _onclick_fn)
                if _direct_url:
                    # Resolver URL relativa
                    if _direct_url.startswith('/'):
                        from urllib.parse import urljoin
                        _direct_url = urljoin(page.url, _direct_url)
                    logs.append({'nivel': 'info', 'msg': f'{label}: 🎯 URL direta extraída: {_direct_url[:100]}'})

                    # Tentar buscar direto com requests — sem proxy primeiro (ContadorAcessoAto pode não ter WAF)
                    try:
                        import requests as _req_a
                        _proxies_a = _get_proxy_requests()
                        _ua_a = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                                 'AppleWebKit/537.36 (KHTML, like Gecko) '
                                 'Chrome/120.0.0.0 Safari/537.36')
                        _cookies_a = {}
                        try:
                            for c in page.context.cookies():
                                _cookies_a[c['name']] = c['value']
                        except Exception:
                            pass
                        _r_a = None
                        # Tentar sem proxy primeiro (evita que o proxy bloqueie ContadorAcessoAto)
                        for _use_proxy in [False, True]:
                            _proxy_label = 'com proxy' if _use_proxy else 'sem proxy'
                            try:
                                _r_a = _req_a.get(
                                    _direct_url,
                                    cookies=_cookies_a,
                                    proxies=(_proxies_a if _use_proxy else None),
                                    timeout=20,
                                    allow_redirects=True,
                                    headers={'User-Agent': _ua_a, 'Referer': page.url}
                                )
                                _ct_try = _r_a.headers.get('content-type', '').lower()
                                _sz_try = len(_r_a.content)
                                _url_try = _r_a.url[:70]
                                logs.append({'nivel': 'info', 'msg': f'{label}: 📡 A½ requests ({_proxy_label}): {_r_a.status_code} {_ct_try[:25]} {_sz_try}b → {_url_try}'})
                                if ('pdf' in _ct_try or 'octet' in _ct_try or
                                        _r_a.url.lower().endswith('.pdf') or
                                        _sz_try > 5000):
                                    break  # Resultado útil
                                # 200 sem proxy = resposta real — mas verificar se não é WAF (247b "Request Rejected")
                                _is_waf_a = (_sz_try < 400 and b'Request Rejected' in (_r_a.content or b''))
                                if _is_waf_a:
                                    logs.append({'nivel': 'info', 'msg': f'{label}: 📡 A½ sem proxy retornou WAF ({_sz_try}b) — tentando com proxy'})
                                    # Não break: continua loop para tentar com proxy
                                elif not _use_proxy and _r_a.status_code == 200 and _sz_try > 50:
                                    logs.append({'nivel': 'info', 'msg': f'{label}: 📡 A½ usando resposta sem proxy ({_sz_try}b) para parse JS'})
                                    break
                            except Exception as _e_try:
                                logs.append({'nivel': 'info', 'msg': f'{label}: 📡 A½ requests ({_proxy_label}) falhou: {str(_e_try)[:60]}'})
                                _r_a = None
                                continue
                        if _r_a is None:
                            raise Exception('Todas tentativas requests falharam')
                        _ct_a = _r_a.headers.get('content-type', '').lower()
                        _url_a = _r_a.url
                        if 'pdf' in _ct_a or _url_a.lower().endswith('.pdf') or 'octet' in _ct_a:
                            _dl_path_a = tempfile.mktemp(suffix='.pdf')
                            with open(_dl_path_a, 'wb') as _f_a:
                                _f_a.write(_r_a.content)
                            _sz_a = len(_r_a.content) // 1024
                            logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ A½ PDF direto: {_sz_a}KB ({_url_a[:60]})'})
                            _cleanup_listeners()
                            return f'Download: {_dl_path_a}'
                        elif len(_r_a.content) > 50:
                            # ContadorAcessoAto retorna HTML com redirect JS — extrair URL real
                            # Aceita mesmo 247b (resposta real sem proxy) além de 1208b
                            _html_contador = _r_a.text
                            logs.append({'nivel': 'info', 'msg': f'{label}: 📄 A½ HTML ({len(_r_a.content)}b status={_r_a.status_code}): {repr(_html_contador[:800])}'})
                            
                            # Extrair URL do redirect (window.open, window.location, meta-refresh, href, etc.)
                            import re as _re_cnt
                            _redirect_url = None

                            # window.open('url') — ContadorAcessoAto frequentemente usa isso
                            _m = _re_cnt.search(r"window\.open\s*\(\s*['\"]([^'\"]+)['\"]", _html_contador)
                            if _m:
                                _redirect_url = _m.group(1)
                                logs.append({'nivel': 'info', 'msg': f'{label}: 🔍 A½ JS: window.open → {_redirect_url[:80]}'})

                            # window.location.href = '...' ou window.location = '...'
                            if not _redirect_url:
                                _m = _re_cnt.search(r"window\.location(?:\.href)?\s*=\s*['\"]([^'\"]+)['\"]", _html_contador)
                                if _m:
                                    _redirect_url = _m.group(1)
                                    logs.append({'nivel': 'info', 'msg': f'{label}: 🔍 A½ JS: window.location → {_redirect_url[:80]}'})
                            
                            # meta http-equiv refresh
                            if not _redirect_url:
                                _m = _re_cnt.search(r'content=["\'][^"\']*url=([^"\'>\s]+)', _html_contador, _re_cnt.IGNORECASE)
                                if _m:
                                    _redirect_url = _m.group(1)
                            
                            # location.replace('...') ou location.assign('...')
                            if not _redirect_url:
                                _m = _re_cnt.search(r"location\.(?:replace|assign)\s*\(\s*['\"]([^'\"]+)['\"]", _html_contador)
                                if _m:
                                    _redirect_url = _m.group(1)
                            
                            # href qualquer link com .pdf ou download
                            if not _redirect_url:
                                _m = _re_cnt.search(r'href=["\']([^"\']*(?:\.pdf|download|arquivo)[^"\']*)["\']', _html_contador, _re_cnt.IGNORECASE)
                                if _m:
                                    _redirect_url = _m.group(1)
                            
                            if _redirect_url:
                                # Resolver URL relativa
                                if not _redirect_url.startswith('http'):
                                    from urllib.parse import urljoin as _urljoin_cnt
                                    _redirect_url = _urljoin_cnt(_direct_url, _redirect_url)
                                
                                logs.append({'nivel': 'info', 'msg': f'{label}: 🎯 A½ redirect extraído: {_redirect_url[:100]}'})
                                try:
                                    _r_final = _req_a.get(
                                        _redirect_url,
                                        cookies=_cookies_a,
                                        proxies=_proxies_a,
                                        timeout=25,
                                        allow_redirects=True,
                                        headers={'User-Agent': _ua_a, 'Referer': _direct_url}
                                    )
                                    _ct_final = _r_final.headers.get('content-type', '').lower()
                                    _url_final = _r_final.url
                                    if 'pdf' in _ct_final or _url_final.lower().endswith('.pdf') or 'octet' in _ct_final:
                                        _dl_path_final = tempfile.mktemp(suffix='.pdf')
                                        with open(_dl_path_final, 'wb') as _f_final:
                                            _f_final.write(_r_final.content)
                                        _sz_final = len(_r_final.content) // 1024
                                        logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ A½ PDF via redirect: {_sz_final}KB ({_url_final[:60]})'})
                                        _cleanup_listeners()
                                        return f'Download: {_dl_path_final}'
                                    else:
                                        logs.append({'nivel': 'info', 'msg': f'{label}: 📄 A½ redirect retornou HTML: {len(_r_final.content)}b'})
                                except Exception as _e_final:
                                    logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ A½ redirect fetch falhou: {str(_e_final)[:60]}'})
                            else:
                                logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ A½ sem redirect detectável — seguindo para popup'})
                            
                            logs.append({'nivel': 'info', 'msg': f'{label}: 📄 A½ seguindo para popup'})
                    except Exception as _e_a:
                        logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ A½ requests falhou: {str(_e_a)[:60]}'})
            except Exception as _e_half:
                logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ A½ JS extract falhou: {str(_e_half)[:60]}'})

            # ═══════════════════════════════════════════════════════════════════
            # ESTADO COMPARTILHADO entre todas as estratégias
            # ═══════════════════════════════════════════════════════════════════
            _ctx_responses   = []   # (url, content_type, body_bytes) capturados
            _popup_nav_urls  = []   # URLs navegadas dentro do popup
            _popup_dl        = [None]
            _popup_page_ref  = [None]

            # ── PRÉ-CLIQUE: interceptar via context.route() ───────────────────
            # context.route() é mais confiável que context.on('response') com proxy:
            # captura o body ANTES da resposta ser consumida/descartada pelo proxy,
            # funciona mesmo que o contexto morra imediatamente após.
            def _route_interceptor(route):
                req_url = route.request.url
                # ContadorAcessoAto: route.fetch() segue redirect e captura PDF
                if 'ContadorAcessoAto' in req_url or 'ContadorAcesso' in req_url:
                    # Estratégia dupla:
                    # 1) route.continue_() imediato → browser dispara download event
                    # 2) route.fetch() em background thread → captura body antes do proxy consumir
                    # O resultado do thread fica em _ctx_responses para o A0 usar se save_as cancelar.
                    logs.append({'nivel': 'info', 'msg': f'{label}: 🔀 route: ContadorAcessoAto — continue_'})
                    try: route.continue_()
                    except Exception: pass
                    return
                try:
                    resp = route.fetch()
                    ct   = resp.headers.get('content-type', '').lower()
                    url  = resp.url
                    is_doc = (
                        'pdf'   in ct or
                        'octet' in ct or
                        url.lower().endswith('.pdf')
                    )
                    is_html = 'text/html' in ct and url != page.url and 'about:' not in url
                    if is_doc or is_html:
                        try:
                            body = resp.body()
                            if len(body) > 500:
                                _ctx_responses.append((url, ct, body))
                                logs.append({'nivel': 'info', 'msg': f'{label}: 🔀 route capturou {"PDF" if is_doc else "HTML"} {len(body)//1024}KB: {url[:60]}'})
                        except Exception:
                            _ctx_responses.append((url, ct, None))
                    route.fulfill(response=resp)
                except Exception:
                    try:
                        route.continue_()
                    except Exception:
                        pass

            def _setup_popup_listeners(new_popup):
                """Chamado no instante em que a nova janela é criada."""
                _popup_page_ref[0] = new_popup
                def _on_nav(frame):
                    try:
                        if frame == new_popup.main_frame:
                            u = frame.url or ''
                            if u and u != 'about:blank' and u not in _popup_nav_urls:
                                _popup_nav_urls.append(u)
                    except Exception:
                        pass
                def _on_dl(d):
                    _popup_dl[0] = d
                try:
                    new_popup.on('framenavigated', _on_nav)
                    new_popup.on('download',       _on_dl)
                except Exception:
                    pass

            try:
                # Interceptar apenas URLs do mesmo domínio para não sobrecarregar
                from urllib.parse import urlparse as _urlparse_route
                _host = _urlparse_route(page.url).netloc
                page.context.route(f'**/{_host}/**', _route_interceptor)
            except Exception:
                try:
                    page.context.route('**/*', _route_interceptor)
                except Exception:
                    pass
            try:
                page.context.on('page', _setup_popup_listeners)
            except Exception:
                pass

            # ── Helper: salvar bytes como PDF/HTML e retornar caminho ─────────
            def _salvar_corpo(url, ct, body):
                if not body or len(body) < 500:
                    return None
                ext = '.pdf' if ('pdf' in ct or url.lower().endswith('.pdf')) else '.html'
                path = tempfile.mktemp(suffix=ext)
                with open(path, 'wb') as f_:
                    f_.write(body)
                size_kb = len(body) // 1024
                logs.append({'nivel': 'ok', 'msg': f'{label}: 📥 Documento salvo ({ext}, {size_kb}KB): {url[:60]}'})
                return path

            # ── Helper: remover interceptores ─────────────────────────────────
            def _remove_ctx_listeners():
                try:
                    page.context.unroute(f'**/{_host}/**', _route_interceptor)
                except Exception:
                    try:
                        page.context.unroute('**/*', _route_interceptor)
                    except Exception:
                        pass
                try:
                    page.context.remove_listener('page', _setup_popup_listeners)
                except Exception:
                    pass

            # ═══════════════════════════════════════════════════════════════════
            # ESTRATÉGIA A0 — monkeypatch window.open → mesma janela
            #
            # Causa raiz BuscaFácil: MostraDocumento() chama window.open() →
            # popup abre ContadorAcessoAto → proxy bloqueia o popup (ERR_EMPTY).
            # window.opener validation: servidor só serve o doc se aberto via popup.
            #
            # Solução: antes do clique, substituir window.open por função que
            # navega na MESMA página (window.location.href = url).
            # ContadorAcessoAto carrega na janela principal → sem popup →
            # sem window.opener check → route interceptor captura PDF/redirect.
            # Se redirecionar para PDF: route captura. Se gerar download: captura.
            # ═══════════════════════════════════════════════════════════════════
            if _onclick_fn and 'MostraDocumento' in _onclick_fn:
                logs.append({'nivel': 'info', 'msg': f'{label}: 🔄 A0 — window.open → mesma janela'})
                try:
                    # 1) Injetar monkeypatch ANTES do clique
                    page.evaluate('''() => {
                        window.__orig_open = window.open;
                        window.open = function(url, name, features) {
                            if (url && url !== '' && url !== 'about:blank') {
                                // Navegar na mesma janela em vez de abrir popup
                                window.location.href = url;
                                return window;
                            }
                            return window.__orig_open(url, name, features);
                        };
                    }''')

                    # 2) Ouvir dialogs (alert do arquivo grande) na mesma página
                    _a0_dialog = [None]
                    _a0_dialog_extended = [False]  # flag para extender prazo uma única vez
                    def _on_dialog_a0(dlg):
                        _a0_dialog[0] = dlg.message
                        try: dlg.accept()
                        except Exception: pass
                    page.on('dialog', _on_dialog_a0)
                    page.context.on('dialog', _on_dialog_a0)

                    # 3) Clicar no ícone — JS chama window.open → monkeypatch
                    #    desvia para window.location.href → page navega
                    _a0_dl = [None]
                    _a0_redirect_url = [None]  # URL final do PDF após redirect do ContadorAcessoAto
                    def _on_dl_a0(d): _a0_dl[0] = d
                    page.on('download', _on_dl_a0)
                    page.context.on('download', _on_dl_a0)

                    # Capturar body/redirect do ContadorAcessoAto via response listener
                    # CRÍTICO: resp.body() aqui é thread-safe (loop de eventos do Playwright)
                    # e captura o body ANTES de qualquer cancelamento externo.
                    _a0_body_captured = [None]  # body bytes se PDF direto
                    def _on_response_a0(resp):
                        try:
                            if 'ContadorAcessoAto' not in resp.url and 'ContadorAcesso' not in resp.url:
                                return
                            # 1) Tentar capturar Location header (redirect)
                            loc = resp.headers.get('location', '')
                            if loc:
                                if loc.startswith('http'):
                                    _a0_redirect_url[0] = loc
                                else:
                                    from urllib.parse import urljoin as _uj_r0
                                    _a0_redirect_url[0] = _uj_r0(resp.url, loc)
                            # 2) Tentar capturar body diretamente (PDF servido inline)
                            ct = resp.headers.get('content-type', '').lower()
                            cd = resp.headers.get('content-disposition', '').lower()
                            is_pdf = ('pdf' in ct or 'octet' in ct or
                                      'pdf' in cd or resp.url.lower().endswith('.pdf'))
                            # Mesmo sem content-type definitivo, tentar se há Content-Disposition
                            if cd or is_pdf or resp.status == 200:
                                try:
                                    body = resp.body()
                                    sz = len(body) if body else 0
                                    if sz > 1000 and body[:4] == b'%PDF':
                                        _a0_body_captured[0] = body
                                        logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ A0 response_listener: PDF capturado {sz//1024}KB ct={ct} cd={cd[:40]}'})
                                    elif sz > 0:
                                        logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ A0 response_listener: body {sz}b ct={ct} cd={cd[:40]} head={body[:16].hex()}'})
                                except Exception as _be:
                                    logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ A0 response_listener body() falhou: {str(_be)[:60]}'})
                        except Exception:
                            pass
                    page.context.on('response', _on_response_a0)

                    _url_antes_a0 = page.url
                    logs.append({'nivel': 'info', 'msg': f'{label}: 🖱️ A0: clicando — URL antes: {_url_antes_a0[:80]}'})
                    el.click()

                    # 4) Esperar navegação + interceptação (route já está ativo)
                    _deadline_a0 = time.time() + 30
                    _last_log_a0 = time.time()
                    _last_url_a0 = _url_antes_a0
                    while time.time() < _deadline_a0:
                        # Route interceptou PDF?
                        _pdfs_a0 = [(u, ct, b) for u, ct, b in _ctx_responses
                                    if 'pdf' in ct or u.lower().endswith('.pdf') or 'octet' in ct]
                        if _pdfs_a0 or _a0_dl[0]:
                            break
                        # Página navegou para URL diferente?
                        try:
                            _curr_url = page.url
                            if _curr_url != _last_url_a0:
                                logs.append({'nivel': 'info', 'msg': f'{label}: 🔀 A0: página navegou → {_curr_url[:80]}'})
                                _last_url_a0 = _curr_url
                                # Ignorar mudança apenas de fragmento (#) — não é navegação real
                                _url_sem_frag = _curr_url.split('#')[0]
                                _antes_sem_frag = _url_antes_a0.split('#')[0]
                                _eh_navegacao_real = (
                                    _url_sem_frag != _antes_sem_frag and
                                    'ContadorAcessoAto' not in _curr_url and
                                    'about:blank' not in _curr_url
                                )
                                if _eh_navegacao_real:
                                    time.sleep(2)  # dar tempo ao route
                                    break
                        except Exception:
                            pass
                        # Se dialog foi disparado, o download ESTÁ vindo — extender prazo generosamente
                        if _a0_dialog[0] and not _a0_dialog_extended[0]:
                            _a0_dialog_extended[0] = True
                            _novo_prazo = time.time() + 90  # arquivo grande pode demorar
                            if _novo_prazo > _deadline_a0:
                                logs.append({'nivel': 'info', 'msg': f'{label}: ⏳ A0: dialog detectado — extendendo espera 90s para download grande'})
                                _deadline_a0 = _novo_prazo
                        # Log de progresso a cada 5s
                        if time.time() - _last_log_a0 >= 5:
                            _dialog_status = f'dialog="{_a0_dialog[0][:30]}"' if _a0_dialog[0] else 'sem dialog'
                            _route_count = len(_ctx_responses)
                            logs.append({'nivel': 'info', 'msg': f'{label}: ⏳ A0: aguardando... {_dialog_status} route={_route_count} url={page.url[:50]}'})
                            _last_log_a0 = time.time()
                        time.sleep(0.4)

                    # 5) Restaurar window.open
                    try:
                        page.evaluate('() => { if (window.__orig_open) window.open = window.__orig_open; }')
                    except Exception:
                        pass

                    # Remover listeners A0
                    for _t_a0 in (page, page.context):
                        try: _t_a0.remove_listener('dialog', _on_dialog_a0)
                        except Exception: pass
                        try: _t_a0.remove_listener('download', _on_dl_a0)
                        except Exception: pass
                        try: _t_a0.remove_listener('response', _on_response_a0)
                        except Exception: pass

                    # 6) Verificar resultados
                    if _a0_dl[0]:
                        _dp_a0 = tempfile.mktemp(suffix='.pdf')
                        _alert_a0 = f' alert={_a0_dialog[0][:40]}' if _a0_dialog[0] else ''
                        _dl_url_a0 = _a0_dl[0].url
                        _saved_a0_ok = False
                        _fetch_url_a0 = _a0_redirect_url[0] or _dl_url_a0

                        # ── Tentativa 0: body capturado pelo response_listener ─────────
                        if _a0_body_captured[0]:
                            try:
                                with open(_dp_a0, 'wb') as _f0:
                                    _f0.write(_a0_body_captured[0])
                                _sz0 = os.path.getsize(_dp_a0)
                                logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ A0 — PDF via response_listener: {_sz0//1024}KB{_alert_a0}'})
                                _saved_a0_ok = True
                            except Exception as _e0:
                                logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ A0 response_listener write falhou: {_e0}'})
                        # ─────────────────────────────────────────────────────────────

                        # ── HAR: ler entrada do ContadorAcessoAto ──────────────────────
                        try:
                            import json as _jhar, glob as _ghar
                            # NÃO fechar context aqui — mata o download object
                            # HAR só é lido para diagnóstico, context fica vivo
                            _hars = sorted(_ghar.glob('/tmp/buscafacil_har_*.har'), key=lambda f: os.path.getmtime(f), reverse=True)
                            if _hars:
                                _hpath = _hars[0]
                                _hsize = os.path.getsize(_hpath)
                                logs.append({'nivel': 'info', 'msg': f'{label}: 🔬 HAR: {_hpath} ({_hsize//1024}KB)'})
                                with open(_hpath, 'r', errors='ignore') as _hf:
                                    _hdata = _jhar.load(_hf)
                                _entries = _hdata.get('log', {}).get('entries', [])
                                for _he in _entries:
                                    _hurl = _he.get('request', {}).get('url', '')
                                    if 'ContadorAcessoAto' in _hurl or 'ContadorAcesso' in _hurl:
                                        _hresp = _he.get('response', {})
                                        _hstatus = _hresp.get('status')
                                        _hct = _hresp.get('content', {}).get('mimeType', '')
                                        _hbodysize = _hresp.get('bodySize', -1)
                                        _hcontent = _hresp.get('content', {})
                                        _hbody = _hcontent.get('text', '')
                                        _hencoding = _hcontent.get('encoding', '')
                                        _hheaders = {h['name']: h['value'] for h in _hresp.get('headers', [])}
                                        logs.append({'nivel': 'info', 'msg': (
                                            f'{label}: 🔬 HAR ContadorAcessoAto | '
                                            f'status={_hstatus} | ct={_hct} | '
                                            f'bodySize={_hbodysize} | encoding={_hencoding} | '
                                            f'cd={_hheaders.get("content-disposition","")} | '
                                            f'location={_hheaders.get("location","")} | '
                                            f'body_len={len(_hbody)} | '
                                            f'body_head={_hbody[:200]}'
                                        )})
                            else:
                                logs.append({'nivel': 'info', 'msg': f'{label}: 🔬 HAR: nenhum arquivo encontrado'})
                        except Exception as _har_err:
                            logs.append({'nivel': 'info', 'msg': f'{label}: 🔬 HAR erro: {str(_har_err)[:80]}'})
                        # ───────────────────────────────────────────────────────────────

                        try:
                            _t_bg.join(timeout=20)
                            if _t_bg.is_alive():
                                logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ A0 bg fetch ainda rodando — prosseguindo'})
                            else:
                                logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ A0 bg fetch concluído'})
                        except Exception:
                            pass
                        # Tentativa 1a: download.path() — caminho temp onde browser já salvou
                        try:
                            _tmp_path_a0 = _a0_dl[0].path()
                            if _tmp_path_a0 and os.path.exists(_tmp_path_a0):
                                _sz_tmp = os.path.getsize(_tmp_path_a0)
                                if _sz_tmp > 10000:
                                    import shutil as _sh_a0
                                    _sh_a0.copy2(_tmp_path_a0, _dp_a0)
                                    logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ A0 — download.path() funcionou: {_sz_tmp//1024}KB{_alert_a0}'})
                                    _saved_a0_ok = True
                                else:
                                    logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ A0 download.path() pequeno ({_sz_tmp}b) — tentando save_as'})
                            else:
                                logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ A0 download.path()={_tmp_path_a0} — tentando save_as'})
                        except Exception as _dp_err:
                            logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ A0 download.path() falhou: {str(_dp_err)[:60]}'})

                        # Tentativa 1b: save_as normal
                        if not _saved_a0_ok:
                          try:
                            _a0_dl[0].save_as(_dp_a0)
                            _sz_a0 = os.path.getsize(_dp_a0)
                            if _sz_a0 > 10000:
                                logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ A0 — download salvo direto: {_sz_a0//1024}KB{_alert_a0}'})
                                _saved_a0_ok = True
                          except Exception as _sa_err:
                            logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ A0 save_as falhou ({str(_sa_err)[:50]})'})

                        # Tentativa 1c: curl via subprocess com cookies da sessão + proxy
                        # O curl faz streaming direto para disco — não passa pelo buffer do Playwright
                        # e lida com arquivos grandes de forma diferente do browser
                        if not _saved_a0_ok:
                            try:
                                import subprocess as _sp_curl, shutil as _sh_curl
                                _curl_bin = _sh_curl.which('curl')
                                if _curl_bin and _fetch_url_a0 and _fetch_url_a0.startswith('http'):
                                    # Montar string de cookies da sessão
                                    _curl_cookies = '; '.join(
                                        f"{c['name']}={c['value']}"
                                        for c in page.context.cookies()
                                        if 'rio.rj.gov.br' in c.get('domain', '')
                                    )
                                    _proxy_url_curl = os.getenv('PROXY_URL', '')
                                    if not _proxy_url_curl:
                                        _pu = os.getenv('PROXY_SERVER','')
                                        _puser = os.getenv('PROXY_USERNAME','')
                                        _ppw = os.getenv('PROXY_PASSWORD','')
                                        if _pu and _puser:
                                            _proxy_url_curl = f'http://{_puser}:{_ppw}@{_pu}'
                                        elif _pu:
                                            _proxy_url_curl = f'http://{_pu}'
                                    _curl_cmd = [
                                        _curl_bin, '-L', '-s', '-S',
                                        '--max-time', '60',
                                        '--connect-timeout', '15',
                                        '-A', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
                                        '-e', 'https://www2.rio.rj.gov.br/smu/buscafacil/RelacaoDocumentos.asp',
                                        '-o', _dp_a0,
                                        _fetch_url_a0,
                                    ]
                                    if _curl_cookies:
                                        _curl_cmd += ['--cookie', _curl_cookies]
                                    if _proxy_url_curl:
                                        _curl_cmd += ['--proxy', _proxy_url_curl]
                                    logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ A0 curl: {_fetch_url_a0[:70]} proxy={bool(_proxy_url_curl)} cookies={len(_curl_cookies)}chars'})
                                    _curl_result = _sp_curl.run(_curl_cmd, capture_output=True, timeout=70)
                                    if os.path.exists(_dp_a0):
                                        _sz_curl = os.path.getsize(_dp_a0)
                                        _head_curl = open(_dp_a0,'rb').read(8)
                                        logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ A0 curl: returncode={_curl_result.returncode} sz={_sz_curl}b head={_head_curl.hex()} stderr={_curl_result.stderr[:100]}'})
                                        if _sz_curl > 10000 and _head_curl[:4] == b'%PDF':
                                            logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ A0 — PDF via curl: {_sz_curl//1024}KB{_alert_a0}'})
                                            _saved_a0_ok = True
                                        elif _sz_curl > 100 and _head_curl[:2] == b'<!' or _head_curl[:2] == b'<h':
                                            # HTML do ContadorAcessoAto — extrair URL real do PDF e curl novamente
                                            import re as _re_curl
                                            _html_curl = open(_dp_a0, 'r', errors='ignore').read()
                                            _pdf_url_curl = None
                                            for _pat_curl in [
                                                r"window\.open\s*\(\s*['\"]([^'\"]+)['\"]",
                                                r"window\.location(?:\.href)?\s*=\s*['\"]([^'\"]+)['\"]",
                                                r"href=['\"]([^'\"]*(?:\.pdf|AtoDocumento)[^'\"]*)['\"]",
                                            ]:
                                                _m_curl = _re_curl.search(_pat_curl, _html_curl, _re_curl.IGNORECASE)
                                                if _m_curl:
                                                    _pdf_url_curl = _m_curl.group(1)
                                                    if not _pdf_url_curl.startswith('http'):
                                                        from urllib.parse import urljoin as _uj_curl
                                                        _pdf_url_curl = _uj_curl(_fetch_url_a0, _pdf_url_curl)
                                                    break
                                            if _pdf_url_curl:
                                                logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ A0 curl HTML→PDF URL: {_pdf_url_curl[:80]}'})
                                                _curl_cmd2 = [
                                                    _curl_bin, '-L', '-s', '-S',
                                                    '--max-time', '60',
                                                    '--connect-timeout', '15',
                                                    '-A', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
                                                    '-e', _fetch_url_a0,
                                                    '-o', _dp_a0,
                                                    _pdf_url_curl,
                                                ]
                                                if _curl_cookies:
                                                    _curl_cmd2 += ['--cookie', _curl_cookies]
                                                if _proxy_url_curl:
                                                    _curl_cmd2 += ['--proxy', _proxy_url_curl]
                                                _curl_result2 = _sp_curl.run(_curl_cmd2, capture_output=True, timeout=70)
                                                if os.path.exists(_dp_a0):
                                                    _sz_curl2 = os.path.getsize(_dp_a0)
                                                    _head_curl2 = open(_dp_a0,'rb').read(8)
                                                    logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ A0 curl2: rc={_curl_result2.returncode} sz={_sz_curl2}b head={_head_curl2.hex()} stderr={_curl_result2.stderr[:100]}'})
                                                    if _sz_curl2 > 10000 and _head_curl2[:4] == b'%PDF':
                                                        logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ A0 — PDF via curl2: {_sz_curl2//1024}KB{_alert_a0}'})
                                                        _saved_a0_ok = True
                                            else:
                                                # Logar HTML completo para inspecionar estrutura real
                                                _html_iso = open(_dp_a0, 'r', encoding='iso-8859-1', errors='ignore').read()
                                                logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ A0 curl HTML completo (utf8): {_html_curl}'})
                                                logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ A0 curl HTML completo (iso): {_html_iso}'})
                                else:
                                    logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ A0 curl: não disponível ou URL inválida'})
                            except Exception as _curl_err:
                                logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ A0 curl falhou: {str(_curl_err)[:80]}'})

                        # ══ DIAGNÓSTICO — roda após tentativas de save (contexto ainda vivo) ══

                        # Tentativa 2: context.request (usa proxy do browser — funciona onde Python requests não funciona)
                        # context.request retornou 1208b HTML do ContadorAcessoAto com window.open('URL_PDF').
                        # Parseamos esse HTML para extrair a URL real do PDF e buscamos com context.request novamente.
                        if not _saved_a0_ok and _fetch_url_a0 and _fetch_url_a0.startswith('http'):
                            try:
                                logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ A0 context.request → {_fetch_url_a0[:80]}'})
                                _ctx_resp_a0 = page.context.request.get(
                                    _fetch_url_a0,
                                    timeout=30000,
                                    headers={'Referer': 'https://www2.rio.rj.gov.br/smu/buscafacil/RelacaoDocumentos.asp'}
                                )
                                _body_a0 = _ctx_resp_a0.body()
                                _ct_a0 = _ctx_resp_a0.headers.get('content-type', '').lower()
                                logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ A0 context.request: {_ctx_resp_a0.status} ct={_ct_a0[:30]} sz={len(_body_a0)}b'})
                                if len(_body_a0) > 1000 and (_body_a0[:4] == b'%PDF' or 'pdf' in _ct_a0 or 'octet' in _ct_a0):
                                    open(_dp_a0, 'wb').write(_body_a0)
                                    logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ A0 — PDF via context.request: {len(_body_a0)//1024}KB{_alert_a0}'})
                                    _saved_a0_ok = True
                                elif len(_body_a0) > 100:
                                    # HTML do ContadorAcessoAto (1208b) — extrair URL real do PDF
                                    _html_a0 = _body_a0.decode('utf-8', errors='ignore')
                                    logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ A0 context.request HTML ({len(_body_a0)}b): {repr(_html_a0[:200])}'})
                                    import re as _re_a0
                                    _pdf_url_a0 = None
                                    _pats_a0 = [
                                        r'window\.open\s*\(\s*[\x27\x22]([^\x27\x22]+)[\x27\x22]',
                                        r'window\.location(?:\.href)?\s*=\s*[\x27\x22]([^\x27\x22]+)[\x27\x22]',
                                        r'location\.(?:replace|assign)\s*\(\s*[\x27\x22]([^\x27\x22]+)[\x27\x22]',
                                        r'content=[\x27\x22][^\x27\x22]*url=([^\x27\x22> \s]+)',
                                        r'href=[\x27\x22]([^\x27\x22]*(?:\.pdf|AtoDocumento|arquivo)[^\x27\x22]*)[\x27\x22]',
                                    ]
                                    for _pat_a0 in _pats_a0:
                                        _m_a0 = _re_a0.search(_pat_a0, _html_a0, _re_a0.IGNORECASE)
                                        if _m_a0:
                                            _pdf_url_a0 = _m_a0.group(1)
                                            break
                                    if _pdf_url_a0:
                                        if not _pdf_url_a0.startswith('http'):
                                            from urllib.parse import urljoin as _uj_a0
                                            _pdf_url_a0 = _uj_a0('https://www2.rio.rj.gov.br', _pdf_url_a0)
                                        logs.append({'nivel': 'info', 'msg': f'{label}: 🎯 A0 URL extraída do HTML: {_pdf_url_a0[:100]}'})
                                        try:
                                            _ctx_pdf_a0 = page.context.request.get(
                                                _pdf_url_a0,
                                                timeout=60000,
                                                headers={'Referer': _fetch_url_a0}
                                            )
                                            _body_pdf_a0 = _ctx_pdf_a0.body()
                                            if _body_pdf_a0[:4] == b'%PDF' and len(_body_pdf_a0) > 1000:
                                                open(_dp_a0, 'wb').write(_body_pdf_a0)
                                                logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ A0 — PDF via context.request+parse: {len(_body_pdf_a0)//1024}KB{_alert_a0}'})
                                                _saved_a0_ok = True
                                            else:
                                                logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ A0 PDF URL retornou não-PDF ({len(_body_pdf_a0)}b)'})
                                        except Exception as _e_pdf_a0:
                                            logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ A0 fetch PDF URL falhou: {str(_e_pdf_a0)[:80]}'})
                                    else:
                                        logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ A0 sem URL detectável no HTML'})
                            except Exception as _cr_err:
                                logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ A0 context.request falhou: {str(_cr_err)[:80]}'})
                        if _saved_a0_ok:
                            _remove_ctx_listeners()
                            _cleanup_listeners()
                            return f'Download: {_dp_a0}'

                    _pdfs_a0 = [(u, ct, b) for u, ct, b in _ctx_responses
                                if ('pdf' in ct or u.lower().endswith('.pdf') or 'octet' in ct) and b and len(b) > 1000]
                    if _pdfs_a0:
                        u0, ct0, b0 = _pdfs_a0[0]
                        _saved_a0 = _salvar_corpo(u0, ct0, b0)
                        if _saved_a0:
                            logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ A0 — route capturou PDF: {len(b0)//1024}KB'})
                            _remove_ctx_listeners()
                            _cleanup_listeners()
                            return f'Download: {_saved_a0}'

                    # Tentar URLs enfileiradas pelo route (JS redirect extraído do HTML 247b)
                    _pending_urls_a0 = [(u, ct) for u, ct, b in _ctx_responses
                                        if b is None and ('pdf' in ct or 'pdf' in u.lower() or 'download' in u.lower())]
                    for _pu_a0, _pct_a0 in _pending_urls_a0:
                        logs.append({'nivel': 'info', 'msg': f'{label}: 🔄 A0: buscando URL do redirect JS: {_pu_a0[:80]}'})
                        try:
                            import requests as _req_a0
                            _ck_a0 = {}
                            try:
                                for c in page.context.cookies():
                                    _ck_a0[c['name']] = c['value']
                            except Exception:
                                pass
                            # Tentar sem proxy primeiro
                            for _proxy_a0 in [None, _get_proxy_requests()]:
                                _r_pu = _req_a0.get(
                                    _pu_a0, cookies=_ck_a0, proxies=_proxy_a0, timeout=20,
                                    allow_redirects=True,
                                    headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                                             'Referer': _direct_url}
                                )
                                _ct_pu = _r_pu.headers.get('content-type', '').lower()
                                _sz_pu = len(_r_pu.content)
                                _pxy_lbl = 'sem proxy' if _proxy_a0 is None else 'com proxy'
                                logs.append({'nivel': 'info', 'msg': f'{label}: 📡 A0 redirect fetch ({_pxy_lbl}): {_r_pu.status_code} {_ct_pu[:25]} {_sz_pu}b → {_r_pu.url[:60]}'})
                                if 'pdf' in _ct_pu or 'octet' in _ct_pu or _r_pu.url.lower().endswith('.pdf') or _r_pu.content[:4] == b'%PDF':
                                    _dp_pu = tempfile.mktemp(suffix='.pdf')
                                    with open(_dp_pu, 'wb') as _f_pu:
                                        _f_pu.write(_r_pu.content)
                                    logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ A0 — PDF via redirect JS: {_sz_pu//1024}KB'})
                                    _remove_ctx_listeners()
                                    _cleanup_listeners()
                                    return f'Download: {_dp_pu}'
                                if _sz_pu > 1000:
                                    break  # Teve resposta, não adianta tentar proxy
                        except Exception as _e_pu:
                            logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ A0 redirect fetch falhou: {str(_e_pu)[:60]}'})

                    _all_resp_a0 = [(u[:60], ct[:30], len(b) if b else 0) for u,ct,b in _ctx_responses]
                    _dialog_fim_a0 = f'dialog="{_a0_dialog[0][:50]}"' if _a0_dialog[0] else 'sem dialog'
                    _url_fim_a0 = page.url[:80] if page else '?'
                    logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ A0 — sem resultado em 30s | url_final={_url_fim_a0} | {_dialog_fim_a0} | route_responses={_all_resp_a0}'})

                    # ── A0-WAIT: dialog aceito → download em background ─────────────
                    # O Chromium inicia o download logo após o OK do dialog.
                    # save_as falhou com "canceled" porque o download ainda estava
                    # em andamento. Aguardar até 180s com polling em download.path().
                    _dl_wait = _a0_dl[0] if _a0_dl[0] else _download_obj
                    if _a0_dialog[0] and _dl_wait:
                        logs.append({'nivel': 'info', 'msg': f'{label}: ⏳ A0-WAIT: dialog aceito + download em background — aguardando conclusão (até 180s)...'})
                        _wait_deadline = time.time() + 180
                        _last_wait_log = time.time()
                        _dp_wait = tempfile.mktemp(suffix='.pdf')
                        _wait_ok = False
                        while time.time() < _wait_deadline:
                            try:
                                _path_now = _dl_wait.path()
                                if _path_now and os.path.exists(_path_now):
                                    _sz_now = os.path.getsize(_path_now)
                                    if _sz_now > 50000:
                                        import shutil as _sh_wait
                                        _sh_wait.copy2(_path_now, _dp_wait)
                                        logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ A0-WAIT — download.path() concluído: {_sz_now//1024}KB'})
                                        _wait_ok = True
                                        break
                            except Exception:
                                pass
                            if time.time() - _last_wait_log >= 10:
                                try:
                                    _dl_wait.save_as(_dp_wait)
                                    _sz_sa = os.path.getsize(_dp_wait)
                                    if _sz_sa > 50000:
                                        logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ A0-WAIT — save_as concluído: {_sz_sa//1024}KB'})
                                        _wait_ok = True
                                        break
                                except Exception:
                                    pass
                                _elapsed = round(time.time() - (_wait_deadline - 180))
                                logs.append({'nivel': 'info', 'msg': f'{label}: ⏳ A0-WAIT: aguardando download... {_elapsed}s'})
                                _last_wait_log = time.time()
                            time.sleep(2)
                        if _wait_ok:
                            _remove_ctx_listeners()
                            _cleanup_listeners()
                            return f'Download: {_dp_wait}'
                        else:
                            logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ A0-WAIT: não concluiu em 180s — prosseguindo cascata'})
                    # ────────────────────────────────────────────────────────────────

                except Exception as _e_a0:
                    logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ A0 erro: {str(_e_a0)[:80]}'})
            #
            # Fluxo LC 270 (arquivo grande):
            #   clique → popup abre → alert NO POPUP → OK → download no popup
            #   context.on('dialog') aceita o alert mesmo sendo do popup
            #   context.expect_event('download') captura download de qualquer página
            #   Timeout curto (5s): se não vier download, provavelmente é LC 198
            #
            # Fluxo LC 198 (arquivo pequeno):
            #   clique → popup abre → redirect HTTP → route captura PDF
            #   expect_download dá timeout em 5s (sem efeito colateral)
            #   popup já está nas pages do contexto → route já capturou via _ctx_responses
            # ═══════════════════════════════════════════════════════════════════
            _dialog_alert = [None]
            def _on_dialog(dialog):
                _dialog_alert[0] = dialog.message
                try:
                    dialog.accept()
                except Exception:
                    pass
            # Registrar em page E context — alert pode vir da página principal ou do popup
            for _dialog_target in (page, page.context):
                try:
                    _dialog_target.on('dialog', _on_dialog)
                except Exception:
                    pass

            popup_page = None

            # ── Tentativa 1: context-level download wrapping o clique ─────────
            # Fluxo real LC 270 (confirmado pelo usuário):
            #   clique → JS abre janela 1×1px → ContadorAcessoAto carrega →
            #   JS verifica tamanho → alert("arquivo grande, será baixado") →
            #   context.on('dialog') aceita → download dispara →
            #   context.expect_event('download') captura.
            # route.continue_() (não fetch) deixa JS do ContadorAcessoAto executar.
            # Timeout 40s: proxy pode atrasar. LC 198 não dispara download → timeout.
            _t_start_a = time.time()
            logs.append({'nivel': 'info', 'msg': f'{label}: 🔄 A — aguardando download/popup (40s timeout)...'})
            try:
                with page.context.expect_event('download', timeout=40000) as _dl_primary:
                    el.click()
                _dl_val = _dl_primary.value
                _dp_primary = tempfile.mktemp(suffix='.pdf')
                _dl_val.save_as(_dp_primary)
                _sz_primary = os.path.getsize(_dp_primary) // 1024
                _alert_msg = f' — "{_dialog_alert[0][:60]}"' if _dialog_alert[0] else ''
                _t_a = round(time.time() - _t_start_a, 1)
                logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ A (popup+alert+download): {_sz_primary}KB em {_t_a}s{_alert_msg}'})
                for _dt in (page, page.context):
                    try: _dt.remove_listener('dialog', _on_dialog)
                    except Exception: pass
                _remove_ctx_listeners()
                _cleanup_listeners()
                return f'Download: {_dp_primary}'
            except Exception:
                # Sem download em 5s — popup provavelmente abriu e route capturou (LC 198)
                time.sleep(1)  # margem para route finalizar

            # ── Tentativa 2: popup já aberto, route capturou (LC 198) ─────────
            try:
                _all_pages = page.context.pages
                if len(_all_pages) > 1:
                    popup_page = _all_pages[-1]
                    try:
                        pu = popup_page.url or ''
                        if pu and pu != 'about:blank' and pu not in _popup_nav_urls:
                            _popup_nav_urls.insert(0, pu)
                    except Exception:
                        pass
                    logs.append({'nivel': 'ok', 'msg': f'{label}: 🖪 Popup aberto: {(_popup_nav_urls[0] if _popup_nav_urls else "?")[:80]}'})
                    # Aguardar route capturar se ainda não capturou
                    deadline = time.time() + 10
                    while time.time() < deadline:
                        if _ctx_responses or len(_popup_nav_urls) >= 2:
                            break
                        time.sleep(0.3)
                    time.sleep(1)
                else:
                    # Nenhum popup e nenhum download — re-clicar esperando popup
                    with page.expect_popup(timeout=12000) as popup_info:
                        el.click()
                    popup_page = popup_info.value
                    try:
                        pu = popup_page.url or ''
                        if pu and pu != 'about:blank' and pu not in _popup_nav_urls:
                            _popup_nav_urls.insert(0, pu)
                    except Exception:
                        pass
                    logs.append({'nivel': 'ok', 'msg': f'{label}: 🖪 Popup (re-clique): {(_popup_nav_urls[0] if _popup_nav_urls else "?")[:80]}'})
                    deadline = time.time() + 10
                    while time.time() < deadline:
                        if _ctx_responses or len(_popup_nav_urls) >= 2:
                            break
                        time.sleep(0.3)
                    time.sleep(1)

            except Exception as e_popup_open:
                logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ A — sem download nem popup: {str(e_popup_open)[:60]}'})
                time.sleep(4)

            for _dt in (page, page.context):
                try:
                    _dt.remove_listener('dialog', _on_dialog)
                except Exception:
                    pass

            _popup_nav_str = " → ".join(u[:60] for u in _popup_nav_urls) if _popup_nav_urls else "(nenhuma)"
            logs.append({'nivel': 'info', 'msg': f'{label}: 🔗 Nav popup: {_popup_nav_str}'})
            _route_detail = [(u[:60], ct[:25], len(b) if b else 0) for u,ct,b in _ctx_responses]
            logs.append({'nivel': 'info', 'msg': f'{label}: 📡 Respostas route ({len(_ctx_responses)}): {_route_detail}'})

            # ── A: route interceptou PDF? ─────────────────────────────────────
            for cap_url, cap_ct, cap_body in _ctx_responses:
                if 'pdf' in cap_ct or cap_url.lower().endswith('.pdf') or 'octet' in cap_ct:
                    saved = _salvar_corpo(cap_url, cap_ct, cap_body)
                    if saved:
                        logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ A — route capturou PDF'})
                        _remove_ctx_listeners()
                        _cleanup_listeners()
                        return f'Download: {saved}'
            if _ctx_responses:
                logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ A — route capturou HTML mas não PDF | Causa: ContadorAcessoAto serve HTML vazio sem JS executado'})
            else:
                logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ A — route não capturou nada | Causa provável: proxy processa respostas antes do route interceptar'})

            # ═══════════════════════════════════════════════════════════════════
            # ESTRATÉGIA A1 — navegar página principal para URL do contador
            #
            # Em vez de clicar no ícone (que abre popup onde proxy interfere),
            # redireciona a página principal diretamente para ContadorAcessoAto.
            # O alert aparece na página principal → context.on('dialog') aceita →
            # download dispara na página principal → sem popup = sem interferência.
            #
            # Requer _direct_url extraída pelo A½.
            # ═══════════════════════════════════════════════════════════════════
            if _direct_url:
                # Montar URL absoluta
                try:
                    from urllib.parse import urlparse as _up_a1, urljoin as _uj_a1
                    _base_a1 = f"{_up_a1(page.url).scheme}://{_up_a1(page.url).netloc}"
                    _abs_url_a1 = _direct_url if _direct_url.startswith('http') else _uj_a1(_base_a1, _direct_url)
                except Exception:
                    _abs_url_a1 = _direct_url

                _dialog_a1 = [None]
                def _on_dialog_a1(dialog):
                    _dialog_a1[0] = dialog.message
                    try: dialog.accept()
                    except Exception: pass

                for _dt_a1 in (page, page.context):
                    try: _dt_a1.on('dialog', _on_dialog_a1)
                    except Exception: pass

                logs.append({'nivel': 'info', 'msg': f'{label}: 🔄 A1 — nova aba: {_abs_url_a1[:80]}'})
                _page_a1 = None
                try:
                    # Abrir nova aba com mesmas cookies/proxy.
                    # ContadorAcessoAto serve HTML com JS que dispara alert+download.
                    # route.continue_() (não fetch()) deixa o JS executar normalmente.
                    _page_a1 = page.context.new_page()

                    _a1_dl   = [None]
                    _a1_urls = []
                    def _on_dl_a1_page(d):  _a1_dl[0] = d
                    def _on_nav_a1(frame):
                        try:
                            u = frame.url or ''
                            if u and u not in ('about:blank', '') and u not in _a1_urls:
                                _a1_urls.append(u)
                        except Exception: pass

                    _page_a1.on('download', _on_dl_a1_page)
                    _page_a1.on('framenavigated', _on_nav_a1)
                    page.context.on('download', _on_dl_a1_page)  # captura cross-page

                    # goto com commit (não wait_for_load) — JS executa depois do commit
                    try:
                        _page_a1.goto(_abs_url_a1, wait_until='commit', timeout=15000)
                    except Exception:
                        pass  # ERR_EMPTY_RESPONSE normal se o servidor só envia JS redirect

                    # Aguardar até 35s para JS executar, alert ser aceito, download iniciar
                    _deadline_a1 = time.time() + 35
                    while time.time() < _deadline_a1:
                        if _a1_dl[0]:
                            break
                        if len(_a1_urls) >= 2:
                            # JS navegou para outra URL (provavelmente PDF)
                            _nav_target = _a1_urls[-1]
                            if _nav_target.lower().endswith('.pdf') or 'pdf' in _nav_target.lower():
                                break
                        time.sleep(0.3)

                    if _a1_dl[0]:
                        _dp_a1 = tempfile.mktemp(suffix='.pdf')
                        _a1_dl[0].save_as(_dp_a1)
                        _sz_a1 = os.path.getsize(_dp_a1) // 1024
                        _alert_a1 = f' — "{_dialog_a1[0][:60]}"' if _dialog_a1[0] else ''
                        logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ A1 — download via nova aba: {_sz_a1}KB{_alert_a1}'})
                        try: _page_a1.close()
                        except Exception: pass
                        for _dt_a1 in (page, page.context):
                            try: _dt_a1.remove_listener('dialog', _on_dialog_a1)
                            except Exception: pass
                        try: page.context.remove_listener('download', _on_dl_a1_page)
                        except Exception: pass
                        _remove_ctx_listeners()
                        _cleanup_listeners()
                        return f'Download: {_dp_a1}'
                    else:
                        _nav_log = ' → '.join(_a1_urls[-3:]) if _a1_urls else 'nenhuma'
                        _alert_seen = f' alert={_dialog_a1[0][:40]}' if _dialog_a1[0] else ''
                        logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ A1 — sem download após 35s | urls: {_nav_log[:80]}{_alert_seen}'})
                    try: _page_a1.close()
                    except Exception: pass
                    try: page.context.remove_listener('download', _on_dl_a1_page)
                    except Exception: pass
                except Exception as _e_a1:
                    logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ A1 — erro: {str(_e_a1)[:80]}'})
                    try:
                        if _page_a1: _page_a1.close()
                    except Exception: pass
                for _dt_a1 in (page, page.context):
                    try: _dt_a1.remove_listener('dialog', _on_dialog_a1)
                    except Exception: pass

            # ═══════════════════════════════════════════════════════════════════
            # ESTRATÉGIA A2 — novo contexto SEM proxy, cookies transferidos
            #
            # Causa raiz: o proxy residencial bloqueia ContadorAcessoAto (retorna
            # ERR_EMPTY_RESPONSE no popup). Sem proxy: WAF bloqueia a navegação
            # principal, mas ContadorAcessoAto pode não ter WAF.
            # Solução: criar contexto limpo (sem proxy), transferir cookies da
            # sessão autenticada via proxy, abrir ContadorAcessoAto nesse contexto.
            # ═══════════════════════════════════════════════════════════════════
            if _direct_url:
                logs.append({'nivel': 'info', 'msg': f'{label}: 🔄 A2 — contexto sem proxy + cookies transferidos'})
                _saved_a2_holder = [None]
                def _run_a2():
                 try:
                    from playwright.sync_api import sync_playwright as _spw_a2
                    _exec_a2 = None
                    import shutil as _sh_a2, glob as _gl_a2
                    for _cn_a2 in ['chromium', 'chromium-browser', 'google-chrome-stable']:
                        _p_a2 = _sh_a2.which(_cn_a2)
                        if _p_a2:
                            _exec_a2 = _p_a2
                            break
                    if not _exec_a2:
                        _nix_a2 = _gl_a2.glob('/nix/store/*/bin/chromium')
                        if _nix_a2:
                            _exec_a2 = _nix_a2[0]

                    # Exportar cookies da sessão proxy
                    _storage_a2 = None
                    try:
                        _storage_a2 = page.context.storage_state()
                    except Exception:
                        pass

                    with _spw_a2() as _pw_a2:
                        _la_a2 = {
                            'headless': True,
                            'args': ['--no-sandbox', '--disable-dev-shm-usage',
                                     '--disable-gpu', '--single-process', '--no-zygote']
                        }
                        if _exec_a2:
                            _la_a2['executable_path'] = _exec_a2
                        # SEM proxy — intencionalmente
                        _br_a2 = _pw_a2.chromium.launch(**_la_a2)

                        _ctx_kwargs_a2 = {
                            'viewport': {'width': 1280, 'height': 800},
                            'user_agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                                           'AppleWebKit/537.36 (KHTML, like Gecko) '
                                           'Chrome/120.0.0.0 Safari/537.36'),
                            'ignore_https_errors': True,
                        }
                        if _storage_a2:
                            _ctx_kwargs_a2['storage_state'] = _storage_a2

                        _ctx_a2 = _br_a2.new_context(**_ctx_kwargs_a2)
                        _pg_a2 = _ctx_a2.new_page()

                        _a2_dl = [None]
                        _a2_alert = [None]
                        def _on_dl_a2(d): _a2_dl[0] = d
                        def _on_dialog_a2(dlg):
                            _a2_alert[0] = dlg.message
                            try: dlg.accept()
                            except Exception: pass
                        _pg_a2.on('download', _on_dl_a2)
                        _ctx_a2.on('download', _on_dl_a2)
                        _pg_a2.on('dialog', _on_dialog_a2)
                        _ctx_a2.on('dialog', _on_dialog_a2)

                        # Interceptar PDF via route (sem proxy, body disponível)
                        _a2_pdf_body = [None]
                        _a2_pdf_url  = [None]
                        def _a2_route(route):
                            try:
                                resp = route.fetch()
                                ct = resp.headers.get('content-type', '').lower()
                                if 'pdf' in ct or 'octet' in ct or route.request.url.lower().endswith('.pdf'):
                                    try:
                                        _a2_pdf_body[0] = resp.body()
                                        _a2_pdf_url[0]  = resp.url
                                    except Exception:
                                        pass
                                route.fulfill(response=resp)
                            except Exception:
                                try: route.continue_()
                                except Exception: pass
                        try:
                            _ctx_a2.route('**/*', _a2_route)
                        except Exception:
                            pass

                        try:
                            _pg_a2.goto(_direct_url, wait_until='commit', timeout=15000)
                        except Exception:
                            pass  # ERR_EMPTY_RESPONSE esperado se redirect HTTP imediato

                        # Esperar download ou PDF interceptado (25s)
                        _deadline_a2 = time.time() + 25
                        _last_log_a2 = time.time()
                        while time.time() < _deadline_a2:
                            if _a2_dl[0] or _a2_pdf_body[0]:
                                break
                            if time.time() - _last_log_a2 >= 5:
                                try:
                                    _url_a2_cur = _pg_a2.url[:70]
                                except Exception:
                                    _url_a2_cur = '?'
                                _dl_a2_status = f'dl={_a2_dl[0]}' if _a2_dl[0] else 'sem dl'
                                _pdf_a2_status = f'pdf={len(_a2_pdf_body[0])}b' if _a2_pdf_body[0] else 'sem pdf'
                                logs.append({'nivel': 'info', 'msg': f'{label}: ⏳ A2: {_dl_a2_status} | {_pdf_a2_status} | url={_url_a2_cur}'})
                                _last_log_a2 = time.time()
                            time.sleep(0.4)

                        _saved_a2 = None
                        if _a2_pdf_body[0] and len(_a2_pdf_body[0]) > 1000:
                            _path_a2 = tempfile.mktemp(suffix='.pdf')
                            with open(_path_a2, 'wb') as _f_a2:
                                _f_a2.write(_a2_pdf_body[0])
                            _sz_a2 = len(_a2_pdf_body[0]) // 1024
                            logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ A2 — route interceptou PDF: {_sz_a2}KB'})
                            _saved_a2 = _path_a2
                        elif _a2_dl[0]:
                            _path_a2 = tempfile.mktemp(suffix='.pdf')
                            _a2_dl[0].save_as(_path_a2)
                            _sz_a2 = os.path.getsize(_path_a2) // 1024
                            _alert_a2 = f' alert={_a2_alert[0][:40]}' if _a2_alert[0] else ''
                            logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ A2 — download sem proxy: {_sz_a2}KB{_alert_a2}'})
                            _saved_a2 = _path_a2
                        else:
                            try:
                                _url_a2_fim = _pg_a2.url[:80]
                            except Exception:
                                _url_a2_fim = '?'
                            _alert_a2_fim = f'dialog="{_a2_alert[0][:40]}"' if _a2_alert[0] else 'sem dialog'
                            _pdf_a2_sz = len(_a2_pdf_body[0]) if _a2_pdf_body[0] else 0
                            logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ A2 — sem resultado em 25s | url_final={_url_a2_fim} | {_alert_a2_fim} | pdf_body={_pdf_a2_sz}b'})

                        try: _br_a2.close()
                        except Exception: pass

                        if _saved_a2:
                            _saved_a2_holder[0] = _saved_a2

                 except Exception as _e_a2:
                    logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ A2 erro: {str(_e_a2)[:80]}'})

                import threading as _thr_a2
                _t_a2 = _thr_a2.Thread(target=_run_a2, daemon=True)
                _t_a2.start()
                _t_a2.join(timeout=60)
                if _saved_a2_holder[0]:
                    _remove_ctx_listeners()
                    _cleanup_listeners()
                    return f'Download: {_saved_a2_holder[0]}'
            dl_obj = _popup_dl[0] or _download_obj
            if dl_obj:
                try:
                    dl_path = tempfile.mktemp(suffix='.pdf')
                    dl_obj.save_as(dl_path)
                    dl_size = os.path.getsize(dl_path) // 1024
                    logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ B — download direto: {dl_size}KB'})
                    _remove_ctx_listeners()
                    _cleanup_listeners()
                    return f'Download: {dl_path}'
                except Exception as e_b:
                    logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ B — download falhou: {str(e_b)[:60]}'})

            # URL final para as próximas estratégias
            final_url = _popup_nav_urls[-1] if _popup_nav_urls else (_direct_url or '')
            if popup_page:
                try:
                    curr = popup_page.url
                    if curr and curr != 'about:blank':
                        final_url = curr
                        if curr not in _popup_nav_urls:
                            _popup_nav_urls.append(curr)
                except Exception:
                    pass

            # ── C: ler popup diretamente (se ainda vivo) ─────────────────────
            if popup_page and final_url:
                logs.append({'nivel': 'info', 'msg': f'{label}: 🔄 C — leitura direta do popup'})
                try:
                    ct_popup = popup_page.evaluate('() => document.contentType || ""') or ''
                    if 'pdf' in ct_popup.lower():
                        resp_p = popup_page.request.get(final_url, timeout=20000)
                        if resp_p.status == 200:
                            saved = _salvar_corpo(final_url, ct_popup, resp_p.body())
                            if saved:
                                logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ C — PDF inline do popup'})
                                _remove_ctx_listeners()
                                _cleanup_listeners()
                                return f'Download: {saved}'
                    else:
                        body_popup = popup_page.inner_text('body') or ''
                        if len(body_popup) > 300:
                            logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ C — conteúdo popup: {len(body_popup)} chars'})
                            _remove_ctx_listeners()
                            _cleanup_listeners()
                            return f'Nova aba: {final_url}'
                except Exception as e_c:
                    logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ C falhou: {str(e_c)[:60]} | Causa: popup fechou antes da leitura (contexto morreu)'})

            if not final_url:
                logs.append({'nivel': 'aviso', 'msg': f'{label}: ⚠️ Nenhuma URL capturada — abortando cascade'})
                _remove_ctx_listeners()
                _cleanup_listeners()
                return 'Erro: nenhuma URL capturada no popup'

            # ── D: context.request (mesmo proxy/sessão) ───────────────────────
            logs.append({'nivel': 'info', 'msg': f'{label}: 🔄 D — context.request: {final_url[:70]}'})
            try:
                resp_d = page.context.request.get(final_url, timeout=25000, headers={'Referer': page.url})
                ct_d   = resp_d.headers.get('content-type', '').lower()
                body_d = resp_d.body()
                _is_pdf_d = ('pdf' in ct_d or 'octet' in ct_d or
                             final_url.lower().endswith('.pdf') or
                             body_d[:4] == b'%PDF')
                if _is_pdf_d and len(body_d) > 1000:
                    saved = _salvar_corpo(final_url, ct_d, body_d)
                    if saved:
                        logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ D — PDF via context.request: {len(body_d)//1024}KB'})
                        _remove_ctx_listeners()
                        _cleanup_listeners()
                        return f'Download: {saved}'
                elif len(body_d) > 300:
                    _preview_d = body_d[:200].decode('utf-8','replace').replace('\n',' ')
                    logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ D retornou {ct_d[:25]} ({len(body_d)}b) não-PDF | preview={repr(_preview_d)}'})
                else:
                    logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ D retornou resposta vazia | Causa: sessão do contexto expirou'})
            except Exception as e_d:
                logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ D falhou: {str(e_d)[:80]} | Causa: contexto Playwright destruído junto com o popup'})

            # ── E: requests Python + proxy ────────────────────────────────────
            logs.append({'nivel': 'info', 'msg': f'{label}: 🔄 E — requests Python + proxy: {final_url[:70]}'})
            try:
                import requests as _req
                _cookies_e = {}
                try:
                    for c in page.context.cookies():
                        _cookies_e[c['name']] = c['value']
                except Exception:
                    pass
                _proxies_e = _get_proxy_requests()
                _ua_e = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                         'AppleWebKit/537.36 (KHTML, like Gecko) '
                         'Chrome/120.0.0.0 Safari/537.36')
                r_e   = _req.get(final_url, cookies=_cookies_e, proxies=_proxies_e,
                                 timeout=25, allow_redirects=True,
                                 headers={'User-Agent': _ua_e, 'Referer': page.url})
                ct_e  = r_e.headers.get('content-type', '').lower()
                _is_pdf_e = ('pdf' in ct_e or 'octet' in ct_e or
                             r_e.url.lower().endswith('.pdf') or
                             r_e.content[:4] == b'%PDF')
                if _is_pdf_e and len(r_e.content) > 1000:
                    saved = _salvar_corpo(r_e.url, ct_e, r_e.content)
                    if saved:
                        logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ E — PDF via requests+proxy: {len(r_e.content)//1024}KB'})
                        _remove_ctx_listeners()
                        _cleanup_listeners()
                        return f'Download: {saved}'
                elif len(r_e.content) > 300:
                    _preview_e = r_e.text[:200].replace('\n', ' ') if r_e.text else ''
                    logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ E retornou {ct_e[:25]} ({len(r_e.content)}b) não-PDF | preview={repr(_preview_e)}'})
                else:
                    logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ E retornou vazio ({r_e.status_code}) | Causa: servidor valida sessão + IP'})
            except Exception as e_e:
                logs.append({'nivel': 'aviso', 'msg': f'{label}: ⚠️ E falhou: {str(e_e)[:80]}'})

            # ── F: HTML capturado pelo route (último recurso do route) ────────
            # Ignorar: ContadorAcessoAto (HTML vazio de 1208b), HTMLs pequenos < 5000b
            for cap_url, cap_ct, cap_body in _ctx_responses:
                _is_contador = 'ContadorAcesso' in cap_url
                _is_pdf_f = 'pdf' in (cap_ct or '') or (cap_url or '').lower().endswith('.pdf')
                _body_sz = len(cap_body) if cap_body else 0
                # Aceitar: PDF de qualquer tamanho, ou HTML útil (>5KB, não ContadorAcesso)
                if cap_body and _is_pdf_f and _body_sz > 500:
                    saved = _salvar_corpo(cap_url, cap_ct, cap_body)
                    if saved:
                        logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ F — PDF do route: {_body_sz//1024}KB'})
                        _remove_ctx_listeners()
                        _cleanup_listeners()
                        return f'Download: {saved}'
                elif cap_body and not _is_contador and not _is_pdf_f and _body_sz > 5000:
                    saved = _salvar_corpo(cap_url, cap_ct, cap_body)
                    if saved:
                        logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ F — HTML do route: {_body_sz//1024}KB'})
                        _remove_ctx_listeners()
                        _cleanup_listeners()
                        return f'Nova aba: {cap_url}'
            logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ F — route sem conteúdo útil | Causa: servidor não serve documento fora de sessão de popup autenticada'})

            # ═══════════════════════════════════════════════════════════════════
            # ESTRATÉGIA G — Browser sem proxy + cookies da sessão principal
            # O WAF foi bypassado pelo browser principal. Cookies de sessão podem
            # ser suficientes para autenticar mesmo sem proxy, se o servidor não
            # validar IP de origem da sessão.
            # ═══════════════════════════════════════════════════════════════════
            def _tentar_browser_isolado(usar_proxy: bool, storage_state: dict = None) -> str | None:
                """Lança browser isolado num thread separado para evitar conflito com asyncio."""
                import threading as _threading
                modo = 'com proxy' if usar_proxy else 'sem proxy'
                logs.append({'nivel': 'info', 'msg': f'{label}: 🔄 Browser isolado ({modo}): {final_url[:70]}'})

                _result_holder = [None]

                def _run_in_thread():
                    try:
                        from playwright.sync_api import sync_playwright as _sync_pw2
                        _proxy_cfg = _get_proxy_config_nav() if usar_proxy else None
                        with _sync_pw2() as _pw2:
                            _launch_args2 = {'headless': True, 'args': ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']}
                            if _proxy_cfg:
                                _launch_args2['proxy'] = _proxy_cfg
                            _browser2 = _pw2.chromium.launch(**_launch_args2)
                            try:
                                _ctx_args2 = {
                                    'viewport': {'width': VIEWPORT_W, 'height': VIEWPORT_H},
                                    'user_agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                                                   'AppleWebKit/537.36 (KHTML, like Gecko) '
                                                   'Chrome/120.0.0.0 Safari/537.36')
                                }
                                if storage_state:
                                    _ctx_args2['storage_state'] = storage_state
                                _ctx2  = _browser2.new_context(**_ctx_args2)
                                try:
                                    _ck2 = page.context.cookies()
                                    if _ck2:
                                        _ctx2.add_cookies(_ck2)
                                except Exception:
                                    pass
                                _page2 = _ctx2.new_page()
                                _dl2   = [None]
                                _page2.on('download', lambda d: _dl2.__setitem__(0, d))

                                try:
                                    _page2.goto(final_url, wait_until='domcontentloaded', timeout=20000)
                                except Exception:
                                    pass

                                _last2    = _page2.url
                                _urls2    = [_last2]
                                deadline2 = time.time() + 15
                                while time.time() < deadline2:
                                    try:
                                        _cur2 = _page2.url
                                        if _cur2 != _last2:
                                            _last2 = _cur2
                                            _urls2.append(_cur2)
                                        if _dl2[0]:
                                            break
                                    except Exception:
                                        break
                                    time.sleep(0.4)
                                if len(_urls2) > 1:
                                    logs.append({'nivel': 'info', 'msg': f'{label}: 🔀 Browser isolado ({modo}) nav: {" → ".join(u[:55] for u in _urls2)}'})

                                if _dl2[0]:
                                    _dp2 = tempfile.mktemp(suffix='.pdf')
                                    _dl2[0].save_as(_dp2)
                                    _sz2 = os.path.getsize(_dp2) // 1024
                                    logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ Browser isolado ({modo}) download: {_sz2}KB'})
                                    _result_holder[0] = _dp2
                                    return

                                _fu2 = _page2.url
                                try:
                                    _ct2i = _page2.evaluate('() => document.contentType || ""') or ''
                                    if 'pdf' in _ct2i.lower():
                                        _r2i = _ctx2.request.get(_fu2, timeout=20000)
                                        if _r2i.status == 200:
                                            _b2i = _r2i.body()
                                            if len(_b2i) > 1000:
                                                _dp2 = tempfile.mktemp(suffix='.pdf')
                                                with open(_dp2, 'wb') as _f2i:
                                                    _f2i.write(_b2i)
                                                _sz2 = len(_b2i) // 1024
                                                logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ Browser isolado ({modo}) PDF inline: {_sz2}KB'})
                                                _result_holder[0] = _dp2
                                                return
                                except Exception:
                                    pass

                                try:
                                    _r2r  = _ctx2.request.get(_fu2, timeout=20000, headers={'Referer': final_url})
                                    _ct2r = _r2r.headers.get('content-type', '').lower()
                                    _b2r  = _r2r.body()
                                    _sv2  = _salvar_corpo(_fu2, _ct2r, _b2r)
                                    if _sv2:
                                        logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ Browser isolado ({modo}) request: {_fu2[:60]}'})
                                        _result_holder[0] = _sv2
                                        return
                                except Exception:
                                    pass

                                # Tentar ler o que a página tem
                                try:
                                    _bt2 = _page2.inner_text('body') or ''
                                    _title2 = _page2.title() or ''
                                    _preview2 = _bt2[:150].replace('\n', ' ')
                                except Exception:
                                    _bt2 = ''
                                    _title2 = ''
                                    _preview2 = ''
                                _ct2_final = ''
                                try:
                                    _ct2_final = _page2.evaluate('() => document.contentType || ""') or ''
                                except Exception:
                                    pass
                                if _fu2 == final_url or 'ContadorAcesso' in _fu2:
                                    logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ Browser isolado ({modo}) preso em {_fu2[:60]} | ct={_ct2_final} | title={_title2[:40]} | body={repr(_preview2)}'})
                                else:
                                    logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ Browser isolado ({modo}) chegou em {_fu2[:60]} | ct={_ct2_final} | body={len(_bt2)}chars | title={_title2[:40]}'})
                            finally:
                                try:
                                    _browser2.close()
                                except Exception:
                                    pass
                    except Exception as _eg:
                        logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ Browser isolado ({modo}) erro: {str(_eg)[:70]}'})

                _t = _threading.Thread(target=_run_in_thread, daemon=True)
                _t.start()
                _t.join(timeout=90)
                return _result_holder[0]

            def _get_proxy_config_nav():
                """Reutiliza a mesma lógica de proxy do buscador."""
                import os as _os
                proxy_url = _os.getenv('PROXY_URL', '').strip()
                if proxy_url:
                    try:
                        from urllib.parse import urlparse as _up
                        p = _up(proxy_url)
                        cfg = {'server': f'{p.scheme}://{p.hostname}:{p.port}'}
                        if p.username: cfg['username'] = p.username
                        if p.password: cfg['password'] = p.password
                        return cfg
                    except Exception:
                        return None
                server = _os.getenv('PROXY_SERVER', '').strip()
                if server:
                    cfg = {'server': f'http://{server}'}
                    u = _os.getenv('PROXY_USER', '').strip()
                    pw = _os.getenv('PROXY_PASS', '').strip()
                    if u: cfg['username'] = u
                    if pw: cfg['password'] = pw
                    return cfg
                return None

            # G: browser isolado SEM proxy
            result_g = _tentar_browser_isolado(usar_proxy=False)
            if result_g:
                _remove_ctx_listeners()
                _cleanup_listeners()
                return f'Download: {result_g}' if result_g.endswith('.pdf') else f'Nova aba: {result_g}'

            # ═══════════════════════════════════════════════════════════════════
            # ESTRATÉGIA H — JS Fetch Hijack dentro do browser principal
            # Sobrescreve window.open para capturar o documento via fetch()
            # dentro do browser — mesma sessão, mesmo IP, sem popup.
            # ═══════════════════════════════════════════════════════════════════
            logs.append({'nivel': 'info', 'msg': f'{label}: 🔄 H — JS fetch hijack'})
            try:
                _fetch_result = page.evaluate('''async (url) => {
                    try {
                        const resp = await fetch(url, {
                            credentials: "include",
                            redirect: "follow",
                            headers: {"Referer": window.location.href}
                        });
                        const ct = resp.headers.get("content-type") || "";
                        const finalUrl = resp.url;
                        const buf = await resp.arrayBuffer();
                        const bytes = Array.from(new Uint8Array(buf));
                        return {ok: true, ct: ct, url: finalUrl, bytes: bytes, size: bytes.length};
                    } catch(e) {
                        return {ok: false, error: e.toString()};
                    }
                }''', final_url)

                if _fetch_result and _fetch_result.get('ok'):
                    _bytes_h  = bytes(_fetch_result['bytes'])
                    _ct_h     = _fetch_result.get('ct', '').lower()
                    _url_h    = _fetch_result.get('url', final_url)
                    _size_h   = _fetch_result.get('size', 0)
                    _is_pdf_h = ('pdf' in _ct_h or _url_h.lower().endswith('.pdf') or _bytes_h[:4] == b'%PDF')
                    if _size_h > 500 and _is_pdf_h:
                        _saved_h = _salvar_corpo(_url_h, _ct_h, _bytes_h)
                        if _saved_h:
                            logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ H — fetch hijack PDF: {_size_h//1024}KB'})
                            _remove_ctx_listeners()
                            _cleanup_listeners()
                            return f'Download: {_saved_h}'
                        else:
                            logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ H — fetch retornou PDF mas falhou ao salvar'})
                    elif _size_h > 500:
                        logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ H — fetch retornou {_size_h}b mas não é PDF (ct={_ct_h[:40]}) — descartado'})
                    else:
                        logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ H — fetch retornou vazio ({_size_h}b) | Causa: servidor exige sessão de popup'})
                else:
                    _err_h = (_fetch_result or {}).get('error', 'desconhecido')
                    logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ H falhou: {str(_err_h)[:80]} | Causa provável: CORS bloqueou o fetch cross-origin'})
            except Exception as e_h:
                logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ H erro: {str(e_h)[:80]}'})

            # ── G com proxy ──────────────────────────────────────────────────
            # Se G sem proxy falhou por IP mismatch, tentar com proxy (mesmo IP da sessão)
            result_gp = _tentar_browser_isolado(usar_proxy=True)
            if result_gp:
                _remove_ctx_listeners()
                _cleanup_listeners()
                return f'Download: {result_gp}' if result_gp.endswith('.pdf') else f'Nova aba: {result_gp}'

            # ═══════════════════════════════════════════════════════════════════
            # ESTRATÉGIA I — CDP Network Interception (nível mais baixo)
            # Chrome DevTools Protocol intercepta abaixo do Playwright e do proxy.
            # Captura a resposta antes de qualquer camada descartar.
            # ═══════════════════════════════════════════════════════════════════
            logs.append({'nivel': 'info', 'msg': f'{label}: 🔄 I — CDP interception'})
            try:
                _cdp_bodies  = []
                _cdp_session = page.context.new_cdp_session(page)
                
                _cdp_all_urls = []  # log de todas URLs que passaram pelo CDP
                def _on_cdp_paused(event):
                    try:
                        req_id  = event.get('requestId', '')
                        ct      = ''
                        status  = event.get('responseStatusCode', 0)
                        for h in event.get('responseHeaders', []):
                            if h.get('name', '').lower() == 'content-type':
                                ct = h.get('value', '').lower()
                        url_cdp = event.get('request', {}).get('url', '')
                        _cdp_all_urls.append(f'{status} {ct[:20]} {url_cdp[:60]}')
                        is_doc  = 'pdf' in ct or 'octet' in ct or url_cdp.lower().endswith('.pdf')
                        if is_doc:
                            logs.append({'nivel': 'info', 'msg': f'{label}: 🔍 I CDP: doc interceptado {ct[:25]} status={status} url={url_cdp[:70]}'})
                            try:
                                body_resp = _cdp_session.send('Fetch.getResponseBody', {'requestId': req_id})
                                b64_body  = body_resp.get('body', '')
                                is_b64    = body_resp.get('base64Encoded', False)
                                raw       = base64.b64decode(b64_body) if is_b64 else b64_body.encode()
                                logs.append({'nivel': 'info', 'msg': f'{label}: 🔍 I CDP: body={len(raw)}b b64={is_b64}'})
                                if len(raw) > 500:
                                    _cdp_bodies.append((url_cdp, ct, raw))
                            except Exception as _e_cdpb:
                                logs.append({'nivel': 'info', 'msg': f'{label}: 🔍 I CDP: getResponseBody falhou: {str(_e_cdpb)[:60]}'})
                        _cdp_session.send('Fetch.continueRequest', {'requestId': req_id})
                    except Exception:
                        pass

                _cdp_session.on('Fetch.requestPaused', _on_cdp_paused)
                _cdp_session.send('Fetch.enable', {
                    'patterns': [{'requestStage': 'Response', 'urlPattern': '*'}]
                })

                # Re-clicar com CDP ativo
                try:
                    el.click()
                except Exception:
                    pass
                time.sleep(6)

                _cdp_session.send('Fetch.disable')
                logs.append({'nivel': 'info', 'msg': f'{label}: 🔍 I CDP: {len(_cdp_all_urls)} requests interceptadas, {len(_cdp_bodies)} docs: {_cdp_all_urls[:8]}'})

                for _cu_i, _ct_i, _body_i in _cdp_bodies:
                    _saved_i = _salvar_corpo(_cu_i, _ct_i, _body_i)
                    if _saved_i:
                        logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ I — CDP capturou: {_cu_i[:60]}'})
                        _remove_ctx_listeners()
                        _cleanup_listeners()
                        return f'Download: {_saved_i}' if _saved_i.endswith('.pdf') else f'Nova aba: {_cu_i}'
                
                if not _cdp_bodies:
                    logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ I — CDP não capturou nada | Causa provável: proxy processa respostas antes do CDP, body já consumido'})

            except Exception as e_i:
                logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ I erro: {str(e_i)[:80]} | Causa: CDP não suportado neste contexto de proxy'})

            # ═══════════════════════════════════════════════════════════════════
            # ESTRATÉGIA J — Storage state completo + browser isolado
            # Exporta cookies + localStorage + sessionStorage do contexto principal.
            # Mais fiel que só cookies — tokens de autenticação em storage são incluídos.
            # ═══════════════════════════════════════════════════════════════════
            logs.append({'nivel': 'info', 'msg': f'{label}: 🔄 J — storage state completo'})
            try:
                import tempfile as _tmpf_j, json as _json_j
                _ss_path = _tmpf_j.mktemp(suffix='.json')
                _storage_state = None
                try:
                    page.context.storage_state(path=_ss_path)
                    with open(_ss_path, 'r') as _f_ss:
                        _storage_state = _json_j.load(_f_ss)
                    logs.append({'nivel': 'info', 'msg': f'{label}: 📦 J — storage exportado: {len(str(_storage_state))} chars'})
                except Exception as _e_ss:
                    logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ J — storage export falhou: {str(_e_ss)[:60]}'})

                if _storage_state:
                    # Tentar sem proxy primeiro (mais rápido)
                    result_j = _tentar_browser_isolado(usar_proxy=False, storage_state=_storage_state)
                    if result_j:
                        _remove_ctx_listeners()
                        _cleanup_listeners()
                        return f'Download: {result_j}' if result_j.endswith('.pdf') else f'Nova aba: {result_j}'
                    # Tentar com proxy (cobre IP mismatch)
                    result_jp = _tentar_browser_isolado(usar_proxy=True, storage_state=_storage_state)
                    if result_jp:
                        _remove_ctx_listeners()
                        _cleanup_listeners()
                        return f'Download: {result_jp}' if result_jp.endswith('.pdf') else f'Nova aba: {result_jp}'
                    logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ J — browser com storage state não chegou ao documento | Causa: servidor provavelmente exige que o popup seja aberto pela janela pai específica (window.opener validation)'})
            except Exception as e_j:
                logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ J erro: {str(e_j)[:80]}'})

            # Tudo falhou
            logs.append({'nivel': 'aviso', 'msg': f'{label}: ⚠️ Todas as estratégias (A½→J) falharam | URL disponível para inspeção manual: {final_url[:100]}'})
            _remove_ctx_listeners()
            _cleanup_listeners()
            return f'Nova aba: {final_url}'
        else:
            el.click()
        
        logs.append({'nivel': 'info', 'msg': f'{label}: 🖱️ Clicou em "{texto[:60]}" (via {estrategia_usada})'})
    except Exception as e_click:
        err_msg = str(e_click)[:60]
        logs.append({'nivel': 'info', 'msg': f'{label}: ⚠️ Clique falhou: {err_msg}'})
        
        # Fallback 1: se temos href e clique falhou, navegar direto
        if href_extraido and href_extraido.startswith('http'):
            logs.append({'nivel': 'info', 'msg': f'{label}: 🔄 Tentando navegar direto para href...'})
            try:
                page.goto(href_extraido, wait_until='networkidle', timeout=15000)
                time.sleep(2)
                logs.append({'nivel': 'ok', 'msg': f'{label}: 🖱️ Navegou direto para: {page.url[:60]}'})
                try:
                    page.remove_listener('download', _on_download)
                except Exception:
                    pass
                return f'Navegou: {page.url}'
            except Exception as e_nav:
                logs.append({'nivel': 'info', 'msg': f'{label}: ⚠️ Navegação direta falhou: {str(e_nav)[:40]}'})
        
        try:
            page.remove_listener('download', _on_download)
        except Exception:
            pass
        return f'Erro ao clicar: {err_msg}'
    
    time.sleep(3)
    
    try:
        page.remove_listener('download', _on_download)
    except Exception:
        pass
    
    # Cleanup response interceptor
    try:
        page.remove_listener('response', _on_response)
    except Exception:
        pass
    
    # Download?
    if _download_obj:
        try:
            dl_path = tempfile.mktemp(suffix='.pdf')
            _download_obj.save_as(dl_path)
            dl_size = os.path.getsize(dl_path) // 1024
            logs.append({'nivel': 'ok', 'msg': f'{label}: 📥 Download: {_download_obj.suggested_filename} ({dl_size}KB)'})
            return f'Download: {dl_path}'
        except Exception as e_dl:
            logs.append({'nivel': 'info', 'msg': f'{label}: ⚠️ Download falhou: {str(e_dl)[:40]}'})
    
    # Nova aba?
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
        # Browser pode ter morrido — preferir href com slug completo se disponivel
        _interceptada = _nav_url_real[0] or _pdf_response_url[0]
        _href_ok = href_extraido if href_extraido and href_extraido.startswith('http') else None
        _real_url = (_href_ok if _href_ok and _interceptada and len(_href_ok) > len(_interceptada) else _interceptada) or _href_ok
        if _real_url:
            logs.append({'nivel': 'info', 'msg': f'{label}: 🔄 Browser morreu — URL real interceptada: {_real_url[:80]}'})
            return f'Navegou: {_real_url}'
    
    # URL mudou?
    try:
        page.wait_for_load_state('networkidle', timeout=5000)
        if page.url != url_antes:
            logs.append({'nivel': 'info', 'msg': f'{label}: 🖱️ Navegou para: {page.url[:60]}'})
    except Exception:
        _interceptada = _nav_url_real[0] or _pdf_response_url[0]
        _href_ok = href_extraido if href_extraido and href_extraido.startswith('http') else None
        _real_url = (_href_ok if _href_ok and _interceptada and len(_href_ok) > len(_interceptada) else _interceptada) or _href_ok
        if _real_url:
            logs.append({'nivel': 'info', 'msg': f'{label}: 🔄 Browser morreu — URL real interceptada: {_real_url[:80]}'})
            return f'Navegou: {_real_url}'
            return f'Navegou: {_real_url}'
    
    # Cleanup response listener
    try:
        page.remove_listener('response', _on_response)
    except Exception:
        pass
    
    # Se interceptou URL de PDF, retornar
    if _pdf_response_url[0]:
        logs.append({'nivel': 'info', 'msg': f'{label}: 📄 PDF detectado via response: {_pdf_response_url[0][:80]}'})
        return f'Navegou: {_pdf_response_url[0]}'
    
    # Se href é página /a/ LeisMunicipais com slug — retornar direto como Href:
    if (href_extraido and href_extraido.startswith("http") and
            "leismunicipais.com.br/a/" in href_extraido):
        logs.append({"nivel": "info", "msg": f"{label}: Href LM com slug — retornando direto: {href_extraido[:100]}"})
        return f"Href: {href_extraido}"
    # Se interceptou navegação real, retornar — preferir href com slug completo
    if _nav_url_real[0]:
        _nav_best = _nav_url_real[0]
        _href_slug = href_extraido or ""
        logs.append({'nivel': 'info', 'msg': f'{label}: [DBG] nav_real={_nav_url_real[0][:70]} | href={_href_slug[:70]}'})
        if (_href_slug and _href_slug.startswith("http") and
                _href_slug.startswith(_nav_url_real[0]) and
                len(_href_slug) > len(_nav_url_real[0])):
            _nav_best = _href_slug
        elif _href_slug and _href_slug.startswith("http") and len(_href_slug) > len(_nav_url_real[0]):
            # Tentar mesmo sem startswith (slug pode diferir apenas no sufixo)
            _base = _nav_url_real[0].rstrip("/")
            if _href_slug.startswith(_base + "/"):
                _nav_best = _href_slug
        logs.append({'nivel': 'info', 'msg': f'{label}: 🔗 Navegação real interceptada: {_nav_best[:80]}'})
        return f'Navegou: {_nav_best}'
    
    # Fallback: href estático
    if href_extraido and href_extraido.startswith('http'):
        return f'Href: {href_extraido}'
    
    return f'Clicou em "{texto[:30]}"'


def _digitar_por_label(page, label_campo: str, texto: str, logs: list, label: str) -> str:
    """Encontra campo pelo label e digita texto."""
    
    if not label_campo or not texto:
        return 'Erro: label ou texto vazio'
    
    el = None
    
    # Estrategias para encontrar o campo
    finders = [
        lambda: page.get_by_label(label_campo).first,
        lambda: page.get_by_placeholder(label_campo).first,
    ]
    
    # Frames
    for frame in page.frames:
        if frame != page.main_frame:
            finders.append(lambda f=frame: f.get_by_label(label_campo).first)
            finders.append(lambda f=frame: f.get_by_placeholder(label_campo).first)
    
    for finder in finders:
        try:
            candidate = finder()
            if candidate and candidate.is_visible():
                el = candidate
                break
        except Exception:
            continue
    
    # Fallback: input proximo ao texto do label
    if not el:
        try:
            label_el = page.get_by_text(label_campo).first
            if label_el:
                nearby = label_el.locator('xpath=following::input[1] | following::select[1] | following::textarea[1]').first
                if nearby and nearby.is_visible():
                    el = nearby
        except Exception:
            pass
    
    # Fallback 2: buscar por id/name que contenha palavras do label
    if not el:
        try:
            import re as _re
            palavras = [p for p in label_campo.lower().split() if len(p) > 2]
            for inp in page.query_selector_all('input:not([type="hidden"]):not([type="submit"]), select, textarea'):
                cid = (inp.get_attribute('id') or '').lower()
                cname = (inp.get_attribute('name') or '').lower()
                if any(p in cid or p in cname for p in palavras):
                    el = inp
                    break
        except Exception:
            pass
    
    if not el:
        logs.append({'nivel': 'info', 'msg': f'{label}: ⚠️ Campo "{label_campo[:30]}" não encontrado'})
        return f'Campo não encontrado: "{label_campo[:30]}"'
    
    try:
        el.click()
        time.sleep(0.3)
        page.keyboard.press('Control+a')
        time.sleep(0.1)
        page.keyboard.press('Backspace')
        time.sleep(0.1)
        page.keyboard.type(texto, delay=30)
        time.sleep(0.5)
        logs.append({'nivel': 'info', 'msg': f'{label}: ✏️ Digitou "{texto[:60]}" em "{label_campo[:40]}"'})
        return f'Digitou "{texto[:20]}" em "{label_campo[:20]}"'
    except Exception as e:
        logs.append({'nivel': 'info', 'msg': f'{label}: ⚠️ Erro ao digitar: {str(e)[:40]}'})
        return f'Erro: {str(e)[:40]}'



def _executar_acao(page, acao: dict, logs: list, label: str) -> str:
    """Executa acao: clicar por texto, digitar por label, preencher formulario, scroll."""

    tipo = acao.get('tipo', '')

    if tipo == 'clicar':
        texto_el = acao.get('texto_elemento', '') or ''
        return _clicar_por_texto(page, texto_el, logs, label)

    elif tipo == 'digitar':
        label_campo = acao.get('label_campo', '') or acao.get('texto_elemento', '') or ''
        texto = acao.get('texto', '') or ''
        return _digitar_por_label(page, label_campo, texto, logs, label)

    elif tipo == 'preencher_formulario':
        campos = acao.get('campos', []) or []
        
        if not campos:
            logs.append({'nivel': 'aviso', 'msg': f'{label}: ⚠️ preencher_formulario sem campos — IA não enviou lista de campos'})
        
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
            
            # 2. Buscar select/input por id, name, ou placeholder que contenha o label
            if not el:
                try:
                    all_els = frame.query_selector_all('select, input:not([type="hidden"]):not([type="submit"]), textarea')
                    label_lower = label_campo.lower()
                    for candidate in all_els:
                        cid = (candidate.get_attribute('id') or '').lower()
                        cname = (candidate.get_attribute('name') or '').lower()
                        cplaceholder = (candidate.get_attribute('placeholder') or '').lower()
                        
                        # Checar se algum atributo contém palavras do label
                        palavras = [p for p in label_lower.split() if len(p) > 2]
                        match = any(p in cid or p in cname or p in cplaceholder for p in palavras)
                        if match:
                            el = candidate
                            break
                except Exception:
                    pass
            
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
                            el.select_option(value=opt_val, timeout=5000)
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
                        # Normalizar formato: aceitar DD/MM/YYYY ou MM/DD/YYYY
                        import re as _re
                        m = _re.match(r'(\d{2})/(\d{2})/(\d{4})', valor)
                        if m:
                            p1, p2, p3 = m.group(1), m.group(2), m.group(3)
                            # Se p1 > 12, é DD/MM/YYYY com certeza
                            # Se p2 > 12, é MM/DD/YYYY com certeza
                            # Se ambos <= 12, assumir DD/MM/YYYY (padrão BR)
                            if int(p1) > 12:
                                # DD/MM/YYYY
                                iso_val = f'{p3}-{p2}-{p1}'
                            elif int(p2) > 12:
                                # MM/DD/YYYY -> converter
                                iso_val = f'{p3}-{p1}-{p2}'
                                logs.append({'nivel': 'info', 'msg': f'{label}: 📅 Data convertida de MM/DD para DD/MM: {valor}'})
                            else:
                                # Ambos <= 12: assumir DD/MM/YYYY
                                iso_val = f'{p3}-{p2}-{p1}'
                        else:
                            iso_val = valor
                        try:
                            el.fill(iso_val)
                        except Exception:
                            # Fallback: setar via JavaScript
                            el.evaluate('(el, val) => { el.value = val; el.dispatchEvent(new Event("change")); }', iso_val)
                        logs.append({'nivel': 'info', 'msg': f'{label}: 📅 {label_campo} = "{valor}" (ISO: {iso_val})'})
                    else:
                        try:
                            el.fill(valor)
                        except Exception:
                            # Fallback: limpar e digitar
                            el.click(timeout=5000)
                            page.keyboard.press('Control+a')
                            page.keyboard.type(valor, delay=30)
                        logs.append({'nivel': 'info', 'msg': f'{label}: ✏️ {label_campo} = "{valor}"'})
                    
                    preenchidos.append(label_campo)
            
            except Exception as e_fill:
                logs.append({'nivel': 'info', 'msg': f'{label}: ⚠️ Erro preenchendo {label_campo}: {str(e_fill)[:40]}'})
        
        # Clicar no botão de submit
        botao_raw = acao.get('botao_submit', '') or ''
        btn = None
        
        # Se botao_submit veio como dict com coordenadas, clicar visualmente
        if isinstance(botao_raw, dict) and 'x' in botao_raw and 'y' in botao_raw:
            bx = int(botao_raw['x']) * VIEWPORT_W // 1000
            by = int(botao_raw['y']) * VIEWPORT_H // 1000
            page.mouse.click(bx, by)
            time.sleep(3)
            try:
                page.wait_for_load_state('networkidle', timeout=15000)
            except Exception:
                pass
            logs.append({'nivel': 'info', 'msg': f'{label}: 🖱️ Clicou botão em ({bx}, {by})'})
            return f'Formulário preenchido ({len(preenchidos)} campos) e submetido'
        
        botao = str(botao_raw).strip() if botao_raw else ''
        
        try:
            # 1. Buscar por texto do botão
            if botao:
                for b in frame.query_selector_all('button, input[type="submit"], input[type="button"]'):
                    b_text = (b.text_content() or b.get_attribute('value') or '').strip()
                    if botao.lower() in b_text.lower():
                        btn = b
                        break
                # 2. Buscar por id/name/value
                if not btn:
                    try:
                        btn = frame.query_selector(f'#{botao}') or frame.query_selector(f'[name="{botao}"]') or frame.query_selector(f'[value*="{botao}" i]')
                    except Exception:
                        pass
            
            # 3. Qualquer submit
            if not btn:
                btn = frame.query_selector('input[type="submit"]') or frame.query_selector('button[type="submit"]')
            
            # 4. Qualquer botão com texto comum
            if not btn:
                for b in frame.query_selector_all('button, input[type="button"]'):
                    b_text = (b.text_content() or b.get_attribute('value') or '').strip().lower()
                    if b_text in ['ok', 'buscar', 'consultar', 'pesquisar', 'enviar', 'submit', 'go']:
                        btn = b
                        break
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
            
            btn_text = ''
            try:
                btn_text = (btn.text_content() or btn.get_attribute('value') or '')[:20]
            except Exception:
                pass
            logs.append({'nivel': 'info', 'msg': f'{label}: 🖱️ Clicou: {btn_text or botao}'})
        else:
            # Último recurso: Enter
            logs.append({'nivel': 'info', 'msg': f'{label}: ⚠️ Botão não encontrado — tentando Enter'})
            page.keyboard.press('Enter')
            time.sleep(3)
            try:
                page.wait_for_load_state('networkidle', timeout=15000)
            except Exception:
                pass
        
        return f'Formulário preenchido ({len(preenchidos)} campos) e submetido'


    elif tipo == 'scroll':
        amount = 500 if (acao.get('direcao', 'baixo') or 'baixo') == 'baixo' else -500
        page.mouse.wheel(0, amount)
        time.sleep(1)
        direcao = acao.get('direcao', 'baixo') or 'baixo'
        logs.append({'nivel': 'info', 'msg': f'{label}: 📜 Scroll {direcao}'})
        return f'Scroll {direcao}'

    elif tipo == 'concluido':
        return 'Legislacao encontrada'

    elif tipo == 'desistir':
        return 'Desistiu'

    else:
        return f'Acao desconhecida: {tipo}'



def _montar_prompt_html(legislacao: dict, historico: list, passo: int, url_atual: str, html_resumo: str) -> str:
    """Prompt para navegação via HTML (FlareSolverr) — sem screenshot."""
    tipo = legislacao.get('tipo', 'Lei Complementar')
    numero = legislacao.get('numero', '')
    ano = legislacao.get('ano', '')
    municipio = legislacao.get('municipio', '')
    historico_txt = ''
    if historico:
        historico_txt = '\n'.join([
            f"  Passo {h['passo']}: {h['acao']} -> {h['resultado']}"
            for h in historico[-5:]
        ])
        historico_txt = f"\n\nHISTORICO:\n{historico_txt}"
    return f"""Voce esta navegando via HTML para encontrar: {tipo} nº {numero}/{ano} — {municipio}.
URL ATUAL: {url_atual} | PASSO: {passo}{historico_txt}

HTML RESUMIDO DA PAGINA:
{html_resumo[:8000]}

Analise o HTML e decida a proxima acao para encontrar a legislacao correta.
REGRAS:
1. PRIORIDADE MAXIMA: Se a pagina tiver campo de busca/pesquisa, USE-O imediatamente — preencha com tipo, numero e municipio da legislacao e submeta. Nao navegue por links genericos quando houver campo de busca disponivel.
2. Procure links que contenham o tipo e numero da legislacao nos resultados.
3. Se houver paginacao e nao encontrou, va para proxima pagina.
4. Quando encontrar o link correto, retorne a URL completa.
5. Se a pagina mostra o texto da lei diretamente, marque como concluido.
6. NUNCA invente URLs — use apenas URLs que aparecem no HTML.
7. Se ja tentou a mesma URL mais de uma vez sem progresso, mude de estrategia — nao repita a mesma navegacao.

Responda APENAS com JSON:
{{
    "o_que_vejo": "descricao breve do conteudo da pagina",
    "decisao": "o que vou fazer e por que",
    "acao": {{
        "tipo": "navegar|concluido|desistir",
        "url": "URL completa para navegar (so para navegar)",
        "motivo": "porque escolheu esta URL"
    }},
    "legislacao_encontrada": {{
        "encontrada": false,
        "url": "",
        "confirmacao": ""
    }}
}}"""


def _chamar_gemini_texto(prompt: str, logs: list, label: str) -> str:
    """Chama Gemini com texto puro (sem imagem)."""
    import google.generativeai as genai
    api_key = os.getenv('GEMINI_API_KEY', '')
    if not api_key:
        logs.append({'nivel': 'erro', 'msg': f'{label}: GEMINI_API_KEY nao configurada'})
        return ''
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel('gemini-2.5-flash')
    for tentativa in range(3):
        if tentativa > 0:
            time.sleep(tentativa * 3)
        try:
            response = model.generate_content(prompt)
            return response.text.strip()
        except Exception as e:
            err = str(e)[:120]
            logs.append({'nivel': 'aviso', 'msg': f'{label}: Gemini texto falhou: {err}'})
            if '429' not in err and 'quota' not in err.lower():
                break
    return ''


def _extrair_html_resumido(html: str) -> str:
    """Extrai texto relevante do HTML removendo scripts, estilos e tags desnecessarias."""
    import re as _re
    # Remover scripts e estilos
    html = _re.sub(r'<script[^>]*>[\s\S]*?</script>', '', html, flags=_re.IGNORECASE)
    html = _re.sub(r'<style[^>]*>[\s\S]*?</style>', '', html, flags=_re.IGNORECASE)
    # Preservar href dos links — substituir <a href="URL">TEXTO</a> por [TEXTO](URL)
    html = _re.sub(r'<a[^>]*href=["\']([^"\']+)["\'][^>]*>([\s\S]*?)</a>',
                   lambda m: '[' + _re.sub('<[^>]+>', '', m.group(2)).strip() + '](' + m.group(1) + ')',
                   html, flags=_re.IGNORECASE)
    # Remover todas as outras tags
    html = _re.sub(r'<[^>]+>', ' ', html)
    # Limpar espacos
    html = _re.sub(r'\s+', ' ', html).strip()
    return html


def navegar_via_flaresolverr(
    url_inicial: str,
    legislacao: dict,
    logs: list,
    label: str = '',
    max_passos: int = 15
) -> dict:
    """
    Navega um site usando FlareSolverr (bypass Cloudflare) + Gemini analisa HTML.
    Alternativa ao Playwright quando Cloudflare bloqueia.
    """
    import requests as _req
    import re as _re
    import json as _json

    resultado = {
        'encontrada': False,
        'url': '',
        'status': '',
        'confirmacao': '',
        'pdf_path': None,
    }
    historico = []
    url_atual = url_inicial

    def _flare_get(url):
        try:
            r = _req.post('http://localhost:8191/v1',
                json={'cmd': 'request.get', 'url': url, 'maxTimeout': 60000},
                timeout=70)
            d = r.json()
            if d.get('status') == 'ok':
                sol = d.get('solution', {})
                return sol.get('status', 0), sol.get('response', ''), sol.get('url', url)
        except Exception as e:
            logs.append({'nivel': 'aviso', 'msg': f'{label}: FlareSolverr erro: {str(e)[:80]}'})
        return 0, '', url

    logs.append({'nivel': 'info', 'msg': f'{label}: 🌐 Iniciando navegação via FlareSolverr: {url_inicial[:80]}'})

    for passo in range(1, max_passos + 1):
        # 1. Buscar página via FlareSolverr
        status, html, url_real = _flare_get(url_atual)
        if status != 200 or len(html) < 200:
            logs.append({'nivel': 'aviso', 'msg': f'{label}: Passo {passo}: FlareSolverr retornou status={status} len={len(html)}'})
            break
        url_atual = url_real
        logs.append({'nivel': 'info', 'msg': f'{label}: Passo {passo}: HTML obtido ({len(html)} chars) de {url_atual[:60]}'})

        # 2. Resumir HTML para o Gemini
        html_resumido = _extrair_html_resumido(html)

        # 3. Montar prompt e chamar Gemini
        prompt = _montar_prompt_html(legislacao, historico, passo, url_atual, html_resumido)
        resp = _chamar_gemini_texto(prompt, logs, f'{label} passo {passo}')
        if not resp:
            logs.append({'nivel': 'aviso', 'msg': f'{label}: Passo {passo}: Gemini sem resposta'})
            break

        # 4. Parsear JSON
        decisao = None
        try:
            resp_clean = resp.strip()
            if '```' in resp_clean:
                m = _re.search(r'```(?:json)?\s*\n?([\s\S]*?)\n?\s*```', resp_clean)
                resp_clean = m.group(1).strip() if m else resp_clean.replace('```', '')
            decisao = _json.loads(resp_clean)
        except Exception:
            brace_count = 0
            start = None
            for i, ch in enumerate(resp):
                if ch == '{':
                    if brace_count == 0: start = i
                    brace_count += 1
                elif ch == '}':
                    brace_count -= 1
                    if brace_count == 0 and start is not None:
                        try:
                            decisao = _json.loads(resp[start:i+1])
                        except Exception:
                            pass
                        break

        if not decisao:
            logs.append({'nivel': 'aviso', 'msg': f'{label}: Passo {passo}: JSON invalido'})
            break

        o_que_vejo = decisao.get('o_que_vejo', '')
        acao = decisao.get('acao', {}) or {}
        tipo_acao = acao.get('tipo', '')
        logs.append({'nivel': 'info', 'msg': f'{label}: 👁️ Passo {passo}: {o_que_vejo[:200]}'})
        logs.append({'nivel': 'info', 'msg': f'{label}: 🧠 Decisão: {tipo_acao} — {decisao.get("decisao","")[:200]}'})

        # 5. Legislação encontrada?
        leg = decisao.get('legislacao_encontrada', {}) or {}
        leg_url = (leg.get('url', '') or '').strip()
        if leg.get('encontrada') and leg_url and leg_url.startswith('http'):
            resultado['encontrada'] = True
            resultado['url'] = leg_url
            resultado['confirmacao'] = leg.get('confirmacao', '')
            logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ Encontrada via FlareSolverr! {leg_url[:80]}'})
            break

        # 6. Executar ação
        if tipo_acao == 'concluido':
            if leg_url and leg_url.startswith('http'):
                resultado['encontrada'] = True
                resultado['url'] = leg_url
                logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ Concluído: {leg_url[:80]}'})
            break
        elif tipo_acao == 'desistir':
            pensamento = decisao.get('decisao', '')
            logs.append({'nivel': 'aviso', 'msg': f'{label}: ❌ Gemini desistiu na passo {passo}'})
            historico.append({'passo': passo, 'acao': 'desistir', 'resultado': pensamento[:100]})
            # Antes de desistir: verificar se URL já foi capturada
            if resultado.get('url') and resultado['url'].startswith('http'):
                resultado['encontrada'] = True
                resultado['confirmacao'] = resultado.get('confirmacao', '') or 'URL capturada antes da desistencia'
                logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ IA desistiu mas URL ja capturada: {resultado["url"][:80]}'})
            break
        elif tipo_acao == 'navegar':
            prox_url = (acao.get('url', '') or '').strip()
            if not prox_url or not prox_url.startswith('http'):
                logs.append({'nivel': 'aviso', 'msg': f'{label}: URL inválida para navegar: {prox_url[:60]}'})
                break
            if prox_url == url_atual:
                logs.append({'nivel': 'aviso', 'msg': f'{label}: Loop detectado — mesma URL'})
                break
            historico.append({'passo': passo, 'acao': f'navegar', 'resultado': prox_url[:80]})
            logs.append({'nivel': 'info', 'msg': f'{label}: 🔗 Navegando para: {prox_url[:80]}'})
            url_atual = prox_url
        else:
            logs.append({'nivel': 'aviso', 'msg': f'{label}: Ação desconhecida: {tipo_acao}'})
            break

    return resultado


def navegar_com_cookies_flaresolverr(
    url_inicial: str,
    legislacao: dict,
    logs: list,
    label: str = "",
    chamar_llm=None,
    executable_path: str = None,
    max_passos: int = 20
) -> dict:
    import requests as _req
    from playwright.sync_api import sync_playwright

    resultado = {"encontrada": False, "url": "", "status": "", "confirmacao": "", "pdf_path": None}

    logs.append({"nivel": "info", "msg": f"{label}: Obtendo cookies via FlareSolverr: {url_inicial[:80]}"})
    try:
        # Usar sessao persistente para acumular historico e melhorar score reCAPTCHA
        _fs_session_id = os.environ.get("FLARESOLVERR_SESSION", "")
        _fs_payload = {"cmd": "request.get", "url": url_inicial, "maxTimeout": 60000}
        if _fs_session_id:
            _fs_payload["session"] = _fs_session_id
        r = _req.post("http://localhost:8191/v1",
            json=_fs_payload,
            timeout=70)
        d = r.json()
        if d.get("status") != "ok":
            logs.append({"nivel": "aviso", "msg": f"{label}: FlareSolverr falhou"})
            return resultado
        sol = d.get("solution", {})
        fs_cookies = sol.get("cookies", [])
        fs_user_agent = sol.get("userAgent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        logs.append({"nivel": "ok", "msg": f"{label}: FlareSolverr: {len(fs_cookies)} cookies obtidos"})
    except Exception as e:
        logs.append({"nivel": "aviso", "msg": f"{label}: FlareSolverr erro: {str(e)[:80]}"})
        return resultado

    if not fs_cookies:
        logs.append({"nivel": "aviso", "msg": f"{label}: Nenhum cookie obtido"})
        return resultado

    llm_func = chamar_llm or _chamar_gemini_visao

    if not executable_path:
        for _p in ["/usr/bin/chromium-browser", "/usr/bin/chromium", "/usr/bin/google-chrome"]:
            if os.path.exists(_p):
                executable_path = _p
                break
        if not executable_path:
            import glob as _glob
            _nix = _glob.glob("/root/.cache/ms-playwright/chromium-*/chrome-linux/chrome")
            if _nix:
                executable_path = _nix[0]

    launch_args = {
        "headless": True,
        "args": ["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu", "--single-process", "--no-zygote"]
    }
    if executable_path:
        launch_args["executable_path"] = executable_path

    try:
        with sync_playwright() as pw:
            try:
                browser = pw.chromium.launch(**launch_args)
            except Exception:
                if "executable_path" in launch_args:
                    del launch_args["executable_path"]
                browser = pw.chromium.launch(**launch_args)

            ctx = browser.new_context(
                viewport={"width": 1280, "height": 900},
                user_agent=fs_user_agent,
                accept_downloads=True
            )

            try:
                pw_cookies = []
                for c in fs_cookies:
                    pw_cookie = {
                        "name": c.get("name", ""),
                        "value": c.get("value", ""),
                        "domain": c.get("domain", ".leismunicipais.com.br"),
                        "path": c.get("path", "/"),
                    }
                    if c.get("expires"):
                        pw_cookie["expires"] = int(c["expires"])
                    if c.get("httpOnly") is not None:
                        pw_cookie["httpOnly"] = c["httpOnly"]
                    if c.get("secure") is not None:
                        pw_cookie["secure"] = c["secure"]
                    pw_cookies.append(pw_cookie)
                ctx.add_cookies(pw_cookies)
                logs.append({"nivel": "info", "msg": f"{label}: {len(pw_cookies)} cookies injetados no Playwright"})
            except Exception as e_ck:
                logs.append({"nivel": "aviso", "msg": f"{label}: Erro ao injetar cookies: {str(e_ck)[:80]}"})

            page = ctx.new_page()
            # Bloquear anuncios e trackers para evitar sobreposicao de elementos
            _ad_domains = [
                'doubleclick.net', 'googlesyndication.com', 'googletagmanager.com',
                'googletagservices.com', 'adtrafficquality.google', 'adservice.google.com',
                'amazon-adsystem.com', 'ads.yahoo.com', 'outbrain.com', 'taboola.com',
                'criteo.com', 'pubmatic.com', 'rubiconproject.com', 'openx.net',
                'adnxs.com', 'moatads.com', 'viralize.tv', 'sodar', 'pagead2.googlesyndication',
                'tpc.googlesyndication', 'securepubads.g.doubleclick'
            ]
            def _bloquear_ads(route, request):
                url_req = request.url.lower()
                if any(d in url_req for d in _ad_domains):
                    route.abort()
                else:
                    route.continue_()
            page.route('**/*', _bloquear_ads)
            try:
                from playwright_stealth import stealth_sync
                stealth_sync(page)
            except Exception:
                try:
                    page.add_init_script("""
                        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                        Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
                        Object.defineProperty(navigator, 'languages', { get: () => ['pt-BR', 'pt', 'en-US', 'en'] });
                        window.chrome = { runtime: {} };
                    """)
                except Exception:
                    pass

            logs.append({"nivel": "info", "msg": f"{label}: Abrindo {url_inicial[:80]} com cookies do FlareSolverr"})
            try:
                page.goto(url_inicial, wait_until="networkidle", timeout=20000)
            except Exception:
                try:
                    page.goto(url_inicial, wait_until="domcontentloaded", timeout=15000)
                except Exception as e_goto:
                    logs.append({"nivel": "aviso", "msg": f"{label}: Erro ao abrir pagina: {str(e_goto)[:80]}"})
                    browser.close()
                    return resultado

            import time as _time
            _time.sleep(2)
            titulo = page.title()
            if "just a moment" in titulo.lower() or "cloudflare" in titulo.lower():
                logs.append({"nivel": "aviso", "msg": f"{label}: Ainda bloqueado pelo Cloudflare apos cookies"})
                browser.close()
                return resultado

            logs.append({"nivel": "ok", "msg": f"{label}: Pagina aberta: {titulo[:60]}"})

            # Login no LeisMunicipais se for esse site
            if 'leismunicipais.com.br' in url_inicial:
                try:
                    import time as _tl
                    logs.append({'nivel': 'info', 'msg': f'{label}: Fazendo login no LeisMunicipais...'})
                    page.goto('https://leismunicipais.com.br/login', wait_until='domcontentloaded', timeout=20000)
                    _tl.sleep(2)
                    page.fill('input[type=email], input[name=email], input[id*=email]', 'sistemaurbanlex@gmail.com')
                    page.fill('input[type=password], input[name=password], input[name=senha]', '04leismunicipais04')
                    page.click('button[type=submit], input[type=submit], button:has-text("Entrar"), button:has-text("Login")')
                    _tl.sleep(3)
                    titulo_login = page.title()
                    logs.append({'nivel': 'ok', 'msg': f'{label}: Login realizado: {titulo_login[:60]}'})
                    page.goto(url_inicial, wait_until='domcontentloaded', timeout=20000)
                    _tl.sleep(2)
                    try:
                        page.evaluate("""() => { document.querySelectorAll('[class*=popup],[class*=modal],[class*=overlay],[class*=banner],[id*=popup],[id*=modal]').forEach(e => e.remove()); }""")
                    except Exception:
                        pass
                except Exception as e_login:
                    logs.append({'nivel': 'aviso', 'msg': f'{label}: Erro no login: {str(e_login)[:80]}'})
            resultado = navegar_como_humano(
                page=page,
                frame=page,
                legislacao=legislacao,
                chamar_llm=llm_func,
                logs=logs,
                label=label,
                max_passos=max_passos
            )
            if resultado.get('encontrada') and resultado.get('url'):
                try:
                    import time as _t2
                    _lei_url = resultado['url']
                    # Tentar FlareSolverr diretamente na URL da lei antes do Playwright
                    # Login primeiro para evitar reCAPTCHA
                    _fs_html_lei = ''
                    try:
                        import requests as _req_fs
                        _fs_sid = os.environ.get('FLARESOLVERR_SESSION', '')
                        if _fs_sid and 'leismunicipais' in _lei_url:
                            try:
                                _req_fs.post('http://localhost:8191/v1', json={
                                    'cmd': 'request.get', 'url': 'https://leismunicipais.com.br/login',
                                    'session': _fs_sid, 'maxTimeout': 20000
                                }, timeout=30)
                                _req_fs.post('http://localhost:8191/v1', json={
                                    'cmd': 'request.post', 'url': 'https://leismunicipais.com.br/login',
                                    'session': _fs_sid, 'maxTimeout': 20000,
                                    'postData': 'username=sistemaurbanlex%40gmail.com&password=04leismunicipais04&returnto=',
                                    'headers': {'Content-Type': 'application/x-www-form-urlencoded',
                                                'Referer': 'https://leismunicipais.com.br/login',
                                                'Origin': 'https://leismunicipais.com.br'}
                                }, timeout=30)
                                logs.append({'nivel': 'info', 'msg': f'{label}: [FS] Login LeisMunicipais OK'})
                            except Exception as _e_login:
                                logs.append({'nivel': 'aviso', 'msg': f'{label}: [FS] Login falhou: {str(_e_login)[:60]}'})
                        logs.append({'nivel': 'info', 'msg': f'{label}: [FS] Tentando FlareSolverr direto na URL da lei...'})
                        _fs_payload = {'cmd': 'request.get', 'url': _lei_url, 'maxTimeout': 90000, 'waitTime': 15000}
                        if _fs_sid:
                            _fs_payload['session'] = _fs_sid
                        _r_fs = _req_fs.post('http://localhost:8191/v1',
                            json=_fs_payload,
                            timeout=120)
                        _d_fs = _r_fs.json()
                        if _d_fs.get('status') == 'ok':
                            _fs_html_lei = _d_fs.get('solution', {}).get('response', '')
                            _lm_loading_kws2 = ['norma requisitada est', 'Por favor, aguarde', 'sendo carregada', 'just a moment']
                            _fs_loading = any(s in _fs_html_lei.lower() for s in _lm_loading_kws2)
                            if _fs_html_lei and len(_fs_html_lei) > 10000 and not _fs_loading:
                                logs.append({'nivel': 'ok', 'msg': f'{label}: [FS] Conteudo obtido via FlareSolverr ({len(_fs_html_lei)} chars)'})
                                resultado['html'] = _fs_html_lei
                            else:
                                # Logar preview para debug
                                from bs4 import BeautifulSoup as _BS
                                try:
                                    _soup_fs = _BS(_fs_html_lei, 'html.parser')
                                    _txt_fs = _soup_fs.get_text()[:300].strip()
                                except Exception:
                                    _txt_fs = _fs_html_lei[:300]
                                logs.append({'nivel': 'aviso', 'msg': f'{label}: [FS] FlareSolverr retornou loading/vazio ({len(_fs_html_lei)} chars) preview: {_txt_fs[:200]}'})
                                _fs_html_lei = ''
                    except Exception as e_fs:
                        logs.append({'nivel': 'aviso', 'msg': f'{label}: [FS] Erro FlareSolverr lei: {str(e_fs)[:60]}'})
                    if _fs_html_lei:
                        browser.close()
                        return resultado
                    logs.append({'nivel': 'info', 'msg': f'{label}: Extraindo HTML via sessao Playwright: {_lei_url[:80]}'})
                    _pg2 = ctx.new_page()
                    # Bloquear ads tambem nesta pagina
                    _ad_domains2 = ['doubleclick.net','googlesyndication.com','googletagmanager.com','adtrafficquality.google','viralize.tv','sodar']
                    def _bloquear_ads2(route, request):
                        if any(d in request.url.lower() for d in _ad_domains2): route.abort()
                        else: route.continue_()
                    _pg2.route('**/*', _bloquear_ads2)
                    # Injetar cookies da sessao FlareSolverr para que o AJAX funcione
                    try:
                        _cookies_ctx = ctx.cookies()
                        if _cookies_ctx:
                            _pg2.context.add_cookies(_cookies_ctx)
                    except Exception:
                        pass
                    try:
                        _pg2.goto(_lei_url, wait_until='networkidle', timeout=90000)
                    except Exception:
                        try:
                            _pg2.goto(_lei_url, wait_until='domcontentloaded', timeout=60000)
                        except Exception:
                            pass
                    try:
                        # Monitorar atividade de rede — detectar se spinner travou
                        _ultima_req = [_t2.time()]
                        _ajax_reqs = []
                        def _on_req(req):
                            _ultima_req[0] = _t2.time()
                            if req.resource_type in ('xhr', 'fetch'):
                                _ajax_reqs.append(req.url[:120])
                        def _on_resp(resp):
                            if resp.request.resource_type in ('xhr', 'fetch'):
                                logs.append({'nivel': 'info', 'msg': f'{label}: [AJAX] {resp.status} {resp.url[:100]}'})
                        _pg2.on('request', _on_req)
                        _pg2.on('response', _on_resp)
                        _max_espera = 120
                        _sem_rede_limite = 180  # aguardar 2Captcha resolver
                        _reloads = 0
                        _max_reloads = 2
                        _captcha_resolvido = False
                        _inicio = _t2.time()
                        while _t2.time() - _inicio < _max_espera:
                            try:
                                _pg2.wait_for_selector('div.law-container', timeout=3000)
                                break  # encontrou!
                            except Exception:
                                pass
                            _html_check = _pg2.content() if _pg2 else ''
                            _ainda_loading = any(s in _html_check for s in ['norma requisitada est', 'Por favor, aguarde', 'sendo carregada'])
                            if not _ainda_loading:
                                break  # página mudou, sair
                            # Resolver reCAPTCHA invisível que bloqueia o carregamento
                            if not _captcha_resolvido:
                                try:
                                    import re as _re2, requests as _req2
                                    _2ck = os.environ.get('TWOCAPTCHA_API_KEY', '')
                                    # sitekey fixo do leismunicipais (reCAPTCHA invisivel)
                                    _sk2 = "6Lcsu0AUAAAAAPGiUWm7uBfmctlz8sokhRldd3d6" if "leismunicipais" in _lei_url else None
                                    if _sk2 and _2ck:
                                        logs.append({'nivel': 'info', 'msg': f'{label}: [2C] reCAPTCHA invisivel — resolvendo sitekey={_sk2[:20]}...'})
                                        _r3 = _req2.post('http://2captcha.com/in.php', data={'key': _2ck, 'method': 'userrecaptcha', 'googlekey': _sk2, 'pageurl': _lei_url, 'invisible': 1, 'json': 1}, timeout=30)
                                        _cid3 = _r3.json().get('request')
                                        if _cid3 and str(_cid3) not in ('ERROR_WRONG_USER_KEY', 'ERROR_KEY_DOES_NOT_EXIST', 'ERROR_ZERO_BALANCE'):
                                            logs.append({'nivel': 'info', 'msg': f'{label}: [2C] Aguardando solucao (id={_cid3})...'})
                                            _sol3 = None
                                            for _ in range(36):
                                                _t2.sleep(5)
                                                _r4 = _req2.get(f'http://2captcha.com/res.php?key={_2ck}&action=get&id={_cid3}&json=1', timeout=15)
                                                _j4 = _r4.json()
                                                if _j4.get('status') == 1:
                                                    _sol3 = _j4.get('request')
                                                    break
                                                elif _j4.get('request') not in ('CAPCHA_NOT_READY', 'CAPTCHA_NOT_READY'):
                                                    break
                                            if _sol3:
                                                logs.append({'nivel': 'ok', 'msg': f'{label}: [2C] Token obtido — navegando para ?pass=...'})
                                                # Estratégia: navegar para ?pass=TOKEN como o callback validate() faz
                                                try:
                                                    _cur_url = _pg2.url
                                                    _pass_url = _cur_url + ('&' if '?' in _cur_url else '?') + 'pass=' + _sol3
                                                    logs.append({'nivel': 'info', 'msg': f'{label}: [2C] Navegando: {_pass_url[:100]}'})
                                                    try:
                                                        _pg2.goto(_pass_url, wait_until='networkidle', timeout=30000)
                                                    except Exception:
                                                        try:
                                                            _pg2.goto(_pass_url, wait_until='domcontentloaded', timeout=20000)
                                                        except Exception:
                                                            pass
                                                    # Aguardar law-container aparecer após o redirect
                                                    _html_pass = ''
                                                    _law_found = False
                                                    for _wi in range(12):
                                                        _t2.sleep(5)
                                                        try:
                                                            _pg2.wait_for_selector('div.law-container', timeout=4000)
                                                            _law_found = True
                                                            break
                                                        except Exception:
                                                            pass
                                                    if _law_found:
                                                        # Aguardar texto estabilizar — AJAX carrega progressivamente
                                                        _prev_len = 0
                                                        for _wi2 in range(60):
                                                            _t2.sleep(10)
                                                            _cur_html = _pg2.content()
                                                            _cur_len = len(_cur_html)
                                                            if _cur_len == _prev_len:
                                                                logs.append({"nivel": "info", "msg": f"{label}: [2C] HTML estabilizado ({_cur_len} chars, {_wi2+1} iteracoes)"})
                                                                break
                                                            _prev_len = _cur_len
                                                    _html_pass = _pg2.content()
                                                    _loading_pass = any(s in _html_pass for s in ['norma requisitada est', 'Por favor, aguarde', 'sendo carregada'])
                                                    _has_law = 'law-container' in _html_pass
                                                    logs.append({'nivel': 'info', 'msg': f'{label}: [2C] ?pass= resultado: {len(_html_pass)} chars | law={_has_law} | loading={_loading_pass}'})
                                                    if _has_law or (len(_html_pass) > 10000 and not _loading_pass):
                                                        resultado['html'] = _html_pass
                                                        logs.append({'nivel': 'ok', 'msg': f'{label}: [2C] HTML obtido via ?pass= ({len(_html_pass)} chars)'})
                                                        break
                                                except Exception as _ep:
                                                    logs.append({'nivel': 'aviso', 'msg': f'{label}: [2C] Erro ?pass=: {str(_ep)[:80]}'})
                                                # Fallback: injeção direta (método antigo)
                                                _pg2.evaluate(
                                                    "function(token){"
                                                    'var els=document.querySelectorAll(\'[name=g-recaptcha-response],[id=g-recaptcha-response]\');'
                                                    "els.forEach(function(el){"
                                                    "try{Object.getOwnPropertyDescriptor(window.HTMLTextAreaElement.prototype,'value').set.call(el,token);}catch(e){el.value=token;}"
                                                    "el.dispatchEvent(new Event('change',{bubbles:true}));"
                                                    "});"
                                                    "try{var cb=document.querySelector('[data-callback]');if(cb){var fn=cb.getAttribute('data-callback');if(window[fn])window[fn](token);}}catch(e){}"
                                                    "try{"
                                                    "var cfg=window.___grecaptcha_cfg||window.___grecaptcha_cfg_enterprise;"
                                                    "if(cfg&&cfg.clients){var cks=Object.keys(cfg.clients);"
                                                    "for(var ci=0;ci<cks.length;ci++){var cl=cfg.clients[cks[ci]];var iks=Object.keys(cl);"
                                                    "for(var ii=0;ii<iks.length;ii++){var obj=cl[iks[ii]];"
                                                    "if(obj&&typeof obj.callback==='function'){obj.callback(token);}}}}"
                                                    "}catch(e2){}"
                                                    "try{if(window.grecaptcha&&window.grecaptcha.execute){window.grecaptcha.execute();}}catch(e3){}"
                                                    "try{var f=document.querySelector('form');if(f){f.dispatchEvent(new Event('submit',{bubbles:true,cancelable:true}));}}catch(e4){}"
                                                    "}",
                                                    _sol3
                                                )
                                                _ultima_req[0] = _t2.time()
                                                _captcha_resolvido = True
                                                _t2.sleep(3)
                                            else:
                                                logs.append({'nivel': 'aviso', 'msg': f'{label}: [2C] Sem solucao para reCAPTCHA invisivel'})
                                except Exception as e_cap:
                                    logs.append({'nivel': 'aviso', 'msg': f'{label}: [2C] Erro reCAPTCHA: {str(e_cap)[:60]}'})
                            _sem_rede = _t2.time() - _ultima_req[0]
                            if _sem_rede > _sem_rede_limite and _reloads < _max_reloads:
                                logs.append({'nivel': 'info', 'msg': f'{label}: Spinner sem atividade de rede por {int(_sem_rede)}s — recarregando...'})
                                try:
                                    _pg2.reload(wait_until='domcontentloaded', timeout=30000)
                                    _ultima_req[0] = _t2.time()
                                    _captcha_resolvido = False
                                    _reloads += 1
                                except Exception:
                                    pass
                        _pg2.remove_listener('request', _on_req)
                    except Exception:
                        _t2.sleep(5)
                    _html_sessao = _pg2.content()
                    _lm_loading_kws = ['norma requisitada est', 'Por favor, aguarde', 'sendo carregada']
                    _lm_loading = any(s in _html_sessao for s in _lm_loading_kws) if _html_sessao else True
                    if _html_sessao and len(_html_sessao) > 10000 and not _lm_loading:
                        resultado['html'] = _html_sessao
                        logs.append({'nivel': 'ok', 'msg': f'{label}: HTML extraido via sessao ({len(_html_sessao)} chars)'})
                    elif _lm_loading:
                        logs.append({'nivel': 'aviso', 'msg': f'{label}: HTML sessao ainda em loading — descartando'})
                    else:
                        logs.append({'nivel': 'aviso', 'msg': f'{label}: HTML via sessao pequeno ({len(_html_sessao) if _html_sessao else 0} chars)'})
                    _pg2.close()
                except Exception as e_pg2:
                    logs.append({'nivel': 'aviso', 'msg': f'{label}: Erro ao extrair HTML via sessao: {str(e_pg2)[:80]}'})

            browser.close()

    except Exception as e_pw:
        logs.append({"nivel": "aviso", "msg": f"{label}: Erro Playwright: {str(e_pw)[:120]}"})

    return resultado


def navegar_como_humano(
    page,
    frame,
    legislacao: dict,
    chamar_llm,
    logs: list,
    label: str = '',
    max_passos: int = 30
) -> dict:
    """
    Navega uma pagina web como um humano: olha screenshot, decide, clica por texto.
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
    _edicoes_sem_lei = 0  # PDFs do DO baixados que não contêm a lei
    _MAX_EDICOES = 4      # Parar após 4 edições sem encontrar — evita busca infinita

    for passo in range(1, max_passos + 1):
        try:
            # 0. Verificar spinner LeisMunicipais ANTES de tirar screenshot
            # Paginas de lei (/a/...) sempre iniciam com spinner — sair imediatamente
            try:
                _url_pre = pagina_ativa.url if pagina_ativa else ''
                if 'leismunicipais.com.br/a/' in _url_pre:
                    _url_existente = resultado.get('url', '')
                    if not _url_existente and _url_pre.startswith('http'):
                        resultado['url'] = _url_pre
                        resultado['encontrada'] = True
                        resultado['confirmacao'] = 'URL capturada no spinner'
                    elif _url_existente and _url_pre.startswith('http') and _url_pre.startswith(_url_existente) and len(_url_pre) > len(_url_existente):
                        # _url_pre tem slug mais completo — atualizar
                        resultado['url'] = _url_pre
                        resultado['encontrada'] = True
                        resultado['confirmacao'] = 'URL capturada no spinner (slug)'
                    logs.append({'nivel': 'info', 'msg': f'{label}: Pagina de lei LM — saindo sem screenshot (url={resultado["url"][:80]})'})
                    break
            except Exception:
                pass
            # 1. Screenshot
            try:
                screenshot_b64 = _screenshot_base64(pagina_ativa)
                url_atual = pagina_ativa.url
            except Exception:
                try:
                    all_pages = page.context.pages
                    if all_pages:
                        pagina_ativa = all_pages[-1]
                        screenshot_b64 = _screenshot_base64(pagina_ativa)
                        url_atual = pagina_ativa.url
                        logs.append({'nivel': 'info', 'msg': f'{label}: 📸 Mudou para página: {url_atual[:50]}'})
                    else:
                        if resultado.get('url') and resultado['url'].startswith('http'):
                            resultado['encontrada'] = True
                            resultado['confirmacao'] = resultado.get('confirmacao', '') or 'URL capturada antes do browser fechar'
                            logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ Browser fechou mas URL capturada: {resultado["url"][:80]}'})
                        else:
                            logs.append({'nivel': 'aviso', 'msg': f'{label}: Nenhuma página aberta — encerrando'})
                        break
                except Exception:
                    logs.append({'nivel': 'aviso', 'msg': f'{label}: Pagina fechou — encerrando'})
                    if resultado.get('url') and resultado['url'].startswith('http'):
                        resultado['encontrada'] = True
                        resultado['confirmacao'] = 'URL capturada antes do browser fechar'
                        logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ URL capturada: {resultado["url"][:80]}'})
                    break

            # Salvar screenshot para debug
            try:
                debug_dir = '/tmp/nav_screenshots'
                os.makedirs(debug_dir, exist_ok=True)
                import re as _re
                label_clean = _re.sub(r'[^\w\-]', '_', label)
                ss_path = f'{debug_dir}/step_{label_clean}_{passo}.png'
                with open(ss_path, 'wb') as f_ss:
                    f_ss.write(base64.b64decode(screenshot_b64))
                ss_url = f'/debug/screenshots/img/{os.path.basename(ss_path)}'
                logs.append({'nivel': 'info', 'msg': f'{label}: 👁️ <a href="{ss_url}" target="_blank" style="color:#4fc3f7">O que a IA vê (passo {passo})</a>'})
            except Exception:
                pass

            # 2. Prompt
            prompt = _montar_prompt(legislacao, historico, passo, url_atual)

            # 3. Gemini
            resp = _chamar_gemini_visao(prompt, screenshot_b64, logs, f'{label} passo {passo}')

            if not resp:
                logs.append({'nivel': 'aviso', 'msg': f'{label}: Passo {passo}: sem resposta da IA'})
                historico.append({'passo': passo, 'acao': 'sem resposta', 'resultado': 'IA nao respondeu'})
                continue

            # 4. Parsear JSON — múltiplas estratégias
            try:
                resp_clean = resp.strip()
                # Remover markdown code blocks
                if '```' in resp_clean:
                    import re as _re_json
                    # Extrair conteúdo entre ```json e ```
                    block_match = _re_json.search(r'```(?:json)?\s*\n?([\s\S]*?)\n?\s*```', resp_clean)
                    if block_match:
                        resp_clean = block_match.group(1).strip()
                    else:
                        # Remover ``` simples
                        resp_clean = resp_clean.replace('```json', '').replace('```', '').strip()
                
                decisao = json.loads(resp_clean)
            except json.JSONDecodeError:
                import re
                # Tentar encontrar o JSON mais externo { ... }
                # Usar abordagem de contagem de chaves para pegar o JSON completo
                json_str = None
                brace_count = 0
                start_idx = None
                for i, ch in enumerate(resp):
                    if ch == '{':
                        if brace_count == 0:
                            start_idx = i
                        brace_count += 1
                    elif ch == '}':
                        brace_count -= 1
                        if brace_count == 0 and start_idx is not None:
                            json_str = resp[start_idx:i+1]
                            break
                
                if json_str:
                    try:
                        decisao = json.loads(json_str)
                    except json.JSONDecodeError:
                        # Tentar corrigir problemas comuns
                        try:
                            # Remover trailing commas antes de } ou ]
                            fixed = re.sub(r',\s*([}\]])', r'\1', json_str)
                            decisao = json.loads(fixed)
                        except json.JSONDecodeError:
                            logs.append({'nivel': 'aviso', 'msg': f'{label}: Passo {passo}: JSON invalido'})
                            historico.append({'passo': passo, 'acao': 'erro parse', 'resultado': 'JSON invalido'})
                            continue
                else:
                    logs.append({'nivel': 'aviso', 'msg': f'{label}: Passo {passo}: sem JSON'})
                    historico.append({'passo': passo, 'acao': 'erro parse', 'resultado': 'sem JSON'})
                    continue

            # 5. Extrair campos
            o_que_vejo = (decisao.get('o_que_vejo', '') or '')
            pensamento = decisao.get('decisao', '') or ''
            acao = decisao.get('acao', {}) or {}
            tipo_acao = acao.get('tipo', '') or ''

            logs.append({'nivel': 'info', 'msg': f'{label}: 👁️ Passo {passo}: {o_que_vejo}'})
            logs.append({'nivel': 'info', 'msg': f'{label}: 🧠 Decisão: {tipo_acao} — {pensamento[:400]}'})

            # 6. Legislacao encontrada? Só aceitar se ação é "concluido" (não junto com "clicar" etc)
            leg = decisao.get('legislacao_encontrada', {}) or {}
            leg_url = (leg.get('url', '') or '').strip()

            # Rejeitar URLs encurtadas (leis.org, bit.ly etc)
            _url_ok = leg_url and leg_url != '#' and leg_url.startswith('http') and not any(d in leg_url for d in ['leis.org', 'bit.ly', 'tinyurl', 'goo.gl', 'ow.ly'])
            if leg.get('encontrada') and _url_ok:
                if tipo_acao in ('concluido', ''):
                    resultado['encontrada'] = True
                    resultado['url'] = leg_url
                    resultado['status'] = (leg.get('status', '') or '')
                    resultado['confirmacao'] = (leg.get('confirmacao', '') or '')

                    logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ Legislação encontrada! {leg_url[:80]}'})
                    historico.append({'passo': passo, 'acao': 'concluido', 'resultado': f'Encontrada: {leg_url[:60]}'})
                    break
                else:
                    # IA disse encontrada mas também quer executar ação (ex: clicar no ícone)
                    # Executar a ação primeiro — o resultado será avaliado no próximo passo
                    logs.append({'nivel': 'info', 'msg': f'{label}: 🔍 IA disse encontrada mas quer {tipo_acao} primeiro — executando...'})

            # 6b. Spinner LeisMunicipais — sair do loop sem screenshot
            if tipo_acao == 'screenshot' and 'leismunicipais' in (pagina_ativa.url if pagina_ativa else ''):
                try:
                    _html_spinner = pagina_ativa.content()
                    _is_spinner = any(s in _html_spinner for s in ['norma requisitada est', 'Por favor, aguarde', 'sendo carregada'])
                    if _is_spinner:
                        # Capturar URL e sair — _pg2 vai resolver o reCAPTCHA invisivel
                        _url_spinner = pagina_ativa.url
                        if not resultado.get('url') and _url_spinner.startswith('http'):
                            resultado['url'] = _url_spinner
                            resultado['encontrada'] = True
                            resultado['confirmacao'] = 'URL capturada no spinner'
                        logs.append({'nivel': 'info', 'msg': f'{label}: ⏳ Spinner detectado — saindo do loop para resolver via _pg2 sem screenshots'})
                        historico.append({'passo': passo, 'acao': 'screenshot', 'resultado': 'spinner_saindo'})
                        break
                except Exception:
                    pass
            # 7. Desistiu?
            _skip_exec = False
            if tipo_acao == 'desistir':
                # Antes de desistir, verificar se há ícone clicável na tabela (IA pode não ver o ícone)
                pensamento_lower = pensamento.lower()
                legislacao_vista = any(kw in pensamento_lower for kw in ['encontrad', 'listada', 'resultado', 'registro', 'identificad'])
                
                if legislacao_vista:
                    logs.append({'nivel': 'info', 'msg': f'{label}: 🔄 IA desistiu mas viu legislação — tentando clicar ícone automaticamente...'})
                    try:
                        auto_result = _clicar_por_texto(pagina_ativa, 'icone coluna Arquivo linha 1', logs, label)
                        if 'Download:' in auto_result or 'Nova aba:' in auto_result or 'Navegou:' in auto_result:
                            exec_resultado = auto_result
                            _skip_exec = True
                            historico.append({'passo': passo, 'acao': 'auto_click_icone', 'resultado': auto_result[:100]})
                        else:
                            logs.append({'nivel': 'aviso', 'msg': f'{label}: ❌ Auto-clique falhou: {auto_result[:60]}'})
                            historico.append({'passo': passo, 'acao': 'desistir', 'resultado': pensamento[:100]})
                            break
                    except Exception as e_auto:
                        logs.append({'nivel': 'aviso', 'msg': f'{label}: ❌ Auto-clique erro: {str(e_auto)[:40]}'})
                        historico.append({'passo': passo, 'acao': 'desistir', 'resultado': pensamento[:100]})
                        break
                else:
                    _captcha_kws = ['captcha', 'recaptcha', 'motocicleta', 'selecionar imagens', 'selecione imagens', 'verificar que voce', 'desafio visual', 'selecione todas', 'nao sou um robo', 'prove que e humano']
                    _captcha_detectado = any(kw in pensamento.lower() for kw in _captcha_kws)
                    _2captcha_key = os.environ.get('TWOCAPTCHA_API_KEY', '')
                    if _captcha_detectado and _2captcha_key and pagina_ativa:
                        logs.append({'nivel': 'info', 'msg': f'{label}: [2C] CAPTCHA detectado - tentando 2Captcha...'})
                        try:
                            import base64 as _b64mod, requests as _req, time as _time, re as _re
                            _sitekey = None
                            _page_url = pagina_ativa.url
                            try:
                                _html_cap = pagina_ativa.content()
                                _sk_match = _re.search(r'data-sitekey=["\'\']([0-9A-Za-z_-]{20,})["\'\']', _html_cap)
                                if not _sk_match:
                                    _sk_match = _re.search(r'"sitekey"\s*:\s*"([0-9A-Za-z_-]{20,})"', _html_cap)
                                if _sk_match:
                                    _sitekey = _sk_match.group(1)
                            except Exception:
                                pass
                            # Fallback: sitekey conhecido do leismunicipais
                            if not _sitekey and 'leismunicipais' in _page_url:
                                _sitekey = '6Lcsu0AUAAAAAPGiUWm7uBfmctlz8sokhRldd3d6'
                            if _sitekey:
                                logs.append({'nivel': 'info', 'msg': f'{label}: [2C] reCAPTCHA v2 — sitekey={_sitekey[:20]}...'})
                                _post_data = {'key': _2captcha_key, 'method': 'userrecaptcha', 'googlekey': _sitekey, 'pageurl': _page_url, 'json': 1}
                            else:
                                logs.append({'nivel': 'info', 'msg': f'{label}: [2C] Sem sitekey — usando OCR (base64)'})
                                _ss_bytes = pagina_ativa.screenshot(type='png')
                                _b64str = _b64mod.b64encode(_ss_bytes).decode()
                                _post_data = {'key': _2captcha_key, 'method': 'base64', 'body': _b64str, 'json': 1}
                            _r = _req.post('http://2captcha.com/in.php', data=_post_data, timeout=30)
                            _captcha_id = _r.json().get('request')
                            _erros = ('ERROR_WRONG_USER_KEY', 'ERROR_KEY_DOES_NOT_EXIST', 'ERROR_ZERO_BALANCE')
                            if _captcha_id and str(_captcha_id) not in _erros:
                                logs.append({'nivel': 'info', 'msg': f'{label}: [2C] Aguardando (id={_captcha_id})...'})
                                _sol = None
                                for _ in range(24):
                                    _time.sleep(5)
                                    _url2 = f'http://2captcha.com/res.php?key={_2captcha_key}&action=get&id={_captcha_id}&json=1'
                                    _r2 = _req.get(_url2, timeout=15)
                                    _j2 = _r2.json()
                                    if _j2.get('status') == 1:
                                        _sol = _j2.get('request')
                                        break
                                    elif _j2.get('request') not in ('CAPCHA_NOT_READY', 'CAPTCHA_NOT_READY'):
                                        break
                                if _sol:
                                    logs.append({'nivel': 'ok', 'msg': f'{label}: [2C] CAPTCHA resolvido!'})
                                    try:
                                        _js_inject = (
                                            "var els=document.querySelectorAll('[name=\"g-recaptcha-response\"],[id=\"g-recaptcha-response\"]');"
                                            "els.forEach(function(el){"
                                            "try{Object.getOwnPropertyDescriptor(window.HTMLTextAreaElement.prototype,'value').set.call(el,'" + _sol + "');}catch(e){el.value='" + _sol + "';}"
                                            "el.dispatchEvent(new Event('change',{bubbles:true}));"
                                            "});"
                                            "try{"
                                            "var cb=document.querySelector('[data-callback]');"
                                            "if(cb){var fn=cb.getAttribute('data-callback');if(window[fn])window[fn]('" + _sol + "');}"
                                            "}catch(e){}"
                                            "try{"
                                            "var cfg=window.___grecaptcha_cfg||window.___grecaptcha_cfg_enterprise;"
                                            "if(cfg&&cfg.clients){var cks=Object.keys(cfg.clients);"
                                            "for(var ci=0;ci<cks.length;ci++){var cl=cfg.clients[cks[ci]];var iks=Object.keys(cl);"
                                            "for(var ii=0;ii<iks.length;ii++){var obj=cl[iks[ii]];"
                                            "if(obj&&typeof obj.callback==='function'){obj.callback('" + _sol + "');}}}}"
                                            "}catch(e2){}"
                                        )
                                        pagina_ativa.evaluate(_js_inject)
                                    except Exception:
                                        pass
                                    _time.sleep(3)
                                    historico.append({'passo': passo, 'acao': 'captcha_resolvido', 'resultado': '2Captcha OK'})
                                    continue
                                else:
                                    logs.append({'nivel': 'aviso', 'msg': f'{label}: [2C] Sem solucao'})
                            else:
                                logs.append({'nivel': 'aviso', 'msg': f'{label}: [2C] Erro: {_r.text[:80]}'})
                        except Exception as _e2c:
                            logs.append({'nivel': 'aviso', 'msg': f'{label}: [2C] Excecao: {str(_e2c)[:80]}'})
                    logs.append({'nivel': 'aviso', 'msg': f'{label}: ❌  Passo {passo}: IA desistiu — {pensamento[:400]}'})
                    historico.append({'passo': passo, 'acao': 'desistir', 'resultado': pensamento[:100]})
                    break

            # 8. Detectar loop
            if len(historico) >= 2:
                tipos_recentes = [h['acao'].split(' ')[0] for h in historico[-2:]]
                if all(t == tipo_acao for t in tipos_recentes):
                    resultados_recentes = [h['resultado'] for h in historico[-2:]]
                    if len(set(resultados_recentes)) == 1:
                        # Spinner do LeisMunicipais — tentar 2Captcha antes de desistir
                        _is_lm_spinner = (
                            pagina_ativa and
                            'leismunicipais' in (pagina_ativa.url or '') and
                            any(s in (pagina_ativa.content() if pagina_ativa else '') for s in ['norma requisitada est', 'Por favor, aguarde'])
                        )
                        if _is_lm_spinner:
                            try:
                                import requests as _req_lm, time as _t_lm
                                _2ck_lm = os.environ.get('TWOCAPTCHA_API_KEY', '')
                                _sk_lm = '6Lcsu0AUAAAAAPGiUWm7uBfmctlz8sokhRldd3d6'
                                if _2ck_lm:
                                    logs.append({'nivel': 'info', 'msg': f'{label}: [2C] Spinner LM — resolvendo reCAPTCHA invisivel...'})
                                    _r_lm = _req_lm.post('http://2captcha.com/in.php', data={'key': _2ck_lm, 'method': 'userrecaptcha', 'googlekey': _sk_lm, 'pageurl': pagina_ativa.url, 'invisible': 1, 'json': 1}, timeout=30)
                                    _cid_lm = _r_lm.json().get('request')
                                    if _cid_lm and str(_cid_lm) not in ('ERROR_WRONG_USER_KEY', 'ERROR_KEY_DOES_NOT_EXIST', 'ERROR_ZERO_BALANCE'):
                                        logs.append({'nivel': 'info', 'msg': f'{label}: [2C] Aguardando (id={_cid_lm})...'})
                                        _sol_lm = None
                                        for _ in range(36):
                                            _t_lm.sleep(5)
                                            _r2_lm = _req_lm.get(f'http://2captcha.com/res.php?key={_2ck_lm}&action=get&id={_cid_lm}&json=1', timeout=15)
                                            _j_lm = _r2_lm.json()
                                            if _j_lm.get('status') == 1:
                                                _sol_lm = _j_lm.get('request')
                                                break
                                            elif _j_lm.get('request') not in ('CAPCHA_NOT_READY', 'CAPTCHA_NOT_READY'):
                                                break
                                        if _sol_lm:
                                            logs.append({'nivel': 'ok', 'msg': f'{label}: [2C] Token obtido — injetando no spinner...'})
                                            _js_lm = (
                                                "(function(){"
                                                "var t='%s';"
                                                "var els=document.querySelectorAll('[name=g-recaptcha-response],[id=g-recaptcha-response]');"
                                                "els.forEach(function(el){try{Object.getOwnPropertyDescriptor(window.HTMLTextAreaElement.prototype,'value').set.call(el,t);}catch(e){el.value=t;}"
                                                "el.dispatchEvent(new Event('change',{bubbles:true}));});"
                                                "try{var cb=document.querySelector('[data-callback]');if(cb){var fn=cb.getAttribute('data-callback');if(window[fn])window[fn](t);}}catch(e){}"
                                                "try{"
                                                "var cfg=window.___grecaptcha_cfg||window.___grecaptcha_cfg_enterprise;"
                                                "if(cfg&&cfg.clients){var cks=Object.keys(cfg.clients);"
                                                "for(var ci=0;ci<cks.length;ci++){var cl=cfg.clients[cks[ci]];var iks=Object.keys(cl);"
                                                "for(var ii=0;ii<iks.length;ii++){var obj=cl[iks[ii]];"
                                                "if(obj&&typeof obj.callback==='function'){obj.callback(t);}}}}"
                                                "}catch(e2){}"
                                                "})()" % _sol_lm
                                            )
                                            pagina_ativa.evaluate(_js_lm)
                                            _t_lm.sleep(5)
                                            continue  # voltar ao loop principal
                                        else:
                                            logs.append({'nivel': 'aviso', 'msg': f'{label}: [2C] Sem solucao para spinner LM'})
                            except Exception as e_lm:
                                logs.append({'nivel': 'aviso', 'msg': f'{label}: [2C] Erro spinner LM: {str(e_lm)[:60]}'})
                        logs.append({'nivel': 'aviso', 'msg': f'{label}: ⚠️ Loop detectado — {tipo_acao} repetido 3x sem mudança.'})
                        historico.append({'passo': passo, 'acao': 'loop', 'resultado': 'Mesma ação repetida'})
                        break

            # 9. Executar
            if not _skip_exec:
                exec_resultado = _executar_acao(pagina_ativa, acao, logs, label) or 'sem resultado'

                texto_el = acao.get('texto_elemento', '')[:30] if acao.get('texto_elemento') else ''
                coord_info = f' "{texto_el}"' if texto_el else ''

                historico.append({
                    'passo': passo,
                    'acao': f'{tipo_acao}{coord_info}',
                    'resultado': exec_resultado[:100]
                })

            # 10. Nova aba?
            if exec_resultado.startswith('Nova aba:'):
                nova_url = exec_resultado.split(': ', 1)[1] if ': ' in exec_resultado else ''
                try:
                    all_pages = page.context.pages
                    if len(all_pages) > 1:
                        pagina_ativa = all_pages[-1]
                        logs.append({'nivel': 'info', 'msg': f'{label}: 📄 Mudou para nova aba: {pagina_ativa.url[:50]}'})
                except Exception:
                    if nova_url and nova_url.startswith('http'):
                        resultado['encontrada'] = True
                        resultado['url'] = nova_url
                        resultado['confirmacao'] = 'URL capturada antes do browser fechar'
                        logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ URL capturada: {nova_url[:80]}'})
                        break

            # 11. Navegação?
            if exec_resultado.startswith('Navegou:'):
                nav_url = exec_resultado.split(': ', 1)[1] if ': ' in exec_resultado else ''
                if nav_url and nav_url.startswith('http'):
                    # Preferir URL anterior se for mais longa (tem slug) e começa com nav_url
                    _url_anterior = resultado.get('url', '')
                    if _url_anterior and _url_anterior.startswith(nav_url) and len(_url_anterior) > len(nav_url):
                        logs.append({'nivel': 'info', 'msg': f'{label}: 🔗 URL com slug preservada: {_url_anterior[:80]}'})
                    else:
                        resultado['url'] = nav_url
                        logs.append({'nivel': 'info', 'msg': f'{label}: 🔗 URL capturada: {nav_url[:80]}'})
                try:
                    all_pages = page.context.pages
                    if all_pages:
                        pagina_ativa = all_pages[-1]
                except Exception:
                    if nav_url and nav_url.startswith('http'):
                        resultado['encontrada'] = True
                        resultado['confirmacao'] = 'URL capturada antes do browser fechar'
                        logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ Legislação encontrada via navegação: {nav_url[:80]}'})
                        break

            # 12a. Href extraído? Tentar navegar para ele
            if exec_resultado.startswith('Href:'):
                href_url = exec_resultado.split(': ', 1)[1] if ': ' in exec_resultado else ''
                if href_url and href_url.startswith('http'):
                    resultado['url'] = href_url
                    logs.append({'nivel': 'info', 'msg': f'{label}: 🔗 Href capturado: {href_url[:80]}'})
                    # Tentar navegar para o href na mesma página
                    try:
                        pagina_ativa.goto(href_url, wait_until='networkidle', timeout=15000)
                        time.sleep(2)
                        logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ Navegou para href: {pagina_ativa.url[:60]}'})
                        historico.append({
                            'passo': passo,
                            'acao': f'href_nav',
                            'resultado': f'Navegou para: {pagina_ativa.url[:60]}'
                        })
                        continue  # Continuar navegação na nova página
                    except Exception:
                        # Navegação falhou — href pode ser bloqueado
                        logs.append({'nivel': 'info', 'msg': f'{label}: ⚠️ Navegação para href bloqueada'})

            # 13. Download? Verificar se a legislação está no PDF
            if exec_resultado.startswith('Download:'):
                pdf_path = exec_resultado.split(': ', 1)[1]
                resultado['pdf_path'] = pdf_path
                
                # Verificar se o nome do arquivo corresponde à data esperada
                data_esperada = legislacao.get('data_publicacao', '')
                nome_arquivo = os.path.basename(pdf_path).lower()
                if data_esperada and data_esperada not in nome_arquivo:
                    # Checar se a data está no formato do arquivo (ex: 2019-01-14)
                    data_no_nome = False
                    for fmt in [data_esperada, data_esperada.replace('-', '')]:
                        if fmt in nome_arquivo:
                            data_no_nome = True
                            break
                    
                    if not data_no_nome:
                        logs.append({'nivel': 'aviso', 'msg': f'{label}: ⚠️ PDF "{nome_arquivo}" não parece ser da data {data_esperada} — pode ser a edição errada'})
                
                # Extrair texto do PDF e verificar com IA
                legislacao_no_pdf = False
                try:
                    import fitz
                    doc = fitz.open(pdf_path)
                    
                    # Buscar páginas que mencionam TIPO + NÚMERO juntos
                    tipo_lei = legislacao.get('tipo', 'Lei Complementar')
                    numero_lei = str(legislacao.get('numero', ''))
                    ano_lei = legislacao.get('ano', '')
                    municipio_lei = legislacao.get('municipio', '')
                    
                    palavras_tipo = [p.lower() for p in tipo_lei.split() if len(p) > 2]
                    
                    paginas_relevantes = []
                    for pg_num in range(len(doc)):
                        pg_text = doc[pg_num].get_text()
                        pg_lower = pg_text.lower()
                        if numero_lei and numero_lei in pg_text and all(p in pg_lower for p in palavras_tipo):
                            paginas_relevantes.append((pg_num + 1, pg_text))
                    doc.close()
                    
                    if not paginas_relevantes:
                        logs.append({'nivel': 'info', 'msg': f'{label}: 🔍 "{tipo_lei}" + "{numero_lei}" não encontrados juntos em nenhuma página'})
                    else:
                        logs.append({'nivel': 'info', 'msg': f'{label}: 🔍 "{tipo_lei}" + "{numero_lei}" em {len(paginas_relevantes)} página(s) — verificando com IA...'})
                        
                        # Juntar texto das páginas relevantes (max 15000 chars)
                        amostra = ''
                        for pg_num, pg_text in paginas_relevantes:
                            trecho = f'\n--- PÁGINA {pg_num} ---\n{pg_text}'
                            if len(amostra) + len(trecho) > 15000:
                                break
                            amostra += trecho
                        
                        prompt_verif = f"""Analise o texto abaixo, extraido de um PDF de Diario Oficial.

A "{tipo_lei} nº {numero_lei}/{ano_lei}" do municipio de {municipio_lei} está PUBLICADA neste PDF?

CRITERIO: responda SIM (encontrada: true) se o PDF contiver o CABEÇALHO FORMAL da lei (ex: "LEI COMPLEMENTAR Nº {numero_lei}, DE ...") seguido de ao menos alguns artigos (Art. 1º, Art. 2º...).
NAO e necessario o texto COMPLETO — basta confirmar que e a publicacao oficial da lei, nao uma simples citacao ou referencia em outro ato.
Se a lei e apenas MENCIONADA em um despacho, portaria ou ato administrativo de terceiro, responda NAO.

Responda APENAS com JSON:
{{"encontrada": true ou false, "motivo": "explique brevemente"}}

TEXTO DO PDF:
{amostra}"""
                        
                        resp_verif = chamar_llm(prompt_verif, logs, f'{label} verif PDF')
                        
                        if resp_verif:
                            try:
                                import re as _re_v
                                resp_v = resp_verif.strip()
                                # Limpar blocos markdown
                                resp_v = _re_v.sub(r'^```(?:json)?\s*|\s*```$', '', resp_v, flags=_re_v.MULTILINE).strip()
                                # Tentar parse direto primeiro
                                try:
                                    verif = json.loads(resp_v)
                                except Exception:
                                    # Fallback: extrair objeto JSON com regex que suporta multiline
                                    json_m = _re_v.search(r'\{.*?\}', resp_v, _re_v.DOTALL)
                                    verif = json.loads(json_m.group()) if json_m else {}
                                legislacao_no_pdf = bool(verif.get('encontrada', False))
                                motivo = str(verif.get('motivo', ''))[:200]
                                # Salvaguarda: motivo menciona cabecalho formal + artigos mas encontrada=false
                                _motivo_lower = motivo.lower()
                                if not legislacao_no_pdf and ('cabe' in _motivo_lower and 'formal' in _motivo_lower and 'art' in _motivo_lower):
                                    legislacao_no_pdf = True
                                    logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ IA confirmou (salvaguarda): {motivo}'})
                                elif legislacao_no_pdf:
                                    logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ IA confirmou: {motivo}'})
                                else:
                                    logs.append({'nivel': 'info', 'msg': f'{label}: ❌ IA disse nao: {motivo}'})
                            except Exception as _e_parse:
                                # JSON parse falhou — tentar heurística no texto bruto
                                _rv_lower = resp_verif.lower()
                                if '"encontrada": true' in _rv_lower or '"encontrada":true' in _rv_lower:
                                    legislacao_no_pdf = True
                                    logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ IA confirmou (heurística)'})
                                elif 'encontrada": false' in _rv_lower or '"encontrada":false' in _rv_lower:
                                    logs.append({'nivel': 'info', 'msg': f'{label}: ❌ IA disse não (heurística)'})
                                else:
                                    logs.append({'nivel': 'aviso', 'msg': f'{label}: ⚠️ Resposta IA não parseável: {resp_verif[:80]}'})

                    
                except Exception as e_pdf:
                    logs.append({'nivel': 'info', 'msg': f'{label}: ⚠️ Erro ao verificar PDF: {str(e_pdf)[:40]}'})
                
                if legislacao_no_pdf:
                    resultado['encontrada'] = True
                    resultado['confirmacao'] = 'Legislação confirmada no PDF pela IA'
                    logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ PDF confirmado — encerrando navegação'})
                    break
                else:
                    _edicoes_sem_lei += 1
                    if _edicoes_sem_lei >= _MAX_EDICOES:
                        logs.append({'nivel': 'aviso', 'msg': f'{label}: ⏹️ {_edicoes_sem_lei} edições verificadas sem encontrar a lei — encerrando busca no DO'})
                        break
                    logs.append({'nivel': 'info', 'msg': f'{label}: 📅 Legislação não está nesta edição ({_edicoes_sem_lei}/{_MAX_EDICOES}) — IA deve tentar o dia seguinte'})
                    historico.append({
                        'passo': passo,
                        'acao': 'download_verificado',
                        'resultado': f'PDF baixado mas legislação NÃO encontrada nesta edição ({_edicoes_sem_lei}/{_MAX_EDICOES}). Tentar próximo dia.'
                    })
                    continue

        except Exception as e:
            err_msg = str(e)[:80]
            if 'closed' in err_msg.lower() or 'disposed' in err_msg.lower():
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
                        continue
                except Exception:
                    pass
                logs.append({'nivel': 'aviso', 'msg': f'{label}: Pagina fechou — encerrando'})
                if resultado.get('url') and resultado['url'].startswith('http'):
                    resultado['encontrada'] = True
                    resultado['confirmacao'] = 'URL capturada antes do browser fechar'
                    logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ URL capturada: {resultado["url"][:80]}'})
                break

            logs.append({'nivel': 'aviso', 'msg': f'{label}: Passo {passo} erro: {err_msg}'})
            historico.append({'passo': passo, 'acao': 'erro', 'resultado': err_msg})

    # Salvaguarda final: se URL foi capturada mas encontrada=False, marcar como encontrada
    if not resultado['encontrada'] and resultado.get('url') and resultado['url'].startswith('http'):
        resultado['encontrada'] = True
        resultado['confirmacao'] = resultado.get('confirmacao', '') or 'URL capturada durante navegacao'
        logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ URL capturada ao encerrar: {resultado["url"][:80]}'})
    elif not resultado['encontrada']:
        logs.append({'nivel': 'aviso', 'msg': f'{label}: Navegação encerrada após {len(historico)} passos sem encontrar'})

    return resultado
