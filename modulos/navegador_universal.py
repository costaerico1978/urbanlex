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
    proxy_url = os.getenv('PROXY_URL', '').strip()
    if proxy_url:
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
8. Formularios com dropdowns simples: use "preencher_formulario". Datepickers: use "clicar"/"digitar".
9. Se a pagina ja mostra o conteudo (edicao carregada, preview visivel), clique em DOWNLOAD (PDF), nao em buscar de novo.
10. Sempre prefira baixar PDF.
11. SITES DE BUSCA DE LEGISLACAO (NAO diarios oficiais): preencha APENAS Esfera, Tipo de Ato e Numero. NAO preencha campos de data — deixe-os vazios. A busca retornara resultados e voce identifica a legislacao correta pela descricao (tipo, numero, ano, ementa). Se houver varias paginas de resultados, navegue ate encontrar. EXCECAO: se o formulario EXIGIR data (campo obrigatorio, erro ao submeter sem data), use a data de ASSINATURA da legislacao (a data informada no prompt) como Data Inicial, e +5 dias como Data Final. A data de assinatura e diferente da data de publicacao no diario oficial.
12. TEXTO DO ELEMENTO: copie o texto EXATO como aparece na tela. Exemplos:
    - Botao "OK" -> texto_elemento: "OK"
    - Link "PDF" -> texto_elemento: "PDF"  
    - Link "HTML" -> texto_elemento: "HTML"
    - Link "Consultar" -> texto_elemento: "Consultar"
    - Link "Download da Edição nº 200" -> texto_elemento: "Download da Edição nº 200"
    - Se o elemento nao tem texto (icone puro), descreva: "icone coluna Arquivo linha 1"

JSON (sem markdown):
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

                    # Tentar buscar direto com requests + proxy (contexto ainda intacto)
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
                        _r_a = _req_a.get(
                            _direct_url,
                            cookies=_cookies_a,
                            proxies=_proxies_a,
                            timeout=25,
                            allow_redirects=True,
                            headers={'User-Agent': _ua_a, 'Referer': page.url}
                        )
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
                        elif len(_r_a.content) > 300:
                            logs.append({'nivel': 'info', 'msg': f'{label}: 📄 A½ HTML direto: {len(_r_a.content)} bytes — seguindo para popup'})
                    except Exception as _e_a:
                        logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ A½ requests falhou: {str(_e_a)[:60]}'})
            except Exception as _e_half:
                logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ A½ JS extract falhou: {str(_e_half)[:60]}'})

            # ═══════════════════════════════════════════════════════════════════
            # ESTADO COMPARTILHADO entre todas as estratégias
            # ═══════════════════════════════════════════════════════════════════
            _ctx_responses   = []   # (url, content_type, body_bytes) capturados pelo contexto
            _popup_nav_urls  = []   # URLs navegadas dentro do popup
            _popup_dl        = [None]
            _popup_page_ref  = [None]

            # ── PRÉ-CLIQUE: interceptar respostas HTTP no contexto INTEIRO ────
            # Esta é a estratégia mais robusta: captura tudo que trafega pelo
            # contexto Playwright (proxy incluso) antes mesmo do popup existir.
            def _on_ctx_response(resp):
                try:
                    ct  = resp.headers.get('content-type', '').lower()
                    url = resp.url
                    is_doc = (
                        'pdf'      in ct or
                        'octet'    in ct or
                        url.lower().endswith('.pdf') or
                        ('download' in url.lower() and url != page.url)
                    )
                    is_html = 'text/html' in ct and url != page.url and 'about:' not in url
                    if is_doc or is_html:
                        try:
                            body = resp.body()  # lê bytes imediatamente — só disponível agora
                            if len(body) > 500:
                                _ctx_responses.append((url, ct, body))
                        except Exception:
                            _ctx_responses.append((url, ct, None))  # guardamos a URL mesmo sem body
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
                def _on_popup_resp(resp):
                    _on_ctx_response(resp)
                def _on_dl(d):
                    _popup_dl[0] = d
                try:
                    new_popup.on('framenavigated', _on_nav)
                    new_popup.on('response',       _on_popup_resp)
                    new_popup.on('download',       _on_dl)
                except Exception:
                    pass

            try:
                page.context.on('response', _on_ctx_response)
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

            # ── Helper: remover listeners do contexto ─────────────────────────
            def _remove_ctx_listeners():
                try: page.context.remove_listener('response', _on_ctx_response)
                except Exception: pass
                try: page.context.remove_listener('page', _setup_popup_listeners)
                except Exception: pass

            # ═══════════════════════════════════════════════════════════════════
            # ESTRATÉGIA A — context.on('response') + clique (primária)
            # Captura o documento diretamente no nível HTTP do contexto.
            # Funciona mesmo que o popup abra e feche em < 1 segundo.
            # ═══════════════════════════════════════════════════════════════════
            popup_page = None
            try:
                with page.expect_popup(timeout=15000) as popup_info:
                    el.click()
                popup_page = popup_info.value

                # URL imediata antes de qualquer wait
                try:
                    pu = popup_page.url or ''
                    if pu and pu != 'about:blank' and pu not in _popup_nav_urls:
                        _popup_nav_urls.insert(0, pu)
                except Exception:
                    pass

                logs.append({'nivel': 'ok', 'msg': f'{label}: 🪟 Popup aberto: {(_popup_nav_urls[0] if _popup_nav_urls else "?")[:80]}'})

                # Aguardar o popup navegar — espera inteligente sem wait_for_load_state
                deadline = time.time() + 10
                while time.time() < deadline:
                    # Parar se já temos documento capturado OU popup navegou 2+ URLs
                    if _ctx_responses or len(_popup_nav_urls) >= 2:
                        break
                    time.sleep(0.3)
                time.sleep(1)  # margem extra para body ser lido

            except Exception as e_popup_open:
                logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ expect_popup: {str(e_popup_open)[:60]}'})
                # Mesmo sem popup, o listener de response pode ter capturado algo
                # (alguns sites fazem download direto sem abrir janela)
                time.sleep(4)

            logs.append({'nivel': 'info', 'msg': f'{label}: 🔗 Nav popup: {" → ".join(u[:50] for u in _popup_nav_urls)}'})
            logs.append({'nivel': 'info', 'msg': f'{label}: 📡 Respostas ctx: {len(_ctx_responses)}'})

            # Verificar se ESTRATÉGIA A capturou o documento
            for cap_url, cap_ct, cap_body in _ctx_responses:
                if 'pdf' in cap_ct or cap_url.lower().endswith('.pdf') or 'octet' in cap_ct:
                    saved = _salvar_corpo(cap_url, cap_ct, cap_body)
                    if saved:
                        _remove_ctx_listeners()
                        _cleanup_listeners()
                        return f'Download: {saved}'

            # ── Download direto disparado pelo popup? (B) ─────────────────────
            dl_obj = _popup_dl[0] or _download_obj
            if dl_obj:
                try:
                    dl_path = tempfile.mktemp(suffix='.pdf')
                    dl_obj.save_as(dl_path)
                    dl_size = os.path.getsize(dl_path) // 1024
                    logs.append({'nivel': 'ok', 'msg': f'{label}: 📥 Download objeto popup: {dl_size}KB'})
                    _remove_ctx_listeners()
                    _cleanup_listeners()
                    return f'Download: {dl_path}'
                except Exception:
                    pass

            # URL final para as próximas estratégias
            final_url = _popup_nav_urls[-1] if _popup_nav_urls else ''
            # Verificar URL atual do popup (se ainda vivo)
            if popup_page:
                try:
                    curr = popup_page.url
                    if curr and curr != 'about:blank':
                        final_url = curr
                        if curr not in _popup_nav_urls:
                            _popup_nav_urls.append(curr)
                except Exception:
                    pass

            # ═══════════════════════════════════════════════════════════════════
            # ESTRATÉGIA C — Ler popup diretamente (se ainda estiver vivo)
            # ═══════════════════════════════════════════════════════════════════
            if popup_page and final_url:
                logs.append({'nivel': 'info', 'msg': f'{label}: 🔄 Estratégia C — leitura direta do popup'})
                try:
                    ct_popup = popup_page.evaluate('() => document.contentType || ""') or ''
                    if 'pdf' in ct_popup.lower():
                        resp_p = popup_page.request.get(final_url, timeout=20000)
                        if resp_p.status == 200:
                            saved = _salvar_corpo(final_url, ct_popup, resp_p.body())
                            if saved:
                                _remove_ctx_listeners()
                                _cleanup_listeners()
                                return f'Download: {saved}'
                    else:
                        body_popup = popup_page.inner_text('body') or ''
                        if len(body_popup) > 300:
                            logs.append({'nivel': 'info', 'msg': f'{label}: 📄 Conteúdo popup vivo: {len(body_popup)} chars'})
                            _remove_ctx_listeners()
                            _cleanup_listeners()
                            return f'Nova aba: {final_url}'
                except Exception as e_c:
                    logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ C falhou: {str(e_c)[:50]}'})

            if not final_url:
                # Sem URL capturada — não há como continuar
                logs.append({'nivel': 'aviso', 'msg': f'{label}: ⚠️ Nenhuma URL capturada — abortando cascade'})
                _remove_ctx_listeners()
                _cleanup_listeners()
                return 'Erro: nenhuma URL capturada no popup'

            # ═══════════════════════════════════════════════════════════════════
            # ESTRATÉGIA D — page.context.request.get() (mesmo proxy/sessão)
            # Usa o cliente HTTP interno do Playwright: mesma sessão, mesmo proxy,
            # mesmos headers. Popup pode estar morto — não importa.
            # ═══════════════════════════════════════════════════════════════════
            logs.append({'nivel': 'info', 'msg': f'{label}: 🔄 Estratégia D — context.request: {final_url[:70]}'})
            try:
                resp_d = page.context.request.get(
                    final_url,
                    timeout=25000,
                    headers={'Referer': page.url}
                )
                ct_d = resp_d.headers.get('content-type', '').lower()
                body_d = resp_d.body()
                saved = _salvar_corpo(final_url, ct_d, body_d)
                if saved:
                    _remove_ctx_listeners()
                    _cleanup_listeners()
                    return f'Download: {saved}' if saved.endswith('.pdf') else f'Nova aba: {final_url}'
                elif len(body_d) > 300:
                    logs.append({'nivel': 'info', 'msg': f'{label}: 📄 D retornou HTML: {len(body_d)} bytes'})
                    _remove_ctx_listeners()
                    _cleanup_listeners()
                    return f'Nova aba: {final_url}'
            except Exception as e_d:
                logs.append({'nivel': 'info', 'msg': f'{label}: ℹ️ D falhou: {str(e_d)[:60]}'})

            # ═══════════════════════════════════════════════════════════════════
            # ESTRATÉGIA E — requests Python com cookies exportados
            # IP diferente do proxy, mas carrega cookies da sessão.
            # ═══════════════════════════════════════════════════════════════════
            logs.append({'nivel': 'info', 'msg': f'{label}: 🔄 Estratégia E — requests Python: {final_url[:70]}'})
            try:
                import requests as _req
                cookies_e = {}
                try:
                    for c in page.context.cookies():
                        cookies_e[c['name']] = c['value']
                except Exception:
                    pass
                ua = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/120.0.0.0 Safari/537.36')
                _proxies_e = _get_proxy_requests()
                r_e = _req.get(
                    final_url,
                    cookies=cookies_e,
                    proxies=_proxies_e,
                    timeout=25,
                    allow_redirects=True,
                    headers={'User-Agent': ua, 'Referer': page.url}
                )
                ct_e  = r_e.headers.get('content-type', '').lower()
                url_e = r_e.url
                saved = _salvar_corpo(url_e, ct_e, r_e.content)
                if saved:
                    _remove_ctx_listeners()
                    _cleanup_listeners()
                    return f'Download: {saved}' if saved.endswith('.pdf') else f'Nova aba: {url_e}'
                elif len(r_e.text) > 300:
                    logs.append({'nivel': 'info', 'msg': f'{label}: 📄 E retornou HTML: {len(r_e.text)} chars'})
                    _remove_ctx_listeners()
                    _cleanup_listeners()
                    return f'Nova aba: {url_e}'
            except Exception as e_e:
                logs.append({'nivel': 'aviso', 'msg': f'{label}: ⚠️ E falhou: {str(e_e)[:60]}'})

            # ═══════════════════════════════════════════════════════════════════
            # ESTRATÉGIA F — HTML capturado pelo context listener (último recurso)
            # ═══════════════════════════════════════════════════════════════════
            for cap_url, cap_ct, cap_body in _ctx_responses:
                if cap_body and len(cap_body) > 300:
                    saved = _salvar_corpo(cap_url, cap_ct, cap_body)
                    if saved:
                        _remove_ctx_listeners()
                        _cleanup_listeners()
                        return f'Nova aba: {cap_url}'

            # Tudo falhou — ao menos retornar a URL para o loop externo tentar
            logs.append({'nivel': 'aviso', 'msg': f'{label}: ⚠️ Todas as estratégias falharam — retornando URL'})
            _remove_ctx_listeners()
            _cleanup_listeners()
            return f'Nova aba: {final_url}'
        else:
            el.click()
        
        logs.append({'nivel': 'info', 'msg': f'{label}: 🖱️ Clicou em "{texto[:30]}" (via {estrategia_usada})'})
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
        # Browser pode ter morrido — usar URL interceptada (melhor que href estático)
        _real_url = _nav_url_real[0] or _pdf_response_url[0] or (href_extraido if href_extraido and href_extraido.startswith('http') else None)
        if _real_url:
            logs.append({'nivel': 'info', 'msg': f'{label}: 🔄 Browser morreu — URL real interceptada: {_real_url[:80]}'})
            return f'Navegou: {_real_url}'
    
    # URL mudou?
    try:
        page.wait_for_load_state('networkidle', timeout=5000)
        if page.url != url_antes:
            logs.append({'nivel': 'info', 'msg': f'{label}: 🖱️ Navegou para: {page.url[:60]}'})
            return f'Navegou: {page.url}'
    except Exception:
        _real_url = _nav_url_real[0] or _pdf_response_url[0] or (href_extraido if href_extraido and href_extraido.startswith('http') else None)
        if _real_url:
            logs.append({'nivel': 'info', 'msg': f'{label}: 🔄 Browser morreu — URL real interceptada: {_real_url[:80]}'})
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
    
    # Se interceptou navegação real, retornar
    if _nav_url_real[0]:
        logs.append({'nivel': 'info', 'msg': f'{label}: 🔗 Navegação real interceptada: {_nav_url_real[0][:80]}'})
        return f'Navegou: {_nav_url_real[0]}'
    
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
        logs.append({'nivel': 'info', 'msg': f'{label}: ✏️ Digitou "{texto[:20]}" em "{label_campo[:20]}"'})
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
                            el.click()
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

    for passo in range(1, max_passos + 1):
        try:
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
            o_que_vejo = (decisao.get('o_que_vejo', '') or '')[:100]
            pensamento = decisao.get('decisao', '') or ''
            acao = decisao.get('acao', {}) or {}
            tipo_acao = acao.get('tipo', '') or ''

            logs.append({'nivel': 'info', 'msg': f'{label}: 👁️ Passo {passo}: {o_que_vejo}'})
            logs.append({'nivel': 'info', 'msg': f'{label}: 🧠 Decisão: {tipo_acao} — {pensamento[:80]}'})

            # 6. Legislacao encontrada? Só aceitar se ação é "concluido" (não junto com "clicar" etc)
            leg = decisao.get('legislacao_encontrada', {}) or {}
            leg_url = (leg.get('url', '') or '').strip()

            if leg.get('encontrada') and leg_url and leg_url != '#' and leg_url.startswith('http'):
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
                    logs.append({'nivel': 'aviso', 'msg': f'{label}: ❌ Passo {passo}: IA desistiu — {pensamento[:80]}'})
                    historico.append({'passo': passo, 'acao': 'desistir', 'resultado': pensamento[:100]})
                    break

            # 8. Detectar loop
            if len(historico) >= 2:
                tipos_recentes = [h['acao'].split(' ')[0] for h in historico[-2:]]
                if all(t == tipo_acao for t in tipos_recentes):
                    resultados_recentes = [h['resultado'] for h in historico[-2:]]
                    if len(set(resultados_recentes)) == 1:
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

Este PDF contém o TEXTO INTEGRAL (completo, com artigos) da "{tipo_lei} nº {numero_lei}/{ano_lei}" do municipio de {municipio_lei}?

ATENCAO: NAO confunda com simples CITACOES ou REFERENCIAS. Se a lei e apenas MENCIONADA em um despacho, portaria ou outro ato, a resposta e NAO.
Somente responda SIM se o PDF contiver o CABEÇALHO FORMAL da lei seguido dos seus artigos (Art. 1º, Art. 2º, etc).

Responda APENAS com JSON:
{{"encontrada": true ou false, "motivo": "explique brevemente"}}

TEXTO DO PDF:
{amostra}"""
                        
                        resp_verif = chamar_llm(prompt_verif, logs, f'{label} verif PDF')
                        
                        if resp_verif:
                            try:
                                import re as _re_v
                                resp_v = resp_verif.strip()
                                if resp_v.startswith('```'):
                                    resp_v = resp_v.split('\n', 1)[-1]
                                if resp_v.endswith('```'):
                                    resp_v = resp_v.rsplit('```', 1)[0]
                                json_m = _re_v.search(r'\{[^}]+\}', resp_v)
                                if json_m:
                                    verif = json.loads(json_m.group())
                                    legislacao_no_pdf = verif.get('encontrada', False)
                                    motivo = verif.get('motivo', '')[:100]
                                    if legislacao_no_pdf:
                                        logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ IA confirmou: {motivo}'})
                                    else:
                                        logs.append({'nivel': 'info', 'msg': f'{label}: ❌ IA disse não: {motivo}'})
                            except Exception:
                                pass
                    
                except Exception as e_pdf:
                    logs.append({'nivel': 'info', 'msg': f'{label}: ⚠️ Erro ao verificar PDF: {str(e_pdf)[:40]}'})
                
                if legislacao_no_pdf:
                    resultado['encontrada'] = True
                    resultado['confirmacao'] = 'Legislação confirmada no PDF pela IA'
                    logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ PDF confirmado — encerrando navegação'})
                    break
                else:
                    logs.append({'nivel': 'info', 'msg': f'{label}: 📅 Legislação não está nesta edição — IA deve tentar o dia seguinte'})
                    historico.append({
                        'passo': passo,
                        'acao': 'download_verificado',
                        'resultado': f'PDF baixado mas legislação NÃO encontrada nesta edição. Tentar próximo dia.'
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

    if not resultado['encontrada']:
        logs.append({'nivel': 'aviso', 'msg': f'{label}: Navegação encerrada após {len(historico)} passos sem encontrar'})

    return resultado
