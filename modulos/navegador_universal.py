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
- "scroll": rolar pagina ("baixo" ou "cima")
- "concluido": encontrei a legislacao (informar URL)
- "desistir": nao consigo encontrar

DICAS:
- Coordenadas normalizadas: 0,0 = canto superior esquerdo, 1000,1000 = canto inferior direito
- Formularios: preencha rapido e submeta. Sites ASP.NET resetam campos — nao entre em loop.
- Icone na coluna "Arquivo" = link para o documento. Clique nele.
- So marque "encontrada" quando tiver URL real (http...), nao apenas por ver na tabela.
- Se Cloudflare/CAPTCHA, desista.

JSON (sem markdown):
{{
    "o_que_vejo": "...",
    "decisao": "o que vou fazer e por que",
    "acao": {{
        "tipo": "clicar|digitar|selecionar|scroll|concluido|desistir",
        "x": 500,
        "y": 450,
        "texto": "",
        "direcao": ""
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
