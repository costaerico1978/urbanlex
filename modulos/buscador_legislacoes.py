#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
modulos/buscador_legislacoes.py
────────────────────────────────
Motor de busca de legislações. Três modos:

1. AUTOMÁTICO: detecta novos municípios via plataforma → busca tudo
2. REFERÊNCIAS: varre legislações existentes → detecta referências a leis não cadastradas
3. MANUAL: busca por campos (esfera, estado, município, número, palavras-chave)
"""

import os
import re
import json
import logging
from datetime import datetime
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

GEMINI_API_KEY = os.getenv('GEMINI_API_KEY', '')
GROQ_API_KEY = os.getenv('GROQ_API_KEY', '')


def _db():
    import psycopg2
    from psycopg2.extras import RealDictCursor
    return psycopg2.connect(os.getenv('DATABASE_URL'), cursor_factory=RealDictCursor)


def _qry(sql, params=None, fetch='all', commit=False):
    conn = _db()
    cur = conn.cursor()
    cur.execute(sql, params)
    if commit:
        conn.commit()
    result = None
    if fetch == 'one':
        result = cur.fetchone()
    elif fetch == 'all':
        result = cur.fetchall()
    cur.close()
    conn.close()
    return result


def _registrar_atividade(tipo, mensagem, detalhes=None):
    try:
        _qry("""INSERT INTO feed_atividades (tipo, mensagem, detalhes, criado_em)
                VALUES (%s, %s, %s, NOW())""",
             (tipo, mensagem, json.dumps(detalhes or {}, default=str)),
             commit=True, fetch=None)
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# 1. BUSCA POR MUNICÍPIO (automático ou manual)
# ─────────────────────────────────────────────────────────────────────────────

def buscar_municipio(municipio: str, estado: str,
                     cadastrar: bool = False, monitorar: bool = True) -> dict:
    """
    Busca todas as legislações urbanísticas de um município.
    Combina descobridor_diario + descobridor_legislacoes.

    Returns:
        {diario, legislacoes_encontradas, legislacoes_cadastradas}
    """
    resultado = {
        'diario': None,
        'legislacoes_encontradas': [],
        'legislacoes_cadastradas': [],
    }

    # Descobrir diário oficial
    try:
        from modulos.descobridor_diario import descobrir_diario
        diario = descobrir_diario(municipio, estado, 'municipal')
        resultado['diario'] = diario
    except Exception as e:
        logger.warning(f"Erro ao descobrir diário: {e}")
        resultado['diario'] = {'url': '', 'origem': 'erro', 'mensagem': str(e)[:200]}

    # Descobrir legislações
    try:
        from modulos.descobridor_legislacoes import (
            descobrir_legislacoes_municipio, cadastrar_legislacoes_descobertas
        )
        legislacoes = descobrir_legislacoes_municipio(municipio, estado)
        resultado['legislacoes_encontradas'] = legislacoes

        if cadastrar and legislacoes:
            ids = cadastrar_legislacoes_descobertas(
                legislacoes, municipio, estado,
                ativar_monitoramento=monitorar
            )
            resultado['legislacoes_cadastradas'] = ids
    except Exception as e:
        logger.error(f"Erro ao descobrir legislações: {e}")

    # Registrar na fila
    _registrar_busca_fila(
        tipo='municipio_novo',
        municipio=municipio, estado=estado,
        legislacoes_encontradas=len(resultado['legislacoes_encontradas']),
        legislacoes_cadastradas=len(resultado['legislacoes_cadastradas']),
        status='concluido',
        detalhes=resultado,
    )

    return resultado


# ─────────────────────────────────────────────────────────────────────────────
# 2. BUSCA MANUAL (por campos)
# ─────────────────────────────────────────────────────────────────────────────

def _extrair_texto_html(html_raw: str) -> str:
    """Remove tags HTML e retorna texto limpo."""
    txt = re.sub(r'<script[^>]*>.*?</script>', '', html_raw, flags=re.DOTALL)
    txt = re.sub(r'<style[^>]*>.*?</style>', '', txt, flags=re.DOTALL)
    txt = re.sub(r'<[^>]+>', ' ', txt)
    txt = re.sub(r'\s+', ' ', txt).strip()
    return txt


def _chamar_llm(prompt: str, logs: list, label: str = 'LLM', max_retries: int = 2) -> Optional[str]:
    """Chama Gemini ou GROQ e retorna texto. Retry com backoff para 429."""
    import requests as req
    import time
    texto = None

    for tentativa in range(max_retries + 1):
        if tentativa > 0:
            wait = tentativa * 3  # 3s, 6s
            logs.append({'nivel': 'info', 'msg': f'{label}: aguardando {wait}s antes de retry #{tentativa}...'})
            time.sleep(wait)

        # Tentar Gemini
        if GEMINI_API_KEY:
            try:
                import google.generativeai as genai
                genai.configure(api_key=GEMINI_API_KEY)
                model = genai.GenerativeModel('gemini-2.5-flash')
                response = model.generate_content(prompt)
                texto = response.text.strip()
                logs.append({'nivel': 'ok', 'msg': f'{label}: Gemini respondeu ({len(texto)} chars)'})
                return texto
            except Exception as e:
                err = str(e)[:120]
                is_rate_limit = '429' in err or 'quota' in err.lower() or 'rate' in err.lower()
                logs.append({'nivel': 'aviso', 'msg': f'{label}: Gemini falhou: {err}'})
                if not is_rate_limit:
                    break  # Erro não recuperável, pula pro GROQ

        # Tentar GROQ
        if GROQ_API_KEY:
            try:
                resp = req.post(
                    'https://api.groq.com/openai/v1/chat/completions',
                    headers={'Authorization': f'Bearer {GROQ_API_KEY}', 'Content-Type': 'application/json'},
                    json={
                        'model': 'llama-3.3-70b-versatile',
                        'messages': [
                            {'role': 'system', 'content': 'Analise dados e responda APENAS com JSON válido, sem markdown.'},
                            {'role': 'user', 'content': prompt}
                        ],
                        'temperature': 0.1, 'max_tokens': 2000,
                    },
                    timeout=30,
                )
                if resp.status_code == 200:
                    texto = resp.json()['choices'][0]['message']['content'].strip()
                    logs.append({'nivel': 'ok', 'msg': f'{label}: GROQ respondeu ({len(texto)} chars)'})
                    return texto
                elif resp.status_code == 429:
                    logs.append({'nivel': 'aviso', 'msg': f'{label}: GROQ rate limit (429) — tentativa {tentativa + 1}/{max_retries + 1}'})
                    continue  # Vai pro retry
                else:
                    logs.append({'nivel': 'erro', 'msg': f'{label}: GROQ HTTP {resp.status_code}'})
            except Exception as e:
                logs.append({'nivel': 'erro', 'msg': f'{label}: GROQ falhou: {str(e)[:120]}'})

        # Se nenhum funcionou nesta tentativa, tentar retry
        if tentativa < max_retries:
            continue
        break

    return None


def _llm_triar_snippets(todos_snippets: list, descricao_busca: str, logs: list) -> dict:
    """
    ETAPA INTELIGENTE: LLM analisa os snippets do DuckDuckGo e:
    1. Seleciona quais URLs vale a pena acessar
    2. Extrai a data de publicação da legislação (se encontrar)
    Retorna {'urls': [...], 'data_publicacao': 'YYYY-MM-DD' ou ''}
    """
    if not todos_snippets:
        return {'urls': [], 'data_publicacao': '', 'tipo_legislacao': ''}

    # Montar lista numerada de snippets
    snippets_texto = '\n'.join([
        f"[{i+1}] FONTE: {s['fonte_tipo']}\n    URL: {s['url']}\n    TÍTULO: {s['titulo']}\n    SNIPPET: {s['snippet']}"
        for i, s in enumerate(todos_snippets[:20])
    ])

    prompt = f"""Você é um especialista em legislação brasileira. Analise os resultados de busca.

LEGISLAÇÃO BUSCADA:
{descricao_busca}

RESULTADOS DE BUSCA:
{snippets_texto}

FAÇA TRÊS COISAS:

1) SELECIONE URLs relevantes:
- OBRIGATÓRIO: O snippet ou título DEVE mencionar a EXPRESSÃO COMPLETA da legislação (ex: "Lei Complementar 198" ou "LC 198"), NÃO apenas o número solto.
- PRIORIZE: PDFs, câmaras municipais, sites legislativos
- REJEITE: Homepages genéricas, páginas "Quem Somos", outro tipo de ato com mesmo número
- REJEITE: Diários oficiais ESTADUAIS se a busca é MUNICIPAL

2) EXTRAIA A DATA DE PUBLICAÇÃO da legislação, se algum snippet mencionar. Procure datas no formato "de 14 de janeiro de 2019", "14/01/2019", "publicada em 2019-01-14", etc.

3) IDENTIFIQUE O TIPO DA LEGISLAÇÃO se não foi informado na busca. Olhe os snippets e determine se é Lei Complementar, Lei Ordinária, Decreto, Resolução, Portaria, Emenda, etc.

Responda SOMENTE com JSON:
{{
    "urls_selecionadas": [1, 3, 7],
    "data_publicacao": "2019-01-14",
    "tipo_legislacao": "Lei Complementar",
    "justificativa_breve": "Resultado 1 menciona LC 198 com data 14/01/2019..."
}}

- urls_selecionadas: números entre colchetes, 1-5 itens. Lista VAZIA se nenhum é relevante.
- data_publicacao: formato YYYY-MM-DD. Vazio "" se não encontrou data em nenhum snippet.
- tipo_legislacao: tipo completo (ex: "Lei Complementar", "Decreto", "Lei Ordinária"). Vazio "" se não identificou."""

    logs.append({'nivel': 'info', 'msg': f'🧠 Triagem IA: analisando {len(todos_snippets)} snippets...'})
    texto = _chamar_llm(prompt, logs, '🧠 Triagem', max_retries=0)

    resultado = {'urls': [], 'data_publicacao': '', 'tipo_legislacao': ''}

    if not texto:
        logs.append({'nivel': 'aviso', 'msg': '🧠 Triagem IA falhou — usando todos os resultados'})
        resultado['urls'] = [s['url'] for s in todos_snippets[:5]]
        return resultado

    try:
        texto = re.sub(r'^```json\s*', '', texto)
        texto = re.sub(r'\s*```$', '', texto)
        dados = json.loads(texto)
        indices = dados.get('urls_selecionadas', [])
        data_pub_extraida = dados.get('data_publicacao', '')
        tipo_extraido = dados.get('tipo_legislacao', '')
        justificativa = dados.get('justificativa_breve', '')

        if justificativa:
            logs.append({'nivel': 'ok', 'msg': f'🧠 Triagem: {justificativa[:150]}'})

        # Converter índices para URLs
        for idx in indices:
            if isinstance(idx, int) and 1 <= idx <= len(todos_snippets):
                resultado['urls'].append(todos_snippets[idx - 1]['url'])

        # Validar data extraída
        if data_pub_extraida and re.match(r'\d{4}-\d{2}-\d{2}', data_pub_extraida):
            resultado['data_publicacao'] = data_pub_extraida
            logs.append({'nivel': 'ok', 'msg': f'📅 Data de publicação encontrada: {data_pub_extraida}'})
        elif data_pub_extraida:
            logs.append({'nivel': 'aviso', 'msg': f'📅 Data extraída em formato inválido: {data_pub_extraida}'})

        # Tipo da legislação identificado pela IA
        if tipo_extraido:
            resultado['tipo_legislacao'] = tipo_extraido.strip()
            logs.append({'nivel': 'ok', 'msg': f'📋 Tipo identificado pela IA: {tipo_extraido}'})

        logs.append({'nivel': 'ok', 'msg': f'🧠 Triagem: {len(resultado["urls"])} URLs selecionadas'})

        if not resultado['urls']:
            resultado['urls'] = [s['url'] for s in todos_snippets[:3]]

        return resultado
    except Exception as e:
        logs.append({'nivel': 'aviso', 'msg': f'🧠 JSON inválido: {str(e)[:80]}'})
        resultado['urls'] = [s['url'] for s in todos_snippets[:5]]
        return resultado


def _pesquisar_web(query: str, logs: list, label: str, max_results: int = 5) -> list:
    """
    Pesquisa na web usando ddgs (DuckDuckGo Search).
    Retorna lista de {'url', 'titulo', 'snippet'}.
    """
    resultados = []
    try:
        from ddgs import DDGS
        with DDGS() as ddgs:
            results = list(ddgs.text(query, max_results=max_results, region='br-pt'))
        for r in results:
            resultados.append({
                'url': r.get('href', ''),
                'titulo': r.get('title', ''),
                'snippet': r.get('body', ''),
            })
        logs.append({'nivel': 'ok' if resultados else 'aviso',
                     'msg': f'{label}: {len(resultados)} resultado(s) encontrados'})
        for r in resultados[:3]:
            logs.append({'nivel': 'info', 'msg': f'  → {r["titulo"][:60]} ({r["url"][:60]})'})
    except ImportError:
        logs.append({'nivel': 'erro', 'msg': f'{label}: pacote ddgs não instalado. Adicione ddgs ao requirements.txt'})
    except Exception as e:
        logs.append({'nivel': 'aviso', 'msg': f'{label}: erro na busca: {str(e)[:120]}'})
    return resultados


def _buscar_leismunicipais_direto(municipio: str, estado: str, tipo: str, numero: str, ano: str, logs: list) -> list:
    """
    Busca diretamente no LeisMunicipais.com.br sem depender de DuckDuckGo.
    Constrói URLs baseadas nos padrões conhecidos do site.
    """
    import requests as req
    from urllib.parse import quote_plus

    resultados = []
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
        'Accept-Language': 'pt-BR,pt;q=0.9',
        'Referer': 'https://leismunicipais.com.br/',
    }

    # Normalizar nome do município para URL (rio-de-janeiro, sao-paulo, etc.)
    mun_slug = municipio.strip().lower()
    mun_slug = mun_slug.replace(' ', '-').replace("'", '')
    import unicodedata
    mun_slug = unicodedata.normalize('NFD', mun_slug)
    mun_slug = re.sub(r'[\u0300-\u036f]', '', mun_slug)  # Remove acentos

    estado_slug = (estado or '').strip().lower()[:2]

    # Construir termos de busca
    termos = ' '.join(filter(None, [tipo, numero, ano]))

    # Estratégia 1: Página de busca do LeisMunicipais
    try:
        url_busca = f'https://leismunicipais.com.br/pesquisa/{estado_slug}/{mun_slug}?q={quote_plus(termos)}'
        resp = req.get(url_busca, headers=headers, timeout=10, allow_redirects=True)
        if resp.status_code == 200 and 'html' in resp.headers.get('Content-Type', '').lower():
            html = resp.text
            # Extrair links de resultados
            links = re.findall(r'<a\s[^>]*href=["\']([^"\']*leismunicipais\.com\.br[^"\']*)["\'][^>]*>(.*?)</a>',
                              html, re.IGNORECASE | re.DOTALL)
            num_norm = (numero or '').strip()
            for href, texto in links:
                texto_limpo = re.sub(r'<[^>]+>', '', texto).strip()
                # Filtrar: deve conter o número da lei
                if num_norm and num_norm in texto_limpo:
                    if not any(r['url'] == href for r in resultados):
                        resultados.append({
                            'url': href,
                            'titulo': texto_limpo[:120],
                            'snippet': f'LeisMunicipais busca: {texto_limpo[:200]}',
                        })
    except Exception:
        pass

    # Estratégia 2: URL direta baseada em padrão conhecido
    if not resultados and numero and municipio:
        # Mapear tipo para slug
        tipo_map = {
            'lei complementar': 'lei-complementar',
            'lei ordinária': 'lei-ordinaria',
            'lei ordinaria': 'lei-ordinaria',
            'lei': 'lei-ordinaria',
            'decreto': 'decreto',
            'decreto-lei': 'decreto-lei',
            'resolução': 'resolucao',
            'resolucao': 'resolucao',
            'portaria': 'portaria',
            'emenda': 'emenda-a-lei-organica',
        }
        tipo_slug = tipo_map.get((tipo or '').strip().lower(), '')
        num = numero.strip()

        if tipo_slug:
            # Padrão: /a1/rj/r/rio-de-janeiro/lei-complementar/2024/27/270/lei-complementar-n-270-2024
            prefixo_num = num[:len(num)-1] if len(num) > 1 else '0'
            url_direta = f'https://leismunicipais.com.br/a1/{estado_slug}/{mun_slug[0]}/{mun_slug}/{tipo_slug}/{ano or ""}/{prefixo_num}/{num}/{tipo_slug}-n-{num}-{ano or ""}'
            try:
                resp = req.get(url_direta, headers=headers, timeout=10, allow_redirects=True)
                if resp.status_code == 200 and len(resp.text) > 1000:
                    resultados.append({
                        'url': resp.url,  # URL final após redirects
                        'titulo': f'{tipo} {num}/{ano} - LeisMunicipais',
                        'snippet': f'Acesso direto ao LeisMunicipais: {resp.url}',
                    })
            except Exception:
                pass

    if resultados:
        logs.append({'nivel': 'ok', 'msg': f'📖 LeisMunicipais (acesso direto): {len(resultados)} resultado(s)'})
        for r in resultados[:3]:
            logs.append({'nivel': 'info', 'msg': f'  → {r["titulo"][:60]} ({r["url"][:60]})'})
    else:
        logs.append({'nivel': 'info', 'msg': '📖 LeisMunicipais: acesso direto sem resultados — usando DuckDuckGo...'})

    return resultados


def _navegar_formulario_com_ia(url_base: str, tipo_lei: str, numero_lei: str,
                               ano: str, data_pub: str, logs: list, label: str) -> list:
    """
    Usa Playwright para abrir a página como um navegador real, pede pra IA
    analisar o formulário e preencher os campos, submete, e extrai resultados.
    Funciona para qualquer portal de legislação (ASP, PHP, AngularJS, etc).
    """
    import time as _time

    # ── Verificar se Playwright está disponível ──
    try:
        from playwright.sync_api import sync_playwright
    except (ImportError, Exception) as e:
        logs.append({'nivel': 'info', 'msg': f'{label}: Playwright não disponível ({str(e)[:60]}) — pulando navegação'})
        return []

    resultados = []

    # Normalizar URL
    if url_base and not url_base.startswith('http'):
        url_base = 'https://' + url_base

    logs.append({'nivel': 'info', 'msg': f'{label}: 🌐 Abrindo página com navegador real...'})

    # Detectar Chromium do sistema (mesmo approach do browser_pool.py)
    executable_path = os.environ.get('PLAYWRIGHT_CHROMIUM_PATH', '')
    if not executable_path:
        import shutil, glob as _glob
        for chromium_name in ['chromium', 'chromium-browser', 'google-chrome-stable', 'google-chrome']:
            path = shutil.which(chromium_name)
            if path:
                executable_path = path
                break
        # Caminhos fixos comuns
        if not executable_path:
            for p in ['/usr/bin/chromium', '/usr/bin/chromium-browser',
                      '/nix/store/chromium/bin/chromium']:
                if os.path.isfile(p):
                    executable_path = p
                    break
        # Glob no Nix store
        if not executable_path:
            nix_paths = _glob.glob('/nix/store/*/bin/chromium')
            if nix_paths:
                executable_path = nix_paths[0]

    if executable_path:
        logs.append({'nivel': 'info', 'msg': f'{label}: 🌐 Chromium encontrado: {executable_path}'})

    try:
        with sync_playwright() as pw:
            launch_args = {
                'headless': True,
                'args': ['--no-sandbox', '--disable-dev-shm-usage',
                         '--disable-gpu', '--single-process', '--no-zygote']
            }
            if executable_path:
                launch_args['executable_path'] = executable_path

            try:
                browser = pw.chromium.launch(**launch_args)
            except Exception as e1:
                if executable_path:
                    # Se falhou com sistema, tentar sem executable_path (Playwright bundled)
                    logs.append({'nivel': 'info', 'msg': f'{label}: 🌐 Chromium sistema falhou, tentando bundled...'})
                    del launch_args['executable_path']
                    browser = pw.chromium.launch(**launch_args)
                else:
                    raise e1
            ctx = browser.new_context(
                viewport={'width': 1280, 'height': 900},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            page = ctx.new_page()

            # 1) Abrir a página
            try:
                page.goto(url_base, wait_until='networkidle', timeout=20000)
            except Exception:
                try:
                    page.goto(url_base, wait_until='domcontentloaded', timeout=15000)
                except Exception as e:
                    logs.append({'nivel': 'aviso', 'msg': f'{label}: Não conseguiu abrir: {str(e)[:80]}'})
                    browser.close()
                    return []

            _time.sleep(1)
            url_atual = page.url

            # 2) Verificar se a página carregou (não é 404 ou WAF)
            titulo_pagina = page.title() or ''
            body_text = (page.inner_text('body') or '')[:500]
            if 'request rejected' in body_text.lower() or 'access denied' in body_text.lower():
                logs.append({'nivel': 'aviso', 'msg': f'{label}: WAF bloqueou acesso'})
                browser.close()
                return []

            # 3) Extrair estrutura dos formulários e campos
            form_info = page.evaluate('''() => {
                const info = { forms: [], inputs: [], selects: [], buttons: [], links_pdf: [] };

                // Formulários
                document.querySelectorAll('form').forEach((f, i) => {
                    info.forms.push({
                        idx: i, action: f.action || '', method: f.method || 'get',
                        id: f.id || '', name: f.name || ''
                    });
                });

                // Inputs (text, hidden, etc)
                document.querySelectorAll('input').forEach(inp => {
                    if (inp.type === 'hidden' && !inp.name) return;
                    const label = inp.closest('label')?.textContent?.trim() ||
                                  (inp.id && document.querySelector('label[for="'+inp.id+'"]')?.textContent?.trim()) || '';
                    info.inputs.push({
                        tag: 'input', type: inp.type || 'text',
                        name: inp.name || '', id: inp.id || '',
                        value: inp.value || '', placeholder: inp.placeholder || '',
                        label: label.substring(0, 80),
                        visible: inp.offsetParent !== null
                    });
                });

                // Selects com opções
                document.querySelectorAll('select').forEach(sel => {
                    const label = sel.closest('label')?.textContent?.trim() ||
                                  (sel.id && document.querySelector('label[for="'+sel.id+'"]')?.textContent?.trim()) || '';
                    const opts = [];
                    sel.querySelectorAll('option').forEach(o => {
                        opts.push({ value: o.value, text: o.textContent.trim().substring(0, 60) });
                    });
                    info.selects.push({
                        tag: 'select', name: sel.name || '', id: sel.id || '',
                        label: label.substring(0, 80),
                        selected: sel.value,
                        options: opts.slice(0, 30),  // limitar
                        visible: sel.offsetParent !== null
                    });
                });

                // Botões de submit
                document.querySelectorAll('input[type="submit"], button[type="submit"], input[type="button"], button').forEach(btn => {
                    info.buttons.push({
                        tag: btn.tagName.toLowerCase(), type: btn.type || '',
                        name: btn.name || '', id: btn.id || '',
                        value: btn.value || '', text: btn.textContent?.trim()?.substring(0, 40) || '',
                        visible: btn.offsetParent !== null
                    });
                });

                // Links para PDFs já visíveis
                document.querySelectorAll('a[href]').forEach(a => {
                    const href = a.href || '';
                    if (href.toLowerCase().includes('.pdf')) {
                        info.links_pdf.push({ url: href, text: a.textContent?.trim()?.substring(0, 80) || '' });
                    }
                });

                return info;
            }''')

            # Se já tem PDFs relevantes na página, retornar direto
            num = (numero_lei or '').strip()
            if form_info.get('links_pdf') and num:
                for link in form_info['links_pdf']:
                    if num in link.get('url', '') or num in link.get('text', ''):
                        resultados.append({
                            'url': link['url'],
                            'titulo': link.get('text', '') or f'PDF: {tipo_lei} {num}',
                            'snippet': f'Link direto na página: {link.get("text", "")[:120]}',
                        })
                if resultados:
                    logs.append({'nivel': 'ok', 'msg': f'{label}: 🌐 Encontrou {len(resultados)} PDF(s) direto na página!'})
                    browser.close()
                    return resultados

            # Se não tem formulários, não tem o que preencher
            has_form = bool(form_info.get('forms')) or bool(form_info.get('inputs')) or bool(form_info.get('selects'))
            if not has_form:
                logs.append({'nivel': 'info', 'msg': f'{label}: 🌐 Página não tem formulário de busca'})
                browser.close()
                return []

            # 4) Pedir pra IA analisar o formulário e dizer como preencher
            descricao = f'{tipo_lei or "?"} nº {numero_lei or "?"}/{ano or "?"}'
            if data_pub:
                descricao += f' (publicada em {data_pub})'

            prompt = f"""Analise este formulário de busca de legislação e diga como preenchê-lo para encontrar: {descricao}

FORMULÁRIOS: {json.dumps(form_info.get('forms', []), ensure_ascii=False)}

CAMPOS INPUT: {json.dumps([i for i in form_info.get('inputs', []) if i.get('visible') or i.get('type') == 'hidden'], ensure_ascii=False)}

CAMPOS SELECT: {json.dumps([s for s in form_info.get('selects', []) if s.get('visible')], ensure_ascii=False)}

BOTÕES: {json.dumps([b for b in form_info.get('buttons', []) if b.get('visible')], ensure_ascii=False)}

TÍTULO DA PÁGINA: {titulo_pagina}

Responda APENAS com JSON neste formato:
{{
  "campos": [
    {{"seletor": "#id_campo ou [name=nome]", "tipo": "input|select", "valor": "valor_a_preencher"}},
    ...
  ],
  "botao_submit": "#id_botao ou seletor CSS do botão de enviar",
  "confianca": 0.0 a 1.0
}}

REGRAS:
- Para SELECT, use o VALUE da option, não o texto
- Identifique qual option corresponde ao tipo de legislação (ex: "Lei Complementar" pode ser value "6" ou "19")
- Campos de número: coloque só o número (ex: "270")
- Campos de data: formato dd/mm/aaaa
- Se um campo não se aplica, omita
- confianca: 0.9+ se identificou os campos com certeza, <0.5 se incerto"""

            llm_resp = _chamar_llm(prompt, logs, f'{label} IA')

            if not llm_resp:
                logs.append({'nivel': 'aviso', 'msg': f'{label}: IA não respondeu — não consegue preencher formulário'})
                browser.close()
                return []

            # Parsear resposta
            try:
                llm_resp_clean = re.sub(r'^```(?:json)?\s*|\s*```$', '', llm_resp.strip())
                instrucoes = json.loads(llm_resp_clean)
            except (json.JSONDecodeError, ValueError) as e:
                logs.append({'nivel': 'aviso', 'msg': f'{label}: IA retornou JSON inválido: {str(e)[:60]}'})
                browser.close()
                return []

            confianca = instrucoes.get('confianca', 0)
            if confianca < 0.4:
                logs.append({'nivel': 'aviso', 'msg': f'{label}: IA com baixa confiança ({confianca}) — pulando'})
                browser.close()
                return []

            # 5) Preencher os campos
            campos = instrucoes.get('campos', [])
            preenchidos = 0
            for campo in campos:
                sel = campo.get('seletor', '')
                valor = str(campo.get('valor', ''))
                tipo = campo.get('tipo', 'input')
                if not sel or not valor:
                    continue

                try:
                    el = page.query_selector(sel)
                    if not el:
                        # Tentar variações comuns
                        for alt in [sel.replace('#', ''), f'[id="{sel.lstrip("#")}"]', f'[name="{sel.lstrip("#")}"]']:
                            el = page.query_selector(alt)
                            if el:
                                break

                    if not el:
                        logs.append({'nivel': 'info', 'msg': f'{label}: Campo não encontrado: {sel}'})
                        continue

                    if tipo == 'select':
                        el.select_option(value=valor)
                    else:
                        el.click()
                        el.fill(valor)

                    preenchidos += 1
                    logs.append({'nivel': 'info', 'msg': f'{label}: ✏️ Preencheu {sel} = "{valor}"'})
                except Exception as e:
                    logs.append({'nivel': 'aviso', 'msg': f'{label}: Erro ao preencher {sel}: {str(e)[:60]}'})

            if preenchidos == 0:
                logs.append({'nivel': 'aviso', 'msg': f'{label}: Nenhum campo preenchido — abortando'})
                browser.close()
                return []

            # 6) Submeter formulário
            botao_sel = instrucoes.get('botao_submit', '')
            logs.append({'nivel': 'info', 'msg': f'{label}: 🖱️ Submetendo formulário...'})

            submeteu = False
            if botao_sel:
                try:
                    btn = page.query_selector(botao_sel)
                    if btn:
                        btn.click()
                        submeteu = True
                except Exception:
                    pass

            if not submeteu:
                # Tentar clicar qualquer botão submit visível
                for sel_try in ['input[type="submit"]', 'button[type="submit"]',
                                'input[type="button"]', 'button', 'a.btn']:
                    try:
                        btn = page.query_selector(sel_try)
                        if btn and btn.is_visible():
                            btn.click()
                            submeteu = True
                            break
                    except Exception:
                        continue

            if not submeteu:
                # Último recurso: Enter no último campo preenchido
                try:
                    page.keyboard.press('Enter')
                    submeteu = True
                except Exception:
                    pass

            if not submeteu:
                logs.append({'nivel': 'aviso', 'msg': f'{label}: Não conseguiu submeter formulário'})
                browser.close()
                return []

            # 7) Aguardar resultados
            try:
                page.wait_for_load_state('networkidle', timeout=15000)
            except Exception:
                _time.sleep(3)

            url_resultado = page.url
            resultado_html = page.content()

            # 8) Extrair links e conteúdo da página de resultados
            resultado_info = page.evaluate('''() => {
                const info = { links: [], textos: [], title: document.title || '' };

                document.querySelectorAll('a[href]').forEach(a => {
                    const href = a.href || '';
                    const text = a.textContent?.trim()?.substring(0, 120) || '';
                    if (href && text && href !== '#' && !href.startsWith('javascript:')) {
                        info.links.push({ url: href, text: text });
                    }
                });

                // Capturar texto principal da página (pode ser o resultado direto)
                const main = document.querySelector('main, .content, .resultado, #conteudo, #resultado, table, .panel') || document.body;
                info.textos.push(main?.innerText?.substring(0, 3000) || '');

                return info;
            }''')

            # Filtrar links relevantes
            num = (numero_lei or '').strip()
            tipo_lower = (tipo_lei or '').lower()

            for link in resultado_info.get('links', []):
                link_url = link.get('url', '')
                link_text = link.get('text', '')
                combined = f'{link_url} {link_text}'.lower()

                # Relevante se contém o número da lei
                if num and (num in link_text or num in link_url):
                    if not any(r['url'] == link_url for r in resultados):
                        resultados.append({
                            'url': link_url,
                            'titulo': link_text[:120] or f'{tipo_lei} {num}',
                            'snippet': f'Encontrado via formulário: {link_text[:200]}',
                        })
                # Ou se é um PDF (qualquer)
                elif '.pdf' in link_url.lower():
                    if not any(r['url'] == link_url for r in resultados):
                        resultados.append({
                            'url': link_url,
                            'titulo': link_text[:120] or 'PDF',
                            'snippet': f'PDF na página de resultados: {link_text[:200]}',
                        })

            # Se a página de resultados contém a legislação diretamente (não precisa de link)
            texto_resultado = '\n'.join(resultado_info.get('textos', []))
            if not resultados and num and num in texto_resultado:
                # A página de resultados É o conteúdo da lei
                resultados.append({
                    'url': url_resultado,
                    'titulo': f'{tipo_lei} nº {num}/{ano} — conteúdo direto',
                    'snippet': texto_resultado[:300],
                    '_texto_direto': texto_resultado,
                })

            browser.close()

            if resultados:
                logs.append({'nivel': 'ok', 'msg': f'{label}: 🌐 Navegação encontrou {len(resultados)} resultado(s)!'})
                for r in resultados[:3]:
                    logs.append({'nivel': 'info', 'msg': f'  → {r["titulo"][:60]}'})
            else:
                page_text_preview = texto_resultado[:150].replace('\n', ' ')
                logs.append({'nivel': 'info', 'msg': f'{label}: 🌐 Formulário submetido mas sem resultados relevantes. Página: "{page_text_preview}..."'})

    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        logs.append({'nivel': 'aviso', 'msg': f'{label}: 🌐 Erro na navegação: {str(e)[:120]}'})
        logs.append({'nivel': 'info', 'msg': f'{label}: 🌐 Detalhe: {tb[-200:]}'})
        # Garantir browser fechado
        try:
            browser.close()  # noqa
        except Exception:
            pass

    return resultados


def _buscar_no_site_direto(url_base: str, tipo_lei: str, numero_lei: str, ano: str, logs: list, label: str, data_pub: str = '') -> list:
    """
    Acessa diretamente o site informado e busca links/conteúdo relevantes.
    Estratégia:
    0) PRIMEIRO: Playwright + IA (navegador real, preenche formulário)
    1) FALLBACK: Tenta acessar a URL via requests
    2) Se for portal de busca, tenta query params comuns
    3) Varre o HTML procurando links que contenham o tipo/número da lei
    Retorna lista no mesmo formato de _pesquisar_web: [{'url', 'titulo', 'snippet'}]
    """
    import requests as req
    from urllib.parse import urljoin, urlparse, urlencode, parse_qs, urlunparse

    resultados = []
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
        'Accept-Language': 'pt-BR,pt;q=0.9',
    }

    # Termos de busca para o site
    termos_site = ' '.join(filter(None, [tipo_lei, numero_lei, ano]))
    if not termos_site.strip():
        return resultados

    # Normalizar URL
    if url_base and not url_base.startswith('http'):
        url_base = 'https://' + url_base

    # ── ESTRATÉGIA 0: Navegador real (Playwright + IA) ──
    nav_results = _navegar_formulario_com_ia(url_base, tipo_lei, numero_lei, ano, data_pub, logs, label)
    if nav_results:
        return nav_results

    # ── FALLBACK: abordagem requests (sites sem WAF/JS) ──

    urls_tentar = [url_base]

    # Gerar variações de URL com query params comuns (para portais de busca)
    parsed = urlparse(url_base)
    params_busca = ['q', 'busca', 'pesquisa', 'search', 'termo', 'texto']
    for param in params_busca:
        qs = parse_qs(parsed.query)
        qs[param] = [termos_site]
        nova_query = urlencode(qs, doseq=True)
        nova_url = urlunparse(parsed._replace(query=nova_query))
        if nova_url != url_base:
            urls_tentar.append(nova_url)

    # Padrão para encontrar links relevantes
    tipo_norm = (tipo_lei or '').lower().replace(' ', r'[\s_\-]*')
    num_norm = (numero_lei or '').strip()
    padrao_link_texto = None
    if tipo_norm and num_norm:
        padrao_link_texto = re.compile(rf'{tipo_norm}[\s\S]{{0,20}}{num_norm}', re.IGNORECASE)
    elif num_norm:
        padrao_link_texto = re.compile(rf'\b{num_norm}\b')

    for i, url_tentar in enumerate(urls_tentar[:4]):
        try:
            resp = req.get(url_tentar, headers=headers, timeout=10, allow_redirects=True)
            if resp.status_code != 200:
                continue

            content_type = resp.headers.get('Content-Type', '').lower()

            # Se retornou PDF direto, é resultado
            if 'pdf' in content_type or url_tentar.lower().endswith('.pdf'):
                resultados.append({
                    'url': url_tentar,
                    'titulo': f'PDF direto: {tipo_lei} {numero_lei}',
                    'snippet': f'Documento PDF encontrado em {urlparse(url_tentar).netloc}',
                })
                break

            # HTML: extrair links
            if 'html' not in content_type and 'text' not in content_type:
                continue

            html = resp.text
            html_lower = html.lower()

            # Verificar se o HTML contém menção à legislação
            check_text = f'{(tipo_lei or "").lower()} {num_norm}'.strip().lower()
            # Buscar links <a href="...">
            links = re.findall(r'<a\s[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', html, re.IGNORECASE | re.DOTALL)

            for href, texto_link in links:
                texto_limpo = re.sub(r'<[^>]+>', '', texto_link).strip()
                href_full = urljoin(url_tentar, href)

                # Verificar se o link ou texto é relevante
                match_texto = padrao_link_texto and padrao_link_texto.search(texto_limpo)
                match_href = num_norm and (num_norm in href or f'LC{num_norm}' in href.upper() or f'LC{num_norm}M' in href.upper())

                if match_texto or match_href:
                    # Evitar duplicatas
                    if not any(r['url'] == href_full for r in resultados):
                        resultados.append({
                            'url': href_full,
                            'titulo': texto_limpo[:100] or f'Link em {urlparse(url_tentar).netloc}',
                            'snippet': f'Encontrado no site: {texto_limpo[:200]}',
                        })

            # Se achou resultados na URL principal, não precisa tentar query params
            if resultados and i == 0:
                break

        except Exception as e:
            logs.append({'nivel': 'aviso', 'msg': f'{label}: erro ao acessar {url_tentar[:60]}: {str(e)[:60]}'})

    # ── Heurísticas de URL: tentar padrões conhecidos de PDFs de legislação ──
    if not resultados and num_norm:
        import requests as req_h
        from urllib.parse import urljoin

        # Mapear tipo para prefixos comuns em URLs de portais legislativos
        tipo_lower = (tipo_lei or '').lower()
        prefixos_url = []
        if 'complementar' in tipo_lower:
            prefixos_url = [f'LC{num_norm}', f'LC{num_norm}M', f'lc{num_norm}', f'lc{num_norm}m']
        elif 'decreto' in tipo_lower:
            prefixos_url = [f'D{num_norm}', f'D{num_norm}M', f'DEC{num_norm}', f'dec{num_norm}']
        elif tipo_lower:
            prefixos_url = [f'L{num_norm}', f'L{num_norm}M', f'LEI{num_norm}']
        else:
            # Tipo desconhecido: tentar ambos
            prefixos_url = [f'LC{num_norm}', f'LC{num_norm}M', f'L{num_norm}', f'D{num_norm}']

        # Caminhos comuns em portais de legislação
        caminhos_pdf = ['Arquivos/PDF/', 'arquivos/pdf/', 'documentos/', 'pdf/', 'legislacao/']

        parsed_base = urlparse(url_base)
        base_url = f'{parsed_base.scheme}://{parsed_base.netloc}'

        # Tentar derivar o caminho base a partir da URL informada
        path_parts = parsed_base.path.rstrip('/').rsplit('/', 1)
        caminhos_base = [parsed_base.path.rstrip('/') + '/'] if parsed_base.path and parsed_base.path != '/' else []
        caminhos_base.append('/')

        urls_heuristicas = []
        for cb in caminhos_base[:2]:
            for cp in caminhos_pdf:
                for pf in prefixos_url:
                    urls_heuristicas.append(urljoin(base_url + cb, cp + pf))

        # Testar até 8 URLs heurísticas com HEAD rápido
        headers_probe = dict(headers)
        headers_probe['Referer'] = base_url + '/'
        for url_h in urls_heuristicas[:8]:
            try:
                resp_h = req_h.head(url_h, headers=headers_probe, timeout=5, allow_redirects=True)
                ct = resp_h.headers.get('Content-Type', '').lower()
                if resp_h.status_code == 200 and ('pdf' in ct or 'octet' in ct or url_h.endswith('.pdf')):
                    resultados.append({
                        'url': url_h,
                        'titulo': f'PDF heurístico: {tipo_lei or "Legislação"} {num_norm}',
                        'snippet': f'PDF encontrado em {parsed_base.netloc} via padrão de URL',
                    })
                    logs.append({'nivel': 'ok', 'msg': f'{label}: 🔎 PDF encontrado via heurística: {url_h[:80]}'})
                    break
            except Exception:
                continue

    if resultados:
        logs.append({'nivel': 'ok', 'msg': f'{label}: 🔎 Acesso direto encontrou {len(resultados)} link(s) relevante(s)'})
        for r in resultados[:3]:
            logs.append({'nivel': 'info', 'msg': f'  → {r["titulo"][:60]} ({r["url"][:60]})'})
    else:
        logs.append({'nivel': 'info', 'msg': f'{label}: acesso direto não encontrou links — usando DuckDuckGo...'})

    return resultados


def _extrair_links_anexos(html_raw: str, url_base: str, logs: list, label: str) -> list:
    """
    Busca links de download de anexos no HTML original.
    Retorna lista de {'url': ..., 'titulo': ...}
    """
    from urllib.parse import urljoin
    links_anexos = []

    # Padrões de links que indicam anexos
    # 1) Links com texto contendo "anexo" ou "download"
    padrao_link = re.findall(
        r'<a\s[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>',
        html_raw, re.IGNORECASE | re.DOTALL
    )
    for href, texto_link in padrao_link:
        texto_limpo = re.sub(r'<[^>]+>', '', texto_link).strip().lower()
        href_lower = href.lower()

        # Detectar se é link de anexo
        is_anexo = False
        titulo = texto_limpo[:80]

        # Texto do link menciona "anexo"
        if 'anexo' in texto_limpo:
            is_anexo = True
        # URL contém "anexo"
        elif 'anexo' in href_lower:
            is_anexo = True
        # Link para PDF/DOC perto de texto "anexo" — verificar pelo contexto
        elif any(ext in href_lower for ext in ['.pdf', '.doc', '.docx', '.xls', '.xlsx']):
            # Verificar se tem "anexo" no contexto próximo (300 chars antes do link)
            pos_href = html_raw.lower().find(href.lower())
            if pos_href >= 0:
                contexto_antes = html_raw[max(0, pos_href - 300):pos_href].lower()
                if 'anexo' in contexto_antes:
                    is_anexo = True

        if is_anexo and href:
            url_completa = urljoin(url_base, href)
            # Evitar duplicatas
            if not any(a['url'] == url_completa for a in links_anexos):
                links_anexos.append({'url': url_completa, 'titulo': titulo or 'Anexo'})

    if links_anexos:
        logs.append({'nivel': 'ok', 'msg': f'{label}: 📎 {len(links_anexos)} link(s) de anexo encontrado(s)'})
        for a in links_anexos[:5]:
            logs.append({'nivel': 'info', 'msg': f'  📎 {a["titulo"][:50]} → {a["url"][:60]}'})

    return links_anexos[:10]  # Max 10 anexos


def _baixar_anexos(links_anexos: list, headers: dict, logs: list, label: str) -> str:
    """Baixa anexos (PDF/HTML) e retorna o texto concatenado."""
    import requests as req
    textos_anexos = []

    for i, anexo in enumerate(links_anexos[:5]):  # Max 5 downloads
        url = anexo['url']
        titulo = anexo['titulo']
        try:
            resp = req.get(url, headers=headers, timeout=15, allow_redirects=True)
            if resp.status_code != 200:
                logs.append({'nivel': 'aviso', 'msg': f'{label}: anexo "{titulo}" HTTP {resp.status_code}'})
                continue

            content_type = resp.headers.get('Content-Type', '').lower()
            texto_anexo = ''

            if 'pdf' in content_type or url.lower().endswith('.pdf'):
                # Extrair texto do PDF
                pdf_bytes = resp.content
                if len(pdf_bytes) > 25_000_000:
                    logs.append({'nivel': 'aviso', 'msg': f'{label}: anexo PDF muito grande ({len(pdf_bytes)//1024}KB)'})
                    continue
                try:
                    import fitz
                    doc = fitz.open(stream=pdf_bytes, filetype='pdf')
                    pages = []
                    for page in doc:
                        pages.append(page.get_text())
                    doc.close()
                    texto_anexo = ' '.join(pages)
                    texto_anexo = re.sub(r'\s+', ' ', texto_anexo).strip()
                    logs.append({'nivel': 'ok', 'msg': f'{label}: 📎 Anexo "{titulo}" — PDF {len(doc)} págs, {len(texto_anexo)} chars'})
                except Exception as e:
                    logs.append({'nivel': 'aviso', 'msg': f'{label}: falha ao ler PDF do anexo: {str(e)[:60]}'})
                    continue
            else:
                # HTML
                texto_anexo = _extrair_texto_html(resp.text)
                if len(texto_anexo) > 100:
                    logs.append({'nivel': 'ok', 'msg': f'{label}: 📎 Anexo "{titulo}" — HTML {len(texto_anexo)} chars'})

            if texto_anexo and len(texto_anexo) > 100:
                textos_anexos.append(f'\n\n=== ANEXO: {titulo} ===\n{texto_anexo}')

        except Exception as e:
            logs.append({'nivel': 'aviso', 'msg': f'{label}: erro ao baixar anexo "{titulo}": {str(e)[:60]}'})

    return ''.join(textos_anexos)


def _verificar_se_referencia(texto_norm: str, match_pos: int, match_text: str, tipo_lei: str, numero_lei: str) -> dict:
    """
    Verifica se uma menção à legislação encontrada no texto é o CABEÇALHO ORIGINAL
    ou apenas uma REFERÊNCIA/CITAÇÃO dentro de outro documento.

    Retorna {'eh_referencia': bool, 'motivo': str, 'eh_cabecalho': bool}
    """
    # Extrair contexto ANTES da menção (120 chars)
    ctx_antes = texto_norm[max(0, match_pos - 120):match_pos].strip()
    # Extrair contexto DEPOIS (200 chars)
    ctx_depois = texto_norm[match_pos + len(match_text):match_pos + len(match_text) + 200].strip()

    # ── Indicadores de REFERÊNCIA (outro documento cita a lei) ──
    # Preposições/expressões que precedem citações
    # Artigos opcionais no final: a, o, as, os, ao, à, aos, às
    _art = r'(?:\s+(?:a|o|as|os|ao|à|aos|às|da|do|das|dos|na|no|nas|nos))?\s*$'
    padroes_ref_antes = [
        r'(?:da|na|pela|conforme|previsto|considerando)' + _art,
        r'nos\s+termos\s+d[aeo]' + _art,
        r'art(?:igo)?\.?\s+\d+.*?d[aeo]' + _art,
        r'§\s*\d+.*?d[aeo]' + _art,
        r'inciso.*?d[aeo]' + _art,
        r'exigid[ao]s?\s+(?:na|pela)' + _art,
        r'disposto\s+n[ao]' + _art,
        r'acordo\s+com' + _art,
        r'termos\s+d[aeo]' + _art,
        r'alteraç[ãa]o\s+d[aeo]' + _art,
        r'revogaç[ãa]o\s+d[aeo]' + _art,
        r'regulament(?:ada?|o)\s+(?:pela?|n[ao])' + _art,
        r'estabelecid[ao]\s+(?:na|pela)' + _art,
        r'mencionad[ao]\s+n[ao]' + _art,
        r'citad[ao]\s+n[ao]' + _art,
        r'referid[ao]\s+n[ao]' + _art,
        r'definid[ao]\s+(?:na|pela)' + _art,
        r'com\s+base\s+n[ao]' + _art,
        r'segundo' + _art,
        r'(?:instituíd[ao]|criado|previsto)\s+(?:pela|na|no)' + _art,
        r'combinado\s+com' + _art,
        # Novos: traço/travessão antes (indica complemento de título de OUTRO ato)
        r'[-–—]\s*$',
        # Parêntese aberto (referência parentética)
        r'\(\s*$',
        # "anexo da", "anexos da"
        r'anex[oa]s?\s+d[aeo]' + _art,
        # "prevista na/pela"
        r'previst[ao]s?\s+(?:na|pela|no|pelo)' + _art,
        # "nos moldes da"
        r'moldes\s+d[aeo]' + _art,
        # "de acordo com a"
        r'de\s+acordo\s+com' + _art,
        # "altera" / "revoga" / "regulamenta" + a/o
        r'(?:altera|revoga|regulamenta|modifica|complementa)\s+' + _art,
        # Conjunções com artigo: "e a", "ou a", "e da", "bem como a"
        r'\b(?:e|ou)\s+(?:a|o|as|os|da|do|das|dos)\s*$',
        r'bem\s+como\s+(?:a|o|as|os|da|do)\s*$',
        # "pela" sozinho no final
        r'\bpela\s*$',
        r'\bpelo\s*$',
    ]

    for padrao in padroes_ref_antes:
        if re.search(padrao, ctx_antes, re.IGNORECASE):
            return {'eh_referencia': True, 'eh_cabecalho': False,
                    'motivo': f'Precedido por referência: "...{ctx_antes[-40:]}"'}

    # ── Indicadores de REFERÊNCIA no contexto DEPOIS ──
    # Padrões FORTES: indicam referência independente da posição
    padroes_ref_depois_fortes = [
        # "estão especificadas", "são definidos", etc. = outro doc falando sobre a lei
        r'^[^.]{0,30}\b(?:est[aã]o|s[aã]o|ficam|foram|ser[aã]o)\s+(?:especificad|definid|estabelecid|previst|regulamentad)',
        # Fechamento com parêntese
        r'^[^)]{0,60}\)',
    ]
    for padrao in padroes_ref_depois_fortes:
        if re.search(padrao, ctx_depois, re.IGNORECASE):
            return {'eh_referencia': True, 'eh_cabecalho': False,
                    'motivo': f'Seguido por referência: "{ctx_depois[:50]}..."'}

    # Padrões FRACOS: só aplicar se NÃO está no início do documento (ementa real pode ter "que dispõe")
    if match_pos > 300:
        padroes_ref_depois_fracos = [
            # "que dispõe sobre" num contexto de citação (não no início = não é ementa)
            r'^,?\s*que\s+(?:disp[oõ]e|trata|versa|regulamenta)',
        ]
        for padrao in padroes_ref_depois_fracos:
            if re.search(padrao, ctx_depois, re.IGNORECASE):
                return {'eh_referencia': True, 'eh_cabecalho': False,
                        'motivo': f'Seguido por referência: "{ctx_depois[:50]}..."'}

    # Verificar se o documento é de OUTRO TIPO (decreto citando lei, etc.)
    tipo_norm = (tipo_lei or '').lower().replace('á','a').replace('ã','a').replace('ç','c').replace('ó','o')
    outros_tipos = ['decreto', 'resolucao', 'portaria', 'emenda', 'lei ordinaria', 'ato normativo', 'instrucao normativa']
    if tipo_norm:
        # Procurar cabeçalhos de OUTROS tipos de legislação no início do documento (primeiros 1500 chars)
        inicio_doc = texto_norm[:1500]
        for outro in outros_tipos:
            if outro == tipo_norm or outro in tipo_norm or tipo_norm in outro:
                continue  # Mesmo tipo, não é conflito
            # Padrão flexível: "DECRETO [palavras opcionais] Nº 12345"
            tipo_palavras = outro.replace(' ', r'\s+')
            padrao_outro = re.compile(
                r'\b' + tipo_palavras + r'(?:\s+\w+){0,3}\s+(?:no?\.?\s*)\d+',
                re.IGNORECASE
            )
            if padrao_outro.search(inicio_doc):
                return {'eh_referencia': True, 'eh_cabecalho': False,
                        'motivo': f'Documento é {outro.upper()} que cita a {tipo_norm}'}

    # ── Indicadores de CABEÇALHO ORIGINAL ──
    # A menção está no início do documento (primeiros 600 chars)?
    if match_pos < 600:
        # E logo depois vem data ou ementa?
        if re.search(r'(?:de\s+)?\d{1,2}\s+de\s+\w+\s+de\s+\d{4}', ctx_depois[:100]):
            return {'eh_referencia': False, 'eh_cabecalho': True,
                    'motivo': f'Cabeçalho no início do documento com data'}

    # A menção é precedida por um cabeçalho formal?
    padroes_cabecalho_antes = [
        r'(?:poder\s+executivo|poder\s+legislativo|camara\s+municipal|prefeitura|governo)\s*$',
        r'(?:diario\s+oficial|publicacao)\s*$',
        r'atos?\s+do\s+(?:prefeito|governador|executivo|legislativo)\s*$',
    ]
    for padrao in padroes_cabecalho_antes:
        if re.search(padrao, ctx_antes, re.IGNORECASE):
            return {'eh_referencia': False, 'eh_cabecalho': True,
                    'motivo': f'Precedido por cabeçalho institucional'}

    # ── Caso ambíguo: verificar proporção do conteúdo ──
    # Se o texto é curto (< 5000 chars) e a menção está distante do início, provavelmente é referência
    if len(texto_norm) < 5000 and match_pos > 500:
        return {'eh_referencia': True, 'eh_cabecalho': False,
                'motivo': f'Documento curto ({len(texto_norm)} chars) com menção distante do início (pos {match_pos})'}

    # Não conclusivo
    return {'eh_referencia': False, 'eh_cabecalho': False,
            'motivo': 'Não determinado (mantido por padrão)'}


def _acessar_pagina(url: str, termos: str, headers: dict, logs: list, label: str, tipo_lei: str = '', numero_lei: str = '') -> Optional[dict]:
    """Acessa uma URL, detecta se é HTML ou PDF, extrai texto e calcula relevância."""
    import requests as req
    from urllib.parse import urlparse
    # Garantir que URL tem protocolo
    if url and not url.startswith('http'):
        url = 'https://' + url
    # Pular URLs vazias ou inválidas
    if not url or len(url) < 10:
        return None
    try:
        resp = req.get(url, headers=headers, timeout=12, allow_redirects=True, stream=True)

        # Retry com Referer no 403 (bypass WAF básico)
        if resp.status_code == 403:
            parsed = urlparse(url)
            headers_retry = dict(headers)
            headers_retry['Referer'] = f'{parsed.scheme}://{parsed.netloc}/'
            headers_retry['Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
            resp = req.get(url, headers=headers_retry, timeout=12, allow_redirects=True, stream=True)

        if resp.status_code != 200:
            logs.append({'nivel': 'aviso', 'msg': f'{label}: HTTP {resp.status_code} em {url[:60]}'})
            return None

        content_type = resp.headers.get('Content-Type', '').lower()
        texto = ''
        _pagina1_texto = ''  # Texto da pág 1 do PDF (para validação de data do DO)

        # ── PDF ──
        if 'pdf' in content_type or url.lower().endswith('.pdf') or '/download/' in url.lower():
            logs.append({'nivel': 'info', 'msg': f'{label}: Detectado PDF, baixando e extraindo texto...'})
            try:
                pdf_bytes = resp.content
                if len(pdf_bytes) > 25_000_000:  # max 25MB
                    logs.append({'nivel': 'aviso', 'msg': f'{label}: PDF muito grande ({len(pdf_bytes)//1024}KB), pulando'})
                    return None
                import fitz  # PyMuPDF
                doc = fitz.open(stream=pdf_bytes, filetype='pdf')
                total_pages = len(doc)

                # Se temos número da lei, busca inteligente: escaneia TODAS as páginas
                if numero_lei and numero_lei.strip() and total_pages > 20:
                    logs.append({'nivel': 'info', 'msg': f'{label}: PDF grande ({total_pages} págs) — escaneando por "{numero_lei}"...'})

                    # Guardar página 1 para validação de data (cabeçalho do DO)
                    _pagina1_texto = re.sub(r'\s+', ' ', doc[0].get_text()).strip()[:2000] if total_pages > 0 else ''
                    # PASSO 1: Encontrar a página onde a lei COMEÇA (CABEÇALHO, não referência)
                    num = numero_lei.strip()
                    pagina_inicio = -1
                    pagina_referencia = -1  # Página que apenas CITA a lei

                    # Preposições que indicam REFERÊNCIA (não é o cabeçalho da lei)
                    # Sem \s+ no final porque trecho_antes é .strip()
                    preposicoes_ref = r'(?:da|na|pela|conforme|previsto|nos\s+termos\s+d[aeo]|art(?:igo)?\.?\s+\d+.*?d[aeo]|§\s*\d+.*?d[aeo]|inciso.*?d[aeo]|exigid[ao]s?\s+(?:na|pela)|disposto\s+na|acordo\s+com\s+[ao]|termos\s+d[aeo])$'

                    for i in range(total_pages):
                        page_text = doc[i].get_text()
                        pn = re.sub(r'\s+', ' ', page_text.lower())
                        pn = pn.replace('º', 'o').replace('°', 'o').replace('.', '')

                        if tipo_lei:
                            tip = tipo_lei.strip().lower().replace('á','a').replace('ã','a').replace('ç','c').replace('ó','o')
                            abrevs_map = {
                                'lei complementar': ['lei complementar', 'lc'],
                                'lei ordinaria': ['lei ordinaria', 'lei'],
                                'decreto': ['decreto', 'dec'],
                                'resolucao': ['resolucao', 'res'],
                            }
                            tipos_check = [tip]
                            for k, v in abrevs_map.items():
                                if k in tip or tip in k:
                                    tipos_check = v
                                    break

                            for tc in tipos_check:
                                tr = r'\s+'.join(re.escape(w) for w in tc.split())
                                sep = r'(?:\s+(?:no?\s*))?\s*'
                                padrao_tipo_num = tr + sep + re.escape(num)

                                # Buscar TODAS as ocorrências na página
                                for m in re.finditer(padrao_tipo_num, pn):
                                    pos = m.start()
                                    trecho_antes = pn[max(0, pos - 60):pos].strip()

                                    # Checar se é CABEÇALHO: 
                                    # 1) Seguido de ", de DD de MMMM" (data de publicação)
                                    trecho_depois = pn[m.end():m.end() + 40]
                                    eh_cabecalho = bool(re.search(r'^\s*[,/]?\s*de\s+\d{1,2}\s+de\s+', trecho_depois))

                                    # 2) NÃO precedido de preposição (da, na, pela, conforme...)
                                    eh_referencia = bool(re.search(preposicoes_ref, trecho_antes))

                                    if eh_cabecalho and not eh_referencia:
                                        pagina_inicio = i
                                        ctx = pn[max(0, pos-20):m.end()+50]
                                        logs.append({'nivel': 'ok', 'msg': f'{label}: ✓ CABEÇALHO encontrado pág {i+1}: ...{ctx}...'})
                                        break
                                    elif pagina_referencia < 0:
                                        # Guardar como referência (pra log)
                                        pagina_referencia = i
                                        ctx = pn[max(0, pos-40):m.end()+30]
                                        logs.append({'nivel': 'info', 'msg': f'{label}: pág {i+1} apenas CITA a lei: ...{ctx}...'})

                                if pagina_inicio >= 0:
                                    break
                        elif num in page_text:
                            pagina_inicio = i
                            break

                        if pagina_inicio >= 0:
                            break

                    if pagina_inicio < 0 and pagina_referencia >= 0:
                        logs.append({'nivel': 'aviso', 'msg': f'{label}: PDF cita a lei (pág {pagina_referencia+1}) mas NÃO contém o texto original — DESCARTADO'})
                        doc.close()
                        return None
                    if pagina_inicio >= 0:
                        logs.append({'nivel': 'ok', 'msg': f'{label}: início da lei na pág {pagina_inicio + 1}'})

                        # PASSO 2: Ler em lotes, IA decide quando a legislação terminou
                        paginas_lei = []
                        chunk_size = 40
                        fim_encontrado = False
                        tipo_desc = tipo_lei or 'legislação'

                        for batch_start in range(pagina_inicio, total_pages, chunk_size):
                            batch_end = min(batch_start + chunk_size, total_pages)

                            # Ler páginas deste lote
                            for i in range(batch_start, batch_end):
                                paginas_lei.append((i, doc[i].get_text()))

                            # Contexto: primeira página (referência) + últimas 3 páginas
                            primeira = paginas_lei[0]
                            ultimas = paginas_lei[-3:]
                            resumo_ultimas = f"--- PÁGINA {primeira[0]+1} (INÍCIO DA LEI) ---\n{primeira[1][:400]}\n\n[...]\n\n"
                            resumo_ultimas += '\n'.join([
                                f"--- PÁGINA {p[0]+1} ---\n{p[1][:1200]}"
                                for p in ultimas
                            ])

                            total_lidas = len(paginas_lei)
                            logs.append({'nivel': 'info', 'msg': f'{label}: lidas {total_lidas} págs (até pág {batch_end})...'})

                            # Não checar no primeiro lote (lei acabou de começar)
                            if batch_start == pagina_inicio:
                                continue

                            # Perguntar à IA se a legislação ainda continua
                            prompt_fim = f"""Estou extraindo a "{tipo_desc} nº {num}" de um PDF de Diário Oficial.
A legislação começou na página {pagina_inicio + 1}. Já li {total_lidas} páginas.

Aqui está a PRIMEIRA PÁGINA (referência) e as ÚLTIMAS 3 PÁGINAS lidas:
{resumo_ultimas}

PERGUNTA: O conteúdo destas páginas AINDA FAZ PARTE da mesma legislação ({tipo_desc} nº {num})?

ESTRUTURA TÍPICA DE UMA LEGISLAÇÃO BRASILEIRA (nesta ordem):
1. CABEÇALHO: tipo, número, data, ementa
2. CORPO: artigos, parágrafos, incisos, alíneas organizados em títulos, capítulos, seções
3. DISPOSIÇÕES FINAIS/TRANSITÓRIAS: últimos artigos ("Esta Lei entra em vigor...")
4. ASSINATURA: nome do prefeito/governador/presidente, local e data
5. ANEXOS (opcionais): tabelas, mapas, quadros, plantas — podem ter MUITAS páginas
   - Os anexos FAZEM PARTE da legislação! Não pare nos anexos.
   - Anexos podem conter tabelas numéricas, mapas de zoneamento, quadros de parâmetros urbanísticos, etc.

A LEGISLAÇÃO SÓ TERMINA quando:
- Aparece o início de OUTRO ato normativo completamente diferente (outro decreto, outra lei, portaria de nomeação, edital de licitação, ato do poder executivo não relacionado)
- Aparece cabeçalho de outra edição do Diário Oficial
- O conteúdo muda completamente para assuntos administrativos sem relação (nomeações, exonerações, licitações)

A LEGISLAÇÃO NÃO TERMINOU se:
- Está em anexos, tabelas, mapas, quadros (mesmo que pareçam só números)
- Está em disposições transitórias
- Tem a assinatura do prefeito MAS depois vêm anexos
- Referencia artigos ou seções da mesma lei

Responda SOMENTE com JSON:
{{"status": "continua"}} ou {{"status": "terminou", "ultima_pagina": NNN}}
onde NNN é o número da ÚLTIMA página que ainda faz parte da legislação (incluindo anexos)."""

                            resp_fim = _chamar_llm(prompt_fim, logs, f'📄 Leitura pág {batch_end}', max_retries=0)
                            if resp_fim:
                                try:
                                    resp_fim = re.sub(r'^```json\s*', '', resp_fim)
                                    resp_fim = re.sub(r'\s*```$', '', resp_fim)
                                    dados_fim = json.loads(resp_fim)
                                    if dados_fim.get('status') == 'terminou':
                                        ultima_pag = dados_fim.get('ultima_pagina', batch_end)
                                        paginas_lei = [(p, t) for p, t in paginas_lei if p + 1 <= ultima_pag]
                                        logs.append({'nivel': 'ok', 'msg': f'{label}: IA detectou fim da legislação na pág {ultima_pag}'})
                                        fim_encontrado = True
                                        break
                                    else:
                                        logs.append({'nivel': 'info', 'msg': f'{label}: IA confirma: legislação continua (pág {batch_end})'})
                                except (json.JSONDecodeError, KeyError):
                                    logs.append({'nivel': 'aviso', 'msg': f'{label}: resposta IA inválida, continuando leitura'})
                            else:
                                logs.append({'nivel': 'aviso', 'msg': f'{label}: IA indisponível — continuando leitura sem checagem'})

                            # Segurança: máximo 400 páginas (nenhuma lei tem mais que isso)
                            if len(paginas_lei) >= 400:
                                logs.append({'nivel': 'aviso', 'msg': f'{label}: limite de 400 páginas atingido — parando'})
                                break

                        doc.close()

                        if paginas_lei:
                            texto = ' '.join(p[1] for p in paginas_lei)
                            texto = re.sub(r'\s+', ' ', texto).strip()
                            pags = [p[0]+1 for p in paginas_lei]
                            logs.append({'nivel': 'ok', 'msg': f'{label}: legislação extraída — págs {pags[0]} a {pags[-1]} ({len(pags)} págs, {len(texto)} chars)'})
                        else:
                            texto = ''
                    else:
                        doc.close()
                        texto = ''
                        logs.append({'nivel': 'aviso', 'msg': f'{label}: legislação NÃO encontrada nas {total_pages} páginas do PDF'})
                else:
                    # PDF pequeno: ler até 30 páginas normalmente
                    pages_text = []
                    for page in doc[:30]:
                        pages_text.append(page.get_text())
                    _pagina1_texto = re.sub(r'\s+', ' ', pages_text[0]).strip()[:2000] if pages_text else ''
                    doc.close()
                    texto = ' '.join(pages_text)
                    texto = re.sub(r'\s+', ' ', texto).strip()
                    logs.append({'nivel': 'ok', 'msg': f'{label}: PDF extraído: {len(texto)} chars de {min(total_pages, 30)} págs'})
            except Exception as e:
                logs.append({'nivel': 'aviso', 'msg': f'{label}: Falha ao ler PDF: {str(e)[:80]}'})
                return None
        else:
            # ── HTML ──
            html_raw = resp.text
            texto = _extrair_texto_html(html_raw)

            # ── Detectar e baixar ANEXOS ──
            # Verificar se o texto menciona anexos
            texto_lower = texto.lower()
            menciona_anexo = bool(re.search(r'\banexo\b', texto_lower))

            if menciona_anexo:
                # Procurar links de download de anexos no HTML original
                links_anexos = _extrair_links_anexos(html_raw, url, logs, label)
                if links_anexos:
                    texto_anexos = _baixar_anexos(links_anexos, headers, logs, label)
                    if texto_anexos:
                        texto += texto_anexos
                        logs.append({'nivel': 'ok', 'msg': f'{label}: texto dos anexos incorporado (+{len(texto_anexos)} chars)'})
                else:
                    logs.append({'nivel': 'info', 'msg': f'{label}: texto menciona ANEXO mas sem links de download (pode ser inline)'})

        if len(texto) < 80:
            logs.append({'nivel': 'aviso', 'msg': f'{label}: pouco conteúdo ({len(texto)} chars)'})
            return None

        # Relevância
        lista_termos = [t.strip().lower() for t in termos.split() if len(t.strip()) > 2]
        matches = sum(1 for t in lista_termos if t in texto.lower())
        relevancia = matches / max(len(lista_termos), 1)

        # FILTRO INTELIGENTE: busca TIPO + NÚMERO com regex flexível (espaços, acentos, pontuação)
        if numero_lei and numero_lei.strip() and tipo_lei and tipo_lei.strip():
            # Normalizar texto: colapsar espaços, remover acentos problemáticos
            texto_norm = re.sub(r'\s+', ' ', texto.lower())
            # Normalizar caracteres especiais comuns em PDFs
            texto_norm = texto_norm.replace('º', 'o').replace('°', 'o').replace('ª', 'a')
            # Remover pontos (abreviações como L.C., n.º) mas manter estrutura
            texto_norm = texto_norm.replace('.', '')
            # Remover vírgulas
            texto_norm = texto_norm.replace(',', ' ')
            texto_norm = re.sub(r'\s+', ' ', texto_norm)

            num = numero_lei.strip()
            tip = tipo_lei.strip().lower()

            # Gerar abreviações do tipo
            abreviacoes = {
                'lei complementar': ['lei complementar', 'lc'],
                'lei ordinária': ['lei ordinaria', 'lei'],
                'lei': ['lei'],
                'decreto': ['decreto', 'dec'],
                'decreto-lei': ['decreto-lei', 'decreto lei', 'dl'],
                'resolução': ['resolucao', 'res'],
                'portaria': ['portaria', 'port'],
                'emenda': ['emenda'],
            }
            tipos_buscar = [tip.replace('á', 'a').replace('ã', 'a').replace('ç', 'c').replace('ó', 'o').replace('ú', 'u')]
            for chave, abrevs in abreviacoes.items():
                if chave in tip or tip in chave:
                    tipos_buscar = abrevs
                    break

            # Gerar REGEX flexível: "lei\s+complementar\s+(?:no?\.?\s*)?198"
            encontrou_padrao = False
            padrao_encontrado = ''
            for t in tipos_buscar:
                # Cada palavra do tipo separada por \s+
                tipo_regex = r'\s+'.join(re.escape(w) for w in t.split())
                # Separadores opcionais: nº, n°, n., no, n, ou nada
                separadores = r'(?:\s+(?:no?\.?\s*))?\s*'
                padrao = tipo_regex + separadores + re.escape(num)
                match = re.search(padrao, texto_norm)
                if match:
                    encontrou_padrao = True
                    padrao_encontrado = match.group()
                    break

            if encontrou_padrao:
                # Mostrar contexto de onde encontrou
                pos = texto_norm.find(padrao_encontrado)
                ctx_start = max(0, pos - 60)
                ctx_end = min(len(texto_norm), pos + len(padrao_encontrado) + 60)
                contexto = texto_norm[ctx_start:ctx_end].strip()
                logs.append({'nivel': 'ok', 'msg': f'{label}: ✓ encontrou "{padrao_encontrado}" → ...{contexto}...'})

                # ── VERIFICAR SE É A LEGISLAÇÃO OU APENAS CITAÇÃO ──
                verif = _verificar_se_referencia(texto_norm, pos, padrao_encontrado, tipo_lei, numero_lei)
                if verif['eh_referencia']:
                    logs.append({'nivel': 'aviso', 'msg': f'{label}: ⚠️ CITAÇÃO detectada — {verif["motivo"]}'})
                    # Verificar se há OUTRA ocorrência que seja o cabeçalho real
                    # Buscar todas as ocorrências do padrão
                    cabecalho_achado = False
                    for t in tipos_buscar:
                        tipo_regex = r'\s+'.join(re.escape(w) for w in t.split())
                        separadores = r'(?:\s+(?:no?\.?\s*))?\s*'
                        padrao_full = tipo_regex + separadores + re.escape(num)
                        for m in re.finditer(padrao_full, texto_norm):
                            if m.start() == pos:
                                continue  # Pular a mesma ocorrência
                            v2 = _verificar_se_referencia(texto_norm, m.start(), m.group(), tipo_lei, numero_lei)
                            if not v2['eh_referencia']:
                                pos = m.start()
                                padrao_encontrado = m.group()
                                cabecalho_achado = True
                                ctx_start2 = max(0, pos - 60)
                                ctx_end2 = min(len(texto_norm), pos + len(padrao_encontrado) + 60)
                                contexto2 = texto_norm[ctx_start2:ctx_end2].strip()
                                logs.append({'nivel': 'ok', 'msg': f'{label}: ✓ CABEÇALHO encontrado em outra posição → ...{contexto2}...'})
                                break
                        if cabecalho_achado:
                            break

                    if not cabecalho_achado:
                        # Todas as ocorrências são citações → DESCARTAR
                        logs.append({'nivel': 'aviso', 'msg': f'{label}: DESCARTADO — documento apenas CITA a legislação, não é o texto original'})
                        return None

                elif verif['eh_cabecalho']:
                    logs.append({'nivel': 'ok', 'msg': f'{label}: ✓ Confirmado como cabeçalho da legislação'})

                else:
                    # CASO AMBÍGUO: não detectou referência NEM cabeçalho
                    # Verificar identidade do documento: o INÍCIO deve conter o tipo+número buscado
                    # Se o documento começa com outro tipo (DECRETO, RESOLUÇÃO, etc), é referência
                    pass

                # ── VALIDAÇÃO POSITIVA: o documento realmente É a legislação? ──
                # Verifica se o tipo+número da lei aparece como CABEÇALHO no início do texto
                inicio_texto = texto_norm[:2000]
                tipo_lower = (tipo_lei or '').lower().replace('á','a').replace('ã','a').replace('ç','c').replace('ó','o')
                if tipo_lower and num:
                    # Montar regex do cabeçalho esperado: "lei complementar [nº] 270"
                    tipo_palavras = tipo_lower.split()
                    tipo_regex_val = r'\s+'.join(re.escape(w) for w in tipo_palavras)
                    sep_val = r'(?:\s+(?:no?\.?\s*))?\s*'
                    padrao_cabecalho = re.compile(tipo_regex_val + sep_val + re.escape(num), re.IGNORECASE)

                    tem_cabecalho_inicio = padrao_cabecalho.search(inicio_texto)

                    if not tem_cabecalho_inicio:
                        # O cabeçalho NÃO está no início → verificar se é outro tipo de documento
                        _tipos_doc = ['decreto', 'resolucao', 'portaria', 'emenda', 'instrucao normativa', 'ato normativo', 'lei ordinaria']
                        for t_doc in _tipos_doc:
                            if t_doc == tipo_lower or t_doc in tipo_lower or tipo_lower in t_doc:
                                continue
                            t_doc_words = t_doc.split()
                            t_doc_regex = r'\s+'.join(re.escape(w) for w in t_doc_words)
                            padrao_outro_tipo = re.compile(r'\b' + t_doc_regex + r'(?:\s+\w+){0,3}\s+(?:no?\.?\s*)?\d+', re.IGNORECASE)
                            match_outro = padrao_outro_tipo.search(inicio_texto)
                            if match_outro:
                                logs.append({'nivel': 'aviso', 'msg': f'{label}: ❌ Documento é "{match_outro.group()[:50]}" — NÃO é {tipo_lei} {num} — DESCARTADO'})
                                return None

                        # Se não é outro tipo mas tb não tem o cabeçalho, verificar se tem Art. 1º
                        # (pode ser a legislação sem cabeçalho formal, ex: texto extraído de PDF)
                        tem_artigo1 = re.search(r'\bart\.?\s*1[oº°]?\b', inicio_texto, re.IGNORECASE)
                        if not tem_artigo1 and pos > 3000:
                            # Menção muito longe do início E sem Art. 1º → provavelmente referência
                            logs.append({'nivel': 'aviso', 'msg': f'{label}: ❌ Menção distante (pos {pos}) sem cabeçalho no início — DESCARTADO'})
                            return None

                relevancia = max(relevancia, 0.6)
            else:
                # DIAGNÓSTICO: mostrar onde o número aparece (pra debug)
                ocorrencias = [m.start() for m in re.finditer(re.escape(num), texto_norm)]
                if ocorrencias:
                    # Mostrar as primeiras 3 ocorrências com contexto
                    exemplos = []
                    for pos in ocorrencias[:3]:
                        ctx_s = max(0, pos - 50)
                        ctx_e = min(len(texto_norm), pos + len(num) + 50)
                        exemplos.append(f'...{texto_norm[ctx_s:ctx_e].strip()}...')
                    logs.append({'nivel': 'aviso', 'msg': f'{label}: "{num}" aparece {len(ocorrencias)}x mas sem "{tipos_buscar[0]}" junto:'})
                    for ex in exemplos:
                        logs.append({'nivel': 'aviso', 'msg': f'  📎 {ex}'})
                else:
                    logs.append({'nivel': 'aviso', 'msg': f'{label}: número "{num}" não encontrado no texto'})
                logs.append({'nivel': 'aviso', 'msg': f'{label}: DESCARTADO (padrão "{tipos_buscar[0]} {num}" não encontrado)'})
                return None

        elif numero_lei and numero_lei.strip():
            if numero_lei.strip() not in texto:
                logs.append({'nivel': 'aviso', 'msg': f'{label}: número "{numero_lei}" NÃO encontrado — DESCARTADO'})
                return None

        logs.append({'nivel': 'ok' if relevancia > 0.3 else 'info',
                     'msg': f'{label}: {len(texto)} chars, {matches}/{len(lista_termos)} termos ({relevancia:.0%} relevância)'})
        return {'url': url, 'texto': texto, 'nome': label, 'relevancia': relevancia, '_pagina1': _pagina1_texto}
    except Exception as e:
        logs.append({'nivel': 'aviso', 'msg': f'{label}: {str(e)[:80]}'})
        return None


def busca_manual(params: dict, log_callback=None) -> dict:
    """
    Busca legislação com pesquisa REAL na internet.
    Fluxo:
    1º Web (sites do usuário, LeisMunicipais, Google) — pra achar a legislação e DATA
    2º LLM tria snippets e extrai data de publicação
    3º Acessa páginas web selecionadas
    4º Diário Oficial — busca por DATA (dia a dia, até +10 dias)
    5º LLM compara todas as fontes, sugere e justifica
    """
    import requests as req

    # Se log_callback fornecido, cada logs.append() dispara o callback em tempo real
    if log_callback:
        class _LogStream(list):
            def append(self, item):
                super().append(item)
                try: log_callback(item)
                except: pass
        logs = _LogStream()
    else:
        logs = []

    # Diagnóstico: primeiro log pra confirmar que entrou na função
    logs.append({'nivel': 'info', 'msg': f'🔧 busca_manual iniciou. Params: {list(params.keys())}'})

    fontes_status = []  # para exibir no frontend

    # ── Extrair parâmetros ──
    esfera = params.get('esfera', '')
    estado = params.get('estado', '')
    municipio = params.get('municipio', '')
    tipo = params.get('tipo', '')
    numero = params.get('numero', '')
    ano = params.get('ano', '')

    # Sanitizar tipo: remover placeholders vazios
    if tipo and tipo.strip() in ('?', '--', '-', 'Selecione', 'selecione', 'Outro', 'outro', ''):
        tipo = ''
    data_pub = params.get('data_publicacao', '')
    assunto = params.get('assunto', '')
    palavras = params.get('palavras_chave', '')
    url_diario = params.get('url_diario', '')
    urls_extras = params.get('urls_extras', [])
    if isinstance(urls_extras, str):
        urls_extras = [u.strip() for u in urls_extras.split('\n') if u.strip()]

    # Termos de busca
    termos_busca = ' '.join(filter(None, [tipo, numero, ano, municipio, assunto, palavras]))
    if not termos_busca.strip():
        return {'legislacoes': [], 'erro': 'Preencha ao menos um campo.', 'logs': logs}

    # Query principal: com aspas para forçar match exato do número/tipo
    partes_query = []
    if tipo and numero:
        partes_query.append(f'"{tipo} {numero}"')  # ex: "Lei Complementar 198"
    elif tipo:
        partes_query.append(f'"{tipo}"')
    elif numero:
        partes_query.append(f'"{numero}"')
    if ano:
        partes_query.append(str(ano))
    if municipio:
        partes_query.append(f'"{municipio}"')
    if not partes_query:
        partes_query = [termos_busca]
    query_google = ' '.join(partes_query)

    # Query alternativa mais curta (para site: searches onde muitos termos atrapalham)
    query_curta = ' '.join(filter(None, [tipo, numero, ano]))

    logs.append({'nivel': 'info', 'msg': f'Termos de busca: "{query_google}"'})
    logs.append({'nivel': 'info', 'msg': f'Query curta (site:): "{query_curta}"'})

    headers_http = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
        'Accept-Language': 'pt-BR,pt;q=0.9',
    }
    textos_extraidos = []
    todos_snippets = []

    # ══════════════════════════════════════════════════════════════════
    # ETAPA 1: BUSCAR NA WEB (sites, LeisMunicipais, Google) — pra achar a DATA
    # ══════════════════════════════════════════════════════════════════
    logs.append({'nivel': 'info', 'msg': '🔍 ETAPA 1: Buscando na web para encontrar a legislação e sua data de publicação...'})

    # ── 1A: Auto-descoberta do Diário Oficial Municipal (guarda pra depois) ──
    if not url_diario and municipio:
        logs.append({'nivel': 'info', 'msg': f'🏛️ Descobrindo URL do Diário Oficial de {municipio}...'})
        busca_do = f'diário oficial município prefeitura "{municipio}" {estado}'
        ddg_do = _pesquisar_web(busca_do, logs, '🏛️ Descoberta DO', max_results=8)
        dominios_do = ['diariooficial', 'doweb', 'dom.', 'diariomunicipal']
        dominios_estaduais = ['ioerj', 'ioesp', 'iomat', 'iof.mg', 'imprensaoficial.com.br']
        for r in ddg_do:
            url_candidata = r['url'].lower()
            titulo = r.get('titulo', '').lower()
            snippet = r.get('snippet', '').lower()
            if any(d in url_candidata for d in dominios_estaduais):
                continue
            is_dominio_do = any(d in url_candidata for d in dominios_do) or 'diario' in url_candidata
            is_municipal = any(x in titulo + snippet for x in ['prefeitura', 'município', 'municipal', municipio.lower()])
            if is_dominio_do and is_municipal:
                dominio = re.sub(r'https?://', '', r['url'].rstrip('/')).split('/')[0]
                url_diario = f'https://{dominio}'
                logs.append({'nivel': 'ok', 'msg': f'🏛️ DO Municipal descoberto: {url_diario} (será usado na etapa 4)'})
                break
        if not url_diario:
            for r in ddg_do:
                url_candidata = r['url'].lower()
                if any(d in url_candidata for d in dominios_estaduais):
                    continue
                if any(d in url_candidata for d in dominios_do) or 'diario' in url_candidata:
                    dominio = re.sub(r'https?://', '', r['url'].rstrip('/')).split('/')[0]
                    url_diario = f'https://{dominio}'
                    logs.append({'nivel': 'ok', 'msg': f'🏛️ DO descoberto (fallback): {url_diario}'})
                    break

    # ── 1B: Sites informados pelo usuário ──
    if urls_extras:
        logs.append({'nivel': 'info', 'msg': f'🔗 Buscando em {len(urls_extras)} site(s) informado(s)...'})
        for i, url_extra in enumerate(urls_extras[:5]):
            dominio = re.sub(r'https?://', '', url_extra.rstrip('/')).split('/')[0]
            site_label = f'🔗 Site #{i+1} ({dominio})'

            # PRIMEIRO: acesso direto ao site (crawl de links)
            direto_results = _buscar_no_site_direto(url_extra, tipo, numero, ano, logs, site_label, data_pub=data_pub)
            if direto_results:
                for d in direto_results:
                    d['fonte_tipo'] = f'🔗 Site #{i+1} ({dominio})'
                    d['_fonte'] = 'site_informado'
                    todos_snippets.append(d)
                fontes_status.append({'nome': f'🔗 Site #{i+1}', 'url': url_extra, 'encontrou': True})
            else:
                # FALLBACK: DuckDuckGo site: search
                ddg_results = _pesquisar_web(f'site:{dominio} {query_curta}', logs, site_label)
                for d in ddg_results:
                    d['fonte_tipo'] = f'🔗 Site #{i+1} ({dominio})'
                    d['_fonte'] = 'site_informado'
                    todos_snippets.append(d)
                fontes_status.append({'nome': f'🔗 Site #{i+1}', 'url': url_extra, 'encontrou': bool(ddg_results)})

    # ── 1C: LeisMunicipais.com.br ──
    if municipio:
        logs.append({'nivel': 'info', 'msg': '📖 Buscando em LeisMunicipais.com.br...'})

        # PRIMEIRO: Acesso direto à API de busca do LeisMunicipais
        lm_direto = _buscar_leismunicipais_direto(municipio, estado, tipo, numero, ano, logs)
        if lm_direto:
            for d in lm_direto:
                d['fonte_tipo'] = '📖 LeisMunicipais'
                d['_fonte'] = 'leismunicipais'
                todos_snippets.append(d)
            fontes_status.append({'nome': '📖 LeisMunicipais', 'url': 'https://leismunicipais.com.br', 'encontrou': True})
        else:
            # FALLBACK: DuckDuckGo
            ddg_results = _pesquisar_web(f'site:leismunicipais.com.br {query_google}', logs, '📖 LeisMunicipais')
            for d in ddg_results:
                d['fonte_tipo'] = '📖 LeisMunicipais'
                d['_fonte'] = 'leismunicipais'
                todos_snippets.append(d)
            fontes_status.append({'nome': '📖 LeisMunicipais', 'url': 'https://leismunicipais.com.br', 'encontrou': bool(ddg_results)})

    # ── 1D: Busca geral ──
    logs.append({'nivel': 'info', 'msg': f'🔎 Busca geral: "{query_google}"...'})
    ddg_results = _pesquisar_web(query_google, logs, '🔎 Web')
    for d in ddg_results:
        if not any(s['url'] == d['url'] for s in todos_snippets):
            d['fonte_tipo'] = '🔎 Web'
            d['_fonte'] = 'google'
            todos_snippets.append(d)
    fontes_status.append({'nome': '🔎 Web', 'url': '', 'encontrou': bool(ddg_results)})

    logs.append({'nivel': 'ok', 'msg': f'🔍 Total: {len(todos_snippets)} resultado(s) coletados'})

    # ══════════════════════════════════════════════════════════════════
    # ETAPA 2: TRIAGEM + EXTRAÇÃO DE DATA (LLM analisa snippets)
    # ══════════════════════════════════════════════════════════════════
    descricao_busca = '\n'.join(filter(None, [
        f"- Esfera: {esfera}" if esfera else None,
        f"- Estado: {estado}" if estado else None,
        f"- Município: {municipio}" if municipio else None,
        f"- Tipo: {tipo}" if tipo else None,
        f"- Número: {numero}" if numero else None,
        f"- Ano: {ano}" if ano else None,
        f"- Data publicação: {data_pub}" if data_pub else None,
        f"- Assunto: {assunto}" if assunto else None,
        f"- Palavras-chave: {palavras}" if palavras else None,
    ]))

    data_descoberta = data_pub  # Se o usuário já informou, usa essa
    tipo_inferido = tipo  # Tipo que será usado nas validações (pode ser enriquecido pela IA)

    if todos_snippets:
        triagem = _llm_triar_snippets(todos_snippets, descricao_busca, logs)
        urls_selecionadas = triagem['urls']
        if not data_descoberta and triagem['data_publicacao']:
            data_descoberta = triagem['data_publicacao']

        # Enriquecer tipo com resultado da triagem IA (se o usuário não informou)
        if not tipo_inferido and triagem.get('tipo_legislacao'):
            tipo_inferido = triagem['tipo_legislacao']
            logs.append({'nivel': 'ok', 'msg': f'📋 Tipo da legislação inferido pela IA: {tipo_inferido} — será usado para validar resultados'})

        # Fallback regex: extrair tipo dos snippets/títulos
        if not tipo_inferido and numero:
            tipos_regex = [
                (r'lei\s+complementar', 'Lei Complementar'),
                (r'lei\s+ordinária|lei\s+ordinaria', 'Lei Ordinária'),
                (r'decreto[\-\s]*lei', 'Decreto-Lei'),
                (r'decreto', 'Decreto'),
                (r'resolução|resolucao', 'Resolução'),
                (r'portaria', 'Portaria'),
                (r'emenda', 'Emenda'),
                (r'instrução\s+normativa|instrucao\s+normativa', 'Instrução Normativa'),
                (r'\blei\b', 'Lei'),
            ]
            for snp in todos_snippets:
                txt_snp = (snp.get('titulo','') + ' ' + snp.get('snippet','')).lower()
                for padrao_tipo, nome_tipo in tipos_regex:
                    # Padrão: "tipo [nº] NUMERO"
                    m_tipo = re.search(padrao_tipo + r'\s+(?:n[ºo°.]?\s*)?' + re.escape(numero), txt_snp)
                    if m_tipo:
                        tipo_inferido = nome_tipo
                        logs.append({'nivel': 'ok', 'msg': f'📋 Tipo inferido via regex dos snippets: {tipo_inferido}'})
                        break
                if tipo_inferido:
                    break

        # Fallback: tentar extrair data via regex dos snippets/títulos
        if not data_descoberta and numero:
            meses_map = {'janeiro':'01','fevereiro':'02','março':'03','marco':'03','abril':'04',
                         'maio':'05','junho':'06','julho':'07','agosto':'08','setembro':'09',
                         'outubro':'10','novembro':'11','dezembro':'12'}
            for snp in todos_snippets:
                txt_snp = (snp.get('titulo','') + ' ' + snp.get('snippet','')).lower()
                # Padrão: "de DD de MMMM de YYYY" perto do número da lei
                m = re.search(
                    r'(?:' + re.escape(numero) + r')[,\s]+(?:de\s+)?(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})',
                    txt_snp
                )
                if m and m.group(2) in meses_map:
                    data_descoberta = f'{m.group(3)}-{meses_map[m.group(2)]}-{m.group(1).zfill(2)}'
                    logs.append({'nivel': 'ok', 'msg': f'📅 Data extraída via regex do snippet: {data_descoberta}'})
                    break
                # Padrão alternativo: "DD/MM/YYYY"
                m2 = re.search(r'(?:' + re.escape(numero) + r')[\s\S]{0,40}(\d{2})/(\d{2})/(\d{4})', txt_snp)
                if m2:
                    data_descoberta = f'{m2.group(3)}-{m2.group(2)}-{m2.group(1)}'
                    logs.append({'nivel': 'ok', 'msg': f'📅 Data extraída via regex do snippet: {data_descoberta}'})
                    break
    else:
        urls_selecionadas = []
        logs.append({'nivel': 'aviso', 'msg': '🧠 Sem resultados para triar'})

    # ══════════════════════════════════════════════════════════════════
    # ETAPA 3: ACESSAR PÁGINAS WEB SELECIONADAS
    # ══════════════════════════════════════════════════════════════════
    logs.append({'nivel': 'info', 'msg': f'📄 ETAPA 3: Acessando {len(urls_selecionadas)} página(s) selecionadas...'})

    snippet_map = {s['url']: s for s in todos_snippets}

    for url in urls_selecionadas:
        info = snippet_map.get(url, {})
        fonte_tipo = info.get('_fonte', 'google')
        fonte_nome = info.get('fonte_tipo', '🔎 Web')

        result = _acessar_pagina(url, termos_busca, headers_http, logs, fonte_nome, tipo_lei=tipo_inferido, numero_lei=numero)
        if result:
            result['_fonte'] = fonte_tipo
            is_pdf = url.lower().endswith('.pdf') or '/download/' in url.lower()
            if is_pdf:
                result['relevancia'] = min(result['relevancia'] + 0.3, 1.0)
                logs.append({'nivel': 'ok', 'msg': f'{fonte_nome}: bônus PDF → {result["relevancia"]:.0%}'})
            textos_extraidos.append(result)

            # Tentar extrair data do texto se ainda não temos
            if not data_descoberta and numero:
                import re as _re
                # Procurar padrão "de DD de MMMM de YYYY" perto do nome da lei
                match_data = _re.search(
                    r'(?:lei\s+complementar|lc|lei|decreto|resolução)\s*(?:n[ºo°.]?\s*)?' + numero +
                    r'[,\s]*(?:de\s+)?(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})',
                    result['texto'].lower()
                )
                if match_data:
                    meses = {'janeiro':'01','fevereiro':'02','março':'03','marco':'03','abril':'04',
                             'maio':'05','junho':'06','julho':'07','agosto':'08','setembro':'09',
                             'outubro':'10','novembro':'11','dezembro':'12'}
                    dia = match_data.group(1).zfill(2)
                    mes = meses.get(match_data.group(2), '')
                    ano_d = match_data.group(3)
                    if mes:
                        data_descoberta = f'{ano_d}-{mes}-{dia}'
                        logs.append({'nivel': 'ok', 'msg': f'📅 Data extraída do texto: {data_descoberta}'})
        else:
            snippet = info.get('snippet', '')
            titulo = info.get('titulo', '')
            if snippet and len(snippet) > 30:
                textos_extraidos.append({
                    'url': url, 'texto': f'{titulo}: {snippet}',
                    'nome': f'{fonte_nome} (snippet)', 'relevancia': 0.2, '_fonte': fonte_tipo,
                })

    logs.append({'nivel': 'ok', 'msg': f'📄 {len(textos_extraidos)} texto(s) extraídos das fontes web'})

    # ══════════════════════════════════════════════════════════════════
    # ETAPA 4: DIÁRIO OFICIAL (busca por DATA, dia a dia)
    # ══════════════════════════════════════════════════════════════════
    if url_diario:
        dominio_diario = re.sub(r'https?://', '', url_diario.rstrip('/')).split('/')[0]

        if data_descoberta:
            logs.append({'nivel': 'info', 'msg': f'🏛️ ETAPA 4: Buscando no Diário Oficial ({dominio_diario}) a partir de {data_descoberta}...'})
            from datetime import datetime, timedelta
            try:
                data_base = datetime.strptime(data_descoberta, '%Y-%m-%d')
            except ValueError:
                data_base = None
                logs.append({'nivel': 'aviso', 'msg': f'🏛️ Data inválida: {data_descoberta}'})

            encontrou_do = False
            urls_ja_visitadas = set()  # Evitar baixar mesmo PDF várias vezes
            if data_base:
                for delta in range(0, 11):  # data_publicação até 10 dias depois
                    data_busca = data_base + timedelta(days=delta)
                    # Formatos de busca por data
                    data_fmt1 = data_busca.strftime('%d/%m/%Y')          # 14/01/2019
                    data_fmt2 = data_busca.strftime('%Y-%m-%d')          # 2019-01-14
                    meses_pt = ['','janeiro','fevereiro','março','abril','maio','junho',
                                'julho','agosto','setembro','outubro','novembro','dezembro']
                    data_fmt3 = f'{data_busca.day} de {meses_pt[data_busca.month]} de {data_busca.year}'  # 14 de janeiro de 2019

                    query_do = f'site:{dominio_diario} {query_curta} "{data_fmt1}"'
                    ddg_results = _pesquisar_web(query_do, logs, f'🏛️ DO {data_fmt1}', max_results=3)

                    if not ddg_results:
                        # Tentar com formato por extenso
                        query_do2 = f'site:{dominio_diario} {query_curta} "{data_fmt3}"'
                        ddg_results = _pesquisar_web(query_do2, logs, f'🏛️ DO {data_fmt3}', max_results=3)

                    for ddg in ddg_results:
                        # Evitar baixar o mesmo PDF várias vezes
                        if ddg['url'] in urls_ja_visitadas:
                            continue
                        urls_ja_visitadas.add(ddg['url'])

                        # PRÉ-FILTRO 1: verificar se título/snippet indica OUTRO tipo de legislação
                        snippet_titulo = (ddg.get('titulo', '') + ' ' + ddg.get('snippet', '')).lower()
                        tipo_buscado_lower = (tipo_inferido or tipo or '').lower()
                        if tipo_buscado_lower:
                            _tipos_conhecidos = {
                                'decreto': ['decreto'],
                                'lei complementar': ['lei complementar', 'lc'],
                                'lei ordinária': ['lei ordinária', 'lei ordinaria'],
                                'resolução': ['resolução', 'resolucao'],
                                'portaria': ['portaria'],
                                'emenda': ['emenda'],
                            }
                            titulo_snippet = (ddg.get('titulo', '') or '').lower()
                            tipo_no_titulo = None
                            for tipo_chave, variantes in _tipos_conhecidos.items():
                                for v in variantes:
                                    # Procurar "DECRETO Nº 54234" ou "DECRETO RIO Nº" no título
                                    if re.search(rf'\b{re.escape(v)}\b\s+(?:\w+\s+)?(?:no?\.?\s*)?\d', titulo_snippet):
                                        tipo_no_titulo = tipo_chave
                                        break
                                if tipo_no_titulo:
                                    break

                            if tipo_no_titulo and tipo_no_titulo != tipo_buscado_lower and not (tipo_buscado_lower in tipo_no_titulo or tipo_no_titulo in tipo_buscado_lower):
                                logs.append({'nivel': 'aviso', 'msg': f'🏛️ DO ({data_fmt1}): título indica "{tipo_no_titulo.upper()}" mas buscamos "{tipo_buscado_lower.upper()}" — pulando'})
                                continue

                        # PRÉ-FILTRO 2: verificar se o snippet/título indica data errada
                        snippet_titulo = (ddg.get('titulo', '') + ' ' + ddg.get('snippet', '')).lower()
                        # Se o snippet mostra uma data diferente da buscada, pular
                        data_errada = False
                        datas_no_snippet = re.findall(r'data:\s*(\d{2}/\d{2}/\d{4})', snippet_titulo)
                        for ds in datas_no_snippet:
                            if ds != data_fmt1:
                                logs.append({'nivel': 'aviso', 'msg': f'🏛️ DO ({data_fmt1}): snippet indica data {ds} — pulando {ddg["url"][:60]}'})
                                data_errada = True
                                break
                        if data_errada:
                            continue

                        result = _acessar_pagina(ddg['url'], termos_busca, headers_http, logs, f'🏛️ DO ({data_fmt1})', tipo_lei=tipo_inferido, numero_lei=numero)
                        if result:
                            # PÓS-FILTRO: verificar data da EDIÇÃO do DO (página 1 do PDF)
                            # Usar _pagina1 (cabeçalho do DO) — NÃO o texto da lei extraída
                            texto_cabecalho = (result.get('_pagina1', '') or result.get('texto', '')[:2000]).lower()
                            ano_busca = str(data_busca.year)
                            mes_busca = meses_pt[data_busca.month]
                            dia_busca = str(data_busca.day)

                            # Verificar se a data aparece no início (cabeçalho do DO)
                            tem_data_certa = (
                                data_fmt1 in texto_cabecalho or
                                data_fmt2 in texto_cabecalho or
                                (dia_busca in texto_cabecalho and mes_busca in texto_cabecalho and ano_busca in texto_cabecalho)
                            )

                            if not tem_data_certa:
                                # Tentar achar qual data realmente tem no início
                                datas_achadas = re.findall(r'\d{1,2}\s+de\s+\w+\s+de\s+\d{4}', texto_cabecalho)
                                if not datas_achadas:
                                    datas_achadas = re.findall(r'\d{2}/\d{2}/\d{4}', texto_cabecalho)
                                data_real = datas_achadas[0] if datas_achadas else '?'
                                logs.append({'nivel': 'aviso', 'msg': f'🏛️ DO ({data_fmt1}): edição baixada é de {data_real}, não de {data_fmt1} — DESCARTADO'})
                                continue

                            result['_fonte'] = 'diario_oficial'
                            is_pdf = ddg['url'].lower().endswith('.pdf') or '/download/' in ddg['url'].lower()
                            if is_pdf:
                                result['relevancia'] = min(result['relevancia'] + 0.3, 1.0)
                            textos_extraidos.append(result)
                            fontes_status.append({'nome': '🏛️ Diário Oficial', 'url': ddg['url'], 'encontrou': True})
                            encontrou_do = True
                            logs.append({'nivel': 'ok', 'msg': f'🏛️ ✅ Encontrado no DO de {data_fmt1}!'})
                            break
                    if encontrou_do:
                        break

                if not encontrou_do:
                    logs.append({'nivel': 'aviso', 'msg': f'🏛️ Não encontrado no DO entre {data_descoberta} e +10 dias'})
                    fontes_status.append({'nome': '🏛️ Diário Oficial', 'url': url_diario, 'encontrou': False})
        else:
            # Sem data: busca genérica no DO — primeiro acesso direto, depois DuckDuckGo
            logs.append({'nivel': 'info', 'msg': f'🏛️ ETAPA 4: Sem data — busca genérica no DO ({dominio_diario})...'})

            # Tentar acesso direto ao site do DO
            do_direto = _buscar_no_site_direto(url_diario, tipo_inferido, numero, ano, logs, '🏛️ DO', data_pub=data_pub or data_descoberta)

            # Combinar resultados diretos + DuckDuckGo
            ddg_results = _pesquisar_web(f'site:{dominio_diario} {query_curta}', logs, '🏛️ DO')
            all_do_results = do_direto + ddg_results

            # Deduplicar por URL
            seen_urls = set()
            dedup_results = []
            for r in all_do_results:
                if r['url'] not in seen_urls:
                    seen_urls.add(r['url'])
                    dedup_results.append(r)

            encontrou_do = False
            for ddg in dedup_results[:5]:
                result = _acessar_pagina(ddg['url'], termos_busca, headers_http, logs, '🏛️ DO', tipo_lei=tipo_inferido, numero_lei=numero)
                if result:
                    result['_fonte'] = 'diario_oficial'
                    textos_extraidos.append(result)
                    fontes_status.append({'nome': '🏛️ Diário Oficial', 'url': ddg['url'], 'encontrou': True})
                    encontrou_do = True
                    break
            if not encontrou_do:
                fontes_status.append({'nome': '🏛️ Diário Oficial', 'url': url_diario, 'encontrou': False})
    else:
        logs.append({'nivel': 'info', 'msg': '🏛️ ETAPA 4: Sem URL do Diário Oficial (pulando)'})

    # ══════════════════════════════════════════════════════════════════
    # ETAPA 5: LLM COMPARA, SUGERE E JUSTIFICA
    # ══════════════════════════════════════════════════════════════════
    total_fontes = len(textos_extraidos)
    logs.append({'nivel': 'info', 'msg': f'🤖 ETAPA 5: Enviando {total_fontes} fonte(s) para análise final da IA...'})

    if total_fontes == 0:
        logs.append({'nivel': 'aviso', 'msg': 'Nenhuma fonte encontrada na internet. LLM usará apenas conhecimento próprio (pode conter erros).'})

    # Montar textos das fontes (truncamento inteligente: início + fim pra textos grandes)
    def _truncar_inteligente(txt, limite=4000):
        if len(txt) <= limite:
            return txt
        metade = limite // 2
        return txt[:metade] + f'\n\n[... {len(txt) - limite} caracteres omitidos ...]\n\n' + txt[-metade:]

    # Priorizar por relevância, máximo 4 fontes no prompt (economizar tokens)
    fontes_para_llm = sorted(textos_extraidos, key=lambda x: x.get('relevancia', 0), reverse=True)[:4]
    fontes_texto = '\n\n'.join([
        f"=== FONTE {i+1}: {t['nome']} ({t['url'][:80]}) ===\nRelevância: {t['relevancia']:.0%}\nTamanho total: {len(t['texto'])} chars\n{_truncar_inteligente(t['texto'], 4000)}"
        for i, t in enumerate(fontes_para_llm)
    ]) if fontes_para_llm else '(nenhuma fonte encontrada — use apenas seu conhecimento, mas AVISE que os dados podem conter erros)'

    prompt = f"""Analise os textos extraídos de fontes reais da internet e identifique a legislação buscada.

CRITÉRIOS DE BUSCA:
{descricao_busca}

TEXTOS DAS FONTES (em ordem de prioridade):
{fontes_texto}

INSTRUÇÕES:
1. Identifique a legislação nos textos. Use APENAS informações presentes nas fontes.
2. Se múltiplas fontes mencionam a mesma legislação, COMPARE os dados (ementa, data, assunto).
3. Na sugestão, SEMPRE referencie as fontes pelo NÚMERO (ex: "A Fonte 1 é mais confiável porque..."). Explique divergências entre fontes.
4. A ementa deve ser EXATAMENTE como aparece na fonte mais confiável.
5. NÃO invente dados. Se um campo não aparece nas fontes, deixe vazio ("").
6. Inclua justificativa em cada legislação referenciando o número da fonte.

Responda SOMENTE com JSON:
{{
    "sugestao": "Referencie as fontes por número (Fonte 1, Fonte 2...). Explique qual é mais confiável e por quê. Cite divergências entre fontes se houver.",
    "legislacoes": [
        {{
            "tipo": "Lei Complementar",
            "numero": "198",
            "ano": 2019,
            "ementa": "(ementa EXATA da fonte mais confiável)",
            "data_publicacao": "2019-12-20",
            "assunto": "Código de Obras",
            "esfera": "municipal",
            "estado": "RJ",
            "municipio": "Rio de Janeiro",
            "url_fonte": "https://...(URL da fonte mais confiável)",
            "confianca": 0.95,
            "_fonte": "diario_oficial|site_informado|leismunicipais|google",
            "justificativa": "Encontrada na Fonte X com ementa confirmada na Fonte Y..."
        }}
    ]
}}"""

    logs.append({'nivel': 'info', 'msg': f'Prompt montado: {len(prompt)} chars com {total_fontes} fonte(s)'})

    # Chamar LLM com mais retries na etapa final (é a mais importante)
    texto = _chamar_llm(prompt, logs, '🤖 Análise Final', max_retries=3)

    if not texto:
        # FALLBACK: montar resultado sem IA, usando os dados já extraídos
        logs.append({'nivel': 'aviso', 'msg': '🤖 IA indisponível — montando resultado com os dados extraídos...'})
        if textos_extraidos:
            # Ordenar por relevância e pegar o melhor
            textos_ordenados = sorted(textos_extraidos, key=lambda x: x.get('relevancia', 0), reverse=True)
            melhor = textos_ordenados[0]
            # Montar legislação a partir dos dados disponíveis
            leg_fallback = {
                'tipo': tipo_inferido or 'Legislação',
                'numero': numero or '',
                'ano': ano or '',
                'ementa': f'{tipo_inferido} nº {numero}/{ano}' if tipo_inferido and numero else 'Legislação encontrada',
                'data_publicacao': data_descoberta or '',
                'estado': estado or '',
                'municipio': municipio or '',
                'url_fonte': melhor['url'],
                'confianca': melhor.get('relevancia', 0.7),
                '_fonte': melhor.get('_fonte', 'google'),
                'justificativa': f'⚠️ Análise automática (IA indisponível). Fonte: {melhor["nome"]} com {melhor.get("relevancia", 0):.0%} relevância.',
                'texto': melhor.get('texto', '')[:30000],
            }
            legislacoes_finais = [leg_fallback]
            # Adicionar fontes extras como alternativas
            for t in textos_ordenados[1:3]:
                if t.get('relevancia', 0) >= 0.5:
                    legislacoes_finais.append({
                        'tipo': tipo_inferido or '', 'numero': numero or '', 'ano': ano or '',
                        'ementa': f'{tipo_inferido} nº {numero}/{ano} (fonte alternativa)',
                        'data_publicacao': data_descoberta or '',
                        'estado': estado or '', 'municipio': municipio or '',
                        'url_fonte': t['url'],
                        'confianca': t.get('relevancia', 0.5),
                        '_fonte': t.get('_fonte', 'google'),
                        'justificativa': f'Fonte alternativa: {t["nome"]}',
                        'texto': t.get('texto', '')[:30000],
                    })
            logs.append({'nivel': 'ok', 'msg': f'✅ Fallback: {len(legislacoes_finais)} resultado(s) montado(s) sem IA'})
            return {
                'legislacoes': legislacoes_finais,
                'sugestao': '⚠️ Resultado montado automaticamente (IA indisponível por rate limit). As fontes foram encontradas mas não analisadas por IA.',
                'logs': logs, 'fontes': fontes_status,
            }
        return {'legislacoes': [], 'erro': 'Nenhum modelo de IA respondeu e nenhuma fonte foi encontrada.', 'logs': logs, 'fontes': fontes_status}

    # Processar resposta
    texto = re.sub(r'^```(?:json)?\s*', '', texto)
    texto = re.sub(r'\s*```$', '', texto)
    logs.append({'nivel': 'info', 'msg': f'Preview: {texto[:200]}...'})

    match = re.search(r'\{.*\}', texto, re.DOTALL)
    if not match:
        logs.append({'nivel': 'erro', 'msg': 'JSON não encontrado na resposta'})
        return {'legislacoes': [], 'erro': 'LLM não retornou JSON válido', 'logs': logs, 'fontes': fontes_status}

    try:
        data = json.loads(match.group())
    except json.JSONDecodeError as e:
        logs.append({'nivel': 'erro', 'msg': f'JSON inválido: {e}'})
        return {'legislacoes': [], 'erro': 'JSON inválido', 'logs': logs, 'fontes': fontes_status}

    legislacoes = data.get('legislacoes', [])
    sugestao = data.get('sugestao', '')

    # Filtrar confiança
    antes = len(legislacoes)
    legislacoes = [l for l in legislacoes if l.get('confianca', 0) >= 0.5]
    if antes != len(legislacoes):
        logs.append({'nivel': 'aviso', 'msg': f'{antes - len(legislacoes)} descartada(s) por confiança < 50%'})

    logs.append({'nivel': 'ok', 'msg': f'✅ Resultado final: {len(legislacoes)} legislação(ões) de {total_fontes} fonte(s) real(is)'})

    # Registrar
    try:
        _registrar_busca_fila(
            tipo='busca_manual',
            municipio=municipio or '—', estado=estado or '—',
            legislacoes_encontradas=len(legislacoes),
            status='concluido',
            detalhes={'params': params, 'resultados': len(legislacoes), 'fontes': total_fontes},
        )
    except Exception:
        pass

    # Preparar textos das fontes (texto completo — app.py trunca para preview no frontend)
    textos_fontes = []
    for t in textos_extraidos:
        textos_fontes.append({
            'nome': t.get('nome', ''),
            'url': t.get('url', ''),
            'relevancia': round(t.get('relevancia', 0), 2),
            'texto': t.get('texto', ''),  # texto integral para salvar no DB via job cache
            '_fonte': t.get('_fonte', ''),
        })

    return {'legislacoes': legislacoes, 'sugestao': sugestao, 'fontes': fontes_status, 'textos_fontes': textos_fontes, 'logs': logs}


# ─────────────────────────────────────────────────────────────────────────────
# 3. REFERÊNCIAS CRUZADAS
# ─────────────────────────────────────────────────────────────────────────────

# Padrões regex para encontrar referências a outras leis
PADROES_REFERENCIA = [
    # "Lei Complementar nº 270/2024" ou "LC 270/2024"
    r'(?:Lei\s+Complementar|LC)\s+n[ºo°]?\s*(\d+)[/\s]*(\d{4})',
    # "Lei nº 1234/2020" ou "Lei 1234/2020"
    r'(?:Lei\s+(?:Ordinária\s+)?|Lei\s+)n[ºo°]?\s*(\d+)[/\s]*(\d{4})',
    # "Decreto nº 456/2023"
    r'Decreto\s+n[ºo°]?\s*(\d+)[/\s]*(\d{4})',
    # "Resolução nº 78/2022"
    r'Resolução\s+n[ºo°]?\s*(\d+)[/\s]*(\d{4})',
    # "Portaria nº 12/2021"
    r'Portaria\s+n[ºo°]?\s*(\d+)[/\s]*(\d{4})',
]


def varrer_referencias() -> List[dict]:
    """
    Varre todas as legislações da biblioteca, detecta referências a outras
    legislações que NÃO estão cadastradas.

    Returns:
        Lista de referências não cadastradas:
        [{tipo, numero, ano, referenciada_por, municipio, estado, esfera}]
    """
    # Buscar todas as legislações com ementa ou conteúdo
    try:
        legislacoes = _qry("""
            SELECT id, tipo_nome, numero, ano, ementa, conteudo_texto,
                   municipio_nome, estado, esfera
            FROM legislacoes
            WHERE pendente_aprovacao = FALSE
        """)
    except Exception as e:
        logger.error(f"Erro ao buscar legislações: {e}")
        return []

    if not legislacoes:
        return []

    # Coletar todas as legislações existentes como set de (numero, ano)
    existentes = set()
    for leg in legislacoes:
        num = str(leg.get('numero', '')).strip()
        ano = str(leg.get('ano', '')).strip()
        if num and ano:
            existentes.add((num, ano))

    # Varrer textos procurando referências
    referencias_encontradas = {}  # chave = (numero, ano), valor = dict

    for leg in legislacoes:
        texto = (leg.get('ementa') or '') + ' ' + (leg.get('conteudo_texto') or '')
        if not texto.strip():
            continue

        leg_titulo = f"{leg.get('tipo_nome', '')} {leg.get('numero', '')}/{leg.get('ano', '')}"

        for padrao in PADROES_REFERENCIA:
            matches = re.finditer(padrao, texto, re.IGNORECASE)
            for m in matches:
                numero = m.group(1)
                ano = m.group(2)

                # Pular auto-referência
                if str(leg.get('numero', '')) == numero and str(leg.get('ano', '')) == ano:
                    continue

                # Pular se já existe na biblioteca
                if (numero, ano) in existentes:
                    continue

                chave = (numero, ano)
                if chave not in referencias_encontradas:
                    # Detectar tipo da referência pelo padrão
                    texto_match = m.group(0)
                    tipo = 'Lei'
                    if 'complementar' in texto_match.lower() or texto_match.startswith('LC'):
                        tipo = 'Lei Complementar'
                    elif 'decreto' in texto_match.lower():
                        tipo = 'Decreto'
                    elif 'resolução' in texto_match.lower() or 'resolucao' in texto_match.lower():
                        tipo = 'Resolução'
                    elif 'portaria' in texto_match.lower():
                        tipo = 'Portaria'

                    referencias_encontradas[chave] = {
                        'tipo': tipo,
                        'numero': numero,
                        'ano': int(ano),
                        'referenciada_por': leg_titulo,
                        'referenciada_por_id': leg['id'],
                        'municipio': leg.get('municipio_nome', ''),
                        'estado': leg.get('estado', ''),
                        'esfera': leg.get('esfera', 'municipal'),
                    }

    resultado = list(referencias_encontradas.values())
    logger.info(f"Varredura: {len(resultado)} referência(s) não cadastrada(s) encontrada(s)")

    if resultado:
        _registrar_atividade(
            'referencias_varridas',
            f'Varredura encontrou {len(resultado)} legislação(ões) referenciada(s) não cadastrada(s)',
            {'total': len(resultado), 'exemplos': [r['tipo'] + ' ' + r['numero'] + '/' + str(r['ano']) for r in resultado[:5]]}
        )

    return resultado


# ─────────────────────────────────────────────────────────────────────────────
# CADASTRO DE RESULTADOS
# ─────────────────────────────────────────────────────────────────────────────

def cadastrar_resultados(legislacoes: List[dict], municipio: str, estado: str,
                          monitorar: bool = True) -> List[int]:
    """Cadastra legislações encontradas pelo buscador na biblioteca."""
    try:
        from modulos.descobridor_legislacoes import cadastrar_legislacoes_descobertas
        return cadastrar_legislacoes_descobertas(
            legislacoes, municipio, estado,
            ativar_monitoramento=monitorar
        )
    except Exception as e:
        logger.error(f"Erro ao cadastrar: {e}")
        return []


# ─────────────────────────────────────────────────────────────────────────────
# FILA DE BUSCAS
# ─────────────────────────────────────────────────────────────────────────────

def _registrar_busca_fila(tipo, municipio, estado, legislacoes_encontradas=0,
                           legislacoes_cadastradas=0, status='concluido',
                           detalhes=None, mensagem=''):
    """Registra busca no log/fila."""
    try:
        _qry("""INSERT INTO integracao_log
                (tipo, municipios_consultados, novos_detectados,
                 legislacoes_cadastradas, detalhes, status, criado_em)
                VALUES (%s, 1, %s, %s, %s, %s, NOW())""",
             (tipo, legislacoes_encontradas, legislacoes_cadastradas,
              json.dumps({
                  'municipio': municipio, 'estado': estado,
                  'mensagem': mensagem,
                  **(detalhes or {})
              }, default=str),
              status),
             commit=True, fetch=None)
    except Exception:
        pass


def listar_fila(limit=30) -> List[dict]:
    """Retorna histórico de buscas."""
    try:
        rows = _qry("""SELECT id, tipo, municipios_consultados,
                              novos_detectados as legislacoes_encontradas,
                              legislacoes_cadastradas, detalhes, status, criado_em
                       FROM integracao_log
                       ORDER BY criado_em DESC LIMIT %s""", (limit,))
        result = []
        for r in (rows or []):
            det = r.get('detalhes') or {}
            if isinstance(det, str):
                try: det = json.loads(det)
                except: det = {}
            result.append({
                'id': r['id'],
                'tipo': r['tipo'],
                'municipio': det.get('municipio', ''),
                'estado': det.get('estado', ''),
                'legislacoes_encontradas': r.get('legislacoes_encontradas', 0),
                'legislacoes_cadastradas': r.get('legislacoes_cadastradas', 0),
                'status': r.get('status', ''),
                'mensagem': det.get('mensagem', ''),
                'criado_em': r.get('criado_em'),
            })
        return result
    except Exception:
        return []
