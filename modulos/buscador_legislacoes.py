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


def _apply_stealth(page):
    """Aplica patches anti-detecção no Playwright para parecer browser real.
    Necessário para passar Cloudflare, DataDome, etc."""
    try:
        from playwright_stealth import stealth_sync
        stealth_sync(page)
    except ImportError:
        # Fallback manual se playwright-stealth não instalado
        try:
            page.add_init_script("""
                // Esconder webdriver
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                // Simular plugins
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
                // Simular languages
                Object.defineProperty(navigator, 'languages', { get: () => ['pt-BR', 'pt', 'en-US', 'en'] });
                // Chrome runtime
                window.chrome = { runtime: {} };
            """)
        except Exception:
            pass
    except Exception:
        pass

GEMINI_API_KEY = os.getenv('GEMINI_API_KEY', '')
GROQ_API_KEY = os.getenv('GROQ_API_KEY', '')


def _get_proxy_config():
    """Retorna config de proxy para Playwright a partir de variáveis de ambiente.
    
    Variáveis:
        PROXY_URL: URL completa (ex: http://user:pass@rp.evomi.com:1000)
        ou separadas:
        PROXY_SERVER: servidor:porta (ex: rp.evomi.com:1000)
        PROXY_USER: username
        PROXY_PASS: password
    
    Retorna dict para Playwright ou None se não configurado.
    """
    proxy_url = os.getenv('PROXY_URL', '').strip()
    if proxy_url:
        # Parsear URL completa: http://user:pass@host:port
        try:
            from urllib.parse import urlparse
            parsed = urlparse(proxy_url)
            config = {'server': f'{parsed.scheme}://{parsed.hostname}:{parsed.port}'}
            if parsed.username:
                config['username'] = parsed.username
            if parsed.password:
                config['password'] = parsed.password
            return config
        except Exception:
            return None
    
    # Tentar variáveis separadas
    server = os.getenv('PROXY_SERVER', '').strip()
    if server:
        config = {'server': f'http://{server}'}
        user = os.getenv('PROXY_USER', '').strip()
        pw = os.getenv('PROXY_PASS', '').strip()
        if user:
            config['username'] = user
        if pw:
            config['password'] = pw
        return config
    
    return None


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
    Usa cloudscraper para contornar Cloudflare.
    """
    from urllib.parse import quote_plus

    # Tentar cloudscraper (bypass Cloudflare), fallback para requests
    try:
        import cloudscraper
        session = cloudscraper.create_scraper(browser={'browser': 'chrome', 'platform': 'windows', 'mobile': False})
    except ImportError:
        import requests
        session = requests.Session()

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
        resp = session.get(url_busca, headers=headers, timeout=10, allow_redirects=True)
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
                resp = session.get(url_direta, headers=headers, timeout=10, allow_redirects=True)
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


def _buscar_diario_oficial(url_diario: str, tipo_lei: str, numero_lei: str,
                           ano: str, data_pub: str, logs: list, label: str,
                           municipio: str = '') -> list:
    """
    Busca legislação no Diário Oficial dia a dia, como um humano faria:
    1. Preenche data de um dia só
    2. Busca
    3. Clica "Baixar Diário completo"
    4. Baixa o PDF INTEIRO
    5. Procura a lei dentro do PDF (Ctrl+F + IA)
    6. Não achou? Próximo dia. Até 7 dias.
    """
    try:
        from playwright.sync_api import sync_playwright
        import fitz as _fitz_do
        import time as _time
    except (ImportError, Exception) as e:
        logs.append({'nivel': 'aviso', 'msg': f'{label}: Dependência faltando: {str(e)[:60]}'})
        return []

    # Gerar lista de datas para tentar (data_pub + 7 dias)
    from datetime import datetime as _dt, timedelta as _td

    datas_tentar = []
    if data_pub:
        try:
            # Aceitar formatos: YYYY-MM-DD, DD/MM/YYYY
            for fmt in ['%Y-%m-%d', '%d/%m/%Y']:
                try:
                    dt_base = _dt.strptime(data_pub, fmt)
                    break
                except ValueError:
                    continue
            else:
                dt_base = None

            if dt_base:
                for delta in range(0, 7):
                    dt = dt_base + _td(days=delta)
                    # Pular fins de semana (DO não publica)
                    if dt.weekday() < 5:  # 0=seg, 4=sex
                        datas_tentar.append(dt.strftime('%d/%m/%Y'))
        except Exception:
            pass

    if not datas_tentar:
        # Sem data conhecida — NÃO tem como buscar no DO (precisa de data da edição)
        logs.append({'nivel': 'aviso', 'msg': f'{label}: Sem data de publicação — impossível buscar no DO (precisa de data)'})
        return []

    # Detectar Chromium
    executable_path = os.environ.get('PLAYWRIGHT_CHROMIUM_PATH', '')
    if not executable_path:
        import shutil, glob as _glob
        for cn in ['chromium', 'chromium-browser', 'google-chrome-stable']:
            p = shutil.which(cn)
            if p:
                executable_path = p
                break
        if not executable_path:
            nps = _glob.glob('/nix/store/*/bin/chromium')
            if nps:
                executable_path = nps[0]

    if not executable_path:
        logs.append({'nivel': 'aviso', 'msg': f'{label}: Chromium não encontrado'})
        return []

    num = (numero_lei or '').strip()
    tipo_desc = tipo_lei or 'legislação'
    resultados = []

    logs.append({'nivel': 'info', 'msg': f'{label}: 📅 Buscando dia a dia: {len(datas_tentar)} dia(s) a partir de {datas_tentar[0]}'})

    try:
        with sync_playwright() as pw:
            launch_args = {
                'headless': True,
                'args': ['--no-sandbox', '--disable-dev-shm-usage',
                         '--disable-gpu', '--single-process', '--no-zygote']
            }
            if executable_path:
                launch_args['executable_path'] = executable_path

            browser = pw.chromium.launch(**launch_args)
            ctx = browser.new_context(
                viewport={'width': 1280, 'height': 900},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                accept_downloads=True
            )

            for data_dia in datas_tentar:
                logs.append({'nivel': 'info', 'msg': f'{label}: 📅 Tentando {data_dia}...'})

                page = ctx.new_page()
                _apply_stealth(page)
                try:
                    page.goto(url_diario, wait_until='networkidle', timeout=20000)
                except Exception:
                    try:
                        page.goto(url_diario, wait_until='domcontentloaded', timeout=15000)
                    except Exception as e:
                        logs.append({'nivel': 'aviso', 'msg': f'{label}: Não abriu DO: {str(e)[:60]}'})
                        page.close()
                        continue

                _time.sleep(1)

                # ── Pedir pra IA analisar a página e dizer como acessar a edição por data ──
                # NÃO usar busca por palavra-chave (pode causar falso negativo)
                # Buscar opção de "data da edição" / calendário / navegação por data
                fields_info = page.evaluate('''() => {
                    const info = [];
                    document.querySelectorAll('input, select, textarea, button, a').forEach(el => {
                        const tag = el.tagName.toLowerCase();
                        const isLink = tag === 'a';
                        info.push({
                            tag: tag,
                            type: el.type || '',
                            id: el.id || '',
                            name: el.name || '',
                            placeholder: el.placeholder || '',
                            text: (el.textContent || '').trim().substring(0, 60),
                            href: isLink ? (el.href || '') : '',
                            label: el.labels?.[0]?.textContent?.trim()?.substring(0, 50) || '',
                            ariaLabel: el.getAttribute('aria-label') || '',
                            visible: el.offsetParent !== null,
                            className: (el.className || '').substring(0, 40),
                        });
                    });
                    return info;
                }''')

                campos_desc = []
                for f in fields_info:
                    if not f['visible']:
                        continue
                    label = f['label'] or f['ariaLabel'] or f['placeholder'] or f['text'] or ''
                    if f['tag'] == 'a' and f['href']:
                        campos_desc.append(f"<a href=\"{f['href'][:80]}\">{label[:40] or f['text'][:40]}</a>")
                    else:
                        campos_desc.append(
                            f"<{f['tag']} type=\"{f['type']}\" id=\"{f['id']}\" name=\"{f['name']}\" "
                            f"placeholder=\"{f['placeholder']}\" label=\"{label}\">"
                        )

                prompt_nav = f"""Esta é a página de um Diário Oficial municipal.
Preciso acessar a EDIÇÃO COMPLETA do dia {data_dia} para baixar o PDF inteiro.

IMPORTANTE — PRIORIDADE DE SEÇÕES:
1. "Busca por Edição" / "Data da Edição" / campo de data simples com botão OK → USAR ESTA (só data, sem keyword)
2. Calendário / navegação por data → USAR ESTA
3. "Busca por Palavra" / formulário com palavra-chave → NUNCA USAR (causa falso negativo)

NUNCA preencher campo de "palavra-chave", "nome completo", "buscar por palavra" — mesmo que exista na página.
IGNORAR completamente a seção de busca por palavra/texto.

CAMPOS E LINKS VISÍVEIS NA PÁGINA:
{chr(10).join(campos_desc[:40])}

Como acessar a edição de {data_dia}? Responda APENAS com JSON:
{{
    "estrategia": "busca_por_edicao" ou "calendario" ou "formulario_data",
    "acoes": [
        {{"tipo": "preencher", "seletor": "#id_do_campo_de_data", "valor": "{data_dia}"}},
        {{"tipo": "clicar", "seletor": "#id_do_botao_OK"}}
    ],
    "motivo": "explicação breve"
}}"""

                resp_nav = _chamar_llm(prompt_nav, logs, f'{label} IA nav', max_retries=1)

                acoes_ok = False
                if resp_nav:
                    try:
                        resp_nav = re.sub(r'^```(?:json)?\s*|\s*```$', '', resp_nav.strip())
                        nav_info = json.loads(resp_nav)
                        logs.append({'nivel': 'info', 'msg': f'{label}: IA: {nav_info.get("estrategia","")} — {nav_info.get("motivo","")}'})

                        for acao in nav_info.get('acoes', []):
                            tipo_acao = acao.get('tipo', '')
                            seletor = acao.get('seletor', '')
                            valor = acao.get('valor', '')

                            if tipo_acao == 'preencher' and seletor and valor:
                                try:
                                    el = page.query_selector(seletor)
                                    if el:
                                        el.click()
                                        _time.sleep(0.2)
                                        el.press('Control+a')
                                        el.press('Delete')
                                        el.type(valor, delay=50)
                                        el.evaluate('''el => {
                                            el.dispatchEvent(new Event("input", {bubbles:true}));
                                            el.dispatchEvent(new Event("change", {bubbles:true}));
                                            el.dispatchEvent(new Event("blur", {bubbles:true}));
                                        }''')
                                        _time.sleep(0.3)
                                        page.keyboard.press('Escape')
                                        logs.append({'nivel': 'info', 'msg': f'{label}: ✏️ {seletor} = "{valor}"'})
                                        acoes_ok = True
                                except Exception as e_a:
                                    logs.append({'nivel': 'aviso', 'msg': f'{label}: Erro preenchendo {seletor}: {str(e_a)[:50]}'})

                            elif tipo_acao == 'clicar' and seletor:
                                try:
                                    _time.sleep(0.5)
                                    page.mouse.click(10, 10)  # blur
                                    _time.sleep(0.5)
                                    el = page.query_selector(seletor)
                                    if el and el.is_visible():
                                        el.click()
                                        acoes_ok = True
                                        logs.append({'nivel': 'info', 'msg': f'{label}: 🖱️ Clicou {seletor}'})
                                    else:
                                        # Tentar por texto
                                        for sel_alt in [f'button:has-text("{seletor}")', f'a:has-text("{seletor}")',
                                                        'button[type="submit"]', 'button.btn-primary']:
                                            try:
                                                el = page.query_selector(sel_alt)
                                                if el and el.is_visible():
                                                    el.click()
                                                    acoes_ok = True
                                                    logs.append({'nivel': 'info', 'msg': f'{label}: 🖱️ Clicou {sel_alt}'})
                                                    break
                                            except Exception:
                                                continue
                                except Exception as e_c:
                                    logs.append({'nivel': 'aviso', 'msg': f'{label}: Erro clicando {seletor}: {str(e_c)[:50]}'})

                    except (json.JSONDecodeError, ValueError):
                        logs.append({'nivel': 'aviso', 'msg': f'{label}: IA respondeu formato inválido'})

                # Se IA não ajudou, fallback: preencher campos de data sem keyword
                if not acoes_ok:
                    logs.append({'nivel': 'info', 'msg': f'{label}: Fallback: preenchendo datas sem palavra-chave'})
                    for field_id in ['dataBuscaInicial', 'dataEdicao', 'dataBuscaFinal', 'data']:
                        try:
                            el = page.query_selector(f'#{field_id}')
                            if el:
                                el.click()
                                _time.sleep(0.2)
                                el.press('Control+a')
                                el.press('Delete')
                                el.type(data_dia, delay=50)
                                el.evaluate('''el => {
                                    el.dispatchEvent(new Event("input", {bubbles:true}));
                                    el.dispatchEvent(new Event("change", {bubbles:true}));
                                    el.dispatchEvent(new Event("blur", {bubbles:true}));
                                }''')
                                _time.sleep(0.3)
                                page.keyboard.press('Escape')
                                logs.append({'nivel': 'info', 'msg': f'{label}: ✏️ #{field_id} = "{data_dia}"'})
                        except Exception:
                            pass

                    # Submeter
                    _time.sleep(0.5)
                    page.mouse.click(10, 10)
                    _time.sleep(0.5)
                    for sel in ['button:has-text("Buscar")', 'button:has-text("Pesquisar")',
                                'button[type="submit"]', 'button.btn-primary']:
                        try:
                            btn = page.query_selector(sel)
                            if btn and btn.is_visible():
                                btn.click()
                                break
                        except Exception:
                            continue

                try:
                    page.wait_for_load_state('networkidle', timeout=15000)
                except Exception:
                    pass
                _time.sleep(2)

                # ── Procurar link "Baixar Diário completo" ──
                download_links = page.evaluate('''() => {
                    const links = [];
                    document.querySelectorAll('a[href]').forEach(a => {
                        const text = (a.textContent || '').trim().toLowerCase();
                        const href = a.href || '';
                        if (href.includes('/download/') && !href.match(/\\/download\\/\\d+\\/\\d+/)) {
                            // Link de download do diário completo (sem /página)
                            links.push({url: href, text: a.textContent.trim()});
                        }
                    });
                    return links;
                }''')

                if not download_links:
                    logs.append({'nivel': 'info', 'msg': f'{label}: 📅 {data_dia} — sem edição do DO nesta data'})
                    page.close()
                    continue

                logs.append({'nivel': 'info', 'msg': f'{label}: 📅 {data_dia} — {len(download_links)} edição(ões) encontrada(s)'})

                # ── Baixar cada edição (geralmente 1-2 por dia) ──
                for dl_info in download_links[:3]:
                    dl_url = dl_info['url']
                    dl_text = dl_info['text'][:40]
                    logs.append({'nivel': 'info', 'msg': f'{label}: 📥 Baixando "{dl_text}" ({dl_url[-30:]})...'})

                    dl_path = None
                    try:
                        with page.expect_download(timeout=60000) as download_promise:
                            page.evaluate(f'() => {{ window.location.href = "{dl_url}"; }}')
                        download = download_promise.value
                        dl_path = download.path()
                        dl_size = os.path.getsize(dl_path) if dl_path else 0
                        logs.append({'nivel': 'ok', 'msg': f'{label}: 📥 Download: {download.suggested_filename} ({dl_size // 1024}KB)'})
                    except Exception as e_dl:
                        logs.append({'nivel': 'aviso', 'msg': f'{label}: Download falhou: {str(e_dl)[:60]}'})
                        # Tentar via requests como fallback
                        try:
                            import requests as _req_do
                            _hdr = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0',
                                    'Referer': url_diario}
                            resp_dl = _req_do.get(dl_url, headers=_hdr, timeout=60, stream=True)
                            if resp_dl.status_code == 200 and len(resp_dl.content) > 1000:
                                import tempfile
                                _tmp = tempfile.NamedTemporaryFile(suffix='.pdf', delete=False)
                                _tmp.write(resp_dl.content)
                                _tmp.close()
                                dl_path = _tmp.name
                                logs.append({'nivel': 'ok', 'msg': f'{label}: 📥 Download via requests: {len(resp_dl.content)//1024}KB'})
                            else:
                                logs.append({'nivel': 'aviso', 'msg': f'{label}: requests retornou {resp_dl.status_code}'})
                                continue
                        except Exception as e_req:
                            logs.append({'nivel': 'aviso', 'msg': f'{label}: Fallback requests também falhou: {str(e_req)[:60]}'})
                            continue

                    if not dl_path:
                        continue

                    # ── Ler o PDF e buscar a lei ──
                    try:
                        doc = _fitz_do.open(dl_path)
                        total_pages = len(doc)
                        logs.append({'nivel': 'info', 'msg': f'{label}: 📄 PDF aberto: {total_pages} páginas'})

                        # Busca rápida: Ctrl+F pelo número em TODAS as páginas
                        pagina_inicio = -1
                        pagina_cabecalho = -1

                        for i in range(total_pages):
                            page_text = doc[i].get_text()
                            page_lower = page_text.lower()

                            if num not in page_text and num not in page_lower:
                                continue

                            # Verificar se é cabeçalho (com tipo e data)
                            if tipo_lei:
                                tip_words = tipo_lei.lower().split()
                                tem_tipo = all(w in page_lower for w in tip_words)
                                if tem_tipo:
                                    pos = page_lower.find(num)
                                    ctx_depois = page_lower[pos + len(num):pos + len(num) + 40]
                                    eh_cabecalho = bool(re.search(r'[,/]\s*de\s+\d{1,2}\s+de\s+', ctx_depois))

                                    if eh_cabecalho:
                                        pagina_cabecalho = i
                                        ctx_preview = page_text[max(0, pos-30):pos + len(num) + 50].strip()
                                        logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ CABEÇALHO na pág {i+1}: "{ctx_preview[:80]}"'})
                                        break
                                    elif pagina_inicio < 0:
                                        pagina_inicio = i
                                        logs.append({'nivel': 'info', 'msg': f'{label}: 📌 Menção na pág {i+1} (candidata, buscando cabeçalho...)'})
                            else:
                                pagina_inicio = i
                                break

                        # Usar SOMENTE se achou cabeçalho (tipo + número + data)
                        # Menção avulsa do número NÃO serve — pode ser referência em outra legislação
                        if pagina_cabecalho >= 0:
                            pagina_inicio = pagina_cabecalho
                        else:
                            if pagina_inicio >= 0:
                                logs.append({'nivel': 'info', 'msg': f'{label}: 📅 {data_dia} — nº {num} mencionado na pág {pagina_inicio+1} mas SEM cabeçalho formal — pulando (pode ser referência)'})
                            else:
                                logs.append({'nivel': 'info', 'msg': f'{label}: 📅 {data_dia} — nº {num} não encontrado no PDF'})
                            pagina_inicio = -1
                            doc.close()
                            try:
                                os.unlink(dl_path)
                            except Exception:
                                pass

                            # Voltar à página de busca para o próximo dia
                            try:
                                page.goto(url_diario, wait_until='domcontentloaded', timeout=15000)
                                _time.sleep(1)
                            except Exception:
                                pass
                            continue

                        # ── Encontrou! Extrair da página início até o fim ──
                        logs.append({'nivel': 'ok', 'msg': f'{label}: 🎯 Legislação na pág {pagina_inicio+1} de {total_pages} — extraindo...'})

                        # Ler em lotes, IA decide quando a lei termina
                        paginas_lei = []
                        chunk_size = 40
                        fim_encontrado = False

                        for batch_start in range(pagina_inicio, total_pages, chunk_size):
                            batch_end = min(batch_start + chunk_size, total_pages)

                            for i in range(batch_start, batch_end):
                                paginas_lei.append((i, doc[i].get_text()))

                            total_lidas = len(paginas_lei)
                            logs.append({'nivel': 'info', 'msg': f'{label}: lidas {total_lidas} págs (até pág {batch_end})...'})

                            # No primeiro lote, pular — a lei acabou de começar
                            if batch_start == pagina_inicio:
                                continue

                            # Perguntar à IA se a lei ainda continua
                            primeira = paginas_lei[0]
                            ultimas = paginas_lei[-3:]
                            resumo = f"--- PÁGINA {primeira[0]+1} (INÍCIO DA LEI) ---\n{primeira[1][:400]}\n\n[...]\n\n"
                            resumo += '\n'.join([f"--- PÁGINA {p[0]+1} ---\n{p[1][:1200]}" for p in ultimas])

                            prompt_fim = f"""Estou extraindo a "{tipo_desc} nº {num}" de um Diário Oficial ({total_pages} págs).
A legislação começa na pág {pagina_inicio+1}. Já li {total_lidas} páginas.

PRIMEIRA PÁGINA e ÚLTIMAS 3:
{resumo}

O conteúdo AINDA FAZ PARTE da mesma legislação?
- Artigos, parágrafos, anexos, tabelas, mapas = FAZ PARTE
- Outro decreto/lei/portaria completamente diferente = TERMINOU
- Nomeações, licitações, atos administrativos = TERMINOU

Responda SOMENTE com JSON:
{{"status": "continua"}} ou {{"status": "terminou", "ultima_pagina": NNN}}"""

                            resp_fim = _chamar_llm(prompt_fim, logs, f'📄 Leitura pág {batch_end}', max_retries=0)
                            if resp_fim:
                                try:
                                    resp_fim = re.sub(r'^```json\s*|\s*```$', '', resp_fim.strip())
                                    dados_fim = json.loads(resp_fim)
                                    if dados_fim.get('status') == 'terminou':
                                        ultima_pag = dados_fim.get('ultima_pagina', batch_end)
                                        paginas_lei = [(p, t) for p, t in paginas_lei if p + 1 <= ultima_pag]
                                        logs.append({'nivel': 'ok', 'msg': f'{label}: IA: legislação termina na pág {ultima_pag}'})
                                        fim_encontrado = True
                                        break
                                    else:
                                        logs.append({'nivel': 'info', 'msg': f'{label}: IA: legislação continua (pág {batch_end})'})
                                except (json.JSONDecodeError, KeyError):
                                    pass

                            if len(paginas_lei) >= 400:
                                logs.append({'nivel': 'aviso', 'msg': f'{label}: limite de 400 páginas atingido'})
                                break

                        doc.close()
                        try:
                            os.unlink(dl_path)
                        except Exception:
                            pass

                        # Montar resultado
                        if paginas_lei:
                            texto_final = re.sub(r'\s+', ' ', ' '.join(p[1] for p in paginas_lei)).strip()
                            pags = [p[0]+1 for p in paginas_lei]
                            logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ Legislação extraída — págs {pags[0]} a {pags[-1]} ({len(pags)} págs, {len(texto_final)} chars)'})

                            resultados.append({
                                'url': dl_url,
                                'titulo': f'{tipo_desc} nº {num} — DO {data_dia}',
                                'snippet': f'Diário Oficial de {data_dia}, págs {pags[0]}-{pags[-1]}',
                                '_texto_direto': texto_final,
                            })

                            # Encontrou! Fechar browser e retornar
                            browser.close()
                            return resultados

                    except Exception as e_pdf:
                        logs.append({'nivel': 'aviso', 'msg': f'{label}: Erro ao ler PDF: {str(e_pdf)[:80]}'})
                        try:
                            os.unlink(dl_path)
                        except Exception:
                            pass

                page.close()

            # Nenhum dia funcionou
            browser.close()
            logs.append({'nivel': 'aviso', 'msg': f'{label}: 📅 Legislação não encontrada em {len(datas_tentar)} dia(s) do DO'})

    except Exception as e:
        import traceback
        logs.append({'nivel': 'aviso', 'msg': f'{label}: Erro DO: {str(e)[:100]}'})
        logs.append({'nivel': 'info', 'msg': f'{label}: {traceback.format_exc()[-200:]}'})

    return resultados


def _navegar_formulario_com_ia(url_base: str, tipo_lei: str, numero_lei: str,
                               ano: str, data_pub: str, logs: list, label: str,
                               municipio: str = '') -> list:
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
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                accept_downloads=True
            )
            page = ctx.new_page()
            _apply_stealth(page)

            # 1) Abrir a página
            _bloqueado = False

            try:
                page.goto(url_base, wait_until='networkidle', timeout=20000)
            except Exception:
                try:
                    page.goto(url_base, wait_until='domcontentloaded', timeout=15000)
                except Exception as e:
                    logs.append({'nivel': 'aviso', 'msg': f'{label}: Não conseguiu abrir: {str(e)[:80]}'})
                    _bloqueado = True

            if not _bloqueado:
                _time.sleep(1)
                url_atual = page.url
                titulo_pagina = page.title() or ''
                body_text = (page.inner_text('body') or '')[:500]

                # Bloqueio WAF definitivo
                _waf_patterns = ['request rejected', 'was rejected', 'access denied',
                                 'url was rejected', 'forbidden', 'has been blocked']
                if any(p in body_text.lower() for p in _waf_patterns):
                    logs.append({'nivel': 'aviso', 'msg': f'{label}: WAF bloqueou acesso'})
                    _bloqueado = True

                # Cloudflare — aguardar até 15s
                if not _bloqueado:
                    _cf_patterns = ['just a moment', 'checking your browser', 'security verification',
                                    'verify you are human', 'challenge-platform', 'cloudflare',
                                    'aguarde', 'um momento', 'verificação de segurança']
                    _body_lower = body_text.lower()
                    _titulo_lower = titulo_pagina.lower()
                    if any(p in _body_lower or p in _titulo_lower for p in _cf_patterns):
                        logs.append({'nivel': 'info', 'msg': f'{label}: ⏳ Verificação de segurança — aguardando até 15s...'})
                        _cf_resolveu = False
                        for _cf_wait in range(15):
                            _time.sleep(1)
                            try:
                                _new_title = page.title() or ''
                                _new_body = (page.inner_text('body') or '')[:500]
                                _new_lower = _new_body.lower()
                                if not any(p in _new_lower or p in _new_title.lower() for p in _cf_patterns):
                                    logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ Verificação resolvida após {_cf_wait+1}s'})
                                    titulo_pagina = _new_title
                                    body_text = _new_body
                                    url_atual = page.url
                                    _cf_resolveu = True
                                    break
                            except Exception:
                                pass
                        if not _cf_resolveu:
                            logs.append({'nivel': 'aviso', 'msg': f'{label}: ⏳ Verificação não resolveu em 15s'})
                            _bloqueado = True

            # ── RETRY COM PROXY se bloqueado ──
            if _bloqueado:
                proxy_config = _get_proxy_config()
                if proxy_config:
                    logs.append({'nivel': 'info', 'msg': f'{label}: 🔄 Tentando com proxy residencial...'})
                    try:
                        browser.close()
                    except Exception:
                        pass

                    # Relançar browser COM proxy (mesmo pw instance)
                    launch_args['proxy'] = proxy_config
                    try:
                        browser = pw.chromium.launch(**launch_args)
                    except Exception:
                        if 'executable_path' in launch_args:
                            del launch_args['executable_path']
                        browser = pw.chromium.launch(**launch_args)

                    ctx = browser.new_context(
                        viewport={'width': 1280, 'height': 900},
                        user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                        accept_downloads=True
                    )
                    page = ctx.new_page()
                    _apply_stealth(page)

                    try:
                        page.goto(url_base, wait_until='networkidle', timeout=25000)
                    except Exception:
                        try:
                            page.goto(url_base, wait_until='domcontentloaded', timeout=15000)
                        except Exception as e:
                            logs.append({'nivel': 'aviso', 'msg': f'{label}: ❌ Proxy falhou ao abrir: {str(e)[:80]}'})
                            browser.close()
                            return []

                    _time.sleep(1)
                    url_atual = page.url
                    titulo_pagina = page.title() or ''
                    body_text = (page.inner_text('body') or '')[:500]

                    # WAF definitivo mesmo com proxy?
                    _waf_patterns_proxy = ['was rejected', 'request rejected', 'access denied',
                                           'url was rejected', 'forbidden']
                    if any(p in body_text.lower() for p in _waf_patterns_proxy):
                        logs.append({'nivel': 'aviso', 'msg': f'{label}: ❌ Proxy também foi bloqueado (WAF)'})
                        browser.close()
                        return []

                    # Cloudflare com proxy — aguardar até 20s (proxy resolve mais rápido)
                    _cf_patterns_proxy = ['just a moment', 'checking your browser', 'security verification',
                                          'verify you are human', 'challenge-platform', 'cloudflare',
                                          'aguarde', 'um momento', 'verificação de segurança']
                    _proxy_body_lower = body_text.lower()
                    _proxy_titulo_lower = titulo_pagina.lower()
                    if any(p in _proxy_body_lower or p in _proxy_titulo_lower for p in _cf_patterns_proxy):
                        logs.append({'nivel': 'info', 'msg': f'{label}: ⏳ Proxy: Cloudflare detectado — aguardando até 20s...'})
                        _proxy_cf_ok = False
                        for _pcf in range(20):
                            _time.sleep(1)
                            try:
                                _pt = page.title() or ''
                                _pb = (page.inner_text('body') or '')[:500]
                                if not any(p in _pb.lower() or p in _pt.lower() for p in _cf_patterns_proxy):
                                    logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ Proxy: Cloudflare resolveu após {_pcf+1}s'})
                                    titulo_pagina = _pt
                                    body_text = _pb
                                    url_atual = page.url
                                    _proxy_cf_ok = True
                                    break
                            except Exception:
                                pass
                        if not _proxy_cf_ok:
                            logs.append({'nivel': 'aviso', 'msg': f'{label}: ❌ Proxy: Cloudflare não resolveu em 20s'})
                            browser.close()
                            return []

                    logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ Proxy funcionou — página carregada'})
                    _bloqueado = False
                else:
                    logs.append({'nivel': 'aviso', 'msg': f'{label}: 🔒 Bloqueado (configure PROXY_URL no Railway para contornar)'})
                    browser.close()
                    return []

            # 2A) Esperar conteúdo renderizar + detectar iframes/framesets
            target_frame = page  # padrão: frame principal
            _n_fields = 0

            for _spa_wait in range(8):  # até 8 segundos
                # Checar campos no frame principal
                _n_fields = page.evaluate('''() => {
                    return document.querySelectorAll(
                        'input:not([type="hidden"]), select, textarea, ' +
                        '[role="textbox"], [role="searchbox"], [contenteditable="true"], ' +
                        '[type="search"], .form-control, [ng-model], [v-model]'
                    ).length;
                }''')
                if _n_fields > 0:
                    if _spa_wait > 0:
                        logs.append({'nivel': 'info', 'msg': f'{label}: 🌐 SPA renderizou após {_spa_wait}s ({_n_fields} campos)'})
                    break

                # Se não tem campos no main, procurar em frames filhos
                frames = page.frames
                if len(frames) > 1:
                    for frame in frames:
                        if frame == page.main_frame:
                            continue
                        try:
                            _n_sub = frame.evaluate('''() => {
                                return document.querySelectorAll('input:not([type="hidden"]), select, textarea').length;
                            }''')
                            if _n_sub > 0:
                                frame_url = frame.url or '?'
                                logs.append({'nivel': 'info', 'msg': f'{label}: 🌐 Formulário em sub-frame ({_n_sub} campos): {frame_url[:60]}'})
                                target_frame = frame
                                _n_fields = _n_sub
                                break
                        except Exception:
                            continue
                    if _n_fields > 0:
                        break

                _time.sleep(1)

            # 2B) Se ainda sem campos, tentar navegar direto pro frameset content
            if _n_fields == 0:
                has_frameset = page.evaluate('''() => {
                    // Checar frameset ou iframe
                    const fs = document.querySelector('frameset');
                    if (fs) {
                        const frames = document.querySelectorAll('frame');
                        for (const f of frames) {
                            const name = (f.name || '').toLowerCase();
                            if (name.includes('main') || name.includes('conteudo') || name.includes('content') || name.includes('corpo') || name.includes('busca')) {
                                return f.src || null;
                            }
                        }
                        // Fallback: último frame
                        return frames.length > 0 ? frames[frames.length - 1].src : null;
                    }
                    // Checar iframe único
                    const iframe = document.querySelector('iframe');
                    if (iframe && iframe.src) return iframe.src;
                    return null;
                }''')

                if has_frameset:
                    from urllib.parse import urljoin
                    frame_url_full = urljoin(url_base, has_frameset)
                    logs.append({'nivel': 'info', 'msg': f'{label}: 🌐 Frameset detectado — navegando pra {frame_url_full[:80]}'})
                    try:
                        page.goto(frame_url_full, wait_until='networkidle', timeout=15000)
                        _time.sleep(1)
                        target_frame = page  # agora o page É o conteúdo do frame
                        _n_fields = page.evaluate('''() => {
                            return document.querySelectorAll('input:not([type="hidden"]), select, textarea').length;
                        }''')
                        if _n_fields > 0:
                            logs.append({'nivel': 'info', 'msg': f'{label}: 🌐 Frame carregado com {_n_fields} campos'})
                    except Exception as ef:
                        logs.append({'nivel': 'aviso', 'msg': f'{label}: 🌐 Erro ao abrir frame: {str(ef)[:60]}'})
                else:
                    # Log: nenhum frameset encontrado, checar se tem pouco HTML
                    html_len = page.evaluate('() => document.body ? document.body.innerHTML.length : 0')
                    logs.append({'nivel': 'info', 'msg': f'{label}: 🌐 Sem frameset. HTML body: {html_len} chars'})

                    # DIAGNÓSTICO: se body é muito pequeno, loggar o HTML pra debug
                    if html_len < 500:
                        diag = page.evaluate('''() => {
                            const html = document.documentElement.outerHTML.substring(0, 800);
                            const url = window.location.href;
                            const iframes = document.querySelectorAll('iframe').length;
                            const frames_count = window.frames ? window.frames.length : 0;
                            const title = document.title;
                            return {html, url, iframes, frames_count, title};
                        }''')
                        logs.append({'nivel': 'info', 'msg': f'{label}: 📋 URL atual: {diag.get("url","?")}'})
                        logs.append({'nivel': 'info', 'msg': f'{label}: 📋 Title: {diag.get("title","?")}'})
                        logs.append({'nivel': 'info', 'msg': f'{label}: 📋 iframes: {diag.get("iframes",0)}, window.frames: {diag.get("frames_count",0)}'})
                        _html_preview = diag.get('html','')[:300].replace('\n', ' ')
                        logs.append({'nivel': 'info', 'msg': f'{label}: 📋 HTML: {_html_preview}'})

            # 3) Navegar como humano — IA decide passo a passo
            from modulos.navegador_universal import navegar_como_humano
            
            legislacao_info = {
                'tipo': tipo_lei or 'Lei Complementar',
                'numero': numero_lei or '',
                'ano': ano or '',
                'municipio': municipio or '',
                'data_publicacao': data_pub or ''
            }
            
            nav_resultado = navegar_como_humano(
                page=page,
                frame=target_frame,
                legislacao=legislacao_info,
                chamar_llm=_chamar_llm,
                logs=logs,
                label=label,
                max_passos=20
            )
            
            if nav_resultado.get('encontrada'):
                url_resultado = nav_resultado.get('url', '')
                status_lei = nav_resultado.get('status', '')
                confirmacao = nav_resultado.get('confirmacao', '')
                
                resultado_item = {
                    'url': url_resultado,
                    'titulo': f'{tipo_lei} nº {numero_lei}/{ano}',
                    'snippet': confirmacao[:300],
                    'status': status_lei,
                }
                
                # Se baixou PDF, guardar caminho
                if nav_resultado.get('pdf_path'):
                    resultado_item['pdf_path'] = nav_resultado['pdf_path']
                    if nav_resultado.get('pagina_pdf'):
                        resultado_item['pagina_pdf'] = nav_resultado['pagina_pdf']
                
                resultados.append(resultado_item)

            browser.close()

            if resultados:
                logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ {len(resultados)} resultado(s) extraído(s)'})
            else:
                logs.append({'nivel': 'aviso', 'msg': f'{label}: Nenhum resultado relevante encontrado'})

    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        logs.append({'nivel': 'aviso', 'msg': f'{label}: 🌐 Erro na navegação: {str(e)[:120]}'})
        logs.append({'nivel': 'info', 'msg': f'{label}: 🌐 Detalhe: {tb[-200:]}'})
        try:
            browser.close()  # noqa
        except Exception:
            pass

    return resultados


def _buscar_no_site_direto(url_base: str, tipo_lei: str, numero_lei: str, ano: str, logs: list, label: str, data_pub: str = '', municipio: str = '') -> list:
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
    nav_results = _navegar_formulario_com_ia(url_base, tipo_lei, numero_lei, ano, data_pub, logs, label, municipio=municipio)
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


def _ia_validar_documento(texto: str, tipo_lei: str, numero_lei: str, ano: str,
                          logs: list, label: str) -> dict:
    """
    Usa IA para ler o início do documento e determinar se é a legislação buscada
    ou apenas uma referência/citação.

    Retorna: {'eh_legislacao': bool, 'motivo': str, 'confianca': float}
    """
    # Pegar início do documento (onde está o cabeçalho/ementa)
    trecho = texto[:2000] if len(texto) > 2000 else texto

    desc = f'{tipo_lei} nº {numero_lei}' if tipo_lei and numero_lei else f'legislação nº {numero_lei}'
    if ano:
        desc += f'/{ano}'

    prompt = f"""Leia o início deste documento e responda: este é o TEXTO INTEGRAL da {desc} ou é outro documento que apenas CITA/REFERENCIA essa legislação?

INÍCIO DO DOCUMENTO:
{trecho}

Responda APENAS com JSON:
{{
    "eh_legislacao": true ou false,
    "motivo": "explicação breve (ex: 'É o texto da lei, começa com o cabeçalho e ementa' ou 'É um decreto que cita a lei no artigo X')",
    "confianca": 0.0 a 1.0
}}

CRITÉRIOS:
- Se o documento COMEÇA com o tipo e número da legislação (cabeçalho formal), é a legislação
- Se o documento é outro tipo (ex: um DECRETO que cita a lei), NÃO é a legislação
- Se é um Diário Oficial com várias publicações, identifique se a legislação buscada ESTÁ publicada ali
- Se é uma página web que mostra o texto completo da lei, é a legislação
- Não importa o formato exato — entenda o CONTEÚDO"""

    resp = _chamar_llm(prompt, logs, f'{label} validação', max_retries=1)
    if not resp:
        # Sem IA: aceitar por padrão (melhor incluir do que perder)
        return {'eh_legislacao': True, 'motivo': 'IA indisponível — aceito por padrão', 'confianca': 0.5}

    try:
        resp = re.sub(r'^```(?:json)?\s*|\s*```$', '', resp.strip())
        info = json.loads(resp)
        return {
            'eh_legislacao': bool(info.get('eh_legislacao', True)),
            'motivo': info.get('motivo', ''),
            'confianca': float(info.get('confianca', 0.5)),
        }
    except (json.JSONDecodeError, ValueError):
        return {'eh_legislacao': True, 'motivo': 'Resposta IA inválida — aceito por padrão', 'confianca': 0.4}


def _acessar_pagina(url: str, termos: str, headers: dict, logs: list, label: str, tipo_lei: str = '', numero_lei: str = '', ano: str = '') -> Optional[dict]:
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
        _playwright_ok = False
        resp = req.get(url, headers=headers, timeout=12, allow_redirects=True, stream=True)

        # Retry com Referer no 403 (bypass WAF básico)
        if resp.status_code == 403:
            parsed = urlparse(url)
            headers_retry = dict(headers)
            headers_retry['Referer'] = f'{parsed.scheme}://{parsed.netloc}/'
            headers_retry['Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
            resp = req.get(url, headers=headers_retry, timeout=12, allow_redirects=True, stream=True)

        # ── FALLBACK PROXY CLOUDFLARE para 403 (WAF) ──
        if resp.status_code == 403:
            proxy_url = os.environ.get('PROXY_WORKER_URL', '')
            proxy_key = os.environ.get('PROXY_WORKER_KEY', '')
            if proxy_url and proxy_key:
                logs.append({'nivel': 'info', 'msg': f'{label}: HTTP 403 — tentando via proxy...'})
                try:
                    proxy_headers = {
                        'X-Proxy-Key': proxy_key,
                        'X-Target-URL': url,
                    }
                    resp_proxy = req.get(f'{proxy_url}/proxy', headers=proxy_headers, timeout=15, stream=True)
                    if resp_proxy.status_code == 200:
                        logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ Proxy obteve resposta ({len(resp_proxy.content)} bytes)'})
                        resp = resp_proxy  # Usar resposta do proxy
                    else:
                        logs.append({'nivel': 'aviso', 'msg': f'{label}: Proxy retornou {resp_proxy.status_code}'})
                except Exception as e_proxy:
                    logs.append({'nivel': 'aviso', 'msg': f'{label}: Proxy falhou: {str(e_proxy)[:60]}'})

        if resp.status_code != 200:
            # ── FALLBACK PLAYWRIGHT para sites que bloqueiam requests (WAF) ──
            if resp.status_code == 403:
                logs.append({'nivel': 'info', 'msg': f'{label}: HTTP 403 — tentando com navegador real...'})
                try:
                    from playwright.sync_api import sync_playwright as _sp
                    import shutil as _sh, glob as _gl
                    _exec_path = os.environ.get('PLAYWRIGHT_CHROMIUM_PATH', '')
                    if not _exec_path:
                        for _cn in ['chromium', 'chromium-browser', 'google-chrome-stable']:
                            _p = _sh.which(_cn)
                            if _p:
                                _exec_path = _p
                                break
                    if not _exec_path:
                        for _fp in ['/usr/bin/chromium', '/usr/bin/chromium-browser']:
                            if os.path.isfile(_fp):
                                _exec_path = _fp
                                break
                    if not _exec_path:
                        _nps = _gl.glob('/nix/store/*/bin/chromium')
                        if _nps:
                            _exec_path = _nps[0]

                    with _sp() as _pw:
                        _la = {'headless': True, 'args': ['--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu', '--single-process', '--no-zygote']}
                        if _exec_path:
                            _la['executable_path'] = _exec_path
                        _br = _pw.chromium.launch(**_la)
                        _ctx = _br.new_context(
                            viewport={'width': 1280, 'height': 900},
                            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                        )
                        _pg = _ctx.new_page()
                        _apply_stealth(_pg)
                        try:
                            _pg.goto(url, wait_until='networkidle', timeout=20000)
                        except Exception:
                            _pg.goto(url, wait_until='domcontentloaded', timeout=15000)
                        _time.sleep(1)

                        # Extrair texto da página
                        _html = _pg.content()
                        _body_text = _pg.inner_text('body') or ''

                        # Se bloqueado, tentar com proxy
                        _bl = _body_text.lower()[:300]
                        if len(_body_text) < 100 or 'was rejected' in _bl or 'request rejected' in _bl or 'access denied' in _bl or 'just a moment' in _bl or 'cloudflare' in _bl:
                            _proxy_cfg = _get_proxy_config()
                            if _proxy_cfg:
                                logs.append({'nivel': 'info', 'msg': f'{label}: 🔄 Tentando com proxy...'})
                                _br.close()
                                _la['proxy'] = _proxy_cfg
                                _br = _pw.chromium.launch(**_la)
                                _ctx = _br.new_context(
                                    viewport={'width': 1280, 'height': 900},
                                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                                )
                                _pg = _ctx.new_page()
                                _apply_stealth(_pg)
                                try:
                                    _pg.goto(url, wait_until='networkidle', timeout=25000)
                                except Exception:
                                    _pg.goto(url, wait_until='domcontentloaded', timeout=15000)
                                _time.sleep(1)
                                _body_text = _pg.inner_text('body') or ''

                                # Esperar Cloudflare resolver com proxy (até 15s)
                                _bl2 = _body_text.lower()[:300]
                                _cf_web = ['just a moment', 'checking your browser', 'cloudflare',
                                           'security verification', 'verify you are human']
                                if any(p in _bl2 for p in _cf_web):
                                    for _ww in range(15):
                                        _time.sleep(1)
                                        try:
                                            _body_text = _pg.inner_text('body') or ''
                                            if not any(p in _body_text.lower()[:300] for p in _cf_web):
                                                break
                                        except Exception:
                                            pass

                        _br.close()

                        if _body_text and len(_body_text) > 100:
                            logs.append({'nivel': 'ok', 'msg': f'{label}: ✓ Playwright obteve {len(_body_text)} chars de {url[:50]}'})
                            texto = _body_text
                            _pagina1_texto = ''
                            _playwright_ok = True
                            # Continuar processamento normal com esse texto
                        else:
                            logs.append({'nivel': 'aviso', 'msg': f'{label}: Playwright obteve pouco conteúdo ({len(_body_text)} chars)'})
                            return None
                except (ImportError, Exception) as e_pw:
                    logs.append({'nivel': 'aviso', 'msg': f'{label}: HTTP {resp.status_code} em {url[:60]}'})
                    return None
            else:
                logs.append({'nivel': 'aviso', 'msg': f'{label}: HTTP {resp.status_code} em {url[:60]}'})
                return None

        content_type = resp.headers.get('Content-Type', '').lower() if not _playwright_ok else ''
        if not _playwright_ok:
            texto = ''
        _pagina1_texto = '' if not _playwright_ok else _pagina1_texto

        # ── PDF ──
        if not _playwright_ok and ('pdf' in content_type or url.lower().endswith('.pdf') or '/download/' in url.lower()):
            logs.append({'nivel': 'info', 'msg': f'{label}: Detectado PDF, baixando e extraindo texto...'})
            try:
                pdf_bytes = resp.content
                if len(pdf_bytes) > 50_000_000:  # max 50MB
                    logs.append({'nivel': 'aviso', 'msg': f'{label}: PDF muito grande ({len(pdf_bytes)//1024}KB), pulando'})
                    return None
                import fitz  # PyMuPDF
                doc = fitz.open(stream=pdf_bytes, filetype='pdf')
                total_pages = len(doc)

                # Se temos número da lei, busca inteligente em PDFs grandes
                if numero_lei and numero_lei.strip() and total_pages > 20:
                    logs.append({'nivel': 'info', 'msg': f'{label}: PDF grande ({total_pages} págs) — buscando legislação...'})

                    _pagina1_texto = re.sub(r'\s+', ' ', doc[0].get_text()).strip()[:2000] if total_pages > 0 else ''
                    tipo_desc = tipo_lei or 'legislação'
                    num = numero_lei.strip()
                    pagina_inicio = -1

                    # ═══ PASSO 1: Ler sumário/índice (primeiras 3 páginas) ═══
                    sumario_texto = ''
                    for i in range(min(3, total_pages)):
                        sumario_texto += f'\n--- PÁGINA {i+1} ---\n' + doc[i].get_text()

                    # Checar se o sumário menciona a lei
                    num_no_sumario = num in sumario_texto
                    if num_no_sumario:
                        logs.append({'nivel': 'info', 'msg': f'{label}: 📑 Sumário menciona nº {num} — IA vai localizar a página...'})
                        prompt_sumario = f"""Leia este sumário/índice de um Diário Oficial e me diga em qual PÁGINA começa a "{tipo_desc} nº {num}".

SUMÁRIO (primeiras 3 páginas do PDF):
{sumario_texto[:3000]}

Responda APENAS com JSON:
{{"pagina": número_da_página, "motivo": "explicação breve"}}

Se o sumário lista a lei com número de página, use esse número.
Se não conseguir determinar a página exata, responda {{"pagina": 0, "motivo": "não encontrado no sumário"}}"""

                        resp_sum = _chamar_llm(prompt_sumario, logs, f'{label} sumário', max_retries=1)
                        if resp_sum:
                            try:
                                resp_sum = re.sub(r'^```(?:json)?\s*|\s*```$', '', resp_sum.strip())
                                sum_info = json.loads(resp_sum)
                                pag_sum = int(sum_info.get('pagina', 0))
                                if pag_sum > 0 and pag_sum <= total_pages:
                                    pagina_inicio = pag_sum - 1
                                    logs.append({'nivel': 'ok', 'msg': f'{label}: 📑 Sumário indica pág {pag_sum} — {sum_info.get("motivo","")}'})
                            except (json.JSONDecodeError, ValueError):
                                pass

                    # ═══ PASSO 2: Busca rápida em TODAS as páginas (Ctrl+F) ═══
                    if pagina_inicio < 0:
                        logs.append({'nivel': 'info', 'msg': f'{label}: 🔍 Buscando nº {num} em todas as {total_pages} páginas...'})

                        # Normalizar o número pra busca
                        num_variantes = [num]
                        # Adicionar variantes: "270" pode aparecer como "Nº 270", "N° 270", "nº270"
                        # Não precisa regex aqui — basta procurar o número como string

                        for i in range(total_pages):
                            page_text = doc[i].get_text()
                            page_lower = page_text.lower()

                            # Checar se o número aparece
                            if num not in page_text and num not in page_lower:
                                continue

                            # Número encontrado! Checar se é no contexto certo
                            # (tipo_lei junto do número, não número solto)
                            if tipo_lei:
                                tip_lower = tipo_lei.lower()
                                # Procurar tipo + número na mesma página
                                tip_words = tip_lower.split()
                                tem_tipo = all(w in page_lower for w in tip_words)
                                if tem_tipo:
                                    # Verificar se é cabeçalho (não referência)
                                    # Cabeçalho: "LEI COMPLEMENTAR Nº 270, DE 16 DE JANEIRO DE 2024"
                                    # Referência: "nos termos da Lei Complementar nº 270"
                                    # Pegar contexto ao redor do número
                                    pos = page_lower.find(num)
                                    ctx_antes = page_lower[max(0, pos-80):pos]

                                    # Se seguido de ", de DD de" → é cabeçalho com data
                                    ctx_depois = page_lower[pos+len(num):pos+len(num)+40]
                                    eh_cabecalho = bool(re.search(r'[,/]\s*de\s+\d{1,2}\s+de\s+', ctx_depois))

                                    if eh_cabecalho:
                                        pagina_inicio = i
                                        ctx_preview = page_text[max(0,pos-30):pos+len(num)+50].strip()
                                        logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ CABEÇALHO na pág {i+1}: "{ctx_preview[:80]}"'})
                                        break
                                    elif pagina_inicio < 0:
                                        # Guardar como candidata (pode ser referência)
                                        pagina_inicio = i
                                        ctx_preview = page_text[max(0,pos-30):pos+len(num)+50].strip()
                                        logs.append({'nivel': 'info', 'msg': f'{label}: 📌 Menção na pág {i+1}: "{ctx_preview[:80]}" (verificando se há cabeçalho...)'})
                                        # Não parar — continuar buscando um cabeçalho
                            else:
                                # Sem tipo_lei, qualquer menção serve
                                pagina_inicio = i
                                logs.append({'nivel': 'ok', 'msg': f'{label}: 📌 Nº {num} encontrado na pág {i+1}'})
                                break

                        if pagina_inicio >= 0:
                            logs.append({'nivel': 'ok', 'msg': f'{label}: 🎯 Legislação localizada na pág {pagina_inicio + 1} de {total_pages}'})
                        else:
                            logs.append({'nivel': 'aviso', 'msg': f'{label}: nº {num} não encontrado em nenhuma das {total_pages} páginas'})
                            doc.close()
                            return None

                    # ═══ PASSO 3: Ler a partir da página encontrada ═══
                    if pagina_inicio >= 0:
                        logs.append({'nivel': 'ok', 'msg': f'{label}: lendo a partir da pág {pagina_inicio + 1}'})

                        # Ler em lotes, IA decide quando a legislação terminou
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
            # ── HTML ── (ou skip se Playwright já obteve o texto)
            if not _playwright_ok:
                html_raw = resp.text
                texto = _extrair_texto_html(html_raw)

                # ── Detectar e baixar ANEXOS ──
                texto_lower = texto.lower()
                menciona_anexo = bool(re.search(r'\banexo\b', texto_lower))

                if menciona_anexo:
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

        # ── VALIDAÇÃO POR IA: a IA lê o documento e decide se é a legislação ──
        if tipo_lei and numero_lei:
            validacao = _ia_validar_documento(texto, tipo_lei, numero_lei, ano, logs, label)

            if not validacao['eh_legislacao']:
                logs.append({'nivel': 'aviso', 'msg': f'{label}: ❌ IA descartou — {validacao["motivo"]}'})
                return None

            logs.append({'nivel': 'ok', 'msg': f'{label}: ✅ IA confirmou — {validacao["motivo"]}'})
            relevancia = max(relevancia, validacao['confianca'])

        logs.append({'nivel': 'ok' if relevancia > 0.3 else 'info',
                     'msg': f'{label}: {len(texto)} chars, {matches}/{len(lista_termos)} termos ({relevancia:.0%} relevância)'})
        return {'url': url, 'texto': texto, 'nome': label, 'relevancia': relevancia, '_pagina1': _pagina1_texto}
    except Exception as e:
        logs.append({'nivel': 'aviso', 'msg': f'{label}: {str(e)[:80]}'})
        return None


def busca_manual(params: dict, log_callback=None) -> dict:
    """
    Busca legislação com pesquisa REAL na internet.
    Fluxo v3 (inteligente):
    1º Descoberta rápida: UMA busca web pra achar TIPO + DATA de publicação
    2º Sites prioritários: Playwright navega formulários (DO, sites do usuário)
    3º Fallback web: LeisMunicipais, Legisweb, Google (se 2 não bastou)
    4º IA compara fontes e sugere resultado final
    """
    if log_callback:
        class _LogStream(list):
            def append(self, item):
                super().append(item)
                try: log_callback(item)
                except: pass
        logs = _LogStream()
    else:
        logs = []

    logs.append({'nivel': 'info', 'msg': f'🔧 busca_manual v3 iniciou. Params: {list(params.keys())}'})
    fontes_status = []

    try:
        return _busca_manual_core(params, logs, fontes_status)
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        logs.append({'nivel': 'erro', 'msg': f'❌ ERRO FATAL: {str(e)[:200]}'})
        logs.append({'nivel': 'erro', 'msg': f'Traceback: {tb[-500:]}'})
        return {'legislacoes': [], 'erro': str(e)[:300], 'logs': logs, 'fontes': fontes_status}


def _busca_manual_core(params, logs, fontes_status):
    """Lógica principal do busca_manual (separada para capturar exceções)."""
    import requests as req
    esfera = params.get('esfera', '')
    estado = params.get('estado', '')
    municipio = params.get('municipio', '')
    tipo = params.get('tipo', '')
    numero = params.get('numero', '')
    ano = params.get('ano', '')
    # Proteção: ano pode vir None, int, ou string
    ano = str(ano).strip() if ano else ''
    # Extrair ano do número se formato "198/2019"
    if not ano and numero and '/' in str(numero):
        _partes_num = str(numero).split('/')
        if len(_partes_num) == 2 and _partes_num[1].isdigit() and len(_partes_num[1]) == 4:
            ano = _partes_num[1]
            numero = _partes_num[0]

    if tipo and tipo.strip() in ('?', '--', '-', 'Selecione', 'selecione', 'Outro', 'outro', ''):
        tipo = ''
    data_pub = params.get('data_publicacao', '')
    assunto = params.get('assunto', '')
    palavras = params.get('palavras_chave', '')
    url_diario = params.get('url_diario', '')
    urls_extras = params.get('urls_extras', [])
    fontes_prioritarias = params.get('fontes_prioritarias', [])
    if isinstance(fontes_prioritarias, str):
        fontes_prioritarias = [u.strip() for u in fontes_prioritarias.split('\n') if u.strip()]

    # Compatibilidade: se veio no formato antigo, converter
    if not fontes_prioritarias and (url_diario or urls_extras):
        if isinstance(urls_extras, str):
            urls_extras = [u.strip() for u in urls_extras.split('\n') if u.strip()]
        if url_diario:
            fontes_prioritarias = [url_diario] + list(urls_extras)
        else:
            fontes_prioritarias = list(urls_extras)
    fontes_prioritarias = [f for f in fontes_prioritarias[:3] if f]

    termos_busca = ' '.join(filter(None, [tipo, numero, ano, municipio, assunto, palavras]))
    if not termos_busca.strip():
        return {'legislacoes': [], 'erro': 'Preencha ao menos um campo.', 'logs': logs}

    headers_http = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
        'Accept-Language': 'pt-BR,pt;q=0.9',
    }
    textos_extraidos = []

    desc_legislacao = f'{tipo} nº {numero}/{ano}' if tipo and numero else f'legislação {numero}/{ano}'
    desc_completa = f'{desc_legislacao} — {municipio}, {estado}' if municipio else desc_legislacao

    logs.append({'nivel': 'info', 'msg': f'📋 Legislação: {desc_completa}'})

    # Detectar quais fontes são DO (para usar função dedicada)
    dominios_do_patterns = ['diariooficial', 'doweb', 'dom.', 'diariomunicipal', 'imprensaoficial']

    def _eh_diario_oficial(url):
        url_lower = url.lower()
        return any(d in url_lower for d in dominios_do_patterns)

    medalhas = ['🥇', '🥈', '🥉']
    for i, f in enumerate(fontes_prioritarias):
        tipo_f = 'DO' if _eh_diario_oficial(f) else 'Site'
        logs.append({'nivel': 'info', 'msg': f'{medalhas[i]} Fonte {i+1} ({tipo_f}): {f}'})

    # ══════════════════════════════════════════════════════════════════
    # ETAPA 1: DESCOBERTA — perguntar direto ao Gemini
    # ══════════════════════════════════════════════════════════════════
    logs.append({'nivel': 'info', 'msg': '🔍 ETAPA 1: Descobrindo tipo e data de publicação...'})

    data_descoberta = data_pub or ''
    tipo_inferido = tipo or ''
    snippets_web = []  # guardar pra fallback na ETAPA 3

    if not data_descoberta or not tipo_inferido:
        # PASSO 1: Buscar na web com query direta
        esfera_txt = esfera or 'municipal'
        if esfera_txt.lower() == 'municipal':
            local_txt = f'{municipio} {estado}'
        elif esfera_txt.lower() == 'estadual':
            local_txt = estado
        else:
            local_txt = 'federal'
        
        query_direta = f'"{tipo or "lei"} {numero}" "{ano}" {local_txt} data publicação'.strip()
        snippets_web = _pesquisar_web(query_direta, logs, '🔍 Descoberta', max_results=5)

        if snippets_web:
            snippets_texto = '\n'.join([
                f'[{i+1}] {s.get("titulo","")}: {s.get("snippet","")}'
                for i, s in enumerate(snippets_web[:5])
            ])
            prompt_desc = f"""Analise estes resultados de busca e extraia informações sobre a legislação:

Legislação buscada: {desc_completa}

Resultados:
{snippets_texto}

Responda APENAS com JSON:
{{
    "tipo": "Lei Complementar|Lei Ordinária|Decreto|Resolução|...",
    "data_publicacao": "AAAA-MM-DD",
    "ementa_resumida": "breve descrição"
}}
IMPORTANTE: a legislação é do {local_txt}. Ignore resultados de outros municípios/estados.
Se não encontrar, deixe "". NÃO invente."""

            resp_ia = _chamar_llm(prompt_desc, logs, '🔍 IA Descoberta')
            if resp_ia:
                try:
                    resp_ia = re.sub(r'^```(?:json)?\s*|\s*```$', '', resp_ia.strip())
                    info = json.loads(resp_ia)
                    if not tipo_inferido and info.get('tipo'):
                        tipo_inferido = info['tipo']
                        logs.append({'nivel': 'ok', 'msg': f'📋 Tipo: {tipo_inferido}'})
                    if not data_descoberta and info.get('data_publicacao'):
                        _data_cand = info['data_publicacao']
                        if ano and _data_cand[:4] != str(ano):
                            logs.append({'nivel': 'aviso', 'msg': f'📅 Data {_data_cand} descartada (ano {_data_cand[:4]} ≠ {ano})'})
                        else:
                            data_descoberta = _data_cand
                            logs.append({'nivel': 'ok', 'msg': f'📅 Data: {data_descoberta}'})
                    if info.get('ementa_resumida'):
                        logs.append({'nivel': 'info', 'msg': f'📝 Assunto: {info["ementa_resumida"][:100]}'})
                except (json.JSONDecodeError, ValueError):
                    pass
        
        # Fallback: regex nos snippets
        if not data_descoberta and numero and snippets_web:
            meses_map = {'janeiro':'01','fevereiro':'02','março':'03','marco':'03','abril':'04',
                         'maio':'05','junho':'06','julho':'07','agosto':'08','setembro':'09',
                         'outubro':'10','novembro':'11','dezembro':'12'}
            for snp in snippets_web:
                txt = (snp.get('titulo','') + ' ' + snp.get('snippet','')).lower()
                m = re.search(r'(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})', txt)
                if m and m.group(2) in meses_map:
                    _ano_regex = m.group(3)
                    if ano and _ano_regex != str(ano):
                        continue
                    data_descoberta = f'{_ano_regex}-{meses_map[m.group(2)]}-{m.group(1).zfill(2)}'
                    logs.append({'nivel': 'ok', 'msg': f'📅 Data (regex): {data_descoberta}'})
                    break
                m2 = re.search(r'(\d{2})/(\d{2})/(\d{4})', txt)
                if m2:
                    _ano_regex2 = m2.group(3)
                    if ano and _ano_regex2 != str(ano):
                        continue
                    data_descoberta = f'{_ano_regex2}-{m2.group(2)}-{m2.group(1)}'
                    logs.append({'nivel': 'ok', 'msg': f'📅 Data (regex): {data_descoberta}'})
                    break
    else:
        logs.append({'nivel': 'ok', 'msg': f'📋 Tipo: {tipo_inferido} | Data: {data_descoberta} (informados pelo usuário)'})

    # Segurança: validar que data descoberta bate com o ano informado
    if data_descoberta and ano and data_descoberta[:4] != str(ano):
        logs.append({'nivel': 'aviso', 'msg': f'📅 Data {data_descoberta} descartada (ano {data_descoberta[:4]} != {ano} informado)'})
        data_descoberta = ''

    if not data_descoberta:
        if ano:
            logs.append({'nivel': 'info', 'msg': f'📅 Data exata desconhecida — usando ano {ano} para direcionar buscas'})
        else:
            logs.append({'nivel': 'aviso', 'msg': '📅 Data e ano desconhecidos — buscas menos precisas'})

    # ══════════════════════════════════════════════════════════════════
    # ETAPA 2: FONTES PRIORITÁRIAS — na ordem 1 → 2 → 3
    # ══════════════════════════════════════════════════════════════════
    logs.append({'nivel': 'info', 'msg': '🌐 ETAPA 2: Navegando nas fontes prioritárias...'})

    sites_prioritarios = []
    for i, fonte_url in enumerate(fontes_prioritarias):
        if not fonte_url.startswith('http'):
            fonte_url = 'https://' + fonte_url
        dom = re.sub(r'https?://', '', fonte_url.rstrip('/')).split('/')[0]
        eh_do = _eh_diario_oficial(fonte_url)
        sites_prioritarios.append({
            'url': fonte_url,
            'label': f'{medalhas[i]} Fonte {i+1} ({dom})',
            'tipo_fonte': 'diario_oficial' if eh_do else 'site_informado',
        })

    for site in sites_prioritarios:
        logs.append({'nivel': 'info', 'msg': f'{site["label"]}: Abrindo com navegador...'})

        # ── TODAS as fontes usam o navegador universal ──
        nav_results = _navegar_formulario_com_ia(
            site['url'], tipo_inferido or tipo, numero, ano,
            data_descoberta or data_pub,
            logs, site['label'],
            municipio=municipio
        )

        encontrou_site = False
        if nav_results:
            # ── Priorizar resultados: links com nome/número da lei primeiro ──
            num_busca = (numero or '').strip().lower()
            tipo_busca = (tipo_inferido or tipo or '').lower()

            def _prioridade_link(r):
                """Menor número = maior prioridade. Downloads antes de viewers."""
                url_lower = (r.get('url') or '').lower()
                titulo_lower = (r.get('titulo') or '').lower()
                combined = url_lower + ' ' + titulo_lower
                is_download = '/download/' in url_lower
                is_pagina = bool(re.search(r'/download/\d+/\d+', url_lower))

                # Prioridade 0: Já tem conteúdo extraído
                if r.get('_texto_direto'):
                    return 0
                # Prioridade 1: Download de página específica com nº da lei
                if is_download and is_pagina and num_busca and num_busca in combined:
                    return 1
                # Prioridade 2: Download de página específica
                if is_download and is_pagina:
                    return 2
                # Prioridade 3: Download completo com nº da lei
                if is_download and num_busca and num_busca in combined:
                    return 3
                # Prioridade 4: Viewer PDF com nome da lei
                if '/ver/' in url_lower and '/ver-html/' not in url_lower:
                    return 4
                # Prioridade 5: Qualquer download
                if is_download or '.pdf' in url_lower:
                    return 5
                # Prioridade 6: HTML viewer
                if '/ver-html/' in url_lower:
                    return 6
                return 9

            nav_results_sorted = sorted(nav_results, key=_prioridade_link)
            logs.append({'nivel': 'ok', 'msg': f'{site["label"]}: {len(nav_results_sorted)} resultado(s) — validando...'})
            for nav_r in nav_results_sorted[:8]:
                # Se tem PDF local baixado, extrair texto direto
                if nav_r.get('pdf_path') and os.path.isfile(nav_r['pdf_path']):
                    try:
                        import fitz  # PyMuPDF
                        doc = fitz.open(nav_r['pdf_path'])
                        texto_pdf = ''
                        for pg in doc:
                            texto_pdf += pg.get_text()
                        doc.close()
                        
                        if len(texto_pdf) > 100:
                            textos_extraidos.append({
                                'url': nav_r.get('url', '') or site['url'],
                                'texto': texto_pdf,
                                'nome': site['label'],
                                'relevancia': 0.9,
                                '_fonte': site['tipo_fonte'],
                                'pdf_path': nav_r['pdf_path'],
                            })
                            fontes_status.append({'nome': site['label'], 'url': site['url'], 'encontrou': True})
                            encontrou_site = True
                            logs.append({'nivel': 'ok', 'msg': f'{site["label"]}: ✅ PDF extraído ({len(texto_pdf)} chars)'})
                            break
                        else:
                            logs.append({'nivel': 'aviso', 'msg': f'{site["label"]}: PDF muito curto ({len(texto_pdf)} chars)'})
                    except Exception as e_pdf:
                        logs.append({'nivel': 'aviso', 'msg': f'{site["label"]}: Erro ao ler PDF: {str(e_pdf)[:60]}'})
                    continue

                if nav_r.get('_texto_direto'):
                    texto_nav = nav_r['_texto_direto']
                    if len(texto_nav) > 100:
                        textos_extraidos.append({
                            'url': nav_r['url'], 'texto': texto_nav,
                            'nome': site['label'], 'relevancia': 0.7,
                            '_fonte': site['tipo_fonte'],
                        })
                        fontes_status.append({'nome': site['label'], 'url': nav_r['url'], 'encontrou': True})
                        encontrou_site = True
                        logs.append({'nivel': 'ok', 'msg': f'{site["label"]}: ✅ Conteúdo direto ({len(texto_nav)} chars)'})
                        break
                    else:
                        logs.append({'nivel': 'aviso', 'msg': f'{site["label"]}: resultado inline muito curto ({len(texto_nav)} chars) — pulando'})
                        continue

                result = _acessar_pagina(
                    nav_r['url'], termos_busca, headers_http, logs, site['label'],
                    tipo_lei=tipo_inferido or tipo, numero_lei=numero, ano=ano
                )
                if result:
                    result['_fonte'] = site['tipo_fonte']
                    textos_extraidos.append(result)
                    fontes_status.append({'nome': site['label'], 'url': nav_r['url'], 'encontrou': True})
                    encontrou_site = True
                    logs.append({'nivel': 'ok', 'msg': f'{site["label"]}: ✅ Legislação encontrada!'})
                    break
                else:
                    # Fallback: tentar com browser + proxy se HTTP falhou
                    nav_url = nav_r.get('url', '')
                    if nav_url and nav_url.startswith('http'):
                        proxy_config = _get_proxy_config()
                        if proxy_config:
                            logs.append({'nivel': 'info', 'msg': f'{site["label"]}: 🔄 HTTP rejeitado — tentando com browser+proxy...'})
                            try:
                                from playwright.sync_api import sync_playwright as _sp_retry
                                with _sp_retry() as pw_retry:
                                    retry_args = {
                                        'headless': True,
                                        'args': ['--no-sandbox', '--disable-blink-features=AutomationControlled'],
                                        'proxy': proxy_config,
                                    }
                                    # Encontrar chromium
                                    _exec_path = os.environ.get('PLAYWRIGHT_CHROMIUM_PATH', '')
                                    if not _exec_path:
                                        import shutil as _sh_r
                                        for cn in ['chromium', 'chromium-browser']:
                                            p = _sh_r.which(cn)
                                            if p:
                                                _exec_path = p
                                                break
                                        if not _exec_path:
                                            import glob as _gl_r
                                            nps = _gl_r.glob('/nix/store/*/bin/chromium')
                                            if nps:
                                                _exec_path = nps[0]
                                    if _exec_path:
                                        retry_args['executable_path'] = _exec_path
                                    
                                    br_retry = pw_retry.chromium.launch(**retry_args)
                                    ctx_retry = br_retry.new_context(
                                        viewport={'width': 1280, 'height': 900},
                                        user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                                    )
                                    pg_retry = ctx_retry.new_page()
                                    _apply_stealth(pg_retry)
                                    
                                    import time as _time_r
                                    try:
                                        pg_retry.goto(nav_url, wait_until='networkidle', timeout=20000)
                                    except Exception:
                                        pg_retry.goto(nav_url, wait_until='domcontentloaded', timeout=15000)
                                    
                                    _time_r.sleep(2)
                                    body_text = (pg_retry.inner_text('body') or '')[:50000]
                                    
                                    # Verificar se não é WAF
                                    waf_check = body_text[:500].lower()
                                    if not any(p in waf_check for p in ['request rejected', 'access denied', 'forbidden']):
                                        if len(body_text) > 100:
                                            textos_extraidos.append({
                                                'url': nav_url,
                                                'texto': body_text,
                                                'nome': site['label'],
                                                'relevancia': 0.8,
                                                '_fonte': site['tipo_fonte'],
                                            })
                                            fontes_status.append({'nome': site['label'], 'url': nav_url, 'encontrou': True})
                                            encontrou_site = True
                                            logs.append({'nivel': 'ok', 'msg': f'{site["label"]}: ✅ Browser+proxy: {len(body_text)} chars'})
                                    else:
                                        logs.append({'nivel': 'aviso', 'msg': f'{site["label"]}: ❌ Browser+proxy também bloqueado'})
                                    
                                    br_retry.close()
                                    if encontrou_site:
                                        break
                            except Exception as e_retry:
                                logs.append({'nivel': 'aviso', 'msg': f'{site["label"]}: ⚠️ Browser+proxy falhou: {str(e_retry)[:40]}'})
                    
                    logs.append({'nivel': 'aviso', 'msg': f'{site["label"]}: resultado descartado'})

        if not encontrou_site:
            fontes_status.append({'nome': site['label'], 'url': site['url'], 'encontrou': False})

    # ══════════════════════════════════════════════════════════════════
    # ETAPA 3: FALLBACK WEB — LeisMunicipais, Legisweb, Google
    # ══════════════════════════════════════════════════════════════════
    logs.append({'nivel': 'info', 'msg': '📖 ETAPA 3: Fontes web complementares...'})

    # 3A: LeisMunicipais
    if municipio and numero:
        lm_results = _buscar_leismunicipais_direto(municipio, estado, tipo_inferido or tipo, numero, ano, logs)
        for lm in lm_results[:2]:
            result = _acessar_pagina(lm['url'], termos_busca, headers_http, logs, '📖 LeisMunicipais',
                                     tipo_lei=tipo_inferido or tipo, numero_lei=numero, ano=ano)
            if result:
                result['_fonte'] = 'leismunicipais'
                textos_extraidos.append(result)
                fontes_status.append({'nome': '📖 LeisMunicipais', 'url': lm['url'], 'encontrou': True})
                break

    # 3B: Busca geral DuckDuckGo
    query_web = f'"{tipo_inferido or tipo} {numero}" {ano} {municipio}'.strip()
    if not query_web or len(query_web) < 5:
        query_web = termos_busca
    web_results = _pesquisar_web(query_web, logs, '🔎 Web', max_results=5)

    # Adicionar snippets da ETAPA 1 que não vieram agora
    urls_ja = {w['url'] for w in web_results}
    for snp in snippets_web:
        if snp['url'] not in urls_ja:
            web_results.append(snp)

    urls_visitadas = {t['url'] for t in textos_extraidos}
    acessados = 0
    for wr in web_results:
        if wr['url'] in urls_visitadas or acessados >= 3:
            continue
        result = _acessar_pagina(wr['url'], termos_busca, headers_http, logs, '🔎 Web',
                                 tipo_lei=tipo_inferido or tipo, numero_lei=numero, ano=ano)
        if result:
            result['_fonte'] = 'google'
            textos_extraidos.append(result)
            fontes_status.append({'nome': '🔎 Web', 'url': wr['url'], 'encontrou': True})
            acessados += 1

    # 3C: DuckDuckGo nas fontes DO (se Playwright falhou E não temos fontes suficientes)
    ja_tem_fontes = len(textos_extraidos) > 0
    fontes_do = [f for f in fontes_prioritarias if _eh_diario_oficial(f)]
    do_ja_achou = any(f.get('encontrou') and ('Fonte' in f.get('nome','') or '🏛️' in f.get('nome','')) for f in fontes_status
                      if any(_eh_diario_oficial(fp) for fp in fontes_prioritarias))

    if fontes_do and not do_ja_achou and not ja_tem_fontes:
        dominio_do = re.sub(r'https?://', '', fontes_do[0].rstrip('/')).split('/')[0]
        if data_descoberta:
            logs.append({'nivel': 'info', 'msg': f'🏛️ DO: formulário falhou e sem fontes — DuckDuckGo por data...'})
            from datetime import datetime, timedelta
            try:
                data_base = datetime.strptime(data_descoberta, '%Y-%m-%d')
            except ValueError:
                data_base = None

            query_curta = ' '.join(filter(None, [tipo_inferido or tipo, numero, ano]))
            if data_base:
                encontrou_do_ddg = False
                for delta in range(0, 8):
                    if encontrou_do_ddg:
                        break
                    data_busca = data_base + timedelta(days=delta)
                    data_fmt1 = data_busca.strftime('%d/%m/%Y')
                    meses_pt = ['','janeiro','fevereiro','março','abril','maio','junho',
                                'julho','agosto','setembro','outubro','novembro','dezembro']
                    data_fmt3 = f'{data_busca.day} de {meses_pt[data_busca.month]} de {data_busca.year}'

                    ddg_do = _pesquisar_web(f'site:{dominio_do} {query_curta} "{data_fmt1}"', logs, f'🏛️ DO {data_fmt1}', max_results=3)
                    if not ddg_do:
                        ddg_do = _pesquisar_web(f'site:{dominio_do} {query_curta} "{data_fmt3}"', logs, f'🏛️ DO {data_fmt3}', max_results=3)

                    for ddg in ddg_do:
                        titulo_l = (ddg.get('titulo','') or '').lower()
                        tipo_b = (tipo_inferido or tipo or '').lower()
                        if tipo_b and 'decreto' in titulo_l and 'decreto' not in tipo_b:
                            logs.append({'nivel': 'aviso', 'msg': f'🏛️ DO ({data_fmt1}): título DECRETO — pulando'})
                            continue
                        result = _acessar_pagina(ddg['url'], termos_busca, headers_http, logs, f'🏛️ DO ({data_fmt1})',
                                                 tipo_lei=tipo_inferido or tipo, numero_lei=numero, ano=ano)
                        if result:
                            result['_fonte'] = 'diario_oficial'
                            textos_extraidos.append(result)
                            fontes_status.append({'nome': '🏛️ Diário Oficial', 'url': ddg['url'], 'encontrou': True})
                            encontrou_do_ddg = True
                            logs.append({'nivel': 'ok', 'msg': f'🏛️ ✅ Encontrado no DO de {data_fmt1}!'})
                            break

    elif fontes_do and ja_tem_fontes and not do_ja_achou:
        logs.append({'nivel': 'ok', 'msg': f'🏛️ DO: pulado (já encontrou {len(textos_extraidos)} fonte(s) em outros sites)'})

    logs.append({'nivel': 'ok', 'msg': f'📄 {len(textos_extraidos)} fonte(s) com texto extraído'})

    # ── Priorizar fontes: prioritárias primeiro, depois web ──
    def _prioridade_fonte(t):
        fonte = t.get('_fonte', '')
        nome = t.get('nome', '')
        # Fontes prioritárias (Fonte 1, 2, 3) → prioridade 0-2
        for i, fp in enumerate(fontes_prioritarias):
            dom_fp = re.sub(r'https?://', '', fp.rstrip('/')).split('/')[0].lower()
            url_t = (t.get('url', '') or '').lower()
            if dom_fp and dom_fp in url_t:
                return i
        # Web → prioridade 10
        return 10

    textos_extraidos.sort(key=_prioridade_fonte)

    # Máximo 4 resultados — priorizando fontes prioritárias
    if len(textos_extraidos) > 4:
        logs.append({'nivel': 'info', 'msg': f'📄 Limitando a 4 melhores fontes (de {len(textos_extraidos)})'})
        textos_extraidos = textos_extraidos[:4]

    # ══════════════════════════════════════════════════════════════════
    # ETAPA 4: IA COMPARA, SUGERE E JUSTIFICA
    # ══════════════════════════════════════════════════════════════════
    descricao_busca = '\n'.join(filter(None, [
        f"- Esfera: {esfera}" if esfera else None,
        f"- Estado: {estado}" if estado else None,
        f"- Município: {municipio}" if municipio else None,
        f"- Tipo: {tipo_inferido or tipo}" if (tipo_inferido or tipo) else None,
        f"- Número: {numero}" if numero else None,
        f"- Ano: {ano}" if ano else None,
        f"- Data publicação: {data_descoberta or data_pub}" if (data_descoberta or data_pub) else None,
        f"- Assunto: {assunto}" if assunto else None,
        f"- Palavras-chave: {palavras}" if palavras else None,
    ]))

    total_fontes = len(textos_extraidos)
    logs.append({'nivel': 'info', 'msg': f'🤖 ETAPA 4: Enviando {total_fontes} fonte(s) para análise final da IA...'})

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

    # Ajustar limite de truncamento: menos fontes = mais texto por fonte
    _chars_por_fonte = 4000
    if len(fontes_para_llm) == 1:
        _chars_por_fonte = 8000  # fonte única: mais contexto
    elif len(fontes_para_llm) == 2:
        _chars_por_fonte = 6000

    fontes_texto = '\n\n'.join([
        f"=== FONTE {i+1}: {t['nome']} ({t['url'][:80]}) ===\nRelevância: {t['relevancia']:.0%}\nTamanho total: {len(t['texto'])} chars\n"
        f"{'⚠️ FONTE PRÉ-VALIDADA: Texto extraído diretamente do Diário Oficial com cabeçalho formal confirmado. Esta fonte é altamente confiável.' if t.get('_fonte') == 'diario_oficial' else ''}\n"
        f"{_truncar_inteligente(t['texto'], _chars_por_fonte)}"
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
7. Fontes marcadas como PRÉ-VALIDADAS (ex: Diário Oficial com cabeçalho confirmado) são ALTAMENTE confiáveis. Se o texto truncado contém o cabeçalho formal da legislação buscada (tipo, número e ano), ACEITE como resultado válido com confiança alta. O texto completo já foi verificado pelo sistema.
8. IMPORTANTE: Se o texto começa com ou contém o cabeçalho da legislação buscada (mesmo que truncado), é a legislação correta. NÃO rejeite apenas porque o texto está truncado.

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
    legislacoes = [l for l in legislacoes if (l.get('confianca') or 0) >= 0.5]
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
        tf = {
            'nome': t.get('nome', ''),
            'url': t.get('url', ''),
            'relevancia': round(t.get('relevancia', 0), 2),
            'texto': t.get('texto', ''),  # texto integral para salvar no DB via job cache
            '_fonte': t.get('_fonte', ''),
        }
        if t.get('pdf_path') and os.path.isfile(t['pdf_path']):
            tf['pdf_path'] = t['pdf_path']
        textos_fontes.append(tf)

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
