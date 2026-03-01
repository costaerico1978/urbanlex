#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
modulos/scraper_inteligente.py  (v5.0 — busca por legislação)
──────────────────────────────────────────────────────────────────────────────
Scraper de diários oficiais com busca DIRECIONADA por legislação.

Diferença fundamental da v4:
  - Antes: acessava o diário e tentava parsear com seletores CSS genéricos
  - Agora: PESQUISA pelo número da lei no sistema de busca do diário

Estratégias de busca (em ordem de preferência):
  1. Playwright — navega até a página de busca, digita o termo, lê resultados
  2. Requests + IA — acessa via HTTP e pede à IA para extrair
  3. Fallback — busca genérica (compatibilidade)

Fluxo principal:
  buscar_publicacoes_legislacao(mun_id, leg_info, data_ini, data_fim)
    → retorna {sucesso, publicacoes: [{titulo, data, url, conteudo}], total}

  detectar_e_salvar_perfil(municipio_id)  ← mantido para aprender sobre o site
"""

import os
import json
import base64
import logging
import traceback
import re
import time
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict
from urllib.parse import urljoin, urlencode, quote

import psycopg2
from psycopg2.extras import RealDictCursor
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv('DATABASE_URL')

# ─────────────────────────────────────────────────────────────────────────────
# Helpers de banco
# ─────────────────────────────────────────────────────────────────────────────

def _get_db():
    if DATABASE_URL:
        return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    return psycopg2.connect(
        host=os.getenv('DB_HOST','localhost'),
        database=os.getenv('DB_NAME','urbanismo'),
        user=os.getenv('DB_USER','postgres'),
        password=os.getenv('DB_PASSWORD',''),
        cursor_factory=RealDictCursor
    )

def _qry(sql, params=None, fetch=None, commit=False):
    conn = _get_db()
    cur  = conn.cursor()
    try:
        cur.execute(sql, params or ())
        result = None
        if fetch == 'one':
            result = cur.fetchone()
        elif fetch == 'all' or fetch is None:
            try: result = cur.fetchall()
            except Exception: result = []
        if commit:
            conn.commit()
        return result
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# Montar termos de busca a partir da legislação
# ─────────────────────────────────────────────────────────────────────────────

def _montar_termos_busca(leg_info: dict) -> List[str]:
    """
    Gera variações do nome da legislação para busca.
    Ex: Lei Complementar 270/2024 → [
      'Lei Complementar 270',
      'Lei Complementar nº 270',
      'LC 270/2024',
    ]
    """
    tipo = (leg_info.get('tipo_nome') or 'Lei').strip()
    numero = str(leg_info.get('numero') or '').strip()
    ano = str(leg_info.get('ano') or '').strip()

    if not numero:
        return []

    termos = []

    # Termo principal: "Tipo Numero"
    termos.append(f'{tipo} {numero}')

    # Com "nº": "Tipo nº Numero"
    termos.append(f'{tipo} nº {numero}')

    # Com ano: "Tipo Numero/Ano"
    if ano:
        termos.append(f'{tipo} {numero}/{ano}')

    # Abreviações comuns
    abrev_map = {
        'Lei Complementar': 'LC',
        'Lei Ordinária': 'Lei',
        'Decreto': 'Dec',
        'Decreto-Lei': 'DL',
        'Resolução': 'Res',
        'Portaria': 'Port',
    }
    abrev = abrev_map.get(tipo)
    if abrev and abrev != tipo:
        termos.append(f'{abrev} {numero}')
        if ano:
            termos.append(f'{abrev} {numero}/{ano}')

    # Número/Ano sozinho (último recurso, mais genérico)
    if ano:
        termos.append(f'{numero}/{ano}')

    return termos


# ─────────────────────────────────────────────────────────────────────────────
# ESTRATÉGIA 1: Busca direta na API interna do site (doweb, etc.)
# Tenta descobrir e chamar o endpoint Elasticsearch/API do diário
# ─────────────────────────────────────────────────────────────────────────────

def _buscar_api_direta(url_base: str, termo: str,
                        data_inicio: date, data_fim: date) -> dict:
    """
    Tenta descobrir e usar a API interna do diário oficial.
    Sites doweb (e similares) usam Elasticsearch por trás do Angular.
    """
    url_base = url_base.rstrip('/')
    headers = {
        'User-Agent': 'Mozilla/5.0 Chrome/120',
        'Accept': 'application/json, text/html, */*',
    }

    # Lista de endpoints comuns em plataformas de diário oficial
    endpoints_busca = [
        # doweb padrão
        (f"{url_base}/apifront/portal/edicoes/pesquisar_materias",
         {'termo': termo}, 'GET'),
        (f"{url_base}/apifront/portal/edicoes/pesquisar",
         {'termo': termo}, 'GET'),
        (f"{url_base}/apifront/portal/busca",
         {'termo': termo, 'pagina': '0', 'quantidade': '20'}, 'GET'),
        (f"{url_base}/apifront/portal/edicoes/buscar_por_texto",
         {'texto': termo, 'dataInicial': data_inicio.strftime('%d/%m/%Y'),
          'dataFinal': data_fim.strftime('%d/%m/%Y')}, 'GET'),
        # Elasticsearch proxy comum
        (f"{url_base}/elasticsearch/_search",
         None, 'POST_ES'),
        # Variações com query string simples
        (f"{url_base}/api/busca",
         {'q': termo}, 'GET'),
        (f"{url_base}/api/search",
         {'q': termo}, 'GET'),
    ]

    for url_ep, params, metodo in endpoints_busca:
        try:
            if metodo == 'POST_ES':
                # Tentar query Elasticsearch direta
                es_body = {
                    "query": {
                        "query_string": {
                            "query": f'"{termo}"'
                        }
                    },
                    "size": 20,
                    "sort": [{"_score": "desc"}]
                }
                resp = requests.post(url_ep, json=es_body, headers={
                    **headers, 'Content-Type': 'application/json'
                }, timeout=10)
            else:
                resp = requests.get(url_ep, params=params,
                                    headers=headers, timeout=10)

            if resp.status_code == 404 or resp.status_code == 405:
                continue

            if resp.status_code == 200:
                ct = resp.headers.get('content-type', '')
                texto = resp.text.strip()

                # Verificar se retornou JSON válido
                if 'json' in ct or (texto.startswith('{') or texto.startswith('[')):
                    try:
                        dados = resp.json()
                        pubs = _parsear_resposta_api(dados, url_base, termo)
                        if pubs is not None:  # None = formato não reconhecido
                            logger.info(f"  ✓ API direta OK: {url_ep} → {len(pubs)} resultados")
                            return {
                                'sucesso': True,
                                'publicacoes': pubs,
                                'total': len(pubs),
                                'endpoint': url_ep,
                                'mensagem': f'API direta: {len(pubs)} resultado(s)',
                            }
                    except (json.JSONDecodeError, ValueError):
                        pass

        except requests.exceptions.ConnectionError:
            continue
        except requests.exceptions.Timeout:
            continue
        except Exception:
            continue

    return {'sucesso': False, 'publicacoes': [], 'total': 0,
            'mensagem': 'Nenhum endpoint de API interna encontrado'}


def _parsear_resposta_api(dados: dict, url_base: str, termo: str) -> list:
    """
    Tenta interpretar a resposta JSON de diferentes formatos de API.
    Retorna lista de pubs ou None se o formato não for reconhecido.
    """
    pubs = []

    # Formato Elasticsearch direto: {hits: {total: N, hits: [{_source: {...}}]}}
    if isinstance(dados, dict) and 'hits' in dados:
        hits = dados['hits']
        if isinstance(hits, dict) and 'hits' in hits:
            for hit in hits['hits']:
                src = hit.get('_source', {})
                dia = src.get('day', '')
                mes = src.get('month', '')
                ano = src.get('year', '')
                data_pub = f"{ano}-{str(mes).zfill(2)}-{str(dia).zfill(2)}" if ano else ''

                # Tentar montar URL da matéria
                materia_id = src.get('id_materia') or src.get('materia') or ''
                edicao_id = src.get('id_edicao') or src.get('edicao') or ''
                url_mat = ''
                if materia_id and edicao_id:
                    url_mat = f"{url_base}/apifront/portal/edicoes/imprimir_materia/{materia_id}/{edicao_id}"

                texto = src.get('texto', '') or src.get('conteudo', '') or ''
                titulo = src.get('titulo', '') or f"Diário {data_pub}"

                pubs.append({
                    'titulo': titulo[:200],
                    'data': data_pub,
                    'url': url_mat,
                    'conteudo': texto[:50000],
                    'tipo': 'materia',
                })
            return pubs

    # Formato lista simples: [{titulo, data, url, ...}]
    if isinstance(dados, list):
        for item in dados[:20]:
            if isinstance(item, dict) and ('titulo' in item or 'data' in item):
                pubs.append({
                    'titulo': item.get('titulo', '')[:200],
                    'data': item.get('data', '') or item.get('date', ''),
                    'url': item.get('url', '') or item.get('link', ''),
                    'conteudo': item.get('conteudo', '') or item.get('texto', ''),
                    'tipo': 'materia',
                })
        return pubs if pubs else None

    # Formato com campo 'resultados', 'items', 'data'
    for campo in ['resultados', 'items', 'data', 'materias', 'publicacoes']:
        if isinstance(dados, dict) and campo in dados:
            items = dados[campo]
            if isinstance(items, list) and items:
                return _parsear_resposta_api(items, url_base, termo)

    # Formato com campo 'total' indicando que houve busca mas 0 resultados
    if isinstance(dados, dict) and 'total' in dados:
        total = dados.get('total', 0)
        if isinstance(total, int) and total == 0:
            return []  # Busca funcionou, mas 0 resultados

    return None  # Formato não reconhecido


# Cobre 5500+ municípios brasileiros. API gratuita e aberta.
# https://queridodiario.ok.org.br/api/docs
# ─────────────────────────────────────────────────────────────────────────────

QUERIDO_DIARIO_API = 'https://queridodiario.ok.org.br/api/gazettes'
QUERIDO_DIARIO_API_ALT = 'https://api.queridodiario.ok.org.br/gazettes'

def _buscar_querido_diario(codigo_ibge: str, termo: str,
                            data_inicio: date, data_fim: date) -> dict:
    """
    Busca no Querido Diário (OKBR) via API REST.
    Retorna {sucesso, publicacoes: [{titulo, data, url, conteudo}], total}
    """
    if not codigo_ibge:
        return {'sucesso': False, 'publicacoes': [], 'total': 0,
                'mensagem': 'Código IBGE não disponível'}

    logger.info(f"[Querido Diário] Buscando '{termo}' em {codigo_ibge}")

    try:
        params = {
            'territory_ids': codigo_ibge,
            'querystring': f'"{termo}"',
            'published_since': data_inicio.strftime('%Y-%m-%d'),
            'published_until': data_fim.strftime('%Y-%m-%d'),
            'excerpt_size': 500,
            'number_of_excerpts': 3,
            'size': 20,
        }
        headers = {
            'User-Agent': 'UrbanLex/5.0 (monitoramento legislativo)',
            'Accept': 'application/json',
        }

        resp = requests.get(QUERIDO_DIARIO_API, params=params,
                            headers=headers, timeout=30)

        # Fallback para URL alternativa se a principal falhar
        if resp.status_code != 200:
            logger.info(f"  QD API principal HTTP {resp.status_code}, tentando alternativa...")
            resp = requests.get(QUERIDO_DIARIO_API_ALT, params=params,
                                headers=headers, timeout=30)

        if resp.status_code != 200:
            logger.warning(f"  QD API: HTTP {resp.status_code}")
            return {'sucesso': False, 'publicacoes': [], 'total': 0,
                    'mensagem': f'QD API HTTP {resp.status_code}'}

        dados = resp.json()
        total = dados.get('total_gazettes', 0)
        gazettes = dados.get('gazettes', [])

        logger.info(f"  QD API: {total} resultados encontrados")

        publicacoes = []
        for g in gazettes:
            data_pub = g.get('date', '')
            edition = g.get('edition_number', '')
            extra = ' (Extra)' if g.get('is_extra_edition') else ''
            territorio = g.get('territory_name', '')

            titulo = f"DO {territorio} - Edição {edition}{extra} - {data_pub}"

            # Excerpts/highlights contêm o texto relevante
            excerpts = g.get('excerpts') or g.get('highlight_texts') or []
            conteudo = '\n---\n'.join(excerpts) if excerpts else ''

            # URLs para conteúdo
            url_txt = g.get('txt_url', '')
            url_pub = g.get('url', '') or g.get('file_url', '')

            pub = {
                'titulo': titulo,
                'data': data_pub,
                'url': url_pub,
                'url_texto': url_txt,
                'conteudo': conteudo[:50000] if conteudo else '',
                'tipo': 'edicao',
                'fonte': 'querido_diario',
            }
            publicacoes.append(pub)

        return {
            'sucesso': True,
            'publicacoes': publicacoes,
            'total': total,
            'mensagem': f'QD API: {total} resultado(s)',
        }

    except requests.exceptions.Timeout:
        logger.warning("  QD API: timeout")
        return {'sucesso': False, 'publicacoes': [], 'total': 0,
                'mensagem': 'QD API timeout'}
    except Exception as e:
        logger.error(f"  QD API erro: {e}")
        return {'sucesso': False, 'publicacoes': [], 'total': 0,
                'mensagem': f'QD API erro: {str(e)[:200]}'}


def _obter_codigo_ibge(municipio_id: int) -> str:
    """
    Obtém o código IBGE do município.
    1. Tenta do banco
    2. Se não tem, busca automaticamente na API do IBGE
    3. Salva no banco pra não precisar buscar de novo
    """
    try:
        mun = _qry("SELECT codigo_ibge, nome, estado FROM municipios WHERE id=%s",
                    (municipio_id,), 'one')
        if not mun:
            return ''

        # Já tem no banco?
        if mun.get('codigo_ibge'):
            return str(mun['codigo_ibge'])

        # Auto-lookup via API do IBGE
        nome = mun.get('nome', '')
        estado = mun.get('estado', '')
        if not nome:
            return ''

        logger.info(f"  Auto-buscando código IBGE para {nome}/{estado}...")
        codigo = _buscar_codigo_ibge_api(nome, estado)

        if codigo:
            # Salvar no banco pra não precisar buscar de novo
            try:
                _qry("UPDATE municipios SET codigo_ibge=%s WHERE id=%s",
                     (codigo, municipio_id), commit=True)
                logger.info(f"  ✓ Código IBGE {codigo} salvo automaticamente")
            except Exception:
                pass
            return codigo

    except Exception as e:
        logger.warning(f"  Erro ao obter código IBGE: {e}")
    return ''


def _buscar_codigo_ibge_api(nome_municipio: str, estado: str) -> str:
    """Busca o código IBGE na API pública do IBGE."""
    try:
        # API do IBGE: buscar todos municípios do estado
        UF_MAP = {
            'AC': 12, 'AL': 27, 'AP': 16, 'AM': 13, 'BA': 29, 'CE': 23,
            'DF': 53, 'ES': 32, 'GO': 52, 'MA': 21, 'MT': 51, 'MS': 50,
            'MG': 31, 'PA': 15, 'PB': 25, 'PR': 41, 'PE': 26, 'PI': 22,
            'RJ': 33, 'RN': 24, 'RS': 43, 'RO': 11, 'RR': 14, 'SC': 42,
            'SP': 35, 'SE': 28, 'TO': 17,
        }

        uf_code = UF_MAP.get(estado.upper().strip(), '')
        if not uf_code:
            return ''

        url = f'https://servicodados.ibge.gov.br/api/v1/localidades/estados/{uf_code}/municipios'
        resp = requests.get(url, timeout=15,
                            headers={'Accept': 'application/json'})

        if resp.status_code != 200:
            return ''

        municipios = resp.json()
        nome_norm = _normalizar_nome(nome_municipio)

        for m in municipios:
            if _normalizar_nome(m.get('nome', '')) == nome_norm:
                return str(m['id'])

        # Busca parcial (caso nome tenha diferenças menores)
        for m in municipios:
            if nome_norm in _normalizar_nome(m.get('nome', '')):
                return str(m['id'])

    except Exception as e:
        logger.warning(f"  Erro API IBGE: {e}")
    return ''


def _normalizar_nome(nome: str) -> str:
    """Remove acentos e normaliza para comparação."""
    import unicodedata
    nfkd = unicodedata.normalize('NFKD', nome.lower().strip())
    return ''.join(c for c in nfkd if not unicodedata.combining(c))


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _playwright_disponivel() -> bool:
    try:
        from playwright.sync_api import sync_playwright  # noqa
        return True
    except ImportError:
        return False

def _fmt_data_br(d):
    """date → 'dd/mm/aaaa'"""
    if isinstance(d, str):
        return d
    return d.strftime('%d/%m/%Y')


# ─────────────────────────────────────────────────────────────────────────────
# ESTRATÉGIA 1: Busca via Playwright (funciona para qualquer site com JS)
# ─────────────────────────────────────────────────────────────────────────────

def _buscar_com_playwright(url_busca: str, termo: str,
                            data_inicio: date, data_fim: date,
                            nome_mun: str) -> dict:
    """
    Abre a página de busca com Playwright, digita o termo, define datas e busca.
    Retorna {sucesso, html, url_final, mensagem}
    """
    if not _playwright_disponivel():
        return {'sucesso': False, 'html': '',
                'mensagem': 'Playwright não instalado'}

    logger.info(f"[Playwright] Buscando '{termo}' em {url_busca}")

    try:
        from playwright.sync_api import sync_playwright

        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=True,
                args=['--no-sandbox', '--disable-dev-shm-usage',
                      '--disable-gpu', '--single-process', '--no-zygote']
            )
            ctx = browser.new_context(
                viewport={'width': 1280, 'height': 900},
                user_agent='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120'
            )
            page = ctx.new_page()
            page.goto(url_busca, wait_until='networkidle', timeout=30000)
            time.sleep(2)

            # ── Preencher campo de busca ──
            campo_ok = False
            seletores_busca = [
                'input[type="search"]',
                'input[type="text"][placeholder*="usca"]',
                'input[type="text"][placeholder*="alavra"]',
                'input[type="text"][placeholder*="ermo"]',
                'input[ng-model*="query"]',
                'input[ng-model*="busca"]',
                'input[ng-model*="search"]',
                '#txtBusca', '#searchInput', '#query',
                'input.search-input',
                'input[name="q"]', 'input[name="query"]',
                'input[name="busca"]', 'input[name="termo"]',
            ]

            for sel in seletores_busca:
                try:
                    el = page.query_selector(sel)
                    if el and el.is_visible():
                        el.click()
                        el.fill(termo)
                        campo_ok = True
                        logger.info(f"  Campo de busca: {sel}")
                        break
                except Exception:
                    continue

            if not campo_ok:
                # Fallback: primeiro input text visível
                try:
                    inputs = page.query_selector_all('input[type="text"]')
                    for inp in inputs:
                        if inp.is_visible():
                            inp.click()
                            inp.fill(termo)
                            campo_ok = True
                            logger.info("  Campo de busca: primeiro input visível")
                            break
                except Exception:
                    pass

            if not campo_ok:
                logger.warning("  Não encontrou campo de busca")

            # ── Preencher datas (se existirem) ──
            data_ini_str = _fmt_data_br(data_inicio)
            data_fim_str = _fmt_data_br(data_fim)

            for sel in ['input[placeholder*="nicial"]', 'input[placeholder*="nício"]',
                        '#dataInicial', '#dtInicio', 'input[ng-model*="dataIni"]']:
                try:
                    el = page.query_selector(sel)
                    if el and el.is_visible():
                        el.click(); el.fill(data_ini_str)
                        logger.info(f"  Data início: {sel}")
                        break
                except Exception:
                    continue

            for sel in ['input[placeholder*="inal"]', 'input[placeholder*="Até"]',
                        '#dataFinal', '#dtFim', 'input[ng-model*="dataFim"]']:
                try:
                    el = page.query_selector(sel)
                    if el and el.is_visible():
                        el.click(); el.fill(data_fim_str)
                        logger.info(f"  Data fim: {sel}")
                        break
                except Exception:
                    continue

            # ── Clicar busca ou Enter ──
            btn_ok = False
            for sel in ['button[type="submit"]', 'input[type="submit"]',
                        'button:has-text("Buscar")', 'button:has-text("Pesquisar")',
                        'a:has-text("Buscar")', '#btnBuscar', '#btnSearch']:
                try:
                    el = page.query_selector(sel)
                    if el and el.is_visible():
                        el.click()
                        btn_ok = True
                        logger.info(f"  Botão: {sel}")
                        break
                except Exception:
                    continue

            if not btn_ok:
                try:
                    page.keyboard.press('Enter')
                    logger.info("  Enter pressionado")
                except Exception:
                    pass

            # Aguardar resultados
            page.wait_for_load_state('networkidle', timeout=15000)
            time.sleep(3)

            html = page.content()
            url_final = page.url
            browser.close()

            return {
                'sucesso': True,
                'html': html,
                'url_final': url_final,
                'mensagem': 'Busca via Playwright OK'
            }

    except Exception as e:
        logger.error(f"[Playwright] Erro: {e}")
        return {'sucesso': False, 'html': '',
                'mensagem': f'Erro Playwright: {str(e)[:200]}'}


# ─────────────────────────────────────────────────────────────────────────────
# Baixar conteúdo de uma matéria/publicação
# ─────────────────────────────────────────────────────────────────────────────

def _buscar_conteudo_materia(url_materia: str) -> str:
    """Baixa o conteúdo HTML de uma matéria específica e retorna texto limpo."""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 Chrome/120',
            'Accept': 'text/html,*/*',
        }
        resp = requests.get(url_materia, headers=headers, timeout=20)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, 'html.parser')
            for tag in soup.find_all(['script', 'style', 'nav', 'header', 'footer']):
                tag.decompose()
            return soup.get_text(separator='\n', strip=True)
    except Exception as e:
        logger.warning(f"Erro ao acessar matéria {url_materia}: {e}")
    return ''


# ─────────────────────────────────────────────────────────────────────────────
# IA: Extrair publicações do HTML de resultado de busca
# ─────────────────────────────────────────────────────────────────────────────

def _extrair_publicacoes_com_ia(html: str, url_base: str,
                                  termo_busca: str,
                                  data_inicio: date, data_fim: date) -> list:
    """
    Usa Gemini para interpretar o HTML da página de resultados
    e extrair as publicações encontradas.
    """
    gemini_key = os.getenv('GEMINI_API_KEY', '')
    if not gemini_key:
        logger.warning("GEMINI_API_KEY não configurada")
        return []

    html_t = html[:20000]

    try:
        import google.generativeai as genai
        genai.configure(api_key=gemini_key)
        model = genai.GenerativeModel('gemini-2.5-flash')

        prompt = f"""Analise o HTML abaixo de um sistema de busca de Diário Oficial.
O termo buscado foi: "{termo_busca}"
Período: {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}
URL base: {url_base}

Extraia TODAS as publicações/resultados listados na página.

IMPORTANTE:
- Cada resultado tem: título/descrição, data de publicação, link para conteúdo
- URLs relativas devem ser completadas com: {url_base}
- Se a página mostrar "nenhum resultado" ou similar, retorne lista vazia
- Se houver templates Angular como {{{{variavel}}}}, a página não renderizou — retorne vazio

Responda APENAS com JSON válido (sem markdown):
{{
  "total_informado": 0,
  "pagina_renderizou": true,
  "publicacoes": [
    {{
      "titulo": "título ou descrição",
      "data": "dd/mm/aaaa",
      "url": "URL completa para o conteúdo",
      "tipo": "edicao|materia|decreto|lei|portaria|outro"
    }}
  ],
  "observacao": "nota sobre os resultados"
}}

HTML:
{html_t}"""

        resp = model.generate_content(prompt)
        texto = resp.text.strip().replace('```json', '').replace('```', '').strip()
        dados = json.loads(texto)

        if not dados.get('pagina_renderizou', True):
            logger.warning("  IA detectou que a página não renderizou (Angular/JS)")
            return []

        pubs = dados.get('publicacoes', [])
        logger.info(f"  IA: {len(pubs)} publicações (total informado: {dados.get('total_informado', '?')})")
        return pubs

    except Exception as e:
        logger.error(f"Erro ao extrair publicações com IA: {e}")
        return []


# ─────────────────────────────────────────────────────────────────────────────
# FUNÇÃO PRINCIPAL v5: Buscar publicações para uma legislação específica
# ─────────────────────────────────────────────────────────────────────────────

def buscar_publicacoes_legislacao(municipio_id: int,
                                    legislacao_info: dict,
                                    data_inicio: date,
                                    data_fim: Optional[date] = None) -> dict:
    """
    Busca publicações no diário oficial que mencionam uma legislação específica.

    Args:
        municipio_id: ID do município
        legislacao_info: dict com {tipo_nome, numero, ano, ementa}
        data_inicio: data de início da busca
        data_fim: data final (default: hoje)

    Returns:
        {
            'sucesso': bool,
            'publicacoes': [{titulo, data, url, conteudo}],
            'total': int,
            'termos_usados': [str],
            'metodo': str,
            'mensagem': str
        }
    """
    if data_fim is None:
        data_fim = date.today()

    resultado = {
        'sucesso': False, 'publicacoes': [], 'total': 0,
        'termos_usados': [], 'metodo': 'nenhum', 'mensagem': '',
    }

    # ── 1. Buscar dados do município e perfil ──
    mun = _qry("SELECT * FROM municipios WHERE id=%s", (municipio_id,), 'one')
    if not mun:
        resultado['mensagem'] = 'Município não encontrado'
        return resultado

    nome_mun = mun['nome']

    # ── 2. Montar termos de busca ──
    termos = _montar_termos_busca(legislacao_info)
    if not termos:
        resultado['mensagem'] = 'Número da lei ausente — não é possível buscar'
        return resultado

    resultado['termos_usados'] = termos
    tipo_leg = legislacao_info.get('tipo_nome', 'Lei')
    numero_leg = legislacao_info.get('numero', '?')
    ano_leg = legislacao_info.get('ano', '')
    titulo_leg = f"{tipo_leg} {numero_leg}/{ano_leg}" if ano_leg else f"{tipo_leg} {numero_leg}"

    logger.info(f"=== Buscando {titulo_leg} no diário de {nome_mun} ===")
    logger.info(f"  Período: {data_inicio} → {data_fim}")

    # ── 3. ESTRATÉGIA 0: Querido Diário API (REST puro, sem JS) ──
    codigo_ibge = _obter_codigo_ibge(municipio_id)
    if codigo_ibge:
        logger.info(f"  Tentando Querido Diário API (IBGE: {codigo_ibge})")
        for i, termo in enumerate(termos[:2]):
            res_qd = _buscar_querido_diario(codigo_ibge, termo, data_inicio, data_fim)
            if res_qd.get('sucesso') and res_qd.get('total', 0) > 0:
                resultado['sucesso'] = True
                resultado['publicacoes'] = res_qd['publicacoes']
                resultado['total'] = res_qd['total']
                resultado['metodo'] = 'querido_diario'
                resultado['termos_usados'] = termos
                resultado['mensagem'] = (
                    f'{res_qd["total"]} publicação(ões) para "{titulo_leg}" '
                    f'via Querido Diário API ({data_inicio} a {data_fim})'
                )
                logger.info(f"  ✓ QD API: {res_qd['total']} resultados com '{termo}'")
                return resultado
            else:
                logger.info(f"  QD API: 0 resultados com '{termo}'")
        logger.info("  QD API: sem resultados, tentando scraping direto...")
    else:
        logger.info("  Código IBGE não disponível, pulando Querido Diário API")

    # ── 4. Preparar scraping direto (precisa URL) ──
    perfil = _qry("SELECT * FROM perfis_diario WHERE municipio_id=%s",
                   (municipio_id,), 'one')
    url_base = (mun.get('url_diario') or
                (perfil.get('url_base') if perfil else '') or '').rstrip('/')

    if not url_base:
        resultado['mensagem'] = 'URL do diário não configurada e Querido Diário sem resultados'
        resultado['sucesso'] = True  # não é erro, apenas sem resultados
        resultado['total'] = 0
        return resultado

    pjson = (perfil.get('perfil_json') or {}) if perfil else {}
    if isinstance(pjson, str):
        try: pjson = json.loads(pjson)
        except: pjson = {}

    url_busca = pjson.get('url_busca') or ''

    if not url_busca:
        # Testar URLs de busca comuns
        candidatas = [
            f"{url_base}/buscanova",
            f"{url_base}/buscanova/",
            f"{url_base}/busca",
            f"{url_base}/pesquisa",
            url_base,
        ]
        headers = {'User-Agent': 'Mozilla/5.0 Chrome/120'}
        for url_cand in candidatas:
            try:
                r = requests.head(url_cand, headers=headers, timeout=10,
                                   allow_redirects=True)
                if r.status_code == 200:
                    url_busca = url_cand
                    logger.info(f"  Página de busca: {url_busca}")
                    break
            except Exception:
                continue

    if not url_busca:
        url_busca = url_base

    # ── 5. Buscar via scraping — tentar cada termo até encontrar resultados ──
    todas_pubs = []

    for i, termo in enumerate(termos[:3]):
        logger.info(f"  Tentativa {i+1}: '{termo}'")

        # --- Estratégia A: Playwright ---
        if _playwright_disponivel():
            res = _buscar_com_playwright(url_busca, termo,
                                          data_inicio, data_fim, nome_mun)
            if res.get('sucesso') and res.get('html'):
                resultado['metodo'] = 'playwright'
                pubs = _extrair_publicacoes_com_ia(
                    res['html'], url_base, termo, data_inicio, data_fim
                )
                if pubs:
                    todas_pubs.extend(pubs)
                    logger.info(f"    → {len(pubs)} publicações com '{termo}'")
                    break
                else:
                    logger.info(f"    → 0 publicações com '{termo}'")
                    continue

        # --- Estratégia B: Requests simples ---
        try:
            headers = {'User-Agent': 'Mozilla/5.0 Chrome/120', 'Accept': 'text/html,*/*'}
            params = {'q': termo}
            p_ini = pjson.get('parametro_data_inicio')
            p_fim = pjson.get('parametro_data_fim')
            if p_ini:
                params[p_ini] = _fmt_data_br(data_inicio)
            if p_fim:
                params[p_fim] = _fmt_data_br(data_fim)

            resp = requests.get(url_busca, params=params, headers=headers, timeout=20)
            if resp.status_code == 200 and len(resp.text) > 500:
                resultado['metodo'] = 'requests'
                pubs = _extrair_publicacoes_com_ia(
                    resp.text, url_base, termo, data_inicio, data_fim
                )
                if pubs:
                    todas_pubs.extend(pubs)
                    logger.info(f"    → {len(pubs)} publicações (requests)")
                    break
        except Exception as e:
            logger.warning(f"    Requests falhou: {e}")

        time.sleep(1)

    # ── 5. Baixar conteúdo completo de cada publicação ──
    for pub in todas_pubs:
        url_pub = pub.get('url', '')
        if url_pub and not url_pub.startswith('http'):
            url_pub = urljoin(url_base + '/', url_pub)
            pub['url'] = url_pub

        if url_pub:
            logger.info(f"  Baixando: {url_pub[:80]}...")
            conteudo = _buscar_conteudo_materia(url_pub)
            pub['conteudo'] = conteudo[:50000] if conteudo else ''
            time.sleep(0.5)

    resultado['sucesso'] = True
    resultado['publicacoes'] = todas_pubs
    resultado['total'] = len(todas_pubs)
    resultado['mensagem'] = (
        f'{len(todas_pubs)} publicação(ões) para "{titulo_leg}" '
        f'de {data_inicio} a {data_fim}'
    )

    logger.info(f"  Resultado: {resultado['mensagem']}")
    return resultado


# ─────────────────────────────────────────────────────────────────────────────
# FUNÇÃO LEGADA (mantida para compatibilidade)
# ─────────────────────────────────────────────────────────────────────────────

def buscar_publicacoes(municipio_id: int,
                        data_inicio: date,
                        data_fim: Optional[date] = None) -> dict:
    """
    Busca genérica sem termo. Mantida para compatibilidade.
    """
    logger.warning("buscar_publicacoes() chamada sem legislação — busca genérica")
    return buscar_publicacoes_legislacao(
        municipio_id, {'tipo_nome': '', 'numero': '', 'ano': ''},
        data_inicio, data_fim
    )


# ─────────────────────────────────────────────────────────────────────────────
# Registro de falhas
# ─────────────────────────────────────────────────────────────────────────────

def registrar_falha(municipio_id: int, erro: str):
    """Incrementa contador de falhas."""
    try:
        _qry("""UPDATE perfis_diario
                SET falhas_consecutivas = COALESCE(falhas_consecutivas,0) + 1,
                    erro_deteccao = %s, atualizado_em = NOW()
                WHERE municipio_id = %s""", (erro[:500], municipio_id), commit=True)
    except Exception as e:
        logger.error(f"Erro ao registrar falha: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Inferir URL do diário
# ─────────────────────────────────────────────────────────────────────────────

def _inferir_url_diario(nome_municipio: str, estado: str) -> dict:
    gemini_key = os.getenv('GEMINI_API_KEY', '')
    if not gemini_key:
        return {'url': None, 'plataforma': 'desconhecido', 'confianca': 'baixa'}
    try:
        import google.generativeai as genai
        genai.configure(api_key=gemini_key)
        model = genai.GenerativeModel('gemini-2.5-flash')

        prompt = f"""Você é especialista em legislação municipal brasileira.
Preciso da URL oficial do Diário Oficial de: {nome_municipio} - {estado} - Brasil

Responda APENAS com JSON válido, sem markdown:
{{
  "url": "https://...",
  "plataforma": "iobnet|dom|amm|doweb|imprensa_oficial|custom|desconhecido",
  "confianca": "alta|media|baixa",
  "url_busca": "URL da página de busca se diferente da principal",
  "observacao": "breve nota"
}}"""

        resp = model.generate_content(prompt)
        texto = resp.text.strip().replace('```json', '').replace('```', '').strip()
        return json.loads(texto)
    except Exception as e:
        logger.error(f"Erro ao inferir URL: {e}")
        return {'url': None, 'plataforma': 'desconhecido', 'confianca': 'baixa'}


# ─────────────────────────────────────────────────────────────────────────────
# Screenshot e análise visual
# ─────────────────────────────────────────────────────────────────────────────

def _tirar_screenshot(url: str, timeout_ms: int = 20000) -> Optional[bytes]:
    if not _playwright_disponivel():
        return None
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=True,
                args=['--no-sandbox', '--disable-dev-shm-usage',
                      '--disable-gpu', '--single-process', '--no-zygote']
            )
            ctx = browser.new_context(viewport={'width': 1280, 'height': 900},
                                       user_agent='Mozilla/5.0 Chrome/120')
            page = ctx.new_page()
            page.goto(url, wait_until='networkidle', timeout=timeout_ms)
            shot = page.screenshot(full_page=False)
            browser.close()
            return shot
    except Exception as e:
        logger.error(f"Screenshot falhou: {e}")
        return None


def _analisar_screenshot_com_ia(screenshot_bytes: bytes, url: str,
                                  nome_municipio: str) -> dict:
    gemini_key = os.getenv('GEMINI_API_KEY', '')
    if not gemini_key or not screenshot_bytes:
        return {}
    try:
        import google.generativeai as genai
        genai.configure(api_key=gemini_key)
        model = genai.GenerativeModel('gemini-2.5-flash')

        img_b64 = base64.b64encode(screenshot_bytes).decode()
        img_part = {'mime_type': 'image/png', 'data': img_b64}

        prompt = f"""Analise o screenshot do Diário Oficial de {nome_municipio} ({url}).
Extraia perfil de navegação em JSON (sem markdown):
{{
  "plataforma_identificada": "nome",
  "tem_busca_por_palavra": true/false,
  "tem_busca_por_data": true/false,
  "url_busca": "URL da página de busca se diferente",
  "requer_javascript": true/false,
  "requer_login": true/false,
  "tem_captcha": true/false,
  "observacoes": "como buscar neste site"
}}"""

        resp = model.generate_content([prompt, img_part])
        texto = resp.text.strip().replace('```json', '').replace('```', '').strip()
        return json.loads(texto)
    except Exception as e:
        logger.error(f"Erro análise screenshot: {e}")
        return {}


def _extrair_perfil_do_html(html: str, url: str, nome_municipio: str) -> dict:
    gemini_key = os.getenv('GEMINI_API_KEY', '')
    if not gemini_key:
        return {}
    try:
        import google.generativeai as genai
        genai.configure(api_key=gemini_key)
        model = genai.GenerativeModel('gemini-2.5-flash')
        html_t = html[:12000]
        prompt = f"""Analise o HTML do Diário Oficial de {nome_municipio} ({url}).
Extraia o perfil de navegação em JSON (sem markdown):
{{"plataforma_identificada":"...","tem_busca_por_palavra":true/false,
"tem_busca_por_data":true/false,"url_busca":"...","requer_javascript":true/false,
"requer_login":false,"tem_captcha":false,"observacoes":"..."}}
HTML: {html_t}"""
        resp = model.generate_content(prompt)
        texto = resp.text.strip().replace('```json', '').replace('```', '').strip()
        return json.loads(texto)
    except Exception:
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# Salvar perfil no banco
# ─────────────────────────────────────────────────────────────────────────────

def _salvar_perfil_banco(municipio_id: int, resultado: dict):
    try:
        perfil_json_str = json.dumps(resultado.get('perfil_json') or {})
        _qry("""
            INSERT INTO perfis_diario
                (municipio_id, url_base, plataforma, status_deteccao, erro_deteccao,
                 perfil_json, screenshot_b64, detectado_em, requer_playwright,
                 requer_login, requer_captcha, atualizado_em)
            VALUES (%s,%s,%s,%s,%s,%s::jsonb,%s,NOW(),%s,%s,%s,NOW())
            ON CONFLICT (municipio_id) DO UPDATE SET
                url_base = EXCLUDED.url_base, plataforma = EXCLUDED.plataforma,
                status_deteccao = EXCLUDED.status_deteccao,
                erro_deteccao = EXCLUDED.erro_deteccao,
                perfil_json = EXCLUDED.perfil_json,
                screenshot_b64 = EXCLUDED.screenshot_b64,
                detectado_em = NOW(), requer_playwright = EXCLUDED.requer_playwright,
                requer_login = EXCLUDED.requer_login,
                requer_captcha = EXCLUDED.requer_captcha,
                falhas_consecutivas = 0, atualizado_em = NOW()
        """, (
            municipio_id, resultado.get('url_detectada', ''),
            resultado.get('plataforma', 'desconhecido'),
            resultado.get('status', 'pendente'), resultado.get('erro'),
            perfil_json_str, resultado.get('screenshot_b64'),
            resultado.get('requer_playwright', False),
            resultado.get('requer_login', False),
            resultado.get('requer_captcha', False),
        ), commit=True)
        _qry("UPDATE municipios SET perfil_detectado_em=NOW() WHERE id=%s",
             (municipio_id,), commit=True)
    except Exception as e:
        logger.error(f"Erro ao salvar perfil: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Detectar e salvar perfil (API pública)
# ─────────────────────────────────────────────────────────────────────────────

def detectar_e_salvar_perfil(municipio_id: int,
                               forcar_redeteccao: bool = False) -> dict:
    mun = _qry("SELECT * FROM municipios WHERE id=%s", (municipio_id,), 'one')
    if not mun:
        return {'sucesso': False, 'mensagem': 'Município não encontrado'}

    nome = mun['nome']
    estado = mun.get('estado', '')
    url_atual = mun.get('url_diario', '')

    if not forcar_redeteccao:
        pe = _qry("SELECT * FROM perfis_diario WHERE municipio_id=%s",
                   (municipio_id,), 'one')
        if pe and pe.get('status_deteccao') == 'ok':
            dias = (datetime.now() - pe['detectado_em']).days if pe.get('detectado_em') else 999
            if dias < 7:
                return {'sucesso': True, 'status': 'ok',
                        'perfil': dict(pe),
                        'mensagem': f'Perfil existente ({dias} dias atrás)'}

    logger.info(f"Detecção para {nome}/{estado}")
    resultado = {
        'sucesso': False, 'status': 'pendente', 'url_detectada': url_atual,
        'plataforma': 'desconhecido', 'perfil_json': {},
        'requer_playwright': False, 'requer_login': False,
        'requer_captcha': False, 'erro': None, 'mensagem': '',
        'screenshot_b64': None,
    }

    try:
        if not url_atual:
            inf = _inferir_url_diario(nome, estado)
            if inf.get('url'):
                url_atual = inf['url']
                resultado['url_detectada'] = url_atual
                resultado['plataforma'] = inf.get('plataforma', 'desconhecido')
                _qry("UPDATE municipios SET url_diario=%s, tipo_site=%s WHERE id=%s",
                     (url_atual, inf.get('plataforma'), municipio_id), commit=True)
                if inf.get('url_busca'):
                    resultado['perfil_json']['url_busca'] = inf['url_busca']
            else:
                resultado['status'] = 'falhou'
                resultado['erro'] = 'IA não conseguiu inferir URL'
                resultado['mensagem'] = 'URL não encontrada automaticamente.'
                _salvar_perfil_banco(municipio_id, resultado)
                return resultado

        # Teste de acesso
        acesso_ok = False
        html_ini = ''
        try:
            r = requests.get(url_atual, headers={'User-Agent': 'Mozilla/5.0 Chrome/120'}, timeout=15)
            if r.status_code == 200:
                acesso_ok = True
                html_ini = r.text
        except Exception:
            pass

        # Screenshot + IA
        shot = None
        if _playwright_disponivel():
            shot = _tirar_screenshot(url_atual)
            if shot:
                resultado['screenshot_b64'] = base64.b64encode(shot).decode()
                resultado['requer_playwright'] = True

        perfil_ia = {}
        if shot:
            perfil_ia = _analisar_screenshot_com_ia(shot, url_atual, nome)
        elif html_ini:
            perfil_ia = _extrair_perfil_do_html(html_ini, url_atual, nome)

        if perfil_ia:
            resultado['perfil_json'].update(perfil_ia)
            resultado['plataforma'] = perfil_ia.get('plataforma_identificada',
                                                      resultado['plataforma'])
            resultado['requer_playwright'] = perfil_ia.get('requer_javascript', False)
            resultado['requer_login'] = perfil_ia.get('requer_login', False)
            resultado['requer_captcha'] = perfil_ia.get('tem_captcha', False)
            resultado['perfil_json']['url_base'] = url_atual

        if resultado['requer_captcha']:
            resultado['status'] = 'captcha'
            resultado['mensagem'] = 'Site requer CAPTCHA.'
        elif resultado['requer_login']:
            resultado['status'] = 'login'
            resultado['mensagem'] = 'Site requer login.'
        elif not acesso_ok and not shot:
            resultado['status'] = 'falhou'
            resultado['mensagem'] = 'Não foi possível acessar o site.'
        else:
            resultado['sucesso'] = True
            resultado['status'] = 'ok'
            resultado['mensagem'] = f"Perfil detectado: {resultado['plataforma']}"

    except Exception as e:
        resultado['status'] = 'falhou'
        resultado['erro'] = str(e)
        resultado['mensagem'] = f"Erro: {e}"
        logger.error(f"Erro detecção {nome}: {e}\n{traceback.format_exc()}")

    _salvar_perfil_banco(municipio_id, resultado)
    logger.info(f"Detecção {nome}: {resultado['status']}")
    return resultado
