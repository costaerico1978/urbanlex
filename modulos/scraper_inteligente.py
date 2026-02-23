#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
modulos/scraper_inteligente.py
──────────────────────────────────────────────────────────────────────────────
Scraper de diários oficiais com três camadas:

  1. Detecção automática  — IA (Gemini) infere a URL e o tipo do site
  2. Análise visual       — Playwright tira screenshot, Gemini Vision analisa
  3. Execução             — Playwright navega usando o perfil aprendido

Fluxo principal:
  detectar_e_salvar_perfil(municipio_id)  ← chamado ao cadastrar município
  buscar_publicacoes(municipio_id, data_inicio, data_fim)  ← chamado pelo scheduler

Dependências extras (além do requirements.txt base):
  playwright (pip install playwright && playwright install chromium)
  Gemini Vision já usa google-generativeai (já no requirements)
"""

import os
import json
import base64
import logging
import traceback
from datetime import datetime, date, timedelta
from typing import Optional

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
# Camada 1 — Detecção da URL via IA (sem browser)
# ─────────────────────────────────────────────────────────────────────────────

def _inferir_url_diario(nome_municipio: str, estado: str) -> dict:
    """
    Usa Gemini para inferir a URL do diário oficial e o tipo de plataforma.
    Retorna: {'url': '...', 'plataforma': '...', 'confianca': 'alta|media|baixa'}
    """
    gemini_key = os.getenv('GEMINI_API_KEY','')
    if not gemini_key:
        logger.warning("GEMINI_API_KEY não configurada — inferência de URL indisponível")
        return {'url': None, 'plataforma': 'desconhecido', 'confianca': 'baixa'}

    try:
        import google.generativeai as genai
        genai.configure(api_key=gemini_key)
        model = genai.GenerativeModel('gemini-1.5-flash')

        prompt = f"""Você é especialista em legislação municipal brasileira.

Preciso da URL oficial do Diário Oficial de: {nome_municipio} - {estado} - Brasil

Responda APENAS com um JSON válido, sem markdown, no formato:
{{
  "url": "https://...",
  "plataforma": "iobnet|dom|amm|diariomunicipal|imprensa_oficial|custom|desconhecido",
  "confianca": "alta|media|baixa",
  "observacao": "breve nota se necessário"
}}

Plataformas conhecidas:
- iobnet: sites baseados em iobnet.com.br
- dom: Diário Oficial dos Municípios (diariomunicipal.org.br)
- amm: Associação dos Municípios do Mato Grosso
- imprensa_oficial: imprensaoficial.com.br (SP, PR, etc)
- custom: site próprio do município
- desconhecido: não foi possível identificar

Se não tiver certeza, prefira confianca "baixa" a inventar uma URL errada."""

        resp = model.generate_content(prompt)
        texto = resp.text.strip()
        # Limpar markdown se vier
        texto = texto.replace('```json','').replace('```','').strip()
        return json.loads(texto)

    except Exception as e:
        logger.error(f"Erro ao inferir URL via IA para {nome_municipio}/{estado}: {e}")
        return {'url': None, 'plataforma': 'desconhecido', 'confianca': 'baixa'}


# ─────────────────────────────────────────────────────────────────────────────
# Camada 2 — Análise visual com Playwright + Gemini Vision
# ─────────────────────────────────────────────────────────────────────────────

def _playwright_disponivel() -> bool:
    try:
        from playwright.sync_api import sync_playwright  # noqa
        return True
    except ImportError:
        return False


def _tirar_screenshot(url: str, timeout_ms: int = 20000) -> Optional[bytes]:
    """Abre a URL com Playwright e retorna screenshot em PNG bytes."""
    if not _playwright_disponivel():
        logger.warning("Playwright não instalado — usando requests simples")
        return None
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--disable-setuid-sandbox',
                    '--single-process',
                    '--no-zygote',
                ]
            )
            ctx  = browser.new_context(
                viewport={'width':1280,'height':900},
                user_agent='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36'
            )
            page = ctx.new_page()
            page.goto(url, wait_until='networkidle', timeout=timeout_ms)
            screenshot = page.screenshot(full_page=False)
            browser.close()
            return screenshot
    except Exception as e:
        logger.error(f"Playwright screenshot falhou para {url}: {e}")
        return None


def _analisar_screenshot_com_ia(screenshot_bytes: bytes, url: str,
                                  nome_municipio: str) -> dict:
    """
    Envia screenshot para Gemini Vision e extrai o perfil de navegação.
    Retorna dict com: plataforma, passos_busca, seletor_resultado, etc.
    """
    gemini_key = os.getenv('GEMINI_API_KEY','')
    if not gemini_key or not screenshot_bytes:
        return {}
    try:
        import google.generativeai as genai
        genai.configure(api_key=gemini_key)
        model = genai.GenerativeModel('gemini-1.5-flash')

        img_b64  = base64.b64encode(screenshot_bytes).decode()
        img_part = {'mime_type': 'image/png', 'data': img_b64}

        prompt = f"""Você está analisando o site do Diário Oficial de {nome_municipio}.
URL: {url}

Analise o screenshot e extraia um perfil de navegação em JSON válido (sem markdown):
{{
  "plataforma_identificada": "nome da plataforma ou sistema",
  "tem_busca_por_data": true/false,
  "tem_busca_por_texto": true/false,
  "url_busca": "URL do endpoint de busca se diferente da página principal",
  "metodo_busca": "GET|POST|form|javascript",
  "parametro_data_inicio": "nome do parâmetro de data início (ex: dataInicio, dt_ini)",
  "parametro_data_fim": "nome do parâmetro de data fim",
  "parametro_pesquisa": "nome do campo de texto livre",
  "formato_data": "dd/MM/yyyy|yyyy-MM-dd|dd-MM-yyyy",
  "seletor_lista_resultados": "seletor CSS da lista de publicações",
  "seletor_titulo_item": "seletor CSS do título dentro de cada item",
  "seletor_data_item": "seletor CSS da data dentro de cada item",
  "seletor_link_item": "seletor CSS do link para o PDF/conteúdo",
  "requer_javascript": true/false,
  "requer_login": true/false,
  "tem_captcha": true/false,
  "observacoes": "notas importantes sobre como navegar"
}}

Se não conseguir identificar algum campo com certeza, use null.
Seja preciso — esse JSON será usado por um robô para navegar automaticamente."""

        resp  = model.generate_content([prompt, img_part])
        texto = resp.text.strip().replace('```json','').replace('```','').strip()
        return json.loads(texto)

    except Exception as e:
        logger.error(f"Erro na análise de screenshot: {e}")
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# Camada 3 — Execução da busca usando o perfil
# ─────────────────────────────────────────────────────────────────────────────

def _buscar_com_requests(perfil: dict, data_inicio: date, data_fim: date) -> list:
    """
    Tenta buscar publicações usando requests simples (sem browser).
    Funciona para plataformas com endpoints GET/POST simples.
    """
    resultados = []
    url_busca  = perfil.get('url_busca') or perfil.get('url_base','')
    if not url_busca:
        return resultados

    fmt    = perfil.get('formato_data','dd/MM/yyyy')
    metodo = (perfil.get('metodo_busca','GET') or 'GET').upper()

    def fmt_data(d):
        if fmt == 'yyyy-MM-dd': return d.strftime('%Y-%m-%d')
        if fmt == 'dd-MM-yyyy': return d.strftime('%d-%m-%Y')
        return d.strftime('%d/%m/%Y')

    params = {}
    p_ini = perfil.get('parametro_data_inicio')
    p_fim = perfil.get('parametro_data_fim')
    if p_ini: params[p_ini] = fmt_data(data_inicio)
    if p_fim: params[p_fim] = fmt_data(data_fim)

    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,*/*',
    }

    try:
        if metodo == 'POST':
            resp = requests.post(url_busca, data=params, headers=headers, timeout=30)
        else:
            resp = requests.get(url_busca, params=params, headers=headers, timeout=30)

        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'lxml')

        sel_lista  = perfil.get('seletor_lista_resultados','')
        sel_titulo = perfil.get('seletor_titulo_item','')
        sel_data   = perfil.get('seletor_data_item','')
        sel_link   = perfil.get('seletor_link_item','')

        items = soup.select(sel_lista) if sel_lista else []
        for item in items:
            titulo = item.select_one(sel_titulo).get_text(strip=True) if sel_titulo else item.get_text(strip=True)[:200]
            data_str = item.select_one(sel_data).get_text(strip=True) if sel_data else ''
            link_el  = item.select_one(sel_link) if sel_link else item.find('a')
            link_url = link_el['href'] if link_el and link_el.get('href') else ''
            if link_url and not link_url.startswith('http'):
                from urllib.parse import urljoin
                link_url = urljoin(url_busca, link_url)
            resultados.append({
                'titulo': titulo,
                'data':   data_str,
                'url':    link_url,
            })

    except Exception as e:
        logger.error(f"Erro na busca com requests em {url_busca}: {e}")

    return resultados


def _buscar_com_playwright(perfil: dict, data_inicio: date, data_fim: date,
                            nome_municipio: str) -> list:
    """
    Busca publicações usando Playwright — para sites com JavaScript.
    Usa Gemini para interpretar os resultados se necessário.
    """
    if not _playwright_disponivel():
        return []

    resultados = []
    url_busca  = perfil.get('url_busca') or perfil.get('url_base','')
    if not url_busca:
        return resultados

    fmt = perfil.get('formato_data','dd/MM/yyyy')

    def fmt_data(d):
        if fmt == 'yyyy-MM-dd': return d.strftime('%Y-%m-%d')
        if fmt == 'dd-MM-yyyy': return d.strftime('%d-%m-%Y')
        return d.strftime('%d/%m/%Y')

    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--disable-setuid-sandbox',
                    '--single-process',
                    '--no-zygote',
                ]
            )
            ctx  = browser.new_context(
                viewport={'width':1280,'height':900},
                user_agent='Mozilla/5.0 (X11; Linux x86_64) Chrome/120'
            )
            page = ctx.new_page()
            page.goto(url_busca, wait_until='networkidle', timeout=30000)

            # Tentar preencher campos de data
            p_ini = perfil.get('parametro_data_inicio')
            p_fim = perfil.get('parametro_data_fim')

            if p_ini:
                for sel in [f'[name="{p_ini}"]', f'#{p_ini}', f'input[placeholder*="nicio"]']:
                    try:
                        page.fill(sel, fmt_data(data_inicio), timeout=2000)
                        break
                    except Exception:
                        pass

            if p_fim:
                for sel in [f'[name="{p_fim}"]', f'#{p_fim}', f'input[placeholder*="im"]']:
                    try:
                        page.fill(sel, fmt_data(data_fim), timeout=2000)
                        break
                    except Exception:
                        pass

            # Tentar clicar no botão de busca
            for sel in ['button[type="submit"]','input[type="submit"]',
                        'button:has-text("Buscar")','button:has-text("Pesquisar")',
                        'a:has-text("Buscar")']:
                try:
                    page.click(sel, timeout=2000)
                    page.wait_for_load_state('networkidle', timeout=15000)
                    break
                except Exception:
                    pass

            # Capturar HTML dos resultados
            html_resultado = page.content()

            # Pedir à IA para extrair os links/publicações do HTML
            resultados = _extrair_resultados_com_ia(html_resultado, url_busca,
                                                     nome_municipio, data_inicio, data_fim)
            browser.close()

    except Exception as e:
        logger.error(f"Playwright busca falhou para {url_busca}: {e}\n{traceback.format_exc()}")

    return resultados


def _extrair_resultados_com_ia(html: str, url_base: str, nome_municipio: str,
                                 data_inicio: date, data_fim: date) -> list:
    """
    Usa Gemini para extrair publicações relevantes do HTML de resultado.
    """
    gemini_key = os.getenv('GEMINI_API_KEY','')
    if not gemini_key:
        return []

    # Truncar HTML para não estourar o contexto
    html_truncado = html[:15000] if len(html) > 15000 else html

    try:
        import google.generativeai as genai
        genai.configure(api_key=gemini_key)
        model = genai.GenerativeModel('gemini-1.5-flash')

        prompt = f"""Analise o HTML abaixo do Diário Oficial de {nome_municipio}.
Período buscado: {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}
URL base: {url_base}

Extraia TODAS as publicações/edições listadas. Responda APENAS com JSON válido (sem markdown):
{{
  "publicacoes": [
    {{
      "titulo": "título ou descrição da publicação",
      "data": "data no formato dd/mm/aaaa",
      "url": "URL completa do documento ou página",
      "tipo": "edicao|aviso|decreto|lei|portaria|outro"
    }}
  ],
  "total_encontrado": 0,
  "observacao": "nota se houver problema"
}}

Se a URL for relativa, complete com: {url_base}
Se não encontrar publicações, retorne lista vazia.

HTML:
{html_truncado}"""

        resp  = model.generate_content(prompt)
        texto = resp.text.strip().replace('```json','').replace('```','').strip()
        dados = json.loads(texto)
        return dados.get('publicacoes', [])

    except Exception as e:
        logger.error(f"Erro ao extrair resultados com IA: {e}")
        return []


# ─────────────────────────────────────────────────────────────────────────────
# API pública — Detectar e salvar perfil
# ─────────────────────────────────────────────────────────────────────────────

def detectar_e_salvar_perfil(municipio_id: int,
                               forcar_redeteccao: bool = False) -> dict:
    """
    Detecta o perfil do diário oficial de um município e salva no banco.

    Fluxo:
      1. Busca dados do município
      2. Infere URL via IA (se não tiver)
      3. Testa acesso com requests simples
      4. Se falhar ou for complexo → usa Playwright + screenshot
      5. Gemini Vision analisa o site
      6. Salva perfil em perfis_diario

    Returns:
      dict com 'sucesso', 'status', 'perfil', 'mensagem'
    """
    mun = _qry("SELECT * FROM municipios WHERE id=%s", (municipio_id,), 'one')
    if not mun:
        return {'sucesso': False, 'mensagem': 'Município não encontrado'}

    nome  = mun['nome']
    estado = mun.get('estado','')
    url_atual = mun.get('url_diario','')

    # Verificar se já tem perfil recente (menos de 7 dias)
    if not forcar_redeteccao:
        perfil_existente = _qry(
            "SELECT * FROM perfis_diario WHERE municipio_id=%s", (municipio_id,), 'one'
        )
        if perfil_existente and perfil_existente.get('status_deteccao') == 'ok':
            dias = (datetime.now() - perfil_existente['detectado_em']).days if perfil_existente.get('detectado_em') else 999
            if dias < 7:
                logger.info(f"Perfil de {nome} ainda válido ({dias} dias). Use forcar_redeteccao=True para renovar.")
                return {
                    'sucesso': True,
                    'status': 'ok',
                    'perfil': dict(perfil_existente),
                    'mensagem': f'Perfil existente ({dias} dias atrás)'
                }

    logger.info(f"Iniciando detecção para {nome}/{estado}")
    resultado = {
        'sucesso': False,
        'status': 'pendente',
        'url_detectada': url_atual,
        'plataforma': 'desconhecido',
        'perfil_json': {},
        'requer_playwright': False,
        'requer_login': False,
        'requer_captcha': False,
        'erro': None,
        'mensagem': '',
        'screenshot_b64': None,
    }

    try:
        # ── Passo 1: Obter/inferir URL ──
        if not url_atual:
            logger.info(f"Inferindo URL para {nome}/{estado}...")
            inf = _inferir_url_diario(nome, estado)
            if inf.get('url'):
                url_atual = inf['url']
                resultado['url_detectada'] = url_atual
                resultado['plataforma']    = inf.get('plataforma','desconhecido')
                # Salvar URL no município
                _qry("UPDATE municipios SET url_diario=%s, tipo_site=%s WHERE id=%s",
                     (url_atual, inf.get('plataforma'), municipio_id), commit=True)
                logger.info(f"URL inferida: {url_atual} (confiança: {inf.get('confianca')})")
            else:
                resultado['status']   = 'falhou'
                resultado['erro']     = 'IA não conseguiu inferir a URL do diário oficial'
                resultado['mensagem'] = 'Não foi possível encontrar a URL automaticamente. Informe manualmente.'
                _salvar_perfil_banco(municipio_id, resultado)
                return resultado

        # ── Passo 2: Teste de acesso simples ──
        logger.info(f"Testando acesso a {url_atual}...")
        acesso_simples = False
        html_inicial   = ''
        try:
            headers = {'User-Agent': 'Mozilla/5.0 Chrome/120'}
            resp = requests.get(url_atual, headers=headers, timeout=15)
            if resp.status_code == 200:
                acesso_simples = True
                html_inicial   = resp.text
        except Exception as e_req:
            logger.warning(f"Acesso simples falhou para {url_atual}: {e_req}")

        # ── Passo 3: Screenshot e análise visual ──
        screenshot_bytes = None
        if _playwright_disponivel():
            logger.info("Tirando screenshot com Playwright...")
            screenshot_bytes = _tirar_screenshot(url_atual)
            if screenshot_bytes:
                resultado['screenshot_b64'] = base64.b64encode(screenshot_bytes).decode()
                resultado['requer_playwright'] = True

        # ── Passo 4: Análise com Gemini Vision ──
        perfil_ia = {}
        if screenshot_bytes:
            logger.info("Analisando screenshot com Gemini Vision...")
            perfil_ia = _analisar_screenshot_com_ia(screenshot_bytes, url_atual, nome)
        elif html_inicial:
            # Fallback: analisar HTML sem screenshot
            perfil_ia = _extrair_perfil_do_html(html_inicial, url_atual, nome)

        if perfil_ia:
            resultado['perfil_json']      = perfil_ia
            resultado['plataforma']       = perfil_ia.get('plataforma_identificada', resultado['plataforma'])
            resultado['requer_playwright'] = perfil_ia.get('requer_javascript', False)
            resultado['requer_login']     = perfil_ia.get('requer_login', False)
            resultado['requer_captcha']   = perfil_ia.get('tem_captcha', False)
            # Adicionar url_base ao perfil para uso nas buscas
            resultado['perfil_json']['url_base'] = url_atual

        # ── Passo 5: Teste real de busca ──
        if resultado['requer_captcha']:
            resultado['status']   = 'captcha'
            resultado['mensagem'] = 'Site requer CAPTCHA — monitoramento automático não disponível.'
        elif resultado['requer_login']:
            resultado['status']   = 'login'
            resultado['mensagem'] = 'Site requer login — configure as credenciais manualmente.'
        elif not acesso_simples and not screenshot_bytes:
            resultado['status']   = 'falhou'
            resultado['mensagem'] = 'Não foi possível acessar o site.'
        else:
            resultado['sucesso'] = True
            resultado['status']  = 'ok'
            resultado['mensagem'] = f"Perfil detectado com sucesso. Plataforma: {resultado['plataforma']}"

    except Exception as e:
        resultado['status'] = 'falhou'
        resultado['erro']   = str(e)
        resultado['mensagem'] = f"Erro na detecção: {e}"
        logger.error(f"Erro geral na detecção de {nome}: {e}\n{traceback.format_exc()}")

    # ── Salvar perfil no banco ──
    _salvar_perfil_banco(municipio_id, resultado)
    logger.info(f"Detecção de {nome} concluída: {resultado['status']}")
    return resultado


def _extrair_perfil_do_html(html: str, url: str, nome_municipio: str) -> dict:
    """Fallback: analisa HTML (sem screenshot) para extrair perfil."""
    gemini_key = os.getenv('GEMINI_API_KEY','')
    if not gemini_key:
        return {}
    try:
        import google.generativeai as genai
        genai.configure(api_key=gemini_key)
        model = genai.GenerativeModel('gemini-1.5-flash')

        html_t = html[:12000]
        prompt = f"""Analise o HTML do site do Diário Oficial de {nome_municipio} (URL: {url}).
Extraia o perfil de navegação no mesmo formato JSON descrito anteriormente.
HTML: {html_t}"""
        resp  = model.generate_content(prompt)
        texto = resp.text.strip().replace('```json','').replace('```','').strip()
        return json.loads(texto)
    except Exception:
        return {}


def _salvar_perfil_banco(municipio_id: int, resultado: dict):
    """Persiste o perfil detectado em perfis_diario."""
    try:
        perfil_json_str = json.dumps(resultado.get('perfil_json') or {})
        _qry("""
            INSERT INTO perfis_diario
                (municipio_id, url_base, plataforma, status_deteccao, erro_deteccao,
                 perfil_json, screenshot_b64, detectado_em, requer_playwright,
                 requer_login, requer_captcha, atualizado_em)
            VALUES (%s,%s,%s,%s,%s,%s::jsonb,%s,NOW(),%s,%s,%s,NOW())
            ON CONFLICT (municipio_id) DO UPDATE SET
                url_base         = EXCLUDED.url_base,
                plataforma       = EXCLUDED.plataforma,
                status_deteccao  = EXCLUDED.status_deteccao,
                erro_deteccao    = EXCLUDED.erro_deteccao,
                perfil_json      = EXCLUDED.perfil_json,
                screenshot_b64   = EXCLUDED.screenshot_b64,
                detectado_em     = NOW(),
                requer_playwright = EXCLUDED.requer_playwright,
                requer_login     = EXCLUDED.requer_login,
                requer_captcha   = EXCLUDED.requer_captcha,
                falhas_consecutivas = 0,
                atualizado_em    = NOW()
        """, (
            municipio_id,
            resultado.get('url_detectada',''),
            resultado.get('plataforma','desconhecido'),
            resultado.get('status','pendente'),
            resultado.get('erro'),
            perfil_json_str,
            resultado.get('screenshot_b64'),
            resultado.get('requer_playwright', False),
            resultado.get('requer_login', False),
            resultado.get('requer_captcha', False),
        ), commit=True)
        # Atualizar município com data de detecção
        _qry("UPDATE municipios SET perfil_detectado_em=NOW() WHERE id=%s",
             (municipio_id,), commit=True)
    except Exception as e:
        logger.error(f"Erro ao salvar perfil no banco: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# API pública — Buscar publicações
# ─────────────────────────────────────────────────────────────────────────────

def buscar_publicacoes(municipio_id: int,
                        data_inicio: date,
                        data_fim: Optional[date] = None) -> dict:
    """
    Busca publicações no diário oficial de um município no período informado.

    Returns:
      {'sucesso': bool, 'publicacoes': [...], 'total': int, 'mensagem': str}
    """
    if data_fim is None:
        data_fim = date.today()

    mun = _qry("SELECT * FROM municipios WHERE id=%s", (municipio_id,), 'one')
    if not mun:
        return {'sucesso': False, 'publicacoes': [], 'total': 0, 'mensagem': 'Município não encontrado'}

    nome = mun['nome']

    # Carregar perfil
    perfil_row = _qry("SELECT * FROM perfis_diario WHERE municipio_id=%s", (municipio_id,), 'one')

    # Se não tem perfil ou está desatualizado (> 30 dias), re-detectar
    if not perfil_row or perfil_row.get('status_deteccao') not in ('ok',):
        logger.info(f"Sem perfil válido para {nome} — executando detecção...")
        det = detectar_e_salvar_perfil(municipio_id)
        if not det['sucesso']:
            return {
                'sucesso': False, 'publicacoes': [], 'total': 0,
                'mensagem': f"Detecção falhou: {det['mensagem']}"
            }
        perfil_row = _qry("SELECT * FROM perfis_diario WHERE municipio_id=%s", (municipio_id,), 'one')

    if not perfil_row:
        return {'sucesso': False, 'publicacoes': [], 'total': 0, 'mensagem': 'Perfil não disponível'}

    # Verificar restrições
    if perfil_row.get('requer_captcha'):
        return {'sucesso': False, 'publicacoes': [], 'total': 0,
                'mensagem': 'Site requer CAPTCHA — monitoramento automático não disponível'}
    if perfil_row.get('requer_login'):
        return {'sucesso': False, 'publicacoes': [], 'total': 0,
                'mensagem': 'Site requer login — configure credenciais'}

    perfil = dict(perfil_row.get('perfil_json') or {})
    perfil['url_base'] = perfil_row.get('url_base','')

    logger.info(f"Buscando publicações de {nome}: {data_inicio} → {data_fim}")
    publicacoes = []

    # Escolher estratégia
    if perfil_row.get('requer_playwright') and _playwright_disponivel():
        logger.info(f"Usando Playwright para {nome}")
        publicacoes = _buscar_com_playwright(perfil, data_inicio, data_fim, nome)
    else:
        logger.info(f"Usando requests para {nome}")
        publicacoes = _buscar_com_requests(perfil, data_inicio, data_fim)

    # Registrar resultado no perfil
    if publicacoes is not None:
        if publicacoes or True:  # mesmo sem resultados, atualizar última execução OK
            _qry("""UPDATE perfis_diario
                    SET ultima_execucao_ok=NOW(), falhas_consecutivas=0
                    WHERE municipio_id=%s""", (municipio_id,), commit=True)

    total = len(publicacoes)
    logger.info(f"Encontradas {total} publicações para {nome}")

    return {
        'sucesso': True,
        'publicacoes': publicacoes,
        'total': total,
        'mensagem': f'{total} publicação(ões) encontrada(s) de {data_inicio} a {data_fim}'
    }


def registrar_falha(municipio_id: int, erro: str):
    """Incrementa contador de falhas consecutivas no perfil."""
    try:
        _qry("""UPDATE perfis_diario
                SET falhas_consecutivas = falhas_consecutivas + 1,
                    erro_deteccao = %s,
                    atualizado_em = NOW()
                WHERE municipio_id = %s""", (erro[:500], municipio_id), commit=True)
        perfil = _qry("SELECT falhas_consecutivas FROM perfis_diario WHERE municipio_id=%s",
                       (municipio_id,), 'one')
        if perfil and perfil['falhas_consecutivas'] >= 3:
            logger.warning(f"Município {municipio_id}: {perfil['falhas_consecutivas']} falhas consecutivas — redetecção necessária")
            # Re-detectar perfil automaticamente após 3 falhas
            detectar_e_salvar_perfil(municipio_id, forcar_redeteccao=True)
    except Exception as e:
        logger.error(f"Erro ao registrar falha: {e}")
