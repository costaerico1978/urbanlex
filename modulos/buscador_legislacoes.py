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


def _chamar_llm(prompt: str, logs: list, label: str = 'LLM') -> Optional[str]:
    """Chama Gemini ou GROQ e retorna texto. Reutilizável para múltiplas etapas."""
    import requests as req
    texto = None

    if GEMINI_API_KEY:
        try:
            import google.generativeai as genai
            genai.configure(api_key=GEMINI_API_KEY)
            model = genai.GenerativeModel('gemini-2.0-flash')
            response = model.generate_content(prompt)
            texto = response.text.strip()
            logs.append({'nivel': 'ok', 'msg': f'{label}: Gemini respondeu ({len(texto)} chars)'})
            return texto
        except Exception as e:
            logs.append({'nivel': 'aviso', 'msg': f'{label}: Gemini falhou: {str(e)[:120]}'})

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
            else:
                logs.append({'nivel': 'erro', 'msg': f'{label}: GROQ HTTP {resp.status_code}'})
        except Exception as e:
            logs.append({'nivel': 'erro', 'msg': f'{label}: GROQ falhou: {str(e)[:120]}'})

    return None


def _llm_triar_snippets(todos_snippets: list, descricao_busca: str, logs: list) -> dict:
    """
    ETAPA INTELIGENTE: LLM analisa os snippets do DuckDuckGo e:
    1. Seleciona quais URLs vale a pena acessar
    2. Extrai a data de publicação da legislação (se encontrar)
    Retorna {'urls': [...], 'data_publicacao': 'YYYY-MM-DD' ou ''}
    """
    if not todos_snippets:
        return {'urls': [], 'data_publicacao': ''}

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

FAÇA DUAS COISAS:

1) SELECIONE URLs relevantes:
- OBRIGATÓRIO: O snippet ou título DEVE mencionar a EXPRESSÃO COMPLETA da legislação (ex: "Lei Complementar 198" ou "LC 198"), NÃO apenas o número solto.
- PRIORIZE: PDFs, câmaras municipais, sites legislativos
- REJEITE: Homepages genéricas, páginas "Quem Somos", outro tipo de ato com mesmo número
- REJEITE: Diários oficiais ESTADUAIS se a busca é MUNICIPAL

2) EXTRAIA A DATA DE PUBLICAÇÃO da legislação, se algum snippet mencionar. Procure datas no formato "de 14 de janeiro de 2019", "14/01/2019", "publicada em 2019-01-14", etc.

Responda SOMENTE com JSON:
{{
    "urls_selecionadas": [1, 3, 7],
    "data_publicacao": "2019-01-14",
    "justificativa_breve": "Resultado 1 menciona LC 198 com data 14/01/2019..."
}}

- urls_selecionadas: números entre colchetes, 1-5 itens. Lista VAZIA se nenhum é relevante.
- data_publicacao: formato YYYY-MM-DD. Vazio "" se não encontrou data em nenhum snippet."""

    logs.append({'nivel': 'info', 'msg': f'🧠 Triagem IA: analisando {len(todos_snippets)} snippets...'})
    texto = _chamar_llm(prompt, logs, '🧠 Triagem')

    resultado = {'urls': [], 'data_publicacao': ''}

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


def _acessar_pagina(url: str, termos: str, headers: dict, logs: list, label: str, tipo_lei: str = '', numero_lei: str = '') -> Optional[dict]:
    """Acessa uma URL, detecta se é HTML ou PDF, extrai texto e calcula relevância."""
    import requests as req
    # Garantir que URL tem protocolo
    if url and not url.startswith('http'):
        url = 'https://' + url
    try:
        resp = req.get(url, headers=headers, timeout=12, allow_redirects=True, stream=True)
        if resp.status_code != 200:
            logs.append({'nivel': 'aviso', 'msg': f'{label}: HTTP {resp.status_code} em {url[:60]}'})
            return None

        content_type = resp.headers.get('Content-Type', '').lower()
        texto = ''

        # ── PDF ──
        if 'pdf' in content_type or url.lower().endswith('.pdf') or '/download/' in url.lower():
            logs.append({'nivel': 'info', 'msg': f'{label}: Detectado PDF, baixando e extraindo texto...'})
            try:
                pdf_bytes = resp.content
                if len(pdf_bytes) > 10_000_000:  # max 10MB
                    logs.append({'nivel': 'aviso', 'msg': f'{label}: PDF muito grande ({len(pdf_bytes)//1024}KB), pulando'})
                    return None
                import fitz  # PyMuPDF
                doc = fitz.open(stream=pdf_bytes, filetype='pdf')
                pages_text = []
                for page in doc[:20]:  # max 20 páginas
                    pages_text.append(page.get_text())
                doc.close()
                texto = ' '.join(pages_text)
                texto = re.sub(r'\s+', ' ', texto).strip()
                logs.append({'nivel': 'ok', 'msg': f'{label}: PDF extraído: {len(texto)} chars de {len(pages_text)} págs'})
            except Exception as e:
                logs.append({'nivel': 'aviso', 'msg': f'{label}: Falha ao ler PDF: {str(e)[:80]}'})
                return None
        else:
            # ── HTML ──
            texto = _extrair_texto_html(resp.text)

        if len(texto) < 80:
            logs.append({'nivel': 'aviso', 'msg': f'{label}: pouco conteúdo ({len(texto)} chars)'})
            return None

        # Relevância
        lista_termos = [t.strip().lower() for t in termos.split() if len(t.strip()) > 2]
        matches = sum(1 for t in lista_termos if t in texto.lower())
        relevancia = matches / max(len(lista_termos), 1)

        # FILTRO DURO: busca TIPO + NÚMERO combinados (não número sozinho)
        if numero_lei and numero_lei.strip() and tipo_lei and tipo_lei.strip():
            texto_lower = texto.lower()
            num = numero_lei.strip()
            tip = tipo_lei.strip().lower()

            # Gerar abreviações do tipo
            abreviacoes = {
                'lei complementar': ['lei complementar', 'lc'],
                'lei ordinária': ['lei ordinária', 'lei ordinaria', 'lei'],
                'lei': ['lei'],
                'decreto': ['decreto', 'dec'],
                'decreto-lei': ['decreto-lei', 'decreto lei', 'dl'],
                'resolução': ['resolução', 'resolucao', 'res'],
                'portaria': ['portaria', 'port'],
                'emenda': ['emenda'],
            }
            # Pegar abreviações para o tipo informado
            tipos_buscar = [tip]
            for chave, abrevs in abreviacoes.items():
                if chave in tip or tip in chave:
                    tipos_buscar = abrevs
                    break

            # Gerar padrões: "lei complementar 198", "lc 198", "lc nº 198", "lc n° 198", "lc n. 198"
            padroes = []
            for t in tipos_buscar:
                padroes.append(f'{t} {num}')
                padroes.append(f'{t} nº {num}')
                padroes.append(f'{t} n° {num}')
                padroes.append(f'{t} n. {num}')
                padroes.append(f'{t} no {num}')
                padroes.append(f'{t} n {num}')

            encontrou_padrao = any(p in texto_lower for p in padroes)
            if not encontrou_padrao:
                logs.append({'nivel': 'aviso', 'msg': f'{label}: nenhum padrão "{tipos_buscar[0]} {num}" encontrado — DESCARTADO'})
                return None
            else:
                # Encontrou! Boost de relevância
                padrao_encontrado = next(p for p in padroes if p in texto_lower)
                logs.append({'nivel': 'ok', 'msg': f'{label}: ✓ encontrou "{padrao_encontrado}" no texto'})
                relevancia = max(relevancia, 0.6)  # mínimo 60% se achou o padrão

        elif numero_lei and numero_lei.strip():
            # Sem tipo: pelo menos o número deve aparecer
            if numero_lei.strip() not in texto:
                logs.append({'nivel': 'aviso', 'msg': f'{label}: número "{numero_lei}" NÃO encontrado — DESCARTADO'})
                return None

        logs.append({'nivel': 'ok' if relevancia > 0.3 else 'info',
                     'msg': f'{label}: {len(texto)} chars, {matches}/{len(lista_termos)} termos ({relevancia:.0%} relevância)'})
        return {'url': url, 'texto': texto[:6000], 'nome': label, 'relevancia': relevancia}
    except Exception as e:
        logs.append({'nivel': 'aviso', 'msg': f'{label}: {str(e)[:80]}'})
        return None


def busca_manual(params: dict) -> dict:
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

    logs = []
    fontes_status = []  # para exibir no frontend

    # ── Extrair parâmetros ──
    esfera = params.get('esfera', '')
    estado = params.get('estado', '')
    municipio = params.get('municipio', '')
    tipo = params.get('tipo', '')
    numero = params.get('numero', '')
    ano = params.get('ano', '')
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
            ddg_results = _pesquisar_web(f'site:{dominio} {query_curta}', logs, f'🔗 Site #{i+1}')
            for d in ddg_results:
                d['fonte_tipo'] = f'🔗 Site #{i+1} ({dominio})'
                d['_fonte'] = 'site_informado'
                todos_snippets.append(d)
            fontes_status.append({'nome': f'🔗 Site #{i+1}', 'url': url_extra, 'encontrou': bool(ddg_results)})

    # ── 1C: LeisMunicipais.com.br ──
    if municipio:
        logs.append({'nivel': 'info', 'msg': '📖 Buscando em LeisMunicipais.com.br...'})
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
    if todos_snippets:
        triagem = _llm_triar_snippets(todos_snippets, descricao_busca, logs)
        urls_selecionadas = triagem['urls']
        if not data_descoberta and triagem['data_publicacao']:
            data_descoberta = triagem['data_publicacao']
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

        result = _acessar_pagina(url, termos_busca, headers_http, logs, fonte_nome, tipo_lei=tipo, numero_lei=numero)
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
                        result = _acessar_pagina(ddg['url'], termos_busca, headers_http, logs, f'🏛️ DO ({data_fmt1})', tipo_lei=tipo, numero_lei=numero)
                        if result:
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
            # Sem data: busca genérica no DO (como antes)
            logs.append({'nivel': 'info', 'msg': f'🏛️ ETAPA 4: Sem data — busca genérica no DO ({dominio_diario})...'})
            ddg_results = _pesquisar_web(f'site:{dominio_diario} {query_curta}', logs, '🏛️ DO')
            encontrou_do = False
            for ddg in ddg_results[:3]:
                result = _acessar_pagina(ddg['url'], termos_busca, headers_http, logs, '🏛️ DO', tipo_lei=tipo, numero_lei=numero)
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

    # Montar textos das fontes
    fontes_texto = '\n\n'.join([
        f"=== FONTE {i+1}: {t['nome']} ({t['url'][:80]}) ===\nRelevância: {t['relevancia']:.0%}\n{t['texto'][:3000]}"
        for i, t in enumerate(textos_extraidos[:6])
    ]) if textos_extraidos else '(nenhuma fonte encontrada — use apenas seu conhecimento, mas AVISE que os dados podem conter erros)'

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

    # Chamar LLM (usa helper reutilizável)
    texto = _chamar_llm(prompt, logs, '🤖 Análise Final')

    if not texto:
        return {'legislacoes': [], 'erro': 'Nenhum modelo de IA respondeu.', 'logs': logs, 'fontes': fontes_status}

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

    # Preparar textos das fontes para exibição no frontend
    textos_fontes = []
    for t in textos_extraidos:
        textos_fontes.append({
            'nome': t.get('nome', ''),
            'url': t.get('url', ''),
            'relevancia': round(t.get('relevancia', 0), 2),
            'texto': t.get('texto', '')[:5000],  # limite para não pesar no JSON
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
