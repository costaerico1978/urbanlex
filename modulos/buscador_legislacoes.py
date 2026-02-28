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


def _buscar_em_url(url: str, query_termos: str, headers: dict, logs: list, nome_fonte: str) -> Optional[dict]:
    """Busca legislação em uma URL específica. Retorna dict com texto ou None."""
    import requests as req
    try:
        resp = req.get(url, headers=headers, timeout=10, allow_redirects=True)
        if resp.status_code != 200:
            logs.append({'nivel': 'aviso', 'msg': f'{nome_fonte}: HTTP {resp.status_code}'})
            return None
        texto = _extrair_texto_html(resp.text)
        if len(texto) < 50:
            logs.append({'nivel': 'aviso', 'msg': f'{nome_fonte}: página com pouco conteúdo ({len(texto)} chars)'})
            return None
        # Verificar se contém termos relevantes
        termos = [t.strip().lower() for t in query_termos.split() if len(t.strip()) > 2]
        matches = sum(1 for t in termos if t in texto.lower())
        relevancia = matches / max(len(termos), 1)
        logs.append({'nivel': 'ok' if relevancia > 0.3 else 'info',
                     'msg': f'{nome_fonte}: {len(texto)} chars, {matches}/{len(termos)} termos encontrados'})
        return {'url': url, 'texto': texto[:4000], 'nome': nome_fonte, 'relevancia': relevancia}
    except Exception as e:
        logs.append({'nivel': 'aviso', 'msg': f'{nome_fonte}: {str(e)[:100]}'})
        return None


def busca_manual(params: dict) -> dict:
    """
    Busca legislação com pesquisa REAL na internet, em ordem de prioridade:
    1º Diário Oficial do município
    2º Sites informados pelo usuário
    3º LeisMunicipais.com.br
    4º Google
    Depois o LLM compara as fontes, sugere qual adotar e justifica.
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

    query_google = ' '.join(filter(None, [tipo, f'nº {numero}' if numero else '', ano, municipio, estado])) + ' legislação'
    logs.append({'nivel': 'info', 'msg': f'Termos de busca: "{termos_busca}"'})

    headers_http = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
        'Accept-Language': 'pt-BR,pt;q=0.9',
    }
    textos_extraidos = []

    # ══════════════════════════════════════════════════════════════════
    # ETAPA 1: DIÁRIO OFICIAL DO MUNICÍPIO (fonte primária)
    # ══════════════════════════════════════════════════════════════════
    if url_diario:
        logs.append({'nivel': 'info', 'msg': f'🏛️ ETAPA 1: Buscando no Diário Oficial ({url_diario})...'})
        # Tentar busca no site do diário
        diario_query_url = url_diario.rstrip('/') + f'/busca?q={req.utils.quote(termos_busca)}'
        result = _buscar_em_url(url_diario, termos_busca, headers_http, logs, '🏛️ Diário Oficial (raiz)')
        if result:
            result['_fonte'] = 'diario_oficial'
            textos_extraidos.append(result)
            fontes_status.append({'nome': '🏛️ Diário Oficial', 'url': url_diario, 'encontrou': True})
        else:
            # Tentar com busca
            result2 = _buscar_em_url(diario_query_url, termos_busca, headers_http, logs, '🏛️ Diário Oficial (busca)')
            if result2:
                result2['_fonte'] = 'diario_oficial'
                textos_extraidos.append(result2)
                fontes_status.append({'nome': '🏛️ Diário Oficial', 'url': diario_query_url, 'encontrou': True})
            else:
                fontes_status.append({'nome': '🏛️ Diário Oficial', 'url': url_diario, 'encontrou': False})
    else:
        logs.append({'nivel': 'info', 'msg': '🏛️ ETAPA 1: URL do Diário Oficial não informada (pulando)'})

    # ══════════════════════════════════════════════════════════════════
    # ETAPA 2: SITES INFORMADOS PELO USUÁRIO
    # ══════════════════════════════════════════════════════════════════
    if urls_extras:
        logs.append({'nivel': 'info', 'msg': f'🔗 ETAPA 2: Buscando em {len(urls_extras)} site(s) informado(s)...'})
        for i, url_extra in enumerate(urls_extras[:5]):
            nome = f'🔗 Site #{i+1}'
            result = _buscar_em_url(url_extra, termos_busca, headers_http, logs, nome)
            if result:
                result['_fonte'] = 'site_informado'
                textos_extraidos.append(result)
                fontes_status.append({'nome': nome, 'url': url_extra, 'encontrou': True})
            else:
                fontes_status.append({'nome': nome, 'url': url_extra, 'encontrou': False})
    else:
        logs.append({'nivel': 'info', 'msg': '🔗 ETAPA 2: Nenhum site adicional informado (pulando)'})

    # ══════════════════════════════════════════════════════════════════
    # ETAPA 3: LEISMUNICIPAIS.COM.BR
    # ══════════════════════════════════════════════════════════════════
    if municipio:
        logs.append({'nivel': 'info', 'msg': '📖 ETAPA 3: Buscando em LeisMunicipais.com.br...'})
        mun_slug = municipio.lower().replace(' ', '-').replace('ã', 'a').replace('é', 'e').replace('í', 'i').replace('ó', 'o').replace('ú', 'u').replace('ç', 'c')
        lm_query = ' '.join(filter(None, [tipo, numero, ano]))
        lm_url = f'https://leismunicipais.com.br/pesquisa/{mun_slug}?q={req.utils.quote(lm_query)}'
        result = _buscar_em_url(lm_url, termos_busca, headers_http, logs, '📖 LeisMunicipais')
        if result:
            result['_fonte'] = 'leismunicipais'
            textos_extraidos.append(result)
            fontes_status.append({'nome': '📖 LeisMunicipais', 'url': lm_url, 'encontrou': True})
        else:
            fontes_status.append({'nome': '📖 LeisMunicipais', 'url': lm_url, 'encontrou': False})

    # ══════════════════════════════════════════════════════════════════
    # ETAPA 4: GOOGLE (backup amplo)
    # ══════════════════════════════════════════════════════════════════
    logs.append({'nivel': 'info', 'msg': f'🔎 ETAPA 4: Buscando no Google: "{query_google}"...'})
    try:
        google_url = f'https://www.google.com/search?q={req.utils.quote(query_google)}&hl=pt-BR&num=8'
        resp_g = req.get(google_url, headers=headers_http, timeout=10)
        if resp_g.status_code == 200:
            urls_google = re.findall(r'href="/url\?q=(https?://[^&"]+)', resp_g.text)
            urls_google = [u for u in urls_google if not any(x in u for x in ['google.com','youtube.com','facebook.com','instagram.com','twitter.com'])][:6]
            logs.append({'nivel': 'ok', 'msg': f'Google: {len(urls_google)} links encontrados'})

            for url_g in urls_google[:3]:
                # Evitar duplicatas
                if any(t['url'] == url_g for t in textos_extraidos):
                    continue
                result = _buscar_em_url(url_g, termos_busca, headers_http, logs, f'🔎 Google')
                if result and result['relevancia'] > 0.2:
                    result['_fonte'] = 'google'
                    textos_extraidos.append(result)
                    fontes_status.append({'nome': '🔎 Google', 'url': url_g, 'encontrou': True})
                    break  # Pegar só a mais relevante do Google
            else:
                fontes_status.append({'nome': '🔎 Google', 'url': '', 'encontrou': False})
        else:
            logs.append({'nivel': 'aviso', 'msg': f'Google: HTTP {resp_g.status_code}'})
            fontes_status.append({'nome': '🔎 Google', 'url': '', 'encontrou': False})
    except Exception as e:
        logs.append({'nivel': 'aviso', 'msg': f'Google: {str(e)[:100]}'})
        fontes_status.append({'nome': '🔎 Google', 'url': '', 'encontrou': False})

    # ══════════════════════════════════════════════════════════════════
    # ETAPA 5: LLM COMPARA, SUGERE E JUSTIFICA
    # ══════════════════════════════════════════════════════════════════
    total_fontes = len(textos_extraidos)
    logs.append({'nivel': 'info', 'msg': f'🤖 ETAPA 5: Enviando {total_fontes} fonte(s) para análise da IA...'})

    if total_fontes == 0:
        logs.append({'nivel': 'aviso', 'msg': 'Nenhuma fonte encontrada na internet. LLM usará apenas conhecimento próprio (pode conter erros).'})

    # Montar textos das fontes
    fontes_texto = '\n\n'.join([
        f"=== FONTE {i+1}: {t['nome']} ({t['url'][:80]}) ===\nRelevância: {t['relevancia']:.0%}\n{t['texto'][:3000]}"
        for i, t in enumerate(textos_extraidos[:5])
    ]) if textos_extraidos else '(nenhuma fonte encontrada — use apenas seu conhecimento, mas AVISE que os dados podem conter erros)'

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

    prompt = f"""Analise os textos extraídos de fontes reais da internet e identifique a legislação buscada.

CRITÉRIOS DE BUSCA:
{descricao_busca}

TEXTOS DAS FONTES (em ordem de prioridade):
{fontes_texto}

INSTRUÇÕES:
1. Identifique a legislação nos textos. Use APENAS informações presentes nas fontes.
2. Se múltiplas fontes mencionam a mesma legislação, COMPARE os dados (ementa, data, assunto).
3. Indique qual fonte é mais confiável e por quê.
4. A ementa deve ser EXATAMENTE como aparece na fonte mais confiável.
5. NÃO invente dados. Se um campo não aparece nas fontes, deixe vazio ("").
6. Inclua justificativa explicando por que esta é a legislação correta.

Responda SOMENTE com JSON:
{{
    "sugestao": "Texto explicando qual fonte é a mais confiável, se houve divergências entre fontes, e por que a IA recomenda cadastrar esta legislação.",
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
            "justificativa": "Encontrada no Diário Oficial com ementa confirmada em LeisMunicipais..."
        }}
    ]
}}"""

    logs.append({'nivel': 'info', 'msg': f'Prompt montado: {len(prompt)} chars com {total_fontes} fonte(s)'})

    # Chamar LLM
    texto = None

    if GEMINI_API_KEY:
        logs.append({'nivel': 'info', 'msg': 'Chamando Gemini 2.0 Flash...'})
        try:
            import google.generativeai as genai
            genai.configure(api_key=GEMINI_API_KEY)
            model = genai.GenerativeModel('gemini-2.0-flash')
            response = model.generate_content(prompt)
            texto = response.text.strip()
            logs.append({'nivel': 'ok', 'msg': f'Gemini respondeu: {len(texto)} chars'})
        except Exception as e:
            logs.append({'nivel': 'aviso', 'msg': f'Gemini falhou: {str(e)[:150]}'})

    if texto is None and GROQ_API_KEY:
        logs.append({'nivel': 'info', 'msg': '🔄 Tentando GROQ...'})
        try:
            resp = req.post(
                'https://api.groq.com/openai/v1/chat/completions',
                headers={'Authorization': f'Bearer {GROQ_API_KEY}', 'Content-Type': 'application/json'},
                json={
                    'model': 'llama-3.3-70b-versatile',
                    'messages': [
                        {'role': 'system', 'content': 'Analise textos de fontes reais e extraia informações de legislações brasileiras. Responda APENAS com JSON válido.'},
                        {'role': 'user', 'content': prompt}
                    ],
                    'temperature': 0.1, 'max_tokens': 4000,
                },
                timeout=30,
            )
            if resp.status_code == 200:
                texto = resp.json()['choices'][0]['message']['content'].strip()
                logs.append({'nivel': 'ok', 'msg': f'GROQ respondeu: {len(texto)} chars'})
            else:
                logs.append({'nivel': 'erro', 'msg': f'GROQ HTTP {resp.status_code}: {resp.text[:200]}'})
        except Exception as e:
            logs.append({'nivel': 'erro', 'msg': f'GROQ falhou: {str(e)[:200]}'})

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

    return {'legislacoes': legislacoes, 'sugestao': sugestao, 'fontes': fontes_status, 'logs': logs}


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
