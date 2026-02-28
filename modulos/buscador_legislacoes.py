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

def busca_manual(params: dict) -> dict:
    """
    Busca legislação usando campos específicos via LLM.
    Retorna dict com 'legislacoes', 'erro' e 'logs' (passos detalhados).
    """
    logs = []  # lista de {'nivel': 'info|ok|aviso|erro', 'msg': '...'}

    logs.append({'nivel': 'info', 'msg': 'Verificando GEMINI_API_KEY...'})
    if not GEMINI_API_KEY:
        logs.append({'nivel': 'erro', 'msg': 'GEMINI_API_KEY não encontrada nas variáveis de ambiente do Railway.'})
        logs.append({'nivel': 'info', 'msg': 'Vá em Railway → Variables → adicione GEMINI_API_KEY'})
        return {'legislacoes': [], 'erro': 'GEMINI_API_KEY não configurada', 'logs': logs}

    logs.append({'nivel': 'ok', 'msg': f'GEMINI_API_KEY encontrada ({GEMINI_API_KEY[:8]}...)'})

    # Montar campos
    descricao_campos = []
    esfera = params.get('esfera', '')
    estado = params.get('estado', '')
    municipio = params.get('municipio', '')
    tipo = params.get('tipo', '')
    numero = params.get('numero', '')
    ano = params.get('ano', '')
    data_pub = params.get('data_publicacao', '')
    assunto = params.get('assunto', '')
    palavras = params.get('palavras_chave', '')

    if esfera:    descricao_campos.append(f"Esfera: {esfera}")
    if estado:    descricao_campos.append(f"Estado: {estado}")
    if municipio: descricao_campos.append(f"Município: {municipio}")
    if tipo:      descricao_campos.append(f"Tipo: {tipo}")
    if numero:    descricao_campos.append(f"Número: {numero}")
    if ano:       descricao_campos.append(f"Ano: {ano}")
    if data_pub:  descricao_campos.append(f"Data de publicação: {data_pub}")
    if assunto:   descricao_campos.append(f"Assunto: {assunto}")
    if palavras:  descricao_campos.append(f"Palavras-chave: {palavras}")

    if not descricao_campos:
        logs.append({'nivel': 'erro', 'msg': 'Nenhum campo preenchido.'})
        return {'legislacoes': [], 'erro': 'Preencha ao menos um campo.', 'logs': logs}

    logs.append({'nivel': 'info', 'msg': f'{len(descricao_campos)} campo(s) de busca: {", ".join(descricao_campos)}'})
    campos_str = '\n'.join(f"- {c}" for c in descricao_campos)

    # Importar Gemini
    logs.append({'nivel': 'info', 'msg': 'Importando google.generativeai...'})
    try:
        import google.generativeai as genai
        logs.append({'nivel': 'ok', 'msg': 'google.generativeai importado com sucesso'})
    except ImportError as e:
        logs.append({'nivel': 'erro', 'msg': f'Falha ao importar google.generativeai: {e}'})
        logs.append({'nivel': 'info', 'msg': 'Execute: pip install google-generativeai'})
        return {'legislacoes': [], 'erro': 'Biblioteca google-generativeai não instalada', 'logs': logs}

    # Configurar
    logs.append({'nivel': 'info', 'msg': 'Configurando Gemini API...'})
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel('gemini-2.0-flash')
        logs.append({'nivel': 'ok', 'msg': 'Gemini 2.0 Flash configurado'})
    except Exception as e:
        logs.append({'nivel': 'erro', 'msg': f'Erro ao configurar Gemini: {e}'})
        return {'legislacoes': [], 'erro': f'Erro ao configurar Gemini: {str(e)[:150]}', 'logs': logs}

    # Montar prompt
    prompt = f"""Você é um especialista em legislação urbanística brasileira.
Pesquise na internet e encontre legislações brasileiras que correspondam a TODOS estes critérios:

{campos_str}

INSTRUÇÕES:
- Pesquise em sites oficiais de câmaras municipais, diários oficiais, LeisMunicipais.com.br, etc.
- Retorne APENAS legislações que realmente existem e que você encontrou
- Para cada legislação, preencha TODOS os campos possíveis
- Se a legislação existir, a confiança deve ser alta (0.85+)
- Inclua a ementa completa quando encontrar
- Se não encontrar nenhuma correspondência, retorne lista vazia

Responda SOMENTE com JSON válido (sem markdown, sem ```, sem texto extra):
{{
    "legislacoes": [
        {{
            "tipo": "Lei Complementar",
            "numero": "270",
            "ano": 2024,
            "ementa": "Dispõe sobre o uso e ocupação do solo...",
            "data_publicacao": "2024-06-15",
            "assunto": "Uso e Ocupação do Solo",
            "esfera": "municipal",
            "estado": "RJ",
            "municipio": "Niterói",
            "url_fonte": "https://...",
            "confianca": 0.92
        }}
    ]
}}"""

    logs.append({'nivel': 'info', 'msg': f'Prompt montado ({len(prompt)} chars)'})

    # Tentar com Google Search grounding
    response = None
    logs.append({'nivel': 'info', 'msg': 'Tentando busca com Google Search grounding...'})
    try:
        from google.generativeai.types import Tool
        search_tool = Tool(google_search={})
        response = model.generate_content(prompt, tools=[search_tool])
        logs.append({'nivel': 'ok', 'msg': 'Google Search grounding ativado com sucesso'})
    except Exception as e:
        logs.append({'nivel': 'aviso', 'msg': f'Google Search grounding falhou: {str(e)[:150]}'})
        logs.append({'nivel': 'info', 'msg': 'Tentando sem grounding (apenas conhecimento do modelo)...'})

    # Fallback sem search
    if response is None:
        try:
            response = model.generate_content(prompt)
            logs.append({'nivel': 'ok', 'msg': 'Resposta recebida do Gemini (sem grounding)'})
        except Exception as e:
            logs.append({'nivel': 'erro', 'msg': f'Gemini generate_content falhou: {str(e)[:200]}'})
            return {'legislacoes': [], 'erro': f'Erro ao chamar Gemini: {str(e)[:200]}', 'logs': logs}

    # Processar resposta
    try:
        texto = response.text.strip()
    except Exception as e:
        logs.append({'nivel': 'erro', 'msg': f'Erro ao extrair texto da resposta: {e}'})
        # Tentar extrair de parts
        try:
            texto = ''.join(p.text for p in response.parts if hasattr(p, 'text'))
            logs.append({'nivel': 'aviso', 'msg': f'Texto extraído de parts: {len(texto)} chars'})
        except:
            return {'legislacoes': [], 'erro': 'Gemini retornou resposta sem texto', 'logs': logs}

    logs.append({'nivel': 'info', 'msg': f'Resposta recebida: {len(texto)} chars'})
    logs.append({'nivel': 'info', 'msg': f'Preview: {texto[:200]}{"..." if len(texto)>200 else ""}'})

    # Limpar markdown
    texto_limpo = re.sub(r'^```(?:json)?\s*', '', texto)
    texto_limpo = re.sub(r'\s*```$', '', texto_limpo)
    if texto_limpo != texto:
        logs.append({'nivel': 'info', 'msg': 'Removidos marcadores markdown da resposta'})
        texto = texto_limpo

    # Extrair JSON
    logs.append({'nivel': 'info', 'msg': 'Extraindo JSON da resposta...'})
    match = re.search(r'\{.*\}', texto, re.DOTALL)
    if not match:
        logs.append({'nivel': 'erro', 'msg': f'JSON não encontrado na resposta do Gemini'})
        logs.append({'nivel': 'erro', 'msg': f'Resposta completa: {texto[:500]}'})
        return {'legislacoes': [], 'erro': 'Gemini não retornou JSON válido', 'logs': logs}

    logs.append({'nivel': 'ok', 'msg': 'JSON extraído com sucesso'})

    try:
        data = json.loads(match.group())
    except json.JSONDecodeError as e:
        logs.append({'nivel': 'erro', 'msg': f'Erro ao parsear JSON: {e}'})
        logs.append({'nivel': 'erro', 'msg': f'JSON bruto: {match.group()[:300]}'})
        return {'legislacoes': [], 'erro': f'JSON inválido: {str(e)[:100]}', 'logs': logs}

    legislacoes = data.get('legislacoes', [])
    logs.append({'nivel': 'info', 'msg': f'{len(legislacoes)} legislação(ões) no JSON antes de filtrar'})

    # Filtrar por confiança
    antes = len(legislacoes)
    legislacoes = [l for l in legislacoes if l.get('confianca', 0) >= 0.5]
    if antes != len(legislacoes):
        logs.append({'nivel': 'aviso', 'msg': f'{antes - len(legislacoes)} descartada(s) por confiança < 50%'})

    logs.append({'nivel': 'ok', 'msg': f'Resultado final: {len(legislacoes)} legislação(ões)'})

    # Registrar na fila
    try:
        _registrar_busca_fila(
            tipo='busca_manual',
            municipio=municipio or '—', estado=estado or '—',
            legislacoes_encontradas=len(legislacoes),
            status='concluido',
            detalhes={'params': params, 'resultados': len(legislacoes)},
        )
        logs.append({'nivel': 'ok', 'msg': 'Busca registrada no histórico'})
    except Exception as e:
        logs.append({'nivel': 'aviso', 'msg': f'Falha ao registrar no histórico: {e}'})

    return {'legislacoes': legislacoes, 'logs': logs}


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
