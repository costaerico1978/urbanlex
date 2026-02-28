#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
modulos/integrador_plataforma.py
─────────────────────────────────
Integração com plataforma externa de terrenos.
Consulta API diariamente, detecta municípios novos e dispara descoberta.
"""

import os
import json
import logging
import requests
from datetime import datetime, date
from typing import List, Dict, Tuple

logger = logging.getLogger(__name__)

# Configuração da API externa
PLATAFORMA_API_URL = os.getenv('PLATAFORMA_API_URL', '')  # ex: https://app.exemplo.com/api/municipios
PLATAFORMA_API_KEY = os.getenv('PLATAFORMA_API_KEY', '')


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


def _registrar_atividade(tipo: str, mensagem: str, detalhes: dict = None):
    """Registra no feed de atividades."""
    try:
        _qry("""INSERT INTO feed_atividades (tipo, mensagem, detalhes, criado_em)
                VALUES (%s, %s, %s, NOW())""",
             (tipo, mensagem, json.dumps(detalhes or {})),
             commit=True, fetch=None)
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# Consulta à plataforma externa
# ─────────────────────────────────────────────────────────────────────────────

def consultar_municipios_plataforma() -> List[dict]:
    """
    Consulta API da plataforma externa para obter lista de municípios.

    Formato esperado:
    [
        {"municipio": "Rio de Janeiro", "estado": "RJ"},
        {"municipio": "São Paulo", "estado": "SP"}
    ]
    """
    if not PLATAFORMA_API_URL:
        logger.info("PLATAFORMA_API_URL não configurada — integração desabilitada")
        return []

    headers = {'Accept': 'application/json'}
    if PLATAFORMA_API_KEY:
        headers['Authorization'] = f'Bearer {PLATAFORMA_API_KEY}'

    try:
        resp = requests.get(PLATAFORMA_API_URL, headers=headers, timeout=30)
        if resp.status_code != 200:
            logger.error(f"API plataforma retornou HTTP {resp.status_code}")
            _registrar_atividade(
                'erro_integracao',
                f'API plataforma retornou HTTP {resp.status_code}',
                {'url': PLATAFORMA_API_URL, 'status': resp.status_code}
            )
            return []

        dados = resp.json()

        # Aceitar formato lista direta ou {data: [...]}
        if isinstance(dados, dict):
            dados = dados.get('data') or dados.get('municipios') or dados.get('items') or []

        if not isinstance(dados, list):
            logger.error("Resposta da API não é uma lista")
            return []

        # Normalizar e validar
        municipios = []
        for item in dados:
            mun = item.get('municipio', '') or item.get('nome', '') or item.get('city', '')
            est = item.get('estado', '') or item.get('uf', '') or item.get('state', '')
            if mun and est and len(est) == 2:
                municipios.append({
                    'municipio': mun.strip(),
                    'estado': est.strip().upper()
                })

        logger.info(f"Plataforma retornou {len(municipios)} municípios")
        return municipios

    except requests.exceptions.Timeout:
        logger.error("API plataforma: timeout")
        return []
    except Exception as e:
        logger.error(f"Erro ao consultar plataforma: {e}")
        return []


# ─────────────────────────────────────────────────────────────────────────────
# Detecção de municípios novos
# ─────────────────────────────────────────────────────────────────────────────

def detectar_municipios_novos(municipios_plataforma: List[dict]) -> List[dict]:
    """
    Compara lista da plataforma com municípios já monitorados.

    Returns:
        Lista de municípios que são NOVOS (não monitorados ainda).
    """
    if not municipios_plataforma:
        return []

    # Buscar municípios que já têm legislações monitoradas
    try:
        monitorados = _qry("""
            SELECT DISTINCT LOWER(municipio_nome) as mun, estado
            FROM legislacoes
            WHERE esfera='municipal' AND em_monitoramento=TRUE
              AND municipio_nome IS NOT NULL AND estado IS NOT NULL
        """)
    except Exception:
        monitorados = []

    monitorados_set = {(m['mun'], m['estado']) for m in (monitorados or [])}

    novos = []
    for m in municipios_plataforma:
        chave = (m['municipio'].lower(), m['estado'])
        if chave not in monitorados_set:
            novos.append(m)

    if novos:
        nomes = [n['municipio'] + '/' + n['estado'] for n in novos[:5]]
        extra = '...' if len(novos) > 5 else ''
        logger.info(f"Detectados {len(novos)} municípios novos: {', '.join(nomes)}{extra}")
    else:
        logger.info("Nenhum município novo detectado")

    return novos


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline completo: consultar → detectar → descobrir → cadastrar
# ─────────────────────────────────────────────────────────────────────────────

def executar_integracao_plataforma() -> dict:
    """
    Pipeline completo de integração diária com a plataforma.

    1. Consulta API da plataforma (lista de municípios)
    2. Detecta municípios novos
    3. Para cada novo: descobre legislações + cadastra + ativa monitoramento

    Returns:
        {municipios_consultados, novos_detectados, legislacoes_cadastradas, erros}
    """
    resultado = {
        'municipios_consultados': 0,
        'novos_detectados': 0,
        'legislacoes_cadastradas': 0,
        'municipios_processados': [],
        'erros': [],
    }

    # 1. Consultar plataforma
    municipios = consultar_municipios_plataforma()
    resultado['municipios_consultados'] = len(municipios)

    if not municipios:
        return resultado

    # 2. Detectar novos
    novos = detectar_municipios_novos(municipios)
    resultado['novos_detectados'] = len(novos)

    if not novos:
        return resultado

    # 3. Para cada município novo: descobrir e cadastrar
    from modulos.descobridor_diario import descobrir_diario
    from modulos.descobridor_legislacoes import (
        descobrir_legislacoes_municipio, cadastrar_legislacoes_descobertas
    )

    for mun_info in novos:
        municipio = mun_info['municipio']
        estado = mun_info['estado']

        try:
            logger.info(f"\n── Processando novo município: {municipio}/{estado} ──")

            # Garantir que município existe no banco
            mun_id = _garantir_municipio(municipio, estado)

            # Descobrir diário oficial
            diario = descobrir_diario(municipio, estado, 'municipal', mun_id)
            if diario.get('url'):
                logger.info(f"  Diário: {diario['url']} ({diario['origem']})")

            # Descobrir legislações urbanísticas
            legislacoes = descobrir_legislacoes_municipio(municipio, estado, mun_id)

            if legislacoes:
                # Cadastrar e ativar monitoramento
                ids = cadastrar_legislacoes_descobertas(
                    legislacoes, municipio, estado, mun_id,
                    ativar_monitoramento=True
                )
                resultado['legislacoes_cadastradas'] += len(ids)

                resultado['municipios_processados'].append({
                    'municipio': municipio,
                    'estado': estado,
                    'legislacoes': len(ids),
                    'diario_url': diario.get('url', ''),
                })

                _registrar_atividade(
                    'municipio_novo',
                    f'Novo município detectado: {municipio}/{estado}. '
                    f'{len(ids)} legislação(ões) cadastrada(s) e monitorada(s).',
                    {
                        'municipio': municipio, 'estado': estado,
                        'municipio_id': mun_id,
                        'legislacoes_ids': ids,
                        'diario_url': diario.get('url', ''),
                    }
                )
            else:
                resultado['municipios_processados'].append({
                    'municipio': municipio,
                    'estado': estado,
                    'legislacoes': 0,
                    'mensagem': 'Nenhuma legislação urbanística encontrada',
                })
                _registrar_atividade(
                    'municipio_novo_sem_legislacao',
                    f'Novo município detectado: {municipio}/{estado}, '
                    f'mas nenhuma legislação urbanística encontrada automaticamente.',
                    {'municipio': municipio, 'estado': estado}
                )

        except Exception as e:
            logger.error(f"Erro ao processar {municipio}/{estado}: {e}")
            resultado['erros'].append({
                'municipio': municipio,
                'estado': estado,
                'erro': str(e)[:200],
            })

    logger.info(f"\n=== Integração concluída: {resultado['novos_detectados']} novos, "
                f"{resultado['legislacoes_cadastradas']} legislações cadastradas ===")

    return resultado


def _garantir_municipio(municipio: str, estado: str) -> int:
    """Garante que o município existe na tabela municipios, retorna o ID."""
    try:
        row = _qry("""SELECT id FROM municipios
                      WHERE LOWER(nome)=LOWER(%s) AND estado=%s""",
                  (municipio, estado), 'one')
        if row:
            return row['id']

        row = _qry("""INSERT INTO municipios (nome, estado, pais, criado_em)
                      VALUES (%s, %s, 'BR', NOW()) RETURNING id""",
                  (municipio, estado), 'one', commit=True)
        return row['id'] if row else None
    except Exception as e:
        logger.error(f"Erro ao garantir município: {e}")
        return None
