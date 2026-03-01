#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
modulos/descobridor_legislacoes.py
───────────────────────────────────
Descobre legislações urbanísticas de um município automaticamente.

Busca: Plano Diretor, Lei de Uso e Ocupação do Solo, Código de Obras,
       Lei de Parcelamento do Solo, Lei de Zoneamento, etc.
"""

import os
import json
import logging
import re
import requests
from datetime import date, datetime
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

GEMINI_API_KEY = os.getenv('GEMINI_API_KEY', '')

# Tipos de legislação urbanística que procuramos
LEGISLACOES_URBANISTICAS = [
    {
        'tipo': 'Plano Diretor',
        'termos_busca': ['plano diretor', 'plano diretor municipal'],
        'tipo_legislacao': 'Lei Complementar',
    },
    {
        'tipo': 'Lei de Uso e Ocupação do Solo',
        'termos_busca': ['uso e ocupação do solo', 'uso ocupação solo', 'LUOS',
                          'lei de zoneamento', 'zoneamento urbano'],
        'tipo_legislacao': 'Lei Complementar',
    },
    {
        'tipo': 'Código de Obras',
        'termos_busca': ['código de obras', 'codigo de obras',
                          'código de edificações'],
        'tipo_legislacao': 'Lei Complementar',
    },
    {
        'tipo': 'Lei de Parcelamento do Solo',
        'termos_busca': ['parcelamento do solo', 'parcelamento solo urbano',
                          'loteamento'],
        'tipo_legislacao': 'Lei',
    },
    {
        'tipo': 'Código de Posturas',
        'termos_busca': ['código de posturas', 'posturas municipais'],
        'tipo_legislacao': 'Lei',
    },
]


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


# ─────────────────────────────────────────────────────────────────────────────
# Função principal: descobrir legislações urbanísticas
# ─────────────────────────────────────────────────────────────────────────────

def descobrir_legislacoes_municipio(municipio: str, estado: str,
                                     municipio_id: int = None) -> List[dict]:
    """
    Descobre as legislações urbanísticas vigentes de um município.

    Returns:
        Lista de legislações encontradas, cada uma com:
        {tipo_nome, numero, ano, ementa, data_publicacao, url_original, confianca}
    """
    logger.info(f"=== Descobrindo legislações de {municipio}/{estado} ===")

    # Verificar se já temos legislações cadastradas para este município
    existentes = _legislacoes_existentes(municipio, estado, municipio_id)
    if existentes:
        logger.info(f"  Já existem {len(existentes)} legislações cadastradas")
        # Verificar se temos os tipos essenciais
        tipos_existentes = {e.get('assunto_nome', '').lower() for e in existentes}
        tem_plano_diretor = any('plano diretor' in t for t in tipos_existentes)
        tem_luos = any('uso' in t and 'ocupação' in t for t in tipos_existentes)
        if tem_plano_diretor and tem_luos:
            logger.info("  Já tem Plano Diretor e LUOS — pulando descoberta")
            return []

    # Usar LLM para descobrir as legislações
    legislacoes = _descobrir_via_llm(municipio, estado)

    if not legislacoes:
        logger.warning(f"  Nenhuma legislação descoberta para {municipio}/{estado}")

    return legislacoes


def _legislacoes_existentes(municipio, estado, municipio_id):
    """Busca legislações já cadastradas para o município."""
    try:
        if municipio_id:
            return _qry("""SELECT id, tipo_nome, numero, ano, assunto_nome
                          FROM legislacoes
                          WHERE municipio_id=%s AND esfera='municipal'""",
                       (municipio_id,))
        return _qry("""SELECT id, tipo_nome, numero, ano, assunto_nome
                      FROM legislacoes
                      WHERE LOWER(municipio_nome)=LOWER(%s) AND estado=%s
                        AND esfera='municipal'""",
                   (municipio, estado))
    except Exception:
        return []


def _descobrir_via_llm(municipio: str, estado: str) -> List[dict]:
    """Usa Gemini para descobrir legislações urbanísticas do município."""
    if not GEMINI_API_KEY:
        logger.warning("GEMINI_API_KEY não disponível para descoberta")
        return []

    try:
        import google.generativeai as genai
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel('gemini-2.5-flash')

        prompt = f"""Identifique as principais legislações urbanísticas VIGENTES do município
de {municipio}, estado {estado}, Brasil.

Procure especificamente:
1. Plano Diretor Municipal
2. Lei de Uso e Ocupação do Solo (LUOS) / Lei de Zoneamento
3. Código de Obras / Lei de Edificações
4. Lei de Parcelamento do Solo
5. Código de Posturas (se existir)

Para cada legislação encontrada, informe:
- tipo: tipo da legislação (Lei Complementar, Lei Ordinária, Decreto, etc.)
- numero: número da lei
- ano: ano de publicação
- ementa: ementa ou resumo
- data_publicacao: data de publicação se souber (formato YYYY-MM-DD)
- assunto: classificação (Plano Diretor, Uso e Ocupação do Solo, etc.)
- confianca: de 0.0 a 1.0 sobre a precisão da informação

IMPORTANTE:
- Retorne APENAS legislações que você tem certeza que existem
- Prefira legislações mais recentes (vigentes)
- Se não souber o número exato, informe confiança baixa
- Não invente legislações

Responda APENAS com JSON:
{{
    "municipio": "{municipio}",
    "estado": "{estado}",
    "legislacoes": [
        {{
            "tipo": "Lei Complementar",
            "numero": "270",
            "ano": 2024,
            "ementa": "Dispõe sobre o uso e ocupação do solo...",
            "data_publicacao": "2024-06-15",
            "assunto": "Uso e Ocupação do Solo",
            "confianca": 0.85
        }}
    ]
}}"""

        response = model.generate_content(prompt)
        texto = response.text.strip()

        match = re.search(r'\{.*\}', texto, re.DOTALL)
        if not match:
            logger.warning("LLM não retornou JSON válido")
            return []

        data = json.loads(match.group())
        legislacoes = data.get('legislacoes', [])

        # Filtrar por confiança mínima
        legislacoes = [l for l in legislacoes
                       if l.get('confianca', 0) >= 0.6 and l.get('numero')]

        logger.info(f"  LLM encontrou {len(legislacoes)} legislações com confiança >= 0.6")
        return legislacoes

    except Exception as e:
        logger.error(f"Erro na descoberta via LLM: {e}")
        return []


# ─────────────────────────────────────────────────────────────────────────────
# Cadastrar legislações descobertas na biblioteca
# ─────────────────────────────────────────────────────────────────────────────

def cadastrar_legislacoes_descobertas(legislacoes: List[dict],
                                       municipio: str, estado: str,
                                       municipio_id: int = None,
                                       ativar_monitoramento: bool = True) -> List[int]:
    """
    Cadastra legislações descobertas na biblioteca do UrbanLex.

    Returns:
        Lista de IDs das legislações criadas.
    """
    ids_criados = []

    for leg in legislacoes:
        try:
            numero = str(leg.get('numero', ''))
            ano = leg.get('ano')
            tipo_nome = leg.get('tipo', 'Lei')

            # Verificar se já existe
            existente = _qry("""SELECT id FROM legislacoes
                               WHERE LOWER(municipio_nome)=LOWER(%s) AND estado=%s
                                 AND numero=%s AND ano=%s""",
                            (municipio, estado, numero, ano), 'one')

            if existente:
                logger.info(f"  {tipo_nome} {numero}/{ano} já existe (id={existente['id']})")
                ids_criados.append(existente['id'])
                continue

            # Buscar ou criar tipo_id
            tipo_id = _obter_tipo_id(tipo_nome)
            assunto_id = _obter_assunto_id(leg.get('assunto', ''))

            data_pub = leg.get('data_publicacao')
            ementa = leg.get('ementa', '')

            # Inserir legislação
            row = _qry("""
                INSERT INTO legislacoes (
                    pais, esfera, estado, municipio_nome, municipio_id,
                    tipo_id, tipo_nome, numero, ano, data_publicacao,
                    ementa, assunto_id, assunto_nome, status,
                    em_monitoramento, data_inicio_monitoramento,
                    origem, pendente_aprovacao, texto_integral, url_texto_fonte,
                    criado_em
                ) VALUES (
                    'BR', 'municipal', %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, 'vigente',
                    %s, %s,
                    'agente_autonomo', FALSE, %s, %s,
                    NOW()
                ) RETURNING id
            """, (estado, municipio, municipio_id,
                  tipo_id, tipo_nome, numero, ano, data_pub,
                  ementa, assunto_id, leg.get('assunto', ''),
                  ativar_monitoramento,
                  data_pub if ativar_monitoramento else None,
                  leg.get('texto_integral'),
                  leg.get('url_texto_fonte')),
                'one', commit=True)

            if row:
                leg_id = row['id']
                ids_criados.append(leg_id)
                logger.info(f"  ✓ Cadastrada: {tipo_nome} {numero}/{ano} (id={leg_id})"
                           f"{' + monitoramento' if ativar_monitoramento else ''}")

                # Registrar no feed de atividades
                _registrar_atividade(
                    tipo='legislacao_descoberta',
                    mensagem=f'{tipo_nome} {numero}/{ano} de {municipio}/{estado} '
                             f'descoberta e cadastrada automaticamente',
                    detalhes={
                        'legislacao_id': leg_id,
                        'municipio': municipio,
                        'estado': estado,
                        'confianca': leg.get('confianca', 0),
                    }
                )

        except Exception as e:
            logger.error(f"Erro ao cadastrar {leg}: {e}")

    return ids_criados


def _obter_tipo_id(tipo_nome: str) -> int:
    """Busca ou cria tipo de legislação."""
    try:
        row = _qry("SELECT id FROM tipos_legislacao WHERE LOWER(nome)=LOWER(%s)",
                   (tipo_nome,), 'one')
        if row:
            return row['id']
        row = _qry("INSERT INTO tipos_legislacao (nome) VALUES (%s) RETURNING id",
                   (tipo_nome,), 'one', commit=True)
        return row['id'] if row else None
    except Exception:
        return None


def _obter_assunto_id(assunto_nome: str) -> int:
    """Busca ou cria assunto de legislação."""
    if not assunto_nome:
        return None
    try:
        row = _qry("SELECT id FROM assuntos_legislacao WHERE LOWER(nome)=LOWER(%s)",
                   (assunto_nome,), 'one')
        if row:
            return row['id']
        row = _qry("INSERT INTO assuntos_legislacao (nome) VALUES (%s) RETURNING id",
                   (assunto_nome,), 'one', commit=True)
        return row['id'] if row else None
    except Exception:
        return None


def _registrar_atividade(tipo: str, mensagem: str, detalhes: dict = None):
    """Registra no feed de atividades."""
    try:
        _qry("""INSERT INTO feed_atividades (tipo, mensagem, detalhes, criado_em)
                VALUES (%s, %s, %s, NOW())""",
             (tipo, mensagem, json.dumps(detalhes or {})),
             commit=True, fetch=None)
    except Exception:
        pass  # Feed é secundário, não deve impedir fluxo principal
