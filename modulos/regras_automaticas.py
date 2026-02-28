#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
modulos/regras_automaticas.py
──────────────────────────────
Regras automáticas do sistema de monitoramento:

1. Legislação REVOGADA → para monitoramento automaticamente
2. Nova lei ALTERA monitorada → cadastra na biblioteca + inicia monitoramento
3. Nova lei REGULAMENTA monitorada → cadastra + vincula na árvore
"""

import os
import json
import logging
from datetime import date, datetime
from typing import Optional, Dict, List

logger = logging.getLogger(__name__)


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
             (tipo, mensagem, json.dumps(detalhes or {})),
             commit=True, fetch=None)
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# REGRA 1: Legislação revogada → parar monitoramento
# ─────────────────────────────────────────────────────────────────────────────

def processar_revogacao(legislacao_id: int, revogada_por: dict = None) -> dict:
    """
    Processa a revogação de uma legislação monitorada.

    Args:
        legislacao_id: ID da legislação que foi revogada
        revogada_por: {tipo_nome, numero, ano, data_publicacao} da lei que revogou

    Returns:
        {sucesso, mensagem, monitoramento_desativado}
    """
    resultado = {'sucesso': False, 'mensagem': '', 'monitoramento_desativado': False}

    try:
        leg = _qry("SELECT * FROM legislacoes WHERE id=%s", (legislacao_id,), 'one')
        if not leg:
            resultado['mensagem'] = 'Legislação não encontrada'
            return resultado

        titulo = f"{leg.get('tipo_nome', '')} {leg.get('numero', '')}/{leg.get('ano', '')}"

        # Desativar monitoramento
        _qry("""UPDATE legislacoes
                SET em_monitoramento = FALSE,
                    status = 'revogada',
                    observacoes = COALESCE(observacoes, '') || %s,
                    atualizado_em = NOW()
                WHERE id = %s""",
             (f"\n[AUTO] Revogada em {date.today()}. Monitoramento desativado.",
              legislacao_id),
             commit=True, fetch=None)

        resultado['sucesso'] = True
        resultado['monitoramento_desativado'] = True

        # Registrar info sobre quem revogou
        revogador_desc = ''
        if revogada_por:
            revogador_desc = (f" pela {revogada_por.get('tipo_nome', 'Lei')} "
                             f"{revogada_por.get('numero', '?')}/{revogada_por.get('ano', '?')}")

        resultado['mensagem'] = (f'{titulo} revogada{revogador_desc}. '
                                  f'Monitoramento desativado automaticamente.')

        logger.info(f"  🚫 {resultado['mensagem']}")

        _registrar_atividade(
            'revogacao_detectada',
            resultado['mensagem'],
            {
                'legislacao_id': legislacao_id,
                'revogada_por': revogada_por,
            }
        )

        return resultado

    except Exception as e:
        resultado['mensagem'] = f'Erro: {str(e)[:200]}'
        logger.error(f"Erro ao processar revogação: {e}")
        return resultado


# ─────────────────────────────────────────────────────────────────────────────
# REGRA 2: Nova lei altera monitorada → cadastrar + monitorar
# ─────────────────────────────────────────────────────────────────────────────

def processar_nova_legislacao(legislacao_monitorada_id: int,
                               nova_lei: dict,
                               tipo_relacao: str = 'altera') -> dict:
    """
    Processa uma nova legislação que altera/regulamenta uma legislação monitorada.

    Args:
        legislacao_monitorada_id: ID da legislação que está sendo monitorada
        nova_lei: {tipo_nome, numero, ano, ementa, data_publicacao, ...}
        tipo_relacao: 'altera', 'regulamenta', 'complementa', 'revoga'

    Returns:
        {sucesso, legislacao_id, mensagem, monitoramento_ativado}
    """
    resultado = {
        'sucesso': False, 'legislacao_id': None,
        'mensagem': '', 'monitoramento_ativado': False
    }

    try:
        # Buscar legislação monitorada
        leg_mon = _qry("SELECT * FROM legislacoes WHERE id=%s",
                       (legislacao_monitorada_id,), 'one')
        if not leg_mon:
            resultado['mensagem'] = 'Legislação monitorada não encontrada'
            return resultado

        # Dados da nova lei
        tipo_nome = nova_lei.get('tipo_nome', nova_lei.get('tipo', 'Lei'))
        numero = str(nova_lei.get('numero', ''))
        ano = nova_lei.get('ano')
        ementa = nova_lei.get('ementa', '')
        data_pub = nova_lei.get('data_publicacao')

        if not numero:
            resultado['mensagem'] = 'Número da nova legislação ausente'
            return resultado

        titulo_nova = f"{tipo_nome} {numero}/{ano}" if ano else f"{tipo_nome} {numero}"

        # Verificar se já existe no banco
        existente = _qry("""SELECT id FROM legislacoes
                           WHERE numero=%s AND ano=%s
                             AND LOWER(municipio_nome)=LOWER(%s)
                             AND estado=%s""",
                        (numero, ano, leg_mon['municipio_nome'], leg_mon['estado']),
                        'one')

        if existente:
            nova_id = existente['id']
            logger.info(f"  {titulo_nova} já existe (id={nova_id})")
        else:
            # ── CADASTRAR NA BIBLIOTECA ──
            tipo_id = _obter_tipo_id(tipo_nome)
            assunto_id = leg_mon.get('assunto_id')
            assunto_nome = leg_mon.get('assunto_nome', '')

            row = _qry("""
                INSERT INTO legislacoes (
                    pais, esfera, estado, municipio_nome, municipio_id,
                    tipo_id, tipo_nome, numero, ano, data_publicacao,
                    ementa, assunto_id, assunto_nome, status,
                    em_monitoramento, data_inicio_monitoramento,
                    origem, pendente_aprovacao, criado_em
                ) VALUES (
                    'BR', %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, 'vigente',
                    TRUE, %s,
                    'agente_autonomo', FALSE, NOW()
                ) RETURNING id
            """, (leg_mon['esfera'], leg_mon['estado'],
                  leg_mon['municipio_nome'], leg_mon.get('municipio_id'),
                  tipo_id, tipo_nome, numero, ano, data_pub,
                  ementa, assunto_id, assunto_nome,
                  data_pub),  # data_publicacao = data_inicio_monitoramento
                'one', commit=True)

            if not row:
                resultado['mensagem'] = 'Falha ao inserir legislação'
                return resultado

            nova_id = row['id']
            resultado['monitoramento_ativado'] = True
            logger.info(f"  ✓ Cadastrada: {titulo_nova} (id={nova_id}) + monitoramento")

        # ── CRIAR RELAÇÃO NA ÁRVORE GENEALÓGICA ──
        _criar_relacao(legislacao_monitorada_id, nova_id, tipo_relacao)

        # Se for revogação, processar regra 1
        if tipo_relacao == 'revoga':
            processar_revogacao(legislacao_monitorada_id,
                               revogada_por=nova_lei)

        resultado['sucesso'] = True
        resultado['legislacao_id'] = nova_id

        titulo_mon = (f"{leg_mon.get('tipo_nome', '')} "
                      f"{leg_mon.get('numero', '')}/{leg_mon.get('ano', '')}")

        resultado['mensagem'] = (
            f'{titulo_nova} detectada — {tipo_relacao} {titulo_mon}. '
            f'Cadastrada automaticamente na biblioteca'
            f'{" com monitoramento ativo" if resultado["monitoramento_ativado"] else ""}.'
        )

        logger.info(f"  📜 {resultado['mensagem']}")

        _registrar_atividade(
            'nova_legislacao_detectada',
            resultado['mensagem'],
            {
                'legislacao_monitorada_id': legislacao_monitorada_id,
                'nova_legislacao_id': nova_id,
                'tipo_relacao': tipo_relacao,
                'nova_lei': nova_lei,
            }
        )

        return resultado

    except Exception as e:
        resultado['mensagem'] = f'Erro: {str(e)[:200]}'
        logger.error(f"Erro ao processar nova legislação: {e}")
        return resultado


def _criar_relacao(pai_id: int, filha_id: int, tipo_relacao: str):
    """Cria relação na árvore genealógica se não existir."""
    try:
        existente = _qry("""SELECT id FROM legislacao_relacoes
                           WHERE legislacao_pai_id=%s AND legislacao_filha_id=%s
                             AND tipo_relacao=%s""",
                        (pai_id, filha_id, tipo_relacao), 'one')
        if not existente:
            _qry("""INSERT INTO legislacao_relacoes
                    (legislacao_pai_id, legislacao_filha_id, tipo_relacao, criado_em)
                    VALUES (%s, %s, %s, NOW())""",
                 (pai_id, filha_id, tipo_relacao),
                 commit=True, fetch=None)
            logger.info(f"  🔗 Relação criada: {pai_id} ←[{tipo_relacao}]← {filha_id}")
    except Exception as e:
        logger.warning(f"Erro ao criar relação: {e}")


def _obter_tipo_id(tipo_nome: str) -> int:
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


# ─────────────────────────────────────────────────────────────────────────────
# Processar resultados da análise de IA (chamado pelo scheduler)
# ─────────────────────────────────────────────────────────────────────────────

def aplicar_regras_analise(legislacao_monitorada_id: int,
                            analise: dict) -> List[dict]:
    """
    Aplica regras automáticas com base no resultado da análise de IA.

    Args:
        legislacao_monitorada_id: ID da legislação monitorada
        analise: resultado da análise com {tipo_alteracao, legislacao_relacionada, ...}

    Returns:
        Lista de ações executadas [{tipo, mensagem, ...}]
    """
    acoes = []
    tipo = analise.get('tipo_alteracao', '').lower()
    leg_rel = analise.get('legislacao_relacionada', {})

    if tipo in ('revogacao', 'revogação', 'revoga'):
        r = processar_revogacao(legislacao_monitorada_id, revogada_por=leg_rel)
        acoes.append({'tipo': 'revogacao', **r})

    elif tipo in ('alteracao', 'alteração', 'altera', 'modificacao',
                  'alteracao_parcial', 'alteração_parcial'):
        if leg_rel and leg_rel.get('numero'):
            r = processar_nova_legislacao(legislacao_monitorada_id,
                                          leg_rel, 'altera')
            acoes.append({'tipo': 'nova_legislacao', **r})

    elif tipo in ('regulamentacao', 'regulamentação', 'regulamenta'):
        if leg_rel and leg_rel.get('numero'):
            r = processar_nova_legislacao(legislacao_monitorada_id,
                                          leg_rel, 'regulamenta')
            acoes.append({'tipo': 'nova_legislacao', **r})

    elif tipo in ('complementa', 'complementação'):
        if leg_rel and leg_rel.get('numero'):
            r = processar_nova_legislacao(legislacao_monitorada_id,
                                          leg_rel, 'complementa')
            acoes.append({'tipo': 'nova_legislacao', **r})

    return acoes
