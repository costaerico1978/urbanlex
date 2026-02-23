#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
modulos/monitor.py
──────────────────
Monitor de Diários Oficiais — adaptado para o sistema UrbanLex unificado.
Usa PostgreSQL (Railway) em vez de SQLite do v1.3 original.

Ponto de entrada usado pelo app.py:
    from modulos.monitor import executar_ciclo_completo
    executar_ciclo_completo(bridge_callback=processar_alteracao_detectada)
"""

import os
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Callable

import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv('DATABASE_URL')

# ─────────────────────────────────────────────
# DB
# ─────────────────────────────────────────────

def _get_db():
    if DATABASE_URL:
        return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        database=os.getenv('DB_NAME', 'urbanismo'),
        user=os.getenv('DB_USER', 'postgres'),
        password=os.getenv('DB_PASSWORD', ''),
        cursor_factory=RealDictCursor
    )


# ─────────────────────────────────────────────
# Queries PostgreSQL (espelham o schema_final.sql)
# ─────────────────────────────────────────────

def _listar_municipios_ativos() -> List[Dict]:
    conn = _get_db()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, nome, estado, url_diario,
                   COALESCE(tipo_site, 'generico')  AS tipo_site,
                   COALESCE(config_extracao, '{}')  AS config_extracao
            FROM municipios
            WHERE ativo = TRUE AND url_diario IS NOT NULL
            ORDER BY nome
        """)
        rows = cur.fetchall()
        result = []
        for r in rows:
            d = dict(r)
            import json
            if isinstance(d['config_extracao'], str):
                try:
                    d['config_extracao'] = json.loads(d['config_extracao'])
                except Exception:
                    d['config_extracao'] = {}
            result.append(d)
        return result
    finally:
        conn.close()


def _listar_legislacoes_ativas(municipio_id: int) -> List[Dict]:
    conn = _get_db()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, municipio_id, titulo, tipo, numero,
                   data_publicacao, hash_conteudo
            FROM legislacoes
            WHERE municipio_id = %s AND processado = FALSE
            ORDER BY data_publicacao DESC
        """, (municipio_id,))
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def _registrar_execucao_inicio() -> int:
    conn = _get_db()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO scheduler_execucoes (status, iniciado_em)
            VALUES ('rodando', NOW())
            RETURNING id
        """)
        exec_id = cur.fetchone()['id']
        conn.commit()
        return exec_id
    finally:
        conn.close()


def _registrar_execucao_fim(exec_id: int, municipios: int,
                             alteracoes: int, erros: int, log: str):
    conn = _get_db()
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE scheduler_execucoes
            SET status = 'concluido', finalizado_em = NOW(),
                municipios_processados = %s,
                alteracoes_detectadas = %s,
                erros = %s, log = %s
            WHERE id = %s
        """, (municipios, alteracoes, erros, log[:5000], exec_id))
        # Atualizar ultimo_monitoramento
        cur.execute("UPDATE municipios SET ultimo_monitoramento = NOW() WHERE ativo = TRUE")
        conn.commit()
    finally:
        conn.close()


def _salvar_alteracao(municipio_id: int, municipio_nome: str,
                      alteracao: dict, data: datetime) -> int:
    """
    Salva uma alteração detectada nas tabelas legislacoes + alteracoes.
    Retorna o ID da alteração criada.
    """
    conn = _get_db()
    try:
        cur = conn.cursor()

        leg_alt = alteracao.get('legislacao_alteradora', {})
        leg_orig = alteracao.get('legislacao_original', {})

        # Buscar ou criar legislação original
        cur.execute("""
            SELECT id FROM legislacoes
            WHERE municipio_id = %s
              AND tipo = %s AND numero = %s
        """, (municipio_id,
              str(leg_orig.get('tipo', '')),
              str(leg_orig.get('numero', ''))))
        row = cur.fetchone()
        if row:
            leg_orig_id = row['id']
        else:
            cur.execute("""
                INSERT INTO legislacoes (municipio_id, titulo, tipo, numero, processado)
                VALUES (%s, %s, %s, %s, FALSE)
                RETURNING id
            """, (municipio_id,
                  f"{leg_orig.get('tipo','')} {leg_orig.get('numero','')}/{leg_orig.get('ano','')}",
                  str(leg_orig.get('tipo', '')),
                  str(leg_orig.get('numero', ''))))
            leg_orig_id = cur.fetchone()['id']

        # Criar legislação alteradora
        titulo_alt = (leg_alt.get('ementa') or
                      f"{leg_alt.get('tipo','')} {leg_alt.get('numero','')}/{leg_alt.get('ano','')}")
        cur.execute("""
            INSERT INTO legislacoes
                (municipio_id, titulo, tipo, numero, data_publicacao,
                 conteudo_texto, processado)
            VALUES (%s, %s, %s, %s, %s, %s, FALSE)
            ON CONFLICT DO NOTHING
            RETURNING id
        """, (municipio_id,
              titulo_alt[:500],
              str(leg_alt.get('tipo', '')),
              str(leg_alt.get('numero', '')),
              data.strftime('%Y-%m-%d'),
              alteracao.get('conteudo_completo', '')[:10000]))
        row2 = cur.fetchone()
        leg_alt_id = row2['id'] if row2 else leg_orig_id

        # Criar registro de alteração
        cur.execute("""
            INSERT INTO alteracoes
                (legislacao_id, tipo_alteracao, descricao, data_deteccao, aprovado)
            VALUES (%s, %s, %s, NOW(), FALSE)
            RETURNING id
        """, (leg_orig_id,
              str(alteracao.get('tipo_alteracao', 'alteração')),
              str(alteracao.get('resumo', ''))[:2000]))
        alt_id = cur.fetchone()['id']

        conn.commit()
        logger.info(f"Alteração #{alt_id} salva — {municipio_nome}")
        return alt_id

    except Exception as e:
        conn.rollback()
        logger.error(f"Erro ao salvar alteração: {e}")
        return -1
    finally:
        conn.close()


# ─────────────────────────────────────────────
# Motor principal
# ─────────────────────────────────────────────

def executar_ciclo_completo(
    data_alvo: datetime = None,
    bridge_callback: Optional[Callable] = None
) -> Dict:
    """
    Executa um ciclo completo de monitoramento para todos os municípios ativos.

    Args:
        data_alvo: Data a monitorar (default: ontem)
        bridge_callback: Função chamada para cada alteração urbanística detectada.
                         Assinatura: bridge_callback(alteracao_id, municipio_id, alteracao_dict)

    Returns:
        dict com métricas do ciclo
    """
    if data_alvo is None:
        data_alvo = datetime.now() - timedelta(days=1)

    exec_id = _registrar_execucao_inicio()
    logs = []
    total_municipios = 0
    total_alteracoes = 0
    total_erros = 0

    logger.info(f"{'='*60}")
    logger.info(f"CICLO DE MONITORAMENTO — {data_alvo.strftime('%d/%m/%Y')}")
    logger.info(f"{'='*60}")

    municipios = _listar_municipios_ativos()
    if not municipios:
        logger.warning("Nenhum município ativo com URL configurada")
        _registrar_execucao_fim(exec_id, 0, 0, 0, "Nenhum município ativo")
        return {'municipios': 0, 'alteracoes': 0, 'erros': 0}

    # Importar módulos de scraping e análise
    try:
        from modulos.scraper import DiarioScraper
        from modulos.analisador import AnalisadorIA

        scraper = DiarioScraper()
        analisador = AnalisadorIA()
    except Exception as e:
        logger.error(f"Erro ao inicializar scraper/analisador: {e}")
        _registrar_execucao_fim(exec_id, 0, 0, 1, str(e))
        return {'municipios': 0, 'alteracoes': 0, 'erros': 1}

    for municipio in municipios:
        total_municipios += 1
        nome = municipio['nome']
        logger.info(f"\n>>> {nome} ({municipio['estado']})")

        try:
            # Baixar diário
            sucesso, conteudo, erro = scraper.baixar_diario(
                url=municipio['url_diario'],
                tipo_site=municipio.get('tipo_site', 'generico'),
                config_extracao=municipio.get('config_extracao', {}),
                data_alvo=data_alvo
            )

            if not sucesso or not conteudo:
                msg = erro or "Diário não disponível"
                logger.warning(f"    {msg}")
                logs.append(f"{nome}: {msg}")
                continue

            logger.info(f"    Diário obtido ({len(conteudo):,} chars)")

            # Legislações monitoradas
            legislacoes = _listar_legislacoes_ativas(municipio['id'])
            if not legislacoes:
                logger.info(f"    Sem legislações cadastradas — pulando análise IA")
                continue

            # Analisar com IA
            alteracoes = analisador.analisar_diario(conteudo, legislacoes)

            if not alteracoes:
                logger.info(f"    ✓ Sem alterações")
                continue

            logger.info(f"    ⚡ {len(alteracoes)} alteração(ões) detectada(s)")

            for alt in alteracoes:
                alt_id = _salvar_alteracao(
                    municipio_id=municipio['id'],
                    municipio_nome=nome,
                    alteracao=alt,
                    data=data_alvo
                )

                if alt_id > 0:
                    total_alteracoes += 1
                    logs.append(f"{nome}: {alt.get('tipo_alteracao','alteração')} detectada")

                    # Chamar bridge se disponível
                    if bridge_callback:
                        try:
                            bridge_callback(alt_id, municipio['id'], alt)
                        except Exception as e_bridge:
                            logger.error(f"Erro no bridge_callback: {e_bridge}")

        except Exception as e:
            total_erros += 1
            logger.error(f"    Erro ao processar {nome}: {e}", exc_info=True)
            logs.append(f"{nome}: ERRO — {str(e)[:100]}")

    log_str = '\n'.join(logs) if logs else 'Sem ocorrências'
    _registrar_execucao_fim(exec_id, total_municipios, total_alteracoes, total_erros, log_str)

    logger.info(f"\n{'='*60}")
    logger.info(f"CONCLUÍDO — {total_municipios} municípios | "
                f"{total_alteracoes} alterações | {total_erros} erros")
    logger.info(f"{'='*60}")

    return {
        'municipios': total_municipios,
        'alteracoes': total_alteracoes,
        'erros': total_erros
    }
