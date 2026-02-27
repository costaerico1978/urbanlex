#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
modulos/scheduler_integrado.py
────────────────────────────────
SCHEDULER v4.0 — Monitoramento por Legislação

Fluxo:
  1. Busca legislações com em_monitoramento=TRUE
  2. Agrupa por município
  3. Para cada município, busca publicações do diário oficial no período
  4. Analisa com IA se alguma publicação altera as legislações monitoradas
  5. Se encontrar: salva alerta, cria relação na árvore, notifica por email
  6. Registra log detalhado por legislação
"""

import os
import json
import time
import logging
import traceback
from datetime import datetime, date, timedelta
from typing import Optional, Dict, List, Any

import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Conexão DB
# ─────────────────────────────────────────────────────────────────────────────

def get_db():
    url = os.getenv('DATABASE_URL')
    if url:
        return psycopg2.connect(url, cursor_factory=RealDictCursor)
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=int(os.getenv('DB_PORT', 5432)),
        dbname=os.getenv('DB_NAME', 'urbanlex'),
        user=os.getenv('DB_USER', 'postgres'),
        password=os.getenv('DB_PASSWORD', ''),
        cursor_factory=RealDictCursor
    )

def _parse_data_pub(data_str):
    if not data_str:
        return None
    for fmt in ('%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y', '%d.%m.%Y'):
        try:
            return datetime.strptime(data_str.strip(), fmt).date()
        except (ValueError, AttributeError):
            continue
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Ciclo principal — monitoramento por legislação
# ─────────────────────────────────────────────────────────────────────────────

def executar_ciclo_completo(bridge_callback=None, disparado_por=None, municipio_id=None):
    """
    Executa ciclo de monitoramento por legislação.
    Se municipio_id for informado, processa apenas as legislações daquele município.
    """
    filtro_mun = f" (município {municipio_id})" if municipio_id else ""
    logger.info(f"=== Ciclo de monitoramento v4.0{filtro_mun} iniciado ===")
    inicio = datetime.now()

    conn = get_db()
    cur  = conn.cursor()

    # Ler configurações
    cur.execute("SELECT debug_ativo, email_relatorio FROM scheduler_config ORDER BY id LIMIT 1")
    cfg_row = cur.fetchone()
    debug_ativo     = cfg_row['debug_ativo'] if cfg_row and cfg_row.get('debug_ativo') is not None else True
    email_relatorio = cfg_row['email_relatorio'] if cfg_row and cfg_row.get('email_relatorio') is not None else True

    # Registrar início
    cur.execute("""
        INSERT INTO scheduler_execucoes (status, disparado_por)
        VALUES ('rodando', %s) RETURNING id
    """, (disparado_por,))
    execucao_id = cur.fetchone()['id']
    conn.commit()

    # Contadores globais
    municipios_processados = 0
    municipios_ok          = 0
    municipios_erro        = 0
    alteracoes_detectadas  = 0
    erros_count            = 0
    log_municipios         = []
    total_legislacoes      = 0

    # Importar dependências
    try:
        from modulos.scraper_inteligente import buscar_publicacoes, registrar_falha
        SCRAPER_OK = True
        logger.info("Scraper inteligente: disponível")
    except ImportError:
        SCRAPER_OK = False
        logger.warning("Scraper inteligente não disponível")

    try:
        from modulos.analisador import AnalisadorDiario
        analisador = AnalisadorDiario()
        ANALISADOR_OK = True
        logger.info("Analisador IA: disponível")
    except Exception as e:
        ANALISADOR_OK = False
        analisador = None
        logger.warning(f"Analisador IA não disponível: {e}")

    try:
        # ══════════════════════════════════════════════════════════════════
        # PASSO 1: Buscar legislações monitoradas, agrupadas por município
        # ══════════════════════════════════════════════════════════════════
        cur.execute("""
            SELECT l.id, l.tipo_nome, l.numero, l.ano, l.ementa,
                   l.municipio_id, l.municipio_nome, l.estado,
                   l.data_inicio_monitoramento, l.data_fim_monitoramento,
                   l.ultima_verificacao_monitoramento,
                   COALESCE(m.url_diario, pd.url_base) as url_diario,
                   m.nome as mun_nome
            FROM legislacoes l
            LEFT JOIN municipios m ON m.id = l.municipio_id
            LEFT JOIN perfis_diario pd ON pd.municipio_id = l.municipio_id
            WHERE l.em_monitoramento = TRUE
              AND l.pendente_aprovacao = FALSE
              AND (%s IS NULL OR l.municipio_id = %s)
            ORDER BY l.municipio_nome, l.ano
        """, (municipio_id, municipio_id))
        legislacoes_monitoradas = cur.fetchall() or []
        total_legislacoes = len(legislacoes_monitoradas)

        if not legislacoes_monitoradas:
            _finalizar_execucao(cur, conn, execucao_id, 'concluido', inicio,
                                0, 0, 0, 0, 0, "Nenhuma legislação em monitoramento.", "")
            if email_relatorio:
                _enviar_email_resumo(execucao_id, 0, 0, 0, 0, 0, inicio,
                                     "Nenhuma legislação em monitoramento.", [])
            return

        logger.info(f"Encontradas {total_legislacoes} legislações monitoradas")

        # Agrupar por município
        municipios_dict = {}
        for leg in legislacoes_monitoradas:
            mun_key = leg['municipio_id'] or leg['municipio_nome']
            if mun_key not in municipios_dict:
                municipios_dict[mun_key] = {
                    'id': leg['municipio_id'],
                    'nome': leg['mun_nome'] or leg['municipio_nome'],
                    'estado': leg['estado'],
                    'url_diario': leg['url_diario'],
                    'legislacoes': []
                }
            municipios_dict[mun_key]['legislacoes'].append(leg)

        logger.info(f"Agrupadas em {len(municipios_dict)} município(s)")

        # ══════════════════════════════════════════════════════════════════
        # PASSO 2: Processar cada município
        # ══════════════════════════════════════════════════════════════════
        alertas_enviar_email = []

        for mun_key, mun_info in municipios_dict.items():
            nome_mun    = mun_info['nome']
            mun_id      = mun_info['id']
            url_diario  = mun_info['url_diario']
            legs        = mun_info['legislacoes']
            municipios_processados += 1
            erros_mun   = []
            alt_mun     = 0
            mun_inicio  = datetime.now()

            try:
                logger.info(f"═══ Processando: {nome_mun} ({len(legs)} legislações) ═══")

                if not SCRAPER_OK:
                    msg = "Scraper não disponível"
                    erros_mun.append(msg)
                    logger.warning(f"  ⚠ {nome_mun}: {msg}")
                    for leg in legs:
                        _registrar_log_legislacao(cur, conn, execucao_id, leg['id'],
                            mun_id, mun_inicio, 'erro', False, 0, 0, 0, 0, 'nenhum',
                            url_diario, msg, msg)
                    municipios_erro += 1
                    erros_count += 1
                    log_municipios.append({'nome': nome_mun, 'status': 'erro',
                                           'legislacoes': len(legs), 'alteracoes': 0,
                                           'pubs_encontradas': 0, 'dias_processados': 0,
                                           'erros': erros_mun})
                    conn.commit()
                    continue

                # ── Determinar range de datas ──
                datas_inicio = []
                datas_fim    = []
                for leg in legs:
                    # Início: de onde parou ou data_inicio_monitoramento
                    if leg.get('ultima_verificacao_monitoramento'):
                        d = leg['ultima_verificacao_monitoramento']
                        if isinstance(d, datetime): d = d.date()
                        datas_inicio.append(d)
                    elif leg.get('data_inicio_monitoramento'):
                        d = leg['data_inicio_monitoramento']
                        if isinstance(d, datetime): d = d.date()
                        datas_inicio.append(d)
                    else:
                        datas_inicio.append(date.today() - timedelta(days=7))

                    # Fim: data_fim_monitoramento ou hoje
                    if leg.get('data_fim_monitoramento'):
                        d = leg['data_fim_monitoramento']
                        if isinstance(d, datetime): d = d.date()
                        datas_fim.append(d)
                    else:
                        datas_fim.append(date.today())

                data_inicio_busca = min(datas_inicio)
                data_fim_busca    = max(datas_fim)

                if data_inicio_busca >= data_fim_busca:
                    logger.info(f"  ✓ {nome_mun}: já verificado até hoje")
                    for leg in legs:
                        _registrar_log_legislacao(cur, conn, execucao_id, leg['id'],
                            mun_id, mun_inicio, 'ok', True, 0, 0, 0, 0, 'scraper',
                            url_diario, 'Já verificado até hoje — nada a processar', None)
                    municipios_ok += 1
                    log_municipios.append({'nome': nome_mun, 'status': 'ok',
                                           'legislacoes': len(legs), 'alteracoes': 0,
                                           'pubs_encontradas': 0, 'dias_processados': 0,
                                           'erros': []})
                    conn.commit()
                    continue

                dias_a_processar = (data_fim_busca - data_inicio_busca).days
                logger.info(f"  Período: {data_inicio_busca} → {data_fim_busca} ({dias_a_processar} dias)")

                # ── Formatar legislações para o analisador ──
                legs_para_ia = [{'id': l['id'], 'tipo': l['tipo_nome'] or 'Lei',
                                 'numero': l['numero'] or '', 'ano': l['ano'] or 0,
                                 'ementa': l['ementa'] or ''} for l in legs]

                # ── Processar em blocos de 30 dias ──
                bloco_inicio = data_inicio_busca
                BLOCO_DIAS   = 30

                pubs_total_mun       = 0
                pubs_analisadas_mun  = 0
                alertas_mun          = 0

                while bloco_inicio < data_fim_busca:
                    bloco_fim = min(bloco_inicio + timedelta(days=BLOCO_DIAS), data_fim_busca)
                    logger.info(f"  Bloco: {bloco_inicio} → {bloco_fim}")

                    try:
                        res = buscar_publicacoes(mun_id, bloco_inicio, bloco_fim)

                        if res['sucesso']:
                            pubs = res.get('publicacoes', [])
                            pubs_total_mun += len(pubs)
                            logger.info(f"    Encontradas {len(pubs)} publicações")

                            if pubs and ANALISADOR_OK:
                                texto_diario = _montar_texto_publicacoes(pubs)

                                if texto_diario.strip():
                                    pubs_analisadas_mun += len(pubs)
                                    try:
                                        alteracoes_ia = analisador.analisar_diario(
                                            texto_diario, legs_para_ia)
                                    except Exception as e_ia:
                                        logger.error(f"    Erro IA: {e_ia}")
                                        alteracoes_ia = []
                                        erros_mun.append(f"Erro IA bloco {bloco_inicio}: {e_ia}")

                                    for alt_ia in (alteracoes_ia or []):
                                        try:
                                            resultado = _processar_alteracao_encontrada(
                                                cur, conn, alt_ia, legs, mun_id, nome_mun,
                                                mun_info['estado'], execucao_id)
                                            if resultado.get('inserido'):
                                                alertas_mun += 1
                                                alteracoes_detectadas += 1
                                                alt_mun += 1
                                                alertas_enviar_email.append({
                                                    'municipio': nome_mun,
                                                    'legislacao_original': resultado.get('leg_original_titulo',''),
                                                    'legislacao_alteradora': resultado.get('leg_alteradora_titulo',''),
                                                    'tipo_alteracao': alt_ia.get('tipo_alteracao',''),
                                                    'resumo': alt_ia.get('resumo',''),
                                                    'data': alt_ia.get('data_publicacao',''),
                                                })
                                        except Exception as e_proc:
                                            erros_count += 1
                                            erros_mun.append(f"Proc alteração: {e_proc}")
                                            logger.error(f"    Erro proc: {e_proc}")
                        else:
                            msg = res.get('mensagem', 'Falha no scraper')
                            erros_mun.append(f"Bloco {bloco_inicio}: {msg}")
                            logger.warning(f"    Scraper falhou: {msg}")

                    except Exception as e_bloco:
                        erros_mun.append(f"Bloco {bloco_inicio}: {e_bloco}")
                        logger.error(f"    Erro bloco: {e_bloco}")

                    bloco_inicio = bloco_fim
                    time.sleep(1)

                # ── Atualizar última verificação de cada legislação ──
                for leg in legs:
                    try:
                        # Respeitar data_fim_monitoramento de cada legislação
                        verif_ate = data_fim_busca
                        if leg.get('data_fim_monitoramento'):
                            d = leg['data_fim_monitoramento']
                            if isinstance(d, datetime): d = d.date()
                            verif_ate = min(verif_ate, d)
                        cur.execute("""UPDATE legislacoes
                            SET ultima_verificacao_monitoramento = %s WHERE id = %s
                        """, (verif_ate, leg['id']))
                    except Exception:
                        pass
                conn.commit()

                # ── Registrar log por legislação ──
                mun_status = 'ok' if not erros_mun else 'parcial'
                msg = _montar_mensagem_legislacao(pubs_total_mun, pubs_analisadas_mun,
                    alertas_mun, dias_a_processar, erros_mun)
                for leg in legs:
                    _registrar_log_legislacao(cur, conn, execucao_id, leg['id'],
                        mun_id, mun_inicio, mun_status, mun_status != 'erro',
                        pubs_total_mun, pubs_analisadas_mun, alertas_mun, 0,
                        'scraper', url_diario, msg,
                        '\n'.join(erros_mun) if erros_mun else None)
                conn.commit()

                municipios_ok += 1
                log_municipios.append({
                    'nome': nome_mun, 'status': mun_status, 'alteracoes': alt_mun,
                    'legislacoes': len(legs), 'pubs_encontradas': pubs_total_mun,
                    'dias_processados': dias_a_processar, 'erros': erros_mun})
                logger.info(f"  ✓ {nome_mun}: {pubs_total_mun} pubs, {alt_mun} alt, "
                            f"{len(erros_mun)} erros")

            except Exception as e_mun:
                municipios_erro += 1
                erros_count += 1
                tb = traceback.format_exc()
                erros_mun.append(f"Erro geral: {e_mun}")
                log_municipios.append({'nome': nome_mun, 'status': 'erro',
                    'legislacoes': len(legs), 'alteracoes': alt_mun,
                    'pubs_encontradas': 0, 'dias_processados': 0, 'erros': erros_mun})
                logger.error(f"  ✗ {nome_mun}: {e_mun}")
                for leg in legs:
                    try:
                        conn.rollback()
                        _registrar_log_legislacao(cur, conn, execucao_id, leg['id'],
                            mun_id, mun_inicio, 'erro', False, 0, 0, 0, 0, 'scraper',
                            url_diario, f'Erro: {str(e_mun)[:200]}', f'{e_mun}\n{tb}'[:2000])
                    except Exception:
                        try: conn.rollback()
                        except: pass

        # ══════════════════════════════════════════════════════════════════
        # PASSO 3: Finalizar
        # ══════════════════════════════════════════════════════════════════
        log_resumo = _montar_log_resumo(log_municipios, inicio, total_legislacoes)
        log_erros  = _montar_log_erros(log_municipios)
        status_final = 'erro' if municipios_erro == municipios_processados else 'concluido'
        log_erros_salvar = log_erros if debug_ativo else (
            f"{erros_count} erro(s). Debug desativado." if erros_count else "")

        _finalizar_execucao(cur, conn, execucao_id, status_final, inicio,
                            municipios_processados, municipios_ok, municipios_erro,
                            alteracoes_detectadas, erros_count, log_resumo, log_erros_salvar)

        logger.info(f"=== Ciclo concluído: {total_legislacoes} leis, "
                    f"{municipios_processados} mun, {alteracoes_detectadas} alt ===")

        if email_relatorio:
            _enviar_email_resumo(execucao_id, municipios_processados, municipios_ok,
                municipios_erro, alteracoes_detectadas, erros_count, inicio,
                log_erros if debug_ativo else "", alertas_enviar_email)

    except Exception as e_global:
        tb = traceback.format_exc()
        logger.error(f"Erro global: {e_global}\n{tb}")
        try:
            log_err_g = f"ERRO GLOBAL:\n{tb}" if debug_ativo else f"Erro global: {e_global}"
            _finalizar_execucao(cur, conn, execucao_id, 'erro', inicio,
                municipios_processados, municipios_ok, municipios_erro,
                alteracoes_detectadas, erros_count, f"Erro global: {e_global}", log_err_g)
            if email_relatorio:
                _enviar_email_resumo(execucao_id, municipios_processados, municipios_ok,
                    municipios_erro, alteracoes_detectadas, erros_count, inicio, log_err_g, [])
        except Exception: pass
    finally:
        try: conn.close()
        except Exception: pass


# ─────────────────────────────────────────────────────────────────────────────
# Funções auxiliares do ciclo
# ─────────────────────────────────────────────────────────────────────────────

def _montar_texto_publicacoes(publicacoes: list) -> str:
    """Monta texto consolidado das publicações para enviar ao analisador IA."""
    import requests as _req
    from bs4 import BeautifulSoup

    partes = []
    for pub in publicacoes:
        titulo = pub.get('titulo', '')
        data_p = pub.get('data', '')
        url_p  = pub.get('url', '')
        partes.append(f"[{data_p}] {titulo}")

        if url_p:
            try:
                resp = _req.get(url_p, timeout=15, headers={
                    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) Chrome/120 Safari/537.36'})
                if resp.ok and 'text/html' in resp.headers.get('Content-Type', ''):
                    soup = BeautifulSoup(resp.text, 'lxml')
                    for tag in soup(['script', 'style', 'nav', 'footer', 'header']):
                        tag.decompose()
                    texto = soup.get_text(separator='\n', strip=True)
                    if len(texto) > 5000:
                        texto = texto[:5000] + '\n[...TRUNCADO...]'
                    if texto.strip():
                        partes.append(texto)
                        partes.append('---')
            except Exception:
                pass

    resultado = '\n'.join(partes)
    if len(resultado) > 150000:
        resultado = resultado[:150000] + '\n\n[CONTEÚDO TRUNCADO POR LIMITE]'
    return resultado


def _processar_alteracao_encontrada(cur, conn, alt_ia, legs_municipio,
                                      mun_id, nome_mun, estado, execucao_id) -> dict:
    """
    Processa alteração detectada pela IA:
    1. Identifica legislação original afetada
    2. Cria a legislação alteradora
    3. Cria relação na árvore genealógica
    4. Cria alerta pendente + notificação
    """
    resultado = {'inserido': False}
    leg_orig_ia = alt_ia.get('legislacao_original', {})
    leg_alt_ia  = alt_ia.get('legislacao_alteradora', {})

    # Identificar legislação original
    leg_original_id = None
    leg_original_titulo = ''
    for leg in legs_municipio:
        num_ok = str(leg['numero'] or '') == str(leg_orig_ia.get('numero', '') or '')
        ano_ok = str(leg['ano'] or '') == str(leg_orig_ia.get('ano', '') or '')
        if num_ok and ano_ok:
            leg_original_id = leg['id']
            leg_original_titulo = f"{leg['tipo_nome']} nº {leg['numero']}/{leg['ano']}"
            break

    if not leg_original_id:
        logger.warning(f"  Legislação original não identificada: {leg_orig_ia}")
        return resultado

    # Checar duplicata
    tipo_alt = leg_alt_ia.get('tipo', '')
    num_alt  = str(leg_alt_ia.get('numero', '') or '')
    ano_alt  = leg_alt_ia.get('ano')
    data_pub = _parse_data_pub(alt_ia.get('data_publicacao'))

    if num_alt and ano_alt:
        cur.execute("SELECT id FROM legislacoes WHERE municipio_nome=%s AND numero=%s AND ano=%s LIMIT 1",
                    (nome_mun, num_alt, ano_alt))
        existente = cur.fetchone()
        if existente:
            cur.execute("SELECT id FROM legislacao_relacoes WHERE legislacao_pai_id=%s AND legislacao_filha_id=%s",
                        (leg_original_id, existente['id']))
            if cur.fetchone():
                logger.info(f"    Duplicata: {tipo_alt} {num_alt}/{ano_alt}")
                return resultado

    leg_alteradora_titulo = f"{tipo_alt} nº {num_alt}/{ano_alt}" if num_alt else tipo_alt
    ementa_alt = leg_alt_ia.get('ementa', alt_ia.get('resumo', ''))
    conteudo   = alt_ia.get('conteudo_completo', '')

    # Criar legislação alteradora
    cur.execute("""
        INSERT INTO legislacoes
            (pais, esfera, estado, municipio_id, municipio_nome,
             tipo_nome, numero, ano, data_publicacao, ementa, conteudo_texto,
             origem, pendente_aprovacao, em_monitoramento, criado_em)
        VALUES ('BR', 'municipal', %s, %s, %s, %s, %s, %s, %s, %s, %s,
                'monitoramento', TRUE, FALSE, NOW())
        RETURNING id
    """, (estado, mun_id, nome_mun, tipo_alt, num_alt or None, ano_alt,
          data_pub, ementa_alt[:1000] if ementa_alt else None,
          conteudo[:10000] if conteudo else None))
    leg_alteradora_id = cur.fetchone()['id']

    # Árvore genealógica
    tipo_relacao = (alt_ia.get('tipo_alteracao', 'alteração') or 'alteração').lower()
    cur.execute("""
        INSERT INTO legislacao_relacoes
            (legislacao_pai_id, legislacao_filha_id, tipo_relacao, descricao, data_relacao)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (legislacao_pai_id, legislacao_filha_id) DO NOTHING
    """, (leg_original_id, leg_alteradora_id, tipo_relacao,
          alt_ia.get('resumo', '')[:500], data_pub))

    # Alerta pendente
    cur.execute("""
        INSERT INTO alteracoes_pendentes
            (municipio_id, legislacao_id, tipo_alteracao, descricao, conteudo_novo, status)
        VALUES (%s, %s, %s, %s, %s, 'pendente')
    """, (mun_id, leg_original_id, tipo_relacao,
          f"{leg_alteradora_titulo} {tipo_relacao} {leg_original_titulo}: {alt_ia.get('resumo', '')}"[:500],
          conteudo[:5000] if conteudo else None))

    # Notificação admin
    cur.execute("""
        INSERT INTO notificacoes_admin (tipo, titulo, mensagem)
        VALUES ('alteracao_detectada', %s, %s)
    """, (f"🔔 {tipo_relacao.title()} detectada — {nome_mun}",
          f"{leg_alteradora_titulo} {tipo_relacao} {leg_original_titulo}\n{alt_ia.get('resumo', '')}"))

    conn.commit()
    logger.info(f"    ✓ ALTERAÇÃO: {leg_alteradora_titulo} → {tipo_relacao} → {leg_original_titulo}")

    resultado['inserido'] = True
    resultado['leg_original_titulo']   = leg_original_titulo
    resultado['leg_alteradora_titulo'] = leg_alteradora_titulo
    resultado['leg_alteradora_id']     = leg_alteradora_id
    return resultado


def _registrar_log_legislacao(cur, conn, execucao_id, legislacao_id,
                                municipio_id, inicio, status, sucesso,
                                pubs_encontradas, pubs_analisadas, alteracoes,
                                duplicadas, metodo, url, mensagem, erro):
    """Registra log detalhado por legislação."""
    try:
        cur.execute("""
            INSERT INTO monitoramento_legislacao_log
                (execucao_id, legislacao_id, municipio_id, data,
                 iniciada_em, finalizada_em, status, sucesso,
                 publicacoes_encontradas, publicacoes_analisadas,
                 alteracoes_detectadas, publicacoes_duplicadas,
                 metodo_busca, url_acessada, mensagem, erro)
            VALUES (%s,%s,%s,NOW(),%s,NOW(),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (execucao_id, legislacao_id, municipio_id, inicio,
              status, sucesso, pubs_encontradas, pubs_analisadas,
              alteracoes, duplicadas, metodo, url, mensagem, erro))
        conn.commit()
    except Exception as e:
        logger.warning(f"Erro ao gravar log legislação {legislacao_id}: {e}")
        try: conn.rollback()
        except: pass


# ─────────────────────────────────────────────────────────────────────────────
# Helpers de log e mensagem
# ─────────────────────────────────────────────────────────────────────────────

def _montar_mensagem_legislacao(pubs_total, pubs_analisadas, alertas,
                                  dias_processados, erros):
    partes = [f"Verificados {dias_processados} dias"]
    partes.append(f" → {pubs_total} publicaç{'ão' if pubs_total == 1 else 'ões'} no diário")
    if pubs_analisadas:
        partes.append(f", {pubs_analisadas} analisada{'s' if pubs_analisadas != 1 else ''} pela IA")
    if alertas:
        partes.append(f" → {alertas} alteraç{'ão' if alertas == 1 else 'ões'} detectada{'s' if alertas != 1 else ''}")
    elif pubs_total > 0:
        partes.append(" → nenhuma alteração encontrada")
    if erros:
        partes.append(f" ⚠ {len(erros)} erro{'s' if len(erros) != 1 else ''}")
    return ''.join(partes)

def _montar_log_resumo(log_municipios, inicio, total_legislacoes):
    duracao = (datetime.now() - inicio).total_seconds()
    linhas  = [f"Execução: {inicio.strftime('%d/%m/%Y %H:%M')} — {duracao:.0f}s",
               f"Total: {total_legislacoes} legislações monitoradas\n"]
    for item in log_municipios:
        icone = '✓' if item['status'] == 'ok' else ('⚠' if item['status'] == 'parcial' else '✗')
        linhas.append(f"{icone} {item['nome']}: {item.get('legislacoes','?')} leis, "
                      f"{item.get('pubs_encontradas','?')} pubs, {item['alteracoes']} alt"
                      + (f" ({len(item['erros'])} erros)" if item.get('erros') else ""))
    return "\n".join(linhas)

def _montar_log_erros(log_municipios):
    blocos = []
    for item in log_municipios:
        if item.get('erros'):
            blocos.append(f"=== {item['nome']} ===")
            blocos.extend(item['erros'])
    return "\n".join(blocos) if blocos else ""

def _finalizar_execucao(cur, conn, execucao_id, status, inicio,
                         mun_proc, mun_ok, mun_erro, alt_det, erros, log, log_erros):
    try:
        cur.execute("""
            UPDATE scheduler_execucoes
            SET status=%s, finalizada_em=NOW(),
                municipios_processados=%s, municipios_ok=%s, municipios_erro=%s,
                alteracoes_detectadas=%s, erros=%s, log=%s, log_erros=%s
            WHERE id=%s
        """, (status, mun_proc, mun_ok, mun_erro, alt_det, erros,
              log, log_erros or None, execucao_id))
        conn.commit()
    except Exception as e:
        logger.error(f"Erro ao finalizar execução: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# E-mail resumo + alertas
# ─────────────────────────────────────────────────────────────────────────────

def _enviar_email_resumo(execucao_id, mun_proc, mun_ok, mun_erro,
                          alt_det, erros, inicio, log_erros, alertas=None):
    try:
        import smtplib
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText

        admin_email = os.getenv('ADMIN_EMAIL','')
        if not admin_email: return

        host  = os.getenv('EMAIL_HOST','')
        port  = int(os.getenv('EMAIL_PORT', 587))
        user  = os.getenv('EMAIL_USER','')
        pwd   = os.getenv('EMAIL_PASS', os.getenv('EMAIL_PASSWORD',''))
        sender = os.getenv('EMAIL_FROM', user)
        if not host or not user or not pwd: return

        duracao      = (datetime.now() - inicio).total_seconds()
        tem_erros    = erros > 0 or mun_erro > 0
        tem_alertas  = alt_det > 0
        status_label = "🔔 ALTERAÇÕES DETECTADAS" if tem_alertas else (
                        "⚠️ COM ERROS" if tem_erros else "✅ SUCESSO")
        assunto      = f"UrbanLex — Monitoramento {inicio.strftime('%d/%m/%Y')} {status_label}"
        cor_status   = "#e74c3c" if tem_erros else ("#f39c12" if tem_alertas else "#27ae60")

        alertas_html = ""
        if alertas:
            rows = "".join(f"""<tr>
                <td style="padding:8px;border:1px solid #ddd">{a.get('municipio','')}</td>
                <td style="padding:8px;border:1px solid #ddd">{a.get('legislacao_original','')}</td>
                <td style="padding:8px;border:1px solid #ddd"><strong>{a.get('tipo_alteracao','')}</strong></td>
                <td style="padding:8px;border:1px solid #ddd">{a.get('legislacao_alteradora','')}</td>
                <td style="padding:8px;border:1px solid #ddd;font-size:12px">{a.get('resumo','')[:150]}</td>
            </tr>""" for a in alertas)
            alertas_html = f"""
            <div style="margin-top:20px;padding:15px;background:#fff8e1;border-left:4px solid #f39c12;border-radius:4px;">
                <h3 style="margin:0 0 10px;color:#856404;">🔔 Alterações Detectadas ({alt_det})</h3>
                <table style="width:100%;border-collapse:collapse;font-size:13px;">
                <tr style="background:#f5f5f5"><th style="padding:8px;border:1px solid #ddd;text-align:left">Município</th>
                <th style="padding:8px;border:1px solid #ddd;text-align:left">Lei Monitorada</th>
                <th style="padding:8px;border:1px solid #ddd;text-align:left">Tipo</th>
                <th style="padding:8px;border:1px solid #ddd;text-align:left">Lei Alteradora</th>
                <th style="padding:8px;border:1px solid #ddd;text-align:left">Resumo</th></tr>
                {rows}</table>
                <p style="margin-top:10px;font-size:12px;color:#666;">⚠️ Pendentes de aprovação.</p>
            </div>"""

        erros_html = ""
        if log_erros:
            erros_html = f"""
            <div style="margin-top:20px;padding:15px;background:#fff3cd;border-left:4px solid #f39c12;border-radius:4px;">
                <h3 style="margin:0 0 10px;color:#856404;">🔍 Log de Erros</h3>
                <pre style="font-size:12px;white-space:pre-wrap;margin:0;color:#333;">{str(log_erros)[:4000]}</pre>
            </div>"""

        html = f"""<!DOCTYPE html><html><body style="font-family:Arial,sans-serif;background:#f5f5f5;padding:20px;margin:0;">
  <div style="max-width:700px;margin:0 auto;background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.1);">
    <div style="background:#1a1a2e;padding:24px;text-align:center;">
      <h1 style="margin:0;color:#00e5a0;font-size:22px;">🤖 UrbanLex — Monitoramento por Legislação</h1>
      <p style="margin:8px 0 0;color:#ccc;font-size:13px;">Relatório de {inicio.strftime('%d/%m/%Y %H:%M')} — Duração: {duracao:.0f}s</p>
    </div>
    <div style="padding:24px;">
      <div style="text-align:center;margin-bottom:20px;">
        <span style="display:inline-block;background:{cor_status};color:#fff;padding:6px 18px;border-radius:20px;font-weight:bold;font-size:14px;">{status_label}</span>
      </div>
      <table style="width:100%;border-collapse:collapse;margin:16px 0;">
        <tr><td style="padding:10px;border-bottom:1px solid #eee;font-weight:bold;">Municípios</td><td style="padding:10px;border-bottom:1px solid #eee;text-align:right">{mun_proc}</td></tr>
        <tr><td style="padding:10px;border-bottom:1px solid #eee;font-weight:bold;">✅ OK</td><td style="padding:10px;border-bottom:1px solid #eee;text-align:right">{mun_ok}</td></tr>
        <tr><td style="padding:10px;border-bottom:1px solid #eee;font-weight:bold;">❌ Com erro</td><td style="padding:10px;border-bottom:1px solid #eee;text-align:right;color:#e74c3c">{mun_erro}</td></tr>
        <tr><td style="padding:10px;border-bottom:1px solid #eee;font-weight:bold;">🔔 Alterações</td><td style="padding:10px;border-bottom:1px solid #eee;text-align:right;color:#f39c12;font-weight:bold">{alt_det}</td></tr>
      </table>
      {alertas_html}{erros_html}
    </div>
    <div style="background:#f9f9f9;padding:16px;text-align:center;font-size:11px;color:#999;">
      UrbanLex v4.0 — Monitoramento por Legislação · Execução #{execucao_id}
    </div>
  </div></body></html>"""

        msg = MIMEMultipart('alternative')
        msg['Subject'] = assunto; msg['From'] = sender; msg['To'] = admin_email
        msg.attach(MIMEText(html, 'html'))
        with smtplib.SMTP(host, port) as srv:
            srv.starttls(); srv.login(user, pwd); srv.sendmail(sender, admin_email, msg.as_string())
        logger.info(f"Email resumo enviado para {admin_email}")
    except Exception as e:
        logger.error(f"Erro ao enviar email: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Scheduler (APScheduler)
# ─────────────────────────────────────────────────────────────────────────────

_scheduler = None
_scheduler_status = 'parado'

def iniciar_scheduler():
    global _scheduler, _scheduler_status
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        _scheduler = BackgroundScheduler()
        conn = get_db(); cur = conn.cursor()
        cur.execute("SELECT horario_execucao, status FROM scheduler_config ORDER BY id LIMIT 1")
        cfg = cur.fetchone(); conn.close()
        horario = (cfg['horario_execucao'] if cfg else '02:00') or '02:00'
        h, m = horario.split(':')
        if cfg and cfg.get('status') == 'pausado':
            _scheduler_status = 'pausado'
            logger.info("Scheduler configurado mas PAUSADO"); return
        _scheduler.add_job(executar_ciclo_completo, 'cron', hour=int(h), minute=int(m),
                           id='monitoramento_diario', replace_existing=True)
        _scheduler.start()
        _scheduler_status = 'ativo'
        logger.info(f"Scheduler iniciado: execução diária às {horario}")
    except ImportError:
        logger.warning("APScheduler não instalado — agendamento desativado")
    except Exception as e:
        logger.error(f"Erro ao iniciar scheduler: {e}")

def pausar_scheduler():
    global _scheduler_status
    _scheduler_status = 'pausado'
    if _scheduler:
        try: _scheduler.pause_job('monitoramento_diario')
        except: pass
    try:
        conn = get_db(); cur = conn.cursor()
        cur.execute("UPDATE scheduler_config SET status='pausado'")
        conn.commit(); conn.close()
    except: pass

def retomar_scheduler():
    global _scheduler_status
    _scheduler_status = 'ativo'
    if _scheduler:
        try: _scheduler.resume_job('monitoramento_diario')
        except: pass
    try:
        conn = get_db(); cur = conn.cursor()
        cur.execute("UPDATE scheduler_config SET status='ativo'")
        conn.commit(); conn.close()
    except: pass

def parar_scheduler():
    global _scheduler, _scheduler_status
    _scheduler_status = 'parado'
    if _scheduler:
        try: _scheduler.shutdown(wait=False)
        except: pass
        _scheduler = None
