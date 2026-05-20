"""
═══════════════════════════════════════════════════════════════════════════════
FILA DE EXTRAÇÃO — Worker em Background
═══════════════════════════════════════════════════════════════════════════════

Worker que processa items de fila_extracao chamando o pipeline_extracao_lei.

FLUXO:
1. Loop infinito (sleep 5s entre verificações)
2. Reseta jobs travados (rodando há mais de 60min sem terminar)
3. Pega 1 item 'aguardando' (ordem ASC, criado_em ASC)
4. Marca 'rodando' com job_id e iniciado_em
5. Chama processar_municipio() (pipeline completo)
6. Se sucesso:
   - salvar_processamento() → INSERT em legislacao_processamentos
   - Se consolidar_apos=True: consolidar_municipio_db()
   - Marca 'concluido'
7. Se erro: marca 'erro' com erro_etapa + erro_msg

USO:
    from modulos.fila_extracao_worker import iniciar_worker_extracao
    iniciar_worker_extracao(app, get_db)
═══════════════════════════════════════════════════════════════════════════════
"""

import threading


class PipelineCanceladoError(Exception):
    pass

import time
import uuid
import logging
import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

# Flag global para pausar a fila
_fila_extracao_pausada = False


def pausar_fila():
    global _fila_extracao_pausada
    _fila_extracao_pausada = True
    logger.info("Fila de extração PAUSADA")


def retomar_fila():
    global _fila_extracao_pausada
    _fila_extracao_pausada = False
    logger.info("Fila de extração RETOMADA")


def status_fila():
    return {'pausada': _fila_extracao_pausada}


def _atualizar_progresso(get_db, item_id, msg):
    """Helper pra atualizar progresso_atual durante execução."""
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            "UPDATE fila_extracao SET progresso_atual=%s WHERE id=%s",
            (msg[:500], item_id)
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        pass


def _processar_item(item, get_db):
    """Processa um item da fila chamando o pipeline."""
    from modulos.pipeline_extracao_lei import (
        processar_municipio,
        salvar_processamento,
        consolidar_municipio_db,
        calcular_md5_zip,
        buscar_processamento_por_md5,
    )
    
    item_id = item['id']
    job_id = str(uuid.uuid4())[:8]
    
    # Marca como rodando
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        UPDATE fila_extracao 
        SET status='rodando', job_id=%s, iniciado_em=NOW(), 
            progresso_atual='Iniciando pipeline...'
        WHERE id=%s
    """, (job_id, item_id))
    conn.commit()
    cur.close()
    conn.close()
    
    logger.info(f"[{job_id}] Iniciando extração: {item['municipio']}/{item['estado']}")
    
    # ───────────────────────────────────────────────────────────
    # CACHE INTELIGENTE: checa MD5 antes de rodar o pipeline
    # (pulado se item.usar_cache=False — operador forçou reprocessamento)
    # ───────────────────────────────────────────────────────────
    try:
        if not item.get('usar_cache', True):
            logger.info(f"[{job_id}] Cache desabilitado (forçar_reprocessamento). Pulando verificação MD5.")
            _atualizar_progresso(get_db, item_id, 'Cache desabilitado (forçando reprocessamento)')
            raise StopIteration  # sai do try, vai pro pipeline normal
        _atualizar_progresso(get_db, item_id, 'Verificando cache (MD5)...')
        zip_md5 = calcular_md5_zip(item['zip_path'])
        if zip_md5:
            cached = buscar_processamento_por_md5(
                item['municipio'], item['estado'], zip_md5
            )
            if cached:
                logger.info(f"[{job_id}] CACHE HIT: reaproveitando processamento "
                           f"id={cached['id']} (md5={zip_md5[:8]}...)")
                # Marca como concluido reaproveitando processamento anterior
                conn = get_db()
                cur = conn.cursor()
                cur.execute("""
                    UPDATE fila_extracao 
                    SET status='concluido', concluido_em=NOW(),
                        processamento_id=%s,
                        progresso_atual='Cache HIT (ZIP idêntico já processado)'
                    WHERE id=%s
                """, (cached['id'], item_id))
                conn.commit()
                cur.close()
                conn.close()
                
                # Re-consolida se solicitado
                if item.get('consolidar_apos', True):
                    consolidar_municipio_db(
                        municipio=item['municipio'],
                        estado_uf=item['estado'],
                        consolidado_por=item.get('criado_por'),
                        log_callback=lambda m: logger.info(f"[{job_id}] {m}"),
                    )
                logger.info(f"[{job_id}] CONCLUÍDO via cache (0s, $0)")
                return
    except StopIteration:
        pass  # cache desabilitado pelo operador, continua sem cache
    except Exception as e:
        logger.warning(f"[{job_id}] Erro ao verificar cache: {e} (continua sem cache)")
    
    # Counter pra cursor incremental dos logs
    _log_cursor = [0]
    
    def _gravar_log_banco(msg, nivel='info'):
        '''Grava 1 linha de log em buscas_logs (best-effort, sem bloquear pipeline)'''
        try:
            from datetime import datetime as _dt
            _log_cursor[0] += 1
            conn = get_db(); cur = conn.cursor()
            cur.execute(
                "INSERT INTO buscas_logs (job_id, cursor, nivel, msg, criado_em, ts) "
                "VALUES (%s, %s, %s, %s, NOW(), %s)",
                (job_id, _log_cursor[0], nivel, (msg or '')[:5000], 
                 _dt.now().strftime('%H:%M:%S'))
            )
            conn.commit(); cur.close(); conn.close()
        except Exception as _e:
            logger.warning(f"[{job_id}] falha ao gravar log no banco: {_e}")
    
    _cancel_check = [0]
    
    def _checar_cancelamento():
        try:
            _c = get_db(); _cur = _c.cursor()
            _cur.execute("SELECT status FROM fila_extracao WHERE id=%s", (item_id,))
            _row = _cur.fetchone()
            _cur.close(); _c.close()
            if _row and _row[0] == 'cancelado':
                raise PipelineCanceladoError(f"Job {job_id} cancelado")
        except PipelineCanceladoError:
            raise
        except Exception as _e_cancel:
            logger.warning(f"[{job_id}] erro check cancel: {_e_cancel}")
    
    try:
        # Callback de log + check cancelamento periodico
        def log_cb(msg):
            logger.info(f"[{job_id}] {msg}")
            _gravar_log_banco(msg)
            if 'ETAPA' in msg or 'CACHE HIT' in msg or 'CONCLU' in msg:
                _atualizar_progresso(get_db, item_id, msg)
            _cancel_check[0] += 1
            if _cancel_check[0] % 5 == 0:
                _checar_cancelamento()
        
        # Roda pipeline completo
        resultado = processar_municipio(
            zip_path=item['zip_path'],
            municipio=item['municipio'],
            estado=item['estado'],
            usar_cache=item.get('usar_cache', True),
            log_callback=log_cb,
        )
        
        if not resultado.get('sucesso'):
            # Erro no pipeline
            conn = get_db()
            cur = conn.cursor()
            cur.execute("""
                UPDATE fila_extracao 
                SET status='erro', concluido_em=NOW(),
                    erro_etapa=%s, erro_msg=%s,
                    progresso_atual='Erro'
                WHERE id=%s
            """, (
                resultado.get('erro_etapa'),
                (resultado.get('erro_msg') or '')[:5000],
                item_id,
            ))
            conn.commit()
            cur.close()
            conn.close()
            logger.error(f"[{job_id}] Pipeline falhou na etapa {resultado.get('erro_etapa')}: {resultado.get('erro_msg')}")
            return
        
        # Salva resultado no banco
        _atualizar_progresso(get_db, item_id, 'Salvando no banco...')
        proc_id = salvar_processamento(
            resultado,
            legislacao_id=item.get('legislacao_id'),
            processado_por=item.get('criado_por'),
            log_callback=lambda m: logger.info(f"[{job_id}] {m}"),
            legislacao_label=item.get('legislacao_label'),
            municipio=item.get('municipio'),
            estado=item.get('estado'),
        )
        
        # Consolida se solicitado
        if item.get('consolidar_apos', True) and proc_id:
            _atualizar_progresso(get_db, item_id, 'Consolidando município...')
            cons = consolidar_municipio_db(
                municipio=item['municipio'],
                estado_uf=item['estado'],
                consolidado_por=item.get('criado_por'),
                log_callback=lambda m: logger.info(f"[{job_id}] {m}"),
            )
            logger.info(f"[{job_id}] Consolidação: {cons.get('sucesso')}")
        
        # Marca como concluido
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            UPDATE fila_extracao 
            SET status='concluido', concluido_em=NOW(),
                processamento_id=%s,
                progresso_atual='Concluído'
            WHERE id=%s
        """, (proc_id, item_id))
        conn.commit()
        cur.close()
        conn.close()
        
        logger.info(f"[{job_id}] CONCLUÍDO ({resultado['metricas']['tempo_total']:.0f}s, "
                   f"${resultado['metricas']['custo_total']:.2f})")
    
    except PipelineCanceladoError as _cancel_e:
        logger.info(f"[{job_id}] CANCELADO: {_cancel_e}")
        try:
            _conn_c = get_db(); _cur_c = _conn_c.cursor()
            _cur_c.execute("UPDATE fila_extracao SET concluido_em=NOW(), progresso_atual='Cancelado pelo operador' WHERE id=%s AND status='cancelado'", (item_id,))
            _conn_c.commit(); _cur_c.close(); _conn_c.close()
        except Exception:
            pass
        return
    except Exception as e:
        logger.exception(f"[{job_id}] Erro inesperado")
        try:
            conn = get_db()
            cur = conn.cursor()
            cur.execute("""
                UPDATE fila_extracao 
                SET status='erro', concluido_em=NOW(),
                    erro_msg=%s, progresso_atual='Erro inesperado'
                WHERE id=%s
            """, (str(e)[:5000], item_id))
            conn.commit()
            cur.close()
            conn.close()
        except Exception:
            pass


def iniciar_worker_extracao(app, get_db):
    """
    Inicia o worker em thread separada.
    
    Args:
        app:    Flask app (pra app_context se precisar)
        get_db: função que retorna conexão psycopg2
    """
    def worker():
        time.sleep(30)  # aguarda Gunicorn inicializar
        logger.info("Worker fila_extracao INICIADO")
        
        while True:
            try:
                # Reseta jobs travados (>60min rodando)
                conn = get_db()
                cur = conn.cursor()
                cur.execute("""
                    UPDATE fila_extracao 
                    SET status='aguardando', job_id=NULL, iniciado_em=NULL,
                        progresso_atual='Reset (job travado)'
                    WHERE status='rodando' 
                      AND iniciado_em < NOW() - INTERVAL '60 minutes'
                """)
                conn.commit()
                
                # Verifica pausa
                if _fila_extracao_pausada:
                    cur.close()
                    conn.close()
                    time.sleep(5)
                    continue
                
                # Pega próximo item
                cur2 = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur2.execute("""
                    SELECT * FROM fila_extracao
                    WHERE status='aguardando'
                    ORDER BY ordem ASC, criado_em ASC
                    LIMIT 1
                """)
                item = cur2.fetchone()
                cur.close()
                cur2.close()
                conn.close()
                
                if not item:
                    time.sleep(5)
                    continue
                
                # Processa
                _processar_item(item, get_db)
                
            except Exception:
                logger.exception("Erro no loop do worker fila_extracao")
                time.sleep(10)
    
    t = threading.Thread(target=worker, daemon=True, name='fila_extracao_worker')
    t.start()
    logger.info("Thread fila_extracao_worker iniciada")
    return t
