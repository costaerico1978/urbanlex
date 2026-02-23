#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
modulos/scheduler_integrado.py
────────────────────────────────
Scheduler do monitoramento diário com:
- Scraper inteligente (Playwright + Gemini Vision)
- Log detalhado por município (incluindo stack traces)
- E-mail resumo após cada execução
"""

import os
import logging
import traceback
from datetime import datetime, timedelta

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv('DATABASE_URL')


def get_db():
    if DATABASE_URL:
        return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        database=os.getenv('DB_NAME', 'urbanismo'),
        user=os.getenv('DB_USER', 'postgres'),
        password=os.getenv('DB_PASSWORD', ''),
        cursor_factory=RealDictCursor
    )


def _parse_data_pub(data_str):
    """Tenta converter string de data para DATE."""
    if not data_str:
        return None
    for fmt in ('%d/%m/%Y','%Y-%m-%d','%d-%m-%Y','%d/%m/%y'):
        try:
            return datetime.strptime(data_str.strip(), fmt).date()
        except Exception:
            pass
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Ciclo principal
# ─────────────────────────────────────────────────────────────────────────────

def executar_ciclo_completo(bridge_callback=None, disparado_por=None):
    from modulos.bridge_integracao import processar_alteracao_detectada, e_alteracao_urbanistica

    logger.info("=== Ciclo de monitoramento iniciado ===")
    inicio = datetime.now()

    conn = get_db()
    cur  = conn.cursor()

    # Ler configurações de observabilidade
    cur.execute("SELECT debug_ativo, email_relatorio FROM scheduler_config ORDER BY id LIMIT 1")
    cfg_row = cur.fetchone()
    debug_ativo     = cfg_row['debug_ativo']     if cfg_row and cfg_row.get('debug_ativo')     is not None else True
    email_relatorio = cfg_row['email_relatorio'] if cfg_row and cfg_row.get('email_relatorio') is not None else True

    # Registrar início
    cur.execute("""
        INSERT INTO scheduler_execucoes (status, disparado_por)
        VALUES ('rodando', %s) RETURNING id
    """, (disparado_por,))
    execucao_id = cur.fetchone()['id']
    conn.commit()

    municipios_processados = 0
    municipios_ok          = 0
    municipios_erro        = 0
    alteracoes_detectadas  = 0
    erros_count            = 0
    log_municipios         = []

    # Importar scraper inteligente (opcional)
    try:
        from modulos.scraper_inteligente import buscar_publicacoes, registrar_falha
        SCRAPER_OK = True
        logger.info("Scraper inteligente: disponível")
    except ImportError:
        SCRAPER_OK = False
        logger.warning("Scraper inteligente não disponível — usando fallback")

    try:
        cur.execute("SELECT * FROM municipios WHERE ativo = TRUE ORDER BY nome")
        municipios = cur.fetchall()

        if not municipios:
            _finalizar_execucao(cur, conn, execucao_id, 'concluido', inicio,
                                0, 0, 0, 0, 0, "Nenhum município ativo.", "")
            if email_relatorio:
                _enviar_email_resumo(execucao_id, 0, 0, 0, 0, 0, inicio, "Nenhum município ativo.")
            return

        for mun in municipios:
            nome_mun = mun['nome']
            mun_id   = mun['id']
            municipios_processados += 1
            erros_mun = []
            alt_mun   = 0

            try:
                logger.info(f"Processando: {nome_mun}")

                # ── CAMINHO A: Scraper inteligente (URL configurada) ──────────
                if SCRAPER_OK and mun.get('url_diario'):
                    from datetime import date as _date

                    # Data de início: último monitoramento ou 7 dias atrás
                    if mun.get('ultimo_monitoramento'):
                        data_ini = mun['ultimo_monitoramento'].date()
                    else:
                        data_ini = _date.today() - timedelta(days=7)

                    res = buscar_publicacoes(mun_id, data_ini, _date.today())

                    if res['sucesso']:
                        for pub in res['publicacoes']:
                            titulo  = pub.get('titulo','')
                            url_pub = pub.get('url','')
                            data_p  = pub.get('data','')
                            if not titulo:
                                continue
                            # Checar duplicata
                            cur.execute("""
                                SELECT id FROM legislacoes
                                WHERE url_original=%s AND url_original IS NOT NULL
                                LIMIT 1
                            """, (url_pub or None,))
                            if cur.fetchone():
                                continue
                            # Só processar se urbanística
                            if not e_alteracao_urbanistica(titulo, ''):
                                continue
                            try:
                                cur.execute("""
                                    INSERT INTO legislacoes
                                        (pais,esfera,estado,municipio_nome,ementa,url_original,
                                         origem,pendente_aprovacao,em_monitoramento,
                                         data_publicacao,criado_em)
                                    VALUES ('BR','municipal',%s,%s,%s,%s,
                                            'monitoramento',TRUE,TRUE,%s,NOW())
                                    RETURNING id
                                """, (mun.get('estado',''), nome_mun, titulo[:500],
                                      url_pub or None, _parse_data_pub(data_p)))
                                nova = cur.fetchone()
                                nova_id = nova['id'] if nova else None
                                alteracoes_detectadas += 1
                                alt_mun += 1
                                if bridge_callback and nova_id:
                                    bridge_callback(
                                        alteracao_id=None,
                                        legislacao_id=nova_id,
                                        municipio_nome=nome_mun,
                                        titulo_alteracao=titulo,
                                        conteudo_legislacao='',
                                        zona_nome=None
                                    )
                            except Exception as e_ins:
                                erros_count += 1
                                erros_mun.append(f"  Insert pub: {e_ins}\n{traceback.format_exc()}")
                    else:
                        erros_count += 1
                        msg = res.get('mensagem','')
                        erros_mun.append(f"  Scraper: {msg}")
                        registrar_falha(mun_id, msg)
                        logger.warning(f"  Scraper falhou — {nome_mun}: {msg}")

                # ── CAMINHO B: Fallback — alterações existentes no banco ──────
                else:
                    if not mun.get('url_diario'):
                        erros_mun.append("  URL do diário oficial não configurada")

                    cur.execute("""
                        SELECT a.*, l.conteudo_texto as conteudo, l.ementa as titulo
                        FROM alteracoes a
                        JOIN legislacoes l ON l.id = a.legislacao_id
                        WHERE l.municipio_nome = %s AND a.analisado = FALSE
                    """, (nome_mun,))
                    for alt in (cur.fetchall() or []):
                        try:
                            alteracoes_detectadas += 1
                            alt_mun += 1
                            cur.execute("UPDATE alteracoes SET analisado=TRUE WHERE id=%s", (alt['id'],))
                            if bridge_callback and e_alteracao_urbanistica(
                                alt.get('titulo',''), alt.get('conteudo','')
                            ):
                                bridge_callback(
                                    alteracao_id=alt['id'],
                                    legislacao_id=alt['legislacao_id'],
                                    municipio_nome=nome_mun,
                                    titulo_alteracao=alt.get('titulo',''),
                                    conteudo_legislacao=alt.get('conteudo',''),
                                    zona_nome=None
                                )
                        except Exception as e_alt:
                            erros_count += 1
                            erros_mun.append(f"  Alt {alt.get('id')}: {e_alt}\n{traceback.format_exc()}")

                conn.commit()
                cur.execute("UPDATE municipios SET ultimo_monitoramento=NOW() WHERE id=%s", (mun_id,))
                conn.commit()

                municipios_ok += 1
                log_municipios.append({
                    'nome': nome_mun,
                    'status': 'ok' if not erros_mun else 'parcial',
                    'alteracoes': alt_mun,
                    'erros': erros_mun,
                })
                logger.info(f"  ✓ {nome_mun}: {alt_mun} alt, {len(erros_mun)} erros")

            except Exception as e_mun:
                municipios_erro += 1
                erros_count += 1
                tb = traceback.format_exc()
                erros_mun.append(f"  Erro geral: {e_mun}\n{tb}")
                log_municipios.append({'nome':nome_mun,'status':'erro','alteracoes':alt_mun,'erros':erros_mun})
                logger.error(f"  ✗ {nome_mun}: {e_mun}")
                try: conn.rollback()
                except Exception: pass

        # Montar strings de log
        log_resumo = _montar_log_resumo(log_municipios, inicio)
        log_erros  = _montar_log_erros(log_municipios)
        status_final = 'erro' if municipios_erro == municipios_processados else 'concluido'

        log_erros_salvar = log_erros if debug_ativo else (
            f"{erros_count} erro(s). Debug desativado." if erros_count else "")

        _finalizar_execucao(cur, conn, execucao_id, status_final, inicio,
                            municipios_processados, municipios_ok, municipios_erro,
                            alteracoes_detectadas, erros_count, log_resumo, log_erros_salvar)

        logger.info(f"=== Ciclo concluído: {municipios_processados} mun, "
                    f"{alteracoes_detectadas} alt, {erros_count} erros ===")

        if email_relatorio:
            _enviar_email_resumo(
                execucao_id, municipios_processados, municipios_ok, municipios_erro,
                alteracoes_detectadas, erros_count, inicio,
                log_erros if debug_ativo else ""
            )

    except Exception as e_global:
        tb = traceback.format_exc()
        logger.error(f"Erro global: {e_global}\n{tb}")
        try:
            log_err_g = f"ERRO GLOBAL:\n{tb}" if debug_ativo else f"Erro global: {e_global}"
            _finalizar_execucao(cur, conn, execucao_id, 'erro', inicio,
                                municipios_processados, municipios_ok, municipios_erro,
                                alteracoes_detectadas, erros_count,
                                f"Erro global: {e_global}", log_err_g)
            if email_relatorio:
                _enviar_email_resumo(
                    execucao_id, municipios_processados, municipios_ok, municipios_erro,
                    alteracoes_detectadas, erros_count, inicio, log_err_g)
        except Exception: pass
    finally:
        try: conn.close()
        except Exception: pass


# ─────────────────────────────────────────────────────────────────────────────
# Helpers de log
# ─────────────────────────────────────────────────────────────────────────────

def _montar_log_resumo(log_municipios, inicio):
    duracao = (datetime.now() - inicio).total_seconds()
    linhas  = [f"Execução: {inicio.strftime('%d/%m/%Y %H:%M')} — {duracao:.0f}s\n"]
    for item in log_municipios:
        icone = '✓' if item['status'] == 'ok' else ('⚠' if item['status'] == 'parcial' else '✗')
        linhas.append(f"{icone} {item['nome']}: {item['alteracoes']} alterações"
                      + (f" ({len(item['erros'])} erros)" if item['erros'] else ""))
    return "\n".join(linhas)


def _montar_log_erros(log_municipios):
    blocos = []
    for item in log_municipios:
        if item['erros']:
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
# E-mail resumo
# ─────────────────────────────────────────────────────────────────────────────

def _enviar_email_resumo(execucao_id, mun_proc, mun_ok, mun_erro,
                          alt_det, erros, inicio, log_erros):
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
        status_label = "⚠️ COM ERROS" if tem_erros else "✅ SUCESSO"
        assunto      = f"UrbanLex — Monitoramento {inicio.strftime('%d/%m/%Y')} {status_label}"
        cor_status   = "#e74c3c" if tem_erros else "#27ae60"

        erros_html = ""
        if log_erros:
            erros_html = f"""
            <div style="margin-top:20px;padding:15px;background:#fff3cd;border-left:4px solid #f39c12;border-radius:4px;">
                <h3 style="margin:0 0 10px;color:#856404;">🔍 Log de Erros</h3>
                <pre style="font-size:12px;white-space:pre-wrap;margin:0;color:#333;">{log_erros[:4000]}{'...[truncado]' if len(log_erros)>4000 else ''}</pre>
            </div>"""

        html = f"""<!DOCTYPE html><html><body style="font-family:Arial,sans-serif;background:#f5f5f5;padding:20px;margin:0;">
  <div style="max-width:600px;margin:0 auto;background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.1);">
    <div style="background:#1a1a2e;padding:24px;text-align:center;">
      <h1 style="color:#00e5a0;margin:0;font-size:22px;">UrbanLex</h1>
      <p style="color:#aaa;margin:4px 0 0;font-size:13px;">Relatório de Monitoramento</p>
    </div>
    <div style="background:{cor_status};padding:12px;text-align:center;">
      <span style="color:#fff;font-weight:bold;font-size:16px;">{status_label}</span>
    </div>
    <div style="padding:24px;">
      <table style="width:100%;border-collapse:collapse;margin-bottom:20px;">
        <tr>
          <td style="padding:10px;text-align:center;background:#f8f9fa;border-radius:6px;width:25%;">
            <div style="font-size:28px;font-weight:bold;color:#1a1a2e;">{mun_proc}</div>
            <div style="font-size:12px;color:#666;margin-top:4px;">municípios</div>
          </td><td style="width:4%;"></td>
          <td style="padding:10px;text-align:center;background:#f8f9fa;border-radius:6px;width:25%;">
            <div style="font-size:28px;font-weight:bold;color:#27ae60;">{mun_ok}</div>
            <div style="font-size:12px;color:#666;margin-top:4px;">sem erros</div>
          </td><td style="width:4%;"></td>
          <td style="padding:10px;text-align:center;background:#f8f9fa;border-radius:6px;width:25%;">
            <div style="font-size:28px;font-weight:bold;color:#{'e74c3c' if mun_erro else '27ae60'};">{mun_erro}</div>
            <div style="font-size:12px;color:#666;margin-top:4px;">com erro</div>
          </td><td style="width:4%;"></td>
          <td style="padding:10px;text-align:center;background:#f8f9fa;border-radius:6px;width:25%;">
            <div style="font-size:28px;font-weight:bold;color:#3d9be9;">{alt_det}</div>
            <div style="font-size:12px;color:#666;margin-top:4px;">alterações</div>
          </td>
        </tr>
      </table>
      <table style="width:100%;font-size:13px;color:#555;">
        <tr><td style="padding:4px 0;"><b>Data/hora:</b></td><td>{inicio.strftime('%d/%m/%Y às %H:%M')}</td></tr>
        <tr><td style="padding:4px 0;"><b>Duração:</b></td><td>{duracao:.0f} segundos</td></tr>
        <tr><td style="padding:4px 0;"><b>ID execução:</b></td><td>#{execucao_id}</td></tr>
      </table>
      {erros_html}
    </div>
    <div style="background:#f8f9fa;padding:16px;text-align:center;border-top:1px solid #eee;">
      <p style="color:#999;font-size:12px;margin:0;">Acesse <b>Monitoramento → Histórico</b> para detalhes completos.</p>
    </div>
  </div>
</body></html>"""

        msg = MIMEMultipart('alternative')
        msg['Subject'] = assunto
        msg['From']    = sender
        msg['To']      = admin_email
        msg.attach(MIMEText(html, 'html'))

        with smtplib.SMTP(host, port) as smtp:
            smtp.starttls()
            smtp.login(user, pwd)
            smtp.sendmail(sender, [admin_email], msg.as_string())

        try:
            conn2 = get_db()
            cur2  = conn2.cursor()
            cur2.execute("UPDATE scheduler_execucoes SET email_enviado=TRUE WHERE id=%s", (execucao_id,))
            conn2.commit()
            conn2.close()
        except Exception: pass

        logger.info(f"E-mail enviado para {admin_email}")

    except Exception as e:
        logger.error(f"Erro ao enviar e-mail: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# APScheduler
# ─────────────────────────────────────────────────────────────────────────────

_scheduler = None


def iniciar_scheduler():
    global _scheduler
    if _scheduler and _scheduler.running:
        return _scheduler

    try:
        conn = get_db()
        cur  = conn.cursor()
        cur.execute("SELECT * FROM scheduler_config ORDER BY id DESC LIMIT 1")
        config = cur.fetchone()
        conn.close()
        horario = config['horario_execucao'] if config else '02:00'
        status  = config['status'] if config else 'ativo'
    except Exception:
        horario = '02:00'
        status  = 'ativo'

    hora, minuto = str(horario).split(':')[:2]
    _scheduler = BackgroundScheduler(timezone='America/Sao_Paulo')

    from modulos.bridge_integracao import processar_alteracao_detectada
    _scheduler.add_job(
        func=lambda: executar_ciclo_completo(bridge_callback=processar_alteracao_detectada),
        trigger=CronTrigger(hour=int(hora), minute=int(minuto)),
        id='monitoramento_diario',
        name='Monitoramento Diário',
        replace_existing=True,
        misfire_grace_time=3600
    )

    if status == 'ativo':
        _scheduler.start()
        logger.info(f"✅ Scheduler iniciado — {horario}")
    else:
        logger.info("⚠️  Scheduler pausado")

    return _scheduler


def pausar_scheduler():
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.pause()

def retomar_scheduler():
    global _scheduler
    if _scheduler:
        if not _scheduler.running: _scheduler.start()
        else: _scheduler.resume()

def parar_scheduler():
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
