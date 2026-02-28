#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
modulos/email_resumo_diario.py
───────────────────────────────
Envia email de resumo diário consolidando toda atividade do agente:
- Municípios novos detectados
- Legislações descobertas e cadastradas
- Alterações/revogações detectadas
- Erros e pendências
"""

import os
import json
import logging
import smtplib
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)


def _db():
    import psycopg2
    from psycopg2.extras import RealDictCursor
    return psycopg2.connect(os.getenv('DATABASE_URL'), cursor_factory=RealDictCursor)


def _qry(sql, params=None, fetch='all'):
    conn = _db()
    cur = conn.cursor()
    cur.execute(sql, params)
    result = cur.fetchall() if fetch == 'all' else cur.fetchone()
    cur.close(); conn.close()
    return result


def enviar_resumo_diario():
    """Envia email com resumo de tudo que aconteceu nas últimas 24h."""
    admin_email = os.getenv('ADMIN_EMAIL', '')
    if not admin_email:
        return

    host = os.getenv('EMAIL_HOST', '')
    port = int(os.getenv('EMAIL_PORT', 587))
    user = os.getenv('EMAIL_USER', '')
    pwd = os.getenv('EMAIL_PASS', os.getenv('EMAIL_PASSWORD', ''))
    sender = os.getenv('EMAIL_FROM', user)
    if not host or not user or not pwd:
        return

    try:
        # Buscar atividades das últimas 24h
        desde = datetime.now() - timedelta(hours=24)

        atividades = _qry("""SELECT tipo, mensagem, detalhes, criado_em
                            FROM feed_atividades
                            WHERE criado_em >= %s
                            ORDER BY criado_em DESC""", (desde,))

        if not atividades:
            logger.info("Nenhuma atividade nas últimas 24h — email não enviado")
            return

        # Contadores
        novos_municipios = [a for a in atividades if a['tipo'] == 'municipio_novo']
        legislacoes_desc = [a for a in atividades if a['tipo'] == 'legislacao_descoberta']
        alteracoes = [a for a in atividades if a['tipo'] == 'alteracao_detectada']
        revogacoes = [a for a in atividades if a['tipo'] == 'revogacao_detectada']
        novas_leis = [a for a in atividades if a['tipo'] == 'nova_legislacao_detectada']
        erros = [a for a in atividades if 'erro' in a['tipo']]

        # Integrações
        integracoes = _qry("""SELECT * FROM integracao_log
                             WHERE criado_em >= %s ORDER BY criado_em DESC""",
                          (desde,))

        # Montar HTML
        icon_map = {
            'municipio_novo': '📍', 'legislacao_descoberta': '📜',
            'alteracao_detectada': '🔔', 'revogacao_detectada': '🚫',
            'nova_legislacao_detectada': '📝', 'ciclo_monitoramento': '⚙️',
            'erro_integracao': '⚠️',
        }

        timeline_html = ""
        for a in atividades[:30]:
            icon = icon_map.get(a['tipo'], '•')
            hora = a['criado_em'].strftime('%H:%M') if a.get('criado_em') else ''
            timeline_html += f"""
            <tr>
                <td style="padding:6px 8px;border-bottom:1px solid #eee;font-size:18px;text-align:center;width:32px">{icon}</td>
                <td style="padding:6px 8px;border-bottom:1px solid #eee;font-size:13px">{a['mensagem']}</td>
                <td style="padding:6px 8px;border-bottom:1px solid #eee;font-size:11px;color:#999;font-family:monospace;white-space:nowrap">{hora}</td>
            </tr>"""

        integracao_html = ""
        if integracoes:
            for integ in integracoes:
                integracao_html += f"""
                <div style="padding:8px;background:#f8f8f8;border-radius:4px;margin-top:6px;font-size:12px">
                    <strong>{integ.get('tipo', 'manual')}</strong> —
                    {integ.get('municipios_consultados', 0)} consultados,
                    {integ.get('novos_detectados', 0)} novos,
                    {integ.get('legislacoes_cadastradas', 0)} legislações
                </div>"""

        tem_alertas = len(alteracoes) > 0 or len(revogacoes) > 0
        tem_novos = len(novos_municipios) > 0
        tem_erros = len(erros) > 0

        if tem_alertas:
            status = "🔔 ALTERAÇÕES DETECTADAS"
            cor = "#f39c12"
        elif tem_novos:
            status = "📍 NOVOS MUNICÍPIOS"
            cor = "#3498db"
        elif tem_erros:
            status = "⚠️ COM ERROS"
            cor = "#e74c3c"
        else:
            status = "✅ TUDO OK"
            cor = "#27ae60"

        hoje = datetime.now().strftime('%d/%m/%Y')
        assunto = f"UrbanLex — Resumo Diário {hoje} {status}"

        html = f"""<!DOCTYPE html><html><body style="font-family:Arial,sans-serif;background:#f5f5f5;padding:20px;margin:0;">
<div style="max-width:700px;margin:0 auto;background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.1);">
  <div style="background:#1a1a2e;padding:24px;text-align:center;">
    <h1 style="margin:0;color:#00e5a0;font-size:22px;">🤖 UrbanLex — Resumo Diário</h1>
    <p style="margin:8px 0 0;color:#ccc;font-size:13px;">{hoje} · {len(atividades)} atividade(s)</p>
  </div>
  <div style="padding:24px;">
    <div style="text-align:center;margin-bottom:20px;">
      <span style="display:inline-block;background:{cor};color:#fff;padding:6px 18px;border-radius:20px;font-weight:bold;font-size:14px;">{status}</span>
    </div>

    <table style="width:100%;border-collapse:collapse;margin:16px 0;">
      <tr><td style="padding:10px;border-bottom:1px solid #eee;font-weight:bold;">📍 Municípios novos</td><td style="padding:10px;border-bottom:1px solid #eee;text-align:right;font-weight:bold;color:#3498db">{len(novos_municipios)}</td></tr>
      <tr><td style="padding:10px;border-bottom:1px solid #eee;font-weight:bold;">📜 Legislações descobertas</td><td style="padding:10px;border-bottom:1px solid #eee;text-align:right">{len(legislacoes_desc)}</td></tr>
      <tr><td style="padding:10px;border-bottom:1px solid #eee;font-weight:bold;">🔔 Alterações detectadas</td><td style="padding:10px;border-bottom:1px solid #eee;text-align:right;color:#f39c12;font-weight:bold">{len(alteracoes)}</td></tr>
      <tr><td style="padding:10px;border-bottom:1px solid #eee;font-weight:bold;">🚫 Revogações</td><td style="padding:10px;border-bottom:1px solid #eee;text-align:right;color:#e74c3c">{len(revogacoes)}</td></tr>
      <tr><td style="padding:10px;border-bottom:1px solid #eee;font-weight:bold;">📝 Novas leis cadastradas</td><td style="padding:10px;border-bottom:1px solid #eee;text-align:right">{len(novas_leis)}</td></tr>
      <tr><td style="padding:10px;border-bottom:1px solid #eee;font-weight:bold;">⚠️ Erros</td><td style="padding:10px;border-bottom:1px solid #eee;text-align:right;color:#e74c3c">{len(erros)}</td></tr>
    </table>

    {integracao_html if integracao_html else ''}

    <h3 style="margin-top:24px;margin-bottom:8px;font-size:15px;">Timeline de Atividades</h3>
    <table style="width:100%;border-collapse:collapse;">
      {timeline_html}
    </table>
  </div>
  <div style="background:#f9f9f9;padding:16px;text-align:center;font-size:11px;color:#999;">
    UrbanLex v6.0 — Agente Autônomo
  </div>
</div></body></html>"""

        msg = MIMEMultipart('alternative')
        msg['Subject'] = assunto
        msg['From'] = sender
        msg['To'] = admin_email
        msg.attach(MIMEText(html, 'html'))

        with smtplib.SMTP(host, port) as srv:
            srv.starttls()
            srv.login(user, pwd)
            srv.sendmail(sender, admin_email, msg.as_string())

        logger.info(f"Email resumo diário enviado para {admin_email}")

    except Exception as e:
        logger.error(f"Erro ao enviar resumo diário: {e}")
