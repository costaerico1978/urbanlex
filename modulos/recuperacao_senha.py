#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
modulos/recuperacao_senha.py
─────────────────────────────
Recuperação de senha por e-mail com token temporário.

Variáveis de ambiente necessárias (adicionar no Railway):
  EMAIL_HOST      — ex: smtp.gmail.com
  EMAIL_PORT      — ex: 587
  EMAIL_USER      — ex: sistema@seudominio.com.br
  EMAIL_PASSWORD  — senha do e-mail (ou App Password do Gmail)
  EMAIL_FROM      — ex: UrbanLex <sistema@seudominio.com.br>
  APP_URL         — ex: https://meu-sistema.railway.app
"""

import os
import secrets
import smtplib
import logging
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import psycopg2
from psycopg2.extras import RealDictCursor
import bcrypt

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv('DATABASE_URL')


# ─────────────────────────────────────────────
# Conexão
# ─────────────────────────────────────────────

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


# ─────────────────────────────────────────────
# Criar tabela de tokens (rode no inicializar_banco.py)
# ─────────────────────────────────────────────

SQL_CRIAR_TABELA = """
CREATE TABLE IF NOT EXISTS password_reset_tokens (
    id          SERIAL PRIMARY KEY,
    user_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token       VARCHAR(128) NOT NULL UNIQUE,
    expires_at  TIMESTAMP NOT NULL,
    usado       BOOLEAN DEFAULT FALSE,
    criado_em   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_reset_token ON password_reset_tokens(token);
"""

def criar_tabela_tokens():
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(SQL_CRIAR_TABELA)
        conn.commit()
    finally:
        conn.close()


# ─────────────────────────────────────────────
# Gerar e salvar token
# ─────────────────────────────────────────────

def gerar_token_reset(email: str) -> dict:
    """
    Verifica se o e-mail existe, gera token seguro com validade de 1 hora.
    Retorna {'ok': True, 'token': '...', 'username': '...'} ou {'ok': False, 'erro': '...'}
    """
    conn = get_db()
    try:
        cur = conn.cursor()

        # Buscar usuário
        cur.execute(
            "SELECT id, username, email FROM users WHERE email = %s AND ativo = TRUE",
            (email.strip().lower(),)
        )
        user = cur.fetchone()

        if not user:
            # Retornamos ok=True mesmo assim para não revelar se o e-mail existe
            logger.info(f"Tentativa de reset para e-mail não cadastrado: {email}")
            return {'ok': True, 'enviado': False}

        # Invalidar tokens anteriores deste usuário
        cur.execute(
            "UPDATE password_reset_tokens SET usado = TRUE WHERE user_id = %s AND usado = FALSE",
            (user['id'],)
        )

        # Gerar token criptograficamente seguro (64 bytes hex = 128 chars)
        token = secrets.token_hex(64)
        expires_at = datetime.now() + timedelta(hours=1)

        cur.execute("""
            INSERT INTO password_reset_tokens (user_id, token, expires_at)
            VALUES (%s, %s, %s)
        """, (user['id'], token, expires_at))

        conn.commit()
        logger.info(f"Token de reset gerado para user_id={user['id']}")

        return {
            'ok': True,
            'enviado': True,
            'token': token,
            'username': user['username'],
            'email': user['email'],
            'expires_at': expires_at
        }

    except Exception as e:
        conn.rollback()
        logger.error(f"Erro ao gerar token: {e}")
        return {'ok': False, 'erro': str(e)}
    finally:
        conn.close()


# ─────────────────────────────────────────────
# Validar token
# ─────────────────────────────────────────────

def validar_token(token: str) -> dict:
    """
    Verifica se o token é válido, não expirado e não usado.
    Retorna {'valido': True, 'user_id': ..., 'username': ...} ou {'valido': False, 'motivo': ...}
    """
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT t.*, u.username, u.email
            FROM password_reset_tokens t
            JOIN users u ON u.id = t.user_id
            WHERE t.token = %s
        """, (token,))
        registro = cur.fetchone()

        if not registro:
            return {'valido': False, 'motivo': 'Token inválido'}

        if registro['usado']:
            return {'valido': False, 'motivo': 'Token já utilizado'}

        if datetime.now() > registro['expires_at']:
            return {'valido': False, 'motivo': 'Token expirado (válido por 1 hora)'}

        return {
            'valido': True,
            'user_id': registro['user_id'],
            'username': registro['username'],
            'email': registro['email'],
            'token': token
        }

    finally:
        conn.close()


# ─────────────────────────────────────────────
# Redefinir senha
# ─────────────────────────────────────────────

def redefinir_senha(token: str, nova_senha: str) -> dict:
    """
    Valida o token e atualiza a senha do usuário.
    Marca o token como usado.
    """
    # Validações básicas da senha
    if len(nova_senha) < 8:
        return {'ok': False, 'erro': 'A senha deve ter pelo menos 8 caracteres'}

    validacao = validar_token(token)
    if not validacao['valido']:
        return {'ok': False, 'erro': validacao['motivo']}

    conn = get_db()
    try:
        cur = conn.cursor()

        # Hash da nova senha
        hash_senha = bcrypt.hashpw(nova_senha.encode(), bcrypt.gensalt(12)).decode()

        # Atualizar senha
        cur.execute(
            "UPDATE users SET password_hash = %s WHERE id = %s",
            (hash_senha, validacao['user_id'])
        )

        # Marcar token como usado
        cur.execute(
            "UPDATE password_reset_tokens SET usado = TRUE WHERE token = %s",
            (token,)
        )

        conn.commit()
        logger.info(f"Senha redefinida com sucesso para user_id={validacao['user_id']}")

        return {'ok': True, 'username': validacao['username']}

    except Exception as e:
        conn.rollback()
        logger.error(f"Erro ao redefinir senha: {e}")
        return {'ok': False, 'erro': str(e)}
    finally:
        conn.close()


# ─────────────────────────────────────────────
# Enviar e-mail
# ─────────────────────────────────────────────

def enviar_email_reset(email: str, username: str, token: str) -> dict:
    """
    Envia o e-mail com o link de recuperação.
    """
    app_url  = os.getenv('APP_URL', 'http://localhost:5000').rstrip('/')
    link     = f"{app_url}/reset-senha/{token}"

    host     = os.getenv('EMAIL_HOST', 'smtp.gmail.com')
    port     = int(os.getenv('EMAIL_PORT', '587'))
    user     = os.getenv('EMAIL_USER', '')
    password = os.getenv('EMAIL_PASS', os.getenv('EMAIL_PASSWORD', ''))
    from_    = os.getenv('EMAIL_FROM', f'UrbanLex <{user}>')

    if not user or not password:
        logger.warning("EMAIL_USER ou EMAIL_PASSWORD não configurados — e-mail não enviado")
        return {'ok': False, 'erro': 'E-mail não configurado no servidor'}

    # ── HTML do e-mail ──────────────────────────────────────────────
    html = f"""
<!DOCTYPE html>
<html lang="pt-BR">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#0a0c0f;font-family:'Helvetica Neue',Arial,sans-serif">
<table width="100%" cellpadding="0" cellspacing="0" style="padding:40px 20px">
<tr><td align="center">
<table width="520" cellpadding="0" cellspacing="0"
    style="background:#111418;border:1px solid #1f2830;border-radius:12px;overflow:hidden">

  <!-- Cabeçalho -->
  <tr>
    <td style="background:linear-gradient(135deg,#111418,#181d23);padding:32px;border-bottom:1px solid #1f2830;text-align:center">
      <div style="font-size:22px;font-weight:800;color:#e8edf2;letter-spacing:-0.5px">
        Urban<span style="color:#00e5a0">Lex</span>
      </div>
      <div style="font-size:11px;color:#6b7d8f;margin-top:4px;letter-spacing:0.5px">
        SISTEMA URBANÍSTICO INTEGRADO
      </div>
    </td>
  </tr>

  <!-- Corpo -->
  <tr>
    <td style="padding:36px 40px">
      <p style="color:#6b7d8f;font-size:12px;font-family:monospace;margin:0 0 20px;letter-spacing:0.5px">
        OLÁ, {username.upper()}
      </p>
      <h1 style="color:#e8edf2;font-size:20px;font-weight:700;margin:0 0 12px;letter-spacing:-0.3px">
        Redefinição de senha
      </h1>
      <p style="color:#6b7d8f;font-size:14px;line-height:1.7;margin:0 0 28px">
        Recebemos uma solicitação para redefinir a senha da sua conta.<br>
        Clique no botão abaixo para criar uma nova senha.
      </p>

      <!-- Botão -->
      <table width="100%" cellpadding="0" cellspacing="0">
        <tr>
          <td align="center" style="padding-bottom:28px">
            <a href="{link}"
               style="display:inline-block;background:#00e5a0;color:#0a0c0f;
                      text-decoration:none;padding:14px 32px;border-radius:8px;
                      font-weight:700;font-size:14px;letter-spacing:0.2px">
              Redefinir minha senha →
            </a>
          </td>
        </tr>
      </table>

      <!-- Aviso expiração -->
      <div style="background:#181d23;border:1px solid #1f2830;border-radius:8px;padding:14px 16px;margin-bottom:24px">
        <p style="color:#f5a623;font-size:12px;font-family:monospace;margin:0;letter-spacing:0.3px">
          ⚠ LINK VÁLIDO POR APENAS 1 HORA
        </p>
        <p style="color:#6b7d8f;font-size:12px;margin:6px 0 0">
          Após esse prazo, você precisará solicitar um novo link.
        </p>
      </div>

      <!-- Link manual -->
      <p style="color:#3d4f5e;font-size:11px;margin:0;line-height:1.6">
        Se o botão não funcionar, copie e cole no navegador:<br>
        <span style="color:#6b7d8f;word-break:break-all">{link}</span>
      </p>
    </td>
  </tr>

  <!-- Rodapé -->
  <tr>
    <td style="padding:20px 40px;border-top:1px solid #1f2830">
      <p style="color:#3d4f5e;font-size:11px;font-family:monospace;margin:0;text-align:center">
        SE NÃO FOI VOCÊ — IGNORE ESTE E-MAIL.<br>
        SUA SENHA PERMANECE INALTERADA.
      </p>
    </td>
  </tr>

</table>
</td></tr>
</table>
</body>
</html>
"""

    texto_simples = f"""
UrbanLex — Redefinição de senha

Olá, {username}!

Recebemos uma solicitação para redefinir sua senha.
Acesse o link abaixo para criar uma nova (válido por 1 hora):

{link}

Se não foi você, ignore este e-mail. Sua senha permanece inalterada.
"""

    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = 'UrbanLex — Redefinição de senha'
        msg['From']    = from_
        msg['To']      = email

        msg.attach(MIMEText(texto_simples, 'plain', 'utf-8'))
        msg.attach(MIMEText(html, 'html', 'utf-8'))

        with smtplib.SMTP(host, port) as server:
            server.ehlo()
            server.starttls()
            server.login(user, password)
            server.sendmail(user, email, msg.as_string())

        logger.info(f"E-mail de reset enviado para {email}")
        return {'ok': True}

    except smtplib.SMTPAuthenticationError:
        logger.error("Falha de autenticação SMTP")
        return {'ok': False, 'erro': 'Falha de autenticação no servidor de e-mail'}
    except Exception as e:
        logger.error(f"Erro ao enviar e-mail: {e}")
        return {'ok': False, 'erro': str(e)}
