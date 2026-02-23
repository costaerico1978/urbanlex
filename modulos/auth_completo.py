#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
modulos/auth_completo.py
─────────────────────────
Sistema completo de autenticação:
  - Cadastro com confirmação por e-mail
  - Aprovação do admin para novos usuários
  - Roles: admin / editor / apenas_leitura
  - Ativação via link de uso único
"""

import os
import re
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
APP_URL       = os.getenv('APP_URL', 'http://localhost:5000').rstrip('/')
ADMIN_EMAIL   = os.getenv('ADMIN_EMAIL', 'costa.erico@gmail.com')


# ─────────────────────────────────────────────
# DB
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
# Migração — executa uma vez
# ─────────────────────────────────────────────

SQL_MIGRAR = """
-- Expandir tabela users com os novos campos
ALTER TABLE users
    ADD COLUMN IF NOT EXISTS nome         VARCHAR(150),
    ADD COLUMN IF NOT EXISTS status       VARCHAR(30)  DEFAULT 'pendente',
    ADD COLUMN IF NOT EXISTS token_ativ   VARCHAR(128),
    ADD COLUMN IF NOT EXISTS token_exp    TIMESTAMP,
    ADD COLUMN IF NOT EXISTS ativado_em   TIMESTAMP,
    ADD COLUMN IF NOT EXISTS aprovado_em  TIMESTAMP,
    ADD COLUMN IF NOT EXISTS aprovado_por INTEGER;

-- Atualizar role para valores mais claros
-- roles: admin | editor | apenas_leitura
-- status: pendente | aguardando_aprovacao | ativo | rejeitado

-- Tabela de tokens de aprovação (admin aprova via link no e-mail)
CREATE TABLE IF NOT EXISTS aprovacao_tokens (
    id          SERIAL PRIMARY KEY,
    user_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token       VARCHAR(128) NOT NULL UNIQUE,
    acao        VARCHAR(20)  NOT NULL,   -- 'aprovar' ou 'rejeitar'
    usado       BOOLEAN DEFAULT FALSE,
    criado_em   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_aprov_token ON aprovacao_tokens(token);
"""

def migrar_banco():
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(SQL_MIGRAR)
        conn.commit()
        logger.info("Migração auth_completo executada")
    except Exception as e:
        conn.rollback()
        logger.error(f"Erro na migração: {e}")
    finally:
        conn.close()


# ─────────────────────────────────────────────
# Validação de senha
# ─────────────────────────────────────────────

def validar_senha(senha: str) -> dict:
    """
    Regras: 6-15 chars, maiúscula, minúscula, número, especial.
    Retorna {'ok': True} ou {'ok': False, 'erro': '...'}
    """
    if len(senha) < 6:
        return {'ok': False, 'erro': 'A senha deve ter pelo menos 6 caracteres'}
    if len(senha) > 15:
        return {'ok': False, 'erro': 'A senha deve ter no máximo 15 caracteres'}
    if not re.search(r'[A-Z]', senha):
        return {'ok': False, 'erro': 'A senha deve conter pelo menos uma letra maiúscula'}
    if not re.search(r'[a-z]', senha):
        return {'ok': False, 'erro': 'A senha deve conter pelo menos uma letra minúscula'}
    if not re.search(r'[0-9]', senha):
        return {'ok': False, 'erro': 'A senha deve conter pelo menos um número'}
    if not re.search(r'[^a-zA-Z0-9]', senha):
        return {'ok': False, 'erro': 'A senha deve conter pelo menos um caractere especial (!@#$%...)'}
    return {'ok': True}


# ─────────────────────────────────────────────
# Helpers de e-mail
# ─────────────────────────────────────────────

def _enviar_email(para: str, assunto: str, html: str, texto: str) -> bool:
    host     = os.getenv('EMAIL_HOST', 'smtp.gmail.com')
    port     = int(os.getenv('EMAIL_PORT', '587'))
    user     = os.getenv('EMAIL_USER', '')
    password = os.getenv('EMAIL_PASS', os.getenv('EMAIL_PASSWORD', ''))
    from_    = os.getenv('EMAIL_FROM', f'UrbanLex <{user}>')

    if not user or not password:
        logger.warning(f"[DEV] E-mail não enviado para {para} — configure EMAIL_USER e EMAIL_PASSWORD")
        logger.info(f"[DEV] Assunto: {assunto}")
        return True  # Em dev não bloqueia

    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = assunto
        msg['From']    = from_
        msg['To']      = para
        msg.attach(MIMEText(texto, 'plain', 'utf-8'))
        msg.attach(MIMEText(html,  'html',  'utf-8'))

        with smtplib.SMTP(host, port) as s:
            s.ehlo(); s.starttls(); s.login(user, password)
            s.sendmail(user, para, msg.as_string())

        logger.info(f"E-mail enviado para {para}: {assunto}")
        return True
    except Exception as e:
        logger.error(f"Erro ao enviar e-mail para {para}: {e}")
        return False


def _header_email():
    return """
    <div style="background:linear-gradient(135deg,#111418,#181d23);
                padding:28px 32px;border-bottom:1px solid #1f2830;text-align:center">
      <div style="font-size:20px;font-weight:800;color:#e8edf2;font-family:'Helvetica Neue',Arial,sans-serif;letter-spacing:-0.5px">
        Urban<span style="color:#00e5a0">Lex</span>
      </div>
      <div style="font-size:10px;color:#6b7d8f;margin-top:4px;letter-spacing:1px">
        SISTEMA URBANÍSTICO INTEGRADO
      </div>
    </div>"""

def _footer_email():
    return """
    <div style="padding:18px 32px;border-top:1px solid #1f2830;text-align:center">
      <p style="color:#3d4f5e;font-size:10px;font-family:monospace;margin:0;letter-spacing:0.5px">
        ESTE É UM E-MAIL AUTOMÁTICO — NÃO RESPONDA
      </p>
    </div>"""

def _wrap_email(conteudo: str) -> str:
    return f"""<!DOCTYPE html>
<html lang="pt-BR"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#0a0c0f;font-family:'Helvetica Neue',Arial,sans-serif">
<table width="100%" cellpadding="0" cellspacing="0" style="padding:40px 20px">
<tr><td align="center">
<table width="520" cellpadding="0" cellspacing="0"
  style="background:#111418;border:1px solid #1f2830;border-radius:12px;overflow:hidden">
  {_header_email()}
  <tr><td style="padding:32px 40px">{conteudo}</td></tr>
  {_footer_email()}
</table>
</td></tr></table></body></html>"""


# ─────────────────────────────────────────────
# CADASTRO
# ─────────────────────────────────────────────

def cadastrar_usuario(nome: str, email: str, senha: str) -> dict:
    """
    Cadastra novo usuário.
    - Se for o ADMIN_EMAIL: status='pendente', envia e-mail de confirmação direto.
    - Outros: status='aguardando_aprovacao', notifica admin para aprovar.
    """
    email = email.strip().lower()
    nome  = nome.strip()

    # Validações básicas
    if not nome or len(nome) < 2:
        return {'ok': False, 'erro': 'Nome muito curto'}
    if not re.match(r'^[^@]+@[^@]+\.[^@]+$', email):
        return {'ok': False, 'erro': 'E-mail inválido'}

    val = validar_senha(senha)
    if not val['ok']:
        return val

    conn = get_db()
    try:
        cur = conn.cursor()

        # Verificar e-mail duplicado
        cur.execute("SELECT id FROM users WHERE email = %s", (email,))
        if cur.fetchone():
            return {'ok': False, 'erro': 'Este e-mail já está cadastrado'}

        # Hash da senha
        hash_senha = bcrypt.hashpw(senha.encode(), bcrypt.gensalt(12)).decode()

        # Definir role e status
        eh_admin = (email == ADMIN_EMAIL.lower())
        role     = 'admin'          if eh_admin else 'apenas_leitura'
        status   = 'pendente'       if eh_admin else 'aguardando_aprovacao'

        # Token de ativação (uso único, 24h)
        token    = secrets.token_hex(64)
        token_exp = datetime.now() + timedelta(hours=24)

        cur.execute("""
            INSERT INTO users
                (username, email, password_hash, role, ativo, status,
                 token_ativ, token_exp, nome)
            VALUES (%s, %s, %s, %s, FALSE, %s, %s, %s, %s)
            RETURNING id
        """, (nome, email, hash_senha, role, status, token, token_exp, nome))

        user_id = cur.fetchone()['id']
        conn.commit()

        if eh_admin:
            # Admin confirma direto por e-mail
            _enviar_confirmacao_admin(nome, email, token)
            return {'ok': True, 'tipo': 'admin', 'msg': 'Confirme seu e-mail para ativar a conta'}
        else:
            # Usuário comum → notifica admin
            _enviar_notificacao_admin(user_id, nome, email)
            return {'ok': True, 'tipo': 'usuario', 'msg': 'Cadastro enviado para aprovação do administrador'}

    except Exception as e:
        conn.rollback()
        logger.error(f"Erro no cadastro: {e}")
        return {'ok': False, 'erro': 'Erro interno — tente novamente'}
    finally:
        conn.close()


# ─────────────────────────────────────────────
# E-MAIL 1: Confirmação para o admin
# ─────────────────────────────────────────────

def _enviar_confirmacao_admin(nome: str, email: str, token: str):
    link = f"{APP_URL}/ativar/{token}"
    corpo = f"""
      <p style="color:#6b7d8f;font-size:12px;font-family:monospace;margin:0 0 16px;letter-spacing:0.5px">
        OLÁ, {nome.upper()}
      </p>
      <h2 style="color:#e8edf2;font-size:18px;font-weight:700;margin:0 0 10px">
        Confirme seu e-mail de administrador
      </h2>
      <p style="color:#6b7d8f;font-size:13px;line-height:1.7;margin:0 0 24px">
        Seu cadastro como administrador do UrbanLex foi recebido.<br>
        Clique no botão abaixo para confirmar seu e-mail e ativar sua conta.
      </p>
      <table width="100%" cellpadding="0" cellspacing="0">
        <tr><td align="center" style="padding-bottom:24px">
          <a href="{link}"
             style="display:inline-block;background:#00e5a0;color:#0a0c0f;
                    text-decoration:none;padding:13px 32px;border-radius:8px;
                    font-weight:700;font-size:14px">
            Confirmar e-mail →
          </a>
        </td></tr>
      </table>
      <div style="background:#181d23;border:1px solid #1f2830;border-radius:8px;padding:12px 16px">
        <p style="color:#f5a623;font-size:11px;font-family:monospace;margin:0;letter-spacing:0.3px">
          ⚠ LINK VÁLIDO POR 24 HORAS · USO ÚNICO
        </p>
      </div>"""

    _enviar_email(
        para=email,
        assunto='UrbanLex — Confirme seu e-mail de administrador',
        html=_wrap_email(corpo),
        texto=f"Olá {nome}, confirme seu e-mail acessando: {link}"
    )


# ─────────────────────────────────────────────
# E-MAIL 2: Notificação ao admin para aprovar usuário
# ─────────────────────────────────────────────

def _enviar_notificacao_admin(user_id: int, nome: str, email: str):
    conn = get_db()
    try:
        cur = conn.cursor()

        # Criar tokens de aprovação e rejeição
        token_sim = secrets.token_hex(32)
        token_nao = secrets.token_hex(32)

        cur.execute("""
            INSERT INTO aprovacao_tokens (user_id, token, acao)
            VALUES (%s, %s, 'aprovar'), (%s, %s, 'rejeitar')
        """, (user_id, token_sim, user_id, token_nao))
        conn.commit()

        link_sim = f"{APP_URL}/admin/aprovar/{token_sim}"
        link_nao = f"{APP_URL}/admin/rejeitar/{token_nao}"

        corpo = f"""
          <p style="color:#6b7d8f;font-size:12px;font-family:monospace;margin:0 0 16px;letter-spacing:0.5px">
            NOVA SOLICITAÇÃO DE ACESSO
          </p>
          <h2 style="color:#e8edf2;font-size:18px;font-weight:700;margin:0 0 16px">
            Usuário solicita acesso ao UrbanLex
          </h2>
          <div style="background:#181d23;border:1px solid #2a3540;border-radius:8px;
                      padding:16px 20px;margin-bottom:24px">
            <table width="100%" cellpadding="0" cellspacing="0">
              <tr>
                <td style="color:#6b7d8f;font-size:11px;font-family:monospace;
                           padding-bottom:8px;letter-spacing:0.5px">NOME</td>
                <td style="color:#e8edf2;font-size:14px;font-weight:600;
                           padding-bottom:8px;text-align:right">{nome}</td>
              </tr>
              <tr>
                <td style="color:#6b7d8f;font-size:11px;font-family:monospace;
                           letter-spacing:0.5px">E-MAIL</td>
                <td style="color:#e8edf2;font-size:14px;text-align:right">{email}</td>
              </tr>
            </table>
          </div>
          <p style="color:#6b7d8f;font-size:13px;margin:0 0 20px">
            Deseja conceder acesso a este usuário?
          </p>
          <table width="100%" cellpadding="0" cellspacing="0">
            <tr>
              <td width="48%" align="center">
                <a href="{link_sim}"
                   style="display:block;background:#00e5a0;color:#0a0c0f;
                          text-decoration:none;padding:13px;border-radius:8px;
                          font-weight:700;font-size:14px;text-align:center">
                  ✓ Sim, aprovar
                </a>
              </td>
              <td width="4%"></td>
              <td width="48%" align="center">
                <a href="{link_nao}"
                   style="display:block;background:rgba(255,77,109,0.15);color:#ff4d6d;
                          border:1px solid rgba(255,77,109,0.3);
                          text-decoration:none;padding:13px;border-radius:8px;
                          font-weight:700;font-size:14px;text-align:center">
                  ✗ Não, rejeitar
                </a>
              </td>
            </tr>
          </table>
          <p style="color:#3d4f5e;font-size:11px;margin:20px 0 0;text-align:center">
            Você também pode gerenciar usuários em
            <a href="{APP_URL}/usuarios" style="color:#6b7d8f">{APP_URL}/usuarios</a>
          </p>"""

        _enviar_email(
            para=ADMIN_EMAIL,
            assunto=f'UrbanLex — {nome} quer acesso ao sistema',
            html=_wrap_email(corpo),
            texto=f"Usuário {nome} ({email}) quer acesso. Aprovar: {link_sim} | Rejeitar: {link_nao}"
        )
    finally:
        conn.close()


# ─────────────────────────────────────────────
# ATIVAÇÃO DE CONTA (admin + usuários aprovados)
# ─────────────────────────────────────────────

def ativar_conta(token: str) -> dict:
    """
    Ativa a conta via token enviado por e-mail.
    Funciona tanto para admin quanto para usuários aprovados.
    """
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT * FROM users
            WHERE token_ativ = %s AND ativo = FALSE
        """, (token,))
        user = cur.fetchone()

        if not user:
            return {'ok': False, 'erro': 'Link inválido ou já utilizado'}

        if datetime.now() > user['token_exp']:
            return {'ok': False, 'erro': 'Link expirado — solicite um novo cadastro ou contate o administrador'}

        if user['status'] not in ('pendente', 'aprovado'):
            return {'ok': False, 'erro': 'Conta não está apta para ativação'}

        cur.execute("""
            UPDATE users
            SET ativo = TRUE, status = 'ativo',
                token_ativ = NULL, token_exp = NULL,
                ativado_em = NOW()
            WHERE id = %s
        """, (user['id'],))
        conn.commit()

        logger.info(f"Conta ativada: {user['email']}")
        return {'ok': True, 'nome': user['nome'], 'email': user['email'], 'role': user['role']}

    except Exception as e:
        conn.rollback()
        logger.error(f"Erro ao ativar conta: {e}")
        return {'ok': False, 'erro': 'Erro interno'}
    finally:
        conn.close()


# ─────────────────────────────────────────────
# APROVAÇÃO / REJEIÇÃO pelo admin (via link no e-mail)
# ─────────────────────────────────────────────

def processar_aprovacao(token_aprovacao: str) -> dict:
    """
    Admin clica em "Sim" ou "Não" no e-mail → chega aqui.
    """
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT a.*, u.nome, u.email
            FROM aprovacao_tokens a
            JOIN users u ON u.id = a.user_id
            WHERE a.token = %s AND a.usado = FALSE
        """, (token_aprovacao,))
        reg = cur.fetchone()

        if not reg:
            return {'ok': False, 'erro': 'Link de aprovação inválido ou já utilizado'}

        # Marcar token como usado
        cur.execute("UPDATE aprovacao_tokens SET usado = TRUE WHERE id = %s", (reg['id'],))

        if reg['acao'] == 'aprovar':
            # Gerar token de ativação para o usuário
            token_ativ = secrets.token_hex(64)
            token_exp  = datetime.now() + timedelta(hours=48)

            cur.execute("""
                UPDATE users
                SET status = 'aprovado', token_ativ = %s, token_exp = %s, aprovado_em = NOW()
                WHERE id = %s
            """, (token_ativ, token_exp, reg['user_id']))
            conn.commit()

            # Enviar e-mail de ativação para o usuário
            _enviar_aprovacao_usuario(reg['nome'], reg['email'], token_ativ)

            return {'ok': True, 'acao': 'aprovado', 'nome': reg['nome'], 'email': reg['email']}

        else:  # rejeitar
            cur.execute("""
                UPDATE users SET status = 'rejeitado' WHERE id = %s
            """, (reg['user_id'],))
            conn.commit()

            _enviar_rejeicao_usuario(reg['nome'], reg['email'])

            return {'ok': True, 'acao': 'rejeitado', 'nome': reg['nome'], 'email': reg['email']}

    except Exception as e:
        conn.rollback()
        logger.error(f"Erro na aprovação: {e}")
        return {'ok': False, 'erro': str(e)}
    finally:
        conn.close()


# ─────────────────────────────────────────────
# E-MAIL 3: Usuário aprovado
# ─────────────────────────────────────────────

def _enviar_aprovacao_usuario(nome: str, email: str, token: str):
    link = f"{APP_URL}/ativar/{token}"
    corpo = f"""
      <p style="color:#6b7d8f;font-size:12px;font-family:monospace;margin:0 0 16px;letter-spacing:0.5px">
        OLÁ, {nome.upper()}
      </p>
      <h2 style="color:#e8edf2;font-size:18px;font-weight:700;margin:0 0 10px">
        🎉 Seu acesso foi aprovado!
      </h2>
      <p style="color:#6b7d8f;font-size:13px;line-height:1.7;margin:0 0 24px">
        O administrador aprovou seu cadastro no UrbanLex.<br>
        Clique no botão abaixo para ativar sua conta e começar a usar o sistema.
      </p>
      <table width="100%" cellpadding="0" cellspacing="0">
        <tr><td align="center" style="padding-bottom:24px">
          <a href="{link}"
             style="display:inline-block;background:#00e5a0;color:#0a0c0f;
                    text-decoration:none;padding:13px 32px;border-radius:8px;
                    font-weight:700;font-size:14px">
            Ativar minha conta →
          </a>
        </td></tr>
      </table>
      <div style="background:#181d23;border:1px solid #1f2830;border-radius:8px;padding:12px 16px">
        <p style="color:#f5a623;font-size:11px;font-family:monospace;margin:0;letter-spacing:0.3px">
          ⚠ LINK VÁLIDO POR 48 HORAS · USO ÚNICO
        </p>
      </div>"""

    _enviar_email(
        para=email,
        assunto='UrbanLex — Sua conta foi aprovada! Ative agora',
        html=_wrap_email(corpo),
        texto=f"Olá {nome}! Seu acesso foi aprovado. Ative em: {link}"
    )


# ─────────────────────────────────────────────
# E-MAIL 4: Usuário rejeitado
# ─────────────────────────────────────────────

def _enviar_rejeicao_usuario(nome: str, email: str):
    corpo = f"""
      <p style="color:#6b7d8f;font-size:12px;font-family:monospace;margin:0 0 16px;letter-spacing:0.5px">
        OLÁ, {nome.upper()}
      </p>
      <h2 style="color:#e8edf2;font-size:18px;font-weight:700;margin:0 0 10px">
        Solicitação de acesso
      </h2>
      <p style="color:#6b7d8f;font-size:13px;line-height:1.7;margin:0">
        Infelizmente sua solicitação de acesso ao UrbanLex não foi aprovada
        pelo administrador do sistema.<br><br>
        Se acredita que houve um engano, entre em contato diretamente com o administrador.
      </p>"""

    _enviar_email(
        para=email,
        assunto='UrbanLex — Solicitação de acesso',
        html=_wrap_email(corpo),
        texto=f"Olá {nome}, sua solicitação de acesso ao UrbanLex não foi aprovada."
    )


# ─────────────────────────────────────────────
# GERENCIAMENTO DE USUÁRIOS (painel admin)
# ─────────────────────────────────────────────

def listar_usuarios() -> list:
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, nome, email, role, status, ativo,
                   criado_em, ativado_em, aprovado_em
            FROM users
            ORDER BY criado_em DESC
        """)
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()

def alterar_role(user_id: int, novo_role: str, admin_id: int) -> dict:
    if novo_role not in ('apenas_leitura', 'editor', 'admin'):
        return {'ok': False, 'erro': 'Role inválida'}

    conn = get_db()
    try:
        cur = conn.cursor()
        # Proteção: não pode alterar a própria role
        if user_id == admin_id:
            return {'ok': False, 'erro': 'Você não pode alterar sua própria role'}
        cur.execute("UPDATE users SET role = %s WHERE id = %s", (novo_role, user_id))
        conn.commit()
        return {'ok': True}
    except Exception as e:
        conn.rollback()
        return {'ok': False, 'erro': str(e)}
    finally:
        conn.close()

def excluir_usuario(user_id: int, admin_id: int) -> dict:
    if user_id == admin_id:
        return {'ok': False, 'erro': 'Você não pode excluir sua própria conta'}

    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM users WHERE id = %s", (user_id,))
        conn.commit()
        return {'ok': True}
    except Exception as e:
        conn.rollback()
        return {'ok': False, 'erro': str(e)}
    finally:
        conn.close()
