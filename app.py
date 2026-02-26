"""
UrbanLex — Aplicação Flask Principal v3.5
Parâmetros Urbanísticos + Biblioteca Legislativa + Monitoramento IA

CORREÇÕES APLICADAS (25/02/2026):
  BUG 1: DELETE legislação usava id() em vez de leg_id + foreign keys
  BUG 2: Login API tinha sessão como código morto (indentação)
  BUG 3: Login HTML mostrava erro no GET
  BUG 4: Rotas diagnóstico sem autenticação
  BUG 5: Rotas FIX 6-10 movidas para antes do if __name__
  BUG 6: inicializar() chamada no nível do módulo
  BUG 7: Query alterações usa COALESCE para municipio_nome
  BUG 8: Adicionada rota GET /logout
  BUG 9: Login HTML usa qry() em vez de conexão manual
"""

import os, io, sys, json, hashlib, threading
from pathlib import Path
from datetime import datetime, timedelta
from functools import wraps

from flask import Flask, render_template, request, jsonify, redirect, url_for, send_file, session

sys.path.insert(0, str(Path(__file__).parent.parent))

import psycopg2
import psycopg2.extras
from werkzeug.utils import secure_filename

# Módulos internos
# ── Auth helpers (adapters para compatibilidade) ──────────────
try:
    import bcrypt as _bcrypt
    def hash_senha(s): return _bcrypt.hashpw(s.encode(), _bcrypt.gensalt(12)).decode()
    def verificar_senha(s, h):
        try: return _bcrypt.checkpw(s.encode(), h.encode())
        except: return False
except ImportError:
    import hashlib
    def hash_senha(s): return hashlib.sha256(s.encode()).hexdigest()
    def verificar_senha(s, h): return hash_senha(s) == h

import secrets as _secrets
def gerar_token(): return _secrets.token_urlsafe(32)

def _get_email_cfg():
    import smtplib, ssl
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    host  = os.getenv('EMAIL_HOST','smtp.gmail.com')
    port  = int(os.getenv('EMAIL_PORT','587'))
    user  = os.getenv('EMAIL_USER','')
    pwd   = os.getenv('EMAIL_PASS','')
    frm   = os.getenv('EMAIL_FROM', user)
    return host, port, user, pwd, frm

def enviar_email_generico(para, assunto, html):
    try:
        import smtplib
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText
        host, port, user, pwd, frm = _get_email_cfg()
        if not user: return
        msg = MIMEMultipart('alternative')
        msg['Subject'] = assunto; msg['From'] = frm; msg['To'] = para
        msg.attach(MIMEText(html, 'html'))
        with smtplib.SMTP(host, port) as srv:
            srv.starttls(); srv.login(user, pwd); srv.sendmail(frm, para, msg.as_string())
    except Exception as e:
        print(f"[EMAIL ERROR] {e}")


def get_app_url():
    """Retorna a URL base da aplicação, com fallback para variável de ambiente."""
    url = os.getenv('APP_URL', '')
    if not url:
        try:
            from flask import request
            url = request.host_url.rstrip('/')
        except:
            url = 'http://localhost:5000'
    return url.rstrip('/')

def enviar_email_ativacao(user, token):
    url = f"{get_app_url()}/ativar/{token}"
    enviar_email_generico(user['email'], 'Ative sua conta UrbanLex',
        f'<p>Olá {user["nome"]},</p><p>Clique para ativar sua conta:</p>'
        f'<p><a href="{url}">{url}</a></p><p>Link válido por 24 horas.</p>')

def enviar_email_aprovacao_admin(user):
    app_url = get_app_url()
    token_apr = gerar_token()
    exp = datetime.now() + timedelta(days=7)
    qry("INSERT INTO aprovacao_tokens (user_id,token,tipo,expira_em) VALUES (%s,%s,'aprovacao',%s)",
        (user['id'], token_apr, exp), commit=True)
    admin_email = os.getenv('ADMIN_EMAIL','')
    if not admin_email: return
    url_apr = f"{app_url}/admin/aprovar/{token_apr}"
    url_rej = f"{app_url}/admin/rejeitar/{token_apr}"
    enviar_email_generico(admin_email, f'Novo cadastro: {user["nome"]}',
        f'<p>Novo usuário aguardando aprovação: <strong>{user["nome"]}</strong> ({user["email"]})</p>'
        f'<p><a href="{url_apr}">✅ Aprovar</a> &nbsp; <a href="{url_rej}">❌ Rejeitar</a></p>')

def enviar_email_boas_vindas(user):
    enviar_email_generico(user['email'], 'Bem-vindo ao UrbanLex!',
        f'<p>Olá {user["nome"]}, sua conta foi aprovada! Acesse: {get_app_url()}/login</p>')

def enviar_email_rejeicao(user):
    enviar_email_generico(user['email'], 'Cadastro UrbanLex',
        f'<p>Olá {user["nome"]}, seu cadastro não foi aprovado. Entre em contato com o administrador.</p>')

def enviar_email_reset(user, token):
    url = f"{get_app_url()}/reset-senha/{token}"
    enviar_email_generico(user['email'], 'Redefinição de senha — UrbanLex',
        f'<p>Olá {user["nome"]},</p><p>Clique para redefinir sua senha:</p>'
        f'<p><a href="{url}">{url}</a></p><p>Link válido por 1 hora.</p>')

try:
    from modulos.scheduler_integrado import iniciar_scheduler
    SCHEDULER_OK = True
except ImportError:
    SCHEDULER_OK = False

# ── Cloudflare R2 storage (opcional — ativo somente se variáveis configuradas)
try:
    from modulos.storage_r2 import upload_arquivo as r2_upload, deletar_arquivo as r2_delete, \
                                    gerar_url_assinada as r2_url_assinada, r2_disponivel, \
                                    download_arquivo as r2_download
    _R2_IMPORTADO = True
except ImportError:
    _R2_IMPORTADO = False
    def r2_disponivel(): return False
    def r2_upload(*a, **kw): return None
    def r2_delete(*a, **kw): return False
    def r2_url_assinada(*a, **kw): return None
    def r2_download(*a, **kw): return None

# Conversão de arquivos
try: import PyMuPDF as fitz; PDF_OK = True
except: 
    try: import fitz; PDF_OK = True
    except: PDF_OK = False

try: from docx import Document as DocxDoc; DOCX_OK = True
except: DOCX_OK = False

try: import pytesseract; from PIL import Image; OCR_OK = True
except: OCR_OK = False

try: import pandas as pd; PANDAS_OK = True
except: PANDAS_OK = False

# ── App ────────────────────────────────────────────────────────
app = Flask(__name__, template_folder='templates')
app.secret_key = os.getenv('SECRET_KEY', 'urbanlex-dev-key-change-in-prod')
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100MB
app.config['SESSION_COOKIE_SECURE'] = True
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['SESSION_COOKIE_HTTPONLY'] = True
ADMIN_EMAIL = os.getenv('ADMIN_EMAIL', '')

# ── DB ─────────────────────────────────────────────────────────
def get_db():
    conn = psycopg2.connect(os.environ['DATABASE_URL'])
    conn.autocommit = False
    return conn

def qry(sql, params=None, fetch='all', commit=False):
    conn = get_db()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, params or ())
        result = None
        if fetch == 'all': result = [dict(r) for r in cur.fetchall()]
        elif fetch == 'one': row = cur.fetchone(); result = dict(row) if row else None
        elif fetch == 'id': result = cur.fetchone()[0] if cur.rowcount > 0 else None
        if commit: conn.commit()
        return result
    finally:
        conn.close()

# ── Auth helpers ───────────────────────────────────────────────
def login_required(f):
    @wraps(f)
    def dec(*a, **k):
        if 'user_id' not in session:
            if request.path.startswith('/api/'): return jsonify({'error':'Não autenticado'}), 401
            return redirect('/login')
        return f(*a, **k)
    return dec

def admin_required(f):
    @wraps(f)
    def dec(*a, **k):
        if 'user_id' not in session:
            if request.path.startswith('/api/'): return jsonify({'error':'Não autenticado'}), 401
            return redirect('/login')
        if session.get('role') != 'admin':
            if request.path.startswith('/api/'): return jsonify({'error':'Acesso negado'}), 403
            return redirect('/')
        return f(*a, **k)
    return dec

def editor_required(f):
    @wraps(f)
    def dec(*a, **k):
        if 'user_id' not in session:
            if request.path.startswith('/api/'): return jsonify({'error':'Não autenticado'}), 401
            return redirect('/login')
        if session.get('role') not in ('admin','editor'):
            if request.path.startswith('/api/'): return jsonify({'error':'Acesso negado'}), 403
            return redirect('/')
        return f(*a, **k)
    return dec

def tmpl_ctx():
    return {
        'username': session.get('nome',''),
        'role': session.get('role',''),
        'is_admin': session.get('role') == 'admin'
    }

def extrair_texto_arquivo(arquivo_bytes, nome_arquivo):
    ext = Path(nome_arquivo).suffix.lower()
    texto = ''
    try:
        if ext == '.pdf' and PDF_OK:
            doc = fitz.open(stream=arquivo_bytes, filetype='pdf')
            texto = '\n'.join(p.get_text() for p in doc)
        elif ext in ('.doc','.docx') and DOCX_OK:
            doc = DocxDoc(io.BytesIO(arquivo_bytes))
            texto = '\n'.join(p.text for p in doc.paragraphs)
        elif ext in ('.jpg','.jpeg','.png') and OCR_OK:
            img = Image.open(io.BytesIO(arquivo_bytes))
            texto = pytesseract.image_to_string(img, lang='por')
        elif ext in ('.xls','.xlsx') and PANDAS_OK:
            df = pd.read_excel(io.BytesIO(arquivo_bytes))
            texto = df.to_string()
        elif ext == '.txt':
            texto = arquivo_bytes.decode('utf-8', errors='ignore')
    except Exception as e:
        texto = f'[Erro ao extrair texto: {e}]'
    return texto


# ── Função auxiliar de busca com IA (FIX 5: movida para antes do if __name__) ──
def _buscar_legislacao_internet(consulta: str) -> dict:
    """Tenta encontrar legislação via GROQ ou busca simples."""
    try:
        from groq import Groq
        client = Groq(api_key=os.getenv('GROQ_API_KEY',''))
        prompt = (f"Encontre a seguinte legislação urbanística brasileira: '{consulta}'. "
                  "Retorne APENAS um JSON com: titulo, estado, municipio, numero, ano, url. "
                  "Se não encontrar, retorne {}.")
        resp = client.chat.completions.create(
            model="llama3-8b-8192",
            messages=[{"role":"user","content":prompt}],
            max_tokens=300
        )
        text = resp.choices[0].message.content.strip()
        import re
        m = re.search(r'\{.*\}', text, re.DOTALL)
        if m:
            result = json.loads(m.group(0))
            if result.get('titulo') or result.get('url'):
                return result
    except Exception as e:
        print(f"[BUSCA IA] {e}")
    return {}


# ═══════════════════════════════════════════════════════════════
# PÁGINAS HTML
# ═══════════════════════════════════════════════════════════════

# ── FIX 3: Login corrigido — erro só aparece no POST com falha ──
@app.route('/login', methods=['GET','POST'])
def login_page():
    if 'user_id' in session: return redirect('/')
    error = None
    if request.method == 'POST':
        email = request.form.get('email','').strip().lower()
        senha = request.form.get('senha','')
        user = qry("SELECT * FROM users WHERE email=%s AND ativo=TRUE AND aprovado=TRUE", (email,), 'one')
        if user and verificar_senha(senha, user['senha_hash']):
            session['user_id'] = user['id']
            session['nome'] = user['nome']
            session['email'] = user['email']
            session['role'] = user['role']
            # FIX 9: usar qry() em vez de conexão manual
            qry("UPDATE users SET ultimo_acesso=NOW() WHERE id=%s", (user['id'],), commit=True, fetch=None)
            return redirect('/')
        error = 'E-mail ou senha incorretos'  # ← Agora só no POST com falha
    return render_template('login.html', error=error, **tmpl_ctx())

@app.route('/cadastro')
def pagina_cadastro(): return render_template('cadastro.html', **tmpl_ctx())

@app.route('/esqueci-senha')
def pagina_esqueci_senha(): return render_template('esqueci_senha.html', **tmpl_ctx())

@app.route('/reset-senha/<token>')
def pagina_reset_senha(token):
    tk = qry("SELECT * FROM password_reset_tokens WHERE token=%s AND usado=FALSE AND expira_em>NOW()", (token,), 'one')
    if not tk:
        return render_template('reset_senha.html', token=token,
            erro='Este link é inválido ou já expirou. Solicite um novo.', **tmpl_ctx())
    return render_template('reset_senha.html', token=token, erro=None, **tmpl_ctx())

@app.route('/ativar/<token>')
def pagina_ativar(token):
    tk = qry("SELECT * FROM aprovacao_tokens WHERE token=%s AND tipo='ativacao' AND usado=FALSE AND expira_em>NOW()", (token,), 'one')
    if not tk: return render_template('conta_ativada.html', sucesso=False, msg='Link inválido ou expirado.', **tmpl_ctx())
    qry("UPDATE users SET ativo=TRUE WHERE id=%s", (tk['user_id'],), commit=True)
    qry("UPDATE aprovacao_tokens SET usado=TRUE WHERE id=%s", (tk['id'],), commit=True)
    user = qry("SELECT * FROM users WHERE id=%s", (tk['user_id'],), 'one')
    if ADMIN_EMAIL: enviar_email_aprovacao_admin(user)
    return render_template('conta_ativada.html', sucesso=True,
        msg='Conta ativada! Aguardando aprovação do administrador.',
        nome=user.get('nome',''), email=user.get('email',''), **tmpl_ctx())

@app.route('/admin/aprovar/<token>')
def admin_aprovar(token):
    tk = qry("SELECT * FROM aprovacao_tokens WHERE token=%s AND tipo='aprovacao' AND usado=FALSE", (token,), 'one')
    if not tk: return render_template('resultado_aprovacao.html', sucesso=False, msg='Link inválido.', **tmpl_ctx())
    qry("UPDATE users SET aprovado=TRUE WHERE id=%s", (tk['user_id'],), commit=True)
    qry("UPDATE aprovacao_tokens SET usado=TRUE WHERE id=%s", (tk['id'],), commit=True)
    user = qry("SELECT * FROM users WHERE id=%s", (tk['user_id'],), 'one')
    if not user:
        return render_template('resultado_aprovacao.html', sucesso=False, msg='Usuário não encontrado.', nome='', email='', **tmpl_ctx())
    enviar_email_boas_vindas(user)
    return render_template('resultado_aprovacao.html', sucesso=True,
        msg=f'Usuário {user["nome"]} aprovado com sucesso!',
        nome=user.get('nome',''), email=user.get('email',''), **tmpl_ctx())

@app.route('/admin/rejeitar/<token>')
def admin_rejeitar(token):
    tk = qry("SELECT * FROM aprovacao_tokens WHERE token=%s AND tipo='aprovacao' AND usado=FALSE", (token,), 'one')
    if not tk: return render_template('resultado_aprovacao.html', sucesso=False, msg='Link inválido.', **tmpl_ctx())
    qry("UPDATE users SET aprovado=FALSE, ativo=FALSE WHERE id=%s", (tk['user_id'],), commit=True)
    qry("UPDATE aprovacao_tokens SET usado=TRUE WHERE id=%s", (tk['id'],), commit=True)
    user = qry("SELECT * FROM users WHERE id=%s", (tk['user_id'],), 'one')
    if user: enviar_email_rejeicao(user)
    nome_rej = user.get('nome','usuário') if user else 'usuário'
    return render_template('resultado_aprovacao.html', sucesso=False,
        msg=f'Cadastro de {nome_rej} rejeitado.',
        nome=user.get('nome','') if user else '', email=user.get('email','') if user else '', **tmpl_ctx())

@app.route('/')
@login_required
def index(): return render_template('dashboard.html', active_page='dashboard', active_group='', **tmpl_ctx())

@app.route('/legislacoes')
@login_required
def pagina_legislacoes(): return render_template('legislacoes.html', active_page='legislacoes', active_group='biblioteca', **tmpl_ctx())

@app.route('/legislacoes/arvores')
@login_required
def pagina_arvores(): return render_template('legislacoes.html', active_page='arvores', active_group='biblioteca', **tmpl_ctx())

@app.route('/legislacoes/pendentes')
@login_required
def pagina_leg_pendentes(): return render_template('legislacoes.html', active_page='leg-pendentes', active_group='biblioteca', **tmpl_ctx())

@app.route('/legislacoes/arvore/<int:leg_id>')
@login_required
def pagina_arvore(leg_id): return render_template('arvore.html', legislacao_id=leg_id, active_page='arvores', active_group='biblioteca', **tmpl_ctx())

@app.route('/parametros')
@login_required
def pagina_parametros(): return render_template('parametros.html', active_page='parametros', active_group='parametros', **tmpl_ctx())

@app.route('/parametros/calculadora')
@login_required
def pagina_calculadora(): return render_template('parametros.html', active_page='calculadora', active_group='parametros', **tmpl_ctx())

@app.route('/parametros/importar')
@login_required
def pagina_importar(): return render_template('parametros.html', active_page='importar', active_group='parametros', **tmpl_ctx())

@app.route('/parametros/pendentes')
@login_required
def pagina_param_pendentes(): return render_template('parametros.html', active_page='param-pendentes', active_group='parametros', **tmpl_ctx())

@app.route('/monitoramento')
@login_required
def pagina_monitoramento(): return render_template('monitoramento.html', active_page='municipios-monitor', active_group='monitoramento', **tmpl_ctx())

@app.route('/monitoramento/historico')
@login_required
def pagina_historico(): return render_template('monitoramento.html', active_page='historico', active_group='monitoramento', **tmpl_ctx())

@app.route('/monitoramento/alteracoes')
@login_required
def pagina_alteracoes(): return render_template('monitoramento.html', active_page='alteracoes', active_group='monitoramento', **tmpl_ctx())

@app.route('/monitoramento/scheduler')
@login_required
def pagina_scheduler(): return render_template('monitoramento.html', active_page='scheduler', active_group='monitoramento', **tmpl_ctx())

@app.route('/integracao')
@login_required
def pagina_integracao(): return render_template('integracao.html', active_page='integracao-fila', active_group='integracoes', **tmpl_ctx())

@app.route('/integracao/aprovadas')
@login_required
def pagina_integracao_aprovadas(): return render_template('integracao.html', active_page='integracao-aprovadas', active_group='integracoes', **tmpl_ctx())

@app.route('/integracao/rejeitadas')
@login_required
def pagina_integracao_rejeitadas(): return render_template('integracao.html', active_page='integracao-rejeitadas', active_group='integracoes', **tmpl_ctx())

@app.route('/usuarios')
@admin_required
def pagina_usuarios(): return render_template('usuarios.html', active_page='todos-usuarios', active_group='usuarios', **tmpl_ctx())

@app.route('/usuarios/pendentes')
@admin_required
def pagina_users_pendentes(): return render_template('usuarios.html', active_page='usuarios-pendentes', active_group='usuarios', **tmpl_ctx())

@app.route('/config')
@app.route('/config/tipos-legislacao')
@admin_required
def pagina_config_tipos(): return render_template('configuracoes.html', active_page='tipos-leg', active_group='config', **tmpl_ctx())

@app.route('/config/assuntos')
@admin_required
def pagina_config_assuntos(): return render_template('configuracoes.html', active_page='assuntos', active_group='config', **tmpl_ctx())

@app.route('/config/email')
@admin_required
def pagina_config_email(): return render_template('configuracoes.html', active_page='config-email', active_group='config', **tmpl_ctx())

@app.route('/config/perfil')
@login_required
def pagina_perfil(): return render_template('configuracoes.html', active_page='perfil', active_group='config', **tmpl_ctx())

# ═══════════════════════════════════════════════════════════════
# API: AUTH
# ═══════════════════════════════════════════════════════════════

# ── FIX 2: Login API corrigido — sessão agora é criada corretamente ──
@app.route('/api/auth/login', methods=['POST'])
def api_login():
    d = request.json or {}
    email = d.get('email','').strip().lower()
    senha = d.get('senha','')
    user = qry("SELECT * FROM users WHERE email=%s AND ativo=TRUE AND aprovado=TRUE", (email,), 'one')
    if not user or not verificar_senha(senha, user['senha_hash']):
        return jsonify({'success':False,'error':'E-mail ou senha incorretos'}), 401
    session['user_id'] = user['id']
    session['nome'] = user['nome']
    session['email'] = user['email']
    session['role'] = user['role']
    qry("UPDATE users SET ultimo_acesso=NOW() WHERE id=%s", (user['id'],), commit=True, fetch=None)
    return jsonify({'success':True,'role':user['role']})

@app.route('/api/auth/logout', methods=['POST'])
def api_logout(): session.clear(); return jsonify({'success':True})

# ── FIX 8: Rota GET para logout (links <a href="/logout">) ──
@app.route('/logout')
def logout():
    session.clear()
    return redirect('/login')

@app.route('/api/auth/cadastrar', methods=['POST'])
def api_cadastrar():
    d = request.json or {}
    nome = d.get('nome','').strip()
    email = d.get('email','').strip().lower()
    senha = d.get('senha','')
    if not nome or not email or not senha: return jsonify({'success':False,'error':'Campos obrigatórios'}), 400
    if qry("SELECT id FROM users WHERE email=%s", (email,), 'one'): return jsonify({'success':False,'error':'E-mail já cadastrado'}), 400
    import re
    if not re.match(r'^(?=.*[A-Z])(?=.*[a-z])(?=.*\d)(?=.*[@$!%*?&\-_#])[A-Za-z\d@$!%*?&\-_#]{6,15}$', senha):
        return jsonify({'success':False,'error':'Senha fraca. Use 6-15 chars com maiúscula, minúscula, número e especial.'}), 400
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("INSERT INTO users (nome,email,senha_hash,role,ativo,aprovado) VALUES (%s,%s,%s,'apenas_leitura',FALSE,FALSE) RETURNING id",
                    (nome, email, hash_senha(senha)))
        uid = cur.fetchone()[0]
        token_ativ = gerar_token()
        exp = datetime.now() + timedelta(hours=24)
        cur.execute("INSERT INTO aprovacao_tokens (user_id,token,tipo,expira_em) VALUES (%s,%s,'ativacao',%s)", (uid,token_ativ,exp))
        conn.commit()
    finally: conn.close()
    enviar_email_ativacao({'email':email,'nome':nome}, token_ativ)
    return jsonify({'success':True})

@app.route('/api/auth/esqueci-senha', methods=['POST'])
def api_esqueci_senha():
    d = request.json or {}
    email = d.get('email','').strip().lower()
    user = qry("SELECT * FROM users WHERE email=%s", (email,), 'one')
    if user:
        token = gerar_token()
        exp = datetime.now() + timedelta(hours=1)
        qry("INSERT INTO password_reset_tokens (user_id,token,expira_em) VALUES (%s,%s,%s)", (user['id'],token,exp), commit=True)
        enviar_email_reset(user, token)
    return jsonify({'success':True})

@app.route('/api/auth/reset-senha', methods=['POST'])
def api_reset_senha():
    d = request.json or {}
    token = d.get('token','')
    senha = d.get('senha','')
    tk = qry("SELECT * FROM password_reset_tokens WHERE token=%s AND usado=FALSE AND expira_em>NOW()", (token,), 'one')
    if not tk: return jsonify({'success':False,'error':'Link inválido ou expirado'}), 400
    qry("UPDATE users SET senha_hash=%s WHERE id=%s", (hash_senha(senha), tk['user_id']), commit=True)
    qry("UPDATE password_reset_tokens SET usado=TRUE WHERE id=%s", (tk['id'],), commit=True)
    return jsonify({'success':True})

@app.route('/api/auth/alterar-senha', methods=['POST'])
@login_required
def api_alterar_senha():
    d = request.json or {}
    user = qry("SELECT * FROM users WHERE id=%s", (session['user_id'],), 'one')
    if not user: return jsonify({'success':False,'error':'Usuário não encontrado'}), 404
    if not verificar_senha(d.get('senha_atual',''), user['senha_hash']):
        return jsonify({'success':False,'error':'Senha atual incorreta'}), 400
    qry("UPDATE users SET senha_hash=%s WHERE id=%s", (hash_senha(d['senha_nova']), session['user_id']), commit=True)
    return jsonify({'success':True})

@app.route('/api/auth/perfil', methods=['GET','POST'])
@login_required
def api_perfil():
    if request.method == 'GET':
        user = qry("SELECT id,nome,email,role FROM users WHERE id=%s", (session['user_id'],), 'one')
        return jsonify({'success':True,'data':user})
    d = request.json or {}
    nome = d.get('nome','').strip()
    if not nome: return jsonify({'success':False,'error':'Nome obrigatório'}), 400
    qry("UPDATE users SET nome=%s WHERE id=%s", (nome, session['user_id']), commit=True)
    session['nome'] = nome
    return jsonify({'success':True})

# ═══════════════════════════════════════════════════════════════
# API: ADMIN USUÁRIOS
# ═══════════════════════════════════════════════════════════════

@app.route('/api/admin/usuarios', methods=['GET'])
@admin_required
def api_listar_usuarios():
    users = qry("SELECT id,nome,email,role,ativo,aprovado,criado_em,ultimo_acesso FROM users ORDER BY criado_em DESC")
    return jsonify({'success':True,'data':users})

@app.route('/api/admin/usuarios/<int:uid>/role', methods=['POST'])
@admin_required
def api_alterar_role(uid):
    role = (request.json or {}).get('role')
    if role not in ('admin','editor','apenas_leitura'): return jsonify({'success':False,'error':'Role inválida'}), 400
    qry("UPDATE users SET role=%s WHERE id=%s", (role, uid), commit=True)
    return jsonify({'success':True})

@app.route('/api/admin/usuarios/<int:uid>', methods=['DELETE'])
@admin_required
def api_excluir_usuario(uid):
    if uid == session['user_id']: return jsonify({'success':False,'error':'Não pode excluir a si mesmo'}), 400
    qry("DELETE FROM users WHERE id=%s", (uid,), commit=True)
    return jsonify({'success':True})

# ═══════════════════════════════════════════════════════════════
# API: BIBLIOTECA DE LEGISLAÇÕES
# ═══════════════════════════════════════════════════════════════

@app.route('/api/legislacoes', methods=['GET'])
@login_required
def api_listar_legislacoes():
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 50))
    offset = (page - 1) * per_page
    where = ['l.pendente_aprovacao = FALSE']
    params = []
    for campo, col in [('estado','l.estado'),('municipio','l.municipio_nome'),('numero','l.numero'),
                       ('status','l.status'),('tipo_id','l.tipo_id'),('assunto_id','l.assunto_id')]:
        v = request.args.get(campo)
        if v: where.append(f"{col} = %s"); params.append(v)
    if request.args.get('em_monitoramento') in ('0','1'):
        where.append("l.em_monitoramento = %s"); params.append(request.args['em_monitoramento'] == '1')
    if request.args.get('keywords'):
        where.append("(l.ementa ILIKE %s OR l.palavras_chave ILIKE %s)")
        kw = f"%{request.args['keywords']}%"; params += [kw, kw]
    if request.args.get('ano'):
        where.append("l.ano = %s"); params.append(int(request.args['ano']))
    where_sql = ' AND '.join(where)
    total = qry(f"SELECT COUNT(*) as n FROM legislacoes l WHERE {where_sql}", params, 'one')['n']
    order = 'l.criado_em DESC' if request.args.get('order') == 'recente' else 'l.estado, l.municipio_nome, l.ano DESC'
    data = qry(f"SELECT l.*, tl.nome as tipo_nome, al.nome as assunto_nome FROM legislacoes l LEFT JOIN tipos_legislacao tl ON l.tipo_id=tl.id LEFT JOIN assuntos_legislacao al ON l.assunto_id=al.id WHERE {where_sql} ORDER BY {order} LIMIT %s OFFSET %s",
               params + [per_page, offset])
    return jsonify({'success':True,'data':data,'total':total,'page':page})

@app.route('/api/legislacoes', methods=['POST'])
@editor_required
def api_criar_legislacao():
    arquivo = request.files.get('arquivo')
    if not arquivo: return jsonify({'success':False,'error':'Nenhum arquivo enviado'}), 400
    nome_arquivo = secure_filename(arquivo.filename)
    arquivo_bytes = arquivo.read()
    texto = extrair_texto_arquivo(arquivo_bytes, nome_arquivo)
    hash_c = hashlib.sha256(arquivo_bytes).hexdigest()
    ano = request.form.get('ano')
    tipo_id = request.form.get('tipo_id') or None
    assunto_id = request.form.get('assunto_id') or None
    estado = request.form.get('estado') or None
    esfera = request.form.get('esfera','municipal')
    municipio_nome = request.form.get('municipio') or None
    numero = request.form.get('numero')
    ementa = request.form.get('ementa') or None
    data_pub = request.form.get('data_publicacao') or None
    em_mon = request.form.get('em_monitoramento','0') == '1'
    kw = request.form.get('palavras_chave')
    kw_json = json.dumps([k.strip() for k in kw.split(',') if k.strip()]) if kw else None

    # Buscar nome do tipo
    tipo_row = qry("SELECT nome FROM tipos_legislacao WHERE id=%s", (tipo_id,), 'one') if tipo_id else None
    assunto_row = qry("SELECT nome FROM assuntos_legislacao WHERE id=%s", (assunto_id,), 'one') if assunto_id else None

    # Inserir legislação (sem arquivo_url ainda) para obter o ID
    leg_row = qry("""
        INSERT INTO legislacoes (pais,esfera,estado,municipio_nome,tipo_id,tipo_nome,numero,ano,data_publicacao,
            ementa,assunto_id,assunto_nome,palavras_chave,conteudo_texto,arquivo_nome,arquivo_tipo,
            hash_conteudo,em_monitoramento,origem,pendente_aprovacao,criado_em)
        VALUES ('BR',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'manual',FALSE,NOW()) RETURNING id
    """, (esfera,estado,municipio_nome,tipo_id,tipo_row['nome'] if tipo_row else None,
          numero,int(ano) if ano else None,data_pub,ementa,assunto_id,
          assunto_row['nome'] if assunto_row else None,kw_json,texto,nome_arquivo,
          Path(nome_arquivo).suffix.lower()[1:],hash_c,em_mon), 'one', commit=True)

    novo_id = leg_row['id'] if leg_row else None

    # Upload para Cloudflare R2 (se configurado)
    if novo_id and r2_disponivel():
        url_r2 = r2_upload(arquivo_bytes, nome_arquivo, leg_id=novo_id)
        if url_r2:
            qry("UPDATE legislacoes SET arquivo_url=%s WHERE id=%s", (url_r2, novo_id), commit=True)

    return jsonify({'success':True,'id':novo_id})

@app.route('/api/legislacoes/<int:leg_id>', methods=['GET'])
@login_required
def api_get_legislacao(leg_id):
    l = qry("SELECT l.*, tl.nome as tipo_nome, al.nome as assunto_nome FROM legislacoes l LEFT JOIN tipos_legislacao tl ON l.tipo_id=tl.id LEFT JOIN assuntos_legislacao al ON l.assunto_id=al.id WHERE l.id=%s", (leg_id,), 'one')
    if not l: return jsonify({'success':False,'error':'Não encontrada'}), 404
    return jsonify({'success':True,'data':l})

# ── FIX 1: DELETE corrigido — leg_id + foreign keys + verificação ──
@app.route('/api/legislacoes/<int:leg_id>', methods=['DELETE'])
@editor_required
def api_excluir_legislacao(leg_id):
    leg = qry("SELECT arquivo_url FROM legislacoes WHERE id=%s", (leg_id,), 'one')
    if not leg:
        return jsonify({'success': False, 'error': 'Legislação não encontrada'}), 404

    # Remover arquivo do R2
    if leg.get('arquivo_url') and r2_disponivel():
        r2_delete(leg['arquivo_url'])

    # Remover registros dependentes (foreign keys) e a legislação
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM legislacao_relacoes WHERE legislacao_pai_id=%s OR legislacao_filha_id=%s", (leg_id, leg_id))
        cur.execute("DELETE FROM alteracoes WHERE legislacao_id=%s", (leg_id,))
        cur.execute("DELETE FROM integracao_atualizacoes WHERE legislacao_id=%s", (leg_id,))
        cur.execute("DELETE FROM legislacoes WHERE id=%s", (leg_id,))
        conn.commit()
    except Exception as e:
        conn.rollback()
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        conn.close()

    return jsonify({'success': True})

@app.route('/api/legislacoes/<int:leg_id>/texto')
@login_required
def api_leg_texto(leg_id):
    l = qry("SELECT conteudo_texto, arquivo_nome FROM legislacoes WHERE id=%s", (leg_id,), 'one')
    if not l or not l.get('conteudo_texto'): return jsonify({'success':False,'error':'Texto não disponível'}), 404
    return jsonify({'success':True,'data':{'texto':l['conteudo_texto'],'arquivo_nome':l.get('arquivo_nome','')}})

@app.route('/api/legislacoes/<int:leg_id>/documento')
@login_required
def api_leg_documento(leg_id):
    l = qry("SELECT arquivo_url, arquivo_nome FROM legislacoes WHERE id=%s", (leg_id,), 'one')
    if not l: return jsonify({'error':'Não encontrada'}), 404

    url = l.get('arquivo_url')
    if not url:
        return jsonify({'error':'Documento não disponível. Faça upload do arquivo.'}), 404

    # Se R2 disponível e a URL é uma chave interna (não começa com http),
    # gerar URL assinada de 1 hora
    if r2_disponivel() and not url.startswith('http'):
        url_assinada = r2_url_assinada(url, expiracao_seg=3600)
        if url_assinada:
            return redirect(url_assinada)

    # URL pública ou pré-assinada já completa → redirect direto
    return redirect(url)

@app.route('/api/legislacoes/<int:leg_id>/monitoramento', methods=['POST'])
@editor_required
def api_toggle_monitoramento(leg_id):
    d      = request.json or {}
    ativar = d.get('ativar', True)
    data_inicio = d.get('data_inicio_monitoramento') or None
    qry("UPDATE legislacoes SET em_monitoramento=%s, data_inicio_monitoramento=%s WHERE id=%s",
        (ativar, data_inicio, leg_id), commit=True)
    return jsonify({'success':True})

# Árvore genealógica
@app.route('/api/legislacoes/<int:leg_id>/arvore')
@login_required
def api_leg_arvore(leg_id):
    raiz = qry("SELECT l.*, tl.nome as tipo_nome FROM legislacoes l LEFT JOIN tipos_legislacao tl ON l.tipo_id=tl.id WHERE l.id=%s", (leg_id,), 'one')
    if not raiz: return jsonify({'success':False,'error':'Não encontrada'}), 404
    relacoes = qry("""
        SELECT r.*, lp.id as pai_id, lp.numero as pai_numero, lp.ano as pai_ano, tl1.nome as pai_tipo,
               lf.id as filha_id, lf.numero as filha_numero, lf.ano as filha_ano, tl2.nome as filha_tipo,
               lf.status as filha_status, lf.municipio_nome, lf.data_publicacao, lf.ementa,
               lf.conteudo_texto, lf.arquivo_url
        FROM legislacao_relacoes r
        JOIN legislacoes lp ON r.legislacao_pai_id = lp.id
        JOIN legislacoes lf ON r.legislacao_filha_id = lf.id
        LEFT JOIN tipos_legislacao tl1 ON lp.tipo_id = tl1.id
        LEFT JOIN tipos_legislacao tl2 ON lf.tipo_id = tl2.id
        WHERE lp.id = %s OR lf.id = %s
    """, (leg_id, leg_id))
    ids_vistos = {leg_id}
    nodes = [{'id': leg_id, **raiz}]
    edges = []
    for r in relacoes:
        for node_id, num, ano, tipo, status in [
            (r['pai_id'], r['pai_numero'], r['pai_ano'], r['pai_tipo'], 'vigente'),
            (r['filha_id'], r['filha_numero'], r['filha_ano'], r['filha_tipo'], r['filha_status'])
        ]:
            if node_id not in ids_vistos:
                ids_vistos.add(node_id)
                nodes.append({'id':node_id,'numero':num,'ano':ano,'tipo_nome':tipo,'status':status,
                               'municipio_nome':r.get('municipio_nome'),'ementa':r.get('ementa'),
                               'data_publicacao':str(r.get('data_publicacao','')) if r.get('data_publicacao') else None,
                               'conteudo_texto':bool(r.get('conteudo_texto')),'arquivo_url':r.get('arquivo_url')})
        edges.append({'id':r['id'],'legislacao_pai_id':r['legislacao_pai_id'],
                      'legislacao_filha_id':r['legislacao_filha_id'],'tipo_relacao':r['tipo_relacao']})
    return jsonify({'success':True,'data':{'nodes':nodes,'edges':edges,'raiz':raiz}})

# Busca com IA
@app.route('/api/legislacoes/buscar-ia', methods=['POST'])
@editor_required
def api_buscar_ia():
    consulta = (request.json or {}).get('consulta','').strip()
    if not consulta: return jsonify({'success':False,'error':'Consulta obrigatória'}), 400
    leg_id = qry("INSERT INTO buscas_ia (consulta,status,solicitado_por,criado_em) VALUES (%s,'pendente',%s,NOW()) RETURNING id",
                 (consulta, session['user_id']), 'one', commit=True)
    def rodar_busca(bid, consul):
        try:
            resultado = _buscar_legislacao_internet(consul)
            if resultado and resultado.get('url'):
                qry("UPDATE buscas_ia SET status='encontrado',resultado_url=%s,resultado_nome=%s,finalizado_em=NOW() WHERE id=%s",
                    (resultado['url'],resultado.get('titulo',''),bid), commit=True)
                qry("""INSERT INTO legislacoes (municipio_nome,estado,ementa,url_original,origem,pendente_aprovacao,criado_em)
                    VALUES (%s,%s,%s,%s,'busca_ia',TRUE,NOW())""",
                    (resultado.get('municipio'),resultado.get('estado'),resultado.get('titulo',''),resultado['url']), commit=True)
            else:
                qry("UPDATE buscas_ia SET status='nao_encontrado',finalizado_em=NOW() WHERE id=%s", (bid,), commit=True)
        except Exception as e:
            qry("UPDATE buscas_ia SET status='erro',erro=%s,finalizado_em=NOW() WHERE id=%s", (str(e),bid), commit=True)
    threading.Thread(target=rodar_busca, args=(leg_id['id'] if leg_id else 0, consulta), daemon=True).start()
    return jsonify({'success':True,'message':'Busca iniciada. A legislação será adicionada aos pendentes quando encontrada.'})

# Pendentes de aprovação
@app.route('/api/legislacoes/pendentes', methods=['GET'])
@login_required
def api_leg_pendentes():
    data = qry("SELECT b.*, u.nome as solicitante FROM buscas_ia b LEFT JOIN users u ON b.solicitado_por=u.id WHERE b.status IN ('encontrado','pendente') ORDER BY b.criado_em DESC")
    return jsonify({'success':True,'data':data})

@app.route('/api/legislacoes/pendentes/<int:bid>/aprovar', methods=['POST'])
@admin_required
def api_aprovar_leg_pendente(bid):
    busca = qry("SELECT * FROM buscas_ia WHERE id=%s", (bid,), 'one')
    if not busca: return jsonify({'success':False,'error':'Não encontrado'}), 404
    qry("UPDATE legislacoes SET pendente_aprovacao=FALSE, aprovado_em=NOW(), aprovado_por=%s WHERE url_original=%s AND pendente_aprovacao=TRUE",
        (session['user_id'], busca.get('resultado_url','')), commit=True)
    qry("UPDATE buscas_ia SET status='aprovado' WHERE id=%s", (bid,), commit=True)
    return jsonify({'success':True})

@app.route('/api/legislacoes/pendentes/<int:bid>/rejeitar', methods=['POST'])
@admin_required
def api_rejeitar_leg_pendente(bid):
    qry("DELETE FROM legislacoes WHERE url_original=(SELECT resultado_url FROM buscas_ia WHERE id=%s) AND pendente_aprovacao=TRUE", (bid,), commit=True)
    qry("UPDATE buscas_ia SET status='rejeitado' WHERE id=%s", (bid,), commit=True)
    return jsonify({'success':True})

# ═══════════════════════════════════════════════════════════════
# API: CONFIGURAÇÕES (tipos, assuntos, email)
# ═══════════════════════════════════════════════════════════════

@app.route('/api/config/tipos-legislacao', methods=['GET'])
@login_required
def api_get_tipos(): return jsonify({'success':True,'data':qry("SELECT * FROM tipos_legislacao ORDER BY nome")})

@app.route('/api/config/tipos-legislacao', methods=['POST'])
@editor_required
def api_criar_tipo():
    d = request.json or {}
    nome = d.get('nome','').strip()
    if not nome: return jsonify({'success':False,'error':'Nome obrigatório'}), 400
    try:
        qry("INSERT INTO tipos_legislacao (nome,descricao,criado_por) VALUES (%s,%s,%s)",
            (nome, d.get('descricao',''), session['user_id']), commit=True)
        return jsonify({'success':True})
    except: return jsonify({'success':False,'error':'Tipo já existe'}), 400

@app.route('/api/config/tipos-legislacao/<int:tid>', methods=['DELETE'])
@admin_required
def api_del_tipo(tid):
    qry("DELETE FROM tipos_legislacao WHERE id=%s", (tid,), commit=True)
    return jsonify({'success':True})

@app.route('/api/config/assuntos', methods=['GET'])
@login_required
def api_get_assuntos(): return jsonify({'success':True,'data':qry("SELECT * FROM assuntos_legislacao ORDER BY nome")})

@app.route('/api/config/assuntos', methods=['POST'])
@editor_required
def api_criar_assunto():
    d = request.json or {}
    nome = d.get('nome','').strip()
    if not nome: return jsonify({'success':False,'error':'Nome obrigatório'}), 400
    try:
        qry("INSERT INTO assuntos_legislacao (nome,descricao,criado_por) VALUES (%s,%s,%s)",
            (nome, d.get('descricao',''), session['user_id']), commit=True)
        return jsonify({'success':True})
    except: return jsonify({'success':False,'error':'Assunto já existe'}), 400

@app.route('/api/config/assuntos/<int:aid>', methods=['DELETE'])
@admin_required
def api_del_assunto(aid):
    qry("DELETE FROM assuntos_legislacao WHERE id=%s", (aid,), commit=True)
    return jsonify({'success':True})

@app.route('/api/config/email', methods=['GET'])
@admin_required
def api_config_email():
    return jsonify({'success':True,'data':{'host':os.getenv('EMAIL_HOST',''),'port':os.getenv('EMAIL_PORT',''),'user':os.getenv('EMAIL_USER',''),'from':os.getenv('EMAIL_FROM','')}})

@app.route('/api/config/email/testar', methods=['POST'])
@admin_required
def api_testar_email():
    try:
        enviar_email_generico(session['email'], 'Teste UrbanLex', '<p>E-mail de teste do UrbanLex.</p>')
        return jsonify({'success':True})
    except Exception as e:
        return jsonify({'success':False,'error':str(e)}), 500

# ═══════════════════════════════════════════════════════════════
# API: PARÂMETROS URBANÍSTICOS
# ═══════════════════════════════════════════════════════════════

@app.route('/api/municipios')
@login_required
def api_municipios(): return jsonify(qry("SELECT DISTINCT municipio as nome, estado FROM zonas_urbanas ORDER BY estado, municipio"))

@app.route('/api/zonas/<municipio>')
@login_required
def api_zonas(municipio): return jsonify(qry("SELECT zona, subzona FROM zonas_urbanas WHERE municipio=%s ORDER BY zona, subzona", (municipio,)))

@app.route('/api/zona/<municipio>/<zona>')
@login_required
def api_zona(municipio, zona):
    z = qry("SELECT * FROM zonas_urbanas WHERE municipio=%s AND zona=%s LIMIT 1", (municipio,zona), 'one')
    if not z: return jsonify({'error':'Zona não encontrada'}), 404
    return jsonify(z)

@app.route('/api/zonas/todas')
@login_required
def api_todas_zonas():
    page = int(request.args.get('page',1)); per_page = int(request.args.get('per_page',100))
    offset = (page-1)*per_page
    total = qry("SELECT COUNT(*) as n FROM zonas_urbanas", fetch='one')['n']
    data = qry("SELECT * FROM zonas_urbanas ORDER BY estado, municipio, zona LIMIT %s OFFSET %s", (per_page, offset))
    return jsonify({'success':True,'data':data,'total':total,'page':page})

@app.route('/api/zona', methods=['POST'])
@editor_required
def api_criar_zona():
    d = request.json or {}
    mun = d.get('municipio','').strip()
    zona = d.get('zona','').strip()
    if not mun or not zona: return jsonify({'success':False,'error':'Município e zona obrigatórios'}), 400
    cols = [k for k in d.keys() if k not in ('id','criado_em','atualizado_em','atualizado_por')]
    vals = [d[c] for c in cols]
    cols_str = ','.join(cols); ph = ','.join(['%s']*len(cols))
    upd = ','.join(f"{c}=EXCLUDED.{c}" for c in cols if c not in ('municipio','zona','subzona'))
    upd_full = upd + ",atualizado_em=NOW(),atualizado_por=%s"
    qry(f"INSERT INTO zonas_urbanas ({cols_str}) VALUES ({ph}) ON CONFLICT (municipio,zona,subzona) DO UPDATE SET {upd_full}",
        vals + [session['user_id']], commit=True)
    return jsonify({'success':True})

@app.route('/api/calcular-area', methods=['POST'])
@login_required
def api_calcular_area():
    try:
        from calculador_area_computavel import calcular_areas_computaveis
        resultado = calcular_areas_computaveis(request.json or {})
        return jsonify({'success':True,'resultado':resultado})
    except Exception as e:
        return jsonify({'success':False,'error':str(e)}), 500

# ═══════════════════════════════════════════════════════════════
# API: MONITORAMENTO
# ═══════════════════════════════════════════════════════════════

@app.route('/api/monitor/municipios', methods=['GET'])
@login_required
def api_monitor_municipios(): return jsonify(qry("SELECT * FROM municipios WHERE ativo=TRUE ORDER BY nome"))

@app.route('/api/monitor/municipios', methods=['POST'])
@admin_required
def api_monitor_add_municipio():
    d = request.json or {}
    nome = d.get('nome','').strip()
    if not nome: return jsonify({'success':False,'error':'Nome obrigatório'}), 400
    row = qry("INSERT INTO municipios (nome,estado,url_diario,tipo_site) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING id",
        (nome, d.get('estado',''), d.get('url_diario',''), d.get('tipo_site','generico')), 'one', commit=True)
    mun_id = row['id'] if row else None
    if mun_id and d.get('url_diario'):
        def _detectar(mid):
            try:
                from modulos.scraper_inteligente import detectar_e_salvar_perfil
                detectar_e_salvar_perfil(mid)
            except Exception as e:
                import logging; logging.getLogger(__name__).error(f"Deteccao perfil falhou: {e}")
        threading.Thread(target=_detectar, args=(mun_id,), daemon=True).start()
    return jsonify({'success':True,'id':mun_id})

@app.route('/api/monitor/municipios/<int:mid>', methods=['DELETE'])
@admin_required
def api_monitor_del_municipio(mid):
    qry("UPDATE municipios SET ativo=FALSE WHERE id=%s", (mid,), commit=True)
    return jsonify({'success':True})

@app.route('/api/monitor/municipios/<int:mid>/perfil')
@login_required
def api_monitor_perfil(mid):
    perfil = qry("""SELECT p.*, m.nome as municipio_nome, m.url_diario
                    FROM perfis_diario p JOIN municipios m ON m.id=p.municipio_id
                    WHERE p.municipio_id=%s""", (mid,), 'one')
    return jsonify({'success':True,'data':perfil})

@app.route('/api/monitor/municipios/<int:mid>/detectar', methods=['POST'])
@editor_required
def api_monitor_detectar_perfil(mid):
    forcar = (request.json or {}).get('forcar', False)
    def _detectar():
        try:
            from modulos.scraper_inteligente import detectar_e_salvar_perfil
            detectar_e_salvar_perfil(mid, forcar_redeteccao=forcar)
        except Exception as e:
            import logging; logging.getLogger(__name__).error(f"Deteccao perfil: {e}")
    threading.Thread(target=_detectar, daemon=True).start()
    return jsonify({'success':True,'message':'Deteccao iniciada. Aguarde alguns instantes.'})

@app.route('/api/monitor/municipios/<int:mid>/url', methods=['POST'])
@editor_required
def api_monitor_atualizar_url(mid):
    d = request.json or {}
    url = d.get('url','').strip()
    if not url: return jsonify({'success':False,'error':'URL obrigatoria'}), 400
    qry("UPDATE municipios SET url_diario=%s WHERE id=%s", (url, mid), commit=True)
    def _re_detectar():
        try:
            from modulos.scraper_inteligente import detectar_e_salvar_perfil
            detectar_e_salvar_perfil(mid, forcar_redeteccao=True)
        except Exception as e:
            import logging; logging.getLogger(__name__).error(f"Re-deteccao: {e}")
    threading.Thread(target=_re_detectar, daemon=True).start()
    return jsonify({'success':True,'message':'URL atualizada. Re-detectando perfil...'})

@app.route('/api/monitor/historico')
@login_required
def api_monitor_historico():
    data = qry("""SELECT se.*, u.nome as usuario_nome
                  FROM scheduler_execucoes se
                  LEFT JOIN users u ON se.disparado_por=u.id
                  ORDER BY iniciada_em DESC LIMIT 50""")
    # Não retornar log_erros completo na listagem (pesado) — só na rota de detalhe
    for row in (data or []):
        row.pop('log_erros', None)
    return jsonify({'success':True,'data':data})

@app.route('/api/monitor/historico/<int:exec_id>')
@login_required
def api_monitor_execucao_detalhe(exec_id):
    """Retorna log completo de uma execução, incluindo stack traces."""
    row = qry("""SELECT se.*, u.nome as usuario_nome
                 FROM scheduler_execucoes se
                 LEFT JOIN users u ON se.disparado_por=u.id
                 WHERE se.id=%s""", (exec_id,), 'one')
    if not row:
        return jsonify({'success':False,'error':'Execução não encontrada'}), 404
    return jsonify({'success':True,'data':row})

@app.route('/api/monitor/status')
@login_required
def api_monitor_status_resumo():
    """Badge de status: retorna a última execução com indicador de saúde."""
    ultima = qry("""SELECT id, iniciada_em, finalizada_em, status,
                           municipios_processados, municipios_ok, municipios_erro,
                           alteracoes_detectadas, erros, email_enviado
                    FROM scheduler_execucoes
                    ORDER BY iniciada_em DESC LIMIT 1""", fetch='one')
    if not ultima:
        return jsonify({'success':True,'data':{'status':'nunca_executou','ultima':None}})

    # Calcular "saúde" para o badge
    if ultima['status'] == 'rodando':
        saude = 'executando'
    elif ultima['status'] == 'erro' or (ultima.get('municipios_erro',0) == ultima.get('municipios_processados',1) and ultima.get('municipios_processados',0) > 0):
        saude = 'erro'
    elif ultima.get('municipios_erro', 0) > 0 or ultima.get('erros', 0) > 0:
        saude = 'parcial'
    else:
        saude = 'ok'

    ultima['saude'] = saude
    return jsonify({'success':True,'data':ultima})

# ── FIX 7: Query alterações usa COALESCE para municipio_nome ──
@app.route('/api/monitor/alteracoes')
@login_required
def api_monitor_alteracoes():
    data = qry("""SELECT a.*, l.numero, l.ano, tl.nome as tipo_nome,
        COALESCE(m.nome, l.municipio_nome) as municipio_nome
        FROM alteracoes a
        JOIN legislacoes l ON a.legislacao_id=l.id
        LEFT JOIN tipos_legislacao tl ON l.tipo_id=tl.id
        LEFT JOIN municipios m ON l.municipio_id=m.id
        ORDER BY a.data_deteccao DESC LIMIT 100""")
    return jsonify({'success':True,'data':data})

@app.route('/api/monitor/scheduler', methods=['GET'])
@login_required
def api_scheduler_get():
    cfg = qry("SELECT * FROM scheduler_config ORDER BY id LIMIT 1", fetch='one')
    return jsonify({'success':True,'data':cfg or {}})

@app.route('/api/monitor/scheduler', methods=['POST'])
@admin_required
def api_scheduler_update():
    d = request.json or {}
    upd, vals = [], []
    for campo, col in [('horario','horario_execucao'), ('status','status')]:
        if d.get(campo) is not None:
            upd.append(f"{col}=%s"); vals.append(d[campo])
    for campo, col in [('debug_ativo','debug_ativo'), ('email_relatorio','email_relatorio')]:
        if campo in d:
            valor = d[campo]
            if isinstance(valor, str): valor = valor.lower() == 'true'
            upd.append(f"{col}=%s"); vals.append(bool(valor))
    if upd:
        vals.append(session['user_id'])
        qry(f"UPDATE scheduler_config SET {','.join(upd)}, atualizado_em=NOW() WHERE id=1", vals, commit=True)
    cfg = qry("SELECT * FROM scheduler_config ORDER BY id LIMIT 1", fetch='one')
    return jsonify({'success':True,'data':cfg or {}})

@app.route('/api/monitor/scheduler/toggle', methods=['POST'])
@admin_required
def api_scheduler_toggle():
    d = request.json or {}
    campo = d.get('campo')
    if campo not in ('debug_ativo', 'email_relatorio'):
        return jsonify({'success':False,'error':'Campo invalido'}), 400
    cfg = qry("SELECT * FROM scheduler_config ORDER BY id LIMIT 1", fetch='one')
    if not cfg:
        return jsonify({'success':False,'error':'Configuracao nao encontrada'}), 404
    novo_valor = not bool(cfg[campo])
    qry(f"UPDATE scheduler_config SET {campo}=%s, atualizado_em=NOW() WHERE id=1",
        (novo_valor,), commit=True)
    return jsonify({'success':True, 'campo': campo, 'valor': novo_valor})

@app.route('/api/monitor/executar', methods=['POST'])
@editor_required
def api_monitor_executar():
    try:
        from modulos.scheduler_integrado import executar_ciclo_completo
        from modulos.bridge_integracao import processar_alteracao_detectada
        uid = session['user_id']
        threading.Thread(
            target=executar_ciclo_completo,
            kwargs={'bridge_callback': processar_alteracao_detectada, 'disparado_por': uid},
            daemon=True
        ).start()
        return jsonify({'success':True,'message':'Execução iniciada. Acompanhe em Histórico.'})
    except Exception as e:
        return jsonify({'success':False,'error':str(e)}), 500

# ═══════════════════════════════════════════════════════════════
# API: INTEGRAÇÕES (fila de parâmetros)
# ═══════════════════════════════════════════════════════════════

@app.route('/api/integracao/pendentes')
@login_required
def api_integ_pendentes():
    data = qry("""SELECT i.*, l.numero as legislacao_num, l.ano as legislacao_ano, tl.nome as tipo_nome,
        CONCAT(l.numero,'/',l.ano) as legislacao_ref
        FROM integracao_atualizacoes i LEFT JOIN legislacoes l ON i.legislacao_id=l.id
        LEFT JOIN tipos_legislacao tl ON l.tipo_id=tl.id WHERE i.status='pendente' ORDER BY i.criado_em DESC""")
    return jsonify({'success':True,'data':data})

@app.route('/api/integracao/<int:iid>')
@login_required
def api_integ_detalhe(iid):
    i = qry("""SELECT i.*, CONCAT(l.numero,'/',l.ano) as legislacao_ref FROM integracao_atualizacoes i
        LEFT JOIN legislacoes l ON i.legislacao_id=l.id WHERE i.id=%s""", (iid,), 'one')
    if not i: return jsonify({'success':False,'error':'Não encontrado'}), 404
    if i.get('parametros_json') and isinstance(i['parametros_json'], str):
        try: i['parametros_json'] = json.loads(i['parametros_json'])
        except: pass
    return jsonify({'success':True,'data':i})

@app.route('/api/integracao/<int:iid>/aprovar', methods=['POST'])
@editor_required
def api_integ_aprovar(iid):
    i = qry("SELECT * FROM integracao_atualizacoes WHERE id=%s", (iid,), 'one')
    if not i: return jsonify({'success':False,'error':'Não encontrado'}), 404
    params = i.get('parametros_json') or {}
    if isinstance(params, str):
        try: params = json.loads(params)
        except: params = {}
    if params and i.get('municipio') and i.get('zona'):
        cols = list(params.keys()) + ['municipio','zona','subzona','atualizado_em','atualizado_por']
        vals = list(params.values()) + [i['municipio'],i['zona'],i.get('subzona',''),datetime.now(),session['user_id']]
        ph = ','.join(['%s']*len(cols))
        upd = ','.join(f"{c}=EXCLUDED.{c}" for c in params.keys()) + ',atualizado_em=EXCLUDED.atualizado_em,atualizado_por=EXCLUDED.atualizado_por'
        qry(f"INSERT INTO zonas_urbanas ({','.join(cols)}) VALUES ({ph}) ON CONFLICT (municipio,zona,subzona) DO UPDATE SET {upd}",
            vals, commit=True)
    qry("UPDATE integracao_atualizacoes SET status='aprovado',revisado_em=NOW(),revisado_por=%s WHERE id=%s",
        (session['user_id'], iid), commit=True)
    return jsonify({'success':True})

@app.route('/api/integracao/<int:iid>/rejeitar', methods=['POST'])
@editor_required
def api_integ_rejeitar(iid):
    motivo = (request.json or {}).get('motivo','')
    qry("UPDATE integracao_atualizacoes SET status='rejeitado',revisado_em=NOW(),revisado_por=%s,motivo_rejeicao=%s WHERE id=%s",
        (session['user_id'], motivo, iid), commit=True)
    return jsonify({'success':True})

@app.route('/api/integracao/aprovar-todos', methods=['POST'])
@editor_required
def api_integ_aprovar_todos():
    pendentes = qry("SELECT * FROM integracao_atualizacoes WHERE status='pendente'")
    count = 0
    for i in pendentes:
        params = i.get('parametros_json') or {}
        if isinstance(params, str):
            try: params = json.loads(params)
            except: params = {}
        if params and i.get('municipio') and i.get('zona'):
            cols = list(params.keys()) + ['municipio','zona','subzona','atualizado_em','atualizado_por']
            vals = list(params.values()) + [i['municipio'],i['zona'],i.get('subzona',''),datetime.now(),session['user_id']]
            ph = ','.join(['%s']*len(cols))
            upd = ','.join(f"{c}=EXCLUDED.{c}" for c in params.keys()) + ',atualizado_em=EXCLUDED.atualizado_em'
            qry(f"INSERT INTO zonas_urbanas ({','.join(cols)}) VALUES ({ph}) ON CONFLICT (municipio,zona,subzona) DO UPDATE SET {upd}", vals, commit=True)
        qry("UPDATE integracao_atualizacoes SET status='aprovado',revisado_em=NOW(),revisado_por=%s WHERE id=%s",
            (session['user_id'], i['id']), commit=True)
        count += 1
    return jsonify({'success':True,'count':count})

# ═══════════════════════════════════════════════════════════════
# API: DASHBOARD
# ═══════════════════════════════════════════════════════════════

@app.route('/api/dashboard/stats')
@login_required
def api_dashboard_stats():
    try:
        leg_total = qry("SELECT COUNT(*) as n FROM legislacoes WHERE pendente_aprovacao=FALSE", fetch='one')['n']
        leg_vig   = qry("SELECT COUNT(*) as n FROM legislacoes WHERE status='vigente' AND pendente_aprovacao=FALSE", fetch='one')['n']
        leg_rev   = qry("SELECT COUNT(*) as n FROM legislacoes WHERE status='revogada'", fetch='one')['n']
        leg_pend  = qry("SELECT COUNT(*) as n FROM buscas_ia WHERE status IN ('encontrado','pendente')", fetch='one')['n']
        mun       = qry("SELECT COUNT(*) as n FROM municipios WHERE ativo=TRUE", fetch='one')['n']
        zonas     = qry("SELECT COUNT(*) as n FROM zonas_urbanas", fetch='one')['n']
        mun_zon   = qry("SELECT COUNT(DISTINCT municipio) as n FROM zonas_urbanas", fetch='one')['n']
        integ     = qry("SELECT COUNT(*) as n FROM integracao_atualizacoes WHERE status='pendente'", fetch='one')['n']
        sched     = qry("SELECT * FROM scheduler_config ORDER BY id LIMIT 1", fetch='one') or {}
        return jsonify({'success':True,'data':{
            'total_legislacoes':leg_total,'leg_vigentes':leg_vig,'leg_revogadas':leg_rev,
            'leg_pendentes':leg_pend,'total_municipios':mun,'total_zonas':zonas,
            'total_municipios_zonas':mun_zon,'integ_pendentes':integ,
            'scheduler_status':sched.get('status','desconhecido'),
            'horario_scheduler':sched.get('horario_execucao','02:00'),
            'ultima_execucao':str(sched.get('ultima_execucao','')) if sched.get('ultima_execucao') else None,
            'proxima_execucao':str(sched.get('proxima_execucao','')) if sched.get('proxima_execucao') else None,
        }})
    except Exception as e:
        return jsonify({'success':False,'error':str(e)}), 500

@app.route('/api/dashboard/feed')
@login_required
def api_dashboard_feed():
    rows = qry("""
        SELECT 'legislacao_adicionada' as tipo, tipo_nome||' '||COALESCE(numero,'')||'/'||COALESCE(CAST(ano AS TEXT),'') as titulo,
               municipio_nome as descricao, criado_em FROM legislacoes WHERE pendente_aprovacao=FALSE
        UNION ALL
        SELECT 'execucao_robo', 'Execução do robô: '||COALESCE(status,''), COALESCE(municipios_ok::text,'0')||' municípios processados', iniciada_em FROM scheduler_execucoes
        ORDER BY criado_em DESC LIMIT 20
    """)
    return jsonify({'success':True,'data':rows})

@app.route('/api/badges')
@login_required
def api_badges():
    leg  = qry("SELECT COUNT(*) as n FROM buscas_ia WHERE status IN ('encontrado','pendente')", fetch='one')['n']
    intg = qry("SELECT COUNT(*) as n FROM integracao_atualizacoes WHERE status='pendente'", fetch='one')['n']
    usr  = qry("SELECT COUNT(*) as n FROM users WHERE ativo=TRUE AND aprovado=FALSE", fetch='one')['n']
    return jsonify({'leg_pendentes':leg,'param_pendentes':intg,'integ_pendentes':intg,'users_pendentes':usr})

# ═══════════════════════════════════════════════════════════════
# SISTEMA
# ═══════════════════════════════════════════════════════════════

@app.route('/health')
def health():
    try:
        qry("SELECT 1", fetch='one')
        return jsonify({'status':'ok','db':'conectado','version':'3.5'})
    except Exception as e:
        return jsonify({'status':'erro','error':str(e)}), 500

# ── FIX 4: Rotas diagnóstico agora exigem token SECRET_KEY ──
@app.route('/setup-banco-agora')
def setup_banco():
    token = request.args.get('token', '')
    if token != os.getenv('SECRET_KEY', ''):
        return 'Acesso negado. Use ?token=SUA_SECRET_KEY', 403
    try:
        with open('schema_final.sql', 'r') as f:
            sql = f.read()
        conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()
        return 'Banco inicializado com sucesso!'
    except Exception as e:
        return f'Erro: {str(e)}'

@app.route('/aprovar-admin-agora')
def aprovar_admin():
    token = request.args.get('token', '')
    if token != os.getenv('SECRET_KEY', ''):
        return 'Acesso negado. Use ?token=SUA_SECRET_KEY', 403
    try:
        admin_email = os.environ.get('ADMIN_EMAIL')
        conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
        cur = conn.cursor()
        cur.execute("UPDATE users SET ativo=TRUE, aprovado=TRUE, role='admin' WHERE email=%s", (admin_email,))
        conn.commit()
        cur.close()
        conn.close()
        return f'Admin {admin_email} atualizado! <a href="/login">Fazer login</a>'
    except Exception as e:
        return f'Erro: {str(e)}'

# ═══════════════════════════════════════════════════════════════
# ROTAS E APIs ADICIONAIS (FIX 5: movidas para ANTES do if __name__)
# ═══════════════════════════════════════════════════════════════

# FIX 6: /parametros/nova-zona
@app.route('/parametros/nova-zona')
@login_required
def pagina_nova_zona():
    return render_template('parametros.html', active_page='nova-zona', active_group='parametros',
                           abrir_modal='nova_zona', **tmpl_ctx())

# FIX 7b: PUT /api/zona/<id> — atualizar zona existente
@app.route('/api/zona/<int:zona_id>', methods=['PUT'])
@editor_required
def api_atualizar_zona(zona_id):
    d = request.json or {}
    if not d:
        return jsonify({'success': False, 'error': 'Nenhum dado enviado'}), 400
    campos_proibidos = {'id', 'criado_em', 'municipio', 'zona', 'subzona'}
    cols = [k for k in d.keys() if k not in campos_proibidos]
    if not cols:
        return jsonify({'success': False, 'error': 'Nenhum campo válido para atualizar'}), 400
    set_sql = ', '.join(f"{c} = %s" for c in cols)
    vals = [d[c] for c in cols] + [datetime.now(), session['user_id'], zona_id]
    qry(f"UPDATE zonas_urbanas SET {set_sql}, atualizado_em = %s, atualizado_por = %s WHERE id = %s",
        vals, commit=True)
    return jsonify({'success': True})

# FIX 8b: /api/zonas com querystring (sem municipio no path)
@app.route('/api/zonas')
@login_required
def api_zonas_query():
    municipio = request.args.get('municipio')
    zona      = request.args.get('zona')
    estado    = request.args.get('estado')
    where = []
    params = []
    if municipio: where.append("municipio ILIKE %s"); params.append(f"%{municipio}%")
    if zona:      where.append("zona ILIKE %s");      params.append(f"%{zona}%")
    if estado:    where.append("estado ILIKE %s");    params.append(f"%{estado}%")
    where_sql = ('WHERE ' + ' AND '.join(where)) if where else ''
    data = qry(f"SELECT * FROM zonas_urbanas {where_sql} ORDER BY estado, municipio, zona LIMIT 200", params)
    return jsonify({'success': True, 'data': data, 'total': len(data)})

# FIX 9a: /api/parametros/importar — importar Excel
@app.route('/api/parametros/importar', methods=['POST'])
@editor_required
def api_parametros_importar():
    arquivo = request.files.get('arquivo')
    if not arquivo:
        return jsonify({'success': False, 'error': 'Nenhum arquivo enviado'}), 400
    if not PANDAS_OK:
        return jsonify({'success': False, 'error': 'Pandas não instalado'}), 500
    try:
        df = pd.read_excel(io.BytesIO(arquivo.read()))
        df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns]
        ok = erro = 0
        for _, row in df.iterrows():
            try:
                d = {k: (None if pd.isna(v) else v) for k, v in row.items()}
                municipio = str(d.get('municipio', '')).strip()
                zona      = str(d.get('zona', '')).strip()
                if not municipio or not zona:
                    erro += 1; continue
                subzona = str(d.get('subzona', '') or '')
                cols  = [k for k in d if d[k] is not None]
                vals  = [d[c] for c in cols]
                ph    = ', '.join(['%s'] * len(cols))
                upd   = ', '.join(f"{c}=EXCLUDED.{c}" for c in cols if c not in ('municipio','zona','subzona'))
                if upd:
                    qry(f"INSERT INTO zonas_urbanas ({','.join(cols)}) VALUES ({ph}) "
                        f"ON CONFLICT (municipio,zona,subzona) DO UPDATE SET {upd}, atualizado_em=NOW()",
                        vals, commit=True)
                else:
                    qry(f"INSERT INTO zonas_urbanas ({','.join(cols)}) VALUES ({ph}) ON CONFLICT DO NOTHING",
                        vals, commit=True)
                ok += 1
            except Exception as e:
                erro += 1
        return jsonify({'success': True, 'ok': ok, 'erro': erro,
                        'message': f'{ok} zonas importadas, {erro} erros'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# FIX 9b: /api/parametros/exportar — exportar Excel
@app.route('/api/parametros/exportar')
@login_required
def api_parametros_exportar():
    if not PANDAS_OK:
        return jsonify({'success': False, 'error': 'Pandas não instalado'}), 500
    try:
        data = qry("SELECT * FROM zonas_urbanas ORDER BY estado, municipio, zona, subzona")
        df = pd.DataFrame(data)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Parâmetros')
        output.seek(0)
        return send_file(output, as_attachment=True,
                         download_name=f'urbanlex_parametros_{datetime.now().strftime("%Y%m%d_%H%M")}.xlsx',
                         mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# FIX 10: /api/integracao/aprovadas e /rejeitadas
@app.route('/api/integracao/aprovadas')
@login_required
def api_integ_aprovadas():
    data = qry("""SELECT i.*, CONCAT(l.numero,'/',l.ano) as legislacao_ref, u.nome as revisor_nome
        FROM integracao_atualizacoes i
        LEFT JOIN legislacoes l ON i.legislacao_id=l.id
        LEFT JOIN users u ON i.revisado_por=u.id
        WHERE i.status='aprovado' ORDER BY i.revisado_em DESC LIMIT 100""")
    return jsonify({'success': True, 'data': data})

@app.route('/api/integracao/rejeitadas')
@login_required
def api_integ_rejeitadas():
    data = qry("""SELECT i.*, CONCAT(l.numero,'/',l.ano) as legislacao_ref, u.nome as revisor_nome
        FROM integracao_atualizacoes i
        LEFT JOIN legislacoes l ON i.legislacao_id=l.id
        LEFT JOIN users u ON i.revisado_por=u.id
        WHERE i.status='rejeitado' ORDER BY i.revisado_em DESC LIMIT 100""")
    return jsonify({'success': True, 'data': data})

# ═══════════════════════════════════════════════════════════════
# INICIALIZAÇÃO (FIX 6: inicializar() no nível do módulo)
# ═══════════════════════════════════════════════════════════════

def inicializar():
    print("UrbanLex v3.5 iniciando...")
    if SCHEDULER_OK:
        try: iniciar_scheduler(); print("✅ Scheduler iniciado")
        except Exception as e: print(f"⚠ Scheduler: {e}")

# FIX 6: Chamar inicializar() no nível do módulo para funcionar com gunicorn
inicializar()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT',5000)), debug=os.getenv('FLASK_ENV')!='production')
