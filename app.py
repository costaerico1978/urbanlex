"""
UrbanLex - Aplicacao Flask Principal v3.5
Parametros Urbanisticos + Biblioteca Legislativa + Monitoramento IA

CORRECOES APLICADAS (25/02/2026):
  BUG 1: DELETE legislacao usava id() em vez de leg_id + foreign keys
  BUG 2: Login API tinha sessao como codigo morto (indentacao)
  BUG 3: Login HTML mostrava erro no GET
  BUG 4: Rotas diagnostico sem autenticacao
  BUG 5: Rotas FIX 6-10 movidas para antes do if __name__
  BUG 6: inicializar() chamada no nivel do modulo
  BUG 7: Query alteracoes usa COALESCE para municipio_nome
  BUG 8: Adicionada rota GET /logout
  BUG 9: Login HTML usa qry() em vez de conexao manual
  BUG 10: Rota diagnostico R2 adicionada
  BUG 11: Rota re-upload de arquivo adicionada
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

# -- Auth helpers (adapters para compatibilidade) --
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
    """Retorna a URL base da aplicacao, com fallback para variavel de ambiente."""
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
        f'<p>Ola {user["nome"]},</p><p>Clique para ativar sua conta:</p>'
        f'<p><a href="{url}">{url}</a></p><p>Link valido por 24 horas.</p>')

def enviar_email_aprovacao_admin(user):
    app_url = get_app_url()
    token_apr = gerar_token()
    exp = datetime.now() + timedelta(days=7)
    qry("INSERT INTO aprovacao_tokens (user_id,token,tipo,expira_em) VALUES (%s,%s,'aprovacao',%s)",
        (user['id'], token_apr, exp), commit=True, fetch=None)
    admin_email = os.getenv('ADMIN_EMAIL','')
    if not admin_email: return
    url_apr = f"{app_url}/admin/aprovar/{token_apr}"
    url_rej = f"{app_url}/admin/rejeitar/{token_apr}"
    enviar_email_generico(admin_email, f'Novo cadastro: {user["nome"]}',
        f'<p>Novo usuario aguardando aprovacao: <strong>{user["nome"]}</strong> ({user["email"]})</p>'
        f'<p><a href="{url_apr}">Aprovar</a> &nbsp; <a href="{url_rej}">Rejeitar</a></p>')

def enviar_email_boas_vindas(user):
    enviar_email_generico(user['email'], 'Bem-vindo ao UrbanLex!',
        f'<p>Ola {user["nome"]}, sua conta foi aprovada! Acesse: {get_app_url()}/login</p>')

def enviar_email_rejeicao(user):
    enviar_email_generico(user['email'], 'Cadastro UrbanLex',
        f'<p>Ola {user["nome"]}, seu cadastro nao foi aprovado. Entre em contato com o administrador.</p>')

def enviar_email_reset(user, token):
    url = f"{get_app_url()}/reset-senha/{token}"
    enviar_email_generico(user['email'], 'Redefinicao de senha - UrbanLex',
        f'<p>Ola {user["nome"]},</p><p>Clique para redefinir sua senha:</p>'
        f'<p><a href="{url}">{url}</a></p><p>Link valido por 1 hora.</p>')

try:
    from modulos.scheduler_integrado import iniciar_scheduler
    SCHEDULER_OK = True
except ImportError:
    SCHEDULER_OK = False

# -- Cloudflare R2 storage (opcional) --
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

# Conversao de arquivos
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

# -- App --
app = Flask(__name__, template_folder='templates')
app.secret_key = os.getenv('SECRET_KEY', 'urbanlex-dev-key-change-in-prod')
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100MB
app.config['SESSION_COOKIE_SECURE'] = True
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['SESSION_COOKIE_HTTPONLY'] = True
ADMIN_EMAIL = os.getenv('ADMIN_EMAIL', '')

# -- DB --
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
        elif fetch == 'id':
            row = cur.fetchone()
            result = list(row.values())[0] if row else None
        if commit: conn.commit()
        return result
    finally:
        conn.close()

# -- Auth helpers --
def login_required(f):
    @wraps(f)
    def dec(*a, **k):
        if 'user_id' not in session:
            if request.path.startswith('/api/'): return jsonify({'error':'Nao autenticado'}), 401
            return redirect('/login')
        return f(*a, **k)
    return dec

def admin_required(f):
    @wraps(f)
    def dec(*a, **k):
        if 'user_id' not in session:
            if request.path.startswith('/api/'): return jsonify({'error':'Nao autenticado'}), 401
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
            if request.path.startswith('/api/'): return jsonify({'error':'Nao autenticado'}), 401
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


# -- Funcao auxiliar de busca com IA --
def _buscar_legislacao_internet(consulta: str) -> dict:
    """Tenta encontrar legislacao via GROQ ou busca simples."""
    try:
        from groq import Groq
        client = Groq(api_key=os.getenv('GROQ_API_KEY',''))
        prompt = (f"Encontre a seguinte legislacao urbanistica brasileira: '{consulta}'. "
                  "Retorne APENAS um JSON com: titulo, estado, municipio, numero, ano, url. "
                  "Se nao encontrar, retorne {{}}.")
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


# -------------------------------------------------------------------
# PAGINAS HTML
# -------------------------------------------------------------------

# -- FIX 3: Login corrigido -- erro so aparece no POST com falha --
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
            # FIX 9: usar qry() em vez de conexao manual
            qry("UPDATE users SET ultimo_acesso=NOW() WHERE id=%s", (user['id'],), commit=True, fetch=None)
            return redirect('/')
        error = 'E-mail ou senha incorretos'
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
            erro='Este link e invalido ou ja expirou. Solicite um novo.', **tmpl_ctx())
    return render_template('reset_senha.html', token=token, erro=None, **tmpl_ctx())

@app.route('/ativar/<token>')
def pagina_ativar(token):
    tk = qry("SELECT * FROM aprovacao_tokens WHERE token=%s AND tipo='ativacao' AND usado=FALSE AND expira_em>NOW()", (token,), 'one')
    if not tk: return render_template('conta_ativada.html', sucesso=False, msg='Link invalido ou expirado.', **tmpl_ctx())
    qry("UPDATE users SET ativo=TRUE WHERE id=%s", (tk['user_id'],), commit=True, fetch=None)
    qry("UPDATE aprovacao_tokens SET usado=TRUE WHERE id=%s", (tk['id'],), commit=True, fetch=None)
    user = qry("SELECT * FROM users WHERE id=%s", (tk['user_id'],), 'one')
    if ADMIN_EMAIL: enviar_email_aprovacao_admin(user)
    return render_template('conta_ativada.html', sucesso=True,
        msg='Conta ativada! Aguardando aprovacao do administrador.',
        nome=user.get('nome',''), email=user.get('email',''), **tmpl_ctx())

@app.route('/admin/aprovar/<token>')
def admin_aprovar(token):
    tk = qry("SELECT * FROM aprovacao_tokens WHERE token=%s AND tipo='aprovacao' AND usado=FALSE", (token,), 'one')
    if not tk: return render_template('resultado_aprovacao.html', sucesso=False, msg='Link invalido.', **tmpl_ctx())
    qry("UPDATE users SET aprovado=TRUE WHERE id=%s", (tk['user_id'],), commit=True, fetch=None)
    qry("UPDATE aprovacao_tokens SET usado=TRUE WHERE id=%s", (tk['id'],), commit=True, fetch=None)
    user = qry("SELECT * FROM users WHERE id=%s", (tk['user_id'],), 'one')
    if not user:
        return render_template('resultado_aprovacao.html', sucesso=False, msg='Usuario nao encontrado.', nome='', email='', **tmpl_ctx())
    enviar_email_boas_vindas(user)
    return render_template('resultado_aprovacao.html', sucesso=True,
        msg=f'Usuario {user["nome"]} aprovado com sucesso!',
        nome=user.get('nome',''), email=user.get('email',''), **tmpl_ctx())

@app.route('/admin/rejeitar/<token>')
def admin_rejeitar(token):
    tk = qry("SELECT * FROM aprovacao_tokens WHERE token=%s AND tipo='aprovacao' AND usado=FALSE", (token,), 'one')
    if not tk: return render_template('resultado_aprovacao.html', sucesso=False, msg='Link invalido.', **tmpl_ctx())
    qry("UPDATE users SET aprovado=FALSE, ativo=FALSE WHERE id=%s", (tk['user_id'],), commit=True, fetch=None)
    qry("UPDATE aprovacao_tokens SET usado=TRUE WHERE id=%s", (tk['id'],), commit=True, fetch=None)
    user = qry("SELECT * FROM users WHERE id=%s", (tk['user_id'],), 'one')
    if user: enviar_email_rejeicao(user)
    nome_rej = user.get('nome','usuario') if user else 'usuario'
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
def pagina_monitoramento(): return render_template('monitoramento.html', active_page='municipios', active_group='monitoramento', **tmpl_ctx())

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

# -------------------------------------------------------------------
# API: AUTH
# -------------------------------------------------------------------

# -- FIX 2: Login API corrigido -- sessao agora e criada corretamente --
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

# -- FIX 8: Rota GET para logout --
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
    if not nome or not email or not senha: return jsonify({'success':False,'error':'Campos obrigatorios'}), 400
    if qry("SELECT id FROM users WHERE email=%s", (email,), 'one'): return jsonify({'success':False,'error':'E-mail ja cadastrado'}), 400
    import re
    if not re.match(r'^(?=.*[A-Z])(?=.*[a-z])(?=.*\d)(?=.*[@$!%*?&\-_#])[A-Za-z\d@$!%*?&\-_#]{6,15}$', senha):
        return jsonify({'success':False,'error':'Senha fraca. Use 6-15 chars com maiuscula, minuscula, numero e especial.'}), 400
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
        qry("INSERT INTO password_reset_tokens (user_id,token,expira_em) VALUES (%s,%s,%s)", (user['id'],token,exp), commit=True, fetch=None)
        enviar_email_reset(user, token)
    return jsonify({'success':True})

@app.route('/api/auth/reset-senha', methods=['POST'])
def api_reset_senha():
    d = request.json or {}
    token = d.get('token','')
    senha = d.get('senha','')
    tk = qry("SELECT * FROM password_reset_tokens WHERE token=%s AND usado=FALSE AND expira_em>NOW()", (token,), 'one')
    if not tk: return jsonify({'success':False,'error':'Link invalido ou expirado'}), 400
    qry("UPDATE users SET senha_hash=%s WHERE id=%s", (hash_senha(senha), tk['user_id']), commit=True, fetch=None)
    qry("UPDATE password_reset_tokens SET usado=TRUE WHERE id=%s", (tk['id'],), commit=True, fetch=None)
    return jsonify({'success':True})

@app.route('/api/auth/alterar-senha', methods=['POST'])
@login_required
def api_alterar_senha():
    d = request.json or {}
    user = qry("SELECT * FROM users WHERE id=%s", (session['user_id'],), 'one')
    if not user: return jsonify({'success':False,'error':'Usuario nao encontrado'}), 404
    if not verificar_senha(d.get('senha_atual',''), user['senha_hash']):
        return jsonify({'success':False,'error':'Senha atual incorreta'}), 400
    qry("UPDATE users SET senha_hash=%s WHERE id=%s", (hash_senha(d['senha_nova']), session['user_id']), commit=True, fetch=None)
    return jsonify({'success':True})

@app.route('/api/auth/perfil', methods=['GET','POST'])
@login_required
def api_perfil():
    if request.method == 'GET':
        user = qry("SELECT id,nome,email,role FROM users WHERE id=%s", (session['user_id'],), 'one')
        return jsonify({'success':True,'data':user})
    d = request.json or {}
    nome = d.get('nome','').strip()
    if not nome: return jsonify({'success':False,'error':'Nome obrigatorio'}), 400
    qry("UPDATE users SET nome=%s WHERE id=%s", (nome, session['user_id']), commit=True, fetch=None)
    session['nome'] = nome
    return jsonify({'success':True})

# -------------------------------------------------------------------
# API: ADMIN USUARIOS
# -------------------------------------------------------------------

@app.route('/api/admin/usuarios', methods=['GET'])
@admin_required
def api_listar_usuarios():
    users = qry("SELECT id,nome,email,role,ativo,aprovado,criado_em,ultimo_acesso FROM users ORDER BY criado_em DESC")
    return jsonify({'success':True,'data':users})

@app.route('/api/admin/usuarios/<int:uid>/role', methods=['POST'])
@admin_required
def api_alterar_role(uid):
    role = (request.json or {}).get('role')
    if role not in ('admin','editor','apenas_leitura'): return jsonify({'success':False,'error':'Role invalida'}), 400
    qry("UPDATE users SET role=%s WHERE id=%s", (role, uid), commit=True, fetch=None)
    return jsonify({'success':True})

@app.route('/api/admin/usuarios/<int:uid>', methods=['DELETE'])
@admin_required
def api_excluir_usuario(uid):
    if uid == session['user_id']: return jsonify({'success':False,'error':'Nao pode excluir a si mesmo'}), 400
    qry("DELETE FROM users WHERE id=%s", (uid,), commit=True, fetch=None)
    return jsonify({'success':True})

# -------------------------------------------------------------------
# API: BIBLIOTECA DE LEGISLACOES
# -------------------------------------------------------------------

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
    data = qry(f"""SELECT l.*, tl.nome as tipo_nome, al.nome as assunto_nome,
                   COALESCE(fa.qtd_arquivos, 0) as qtd_arquivos
                   FROM legislacoes l
                   LEFT JOIN tipos_legislacao tl ON l.tipo_id=tl.id
                   LEFT JOIN assuntos_legislacao al ON l.assunto_id=al.id
                   LEFT JOIN (SELECT legislacao_id, COUNT(*) as qtd_arquivos FROM legislacao_arquivos GROUP BY legislacao_id) fa ON fa.legislacao_id=l.id
                   WHERE {where_sql} ORDER BY {order} LIMIT %s OFFSET %s""",
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

    tipo_row = qry("SELECT nome FROM tipos_legislacao WHERE id=%s", (tipo_id,), 'one') if tipo_id else None
    assunto_row = qry("SELECT nome FROM assuntos_legislacao WHERE id=%s", (assunto_id,), 'one') if assunto_id else None

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

    url_r2 = None
    if novo_id and r2_disponivel():
        url_r2 = r2_upload(arquivo_bytes, nome_arquivo, leg_id=novo_id)
        if url_r2:
            qry("UPDATE legislacoes SET arquivo_url=%s WHERE id=%s", (url_r2, novo_id), commit=True, fetch=None)

    # Inserir tambem na tabela de arquivos (multi-arquivo)
    if novo_id:
        qry("""INSERT INTO legislacao_arquivos
               (legislacao_id, nome_arquivo, arquivo_tipo, arquivo_url,
                tamanho_bytes, hash_conteudo, conteudo_texto, criado_por)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
            (novo_id, nome_arquivo, Path(nome_arquivo).suffix.lower().lstrip('.'),
             url_r2, len(arquivo_bytes), hash_c, texto, session['user_id']),
            commit=True, fetch=None)

    return jsonify({'success':True,'id':novo_id})

@app.route('/api/legislacoes/<int:leg_id>', methods=['GET'])
@login_required
def api_get_legislacao(leg_id):
    l = qry("SELECT l.*, tl.nome as tipo_nome, al.nome as assunto_nome FROM legislacoes l LEFT JOIN tipos_legislacao tl ON l.tipo_id=tl.id LEFT JOIN assuntos_legislacao al ON l.assunto_id=al.id WHERE l.id=%s", (leg_id,), 'one')
    if not l: return jsonify({'success':False,'error':'Nao encontrada'}), 404
    arquivos = qry("""SELECT id, nome_arquivo, arquivo_tipo, tamanho_bytes, criado_em
                      FROM legislacao_arquivos WHERE legislacao_id=%s ORDER BY criado_em""", (leg_id,))
    l['arquivos'] = arquivos or []
    l['total_arquivos_bytes'] = sum(a.get('tamanho_bytes', 0) or 0 for a in (arquivos or []))
    # Incluir relacoes categorizadas
    rels = qry("""
        SELECT r.id as relacao_id, r.tipo_relacao,
               r.legislacao_pai_id, r.legislacao_filha_id,
               lp.numero as pai_numero, lp.ano as pai_ano, tlp.nome as pai_tipo,
               lf.numero as filha_numero, lf.ano as filha_ano, tlf.nome as filha_tipo
        FROM legislacao_relacoes r
        JOIN legislacoes lp ON r.legislacao_pai_id = lp.id
        JOIN legislacoes lf ON r.legislacao_filha_id = lf.id
        LEFT JOIN tipos_legislacao tlp ON lp.tipo_id = tlp.id
        LEFT JOIN tipos_legislacao tlf ON lf.tipo_id = tlf.id
        WHERE r.legislacao_pai_id = %s OR r.legislacao_filha_id = %s
    """, (leg_id, leg_id))
    relacoes = {'revogada_por':[],'modificada_por':[],'modifica':[],'revoga':[],'citadas':[],'citada_por':[]}
    for r in (rels or []):
        tipo = (r['tipo_relacao'] or '').lower().strip()
        def mk(prefix):
            return {'id':r[f'legislacao_{prefix}_id'],'numero':r[f'{prefix}_numero'],'ano':r[f'{prefix}_ano'],
                    'tipo_nome':r[f'{prefix}_tipo'],'relacao_id':r['relacao_id'],
                    'ref':f"{r[f'{prefix}_tipo'] or ''} {r[f'{prefix}_numero'] or ''}/{r[f'{prefix}_ano'] or ''}".strip()}
        if tipo in ('revoga','revogacao'):
            if r['legislacao_filha_id']==leg_id: relacoes['revogada_por'].append(mk('pai'))
            else: relacoes['revoga'].append(mk('filha'))
        elif tipo in ('modifica','altera','alteracao','modificacao'):
            if r['legislacao_filha_id']==leg_id: relacoes['modificada_por'].append(mk('pai'))
            else: relacoes['modifica'].append(mk('filha'))
        elif tipo in ('cita','referencia','citacao','menciona'):
            if r['legislacao_pai_id']==leg_id: relacoes['citadas'].append(mk('filha'))
            else: relacoes['citada_por'].append(mk('pai'))
    l['relacoes'] = relacoes
    return jsonify({'success':True,'data':l})

# -- FIX 1: DELETE corrigido -- leg_id + foreign keys + verificacao --
@app.route('/api/legislacoes/<int:leg_id>', methods=['DELETE'])
@editor_required
def api_excluir_legislacao(leg_id):
    leg = qry("SELECT arquivo_url FROM legislacoes WHERE id=%s", (leg_id,), 'one')
    if not leg:
        return jsonify({'success': False, 'error': 'Legislacao nao encontrada'}), 404
    if leg.get('arquivo_url') and r2_disponivel():
        r2_delete(leg['arquivo_url'])
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
    if not l or not l.get('conteudo_texto'): return jsonify({'success':False,'error':'Texto nao disponivel'}), 404
    return jsonify({'success':True,'data':{'texto':l['conteudo_texto'],'arquivo_nome':l.get('arquivo_nome','')}})

@app.route('/api/legislacoes/<int:leg_id>/documento')
@login_required
def api_leg_documento(leg_id):
    l = qry("SELECT arquivo_url, arquivo_nome FROM legislacoes WHERE id=%s", (leg_id,), 'one')
    if not l: return jsonify({'error':'Nao encontrada'}), 404
    url = l.get('arquivo_url')
    if not url:
        return jsonify({'error':'Documento nao disponivel. Faca upload do arquivo.'}), 404
    if url.startswith('http'):
        return redirect(url)
    if r2_disponivel():
        url_assinada = r2_url_assinada(url, expiracao_seg=3600)
        if url_assinada:
            return redirect(url_assinada)
    return jsonify({'error':'Documento armazenado no R2 mas nao foi possivel gerar URL de acesso.'}), 503

@app.route('/api/legislacoes/<int:leg_id>/monitoramento', methods=['POST'])
@editor_required
def api_toggle_monitoramento(leg_id):
    d      = request.json or {}
    ativar = d.get('ativar', True)
    data_inicio = d.get('data_inicio_monitoramento') or None
    data_fim    = d.get('data_fim_monitoramento') or None
    qry("""UPDATE legislacoes
           SET em_monitoramento=%s,
               data_inicio_monitoramento=%s,
               data_fim_monitoramento=%s,
               ultima_verificacao_monitoramento = NULL
           WHERE id=%s""",
        (ativar, data_inicio, data_fim, leg_id), commit=True, fetch=None)
    return jsonify({'success':True})


@app.route('/api/legislacoes/<int:leg_id>/periodo-monitoramento', methods=['POST'])
@editor_required
def api_atualizar_periodo_monitoramento(leg_id):
    """Atualiza período de monitoramento sem alterar o estado ativo/inativo."""
    d = request.json or {}
    data_inicio = d.get('data_inicio') or None
    data_fim    = d.get('data_fim') or None   # NULL = até hoje
    # Se a data de início mudou para antes da última verificação, resetar verificação
    leg = qry("SELECT data_inicio_monitoramento, ultima_verificacao_monitoramento FROM legislacoes WHERE id=%s",
              (leg_id,), 'one')
    reset_verificacao = False
    if leg and data_inicio:
        from datetime import date as _date_type
        try:
            nova_data = _date_type.fromisoformat(data_inicio) if isinstance(data_inicio, str) else data_inicio
            verif_atual = leg.get('ultima_verificacao_monitoramento')
            inicio_atual = leg.get('data_inicio_monitoramento')
            if inicio_atual and nova_data < inicio_atual:
                reset_verificacao = True
            if verif_atual and nova_data < verif_atual:
                reset_verificacao = True
        except Exception:
            pass
    if reset_verificacao:
        qry("""UPDATE legislacoes
               SET data_inicio_monitoramento=%s, data_fim_monitoramento=%s,
                   ultima_verificacao_monitoramento=NULL
               WHERE id=%s""",
            (data_inicio, data_fim, leg_id), commit=True, fetch=None)
    else:
        qry("""UPDATE legislacoes
               SET data_inicio_monitoramento=%s, data_fim_monitoramento=%s
               WHERE id=%s""",
            (data_inicio, data_fim, leg_id), commit=True, fetch=None)
    return jsonify({'success': True, 'reset_verificacao': reset_verificacao})

# -- Re-upload de arquivo para legislacao existente --
@app.route('/api/legislacoes/<int:leg_id>/upload', methods=['POST'])
@editor_required
def api_reupload_legislacao(leg_id):
    """Faz upload/re-upload de arquivo para uma legislacao existente."""
    leg = qry("SELECT id, arquivo_url FROM legislacoes WHERE id=%s", (leg_id,), 'one')
    if not leg:
        return jsonify({'success': False, 'error': 'Legislacao nao encontrada'}), 404
    arquivo = request.files.get('arquivo')
    if not arquivo:
        return jsonify({'success': False, 'error': 'Nenhum arquivo enviado'}), 400
    nome_arquivo = secure_filename(arquivo.filename)
    arquivo_bytes = arquivo.read()
    texto = extrair_texto_arquivo(arquivo_bytes, nome_arquivo)
    hash_c = hashlib.sha256(arquivo_bytes).hexdigest()
    if leg.get('arquivo_url') and r2_disponivel():
        r2_delete(leg['arquivo_url'])
    url_r2 = None
    if r2_disponivel():
        url_r2 = r2_upload(arquivo_bytes, nome_arquivo, leg_id=leg_id)
    qry("""UPDATE legislacoes
           SET arquivo_nome=%s, arquivo_tipo=%s, arquivo_url=%s,
               conteudo_texto=%s, hash_conteudo=%s, atualizado_em=NOW()
           WHERE id=%s""",
        (nome_arquivo, Path(nome_arquivo).suffix.lower()[1:],
         url_r2, texto, hash_c, leg_id),
        commit=True, fetch=None)
    return jsonify({
        'success': True,
        'arquivo_url': url_r2,
        'texto_extraido': bool(texto),
        'message': 'Arquivo salvo no R2' if url_r2 else 'Arquivo salvo (R2 indisponivel - apenas texto extraido)'
    })

# -- Editar campos da legislacao --
@app.route('/api/legislacoes/<int:leg_id>', methods=['PUT'])
@editor_required
def api_editar_legislacao(leg_id):
    """Atualiza campos editaveis de uma legislacao."""
    leg = qry("SELECT id FROM legislacoes WHERE id=%s", (leg_id,), 'one')
    if not leg:
        return jsonify({'success': False, 'error': 'Legislacao nao encontrada'}), 404
    d = request.json or {}
    if not d:
        return jsonify({'success': False, 'error': 'Nenhum dado enviado'}), 400
    campos_editaveis = {
        'numero', 'ano', 'ementa', 'status', 'estado', 'municipio_nome',
        'tipo_id', 'assunto_id', 'palavras_chave', 'data_publicacao',
        'esfera', 'em_monitoramento', 'data_inicio_monitoramento',
        'data_fim_monitoramento', 'url_original', 'observacoes'
    }
    cols = [k for k in d.keys() if k in campos_editaveis]
    if not cols:
        return jsonify({'success': False, 'error': 'Nenhum campo valido para atualizar'}), 400
    vals = []
    for c in cols:
        v = d[c]
        if c == 'ano' and v is not None:
            v = int(v)
        if c == 'em_monitoramento':
            v = bool(v)
        if c == 'palavras_chave' and isinstance(v, list):
            v = json.dumps(v)
        vals.append(v)
    # Atualizar nomes de tipo e assunto se ids mudaram
    extra_sets = []
    extra_vals = []
    if 'tipo_id' in d:
        tipo_row = qry("SELECT nome FROM tipos_legislacao WHERE id=%s", (d['tipo_id'],), 'one') if d['tipo_id'] else None
        extra_sets.append("tipo_nome=%s")
        extra_vals.append(tipo_row['nome'] if tipo_row else None)
    if 'assunto_id' in d:
        assunto_row = qry("SELECT nome FROM assuntos_legislacao WHERE id=%s", (d['assunto_id'],), 'one') if d['assunto_id'] else None
        extra_sets.append("assunto_nome=%s")
        extra_vals.append(assunto_row['nome'] if assunto_row else None)
    set_sql = ', '.join(f"{c}=%s" for c in cols)
    if extra_sets:
        set_sql += ', ' + ', '.join(extra_sets)
    set_sql += ', atualizado_em=NOW()'
    all_vals = vals + extra_vals + [leg_id]
    qry(f"UPDATE legislacoes SET {set_sql} WHERE id=%s", all_vals, commit=True, fetch=None)
    # Retornar legislacao atualizada
    updated = qry("SELECT l.*, tl.nome as tipo_nome, al.nome as assunto_nome FROM legislacoes l LEFT JOIN tipos_legislacao tl ON l.tipo_id=tl.id LEFT JOIN assuntos_legislacao al ON l.assunto_id=al.id WHERE l.id=%s", (leg_id,), 'one')
    return jsonify({'success': True, 'data': updated})

# -------------------------------------------------------------------
# API: GESTAO DE ARQUIVOS POR LEGISLACAO (multi-arquivo)
# -------------------------------------------------------------------

MAX_TOTAL_ARQUIVOS_BYTES = 100 * 1024 * 1024  # 100MB por legislacao

@app.route('/api/legislacoes/<int:leg_id>/arquivos', methods=['GET'])
@login_required
def api_listar_arquivos(leg_id):
    """Lista todos os arquivos associados a uma legislacao."""
    leg = qry("SELECT id FROM legislacoes WHERE id=%s", (leg_id,), 'one')
    if not leg:
        return jsonify({'success': False, 'error': 'Legislacao nao encontrada'}), 404
    arquivos = qry("""SELECT id, legislacao_id, nome_arquivo, arquivo_tipo,
                             tamanho_bytes, criado_em, criado_por
                      FROM legislacao_arquivos
                      WHERE legislacao_id=%s ORDER BY criado_em""", (leg_id,))
    total_bytes = sum(a.get('tamanho_bytes', 0) or 0 for a in arquivos)
    # Incluir tambem o arquivo principal da legislacao (legado)
    leg_principal = qry("SELECT arquivo_nome, arquivo_tipo, arquivo_url FROM legislacoes WHERE id=%s AND arquivo_nome IS NOT NULL", (leg_id,), 'one')
    return jsonify({
        'success': True,
        'data': arquivos,
        'arquivo_principal': leg_principal,
        'total_bytes': total_bytes,
        'limite_bytes': MAX_TOTAL_ARQUIVOS_BYTES,
        'espaco_disponivel': MAX_TOTAL_ARQUIVOS_BYTES - total_bytes
    })

@app.route('/api/legislacoes/<int:leg_id>/arquivos', methods=['POST'])
@editor_required
def api_upload_arquivo(leg_id):
    """Upload de um ou mais arquivos para uma legislacao (limite total: 100MB)."""
    leg = qry("SELECT id FROM legislacoes WHERE id=%s", (leg_id,), 'one')
    if not leg:
        return jsonify({'success': False, 'error': 'Legislacao nao encontrada'}), 404

    arquivos = request.files.getlist('arquivos')
    if not arquivos or all(f.filename == '' for f in arquivos):
        return jsonify({'success': False, 'error': 'Nenhum arquivo enviado'}), 400

    # Verificar espaco total disponivel
    usado = qry("SELECT COALESCE(SUM(tamanho_bytes),0) as total FROM legislacao_arquivos WHERE legislacao_id=%s",
                 (leg_id,), 'one')['total']
    espaco_livre = MAX_TOTAL_ARQUIVOS_BYTES - usado

    resultados = []
    erros = []
    for arquivo in arquivos:
        if not arquivo or arquivo.filename == '':
            continue
        nome_arquivo = secure_filename(arquivo.filename)
        arquivo_bytes = arquivo.read()
        tamanho = len(arquivo_bytes)

        if tamanho > espaco_livre:
            erros.append(f"{nome_arquivo}: excede o limite de 100MB (disponivel: {espaco_livre // (1024*1024)}MB)")
            continue

        hash_c = hashlib.sha256(arquivo_bytes).hexdigest()
        texto = extrair_texto_arquivo(arquivo_bytes, nome_arquivo)

        # Upload para R2
        url_r2 = None
        if r2_disponivel():
            r2_key = f"legislacoes/{leg_id}/arquivos/{hash_c[:8]}_{nome_arquivo}"
            url_r2 = r2_upload(arquivo_bytes, nome_arquivo, leg_id=leg_id)

        # Inserir registro
        row = qry("""INSERT INTO legislacao_arquivos
                      (legislacao_id, nome_arquivo, arquivo_tipo, arquivo_url,
                       tamanho_bytes, hash_conteudo, conteudo_texto, criado_por)
                      VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id, nome_arquivo, tamanho_bytes, criado_em""",
                   (leg_id, nome_arquivo, Path(nome_arquivo).suffix.lower().lstrip('.'),
                    url_r2, tamanho, hash_c, texto, session['user_id']),
                   'one', commit=True)

        espaco_livre -= tamanho
        resultados.append(row)

    # Atualizar texto concatenado na legislacao (opcional - para busca)
    todos_textos = qry("SELECT conteudo_texto FROM legislacao_arquivos WHERE legislacao_id=%s AND conteudo_texto IS NOT NULL", (leg_id,))
    if todos_textos:
        texto_completo = '\n\n---\n\n'.join(t['conteudo_texto'] for t in todos_textos if t.get('conteudo_texto'))
        if texto_completo:
            qry("UPDATE legislacoes SET conteudo_texto=%s, atualizado_em=NOW() WHERE id=%s",
                (texto_completo, leg_id), commit=True, fetch=None)

    return jsonify({
        'success': True,
        'arquivos': resultados,
        'erros': erros,
        'espaco_disponivel': espaco_livre
    })

@app.route('/api/legislacoes/<int:leg_id>/arquivos/<int:arq_id>', methods=['DELETE'])
@editor_required
def api_excluir_arquivo(leg_id, arq_id):
    """Exclui um arquivo especifico de uma legislacao."""
    arq = qry("SELECT * FROM legislacao_arquivos WHERE id=%s AND legislacao_id=%s", (arq_id, leg_id), 'one')
    if not arq:
        return jsonify({'success': False, 'error': 'Arquivo nao encontrado'}), 404

    # Deletar do R2
    if arq.get('arquivo_url') and r2_disponivel():
        r2_delete(arq['arquivo_url'])

    qry("DELETE FROM legislacao_arquivos WHERE id=%s", (arq_id,), commit=True, fetch=None)

    return jsonify({'success': True, 'message': f"Arquivo '{arq['nome_arquivo']}' excluido"})

@app.route('/api/legislacoes/<int:leg_id>/arquivos/<int:arq_id>/download')
@login_required
def api_download_arquivo(leg_id, arq_id):
    """Gera URL de download para um arquivo especifico."""
    arq = qry("SELECT * FROM legislacao_arquivos WHERE id=%s AND legislacao_id=%s", (arq_id, leg_id), 'one')
    if not arq:
        return jsonify({'error': 'Arquivo nao encontrado'}), 404
    url = arq.get('arquivo_url')
    if not url:
        return jsonify({'error': 'Arquivo sem URL de acesso'}), 404
    if url.startswith('http'):
        return redirect(url)
    if r2_disponivel():
        url_assinada = r2_url_assinada(url, expiracao_seg=3600)
        if url_assinada:
            return redirect(url_assinada)
    return jsonify({'error': 'Nao foi possivel gerar URL de download'}), 503

# -- Arvore genealogica --
@app.route('/api/legislacoes/<int:leg_id>/arvore')
@login_required
def api_leg_arvore(leg_id):
    raiz = qry("SELECT l.*, tl.nome as tipo_nome FROM legislacoes l LEFT JOIN tipos_legislacao tl ON l.tipo_id=tl.id WHERE l.id=%s", (leg_id,), 'one')
    if not raiz: return jsonify({'success':False,'error':'Nao encontrada'}), 404
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

# -- Relacoes categorizadas de uma legislacao --
@app.route('/api/legislacoes/<int:leg_id>/relacoes')
@login_required
def api_leg_relacoes(leg_id):
    """Retorna relacoes categorizadas: revogada_por, modificada_por, modifica, citadas."""
    leg = qry("SELECT id FROM legislacoes WHERE id=%s", (leg_id,), 'one')
    if not leg:
        return jsonify({'success': False, 'error': 'Legislacao nao encontrada'}), 404

    # Buscar todas relacoes envolvendo esta legislacao
    rels = qry("""
        SELECT r.id as relacao_id, r.tipo_relacao,
               r.legislacao_pai_id, r.legislacao_filha_id,
               lp.numero as pai_numero, lp.ano as pai_ano, tlp.nome as pai_tipo,
               lp.municipio_nome as pai_municipio, lp.status as pai_status,
               lf.numero as filha_numero, lf.ano as filha_ano, tlf.nome as filha_tipo,
               lf.municipio_nome as filha_municipio, lf.status as filha_status
        FROM legislacao_relacoes r
        JOIN legislacoes lp ON r.legislacao_pai_id = lp.id
        JOIN legislacoes lf ON r.legislacao_filha_id = lf.id
        LEFT JOIN tipos_legislacao tlp ON lp.tipo_id = tlp.id
        LEFT JOIN tipos_legislacao tlf ON lf.tipo_id = tlf.id
        WHERE r.legislacao_pai_id = %s OR r.legislacao_filha_id = %s
    """, (leg_id, leg_id))

    resultado = {
        'revogada_por': [],
        'modificada_por': [],
        'modifica': [],
        'revoga': [],
        'citadas': [],
        'citada_por': []
    }

    for r in (rels or []):
        tipo = (r['tipo_relacao'] or '').lower().strip()
        pai_id = r['legislacao_pai_id']
        filha_id = r['legislacao_filha_id']

        def info_leg(prefix):
            return {
                'id': r[f'legislacao_{prefix}_id'],
                'numero': r[f'{prefix}_numero'],
                'ano': r[f'{prefix}_ano'],
                'tipo_nome': r[f'{prefix}_tipo'],
                'municipio_nome': r[f'{prefix}_municipio'],
                'status': r[f'{prefix}_status'],
                'relacao_id': r['relacao_id'],
                'ref': f"{r[f'{prefix}_tipo'] or ''} {r[f'{prefix}_numero'] or ''}/{r[f'{prefix}_ano'] or ''}".strip()
            }

        if tipo in ('revoga', 'revogacao'):
            if filha_id == leg_id:
                # Esta legislacao foi revogada pela pai
                resultado['revogada_por'].append(info_leg('pai'))
            else:
                # Esta legislacao revoga a filha
                resultado['revoga'].append(info_leg('filha'))

        elif tipo in ('modifica', 'altera', 'alteracao', 'modificacao'):
            if filha_id == leg_id:
                # Esta legislacao foi modificada pela pai
                resultado['modificada_por'].append(info_leg('pai'))
            else:
                # Esta legislacao modifica a filha
                resultado['modifica'].append(info_leg('filha'))

        elif tipo in ('cita', 'referencia', 'citacao', 'menciona'):
            if pai_id == leg_id:
                # Esta legislacao cita a filha
                resultado['citadas'].append(info_leg('filha'))
            else:
                # Esta legislacao e citada pela pai
                resultado['citada_por'].append(info_leg('pai'))

    return jsonify({'success': True, 'data': resultado})

@app.route('/api/legislacoes/<int:leg_id>/relacoes', methods=['POST'])
@editor_required
def api_add_relacao(leg_id):
    """Adiciona uma relacao entre legislacoes."""
    d = request.json or {}
    tipo_relacao = d.get('tipo_relacao', '').strip()
    outra_leg_id = d.get('outra_legislacao_id')
    direcao = d.get('direcao', 'pai')  # 'pai' = outra e pai, 'filha' = outra e filha

    if not tipo_relacao or not outra_leg_id:
        return jsonify({'success': False, 'error': 'tipo_relacao e outra_legislacao_id obrigatorios'}), 400

    outra = qry("SELECT id FROM legislacoes WHERE id=%s", (outra_leg_id,), 'one')
    if not outra:
        return jsonify({'success': False, 'error': 'Legislacao relacionada nao encontrada'}), 404

    if direcao == 'pai':
        pai_id, filha_id = outra_leg_id, leg_id
    else:
        pai_id, filha_id = leg_id, outra_leg_id

    # Verificar se ja existe
    existente = qry("""SELECT id FROM legislacao_relacoes
                       WHERE legislacao_pai_id=%s AND legislacao_filha_id=%s AND tipo_relacao=%s""",
                    (pai_id, filha_id, tipo_relacao), 'one')
    if existente:
        return jsonify({'success': False, 'error': 'Relacao ja existe'}), 400

    qry("""INSERT INTO legislacao_relacoes (legislacao_pai_id, legislacao_filha_id, tipo_relacao)
           VALUES (%s, %s, %s)""",
        (pai_id, filha_id, tipo_relacao), commit=True, fetch=None)

    return jsonify({'success': True})

@app.route('/api/legislacoes/<int:leg_id>/relacoes/<int:rel_id>', methods=['DELETE'])
@editor_required
def api_del_relacao(leg_id, rel_id):
    """Remove uma relacao."""
    qry("DELETE FROM legislacao_relacoes WHERE id=%s AND (legislacao_pai_id=%s OR legislacao_filha_id=%s)",
        (rel_id, leg_id, leg_id), commit=True, fetch=None)
    return jsonify({'success': True})

@app.route('/api/legislacoes/busca-rapida')
@login_required
def api_busca_rapida_legislacoes():
    """Busca rapida por numero/ano para vincular relacoes."""
    q = request.args.get('q', '').strip()
    if len(q) < 2:
        return jsonify({'success': True, 'data': []})
    data = qry("""SELECT l.id, l.numero, l.ano, tl.nome as tipo_nome, l.municipio_nome, l.status
                  FROM legislacoes l LEFT JOIN tipos_legislacao tl ON l.tipo_id=tl.id
                  WHERE l.pendente_aprovacao=FALSE AND (
                    l.numero ILIKE %s OR CAST(l.ano AS TEXT) ILIKE %s
                    OR tl.nome ILIKE %s OR l.municipio_nome ILIKE %s
                    OR l.ementa ILIKE %s
                  ) ORDER BY l.ano DESC, l.numero LIMIT 20""",
               (f'%{q}%', f'%{q}%', f'%{q}%', f'%{q}%', f'%{q}%'))
    return jsonify({'success': True, 'data': data})

# -- Busca com IA --
@app.route('/api/legislacoes/buscar-ia', methods=['POST'])
@editor_required
def api_buscar_ia():
    consulta = (request.json or {}).get('consulta','').strip()
    if not consulta: return jsonify({'success':False,'error':'Consulta obrigatoria'}), 400
    leg_id = qry("INSERT INTO buscas_ia (consulta,status,solicitado_por,criado_em) VALUES (%s,'pendente',%s,NOW()) RETURNING id",
                 (consulta, session['user_id']), 'one', commit=True)
    def rodar_busca(bid, consul):
        try:
            resultado = _buscar_legislacao_internet(consul)
            if resultado and resultado.get('url'):
                qry("UPDATE buscas_ia SET status='encontrado',resultado_url=%s,resultado_nome=%s,finalizado_em=NOW() WHERE id=%s",
                    (resultado['url'],resultado.get('titulo',''),bid), commit=True, fetch=None)
                qry("""INSERT INTO legislacoes (municipio_nome,estado,ementa,url_original,origem,pendente_aprovacao,criado_em)
                    VALUES (%s,%s,%s,%s,'busca_ia',TRUE,NOW())""",
                    (resultado.get('municipio'),resultado.get('estado'),resultado.get('titulo',''),resultado['url']), commit=True, fetch=None)
            else:
                qry("UPDATE buscas_ia SET status='nao_encontrado',finalizado_em=NOW() WHERE id=%s", (bid,), commit=True, fetch=None)
        except Exception as e:
            qry("UPDATE buscas_ia SET status='erro',erro=%s,finalizado_em=NOW() WHERE id=%s", (str(e),bid), commit=True, fetch=None)
    threading.Thread(target=rodar_busca, args=(leg_id['id'] if leg_id else 0, consulta), daemon=True).start()
    return jsonify({'success':True,'message':'Busca iniciada. A legislacao sera adicionada aos pendentes quando encontrada.'})

# -- Pendentes de aprovacao --
@app.route('/api/legislacoes/pendentes', methods=['GET'])
@login_required
def api_leg_pendentes():
    data = qry("SELECT b.*, u.nome as solicitante FROM buscas_ia b LEFT JOIN users u ON b.solicitado_por=u.id WHERE b.status IN ('encontrado','pendente') ORDER BY b.criado_em DESC")
    return jsonify({'success':True,'data':data})

@app.route('/api/legislacoes/pendentes/<int:bid>/aprovar', methods=['POST'])
@admin_required
def api_aprovar_leg_pendente(bid):
    busca = qry("SELECT * FROM buscas_ia WHERE id=%s", (bid,), 'one')
    if not busca: return jsonify({'success':False,'error':'Nao encontrado'}), 404
    qry("UPDATE legislacoes SET pendente_aprovacao=FALSE, aprovado_em=NOW(), aprovado_por=%s WHERE url_original=%s AND pendente_aprovacao=TRUE",
        (session['user_id'], busca.get('resultado_url','')), commit=True, fetch=None)
    qry("UPDATE buscas_ia SET status='aprovado' WHERE id=%s", (bid,), commit=True, fetch=None)
    return jsonify({'success':True})

@app.route('/api/legislacoes/pendentes/<int:bid>/rejeitar', methods=['POST'])
@admin_required
def api_rejeitar_leg_pendente(bid):
    qry("DELETE FROM legislacoes WHERE url_original=(SELECT resultado_url FROM buscas_ia WHERE id=%s) AND pendente_aprovacao=TRUE", (bid,), commit=True, fetch=None)
    qry("UPDATE buscas_ia SET status='rejeitado' WHERE id=%s", (bid,), commit=True, fetch=None)
    return jsonify({'success':True})

@app.route('/api/legislacoes/relacoes-todas', methods=['GET'])
@login_required
def api_relacoes_todas():
    """Retorna todas as relações entre legislações (para árvore genealógica)."""
    try:
        rows = qry("""SELECT lr.legislacao_pai_id, lr.legislacao_filha_id, lr.tipo_relacao,
                              lp.numero as pai_numero, lp.ano as pai_ano,
                              lf.numero as filha_numero, lf.ano as filha_ano
                       FROM legislacao_relacoes lr
                       JOIN legislacoes lp ON lr.legislacao_pai_id = lp.id
                       JOIN legislacoes lf ON lr.legislacao_filha_id = lf.id
                       ORDER BY lp.ano DESC, lf.ano DESC""")
        return jsonify({'success': True, 'data': rows or []})
    except Exception as e:
        return jsonify({'success': True, 'data': []})

# -------------------------------------------------------------------
# API: CONFIGURACOES (tipos, assuntos, email)
# -------------------------------------------------------------------

@app.route('/api/config/tipos-legislacao', methods=['GET'])
@login_required
def api_get_tipos(): return jsonify({'success':True,'data':qry("SELECT * FROM tipos_legislacao ORDER BY nome")})

@app.route('/api/config/tipos-legislacao', methods=['POST'])
@editor_required
def api_criar_tipo():
    d = request.json or {}
    nome = d.get('nome','').strip()
    if not nome: return jsonify({'success':False,'error':'Nome obrigatorio'}), 400
    try:
        qry("INSERT INTO tipos_legislacao (nome,descricao,criado_por) VALUES (%s,%s,%s)",
            (nome, d.get('descricao',''), session['user_id']), commit=True, fetch=None)
        return jsonify({'success':True})
    except: return jsonify({'success':False,'error':'Tipo ja existe'}), 400

@app.route('/api/config/tipos-legislacao/<int:tid>', methods=['DELETE'])
@admin_required
def api_del_tipo(tid):
    qry("DELETE FROM tipos_legislacao WHERE id=%s", (tid,), commit=True, fetch=None)
    return jsonify({'success':True})

@app.route('/api/config/assuntos', methods=['GET'])
@login_required
def api_get_assuntos(): return jsonify({'success':True,'data':qry("SELECT * FROM assuntos_legislacao ORDER BY nome")})

@app.route('/api/config/assuntos', methods=['POST'])
@editor_required
def api_criar_assunto():
    d = request.json or {}
    nome = d.get('nome','').strip()
    if not nome: return jsonify({'success':False,'error':'Nome obrigatorio'}), 400
    try:
        qry("INSERT INTO assuntos_legislacao (nome,descricao,criado_por) VALUES (%s,%s,%s)",
            (nome, d.get('descricao',''), session['user_id']), commit=True, fetch=None)
        return jsonify({'success':True})
    except: return jsonify({'success':False,'error':'Assunto ja existe'}), 400

@app.route('/api/config/assuntos/<int:aid>', methods=['DELETE'])
@admin_required
def api_del_assunto(aid):
    qry("DELETE FROM assuntos_legislacao WHERE id=%s", (aid,), commit=True, fetch=None)
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

# -------------------------------------------------------------------
# API: PARAMETROS URBANISTICOS
# -------------------------------------------------------------------

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
    if not z: return jsonify({'error':'Zona nao encontrada'}), 404
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
    if not mun or not zona: return jsonify({'success':False,'error':'Municipio e zona obrigatorios'}), 400
    cols = [k for k in d.keys() if k not in ('id','criado_em','atualizado_em','atualizado_por')]
    vals = [d[c] for c in cols]
    cols_str = ','.join(cols); ph = ','.join(['%s']*len(cols))
    upd = ','.join(f"{c}=EXCLUDED.{c}" for c in cols if c not in ('municipio','zona','subzona'))
    if upd:
        upd_full = upd + ",atualizado_em=NOW(),atualizado_por=%s"
    else:
        upd_full = "atualizado_em=NOW(),atualizado_por=%s"
    qry(f"INSERT INTO zonas_urbanas ({cols_str}) VALUES ({ph}) ON CONFLICT (municipio,zona,subzona) DO UPDATE SET {upd_full}",
        vals + [session['user_id']], commit=True, fetch=None)
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

# -------------------------------------------------------------------
# API: MONITORAMENTO
# -------------------------------------------------------------------

@app.route('/api/monitor/municipios', methods=['GET'])
@login_required
def api_monitor_municipios(): return jsonify({'success':True,'data':qry("SELECT * FROM municipios WHERE ativo=TRUE ORDER BY nome")})

@app.route('/api/monitor/municipios', methods=['POST'])
@admin_required
def api_monitor_add_municipio():
    d = request.json or {}
    nome = d.get('nome','').strip()
    if not nome: return jsonify({'success':False,'error':'Nome obrigatorio'}), 400
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
    qry("UPDATE municipios SET ativo=FALSE WHERE id=%s", (mid,), commit=True, fetch=None)
    return jsonify({'success':True})

@app.route('/api/monitor/municipios/<int:mid>/perfil')
@login_required
def api_monitor_perfil(mid):
    perfil = qry("""SELECT p.*, m.nome as municipio_nome,
                           COALESCE(m.url_diario, p.url_base) as url_diario
                    FROM perfis_diario p JOIN municipios m ON m.id=p.municipio_id
                    WHERE p.municipio_id=%s""", (mid,), 'one')
    return jsonify({'success':True,'data':perfil})

@app.route('/api/monitor/municipios/<int:mid>/url', methods=['POST'])
@editor_required
def api_monitor_salvar_url(mid):
    d = request.json or {}
    url = d.get('url', '').strip()
    if not url:
        return jsonify({'success': False, 'error': 'URL não informada'}), 400
    # Salvar em municipios.url_diario
    qry("UPDATE municipios SET url_diario=%s, atualizado_em=NOW() WHERE id=%s",
        (url, mid), commit=True, fetch=None)
    # Também atualizar perfis_diario.url_base se existir
    perfil = qry("SELECT id FROM perfis_diario WHERE municipio_id=%s", (mid,), 'one')
    if perfil:
        qry("UPDATE perfis_diario SET url_base=%s WHERE municipio_id=%s",
            (url, mid), commit=True, fetch=None)
    else:
        qry("""INSERT INTO perfis_diario (municipio_id, url_base, status_deteccao)
               VALUES (%s, %s, 'pendente')""",
            (mid, url), commit=True, fetch=None)
    return jsonify({'success': True, 'message': 'URL salva!'})

@app.route('/api/monitor/municipios/<int:mid>/ibge', methods=['POST'])
@editor_required
def api_monitor_salvar_ibge(mid):
    """Salva o código IBGE do município (necessário para API Querido Diário)."""
    d = request.json or {}
    codigo = str(d.get('codigo_ibge', '')).strip()
    if not codigo or not codigo.isdigit():
        return jsonify({'success': False, 'error': 'Código IBGE inválido (deve ser numérico)'}), 400
    qry("UPDATE municipios SET codigo_ibge=%s WHERE id=%s",
        (codigo, mid), commit=True, fetch=None)
    return jsonify({'success': True, 'message': f'Código IBGE {codigo} salvo!'})

@app.route('/api/monitor/municipios/<int:mid>/diagnostico', methods=['POST'])
@editor_required
def api_monitor_diagnostico(mid):
    """
    Executa uma busca diagnóstica SÍNCRONA e retorna cada passo com detalhes.
    Permite ver exatamente o que o scraper faz (ou não faz).
    """
    import traceback, time
    d = request.json or {}
    dias = int(d.get('dias', 7))  # buscar últimos N dias por padrão
    log_steps = []

    def step(emoji, msg, data=None):
        log_steps.append({'emoji': emoji, 'msg': msg, 'data': data})

    # 1. Município
    mun = qry("SELECT * FROM municipios WHERE id=%s", (mid,), 'one')
    if not mun:
        step('❌', 'Município não encontrado no banco')
        return jsonify({'success': False, 'steps': log_steps})
    step('🏙', f'Município: {mun["nome"]} ({mun.get("estado","?")})')
    step('🔗', f'URL em municipios.url_diario: {mun.get("url_diario") or "NULL ⚠️"}')
    codigo_ibge = mun.get('codigo_ibge', '')
    if not codigo_ibge:
        # Auto-preencher código IBGE
        step('🔍', 'Código IBGE não configurado — buscando automaticamente...')
        try:
            from modulos.scraper_inteligente import _obter_codigo_ibge
            codigo_ibge = _obter_codigo_ibge(mid)
            if codigo_ibge:
                step('✅', f'Código IBGE encontrado e salvo: {codigo_ibge}')
                # Recarregar mun do banco
                mun = qry("SELECT * FROM municipios WHERE id=%s", (mid,), 'one')
            else:
                step('⚠️', 'Código IBGE não encontrado na API do IBGE — Querido Diário indisponível')
        except Exception as e:
            step('⚠️', f'Erro ao buscar código IBGE: {e}')

    if codigo_ibge:
        step('🆔', f'Código IBGE: {codigo_ibge} (Querido Diário API habilitada)')

    # 2. Perfil do diário
    perfil = qry("SELECT * FROM perfis_diario WHERE municipio_id=%s", (mid,), 'one')
    if not perfil:
        step('❌', 'Nenhum perfil_diario encontrado! O scraper não sabe como navegar neste site.')
        step('💡', 'Clique em "Re-detectar Perfil" para que a IA aprenda a navegar no diário.')
        return jsonify({'success': False, 'steps': log_steps})

    step('📋', f'Perfil encontrado (id={perfil["id"]})')
    step('🔗', f'URL em perfis_diario.url_base: {perfil.get("url_base") or "NULL ⚠️"}')
    step('📊', f'Status detecção: {perfil.get("status_deteccao","?")}')
    step('🖥', f'Plataforma: {perfil.get("plataforma","desconhecido")}')
    step('📅', f'Detectado em: {perfil.get("detectado_em","nunca")}')
    step('✅', f'Última execução OK: {perfil.get("ultima_execucao_ok","nunca")}')
    step('❌', f'Falhas consecutivas: {perfil.get("falhas_consecutivas",0)}')

    requer_js = perfil.get('requer_playwright', False)
    requer_captcha = perfil.get('requer_captcha', False)
    requer_login = perfil.get('requer_login', False)
    step('🔧', f'Requer JavaScript: {"Sim" if requer_js else "Não"} | CAPTCHA: {"Sim ⚠️" if requer_captcha else "Não"} | Login: {"Sim ⚠️" if requer_login else "Não"}')

    # 3. Perfil JSON (configuração de navegação)
    pjson = perfil.get('perfil_json') or {}
    if pjson:
        step('⚙️', 'Configuração do perfil (perfil_json):', {
            'url_busca': pjson.get('url_busca', '— não definida'),
            'metodo_busca': pjson.get('metodo_busca', 'GET'),
            'formato_data': pjson.get('formato_data', '?'),
            'parametro_data_inicio': pjson.get('parametro_data_inicio', '— não definido'),
            'parametro_data_fim': pjson.get('parametro_data_fim', '— não definido'),
            'seletor_lista_resultados': pjson.get('seletor_lista_resultados', '— não definido'),
            'seletor_titulo_item': pjson.get('seletor_titulo_item', '— não definido'),
            'seletor_data_item': pjson.get('seletor_data_item', '— não definido'),
            'seletor_link_item': pjson.get('seletor_link_item', '— não definido'),
        })
    else:
        step('⚠️', 'perfil_json está VAZIO — o scraper não tem configuração de navegação!')
        step('💡', 'Clique em "Re-detectar Perfil" para que a IA configure os seletores.')

    # 4. Legislações em monitoramento
    legs = qry("""SELECT id, tipo_nome, numero, ano,
                         data_inicio_monitoramento, data_fim_monitoramento,
                         ultima_verificacao_monitoramento, em_monitoramento
                  FROM legislacoes
                  WHERE (municipio_id=%s OR municipio_nome=%s)
                    AND pendente_aprovacao = FALSE
                  ORDER BY em_monitoramento DESC, ano DESC""",
               (mid, mun['nome']))
    if legs:
        for l in (legs or []):
            status = '🟢 Monitorando' if l.get('em_monitoramento') else '⚪ Inativo'
            titulo = f"{l.get('tipo_nome','Lei')} {l.get('numero','?')}/{l.get('ano','?')}"
            step('📜', f'{titulo} — {status} | De: {l.get("data_inicio_monitoramento","não definido")} | Verificado até: {l.get("ultima_verificacao_monitoramento","nunca")}')
    else:
        step('⚠️', 'Nenhuma legislação cadastrada para este município')

    # 5. Teste real de busca (v5: por legislação)
    from datetime import date as _date, timedelta as _td
    dt_fim = _date.today()
    dt_ini = dt_fim - _td(days=dias)

    # Encontrar primeira legislação monitorada para teste
    leg_teste = None
    if legs:
        for l in legs:
            if l.get('em_monitoramento'):
                leg_teste = l
                break
        if not leg_teste:
            leg_teste = legs[0]  # fallback: primeira lei cadastrada

    if leg_teste:
        leg_titulo = f"{leg_teste.get('tipo_nome','Lei')} {leg_teste.get('numero','?')}/{leg_teste.get('ano','?')}"

        # 5a. Teste direto da API Querido Diário
        codigo_ibge = mun.get('codigo_ibge', '')
        if codigo_ibge:
            step('🌐', f'Testando API Querido Diário (IBGE: {codigo_ibge})...')
            try:
                import requests as _req
                termo_qd = f"{leg_teste.get('tipo_nome','Lei')} {leg_teste.get('numero','')}"
                qd_urls = [
                    'https://queridodiario.ok.org.br/api/gazettes',
                    'https://api.queridodiario.ok.org.br/gazettes',
                ]
                qd_ok = False
                for qd_url in qd_urls:
                    try:
                        t0 = time.time()
                        qd_resp = _req.get(qd_url, params={
                            'territory_ids': codigo_ibge,
                            'querystring': f'"{termo_qd}"',
                            'published_since': dt_ini.strftime('%Y-%m-%d'),
                            'published_until': dt_fim.strftime('%Y-%m-%d'),
                            'size': 3,
                        }, timeout=15, headers={'Accept': 'application/json'})
                        elapsed_qd = round(time.time() - t0, 2)

                        if qd_resp.status_code == 200:
                            qd_data = qd_resp.json()
                            qd_total = qd_data.get('total_gazettes', 0)
                            step('✅', f'QD API OK em {elapsed_qd}s: {qd_total} resultado(s) para "{termo_qd}" ({qd_url.split("//")[1][:30]})')
                            qd_ok = True
                            break
                        else:
                            step('⚠️', f'QD API HTTP {qd_resp.status_code} em {elapsed_qd}s ({qd_url.split("//")[1][:30]})')
                    except _req.exceptions.ConnectionError as e:
                        step('❌', f'QD API conexão recusada: {qd_url.split("//")[1][:30]} — {str(e)[:100]}')
                    except _req.exceptions.Timeout:
                        step('❌', f'QD API timeout: {qd_url.split("//")[1][:30]}')
                    except Exception as e:
                        step('❌', f'QD API erro: {str(e)[:150]}')

                if not qd_ok:
                    step('💡', 'API Querido Diário inacessível neste servidor. O scraper usará fallback (requests/Playwright).')
            except Exception as e:
                step('❌', f'Erro teste QD: {e}')

        # 5b. Busca principal
        step('🚀', f'Busca de teste: "{leg_titulo}" nos últimos {dias} dias...')

        try:
            t0 = time.time()
            from modulos.scraper_inteligente import buscar_publicacoes_legislacao
            leg_info = {
                'tipo_nome': leg_teste.get('tipo_nome', 'Lei'),
                'numero': leg_teste.get('numero', ''),
                'ano': leg_teste.get('ano', ''),
                'ementa': '',
            }
            resultado = buscar_publicacoes_legislacao(mid, leg_info, dt_ini, dt_fim)
            elapsed = round(time.time() - t0, 2)

            step('⏱', f'Busca concluída em {elapsed}s (método: {resultado.get("metodo","?")})')
            step('📊', f'Resultado: sucesso={resultado.get("sucesso")} | total={resultado.get("total",0)}')
            step('💬', f'Mensagem: {resultado.get("mensagem","?")}')
            termos = resultado.get('termos_usados', [])
            if termos:
                step('🔍', f'Termos buscados: {", ".join(termos[:4])}')

            pubs = resultado.get('publicacoes', [])
            if pubs:
                step('📰', f'{len(pubs)} publicação(ões) encontrada(s):')
                for i, p in enumerate(pubs[:10]):
                    step('  📄', f'{i+1}. {p.get("titulo","sem título")[:120]} | Data: {p.get("data","?")} | URL: {(p.get("url","") or "sem link")[:80]}')
                if len(pubs) > 10:
                    step('...', f'(+ {len(pubs)-10} publicações omitidas)')
            else:
                step('📭', 'Nenhuma publicação encontrada para este termo')
                step('💡', 'Possíveis causas: site requer JavaScript (precisa Playwright), busca sem resultados para o período, ou site bloqueando acesso automatizado.')

        except ImportError as e:
            step('❌', f'Módulo scraper não encontrado: {e}')
        except Exception as e:
            tb = traceback.format_exc()
            step('❌', f'ERRO na busca: {str(e)[:300]}')
            step('🔍', f'Traceback: {tb[:500]}')
    else:
        step('⚠️', 'Nenhuma legislação cadastrada — não é possível testar a busca (o scraper v5 busca por nome da lei)')

    # 6. Teste de acesso HTTP direto à URL
    url_teste = pjson.get('url_busca') or perfil.get('url_base') or mun.get('url_diario')
    if url_teste:
        step('🌐', f'Teste de acesso HTTP direto a: {url_teste}')
        try:
            import requests as _req
            t0 = time.time()
            resp = _req.get(url_teste, timeout=15, headers={
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
            })
            elapsed = round(time.time() - t0, 2)
            step('📡', f'HTTP {resp.status_code} em {elapsed}s | Content-Type: {resp.headers.get("Content-Type","?")} | Tamanho: {len(resp.text)} chars')
            if resp.status_code == 200:
                # Mostrar um trecho do HTML
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(resp.text, 'html.parser')
                title = soup.title.string.strip() if soup.title else 'sem <title>'
                step('📄', f'Título da página: {title}')
                # Contar formulários e links
                forms = soup.find_all('form')
                links = soup.find_all('a', href=True)
                step('🔎', f'Encontrados: {len(forms)} formulário(s), {len(links)} link(s)')
            else:
                step('⚠️', f'Resposta não-200 — site pode estar bloqueando ou URL incorreta')
        except Exception as e:
            step('❌', f'Falha ao acessar URL: {str(e)[:200]}')

    return jsonify({'success': True, 'steps': log_steps})

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

# --- Histórico por município ---
@app.route('/api/monitor/municipios/<int:mid>/historico')
@login_required
def api_monitor_municipio_historico(mid):
    # Histórico detalhado por legislação para este município
    try:
        data = qry("""
            SELECT mll.data, mll.status, mll.sucesso, mll.alteracoes_detectadas,
                   mll.publicacoes_encontradas, mll.publicacoes_analisadas,
                   mll.publicacoes_duplicadas,
                   mll.metodo_busca, mll.mensagem, mll.erro,
                   mll.legislacao_id,
                   COALESCE(l.tipo_nome,'Lei') || ' nº ' || COALESCE(l.numero,'?') || '/' || COALESCE(l.ano::text,'?') as legislacao_titulo,
                   EXTRACT(EPOCH FROM (mll.finalizada_em - mll.iniciada_em))::int as duracao_seg
            FROM monitoramento_legislacao_log mll
            LEFT JOIN legislacoes l ON l.id = mll.legislacao_id
            WHERE mll.municipio_id = %s
            ORDER BY mll.data DESC LIMIT 50
        """, (mid,))
        return jsonify({'success': True, 'data': data or []})
    except Exception:
        try:
            data = qry("""
                SELECT iniciada_em as data, status,
                       CASE WHEN status='concluido' THEN true WHEN status='erro' THEN false ELSE null END as sucesso,
                       alteracoes_detectadas,
                       EXTRACT(EPOCH FROM (finalizada_em - iniciada_em))::int as duracao_seg
                FROM scheduler_execucoes
                ORDER BY iniciada_em DESC LIMIT 30
            """)
            return jsonify({'success': True, 'data': data or []})
        except Exception:
            return jsonify({'success': True, 'data': []})

# --- Executar monitoramento de um município ---
@app.route('/api/monitor/municipios/<int:mid>/executar', methods=['POST'])
@editor_required
def api_monitor_executar_municipio(mid):
    mun = qry("SELECT * FROM municipios WHERE id=%s AND ativo=TRUE", (mid,), 'one')
    if not mun:
        return jsonify({'success': False, 'error': 'Município não encontrado'}), 404
    # Verificar se há legislações em monitoramento com data definida
    legs = qry("""SELECT id, tipo_nome, numero, ano, data_inicio_monitoramento
                  FROM legislacoes
                  WHERE (municipio_id=%s OR municipio_nome=%s)
                    AND em_monitoramento = TRUE
                    AND pendente_aprovacao = FALSE""",
               (mid, mun['nome']))
    if not legs:
        return jsonify({'success': False,
            'error': 'Nenhuma legislação em monitoramento neste município. Ative o monitoramento e defina o período primeiro.'}), 400
    sem_data = [l for l in legs if not l.get('data_inicio_monitoramento')]
    if sem_data:
        nomes = ', '.join([f"{l['tipo_nome']} {l['numero']}/{l['ano']}" for l in sem_data[:3]])
        return jsonify({'success': False,
            'error': f'Defina a data de início do monitoramento para: {nomes}'}), 400
    def _executar(municipio_id, user_id):
        try:
            from modulos.scheduler_integrado import executar_ciclo_completo
            executar_ciclo_completo(disparado_por=user_id, municipio_id=municipio_id)
        except Exception as e:
            import logging; logging.getLogger(__name__).error(f"Monitoramento municipio {municipio_id}: {e}")
    uid = session.get('user_id')
    threading.Thread(target=_executar, args=(mid, uid), daemon=True).start()
    return jsonify({'success': True,
        'message': f'Monitoramento iniciado para {len(legs)} legislação(ões) de {mun["nome"]}'})

# --- Legislações de um município (para modal de perfil) ---
@app.route('/api/monitor/municipios/<int:mid>/legislacoes')
@login_required
def api_monitor_municipio_legislacoes(mid):
    data = qry("""SELECT l.id, l.tipo_nome, l.numero, l.ano, l.ementa,
                         l.em_monitoramento,
                         to_char(l.data_inicio_monitoramento, 'YYYY-MM-DD') as data_inicio_monitoramento,
                         to_char(l.data_fim_monitoramento, 'YYYY-MM-DD') as data_fim_monitoramento,
                         to_char(l.ultima_verificacao_monitoramento, 'YYYY-MM-DD') as ultima_verificacao_monitoramento,
                         l.municipio_nome, l.status,
                         COALESCE(tl.nome, l.tipo_nome) as tipo_display
                  FROM legislacoes l
                  LEFT JOIN tipos_legislacao tl ON l.tipo_id=tl.id
                  WHERE (l.municipio_id=%s OR l.municipio_nome=(SELECT nome FROM municipios WHERE id=%s))
                    AND l.pendente_aprovacao = FALSE
                  ORDER BY l.em_monitoramento DESC, l.ano DESC, l.numero""",
               (mid, mid))
    return jsonify({'success': True, 'data': data or []})

@app.route('/api/monitor/historico')
@login_required
def api_monitor_historico():
    data = qry("""SELECT se.*, u.nome as usuario_nome
                  FROM scheduler_execucoes se
                  LEFT JOIN users u ON se.disparado_por=u.id
                  ORDER BY iniciada_em DESC LIMIT 50""")
    for row in (data or []):
        row.pop('log_erros', None)
    return jsonify({'success':True,'data':data})

@app.route('/api/monitor/historico/<int:exec_id>')
@login_required
def api_monitor_execucao_detalhe(exec_id):
    row = qry("""SELECT se.*, u.nome as usuario_nome
                 FROM scheduler_execucoes se
                 LEFT JOIN users u ON se.disparado_por=u.id
                 WHERE se.id=%s""", (exec_id,), 'one')
    if not row:
        return jsonify({'success':False,'error':'Execucao nao encontrada'}), 404
    return jsonify({'success':True,'data':row})

@app.route('/api/monitor/status')
@login_required
def api_monitor_status_resumo():
    ultima = qry("""SELECT id, iniciada_em, finalizada_em, status,
                           municipios_processados, municipios_ok, municipios_erro,
                           alteracoes_detectadas, erros, email_enviado
                    FROM scheduler_execucoes
                    ORDER BY iniciada_em DESC LIMIT 1""", fetch='one')
    if not ultima:
        return jsonify({'success':True,'data':{'status':'nunca_executou','ultima':None}})
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

# -- FIX 7: Query alteracoes usa COALESCE para municipio_nome --
@app.route('/api/monitor/alteracoes')
@login_required
def api_monitor_alteracoes():
    # Buscar de alteracoes_pendentes (gravado pelo scheduler v4.0)
    data = qry("""SELECT ap.id, ap.tipo_alteracao, ap.descricao, ap.status,
                         ap.detectada_em as data_deteccao,
                         ap.validada_em, ap.observacoes,
                         COALESCE(m.nome, l.municipio_nome) as municipio,
                         COALESCE(tl.nome, l.tipo_nome, 'Lei') || ' nº ' ||
                            COALESCE(l.numero, '?') || '/' || COALESCE(l.ano::text, '?') as legislacao_titulo,
                         CASE WHEN ap.status='aprovado' THEN true ELSE false END as aprovado
                  FROM alteracoes_pendentes ap
                  LEFT JOIN legislacoes l ON ap.legislacao_id = l.id
                  LEFT JOIN tipos_legislacao tl ON l.tipo_id = tl.id
                  LEFT JOIN municipios m ON ap.municipio_id = m.id
                  ORDER BY ap.detectada_em DESC LIMIT 100""")
    # Fallback: se não houver dados em alteracoes_pendentes, tentar tabela alteracoes antiga
    if not data:
        data = qry("""SELECT a.*, l.numero, l.ano,
                             COALESCE(tl.nome, l.tipo_nome, 'Lei') || ' nº ' ||
                                COALESCE(l.numero, '?') || '/' || COALESCE(l.ano::text, '?') as legislacao_titulo,
                             COALESCE(m.nome, l.municipio_nome) as municipio
                      FROM alteracoes a
                      JOIN legislacoes l ON a.legislacao_id=l.id
                      LEFT JOIN tipos_legislacao tl ON l.tipo_id=tl.id
                      LEFT JOIN municipios m ON l.municipio_id=m.id
                      ORDER BY a.data_deteccao DESC LIMIT 100""")
    return jsonify({'success':True,'data':data or []})

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
        qry(f"UPDATE scheduler_config SET {','.join(upd)}, atualizado_em=NOW() WHERE id=1", vals, commit=True, fetch=None)
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
        (novo_valor,), commit=True, fetch=None)
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
        return jsonify({'success':True,'message':'Execucao iniciada. Acompanhe em Historico.'})
    except Exception as e:
        return jsonify({'success':False,'error':str(e)}), 500

# -------------------------------------------------------------------
# API: INTEGRACOES (fila de parametros)
# -------------------------------------------------------------------

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
    if not i: return jsonify({'success':False,'error':'Nao encontrado'}), 404
    if i.get('parametros_json') and isinstance(i['parametros_json'], str):
        try: i['parametros_json'] = json.loads(i['parametros_json'])
        except: pass
    return jsonify({'success':True,'data':i})

@app.route('/api/integracao/<int:iid>/aprovar', methods=['POST'])
@editor_required
def api_integ_aprovar(iid):
    i = qry("SELECT * FROM integracao_atualizacoes WHERE id=%s", (iid,), 'one')
    if not i: return jsonify({'success':False,'error':'Nao encontrado'}), 404
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
            vals, commit=True, fetch=None)
    qry("UPDATE integracao_atualizacoes SET status='aprovado',revisado_em=NOW(),revisado_por=%s WHERE id=%s",
        (session['user_id'], iid), commit=True, fetch=None)
    return jsonify({'success':True})

@app.route('/api/integracao/<int:iid>/rejeitar', methods=['POST'])
@editor_required
def api_integ_rejeitar(iid):
    motivo = (request.json or {}).get('motivo','')
    qry("UPDATE integracao_atualizacoes SET status='rejeitado',revisado_em=NOW(),revisado_por=%s,motivo_rejeicao=%s WHERE id=%s",
        (session['user_id'], motivo, iid), commit=True, fetch=None)
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
            qry(f"INSERT INTO zonas_urbanas ({','.join(cols)}) VALUES ({ph}) ON CONFLICT (municipio,zona,subzona) DO UPDATE SET {upd}", vals, commit=True, fetch=None)
        qry("UPDATE integracao_atualizacoes SET status='aprovado',revisado_em=NOW(),revisado_por=%s WHERE id=%s",
            (session['user_id'], i['id']), commit=True, fetch=None)
        count += 1
    return jsonify({'success':True,'count':count})

# -------------------------------------------------------------------
# API: DASHBOARD
# -------------------------------------------------------------------

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
        SELECT tipo, titulo, descricao, criado_em FROM (
            SELECT 'legislacao_adicionada' as tipo,
                   tipo_nome||' '||COALESCE(numero,'')||'/'||COALESCE(CAST(ano AS TEXT),'') as titulo,
                   municipio_nome as descricao, criado_em
            FROM legislacoes WHERE pendente_aprovacao=FALSE
            UNION ALL
            SELECT 'execucao_robo', 'Execucao do robo: '||COALESCE(status,''),
                   COALESCE(municipios_ok::text,'0')||' municipios processados', iniciada_em
            FROM scheduler_execucoes
            UNION ALL
            SELECT tipo, mensagem as titulo, '' as descricao, criado_em
            FROM feed_atividades
        ) sub ORDER BY criado_em DESC LIMIT 30
    """)
    return jsonify({'success':True,'data':rows})

@app.route('/api/badges')
@login_required
def api_badges():
    leg  = qry("SELECT COUNT(*) as n FROM buscas_ia WHERE status IN ('encontrado','pendente')", fetch='one')['n']
    intg = qry("SELECT COUNT(*) as n FROM integracao_atualizacoes WHERE status='pendente'", fetch='one')['n']
    usr  = qry("SELECT COUNT(*) as n FROM users WHERE ativo=TRUE AND aprovado=FALSE", fetch='one')['n']
    return jsonify({'leg_pendentes':leg,'param_pendentes':intg,'integ_pendentes':intg,'users_pendentes':usr})

# -------------------------------------------------------------------
# SISTEMA: DIAGNOSTICO E HEALTH
# -------------------------------------------------------------------

# -------------------------------------------------------------------
# API v6: FEED DE ATIVIDADES DO AGENTE
# -------------------------------------------------------------------

@app.route('/api/feed')
@login_required
def api_feed_atividades():
    """Feed de atividades do agente autônomo."""
    limit = request.args.get('limit', 50, type=int)
    tipo = request.args.get('tipo', '')
    try:
        if tipo:
            rows = qry("""SELECT id, tipo, mensagem, detalhes, lida, criado_em
                         FROM feed_atividades WHERE tipo=%s
                         ORDER BY criado_em DESC LIMIT %s""", (tipo, limit))
        else:
            rows = qry("""SELECT id, tipo, mensagem, detalhes, lida, criado_em
                         FROM feed_atividades
                         ORDER BY criado_em DESC LIMIT %s""", (limit,))
        return jsonify({'success': True, 'data': rows or []})
    except Exception as e:
        return jsonify({'success': True, 'data': []})

@app.route('/api/feed/<int:fid>/lida', methods=['POST'])
@login_required
def api_feed_marcar_lida(fid):
    qry("UPDATE feed_atividades SET lida=TRUE WHERE id=%s", (fid,), commit=True, fetch=None)
    return jsonify({'success': True})

@app.route('/api/feed/resumo')
@login_required
def api_feed_resumo():
    """Resumo do feed: contagens por tipo."""
    try:
        rows = qry("""SELECT tipo, COUNT(*) as total,
                             COUNT(*) FILTER (WHERE NOT lida) as nao_lidas
                      FROM feed_atividades
                      WHERE criado_em > NOW() - INTERVAL '7 days'
                      GROUP BY tipo ORDER BY total DESC""")
        nao_lidas = qry("SELECT COUNT(*) as n FROM feed_atividades WHERE NOT lida", fetch='one')
        return jsonify({
            'success': True,
            'nao_lidas': nao_lidas['n'] if nao_lidas else 0,
            'por_tipo': rows or []
        })
    except Exception:
        return jsonify({'success': True, 'nao_lidas': 0, 'por_tipo': []})

# -------------------------------------------------------------------
# API v6: INTEGRAÇÃO COM PLATAFORMA EXTERNA
# -------------------------------------------------------------------

@app.route('/api/integracao-plataforma/executar', methods=['POST'])
@editor_required
def api_executar_integracao():
    """Executa integração manual com plataforma externa."""
    try:
        from modulos.integrador_plataforma import executar_integracao_plataforma
        resultado = executar_integracao_plataforma()
        # Log da integração
        qry("""INSERT INTO integracao_log (tipo, municipios_consultados,
                novos_detectados, legislacoes_cadastradas, detalhes, criado_em)
               VALUES ('manual', %s, %s, %s, %s, NOW())""",
            (resultado['municipios_consultados'], resultado['novos_detectados'],
             resultado['legislacoes_cadastradas'], json.dumps(resultado)),
            commit=True, fetch=None)
        return jsonify({'success': True, 'data': resultado})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)[:300]}), 500

@app.route('/api/integracao-plataforma/config', methods=['GET'])
@editor_required
def api_integracao_config():
    """Retorna configuração atual da integração."""
    return jsonify({
        'success': True,
        'configurada': bool(os.getenv('PLATAFORMA_API_URL')),
        'url': os.getenv('PLATAFORMA_API_URL', '(não configurada)'),
    })

@app.route('/api/integracao-plataforma/historico')
@login_required
def api_integracao_historico():
    rows = qry("SELECT * FROM integracao_log ORDER BY criado_em DESC LIMIT 20")
    return jsonify({'success': True, 'data': rows or []})

# -------------------------------------------------------------------
# API v6: DIÁRIOS OFICIAIS DESCOBERTOS
# -------------------------------------------------------------------

@app.route('/api/diarios-oficiais')
@login_required
def api_listar_diarios():
    rows = qry("""SELECT d.*, m.nome as municipio_nome
                  FROM diarios_oficiais d
                  LEFT JOIN municipios m ON d.municipio_id=m.id
                  ORDER BY d.created_at DESC""")
    return jsonify({'success': True, 'data': rows or []})

# -------------------------------------------------------------------
# API v6: REGRAS AUTOMÁTICAS (teste manual)
# -------------------------------------------------------------------

@app.route('/api/agente/descobrir-legislacoes', methods=['POST'])
@editor_required
def api_descobrir_legislacoes():
    """Descobre legislações urbanísticas de um município."""
    d = request.json or {}
    municipio = d.get('municipio', '')
    estado = d.get('estado', '')
    if not municipio or not estado:
        return jsonify({'success': False, 'error': 'municipio e estado obrigatórios'}), 400
    try:
        from modulos.descobridor_legislacoes import (
            descobrir_legislacoes_municipio, cadastrar_legislacoes_descobertas
        )
        from modulos.descobridor_diario import descobrir_diario

        diario = descobrir_diario(municipio, estado, 'municipal')
        legislacoes = descobrir_legislacoes_municipio(municipio, estado)
        ids = []
        if legislacoes and d.get('cadastrar', False):
            ids = cadastrar_legislacoes_descobertas(legislacoes, municipio, estado,
                                                     ativar_monitoramento=d.get('monitorar', True))
        return jsonify({
            'success': True,
            'diario': diario,
            'legislacoes_encontradas': legislacoes,
            'legislacoes_cadastradas': ids,
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)[:300]}), 500

# -------------------------------------------------------------------

# -------------------------------------------------------------------
# BUSCADOR DE LEGISLAÇÕES — Página + API
# -------------------------------------------------------------------

@app.route('/buscador')
@app.route('/buscador/manual')
@app.route('/buscador/historico')
@login_required
def pg_buscador():
    page_map = {'/buscador': 'buscador-painel', '/buscador/manual': 'buscador-manual', '/buscador/historico': 'buscador-historico'}
    return render_template('buscador.html', active_group='buscador', active_page=page_map.get(request.path, 'buscador-painel'))

@app.route('/api/buscador/municipio', methods=['POST'])
@editor_required
def api_buscador_municipio():
    """Busca legislações de um município (botão 'Buscar Novo Município')."""
    d = request.json or {}
    mun = d.get('municipio', '').strip()
    est = d.get('estado', '').strip()
    if not mun or not est:
        return jsonify({'success': False, 'error': 'municipio e estado obrigatórios'}), 400
    try:
        from modulos.buscador_legislacoes import buscar_municipio
        r = buscar_municipio(mun, est, cadastrar=d.get('cadastrar', False), monitorar=d.get('monitorar', True))
        return jsonify({'success': True, **r})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)[:300]}), 500

@app.route('/api/buscador/manual', methods=['POST'])
@editor_required
def api_buscador_manual():
    """Busca manual por campos."""
    d = request.json or {}
    try:
        from modulos.buscador_legislacoes import busca_manual
        resultado = busca_manual(d)
        if isinstance(resultado, dict):
            return jsonify({
                'success': True,
                'legislacoes': resultado.get('legislacoes', []),
                'erro': resultado.get('erro'),
                'logs': resultado.get('logs', []),
            })
        else:
            return jsonify({'success': True, 'legislacoes': resultado, 'logs': []})
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        return jsonify({
            'success': False,
            'error': str(e)[:300],
            'logs': [
                {'nivel': 'erro', 'msg': f'Exceção no servidor: {str(e)[:200]}'},
                {'nivel': 'erro', 'msg': f'Traceback: {tb[-300:]}'},
            ]
        }), 500

@app.route('/api/buscador/cadastrar', methods=['POST'])
@editor_required
def api_buscador_cadastrar():
    """Cadastra legislações selecionadas na biblioteca."""
    d = request.json or {}
    legislacoes = d.get('legislacoes', [])
    municipio = d.get('municipio', '')
    estado = d.get('estado', '')
    monitorar = d.get('monitorar', True)
    if not legislacoes:
        return jsonify({'success': False, 'error': 'Nenhuma legislação para cadastrar'}), 400
    try:
        from modulos.buscador_legislacoes import cadastrar_resultados
        ids = cadastrar_resultados(legislacoes, municipio, estado, monitorar)
        return jsonify({'success': True, 'cadastradas': len(ids), 'ids': ids})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)[:300]}), 500

@app.route('/api/buscador/varrer-referencias', methods=['POST'])
@editor_required
def api_buscador_varrer_refs():
    """Varre a biblioteca buscando referências a legislações não cadastradas."""
    try:
        from modulos.buscador_legislacoes import varrer_referencias
        refs = varrer_referencias()
        return jsonify({'success': True, 'referencias': refs})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)[:300]}), 500

@app.route('/api/buscador/fila')
@login_required
def api_buscador_fila():
    """Retorna histórico/fila de buscas."""
    try:
        from modulos.buscador_legislacoes import listar_fila
        return jsonify({'success': True, 'data': listar_fila()})
    except Exception as e:
        return jsonify({'success': True, 'data': []})

# -------------------------------------------------------------------

# -- Diagnostico R2 (CORRIGIDO: decorator ativo + sem chars especiais) --
@app.route('/api/diagnostico/r2')
@admin_required
def api_diagnostico_r2():
    info = {
        'r2_importado': _R2_IMPORTADO,
        'r2_disponivel': r2_disponivel(),
        'R2_ACCOUNT_ID': bool(os.getenv('R2_ACCOUNT_ID')),
        'R2_ACCESS_KEY': bool(os.getenv('R2_ACCESS_KEY')),
        'R2_SECRET_KEY': bool(os.getenv('R2_SECRET_KEY')),
        'R2_BUCKET_NAME': os.getenv('R2_BUCKET_NAME', ''),
    }
    if info['r2_disponivel']:
        try:
            url_teste = r2_upload(b'teste urbanlex', 'teste_diagnostico.txt', leg_id=0)
            info['upload_teste'] = url_teste
            info['upload_ok'] = bool(url_teste)
            if url_teste:
                r2_delete(url_teste)
                info['delete_ok'] = True
        except Exception as e:
            info['upload_erro'] = str(e)
            info['upload_ok'] = False
    return jsonify(info)

@app.route('/health')
def health():
    try:
        qry("SELECT 1", fetch='one')
        return jsonify({'status':'ok','db':'conectado','version':'3.5'})
    except Exception as e:
        return jsonify({'status':'erro','error':str(e)}), 500

# -- FIX 4: Rotas diagnostico agora exigem token SECRET_KEY --
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

# -------------------------------------------------------------------
# ROTAS ADICIONAIS (FIX 5: movidas para ANTES do if __name__)
# -------------------------------------------------------------------

@app.route('/parametros/nova-zona')
@login_required
def pagina_nova_zona():
    return render_template('parametros.html', active_page='nova-zona', active_group='parametros',
                           abrir_modal='nova_zona', **tmpl_ctx())

@app.route('/api/zona/<int:zona_id>', methods=['PUT'])
@editor_required
def api_atualizar_zona(zona_id):
    d = request.json or {}
    if not d:
        return jsonify({'success': False, 'error': 'Nenhum dado enviado'}), 400
    campos_proibidos = {'id', 'criado_em', 'municipio', 'zona', 'subzona'}
    cols = [k for k in d.keys() if k not in campos_proibidos]
    if not cols:
        return jsonify({'success': False, 'error': 'Nenhum campo valido para atualizar'}), 400
    set_sql = ', '.join(f"{c} = %s" for c in cols)
    vals = [d[c] for c in cols] + [datetime.now(), session['user_id'], zona_id]
    qry(f"UPDATE zonas_urbanas SET {set_sql}, atualizado_em = %s, atualizado_por = %s WHERE id = %s",
        vals, commit=True, fetch=None)
    return jsonify({'success': True})

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

@app.route('/api/parametros/importar', methods=['POST'])
@editor_required
def api_parametros_importar():
    arquivo = request.files.get('arquivo')
    if not arquivo:
        return jsonify({'success': False, 'error': 'Nenhum arquivo enviado'}), 400
    if not PANDAS_OK:
        return jsonify({'success': False, 'error': 'Pandas nao instalado'}), 500
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
                        vals, commit=True, fetch=None)
                else:
                    qry(f"INSERT INTO zonas_urbanas ({','.join(cols)}) VALUES ({ph}) ON CONFLICT DO NOTHING",
                        vals, commit=True, fetch=None)
                ok += 1
            except Exception as e:
                erro += 1
        return jsonify({'success': True, 'ok': ok, 'erro': erro,
                        'message': f'{ok} zonas importadas, {erro} erros'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/parametros/exportar')
@login_required
def api_parametros_exportar():
    if not PANDAS_OK:
        return jsonify({'success': False, 'error': 'Pandas nao instalado'}), 500
    try:
        data = qry("SELECT * FROM zonas_urbanas ORDER BY estado, municipio, zona, subzona")
        df = pd.DataFrame(data)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Parametros')
        output.seek(0)
        return send_file(output, as_attachment=True,
                         download_name=f'urbanlex_parametros_{datetime.now().strftime("%Y%m%d_%H%M")}.xlsx',
                         mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

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

# -------------------------------------------------------------------
# INICIALIZACAO (FIX 6: inicializar() no nivel do modulo)
# -------------------------------------------------------------------

def inicializar():
    print("UrbanLex v3.5 iniciando...")
    # Criar tabela de arquivos se nao existe
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS legislacao_arquivos (
                id SERIAL PRIMARY KEY,
                legislacao_id INTEGER NOT NULL REFERENCES legislacoes(id) ON DELETE CASCADE,
                nome_arquivo VARCHAR(500) NOT NULL,
                arquivo_tipo VARCHAR(50),
                arquivo_url TEXT,
                tamanho_bytes BIGINT DEFAULT 0,
                hash_conteudo VARCHAR(64),
                conteudo_texto TEXT,
                criado_em TIMESTAMP DEFAULT NOW(),
                criado_por INTEGER
            )
        """)
        # Indice para busca por legislacao
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_leg_arquivos_leg_id
            ON legislacao_arquivos(legislacao_id)
        """)
        conn.commit()
        conn.close()
        print("Tabela legislacao_arquivos verificada/criada")
    except Exception as e:
        print(f"Aviso criacao tabela arquivos: {e}")
    # Criar tabela de log detalhado por legislação
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS monitoramento_legislacao_log (
                id SERIAL PRIMARY KEY,
                execucao_id INTEGER REFERENCES scheduler_execucoes(id) ON DELETE CASCADE,
                legislacao_id INTEGER REFERENCES legislacoes(id) ON DELETE CASCADE,
                municipio_id INTEGER REFERENCES municipios(id) ON DELETE CASCADE,
                data TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                iniciada_em TIMESTAMP,
                finalizada_em TIMESTAMP,
                status VARCHAR(20) DEFAULT 'ok',
                sucesso BOOLEAN DEFAULT TRUE,
                publicacoes_encontradas INTEGER DEFAULT 0,
                publicacoes_analisadas INTEGER DEFAULT 0,
                alteracoes_detectadas INTEGER DEFAULT 0,
                publicacoes_duplicadas INTEGER DEFAULT 0,
                metodo_busca VARCHAR(30),
                url_acessada TEXT,
                mensagem TEXT,
                erro TEXT
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_mll_legislacao ON monitoramento_legislacao_log(legislacao_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_mll_municipio ON monitoramento_legislacao_log(municipio_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_mll_data ON monitoramento_legislacao_log(data DESC)")
        # Adicionar coluna ultima_verificacao_monitoramento se não existir
        try:
            cur.execute("ALTER TABLE legislacoes ADD COLUMN IF NOT EXISTS ultima_verificacao_monitoramento DATE")
        except Exception:
            pass
        try:
            cur.execute("ALTER TABLE legislacoes ADD COLUMN IF NOT EXISTS data_fim_monitoramento DATE")
        except Exception:
            pass
        # v5: coluna para código IBGE (necessária para API Querido Diário)
        try:
            cur.execute("ALTER TABLE municipios ADD COLUMN IF NOT EXISTS codigo_ibge VARCHAR(10)")
        except Exception:
            pass
        # ── v6: Agente Autônomo ──
        try:
            cur.execute("""CREATE TABLE IF NOT EXISTS diarios_oficiais (
                id SERIAL PRIMARY KEY, esfera VARCHAR(20) DEFAULT 'municipal',
                uf VARCHAR(2), municipio_id INTEGER, nome VARCHAR(200),
                url_principal VARCHAR(500), url_busca VARCHAR(500),
                tipo_plataforma VARCHAR(50), metodo_busca JSONB,
                codigo_ibge VARCHAR(10), verificado_em TIMESTAMP,
                funcionando BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT NOW(), updated_at TIMESTAMP DEFAULT NOW())""")
            cur.execute("""CREATE TABLE IF NOT EXISTS feed_atividades (
                id SERIAL PRIMARY KEY, tipo VARCHAR(50) NOT NULL,
                mensagem TEXT NOT NULL, detalhes JSONB,
                lida BOOLEAN DEFAULT FALSE, criado_em TIMESTAMP DEFAULT NOW())""")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_feed_criado ON feed_atividades(criado_em DESC)")
            cur.execute("""CREATE TABLE IF NOT EXISTS alteracoes_parametros (
                id SERIAL PRIMARY KEY, legislacao_id INTEGER, legislacao_alteradora_id INTEGER,
                publicacao_data DATE, parametro VARCHAR(100), zona VARCHAR(50),
                valor_anterior TEXT, valor_novo TEXT, confianca DECIMAL(3,2),
                aprovado_usuario BOOLEAN, aprovado_em TIMESTAMP,
                created_at TIMESTAMP DEFAULT NOW())""")
            cur.execute("""CREATE TABLE IF NOT EXISTS agente_memoria (
                id SERIAL PRIMARY KEY, diario_id INTEGER, url_base VARCHAR(500),
                passos JSONB NOT NULL, sucesso_count INTEGER DEFAULT 1,
                falha_count INTEGER DEFAULT 0, ultimo_uso TIMESTAMP DEFAULT NOW(),
                created_at TIMESTAMP DEFAULT NOW(), updated_at TIMESTAMP DEFAULT NOW())""")
            cur.execute("""CREATE TABLE IF NOT EXISTS integracao_log (
                id SERIAL PRIMARY KEY, tipo VARCHAR(50), municipios_consultados INTEGER DEFAULT 0,
                novos_detectados INTEGER DEFAULT 0, legislacoes_cadastradas INTEGER DEFAULT 0,
                detalhes JSONB, status VARCHAR(20) DEFAULT 'concluido',
                criado_em TIMESTAMP DEFAULT NOW())""")
            cur.execute("ALTER TABLE legislacoes ADD COLUMN IF NOT EXISTS municipio_id INTEGER")
            cur.execute("ALTER TABLE legislacoes ADD COLUMN IF NOT EXISTS origem VARCHAR(50) DEFAULT 'manual'")
        except Exception as e:
            print(f"Aviso migration v6: {e}")
        conn.commit()
        conn.close()
        print("Tabela monitoramento_legislacao_log verificada/criada")
    except Exception as e:
        print(f"Aviso criacao tabela log legislacao: {e}")
    if SCHEDULER_OK:
        try: iniciar_scheduler(); print("Scheduler iniciado")
        except Exception as e: print(f"Scheduler: {e}")

inicializar()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT',5000)), debug=os.getenv('FLASK_ENV')!='production')
