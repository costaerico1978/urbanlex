#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ROTAS DE AUTENTICAÇÃO COMPLETA
Adicione este bloco ao web/app.py.

Adicione no topo do app.py:
    from modulos.auth_completo import (
        cadastrar_usuario, ativar_conta, processar_aprovacao,
        listar_usuarios, alterar_role, excluir_usuario,
        migrar_banco, validar_senha
    )

Adicione antes do if __name__ == '__main__':
    migrar_banco()

Adicione ADMIN_EMAIL como variável de ambiente no Railway:
    ADMIN_EMAIL = costa.erico@gmail.com
"""

# ═══════════════════════════════════════════════════════════════════
# PÁGINAS PÚBLICAS (sem login)
# ═══════════════════════════════════════════════════════════════════

@app.route('/cadastro')
def pagina_cadastro():
    return render_template('cadastro.html')

@app.route('/ativar/<token>')
def pagina_ativar(token):
    resultado = ativar_conta(token)
    if resultado['ok']:
        return render_template('conta_ativada.html',
                               ok=True,
                               nome=resultado['nome'],
                               email=resultado['email'],
                               role=resultado['role'])
    return render_template('conta_ativada.html',
                           ok=False,
                           erro=resultado['erro'])

@app.route('/admin/aprovar/<token>')
def admin_aprovar(token):
    resultado = processar_aprovacao(token)
    return render_template('resultado_aprovacao.html',
                           ok=resultado['ok'],
                           acao=resultado.get('acao'),
                           nome=resultado.get('nome'),
                           email=resultado.get('email'),
                           erro=resultado.get('erro'))

@app.route('/admin/rejeitar/<token>')
def admin_rejeitar(token):
    resultado = processar_aprovacao(token)
    return render_template('resultado_aprovacao.html',
                           ok=resultado['ok'],
                           acao=resultado.get('acao'),
                           nome=resultado.get('nome'),
                           email=resultado.get('email'),
                           erro=resultado.get('erro'))


# ═══════════════════════════════════════════════════════════════════
# API DE AUTENTICAÇÃO
# ═══════════════════════════════════════════════════════════════════

@app.route('/api/auth/cadastrar', methods=['POST'])
def api_cadastrar():
    data     = request.json or {}
    nome     = data.get('nome', '').strip()
    email    = data.get('email', '').strip()
    senha    = data.get('senha', '')
    confirma = data.get('confirmar_senha', '')

    if senha != confirma:
        return jsonify({'success': False, 'error': 'As senhas não coincidem'}), 400

    resultado = cadastrar_usuario(nome, email, senha)
    if resultado['ok']:
        return jsonify({'success': True, 'tipo': resultado['tipo'], 'msg': resultado['msg']})
    return jsonify({'success': False, 'error': resultado['erro']}), 400


# ═══════════════════════════════════════════════════════════════════
# GERENCIAMENTO DE USUÁRIOS (apenas admin)
# ═══════════════════════════════════════════════════════════════════

@app.route('/usuarios')
@login_required
@admin_required
def pagina_usuarios():
    return render_template('usuarios.html',
                           username=session.get('username'),
                           role=session.get('role'))

@app.route('/api/admin/usuarios', methods=['GET'])
@login_required
@admin_required
def api_listar_usuarios():
    return jsonify({'success': True, 'data': listar_usuarios()})

@app.route('/api/admin/usuarios/<int:user_id>/role', methods=['POST'])
@login_required
@admin_required
def api_alterar_role(user_id):
    data     = request.json or {}
    novo_role = data.get('role', '')
    resultado = alterar_role(user_id, novo_role, session['user_id'])
    if resultado['ok']:
        return jsonify({'success': True})
    return jsonify({'success': False, 'error': resultado['erro']}), 400

@app.route('/api/admin/usuarios/<int:user_id>', methods=['DELETE'])
@login_required
@admin_required
def api_excluir_usuario(user_id):
    resultado = excluir_usuario(user_id, session['user_id'])
    if resultado['ok']:
        return jsonify({'success': True})
    return jsonify({'success': False, 'error': resultado['erro']}), 400


# ═══════════════════════════════════════════════════════════════════
# DECORATOR ATUALIZADO — verificar role para editor
# ═══════════════════════════════════════════════════════════════════
# Substitua o decorator editor_required no app.py por este:

def editor_required(f):
    """Permite acesso apenas a admin e editor."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if session.get('role') not in ('admin', 'editor'):
            return jsonify({'error': 'Acesso restrito a editores'}), 403
        return f(*args, **kwargs)
    return decorated
