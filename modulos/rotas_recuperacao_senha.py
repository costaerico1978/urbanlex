#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PATCH — Rotas de recuperação de senha
Adicione este bloco ao web/app.py, antes do bloco de health/páginas.

Também adicione no topo do app.py:
    from modulos.recuperacao_senha import (
        gerar_token_reset, enviar_email_reset,
        validar_token, redefinir_senha, criar_tabela_tokens
    )

E no final do app.py, antes do if __name__ == '__main__':
    with app.app_context():
        criar_tabela_tokens()
"""

# ═══════════════════════════════════════════════════════════════════════════════
# RECUPERAÇÃO DE SENHA
# ═══════════════════════════════════════════════════════════════════════════════

@app.route('/esqueci-senha', methods=['GET'])
def pagina_esqueci_senha():
    return render_template('esqueci_senha.html')


@app.route('/api/auth/esqueci-senha', methods=['POST'])
def api_esqueci_senha():
    """
    1. Recebe o e-mail
    2. Gera token seguro (1h de validade)
    3. Envia e-mail com link de reset
    Sempre retorna sucesso para não revelar se o e-mail existe.
    """
    data  = request.json or {}
    email = data.get('email', '').strip().lower()

    if not email:
        return jsonify({'success': False, 'error': 'E-mail obrigatório'}), 400

    resultado = gerar_token_reset(email)

    if not resultado['ok']:
        logger.error(f"Erro ao gerar token para {email}: {resultado.get('erro')}")
        # Ainda retorna 200 para não expor info
        return jsonify({'success': True})

    if resultado.get('enviado'):
        email_result = enviar_email_reset(
            email=resultado['email'],
            username=resultado['username'],
            token=resultado['token']
        )
        if not email_result['ok']:
            logger.warning(f"E-mail não enviado: {email_result.get('erro')}")
            # Em dev, loga o token para facilitar testes
            logger.info(f"[DEV] Token de reset: {resultado['token']}")

    return jsonify({'success': True})


@app.route('/reset-senha/<token>', methods=['GET'])
def pagina_reset_senha(token):
    """Página HTML para o usuário digitar a nova senha."""
    validacao = validar_token(token)
    if not validacao['valido']:
        return render_template('reset_senha.html',
                               token=None,
                               erro=validacao['motivo'])
    return render_template('reset_senha.html',
                           token=token,
                           username=validacao['username'],
                           erro=None)


@app.route('/api/auth/reset-senha', methods=['POST'])
def api_reset_senha():
    """
    Recebe token + nova_senha, valida e atualiza.
    """
    data       = request.json or {}
    token      = data.get('token', '').strip()
    nova_senha = data.get('nova_senha', '')
    confirmar  = data.get('confirmar_senha', '')

    if not token or not nova_senha:
        return jsonify({'success': False, 'error': 'Dados incompletos'}), 400

    if nova_senha != confirmar:
        return jsonify({'success': False, 'error': 'As senhas não coincidem'}), 400

    resultado = redefinir_senha(token, nova_senha)

    if resultado['ok']:
        return jsonify({'success': True, 'username': resultado['username']})

    return jsonify({'success': False, 'error': resultado['erro']}), 400


@app.route('/api/auth/alterar-senha', methods=['POST'])
@login_required
def api_alterar_senha():
    """
    Alterar senha estando logado (formulário no perfil).
    """
    data         = request.json or {}
    senha_atual  = data.get('senha_atual', '')
    nova_senha   = data.get('nova_senha', '')
    confirmar    = data.get('confirmar_senha', '')

    if nova_senha != confirmar:
        return jsonify({'success': False, 'error': 'As senhas não coincidem'}), 400

    if len(nova_senha) < 8:
        return jsonify({'success': False, 'error': 'A nova senha deve ter pelo menos 8 caracteres'}), 400

    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT password_hash FROM users WHERE id = %s", (session['user_id'],))
        user = cur.fetchone()

        if not bcrypt.checkpw(senha_atual.encode(), user['password_hash'].encode()):
            return jsonify({'success': False, 'error': 'Senha atual incorreta'}), 401

        novo_hash = bcrypt.hashpw(nova_senha.encode(), bcrypt.gensalt(12)).decode()
        cur.execute("UPDATE users SET password_hash = %s WHERE id = %s",
                    (novo_hash, session['user_id']))
        conn.commit()
        return jsonify({'success': True})

    except Exception as e:
        conn.rollback()
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        conn.close()
