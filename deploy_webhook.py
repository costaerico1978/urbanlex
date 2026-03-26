from flask import Flask, request, jsonify, abort
import subprocess
import os, hmac, hashlib, os

app = Flask(__name__)
SECRET = os.getenv('DEPLOY_SECRET', 'urbanlex-deploy-2026')

@app.route('/deploy', methods=['POST'])
def deploy():
    sig = request.headers.get('X-Hub-Signature-256', '')
    mac = 'sha256=' + hmac.new(SECRET.encode(), request.data, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(sig, mac):
        abort(403)
    import threading
    if getattr(deploy, '_lock', None) is None:
        deploy._lock = threading.Lock()
    if deploy._lock.locked():
        return 'Deploy em andamento', 200
    def _run():
        with deploy._lock:
            subprocess.run(['bash', '-c', '''
cd /var/www/urbanlex
HASH_ANTES=$(git rev-parse HEAD)
CHANGES=$(git pull)
HASH_DEPOIS=$(git rev-parse HEAD)
echo "$CHANGES"
# Reiniciar se houver mudancas em arquivos Python
if [ "$HASH_ANTES" != "$HASH_DEPOIS" ] && git diff "$HASH_ANTES" "$HASH_DEPOIS" --name-only | grep -qE "\.py$"; then
touch /tmp/urbanlex_restart_pendente
while true; do
    ATIVOS=$(curl -sf http://localhost:5000/api/buscador/jobs-ativos 2>/dev/null)
    if echo "$ATIVOS" | grep -q '"ativos": true'; then
        echo "$(date): Jobs ativos, aguardando 30s..." >> /var/log/urbanlex-deploy.log
        sleep 30
    else
        echo "$(date): Sem jobs ativos, aguardando 15s antes de reiniciar..." >> /var/log/urbanlex-deploy.log
        sleep 15
        # Verificar novamente se nao iniciou novo job no intervalo
        ATIVOS2=$(curl -sf http://localhost:5000/api/buscador/jobs-ativos 2>/dev/null)
        if echo "$ATIVOS2" | grep -q '"ativos": true'; then
            continue
        fi
        git pull
        systemctl restart urbanlex
        rm -f /tmp/urbanlex_restart_pendente
        echo "$(date): Restart executado" >> /var/log/urbanlex-deploy.log
        break
    fi
done
else
    if [ -f /tmp/urbanlex_restart_pendente ]; then
        echo "$(date): Restart pendente — executando..." >> /var/log/urbanlex-deploy.log
        git pull && systemctl restart urbanlex && rm -f /tmp/urbanlex_restart_pendente
        echo "$(date): Restart pendente executado" >> /var/log/urbanlex-deploy.log
    else
        echo "$(date): Sem mudancas em .py — restart nao necessario" >> /var/log/urbanlex-deploy.log
    fi
fi
'''])
    threading.Thread(target=_run, daemon=True).start()
    return 'OK', 200

ADMIN_TOKEN = os.environ.get('ADMIN_CONTROL_TOKEN', '')

def verificar_token():
    token = request.headers.get('X-Admin-Token', '') or (request.json or {}).get('token', '')
    return token == ADMIN_TOKEN and ADMIN_TOKEN != ''

@app.route('/matar-chromium', methods=['POST'])
def matar_chromium():
    if not verificar_token(): return jsonify({'error': 'não autorizado'}), 403
    subprocess.run(['pkill', '-9', '-f', 'chromium'], capture_output=True)
    return jsonify({'success': True, 'msg': 'Chromium encerrado'})

@app.route('/reiniciar-worker', methods=['POST'])
def reiniciar_worker():
    if not verificar_token(): return jsonify({'error': 'não autorizado'}), 403
    def _restart():
        import time
        time.sleep(1)
        subprocess.run(['systemctl', 'restart', 'urbanlex'])
    import threading
    threading.Thread(target=_restart, daemon=True).start()
    return jsonify({'success': True, 'msg': 'Worker reiniciando...'})

@app.route('/status', methods=['GET'])
def status():
    return jsonify({'ok': True})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, use_reloader=False)
