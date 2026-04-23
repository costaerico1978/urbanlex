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
    payload = request.get_json(force=True, silent=True) or {}
    commit_github = payload.get('after', '')
    def _run():
        with deploy._lock:
            subprocess.run(['bash', '-c', '''
cd /var/www/urbanlex
git pull
ATIVOS=$(curl -sf http://localhost:5000/api/buscador/jobs-ativos 2>/dev/null)
if echo "$ATIVOS" | grep -q '"ativos": true'; then
    echo "$(date): Jobs ativos — restart adiado" >> /var/log/urbanlex-deploy.log
    for i in $(seq 1 120); do
        sleep 30
        ATIVOS2=$(curl -sf http://localhost:5000/api/buscador/jobs-ativos 2>/dev/null)
        if ! echo "$ATIVOS2" | grep -q '"ativos": true'; then
            FILA=$(python3 -c "import psycopg2; conn=psycopg2.connect('postgresql://urbanlex:urbanlex123@localhost:5432/urbanlex'); cur=conn.cursor(); cur.execute(\"SELECT COUNT(*) FROM fila_buscas WHERE status='rodando'\"); print(cur.fetchone()[0]); conn.close()" 2>/dev/null || echo 0)
            if [ "$FILA" = "0" ]; then
            break
            fi
        fi
        fi
    done
fi
pkill -9 -f chromium 2>/dev/null || true
sleep 2
systemctl restart urbanlex
echo "$(date): Deploy executado" >> /var/log/urbanlex-deploy.log
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
