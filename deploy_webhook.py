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
            subprocess.run(['bash', '-c', 'cd /var/www/urbanlex && git pull && systemctl restart urbanlex'])
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
