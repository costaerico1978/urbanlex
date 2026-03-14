from flask import Flask, request, abort
import subprocess, hmac, hashlib, os

app = Flask(__name__)
SECRET = os.getenv('DEPLOY_SECRET', 'urbanlex-deploy-2026')

@app.route('/deploy', methods=['POST'])
def deploy():
    sig = request.headers.get('X-Hub-Signature-256', '')
    mac = 'sha256=' + hmac.new(SECRET.encode(), request.data, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(sig, mac):
        abort(403)
    subprocess.Popen(['bash', '-c', 'cd /var/www/urbanlex && git pull && systemctl restart urbanlex'])
    return 'OK', 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
