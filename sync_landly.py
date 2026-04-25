#!/usr/bin/env python3
import sys, os
sys.path.insert(0, '/var/www/urbanlex')
os.chdir('/var/www/urbanlex')

try:
    from app import app, _executar_sync_landly
    with app.app_context():
        _executar_sync_landly()
        print("Sync Landly concluída")
except Exception as e:
    print(f"Erro sync Landly: {e}")
    import traceback; traceback.print_exc()
