#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
inicializar_banco.py — Execute uma vez no Railway Shell após deploy.
  railway run python inicializar_banco.py
"""
import os, sys, psycopg2

DATABASE_URL = os.getenv('DATABASE_URL')
if not DATABASE_URL:
    print("❌ DATABASE_URL não encontrada."); sys.exit(1)

SCHEMA = os.path.join(os.path.dirname(__file__), 'schema_final.sql')

conn = psycopg2.connect(DATABASE_URL)
conn.autocommit = True
cur = conn.cursor()

with open(SCHEMA, 'r', encoding='utf-8') as f:
    sql = f.read()

# Executar statement por statement
import re
statements = [s.strip() for s in re.split(r';(?:\s*\n)', sql) if s.strip() and not s.strip().startswith('--')]
ok = erro = 0
for stmt in statements:
    if not stmt: continue
    try:
        cur.execute(stmt + ';')
        ok += 1
    except Exception as e:
        msg = str(e)
        if any(x in msg for x in ['already exists','duplicate key','does not exist']):
            ok += 1
        else:
            print(f"  AVISO: {msg[:100]}")
            erro += 1

cur.execute("SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename")
tabelas = [r[0] for r in cur.fetchall()]
conn.close()

print(f"✅ {ok} statements OK | {erro} avisos")
print(f"📋 {len(tabelas)} tabelas: {', '.join(tabelas)}")
print("✅ Banco inicializado!")
