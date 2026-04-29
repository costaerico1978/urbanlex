#!/bin/bash
cd /var/www/urbanlex
git pull
ATIVOS=$(curl -sf http://localhost:5000/api/buscador/jobs-ativos 2>/dev/null)
if echo "$ATIVOS" | grep -q '"ativos": true'; then
    echo "$(date): Jobs ativos — restart adiado" >> /var/log/urbanlex-deploy.log
    for i in $(seq 1 120); do
        sleep 30
        ATIVOS2=$(curl -sf http://localhost:5000/api/buscador/jobs-ativos 2>/dev/null)
        if ! echo "$ATIVOS2" | grep -q '"ativos": true'; then
            FILA=$(psql postgresql://urbanlex:urbanlex123@localhost:5432/urbanlex -t -c "SELECT COUNT(*) FROM fila_buscas WHERE status='rodando'" 2>/dev/null | tr -d ' ' || echo 0)
            if [ "$FILA" = "0" ]; then
                break
            fi
        fi
    done
fi
pkill -9 -f chromium 2>/dev/null || true
sleep 2
systemctl restart urbanlex
echo "$(date): Deploy executado" >> /var/log/urbanlex-deploy.log
