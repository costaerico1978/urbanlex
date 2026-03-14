#!/bin/bash
SERVICE="urbanlex"
EMAIL="costa.erico@gmail.com"

if ! systemctl is-active --quiet $SERVICE; then
    echo "UrbanLex caiu em $(date). Tentando reiniciar..." | mail -s "⚠️ UrbanLex FORA DO AR" $EMAIL
    systemctl restart $SERVICE
    sleep 10
    if systemctl is-active --quiet $SERVICE; then
        echo "UrbanLex reiniciado com sucesso em $(date)." | mail -s "✅ UrbanLex voltou ao ar" $EMAIL
    else
        echo "Falha ao reiniciar UrbanLex em $(date). Intervenção manual necessária." | mail -s "🚨 UrbanLex NÃO reiniciou" $EMAIL
    fi
fi
