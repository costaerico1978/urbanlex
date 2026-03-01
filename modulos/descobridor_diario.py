#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
modulos/descobridor_diario.py
──────────────────────────────
Descobre automaticamente o site do diário oficial de um município/estado.

Estratégia em cascata:
  1. Cache local (tabela diarios_oficiais)
  2. Base conhecida (principais municípios pré-populados)
  3. Busca Google + validação por LLM
  4. Querido Diário (fallback API)
"""

import os
import json
import logging
import requests
from typing import Optional, Dict
from datetime import datetime

logger = logging.getLogger(__name__)

GEMINI_API_KEY = os.getenv('GEMINI_API_KEY', '')

# ─────────────────────────────────────────────────────────────────────────────
# Base conhecida de diários oficiais
# ─────────────────────────────────────────────────────────────────────────────

DIARIO_FEDERAL = {
    'nome': 'Diário Oficial da União',
    'url': 'https://www.in.gov.br/servicos/diario-oficial-da-uniao',
    'tipo_plataforma': 'imprensa_nacional',
}

DIARIOS_ESTADUAIS = {
    'AC': {'nome': 'DOE Acre', 'url': 'http://www.diario.ac.gov.br'},
    'AL': {'nome': 'DOE Alagoas', 'url': 'https://www.imprensaoficialal.com.br'},
    'AM': {'nome': 'DOE Amazonas', 'url': 'https://diario.imprensaoficial.am.gov.br'},
    'AP': {'nome': 'DOE Amapá', 'url': 'https://diofe.ap.gov.br'},
    'BA': {'nome': 'DOE Bahia', 'url': 'https://dool.egba.ba.gov.br'},
    'CE': {'nome': 'DOE Ceará', 'url': 'https://www.ceara.gov.br/diario-oficial'},
    'DF': {'nome': 'DODF', 'url': 'https://dodf.df.gov.br'},
    'ES': {'nome': 'DOE Espírito Santo', 'url': 'https://ioes.dio.es.gov.br'},
    'GO': {'nome': 'DOE Goiás', 'url': 'https://diariooficial.abc.go.gov.br'},
    'MA': {'nome': 'DOE Maranhão', 'url': 'https://www.diariooficial.ma.gov.br'},
    'MG': {'nome': 'DOE Minas Gerais', 'url': 'https://www.jornalminasgerais.mg.gov.br'},
    'MS': {'nome': 'DOE Mato Grosso do Sul', 'url': 'https://www.spdo.ms.gov.br/diariodoe'},
    'MT': {'nome': 'DOE Mato Grosso', 'url': 'https://www.iomat.mt.gov.br'},
    'PA': {'nome': 'DOE Pará', 'url': 'https://www.ioepa.com.br'},
    'PB': {'nome': 'DOE Paraíba', 'url': 'https://auniao.pb.gov.br/doe'},
    'PE': {'nome': 'DOE Pernambuco', 'url': 'https://www.cepe.com.br/diariooficial'},
    'PI': {'nome': 'DOE Piauí', 'url': 'https://www.diariooficial.pi.gov.br'},
    'PR': {'nome': 'DOE Paraná', 'url': 'https://www.legislacao.pr.gov.br/diario'},
    'RJ': {'nome': 'DOE Rio de Janeiro', 'url': 'https://www.ioerj.com.br'},
    'RN': {'nome': 'DOE Rio Grande do Norte', 'url': 'http://www.diariooficial.rn.gov.br'},
    'RO': {'nome': 'DOE Rondônia', 'url': 'https://diof.ro.gov.br'},
    'RR': {'nome': 'DOE Roraima', 'url': 'https://www.imprensaoficial.rr.gov.br'},
    'RS': {'nome': 'DOE Rio Grande do Sul', 'url': 'https://www.diariooficial.rs.gov.br'},
    'SC': {'nome': 'DOE Santa Catarina', 'url': 'https://www.doe.sea.sc.gov.br'},
    'SE': {'nome': 'DOE Sergipe', 'url': 'https://segrase.se.gov.br'},
    'SP': {'nome': 'DOE São Paulo', 'url': 'https://www.doe.sp.gov.br'},
    'TO': {'nome': 'DOE Tocantins', 'url': 'https://diariooficial.to.gov.br'},
}

DIARIOS_MUNICIPAIS = {}
# Base municipal removida intencionalmente.
# URLs de diários municipais mudam frequentemente e variam muito.
# O sistema descobre automaticamente via Google+LLM e salva no cache.


# ─────────────────────────────────────────────────────────────────────────────
# Funções auxiliares de DB
# ─────────────────────────────────────────────────────────────────────────────

def _db():
    import psycopg2
    from psycopg2.extras import RealDictCursor
    return psycopg2.connect(os.getenv('DATABASE_URL'), cursor_factory=RealDictCursor)


def _qry(sql, params=None, fetch='all', commit=False):
    conn = _db()
    cur = conn.cursor()
    cur.execute(sql, params)
    if commit:
        conn.commit()
    result = None
    if fetch == 'one':
        result = cur.fetchone()
    elif fetch == 'all':
        result = cur.fetchall()
    cur.close()
    conn.close()
    return result


def _validar_url(url: str, timeout: int = 12) -> bool:
    """Verifica se a URL realmente responde (HEAD ou GET)."""
    if not url:
        return False
    try:
        r = requests.head(url, timeout=timeout, allow_redirects=True,
                          headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code < 400:
            return True
        # Alguns sites bloqueiam HEAD, tentar GET
        r = requests.get(url, timeout=timeout, allow_redirects=True,
                         headers={'User-Agent': 'Mozilla/5.0'}, stream=True)
        r.close()
        return r.status_code < 400
    except Exception:
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Função principal: descobrir diário oficial
# ─────────────────────────────────────────────────────────────────────────────

def descobrir_diario(municipio: str, estado: str, esfera: str = 'municipal',
                     municipio_id: int = None) -> dict:
    """
    Descobre o site do diário oficial para uma localidade.
    Toda URL é validada antes de ser aceita.

    Returns:
        {url, nome, tipo_plataforma, metodo_busca, origem}
    """
    resultado = {
        'url': '', 'nome': '', 'tipo_plataforma': '',
        'metodo_busca': {}, 'origem': '', 'codigo_ibge': '',
    }

    # ── 1. Cache local (já validado antes de salvar) ──
    cache = _buscar_cache(municipio, estado, esfera, municipio_id)
    if cache and cache.get('url_principal') and cache.get('funcionando'):
        # Re-validar periodicamente (se verificado há mais de 7 dias)
        verificado = cache.get('verificado_em')
        revalidar = True
        if verificado:
            from datetime import timedelta
            revalidar = (datetime.now() - verificado) > timedelta(days=7)

        if not revalidar or _validar_url(cache['url_principal']):
            logger.info(f"  Diário no cache: {cache['url_principal']}")
            resultado.update({
                'url': cache['url_principal'],
                'nome': cache.get('nome', ''),
                'tipo_plataforma': cache.get('tipo_plataforma', ''),
                'metodo_busca': cache.get('metodo_busca') or {},
                'codigo_ibge': cache.get('codigo_ibge', ''),
                'origem': 'cache',
            })
            return resultado
        else:
            logger.warning(f"  Cache invalidado — URL não responde: {cache['url_principal']}")
            _marcar_cache_invalido(cache.get('id'))

    # ── 2. Base conhecida (só federal e estadual — municipais são descobertos) ──
    if esfera == 'federal':
        url = DIARIO_FEDERAL['url']
        if _validar_url(url):
            resultado.update({**DIARIO_FEDERAL, 'origem': 'base_conhecida'})
            _salvar_cache(resultado, esfera, estado, municipio, municipio_id)
            return resultado

    if esfera == 'estadual':
        info = DIARIOS_ESTADUAIS.get(estado)
        if info and _validar_url(info['url']):
            resultado.update({
                'url': info['url'], 'nome': info['nome'],
                'tipo_plataforma': 'estadual', 'origem': 'base_conhecida',
            })
            _salvar_cache(resultado, esfera, estado, municipio, municipio_id)
            return resultado

    # ── 3. Busca Google + LLM (principal método para municipais) ──
    url_google = _buscar_google(municipio, estado, esfera)
    if url_google:
        resultado.update({
            'url': url_google, 'nome': f'DO {municipio}/{estado}',
            'tipo_plataforma': 'descoberto', 'origem': 'busca_google',
        })
        _salvar_cache(resultado, esfera, estado, municipio, municipio_id)
        return resultado

    # ── 4. Querido Diário ──
    qd = _buscar_querido_diario(municipio, estado)
    if qd:
        resultado.update({
            'url': 'https://queridodiario.ok.org.br',
            'nome': f'Querido Diário - {municipio}/{estado}',
            'tipo_plataforma': 'querido_diario',
            'codigo_ibge': qd.get('codigo_ibge', ''),
            'origem': 'querido_diario',
        })
        _salvar_cache(resultado, esfera, estado, municipio, municipio_id)
        return resultado

    logger.warning(f"  Não foi possível descobrir diário para {municipio}/{estado}")
    resultado['origem'] = 'nao_encontrado'
    return resultado


# ─────────────────────────────────────────────────────────────────────────────
# Estratégias individuais
# ─────────────────────────────────────────────────────────────────────────────

def _buscar_cache(municipio, estado, esfera, municipio_id):
    """Busca na tabela diarios_oficiais."""
    try:
        if municipio_id:
            return _qry("SELECT * FROM diarios_oficiais WHERE municipio_id=%s LIMIT 1",
                        (municipio_id,), 'one')
        if esfera == 'estadual':
            return _qry("SELECT * FROM diarios_oficiais WHERE esfera='estadual' AND uf=%s LIMIT 1",
                        (estado,), 'one')
        if esfera == 'federal':
            return _qry("SELECT * FROM diarios_oficiais WHERE esfera='federal' LIMIT 1",
                        fetch='one')
        # Municipal por nome
        return _qry("""SELECT * FROM diarios_oficiais
                       WHERE esfera='municipal' AND uf=%s
                         AND LOWER(nome) LIKE LOWER(%s) LIMIT 1""",
                    (estado, f'%{municipio}%'), 'one')
    except Exception:
        return None


def _salvar_cache(resultado, esfera, estado, municipio, municipio_id):
    """Salva no cache de diários oficiais."""
    try:
        _qry("""INSERT INTO diarios_oficiais (esfera, uf, municipio_id, nome,
                    url_principal, tipo_plataforma, codigo_ibge, verificado_em, funcionando)
                VALUES (%s,%s,%s,%s,%s,%s,%s,NOW(),TRUE)
                ON CONFLICT (esfera, uf, COALESCE(municipio_id, 0))
                DO UPDATE SET url_principal=EXCLUDED.url_principal,
                              tipo_plataforma=EXCLUDED.tipo_plataforma,
                              verificado_em=NOW(), funcionando=TRUE""",
             (esfera, estado, municipio_id, resultado.get('nome', ''),
              resultado.get('url', ''), resultado.get('tipo_plataforma', ''),
              resultado.get('codigo_ibge', '')),
             commit=True, fetch=None)
    except Exception as e:
        logger.debug(f"Cache save falhou (ok na 1a vez): {e}")


def _marcar_cache_invalido(cache_id):
    """Marca um registro de cache como não funcionando."""
    if not cache_id:
        return
    try:
        _qry("UPDATE diarios_oficiais SET funcionando=FALSE, verificado_em=NOW() WHERE id=%s",
             (cache_id,), commit=True, fetch=None)
    except Exception:
        pass


def _buscar_google(municipio: str, estado: str, esfera: str) -> str:
    """Busca diário oficial no Google e valida com LLM."""
    if not GEMINI_API_KEY:
        return ''

    query = f"diário oficial {municipio} {estado}" if esfera == 'municipal' else f"diário oficial estado {estado}"

    try:
        import google.generativeai as genai
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel('gemini-2.5-flash')

        prompt = f"""Qual é a URL do site oficial do diário oficial do município de {municipio}, estado {estado}, Brasil?

Regras:
- Retorne APENAS a URL principal do site (ex: https://doweb.rio.rj.gov.br)
- Priorize domínios .gov.br
- Não retorne páginas específicas, apenas o domínio principal
- Se não souber com certeza, retorne vazio

Responda APENAS com JSON: {{"url": "https://...", "confianca": 0.9}}"""

        response = model.generate_content(prompt)
        texto = response.text.strip()

        import re
        match = re.search(r'\{.*\}', texto, re.DOTALL)
        if match:
            data = json.loads(match.group())
            url = data.get('url', '')
            conf = data.get('confianca', 0)
            if url and conf >= 0.7:
                # Validar se URL responde
                try:
                    r = requests.head(url, timeout=10, allow_redirects=True)
                    if r.status_code < 400:
                        logger.info(f"  Google+LLM encontrou: {url} (confiança: {conf})")
                        return url
                except Exception:
                    pass

    except Exception as e:
        logger.debug(f"Busca Google falhou: {e}")

    return ''


def _buscar_querido_diario(municipio: str, estado: str) -> dict:
    """Verifica se município existe no Querido Diário."""
    try:
        # Buscar código IBGE
        UF_CODES = {
            'AC': 12, 'AL': 27, 'AP': 16, 'AM': 13, 'BA': 29, 'CE': 23,
            'DF': 53, 'ES': 32, 'GO': 52, 'MA': 21, 'MT': 51, 'MS': 50,
            'MG': 31, 'PA': 15, 'PB': 25, 'PR': 41, 'PE': 26, 'PI': 22,
            'RJ': 33, 'RN': 24, 'RS': 43, 'RO': 11, 'RR': 14, 'SC': 42,
            'SP': 35, 'SE': 28, 'TO': 17,
        }
        uf_code = UF_CODES.get(estado)
        if not uf_code:
            return {}

        r = requests.get(
            f'https://servicodados.ibge.gov.br/api/v1/localidades/estados/{uf_code}/municipios',
            timeout=15
        )
        if r.status_code != 200:
            return {}

        import unicodedata
        def norm(s):
            return unicodedata.normalize('NFD', s).encode('ascii', 'ignore').decode().lower().strip()

        nome_norm = norm(municipio)
        for m in r.json():
            if norm(m.get('nome', '')) == nome_norm:
                codigo = str(m['id'])
                # Verificar se QD tem este município
                qd = requests.get(
                    'https://queridodiario.ok.org.br/api/gazettes',
                    params={'territory_ids': codigo, 'size': 1},
                    timeout=15
                )
                if qd.status_code == 200 and qd.json().get('total_gazettes', 0) > 0:
                    logger.info(f"  Querido Diário: {municipio} disponível (IBGE: {codigo})")
                    return {'codigo_ibge': codigo}
                break

    except Exception as e:
        logger.debug(f"QD check falhou: {e}")

    return {}
