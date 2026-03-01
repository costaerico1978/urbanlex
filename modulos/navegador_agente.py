#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
modulos/navegador_agente.py
────────────────────────────
Agente autônomo de navegação web. Usa Playwright + LLM com visão
para navegar qualquer site de diário oficial.

Loop: OBSERVAR (screenshot+HTML) → DECIDIR (LLM) → AGIR (Playwright) → repetir
"""

import os
import json
import asyncio
import logging
import re
from datetime import date
from typing import Optional, Dict, List, Any

logger = logging.getLogger(__name__)

GEMINI_API_KEY = os.getenv('GEMINI_API_KEY', '')
MAX_PASSOS = int(os.getenv('AGENTE_MAX_PASSOS', '15'))
MAX_SCREENSHOTS = int(os.getenv('AGENTE_MAX_SCREENSHOTS', '10'))

# Domínios permitidos para navegação
DOMINIOS_PERMITIDOS = [
    '.gov.br', '.leg.br', '.jus.br',
    'imprensaoficial', 'diariooficial', 'diariomunicipal',
    'queridodiario.ok.org.br', 'google.com', 'google.com.br',
    'diariodomunicipio', 'doem.org.br',
]


def _dominio_permitido(url: str) -> bool:
    """Verifica se a URL é de um domínio permitido."""
    from urllib.parse import urlparse
    host = urlparse(url).hostname or ''
    return any(d in host for d in DOMINIOS_PERMITIDOS)


def _gemini_vision(screenshot_b64: str, html: str, prompt: str) -> dict:
    """Chama Gemini com screenshot + prompt, retorna JSON."""
    try:
        import google.generativeai as genai
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel('gemini-2.5-flash')

        import base64
        img_bytes = base64.b64decode(screenshot_b64)

        response = model.generate_content([
            {'mime_type': 'image/jpeg', 'data': img_bytes},
            f"HTML simplificado da página:\n{html[:4000]}\n\n{prompt}"
        ])

        texto = response.text.strip()
        # Extrair JSON da resposta
        match = re.search(r'\{.*\}', texto, re.DOTALL)
        if match:
            return json.loads(match.group())
        return {'erro': 'Resposta sem JSON', 'raw': texto[:500]}

    except Exception as e:
        logger.error(f"Gemini vision erro: {e}")
        return {'erro': str(e)}


def _gemini_texto(prompt: str) -> dict:
    """Chama Gemini só com texto, retorna JSON."""
    try:
        import google.generativeai as genai
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel('gemini-2.5-flash')
        response = model.generate_content(prompt)
        texto = response.text.strip()
        match = re.search(r'\{.*\}', texto, re.DOTALL)
        if match:
            return json.loads(match.group())
        # Tentar array
        match = re.search(r'\[.*\]', texto, re.DOTALL)
        if match:
            return {'items': json.loads(match.group())}
        return {'erro': 'Resposta sem JSON', 'raw': texto[:500]}
    except Exception as e:
        return {'erro': str(e)}


# ─────────────────────────────────────────────────────────────────────────────
# AGENTE PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

PROMPT_AGENTE = """Você é um agente de navegação web especializado em diários oficiais brasileiros.

TAREFA: {tarefa}

ESTADO ATUAL:
- Passo: {passo} de {max_passos}
- URL: {url_atual}
- Ações anteriores: {historico}

INSTRUÇÕES:
1. Analise o screenshot e o HTML da página
2. Decida a próxima ação para completar a tarefa
3. Se encontrar resultados, use ação "extrair"
4. Se a busca retornar 0 resultados, tente termos alternativos
5. Se não conseguir, use "concluir" com status "sem_resultados"

AÇÕES DISPONÍVEIS:
- navegar: {"url": "https://..."}
- clicar: {"seletor": "CSS selector"}
- digitar: {"seletor": "CSS selector", "texto": "conteúdo"}
- teclar: {"tecla": "Enter"}
- scroll: {"direcao": "baixo", "pixels": 300}
- esperar: {"segundos": 3}
- extrair: {"tipo": "resultados"} — quando VER resultados de busca na tela
- concluir: {"status": "sucesso|sem_resultados|erro", "mensagem": "..."}

Responda APENAS com JSON:
{
    "raciocinio": "O que estou vendo e por que vou fazer esta ação",
    "acao": "nome_da_acao",
    "parametros": {}
}"""


async def executar_busca_agente(url_diario: str, termos_busca: list,
                                 data_inicio: date, data_fim: date) -> dict:
    """
    Executa busca autônoma no diário oficial usando o agente.

    Returns:
        {sucesso, publicacoes, total, passos_executados, metodo}
    """
    from modulos.browser_pool import abrir_pagina, screenshot_base64, html_simplificado

    resultado = {
        'sucesso': False, 'publicacoes': [], 'total': 0,
        'passos_executados': 0, 'metodo': 'agente',
        'log_passos': [], 'mensagem': ''
    }

    if not GEMINI_API_KEY:
        resultado['mensagem'] = 'GEMINI_API_KEY não configurada'
        return resultado

    termo_principal = termos_busca[0] if termos_busca else ''
    if not termo_principal:
        resultado['mensagem'] = 'Nenhum termo de busca'
        return resultado

    tarefa = (f'Buscar publicações sobre "{termo_principal}" no diário oficial '
              f'({url_diario}). Período: {data_inicio} a {data_fim}. '
              f'Termos alternativos: {termos_busca[1:]}')

    historico = []

    try:
        async with abrir_pagina() as page:
            # Navegar para o site
            await page.goto(url_diario, wait_until='domcontentloaded', timeout=30000)
            await page.wait_for_timeout(2000)  # Esperar JS

            for passo in range(1, MAX_PASSOS + 1):
                resultado['passos_executados'] = passo

                # ── OBSERVAR ──
                scr_b64 = await screenshot_base64(page)
                html = await html_simplificado(page)
                url_atual = page.url

                # ── DECIDIR ──
                prompt = PROMPT_AGENTE.format(
                    tarefa=tarefa,
                    passo=passo,
                    max_passos=MAX_PASSOS,
                    url_atual=url_atual,
                    historico=json.dumps(historico[-5:], ensure_ascii=False)
                )

                decisao = _gemini_vision(scr_b64, html, prompt)

                if 'erro' in decisao:
                    logger.warning(f"  Passo {passo}: LLM erro: {decisao['erro']}")
                    historico.append({'passo': passo, 'acao': 'erro_llm', 'detalhe': decisao['erro']})
                    continue

                acao = decisao.get('acao', '')
                params = decisao.get('parametros', {})
                raciocinio = decisao.get('raciocinio', '')

                log_passo = {
                    'passo': passo, 'acao': acao,
                    'params': params, 'raciocinio': raciocinio[:200]
                }
                historico.append(log_passo)
                resultado['log_passos'].append(log_passo)

                logger.info(f"  Passo {passo}: {acao} — {raciocinio[:100]}")

                # ── AGIR ──
                try:
                    if acao == 'navegar':
                        url = params.get('url', '')
                        if url and _dominio_permitido(url):
                            await page.goto(url, wait_until='domcontentloaded', timeout=30000)
                            await page.wait_for_timeout(2000)

                    elif acao == 'clicar':
                        seletor = params.get('seletor', '')
                        if seletor:
                            await page.click(seletor, timeout=10000)
                            await page.wait_for_timeout(2000)

                    elif acao == 'digitar':
                        seletor = params.get('seletor', '')
                        texto = params.get('texto', '')
                        if seletor and texto:
                            await page.fill(seletor, '')
                            await page.fill(seletor, texto)
                            await page.wait_for_timeout(500)

                    elif acao == 'teclar':
                        tecla = params.get('tecla', 'Enter')
                        await page.keyboard.press(tecla)
                        await page.wait_for_timeout(2000)

                    elif acao == 'scroll':
                        px = params.get('pixels', 300)
                        direcao = params.get('direcao', 'baixo')
                        delta = px if direcao == 'baixo' else -px
                        await page.mouse.wheel(0, delta)
                        await page.wait_for_timeout(1000)

                    elif acao == 'esperar':
                        secs = min(params.get('segundos', 2), 10)
                        await page.wait_for_timeout(secs * 1000)

                    elif acao == 'extrair':
                        # Tirar screenshot atualizado e pedir extração
                        scr_b64 = await screenshot_base64(page)
                        html = await html_simplificado(page)
                        pubs = await _extrair_resultados(scr_b64, html, termos_busca)
                        if pubs:
                            resultado['sucesso'] = True
                            resultado['publicacoes'] = pubs
                            resultado['total'] = len(pubs)
                            resultado['mensagem'] = f'{len(pubs)} publicação(ões) encontrada(s)'
                            return resultado
                        else:
                            historico.append({'passo': passo, 'acao': 'extrair', 'detalhe': '0 resultados extraídos'})

                    elif acao == 'concluir':
                        status = params.get('status', 'sem_resultados')
                        resultado['sucesso'] = status == 'sucesso'
                        resultado['mensagem'] = params.get('mensagem', status)
                        return resultado

                except Exception as e:
                    logger.warning(f"  Passo {passo}: Erro ao executar '{acao}': {e}")
                    historico.append({'passo': passo, 'acao': 'erro_acao', 'detalhe': str(e)[:200]})

        resultado['mensagem'] = f'Limite de {MAX_PASSOS} passos atingido'
        return resultado

    except Exception as e:
        resultado['mensagem'] = f'Erro fatal do agente: {str(e)[:200]}'
        logger.error(f"Agente erro fatal: {e}")
        return resultado


async def _extrair_resultados(scr_b64: str, html: str, termos: list) -> list:
    """Usa LLM para extrair resultados da página de busca."""
    prompt = f"""Analise esta página de resultados de busca de um diário oficial.
Extraia TODAS as publicações visíveis que mencionam: {termos}

Para cada publicação encontrada, retorne:
{{
    "publicacoes": [
        {{
            "titulo": "título ou descrição da publicação",
            "data": "YYYY-MM-DD",
            "url": "URL se visível",
            "conteudo": "trecho relevante do texto",
            "tipo": "decreto|lei|portaria|resolucao|outro"
        }}
    ]
}}

Se não houver resultados visíveis, retorne: {{"publicacoes": []}}
Responda APENAS com JSON."""

    resp = _gemini_vision(scr_b64, html, prompt)
    return resp.get('publicacoes', [])


# ─────────────────────────────────────────────────────────────────────────────
# BUSCA COM CACHE DE PASSOS (memória do agente)
# ─────────────────────────────────────────────────────────────────────────────

async def buscar_com_memoria(url_diario: str, termos_busca: list,
                              data_inicio: date, data_fim: date,
                              passos_salvos: list = None) -> dict:
    """
    Busca usando passos memorizados (se disponíveis) antes de explorar.
    """
    from modulos.browser_pool import abrir_pagina, screenshot_base64, html_simplificado

    # Se tem passos salvos, tenta replay
    if passos_salvos:
        logger.info(f"  Tentando {len(passos_salvos)} passos memorizados...")
        resultado = await _replay_passos(url_diario, termos_busca,
                                          data_inicio, data_fim, passos_salvos)
        if resultado.get('sucesso'):
            resultado['metodo'] = 'agente_memorizado'
            return resultado
        logger.info("  Passos memorizados falharam, explorando...")

    # Modo exploratório
    return await executar_busca_agente(url_diario, termos_busca,
                                        data_inicio, data_fim)


async def _replay_passos(url_diario: str, termos: list,
                          data_inicio: date, data_fim: date,
                          passos: list) -> dict:
    """Replay de passos memorizados, substituindo variáveis."""
    from modulos.browser_pool import abrir_pagina, screenshot_base64, html_simplificado

    resultado = {'sucesso': False, 'publicacoes': [], 'total': 0,
                 'passos_executados': 0, 'metodo': 'agente_memorizado'}

    termo = termos[0] if termos else ''

    try:
        async with abrir_pagina() as page:
            await page.goto(url_diario, wait_until='domcontentloaded', timeout=30000)
            await page.wait_for_timeout(2000)

            for i, passo in enumerate(passos):
                acao = passo.get('acao', '')
                params = dict(passo.get('parametros', passo.get('params', {})))

                # Substituir variáveis nos parâmetros
                for k, v in params.items():
                    if isinstance(v, str):
                        params[k] = (v.replace('{termo}', termo)
                                      .replace('{data_inicio}', str(data_inicio))
                                      .replace('{data_fim}', str(data_fim)))

                try:
                    if acao == 'navegar':
                        url = params.get('url', '')
                        if url.startswith('/'):
                            url = url_diario.rstrip('/') + url
                        await page.goto(url, wait_until='domcontentloaded', timeout=30000)
                        await page.wait_for_timeout(2000)
                    elif acao == 'digitar':
                        await page.fill(params.get('seletor', ''), params.get('texto', ''))
                    elif acao == 'clicar':
                        await page.click(params.get('seletor', ''), timeout=10000)
                        await page.wait_for_timeout(2000)
                    elif acao == 'teclar':
                        await page.keyboard.press(params.get('tecla', 'Enter'))
                        await page.wait_for_timeout(2000)
                    elif acao == 'esperar':
                        await page.wait_for_timeout(params.get('segundos', 2) * 1000)
                    elif acao == 'extrair':
                        scr = await screenshot_base64(page)
                        html = await html_simplificado(page)
                        pubs = await _extrair_resultados(scr, html, termos)
                        if pubs:
                            resultado['sucesso'] = True
                            resultado['publicacoes'] = pubs
                            resultado['total'] = len(pubs)
                            return resultado
                except Exception as e:
                    logger.warning(f"  Replay passo {i}: erro {e}")
                    return resultado

                resultado['passos_executados'] = i + 1

    except Exception as e:
        resultado['mensagem'] = str(e)[:200]

    return resultado
