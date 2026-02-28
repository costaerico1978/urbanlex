#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
modulos/browser_pool.py
───────────────────────
Gerencia instâncias do Playwright (Chromium) com pool limitado.
Evita estourar memória no Railway abrindo muitos browsers simultâneos.
"""

import os
import asyncio
import logging
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)

MAX_BROWSERS = int(os.getenv('MAX_BROWSERS', '2'))
HEADLESS = os.getenv('BROWSER_HEADLESS', 'true').lower() == 'true'
TIMEOUT_MS = int(os.getenv('BROWSER_TIMEOUT_MS', '30000'))

_semaphore = asyncio.Semaphore(MAX_BROWSERS)
_playwright = None
_browser = None


async def _get_browser():
    """Retorna instância singleton do browser."""
    global _playwright, _browser
    if _browser and _browser.is_connected():
        return _browser

    try:
        from playwright.async_api import async_playwright
        _playwright = await async_playwright().start()

        # Detectar Chromium do sistema (nixpacks) ou instalado pelo Playwright
        chromium_path = os.getenv('PLAYWRIGHT_CHROMIUM_PATH', '')
        if not chromium_path:
            # Tentar path do nixpacks
            for p in ['/usr/bin/chromium', '/usr/bin/chromium-browser',
                      '/nix/store/chromium/bin/chromium']:
                if os.path.exists(p):
                    chromium_path = p
                    break

        launch_args = {
            'headless': HEADLESS,
            'args': [
                '--no-sandbox', '--disable-setuid-sandbox',
                '--disable-dev-shm-usage', '--disable-gpu',
                '--single-process', '--no-zygote',
            ]
        }
        if chromium_path:
            launch_args['executable_path'] = chromium_path
            logger.info(f"Usando Chromium: {chromium_path}")

        _browser = await _playwright.chromium.launch(**launch_args)
        logger.info("Browser Playwright iniciado")
        return _browser

    except Exception as e:
        logger.error(f"Erro ao iniciar Playwright: {e}")
        raise


@asynccontextmanager
async def abrir_pagina():
    """Context manager que abre uma página com controle de concorrência."""
    async with _semaphore:
        browser = await _get_browser()
        context = await browser.new_context(
            viewport={'width': 1280, 'height': 900},
            user_agent='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                       '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        )
        context.set_default_timeout(TIMEOUT_MS)
        page = await context.new_page()
        try:
            yield page
        finally:
            await page.close()
            await context.close()


async def screenshot_base64(page) -> str:
    """Tira screenshot e retorna como base64 para enviar ao LLM."""
    import base64
    buf = await page.screenshot(type='jpeg', quality=70, full_page=False)
    return base64.b64encode(buf).decode('utf-8')


async def html_simplificado(page) -> str:
    """Extrai HTML simplificado da página (sem scripts/styles, max 5000 chars)."""
    try:
        html = await page.evaluate("""() => {
            const clone = document.body.cloneNode(true);
            clone.querySelectorAll('script, style, svg, noscript, iframe').forEach(e => e.remove());
            let text = clone.innerHTML;
            // Simplificar atributos
            text = text.replace(/\\s(class|style|data-[^=]*)="[^"]*"/g, '');
            return text.substring(0, 8000);
        }""")
        return html
    except Exception:
        return ""


async def fechar_tudo():
    """Fecha browser e playwright ao encerrar a aplicação."""
    global _browser, _playwright
    if _browser:
        await _browser.close()
        _browser = None
    if _playwright:
        await _playwright.stop()
        _playwright = None
    logger.info("Browser pool encerrado")


def playwright_disponivel() -> bool:
    """Verifica se Playwright está instalado."""
    try:
        import playwright
        return True
    except ImportError:
        return False
