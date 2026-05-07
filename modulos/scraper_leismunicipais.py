"""
scraper_leismunicipais.py — Scraper deterministico do LeisMunicipais.com.br
v2 — fluxo home-first.

Em vez de partir da pagina de cada municipio (cujo layout mudou e quebrou os
ids tipo 'mais-atos-4'), comecamos sempre em https://leismunicipais.com.br/
e usamos o autocomplete de cidade + checkboxes 'option_atos' do painel
'Mais opcoes'. Vantagens:
  - nao precisa descobrir/cachear municipio_id
  - layout unico pra manter
  - mais resistente a redesigns

Fluxo:
  1) GET https://leismunicipais.com.br/
  2) preenche input[name="city"] com o municipio
  3) aguarda autocomplete e clica na sugestao que casa "{municipio}/{UF}"
  4) clica em "Mais opcoes" (#search_options)
  5) clica em "Todos os atos" (abre lista de checkboxes)
  6) marca input[name="option_atos"][value=X] correspondente ao tipo
  7) preenche input[name="s"] com o numero do ato
  8) clica em "Pesquisar"
  9) localiza link da lei nos resultados (filtro por slug do tipo + ano + numero)
"""
import re
import time
import json
import unicodedata

# value do <input name="option_atos"> nos checkboxes do painel "Mais opcoes"
TIPO_VALUE_ATO = {
    "lei complementar":     "4",
    "lei":                  "28",
    "lei ordinaria":        "28",
    "decreto":              "5",
    "emenda":               "35",
    "emenda lei organica":  "35",
}

# slug usado nas URLs canonicas dos atos no LM (para casar resultados)
SLUG_TIPO = {
    "lei complementar":     "lei-complementar",
    "lei":                  "lei-ordinaria",
    "lei ordinaria":        "lei-ordinaria",
    "decreto":              "decreto",
    "emenda":               "emenda",
    "emenda lei organica":  "emenda",
}

_FS_CACHE = {"cookies": None, "user_agent": None, "expires_at": 0}
_FS_TTL_SEG = 25 * 60


def _slugify(s):
    s = unicodedata.normalize("NFD", s.strip().lower())
    s = re.sub(r"[\u0300-\u036f]", "", s)
    s = re.sub(r"['`´]", "", s)
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s


def _norm_tipo(tipo):
    t = unicodedata.normalize("NFD", tipo.strip().lower())
    t = re.sub(r"[\u0300-\u036f]", "", t)
    t = re.sub(r"\s+", " ", t)
    t = t.replace("º", "").replace("nº", "").replace(".", "").strip()
    return t


def _log(logs, nivel, msg):
    if logs is not None:
        logs.append({"nivel": nivel, "msg": msg})


def _cookies_para_playwright(cookies_fs):
    return [{
        "name": c["name"], "value": c["value"], "domain": c["domain"],
        "path": c.get("path", "/"),
        "expires": int(c.get("expires", c.get("expiry", -1)) or -1),
        "httpOnly": c.get("httpOnly", False),
        "secure": c.get("secure", False),
        "sameSite": (c.get("sameSite") or "Lax").capitalize(),
    } for c in cookies_fs]


def _flaresolverr_cookies(logs=None, force_refresh=False):
    agora = time.time()
    if not force_refresh and _FS_CACHE["cookies"] and _FS_CACHE["expires_at"] > agora:
        _log(logs, "info", "  ScraperLM: cookies FlareSolverr cacheados")
        return {"cookies": _FS_CACHE["cookies"], "user_agent": _FS_CACHE["user_agent"]}
    import requests as rq
    for tentativa in range(3):
        try:
            r = rq.post("http://localhost:8191/v1", json={
                "cmd": "request.get",
                "url": "https://leismunicipais.com.br/",
                "maxTimeout": 120000,
            }, timeout=130)
            data = r.json()
            if data.get("status") == "ok" and "solution" in data:
                sol = data["solution"]
                _FS_CACHE["cookies"] = sol["cookies"]
                _FS_CACHE["user_agent"] = sol["userAgent"]
                _FS_CACHE["expires_at"] = agora + _FS_TTL_SEG
                _log(logs, "ok", f"  ScraperLM: FlareSolverr OK ({len(sol['cookies'])} cookies)")
                return {"cookies": sol["cookies"], "user_agent": sol["userAgent"]}
            _log(logs, "aviso", f"  ScraperLM: FS tent.{tentativa+1} sem solution")
        except Exception as e:
            _log(logs, "aviso", f"  ScraperLM: FS tent.{tentativa+1} {type(e).__name__}: {str(e)[:80]}")
        time.sleep(3 + tentativa * 2)
    _log(logs, "erro", "  ScraperLM: FlareSolverr falhou apos 3 tentativas")
    return None


def buscar_lei_LM(tipo, numero, ano, municipio, uf, logs=None):
    from playwright.sync_api import sync_playwright

    t0 = time.time()
    res = {
        "encontrada": False, "url": None, "titulo": None,
        "tempo_s": 0.0, "estrategia": "scraper_LM_home", "motivo_falha": None,
    }

    tipo_n = _norm_tipo(tipo)
    valor_ato = TIPO_VALUE_ATO.get(tipo_n)
    slug = SLUG_TIPO.get(tipo_n)
    if not valor_ato or not slug:
        res["motivo_falha"] = f"tipo nao suportado: {tipo!r}"
        return res

    fs_data = _flaresolverr_cookies(logs)
    if not fs_data:
        res["motivo_falha"] = "FlareSolverr indisponivel"
        return res

    uf_up = uf.strip().upper()[:2]
    municipio_str = municipio.strip()
    alvo_mun_uf = f"{municipio_str}/{uf_up}".lower()
    numero_str = str(numero).strip()
    ano_str = str(ano).strip()

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True, args=["--no-sandbox"])
            ctx = browser.new_context(
                user_agent=fs_data["user_agent"],
                viewport={"width": 1366, "height": 900},
            )
            ctx.add_cookies(_cookies_para_playwright(fs_data["cookies"]))
            page = ctx.new_page()

            _log(logs, "info", "  ScraperLM: GET https://leismunicipais.com.br/")
            page.goto("https://leismunicipais.com.br/",
                      wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(1500)

            # banners de cookies / overlays
            for sel in ['button:has-text("Aceitar todos")',
                        'button:has-text("Rejeitar")',
                        'button:has-text("Aceitar")']:
                try:
                    b = page.locator(sel).first
                    if b.count() > 0 and b.is_visible():
                        b.click(timeout=2500)
                        break
                except Exception:
                    pass
            page.evaluate("""
                document.querySelectorAll('.black-courtine,.cb-container,.cb-bar')
                    .forEach(e => { e.style.display='none'; e.style.pointerEvents='none'; });
            """)

            # 2) preenche cidade simulando digitacao real (autocomplete responde a keydown)
            try:
                city_input = page.locator('input[name="city"]')
                city_input.click()
                city_input.fill("")
                city_input.type(municipio_str, delay=80)
            except Exception as e:
                res["motivo_falha"] = f"campo city nao encontrado: {type(e).__name__}"
                res["tempo_s"] = round(time.time() - t0, 2)
                browser.close()
                return res
            page.wait_for_timeout(2500)

            # 3) clica na sugestao do autocomplete (matching robusto: normaliza
            #    ambos os lados removendo acentos e qualquer separador)
            mun_n = unicodedata.normalize("NFD", municipio_str.lower())
            mun_n = re.sub(r"[\u0300-\u036f]", "", mun_n)
            mun_n = re.sub(r"[^a-z0-9]+", "", mun_n)
            uf_n = uf_up.lower()
            alvo_mun_js = json.dumps(mun_n)
            alvo_uf_js = json.dumps(uf_n)
            clicado = page.evaluate(f"""
                () => {{
                    const norm = s => (s||'').toString().toLowerCase()
                        .normalize('NFD').replace(/[\\u0300-\\u036f]/g,'')
                        .replace(/[^a-z0-9]+/g,'');
                    const mun = {alvo_mun_js};
                    const uf  = {alvo_uf_js};
                    const cands = Array.from(document.querySelectorAll(
                        'a[href*="/legislacao-municipal/"], li, .autocomplete-item, .ui-menu-item, .suggestion, .tt-suggestion'
                    ));
                    for (const el of cands) {{
                        const txt = (el.textContent || '').trim();
                        const n = norm(txt);
                        if (!n.includes(mun) || !n.includes(uf)) continue;
                        const r = el.getBoundingClientRect();
                        if (r.width === 0 || r.height === 0) continue;
                        el.click();
                        return txt.slice(0, 120);
                    }}
                    return null;
                }}
            """)
            if not clicado:
                res["motivo_falha"] = f"sugestao autocomplete nao encontrada para {alvo_mun_uf}"
                res["tempo_s"] = round(time.time() - t0, 2)
                browser.close()
                return res
            _log(logs, "info", f"  ScraperLM: sugestao clicada: {clicado}")
            page.wait_for_timeout(800)

            # 4) clica em "Mais opcoes" pra abrir o painel
            abriu = False
            for sel in ['#search_options',
                        '#search-options',
                        '.group-btn-more',
                        '.button-box.group-btn-more',
                        '.group-btn-more button',
                        '.group-btn-more a',
                        'button:has-text("Mais opções")',
                        'a:has-text("Mais opções")',
                        'text=Mais opções']:
                try:
                    b = page.locator(sel).first
                    if b.count() > 0 and b.is_visible():
                        b.click(timeout=3000)
                        abriu = True
                        break
                except Exception:
                    pass
            if not abriu:
                _log(logs, "aviso", "  ScraperLM: nao consegui clicar 'Mais opcoes' por seletor")
            page.wait_for_timeout(400)
            # forca exibir painel mesmo se o click nao pegou
            page.evaluate("""
                () => {
                    document.querySelectorAll('.hide-options').forEach(e => {
                        e.classList.remove('hide-options');
                        e.style.display = '';
                    });
                    document.querySelectorAll('.search-options, [class*="search-options"]').forEach(e => {
                        e.style.display = '';
                        e.style.visibility = 'visible';
                    });
                }
            """)
            page.wait_for_timeout(200)

            # 5) clica em "Todos os atos" pra abrir lista de checkboxes
            for sel in ['button:has-text("Todos os atos")',
                        'a:has-text("Todos os atos")',
                        '[role="button"]:has-text("Todos os atos")',
                        'text=Todos os atos']:
                try:
                    b = page.locator(sel).first
                    if b.count() > 0 and b.is_visible():
                        b.click(timeout=2000)
                        break
                except Exception:
                    pass
            page.wait_for_timeout(400)

            # 6) marca checkbox do tipo via value (usa .check do Playwright)
            try:
                cb_loc = page.locator(f'input[name="option_atos"][value="{valor_ato}"]').first
                if cb_loc.count() == 0:
                    res["motivo_falha"] = f"checkbox option_atos[value={valor_ato}] nao_existe"
                    res["tempo_s"] = round(time.time() - t0, 2)
                    browser.close()
                    return res
                cb_loc.check(force=True, timeout=3000)
                if not cb_loc.is_checked():
                    # fallback: click no label envolvente
                    try:
                        page.locator(f'label:has(input[name="option_atos"][value="{valor_ato}"])').first.click(timeout=2000)
                    except Exception:
                        pass
                if not cb_loc.is_checked():
                    res["motivo_falha"] = f"checkbox option_atos[value={valor_ato}] nao marcou"
                    res["tempo_s"] = round(time.time() - t0, 2)
                    browser.close()
                    return res
            except Exception as e:
                res["motivo_falha"] = f"checkbox erro: {type(e).__name__}: {str(e)[:80]}"
                res["tempo_s"] = round(time.time() - t0, 2)
                browser.close()
                return res

            # 7) preenche o numero
            try:
                page.locator('input[name="s"]').fill(numero_str)
            except Exception as e:
                res["motivo_falha"] = f"campo s nao encontrado: {type(e).__name__}"
                res["tempo_s"] = round(time.time() - t0, 2)
                browser.close()
                return res

            # 8) submete
            submetido = False
            for sel in ['button:has-text("Pesquisar")',
                        'input[type="submit"][value*="Pesquisar"]',
                        'form#search-laws button[type="submit"]',
                        'form button[type="submit"]']:
                try:
                    b = page.locator(sel).first
                    if b.count() > 0 and b.is_visible():
                        b.click(timeout=3000)
                        submetido = True
                        break
                except Exception:
                    pass
            if not submetido:
                page.evaluate("""
                    () => {
                        const f = document.querySelector('form#search-laws, form');
                        if (f) f.submit();
                    }
                """)
            try:
                page.wait_for_load_state("domcontentloaded", timeout=20000)
            except Exception:
                pass
            page.wait_for_timeout(2500)

            # 9) localiza link da lei nos resultados
            slug_js = json.dumps(slug)
            ano_jsv = json.dumps(ano_str)
            num_jsv = json.dumps(numero_str)
            achados = page.evaluate(f"""
                () => {{
                    const slug = {slug_js};
                    const ano  = {ano_jsv};
                    const num  = {num_jsv};
                    return Array.from(document.querySelectorAll('a'))
                        .filter(a => a.href.includes('/' + slug + '/' + ano + '/')
                                  && a.href.includes('/' + num + '/'))
                        .slice(0, 1)
                        .map(a => ({{txt: (a.textContent||'').trim().slice(0, 200), href: a.href}}));
                }}
            """)
            browser.close()

            if not achados:
                res["motivo_falha"] = "lei nao encontrada nos resultados"
                res["tempo_s"] = round(time.time() - t0, 2)
                return res

            res["encontrada"] = True
            res["url"] = achados[0]["href"].split("?")[0]
            res["titulo"] = achados[0]["txt"]
            res["tempo_s"] = round(time.time() - t0, 2)
            _log(logs, "ok", f"  ScraperLM: {tipo} {numero}/{ano} achada em {res['tempo_s']}s")
            return res

    except Exception as e:
        res["motivo_falha"] = f"{type(e).__name__}: {str(e)[:120]}"
        res["tempo_s"] = round(time.time() - t0, 2)
        return res


CASOS_TESTE = [
    {"tipo": "Lei Complementar", "numero": "148",   "ano": "2023",
     "municipio": "Xangri-Lá",       "uf": "RS"},
    {"tipo": "Lei Complementar", "numero": "274",   "ano": "2024",
     "municipio": "Rio de Janeiro",  "uf": "RJ"},
    {"tipo": "Decreto",          "numero": "45917", "ano": "2019",
     "municipio": "Rio de Janeiro",  "uf": "RJ"},
]


def _run_tests():
    print("=" * 70)
    print("TESTE scraper_leismunicipais (home-first) — modo standalone")
    print("=" * 70)
    sucessos = 0
    for i, c in enumerate(CASOS_TESTE, 1):
        print(f"\n[{i}/{len(CASOS_TESTE)}] {c['tipo']} {c['numero']}/{c['ano']} "
              f"— {c['municipio']}/{c['uf']}")
        logs = []
        r = buscar_lei_LM(**c, logs=logs)
        for l in logs:
            print(f"    [{l['nivel']}] {l['msg']}")
        if r["encontrada"]:
            sucessos += 1
            print(f"  OK {r['tempo_s']}s — {(r['titulo'] or '')[:80]}")
            print(f"     {r['url'][:120]}")
        else:
            print(f"  FAIL {r['tempo_s']}s — {r['motivo_falha']}")
    print(f"\n{'=' * 70}")
    print(f"RESULTADO: {sucessos}/{len(CASOS_TESTE)} casos resolvidos")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    _run_tests()
