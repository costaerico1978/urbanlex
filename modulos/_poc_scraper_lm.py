"""Valida o scraper LM em 3 casos antes de empacotar."""
import time
import requests as rq
from playwright.sync_api import sync_playwright

TIPO_CHECKBOX_ID = {
    "Lei Complementar": "mais-atos-4",
    "Lei Ordinaria":    "mais-atos-28",
    "Lei":              "mais-atos-28",
    "Decreto":          "mais-atos-5",
    "Emenda":           "mais-atos-35",
}

# Para extrair o slug do tipo da URL canonica esperada
SLUG_TIPO = {
    "Lei Complementar": "lei-complementar",
    "Lei":              "lei-ordinaria",
    "Lei Ordinaria":    "lei-ordinaria",
    "Decreto":          "decreto",
    "Emenda":           "emenda",
}

CASOS = [
    {"tipo": "Lei Complementar", "numero": "274", "ano": "2024", "obs": "ja achada antes via Gemini"},
    {"tipo": "Lei Complementar", "numero": "270", "ano": "2024", "obs": "falhou no Gemini, achada agora"},
    {"tipo": "Decreto",          "numero": "45917", "ano": "2019", "obs": "tipo diferente, ano antigo"},
]

# Cookies uma vez so (reaproveita pra todos)
print("[*] Cookies via FlareSolverr...")
fs = rq.post("http://localhost:8191/v1", json={
    "cmd": "request.get", "url": "https://leismunicipais.com.br/", "maxTimeout": 60000
}, timeout=90).json()["solution"]
print(f"[ok] {len(fs['cookies'])} cookies")

def buscar_caso(caso, ctx):
    tipo = caso["tipo"]
    numero = caso["numero"]
    ano = caso["ano"]
    cb_id = TIPO_CHECKBOX_ID[tipo]
    slug = SLUG_TIPO[tipo]
    
    page = ctx.new_page()
    t0 = time.time()
    
    try:
        page.goto("https://leismunicipais.com.br/legislacao-municipal/3613/leis-de-rio-de-janeiro",
                  wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(1500)
        
        for sel in ['button:has-text("Aceitar todos")']:
            try:
                b = page.locator(sel).first
                if b.count() > 0 and b.is_visible():
                    b.click(timeout=3000); break
            except: pass
        
        page.evaluate("""
            document.querySelectorAll('.black-courtine,.cb-container,.cb-bar').forEach(e=>{
                e.style.display='none'; e.style.pointerEvents='none';
            });
        """)
        
        page.locator('input[name="q"]').fill(numero)
        
        page.evaluate(f"""
            () => {{
                const cb = document.getElementById('{cb_id}');
                if (cb) {{
                    cb.checked = true;
                    cb.dispatchEvent(new Event('change', {{bubbles: true}}));
                }}
            }}
        """)
        
        page.evaluate("""
            () => {
                const f = document.querySelector('form#form-pesquisa, form[action*="search"], form');
                if (f) f.submit();
            }
        """)
        
        try:
            page.wait_for_load_state("domcontentloaded", timeout=15000)
        except: pass
        page.wait_for_timeout(2500)
        
        achados = page.evaluate(f"""
            () => Array.from(document.querySelectorAll('a'))
                .filter(a => a.href.includes('/{slug}/{ano}/') 
                          && a.href.includes('/{numero}/'))
                .slice(0,1)
                .map(a => ({{txt:a.textContent.trim().slice(0,150), href:a.href}}))
        """)
        
        elapsed = round(time.time() - t0, 1)
        if achados:
            return {"ok": True, "tempo": elapsed, "txt": achados[0]["txt"], "href": achados[0]["href"]}
        return {"ok": False, "tempo": elapsed, "url_final": page.url}
    finally:
        page.close()

with sync_playwright() as pw:
    browser = pw.chromium.launch(headless=True, args=["--no-sandbox"])
    ctx = browser.new_context(user_agent=fs["userAgent"], viewport={"width": 1280, "height": 900})
    ctx.add_cookies([{
        "name": c["name"], "value": c["value"], "domain": c["domain"],
        "path": c.get("path", "/"), "expires": int(c.get("expires", -1) or -1),
        "httpOnly": c.get("httpOnly", False), "secure": c.get("secure", False),
        "sameSite": (c.get("sameSite") or "Lax").capitalize(),
    } for c in fs["cookies"]])
    
    print(f"\n{'='*70}")
    sucessos = 0
    for i, caso in enumerate(CASOS, 1):
        print(f"\n[{i}/3] {caso['tipo']} {caso['numero']}/{caso['ano']} — {caso['obs']}")
        r = buscar_caso(caso, ctx)
        if r["ok"]:
            sucessos += 1
            print(f"  ✅ {r['tempo']}s — {r['txt'][:80]}")
            print(f"     {r['href'][:120]}")
        else:
            print(f"  ❌ {r['tempo']}s — url={r.get('url_final','?')[:100]}")
    
    print(f"\n{'='*70}")
    print(f"RESULTADO: {sucessos}/3 casos resolvidos")
    print(f"{'='*70}")
    browser.close()
