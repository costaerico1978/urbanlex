"""
Módulo de busca automática de legislações urbanísticas por município.
"""
import re
import requests
import json as _json

def buscar_legislacoes_urbanisticas(municipio, estado, logs, chamar_llm):
    resultado = {"encontradas": [], "nao_encontrada": False}
    analisadas = set()

    # ETAPA 1: Gemini com Google Search Grounding
    logs.append({"nivel": "ok", "msg": f"Consultando Gemini com busca web sobre {municipio}/{estado}..."})
    conteudo_web = ""
    legs = []
    try:
        from google import genai as _genai_new
        from google.genai import types as _types_new
        import os
        GEMINI_KEY = os.environ.get("GEMINI_API_KEY", "")
        pergunta = f"Qual legislacao define os parametros urbanisticos de {municipio}, {estado}? Informe o tipo, numero e ano da lei."
        logs.append({"nivel": "info", "msg": f"Pergunta: {pergunta}"})
        client = _genai_new.Client(api_key=GEMINI_KEY)
        google_search_tool = _types_new.Tool(google_search=_types_new.GoogleSearch())
        config = _types_new.GenerateContentConfig(tools=[google_search_tool])
        import concurrent.futures as _cf
        with _cf.ThreadPoolExecutor() as _ex:
            _fut = _ex.submit(client.models.generate_content, model="gemini-2.5-flash", contents=pergunta, config=config)
            response = _fut.result(timeout=30)
        resp_texto = response.text.strip()
        logs.append({"nivel": "ok", "msg": f"Gemini respondeu: {resp_texto[:300]}"})
        # Estruturar resposta em JSON
        prompt_estruturar = (
            f"Com base na resposta abaixo, extraia as legislacoes mencionadas de {municipio}/{estado}.\n"
            f"Use APENAS informacoes presentes — nao invente numeros.\n\n"
            f"RESPOSTA:\n{resp_texto}\n\n"
            'Responda APENAS com JSON: {"legislacoes": [{"tipo": "Lei Complementar", "numero": "148", "ano": "2023", "descricao": "Plano Diretor"}]}'
        )
        resp2 = chamar_llm(prompt_estruturar, logs, "IA estruturar")
        if resp2:
            import re as _re
            resp_c = _re.sub(r"^```json\s*|\s*```$", "", resp2.strip())
            legs = _json.loads(resp_c).get("legislacoes", [])
            logs.append({"nivel": "ok", "msg": f"IA identificou {len(legs)} legislacao(oes)"})
    except Exception as e:
        logs.append({"nivel": "aviso", "msg": f"Gemini Search falhou: {str(e)[:100]} — usando DDG"})
        # Fallback DDG
        try:
            from modulos.buscador_legislacoes import _pesquisar_web
            query = f"qual legislacao define os parametros urbanisticos de {municipio} {estado}"
            resultados_ddg = _pesquisar_web(query, logs, "DDG urbanistico", max_results=5)
            if resultados_ddg:
                for res in resultados_ddg:
                    conteudo_web += f"{res.get('title', '')}\n{res.get('body', '')}\n\n"
            if conteudo_web:
                prompt_ddg = (
                    f"Com base nos resultados abaixo, identifique a legislacao de {municipio}/{estado}.\n"
                    f"Nao invente numeros.\n\nRESULTADOS:\n{conteudo_web[:3000]}\n\n"
                    'Responda APENAS com JSON: {"legislacoes": [{"tipo": "Lei Complementar", "numero": "", "ano": "", "descricao": ""}]}'
                )
                resp_ddg = chamar_llm(prompt_ddg, logs, "IA DDG")
                if resp_ddg:
                    import re as _re2
                    resp_c2 = _re2.sub(r"^```json\s*|\s*```$", "", resp_ddg.strip())
                    legs = _json.loads(resp_c2).get("legislacoes", [])
        except Exception as e2:
            logs.append({"nivel": "aviso", "msg": f"DDG falhou: {str(e2)[:60]}"})

    if not legs:
        logs.append({"nivel": "aviso", "msg": "Nenhuma legislacao identificada"})
        resultado["nao_encontrada"] = True
        return resultado

    # ETAPA 2: Buscar no LeisMunicipais
    for leg in legs:
        tipo = leg.get("tipo", "")
        numero = leg.get("numero", "")
        ano = leg.get("ano", "")
        descricao = leg.get("descricao", "")
        chave = f"{tipo}_{numero}_{ano}".lower()
        if chave in analisadas:
            continue
        analisadas.add(chave)
        logs.append({"nivel": "info", "msg": f"Buscando {tipo} n {numero}/{ano} ({descricao}) no LeisMunicipais..."})
        enc = _buscar_leismunicipais(municipio, estado, tipo, numero, ano, logs, chamar_llm, analisadas)
        if enc:
            resultado["encontradas"].append(enc)

    # ETAPA 3: Fallback Google
    if not resultado["encontradas"]:
        logs.append({"nivel": "info", "msg": "Nenhuma encontrada — buscando no Google..."})
        for termo_tipo in ["plano diretor", "uso e ocupacao do solo", "zoneamento"]:
            if resultado["encontradas"]:
                break
            termo = f"Leis municipais {termo_tipo} {municipio}, {estado}"
            logs.append({"nivel": "info", "msg": f"Google: {termo}"})
            enc = _buscar_google(termo, municipio, estado, logs, chamar_llm, analisadas)
            if enc:
                resultado["encontradas"].append(enc)

    # Resultado final
    if resultado["encontradas"]:
        logs.append({"nivel": "ok", "msg": "-" * 50})
        logs.append({"nivel": "ok", "msg": "Legislacao com os parametros urbanisticos encontrada:"})
        for leg in resultado["encontradas"]:
            logs.append({"nivel": "ok", "msg": f'  - {leg["tipo"]} No {leg["numero"]} de {leg["ano"]} ({leg["link"]})'})
    else:
        resultado["nao_encontrada"] = True
        logs.append({"nivel": "aviso", "msg": "-" * 50})
        logs.append({"nivel": "aviso", "msg": "Legislacao com os parametros urbanisticos nao encontrada"})
    return resultado


def _verificar_parametros(texto, municipio, estado, tipo, numero, ano, logs, chamar_llm):
    prompt = (
        f"O texto abaixo e de uma legislacao de {municipio}/{estado}.\n"
        f"Esta legislacao estabelece parametros urbanisticos (zoneamento, uso do solo, recuos, gabarito, coeficiente de aproveitamento, taxa de ocupacao)?\n\n"
        f"TEXTO:\n{texto[:4000]}\n\n"
        'Responda APENAS com JSON: {"define_parametros": true, "motivo": "explicacao"}'
    )
    resp = chamar_llm(prompt, logs, f"Verif {tipo} {numero}")
    if not resp:
        return False
    try:
        import re as _re2
        resp_c = _re2.sub(r"^```json\s*|\s*```$", "", resp.strip())
        dados = _json.loads(resp_c)
        motivo = dados.get("motivo", "")[:100]
        if dados.get("define_parametros"):
            logs.append({"nivel": "ok", "msg": f"  IA confirmou: {motivo}"})
            return True
        else:
            logs.append({"nivel": "info", "msg": f"  IA: nao define parametros - {motivo}"})
            return False
    except:
        return False


def _buscar_leismunicipais(municipio, estado, tipo, numero, ano, logs, chamar_llm, analisadas):
    try:
        from modulos.navegador_universal import navegar_com_cookies_flaresolverr
        # Renovar sessao FlareSolverr
        try:
            import requests as _rfs, os as _ofs
            _old = _ofs.environ.get("FLARESOLVERR_SESSION", "")
            if _old:
                _rfs.post("http://localhost:8191/v1", json={"cmd": "sessions.destroy", "session": _old}, timeout=5)
            _rn = _rfs.post("http://localhost:8191/v1", json={"cmd": "sessions.create"}, timeout=10)
            _ns = _rn.json().get("session", "")
            if _ns:
                _ofs.environ["FLARESOLVERR_SESSION"] = _ns
                import subprocess as _sp
                _sp.run(["sed", "-i", f"s/FLARESOLVERR_SESSION=.*/FLARESOLVERR_SESSION={_ns}/", "/var/www/urbanlex/.env"], capture_output=True)
                logs.append({"nivel": "info", "msg": f"FlareSolverr sessao renovada: {_ns[:8]}..."})
        except Exception as _ef:
            logs.append({"nivel": "aviso", "msg": f"FlareSolverr renovacao falhou: {str(_ef)[:60]}"})
        url_fs = "https://leismunicipais.com.br"
        leg_dict = {"tipo": tipo, "numero": numero, "ano": ano, "municipio": municipio, "estado": estado}
        logs.append({"nivel": "info", "msg": f"  Buscando {tipo} {numero}/{ano} no LeisMunicipais via FlareSolverr..."})
        fs_result = navegar_com_cookies_flaresolverr(url_fs, leg_dict, logs, label=f"LM {tipo} {numero}", chamar_llm=chamar_llm)
        if fs_result.get("encontrada") and fs_result.get("url"):
            url_enc = fs_result["url"]
            if url_enc.lower() in analisadas:
                return None
            analisadas.add(url_enc.lower())
            logs.append({"nivel": "ok", "msg": f"  LeisMunicipais: encontrada! {url_enc[:80]}"})
            return {"tipo": tipo, "numero": numero, "ano": ano, "link": url_enc}
        logs.append({"nivel": "info", "msg": "  LeisMunicipais: nao encontrada"})
    except Exception as e:
        logs.append({"nivel": "aviso", "msg": f"  Erro LeisMunicipais: {str(e)[:80]}"})
    return None

def _buscar_google(termo, municipio, estado, logs, chamar_llm, analisadas):
    try:
        import urllib.parse
        query = urllib.parse.quote_plus(termo + " site:leismunicipais.com.br")
        url_g = f"https://www.google.com/search?q={query}&num=10"
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url_g, headers=headers, timeout=15)
        pattern = r"https?://(?:www\.)?leismunicipais\.com\.br/[^\s\"&<>]+"
        links = list(dict.fromkeys(re.findall(pattern, r.text)))[:10]
        for link in links:
            if link in analisadas:
                continue
            analisadas.add(link)
            logs.append({"nivel": "info", "msg": f"  Verificando: {link[:80]}"})
            try:
                r2 = requests.get(link, headers=headers, timeout=15)
                if r2.status_code != 200:
                    continue
                from bs4 import BeautifulSoup
                texto = BeautifulSoup(r2.text, "html.parser").get_text()
                m = re.search(r"/([a-z-]+)/(\d{2})/(\d+)/(\d+)", link)
                tipo_u = m.group(1).replace("-", " ").title() if m else "Legislacao"
                ano_u = "20" + m.group(2) if m else ""
                num_u = m.group(4) if m else ""
                if _verificar_parametros(texto, municipio, estado, tipo_u, num_u, ano_u, logs, chamar_llm):
                    return {"tipo": tipo_u, "numero": num_u, "ano": ano_u, "link": link}
            except Exception as e2:
                logs.append({"nivel": "aviso", "msg": f"  Erro link: {str(e2)[:50]}"})
    except Exception as e:
        logs.append({"nivel": "aviso", "msg": f"Erro Google: {str(e)[:60]}"})
    return None
