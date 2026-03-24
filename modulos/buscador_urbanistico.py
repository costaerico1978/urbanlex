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

    # Filtrar legs sem numero — acionar fallback por palavra-chave
    legs_com_numero = [l for l in legs if l.get("numero", "").strip()]

    if not legs_com_numero:
        if not legs:
            logs.append({"nivel": "aviso", "msg": "Gemini nao identificou legislacao — tentando busca por Plano Diretor no LeisMunicipais..."})
        else:
            logs.append({"nivel": "aviso", "msg": "Gemini identificou legislacao mas sem numero — tentando busca por Plano Diretor no LeisMunicipais..."})
        enc = _buscar_plano_diretor_lm(municipio, estado, logs, chamar_llm, analisadas)
        if enc:
            resultado["encontradas"].append(enc)
        else:
            resultado["nao_encontrada"] = True
        return resultado

    legs = legs_com_numero
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
            break  # Para na primeira legislacao que define parametros urbanisticos

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
        f"Leia o texto abaixo e responda se a {tipo} {numero}/{ano} de {municipio}/{estado} "
        f"define os parametros urbanisticos de ocupacao do solo no municipio.\n\n"
        f"TEXTO:\n{texto[:12000]}\n\n"
        "Responda APENAS com JSON (sem markdown):\n"
        "{\n"
        "  \"define_parametros\": true ou false,\n"
        "  \"define_zoneamento\": true ou false,\n"
        "  \"parametros_encontrados\": [\"coeficiente de aproveitamento\", \"taxa de ocupacao\", ...],\n"
        "  \"referencias\": [\"Art. 10\", \"Anexo I\", ...],\n"
        "  \"motivo\": \"A [tipo] n [numero]/[ano] [define / nao define] os parametros urbanisticos de ocupacao no municipio de [municipio], tais como [...].\"\"\n"
        "}\n\n"
        "Regras:\n"
        "- define_zoneamento = true se a lei divide o municipio em zonas, macrozonas ou setores urbanisticos.\n"
        "- parametros_encontrados: liste APENAS os que aparecem claramente no trecho acima. Se nenhum, use [].\n"
        "- referencias: liste APENAS artigos ou anexos vistos no trecho acima. Se nenhum, use [].\n"
        "- No motivo negativo use: A [tipo] n [numero]/[ano] nao define os parametros urbanisticos de ocupacao no municipio de [municipio]."
    )
    resp = chamar_llm(prompt, logs, f"Verif {tipo} {numero}")
    if not resp:
        return False
    try:
        import re as _re2
        resp_c = _re2.sub(r"^```json\s*|\s*```$", "", resp.strip())
        dados = _json.loads(resp_c)
        motivo = dados.get("motivo", "")[:300]
        zoneamento = dados.get("define_zoneamento", False)
        parametros = dados.get("parametros_encontrados", [])
        referencias = dados.get("referencias", [])
        if dados.get("define_parametros"):
            logs.append({"nivel": "ok", "msg": f"  {motivo}"})
            if zoneamento:
                logs.append({"nivel": "ok", "msg": f"  Define tambem o zoneamento do municipio."})
            else:
                logs.append({"nivel": "info", "msg": f"  Nao define o zoneamento do municipio."})
            if parametros:
                logs.append({"nivel": "info", "msg": f"  Parametros: {', '.join(parametros[:10])}"})
            if referencias:
                logs.append({"nivel": "info", "msg": f"  Referencias: {', '.join(referencias[:10])}"})
            return True
        else:
            logs.append({"nivel": "info", "msg": f"  {motivo}"})
            return False
    except:
        return False


def _buscar_plano_diretor_lm(municipio, estado, logs, chamar_llm, analisadas):
    """Fallback: busca Plano Diretor no LeisMunicipais por palavra-chave quando Gemini nao retorna numero."""
    try:
        from modulos.navegador_universal import navegar_com_cookies_flaresolverr
        import requests as _rfs, os as _ofs
        try:
            _old = _ofs.environ.get("FLARESOLVERR_SESSION", "")
            _rn = _rfs.post("http://localhost:8191/v1", json={"cmd": "sessions.create"}, timeout=10)
            _ns = _rn.json().get("session", "")
            if _ns:
                _ofs.environ["FLARESOLVERR_SESSION"] = _ns
                import subprocess as _sp
                _sp.run(["sed", "-i", f"s/FLARESOLVERR_SESSION=.*/FLARESOLVERR_SESSION={_ns}/", "/var/www/urbanlex/.env"], capture_output=True)
                logs.append({"nivel": "info", "msg": f"FlareSolverr sessao renovada: {_ns[:8]}..."})
        except Exception as _ef:
            logs.append({"nivel": "aviso", "msg": f"FlareSolverr renovacao falhou: {str(_ef)[:60]}"})

        leg_dict = {
            "tipo": "Plano Diretor",
            "numero": "",
            "ano": "",
            "municipio": municipio,
            "estado": estado,
            "_palavra_chave": "plano diretor",
            "_fallback_palavra_chave": True
        }
        logs.append({"nivel": "info", "msg": f"  Buscando Plano Diretor de {municipio}/{estado} no LeisMunicipais por palavra-chave..."})
        fs_result = navegar_com_cookies_flaresolverr("https://leismunicipais.com.br", leg_dict, logs, label=f"LM PD {municipio}", chamar_llm=chamar_llm)

        if fs_result.get("site_fora_do_ar"):
            logs.append({"nivel": "erro", "msg": "  LeisMunicipais esta fora do ar — encerrando busca"})
            return None
        if fs_result.get("municipio_nao_encontrado"):
            logs.append({"nivel": "aviso", "msg": f"  Municipio '{municipio}' nao consta no LeisMunicipais"})
            return None
        if fs_result.get("palavra_chave_nao_encontrada"):
            logs.append({"nivel": "aviso", "msg": f"  Termo 'plano diretor' nao encontrado no LeisMunicipais para {municipio}"})
            return None
        if fs_result.get("encontrada") and fs_result.get("url"):
            url_enc = fs_result["url"]
            if url_enc.lower() in analisadas:
                return None
            analisadas.add(url_enc.lower())
            logs.append({"nivel": "ok", "msg": f"  LeisMunicipais: encontrada! {url_enc[:80]}"})
            html_lei = fs_result.get("html", "")
            if html_lei:
                from bs4 import BeautifulSoup as _bs
                texto_lei = _bs(html_lei, "html.parser").get_text()[:12000]
                # Extrair tipo/numero/ano da URL ou do HTML
                import re as _re
                m = _re.search(r"/([a-z-]+)/(\d{4})/\d+/(\d+)/", url_enc)
                tipo_enc = m.group(1).replace("-", " ").title() if m else "Legislacao"
                numero_enc = m.group(3) if m else "?"
                ano_enc = m.group(2) if m else "?"
                define = _verificar_parametros(texto_lei, municipio, estado, tipo_enc, numero_enc, ano_enc, logs, chamar_llm)
                if not define:
                    logs.append({"nivel": "aviso", "msg": "  IA: legislacao nao define parametros urbanisticos — descartando"})
                    return None
            return {"tipo": tipo_enc if 'tipo_enc' in dir() else "Legislacao", "numero": numero_enc if 'numero_enc' in dir() else "?", "ano": ano_enc if 'ano_enc' in dir() else "?", "link": url_enc}
        logs.append({"nivel": "info", "msg": f"  LeisMunicipais: Plano Diretor nao encontrado para {municipio}"})
    except Exception as e:
        logs.append({"nivel": "aviso", "msg": f"  Erro busca palavra-chave LM: {str(e)[:80]}"})
    return None

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
        if fs_result.get("site_fora_do_ar"):
            logs.append({"nivel": "erro", "msg": "  LeisMunicipais esta fora do ar — encerrando busca"})
            return None
        if fs_result.get("encontrada") and fs_result.get("url"):
            url_enc = fs_result["url"]
            if url_enc.lower() in analisadas:
                return None
            analisadas.add(url_enc.lower())
            logs.append({"nivel": "ok", "msg": f"  LeisMunicipais: encontrada! {url_enc[:80]}"})
            # Verificar se define parametros urbanisticos
            html_lei = fs_result.get("html", "")
            if html_lei:
                from bs4 import BeautifulSoup as _bs
                texto_lei = _bs(html_lei, "html.parser").get_text()[:8000]
                define = _verificar_parametros(texto_lei, municipio, estado, tipo, numero, ano, logs, chamar_llm)
                if not define:
                    logs.append({"nivel": "aviso", "msg": "  IA: legislacao nao define parametros urbanisticos — descartando"})
                    return None
            return {"tipo": tipo, "numero": numero, "ano": ano, "link": url_enc}
        logs.append({"nivel": "aviso", "msg": f"  {tipo} {numero}/{ano} nao encontrada no LeisMunicipais — buscando site da prefeitura..."})
        return _buscar_site_prefeitura(municipio, estado, tipo, numero, ano, logs, chamar_llm, analisadas)
    except Exception as e:
        logs.append({"nivel": "aviso", "msg": f"  Erro LeisMunicipais: {str(e)[:80]}"})
    return None

def _buscar_site_prefeitura(municipio, estado, tipo, numero, ano, logs, chamar_llm, analisadas):
    """Fallback: busca legislacao no site oficial da prefeitura via Google."""
    try:
        import urllib.parse
        # Passo 1: buscar site da prefeitura no Google
        query = urllib.parse.quote_plus(f"prefeitura {municipio} {estado}")
        url_g = f"https://www.google.com/search?q={query}&num=10"
        headers = {"User-Agent": "Mozilla/5.0"}
        logs.append({"nivel": "info", "msg": f"  Google: buscando site da prefeitura de {municipio}/{estado}..."})
        r = requests.get(url_g, headers=headers, timeout=15)
        # Extrair links dos resultados
        import re as _re
        links = _re.findall(r'href="(https?://(?!google)[^"&]+)"', r.text)
        # Filtrar links relevantes — ignorar redes sociais e agregadores
        ignorar = ["facebook.com", "twitter.com", "instagram.com", "wikipedia.org",
                   "youtube.com", "linkedin.com", "tiktok.com", "leismunicipais.com.br",
                   "jusbrasil.com", "camara.leg.br", "google.com"]
        links_filtrados = [l for l in links if not any(ig in l for ig in ignorar)][:5]
        if not links_filtrados:
            logs.append({"nivel": "aviso", "msg": f"  Google nao retornou site da prefeitura de {municipio}"})
            return None
        logs.append({"nivel": "info", "msg": f"  Sites candidatos: {', '.join(l[:60] for l in links_filtrados[:3])}"})
        # Passo 2: IA navega no site da prefeitura buscando a legislacao
        from modulos.navegador_universal import navegar_com_cookies_flaresolverr
        for url_pref in links_filtrados[:3]:
            if url_pref.lower() in analisadas:
                continue
            analisadas.add(url_pref.lower())
            logs.append({"nivel": "info", "msg": f"  Tentando site da prefeitura: {url_pref[:80]}"})
            leg_dict = {
                "tipo": tipo,
                "numero": numero,
                "ano": ano,
                "municipio": municipio,
                "estado": estado,
                "_site_prefeitura": True
            }
            try:
                import requests as _rfs, os as _ofs
                _rn = _rfs.post("http://localhost:8191/v1", json={"cmd": "sessions.create"}, timeout=10)
                _ns = _rn.json().get("session", "")
                if _ns:
                    _ofs.environ["FLARESOLVERR_SESSION"] = _ns
                    import subprocess as _sp
                    _sp.run(["sed", "-i", f"s/FLARESOLVERR_SESSION=.*/FLARESOLVERR_SESSION={_ns}/", "/var/www/urbanlex/.env"], capture_output=True)
            except Exception:
                pass
            fs_result = navegar_com_cookies_flaresolverr(
                url_pref, leg_dict, logs,
                label=f"Pref {municipio}",
                chamar_llm=chamar_llm,
                max_passos=15
            )
            if fs_result.get("encontrada") and fs_result.get("url"):
                url_enc = fs_result["url"]
                logs.append({"nivel": "ok", "msg": f"  Prefeitura: encontrada! {url_enc[:80]}"})
                html_lei = fs_result.get("html", "")
                if html_lei:
                    from bs4 import BeautifulSoup as _bs
                    texto_lei = _bs(html_lei, "html.parser").get_text()[:12000]
                    define = _verificar_parametros(texto_lei, municipio, estado, tipo, numero, ano, logs, chamar_llm)
                    if not define:
                        logs.append({"nivel": "aviso", "msg": "  IA: legislacao nao define parametros urbanisticos — descartando"})
                        continue
                return {"tipo": tipo, "numero": numero, "ano": ano, "link": url_enc}
        logs.append({"nivel": "aviso", "msg": f"  Legislacao nao encontrada no site da prefeitura de {municipio}"})
    except Exception as e:
        logs.append({"nivel": "aviso", "msg": f"  Erro busca prefeitura: {str(e)[:80]}"})
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
