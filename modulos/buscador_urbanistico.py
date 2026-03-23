"""
Módulo de busca automática de legislações urbanísticas por município.
"""
import re
import requests
import json as _json

def buscar_legislacoes_urbanisticas(municipio, estado, logs, chamar_llm):
    resultado = {"encontradas": [], "nao_encontrada": False}
    analisadas = set()

    # ETAPA 1: Buscar na web e usar IA para identificar legislacoes
    logs.append({"nivel": "ok", "msg": f"Buscando na web: parametros urbanisticos de {municipio}/{estado}..."})
    conteudo_web = ""
    query = f"qual legislacao define os parametros urbanisticos de {municipio} {estado}"
    
    # Tentar Google primeiro
    try:
        import urllib.parse
        q = urllib.parse.quote_plus(query)
        url_g = f"https://www.google.com/search?q={q}&num=5&hl=pt-BR"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
        r_g = requests.get(url_g, headers=headers, timeout=10)
        if r_g.status_code == 200 and len(r_g.text) > 500:
            from bs4 import BeautifulSoup
            soup_g = BeautifulSoup(r_g.text, "html.parser")
            # Extrair snippets dos resultados
            for div in soup_g.find_all("div", class_=["BNeawe", "s3v9rd", "VwiC3b"])[:10]:
                txt = div.get_text()
                if len(txt) > 30:
                    conteudo_web += txt + "\n"
            logs.append({"nivel": "ok", "msg": f"Google: {len(conteudo_web)} chars de resultados"})
    except Exception as e:
        logs.append({"nivel": "aviso", "msg": f"Google falhou: {str(e)[:60]} — tentando DuckDuckGo..."})
    
    # Fallback DuckDuckGo
    if not conteudo_web:
        try:
            from modulos.buscador_legislacoes import _pesquisar_web
            resultados_ddg = _pesquisar_web(query, logs, "DDG urbanistico", max_results=5)
            if resultados_ddg:
                for res in resultados_ddg:
                    conteudo_web += f"{res.get('title', '')}\n{res.get('body', '')}\n\n"
                logs.append({"nivel": "ok", "msg": f"DuckDuckGo: {len(resultados_ddg)} resultado(s)"})
        except Exception as e:
            logs.append({"nivel": "aviso", "msg": f"DuckDuckGo falhou: {str(e)[:60]}"})
    
    if not conteudo_web:
        logs.append({"nivel": "aviso", "msg": "Nenhum resultado web — usando conhecimento da IA"})
        conteudo_web = f"Municipio: {municipio}, Estado: {estado}"
    
    # IA analisa resultado da web
    prompt_ia = (
        f"Com base nos resultados de busca abaixo, identifique qual legislacao define os parametros urbanisticos de {municipio}, {estado}.\n"
        f"Use APENAS informacoes presentes nos resultados — nao invente numeros.\n"
        f"Se nao encontrar numero especifico, deixe em branco.\n\n"
        f"RESULTADOS:\n{conteudo_web[:3000]}\n\n"
        'Responda APENAS com JSON: {"legislacoes": [{"tipo": "Lei Complementar", "numero": "148", "ano": "2023", "descricao": "Plano Diretor"}]}'
    )
    resp = chamar_llm(prompt_ia, logs, "IA urbanistico")
    if not resp:
        logs.append({"nivel": "aviso", "msg": "IA nao respondeu"})
        resultado["nao_encontrada"] = True
        return resultado
    try:
        import re as _re
        resp_c = _re.sub(r"^```json\s*|\s*```$", "", resp.strip())
        legs = _json.loads(resp_c).get("legislacoes", [])
        logs.append({"nivel": "ok", "msg": f"IA identificou {len(legs)} legislacao(oes)"})
    except Exception as e:
        logs.append({"nivel": "aviso", "msg": f"Erro ao parsear IA: {str(e)[:80]}"})
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
        import unicodedata
        def slugify(s):
            s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
            return s.lower().replace(" ", "-")
        est = estado.lower()
        mun = slugify(municipio)
        tipo_map = {"lei complementar": "lei-complementar", "lei ordinaria": "lei", "lei": "lei", "decreto": "decreto", "resolucao": "resolucao", "resolucao": "resolucao"}
        tipo_norm = tipo.lower().replace("á","a").replace("ã","a").replace("ó","o").replace("ç","c").replace("é","e")
        tipo_slug = tipo_map.get(tipo_norm, slugify(tipo))
        ano2 = str(ano)[:2] if ano else "00"
        url = f"https://leismunicipais.com.br/a/{est}/{mun[0]}/{mun}/{tipo_slug}/{ano2}/{numero}"
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code == 200 and len(r.text) > 1000:
            from bs4 import BeautifulSoup
            texto = BeautifulSoup(r.text, "html.parser").get_text()
            if _verificar_parametros(texto, municipio, estado, tipo, numero, ano, logs, chamar_llm):
                return {"tipo": tipo, "numero": numero, "ano": ano, "link": url}
    except Exception as e:
        logs.append({"nivel": "aviso", "msg": f"  Erro LeisMunicipais: {str(e)[:60]}"})
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
