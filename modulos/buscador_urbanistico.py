"""
Módulo de busca automática de legislações urbanísticas por município.
"""
import re
import requests
import json as _json

def _brt_now():
    from datetime import datetime, timezone, timedelta
    return datetime.now(timezone(timedelta(hours=-3))).strftime('%H:%M:%S')

def _tabela_evento(logs, municipio, estado, tipo, numero, ano, pergunta="", status="analisando",
                   altera=None, alterado_por=None, revoga=None, revogado_por=None,
                   cita=None, citado_em=None, link=None,
                   revoga_parcialmente=None, revogado_parcialmente_por=None, ementa=None):
    """Emite evento estruturado para atualizar a tabela de legislacoes em tempo real."""
    import json as _j
    dados = {
        "municipio": municipio, "estado": estado,
        "tipo": tipo, "numero": numero, "ano": ano,
        "pergunta": pergunta, "status": status,
        "altera": altera or [], "alterado_por": alterado_por or [],
        "revoga": revoga or [], "revogado_por": revogado_por or [],
        "revoga_parcialmente": revoga_parcialmente or [],
        "revogado_parcialmente_por": revogado_parcialmente_por or [],
        "cita": cita or [], "citado_em": citado_em or [],
        "link": link or "",
        "ementa": ementa or ""
    }
    logs.append({"nivel": "tabela", "msg": _j.dumps(dados, ensure_ascii=False)})

def buscar_legislacoes_urbanisticas(municipio, estado, logs, chamar_llm, fallback_url=None, max_legislacoes=None):
    resultado = {"encontradas": [], "nao_encontrada": False}
    _falhas_municipio = 0  # Contador de legislacoes nao encontradas em nenhum fallback
    _browser_mun = None  # Browser Playwright compartilhado por municipio
    _ctx_mun = None
    _page_mun = None
    _pw_mun = None
    _lm_nao_catalogado = False  # Municipio confirmado ausente no LeisMunicipais
    _lm_ja_verificado = False   # Verificacao de catalogo ja realizada para este municipio
    _fonte_funcionou = None      # Fonte que encontrou a 1a lei: lm/fallback1/fallback2
    # Carregar cache do banco
    try:
        from app import get_db as _gdb_cache
        _conn_cache = _gdb_cache()
        _cur_cache = _conn_cache.cursor()
        _cur_cache.execute("SELECT lm_nao_catalogado, fonte_funcionou FROM municipio_fallback WHERE LOWER(municipio)=LOWER(%s) AND LOWER(estado)=LOWER(%s)", (municipio, estado))
        _row_cache = _cur_cache.fetchone()
        _cur_cache.close(); _conn_cache.close()
        if _row_cache:
            if _row_cache[0]: _lm_nao_catalogado = True; _lm_ja_verificado = True
            if _row_cache[1]: _fonte_funcionou = _row_cache[1]
            if _lm_nao_catalogado:
                logs.append({"nivel": "info", "msg": f"  [CACHE] {municipio}/{estado} nao catalogado no LM (cache) — pulando diretamente para {_fonte_funcionou or 'fallback1'}"})
    except Exception as _ec:
        pass
    # Abrir browser compartilhado quando municipio nao catalogado e tem URL direta
    if _lm_nao_catalogado and fallback_url:
        try:
            from playwright.sync_api import sync_playwright as _swp_mun
            _pw_mun = _swp_mun().__enter__()
            _browser_mun = _pw_mun.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
            _ctx_mun = _browser_mun.new_context(viewport={"width": 1280, "height": 800}, user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36")
            _page_mun = _ctx_mun.new_page()
            logs.append({"nivel": "info", "msg": f"  [BROWSER] Browser compartilhado aberto para {municipio}/{estado}"})
        except Exception as _ebm:
            logs.append({"nivel": "aviso", "msg": f"  [BROWSER] Falha ao abrir browser compartilhado: {str(_ebm)[:60]}"})
        pass
    analisadas = set()
    # Resetar contador global de tokens
    try:
        from modulos.buscador_legislacoes import reset_token_stats as _reset_ts, get_token_stats as _get_ts
        _reset_ts()
    except Exception:
        _reset_ts = None
        _get_ts = None
    _token_stats = {'input': 0, 'output': 0}

    # ETAPA 1: 3 perguntas ao Gemini para identificar legislacoes
    PERGUNTAS = [
        f"quais sao as principais legislacoes vigentes que definem parametros urbanisticos de {municipio} {estado}? Inclua leis complementares, decretos regulamentadores e legislacoes especificas por zona. Liste no maximo 7 legislacoes, priorizando as mais recentes e abrangentes. IMPORTANTE: Liste APENAS legislacoes MUNICIPAIS de {municipio}. NAO inclua leis federais nem estaduais.",
        f"quais sao as principais legislacoes vigentes de zoneamento e macrozoneamento de {municipio} {estado}? Inclua leis complementares, planos diretores e decretos relacionados. Liste no maximo 7 legislacoes, priorizando as mais recentes e abrangentes. IMPORTANTE: Liste APENAS legislacoes MUNICIPAIS de {municipio}. NAO inclua leis federais nem estaduais.",
        f"quais sao as principais legislacoes vigentes de uso e ocupacao do solo de {municipio} {estado}? Inclua alteracoes parciais relevantes ainda em vigor. Liste no maximo 7 legislacoes, priorizando as mais recentes e abrangentes. IMPORTANTE: Liste APENAS legislacoes MUNICIPAIS de {municipio}. NAO inclua leis federais nem estaduais.",
        f"quais sao as principais legislacoes vigentes de parcelamento do solo urbano de {municipio} {estado}? Inclua leis de loteamento, desmembramento e condominio. Liste no maximo 7 legislacoes, priorizando as mais recentes e abrangentes. IMPORTANTE: Liste APENAS legislacoes MUNICIPAIS de {municipio}. NAO inclua leis federais nem estaduais.",
        f"quais sao as principais legislacoes vigentes que compoem o codigo de obras e edificacoes de {municipio} {estado}? Inclua decretos regulamentadores relevantes. Liste no maximo 7 legislacoes, priorizando as mais recentes e abrangentes. IMPORTANTE: Liste APENAS legislacoes MUNICIPAIS de {municipio}. NAO inclua leis federais nem estaduais.",
        f"qual e o Plano Diretor vigente de {municipio} {estado}? Inclua o nome oficial, numero e ano. Houve revisoes ou atualizacoes recentes? IMPORTANTE: Liste APENAS legislacoes MUNICIPAIS de {municipio}. NAO inclua leis federais nem estaduais.",
        f"Existem Operacoes Urbanas Consorciadas (OUC) em vigor em {municipio}/{estado}? Se sim, liste as legislacoes municipais que as criaram ou regulamentaram, incluindo numero e ano. IMPORTANTE: Liste APENAS se tiver certeza absoluta da existencia da OUC e do numero da legislacao — NAO invente nem suponha. Se nao houver OUC confirmada, retorne lista vazia.",
    ]
    legs = []
    _chaves_legs = set()

    def _gemini_pergunta(pergunta):
        _legs = []
        logs.append({"nivel": "ok", "msg": f"Consultando Gemini com busca web sobre {municipio}/{estado}..."})
        logs.append({"nivel": "info", "msg": f"Pergunta: {pergunta}"})
        try:
            from google import genai as _genai_new
            from google.genai import types as _types_new
            import os as _os2, time as _t503
            GEMINI_KEY = _os2.environ.get("GEMINI_API_KEY", "")
            client = _genai_new.Client(api_key=GEMINI_KEY)
            google_search_tool = _types_new.Tool(google_search=_types_new.GoogleSearch())
            config = _types_new.GenerateContentConfig(tools=[google_search_tool])
            import concurrent.futures as _cf
            response = None
            for _retry in range(3):
                try:
                    with _cf.ThreadPoolExecutor() as _ex:
                        _fut = _ex.submit(client.models.generate_content, model="gemini-2.5-flash", contents=pergunta, config=config)
                        response = _fut.result(timeout=60)
                    break
                except Exception as _e503:
                    _emsg = str(_e503)
                    _is_overload = '503' in _emsg or 'overload' in _emsg.lower() or 'unavailable' in _emsg.lower() or 'high demand' in _emsg.lower()
                    if _is_overload and _retry < 2:
                        _wait = (_retry + 1) * 15
                        logs.append({"nivel": "aviso", "msg": f"Gemini 503 — aguardando {_wait}s (retry {_retry+1}/2)..."})
                        _t503.sleep(_wait)
                    else:
                        raise
            # Contar tokens
            if hasattr(response, 'usage_metadata') and response.usage_metadata:
                _token_stats['input'] += getattr(response.usage_metadata, 'prompt_token_count', 0) or 0
                _token_stats['output'] += getattr(response.usage_metadata, 'candidates_token_count', 0) or 0
                _ci = _token_stats['input'] / 1_000_000 * 0.30
                _co = _token_stats['output'] / 1_000_000 * 2.50
                logs.append({"nivel": "token", "input": _token_stats['input'], "output": _token_stats['output'], "custo": _ci + _co})
            resp_texto = (response.text or "").strip()
            if not resp_texto:
                raise ValueError("Gemini retornou texto vazio")
            logs.append({"nivel": "ok", "msg": f"Gemini respondeu ({len(resp_texto)} chars)"})
            prompt_e = (
                f"Com base na resposta abaixo, extraia as legislacoes mencionadas de {municipio}/{estado}.\n"
                f"IMPORTANTE: Inclua APENAS legislacoes do municipio de {municipio}/{estado}. IGNORE qualquer legislacao de outros municipios ou estados.\n"
            f"Use APENAS informacoes presentes — nao invente numeros.\n"
                f"Considere APENAS atos do tipo: Lei, Lei Complementar, Decreto, Decreto-Lei, Resolucao. Ignore Portaria, Instrucao Normativa, Edital, Aviso ou qualquer outro tipo.\n\n"
                f"RESPOSTA:\n{resp_texto}\n\n"
                "Responda APENAS com JSON: {\"legislacoes\": [{\"tipo\": \"Lei Complementar\", \"numero\": \"148\", \"ano\": \"2023\", \"descricao\": \"Plano Diretor\"}]}"
            )
            resp2 = chamar_llm(prompt_e, logs, "IA estruturar")
            if resp2:
                import re as _re
                resp_c = _re.sub(r"^```json\s*|\s*```$", "", (resp2 or "").strip())
                _legs = _json.loads(resp_c).get("legislacoes", [])
                logs.append({"nivel": "ok", "msg": f"IA identificou {len(_legs)} legislacao(oes)"})
        except Exception as e:
            logs.append({"nivel": "aviso", "msg": f"Gemini falhou: {str(e)[:100]} — usando Google CSE"})
            try:
                import requests as _req_cse, os as _os_cse
                _cse_key = _os_cse.environ.get("GOOGLE_CSE_KEY", "")
                _cse_cx = _os_cse.environ.get("GOOGLE_CSE_CX", "")
                _resultados_cse = []
                if _cse_key and _cse_cx:
                    _cse_url = f"https://www.googleapis.com/customsearch/v1?key={_cse_key}&cx={_cse_cx}&q={_req_cse.utils.quote(pergunta)}&num=5&lr=lang_pt"
                    _cse_resp = _req_cse.get(_cse_url, timeout=10)
                    if _cse_resp.status_code == 200:
                        _cse_data = _cse_resp.json()
                        for _item in _cse_data.get("items", []):
                            _resultados_cse.append({
                                "title": _item.get("title", ""),
                                "url": _item.get("link", ""),
                                "body": _item.get("snippet", "")
                            })
                        logs.append({"nivel": "ok", "msg": f"Google CSE: {len(_resultados_cse)} resultado(s) encontrados"})
                    else:
                        logs.append({"nivel": "aviso", "msg": f"Google CSE erro: {_cse_resp.status_code}"})
                if not _resultados_cse:
                    from modulos.buscador_legislacoes import _pesquisar_web
                    _resultados_cse = _pesquisar_web(pergunta, logs, "DDG", max_results=5) or []
                    logs.append({"nivel": "info", "msg": f"Fallback DDG: {len(_resultados_cse)} resultado(s)"})
                conteudo_cse = ""
                _mun_norm = municipio.lower().replace(" ","")
                for res in _resultados_cse:
                    _title = res.get("title","")
                    _url = res.get("url","").lower()
                    _check = (_title + " " + _url).lower().replace("-"," ").replace("_"," ")
                    if _mun_norm not in _check.replace(" ","") and municipio.lower() not in _check:
                        logs.append({"nivel": "info", "msg": f"  [CSE] Ignorando resultado de outro municipio: {_title[:50]}"})
                        continue
                    conteudo_cse += f"{_title}\n{res.get('body','')}\n\n"
                if conteudo_cse:
                    prompt_cse = (
                        f"Identifique legislacoes de {municipio}/{estado} nos resultados.\n"
                        f"Nao invente numeros.\n\nRESULTADOS:\n{conteudo_cse[:3000]}\n\n"
                        "Responda APENAS com JSON: {\"legislacoes\": [{\"tipo\": \"\", \"numero\": \"\", \"ano\": \"\", \"descricao\": \"\"}]}"
                    )
                    resp_cse = chamar_llm(prompt_cse, logs, "IA CSE")
                    if resp_cse:
                        import re as _re2
                        resp_c2 = _re2.sub(r"^```json\s*|\s*```$", "", (resp_cse or "").strip())
                        _legs = _json.loads(resp_c2).get("legislacoes", [])
            except Exception as e2:
                logs.append({"nivel": "aviso", "msg": f"DDG falhou: {str(e2)[:60]}"})
        return _legs

    for idx, pergunta in enumerate(PERGUNTAS, 1):
        logs.append({"nivel": "ok", "msg": f"--- Pergunta {idx}/{len(PERGUNTAS)} ---"})
        _modo_pergunta = "parametros" if idx == 1 else "geral"
        for leg in _gemini_pergunta(pergunta):
            numero = leg.get("numero", "").strip()
            if not numero:
                continue
            _num_norm = numero.replace('.','').replace('-','').strip()
            chave = f"{leg.get('tipo','').lower()}_{_num_norm}_{leg.get('ano','')}".lower()
            if chave not in _chaves_legs:
                _chaves_legs.add(chave)
                leg["_modo_verificacao"] = _modo_pergunta
                legs.append(leg)
                logs.append({"nivel": "info", "msg": f"  Nova legislacao: {leg.get('tipo')} {numero}/{leg.get('ano','?')}"})
            else:
                logs.append({"nivel": "info", "msg": f"  Duplicata ignorada: {leg.get('tipo')} {numero}/{leg.get('ano','?')}"})

    logs.append({"nivel": "ok", "msg": f"Total legislacoes unicas: {len(legs)}"})
    conteudo_web = ""

    # Filtrar legs sem numero — acionar fallback por palavra-chave
    # REGRA 1: Ignorar legislacoes federais e estaduais — manter apenas municipais
    def _is_federal_ou_estadual(leg):
        tipo = leg.get("tipo", "").lower()
        desc = leg.get("descricao", "").lower()
        esfera = leg.get("esfera", "").lower()
        palavras_fed_est = ["federal", "estadual", "estado", "constituicao", "codigo civil",
                            "codigo penal", "codigo tributario nacional", "lei organica do estado"]
        if esfera in ["federal", "estadual"]:
            return True
        if any(p in tipo for p in palavras_fed_est):
            return True
        if any(p in desc for p in palavras_fed_est):
            return True
        return False

    legs_municipais = []
    for _l in legs:
        if _is_federal_ou_estadual(_l):
            logs.append({"nivel": "aviso", "msg": f"  Ignorando legislacao federal/estadual: {_l.get('tipo','')} {_l.get('numero','')}/{_l.get('ano','')} — fora do escopo municipal"})
        else:
            legs_municipais.append(_l)
    legs = legs_municipais

    # Filtrar apenas tipos aceitos
    _TIPOS_ACEITOS = {"lei", "lei complementar", "decreto", "decreto-lei", "resolucao", "resolução"}
    legs_tipo_ok = []
    for _l in legs:
        _t = _l.get("tipo", "").strip().lower()
        if any(_t == _ta or _t.startswith(_ta) for _ta in _TIPOS_ACEITOS):
            legs_tipo_ok.append(_l)
        else:
            logs.append({"nivel": "aviso", "msg": f"  Ignorando tipo nao aceito: {_l.get('tipo','')} {_l.get('numero','')}/{_l.get('ano','')} - fora do escopo"})
    legs = legs_tipo_ok

    legs_com_numero = [l for l in legs if l.get("numero", "").strip()]

    if not legs_com_numero:
        if not legs:
            logs.append({"nivel": "aviso", "msg": "Gemini nao identificou legislacao — tentando busca por Plano Diretor no LeisMunicipais..."})
        else:
            logs.append({"nivel": "aviso", "msg": "Gemini identificou legislacao mas sem numero — tentando busca por Plano Diretor no LeisMunicipais..."})
        enc = _buscar_plano_diretor_lm(municipio, estado, logs, chamar_llm, analisadas)
        if enc:
            resultado["encontradas"].append(enc)

            if max_legislacoes and len(resultado["encontradas"]) >= max_legislacoes:
                logs.append({"nivel": "ok", "msg": f"  Limite de {max_legislacoes} legislacoes encontradas atingido — encerrando busca"})
                return resultado
        else:
            resultado["nao_encontrada"] = True
    # Fechar browser compartilhado do municipio
    if _browser_mun:
        try:
            _browser_mun.close()
            if _pw_mun: _pw_mun.__exit__(None, None, None)
            logs.append({"nivel": "info", "msg": "  [BROWSER] Browser compartilhado fechado"})
        except Exception: pass
        return resultado

    # Ordenar por ano mais recente primeiro, depois por numero
    def _sort_key(l):
        try: ano_k = int(l.get("ano", "0") or "0")
        except: ano_k = 0
        try: num_k = int(l.get("numero", "0") or "0")
        except: num_k = 0
        return (-ano_k, -num_k)
    legs = sorted(legs_com_numero, key=_sort_key)
    logs.append({"nivel": "ok", "msg": f"Legislacoes ordenadas por ano (mais recente primeiro): {', '.join(l.get('tipo','')+ ' ' + l.get('numero','') + '/' + l.get('ano','') for l in legs)}"})

    # Conjunto de legislacoes revogadas identificadas durante a busca
    revogadas = set()
    revogadas_lista = []  # lista enriquecida para verificacao por IA

    def _verificar_se_revogada_ia(tipo, numero, ano, rev_lista, logs, chamar_llm):
        if not rev_lista:
            return False, None
        lista_str = "; ".join(
            f"{r['tipo']} {r['numero']}/{r['ano']} (revogada por {r['revogada_por']})"
            for r in rev_lista
        )
        prompt = (
            "Verifique se a legislacao abaixo consta da lista de legislacoes revogadas.\n\n"
            f"LEGISLACAO A VERIFICAR: {tipo} {numero}/{ano}\n\n"
            f"LISTA DE REVOGADAS:\n{lista_str}\n\n"
            "Considere TODAS as formas de nomear a mesma lei:\n"
            "  Dec., Decreto, Decreto N, Decreto no; LC, L.C., Lei Comp., Lei Complementar;\n"
            "  LO, Lei, Lei Ordinaria; numeros com/sem zeros; com/sem ano\n\n"
            "Responda APENAS com JSON (sem markdown):\n"
            '{"esta_revogada": true, "revogada_por": "nome da lei"}'
            "\nou\n"
            '{"esta_revogada": false, "revogada_por": null}'
        )
        resp = chamar_llm(prompt, logs, f"Verif revogada {tipo} {numero}")
        if resp:
            try:
                import re as _re_rv
                resp_c = _re_rv.sub(r"^```json\s*|\s*```$", "", (resp or "").strip())
                _s_c = resp_c.find('{')
                _d_c = 0; _e_c = _s_c
                for _i_c, _ch_c in enumerate(resp_c[_s_c:], _s_c):
                    if _ch_c == '{': _d_c += 1
                    elif _ch_c == '}':
                        _d_c -= 1
                        if _d_c == 0: _e_c = _i_c + 1; break
                dados = _json.loads(resp_c[_s_c:_e_c] if _s_c >= 0 else resp_c)
                if dados.get("esta_revogada"):
                    return True, dados.get("revogada_por", "legislacao mais recente")
            except Exception:
                pass
        return False, None

    # ETAPA 2: Buscar no LeisMunicipais — fila dinamica com niveis de profundidade
    from collections import deque as _deque
    # Adicionar nivel 0 a todas as legs iniciais
    for _l in legs:
        if "_nivel" not in _l:
            _l["_nivel"] = 0
    # Chaves das leis de nivel 0 para deteccao de promocao de nivel
    nivel0_chaves = set()
    for _l0 in legs:
        _n0 = _l0.get("numero","").replace('.','').replace(' ','').strip().lstrip('0') or '0'
        nivel0_chaves.add(f"{_l0.get('tipo','').lower()}_{_n0}_{_l0.get('ano','')}")
    fila = _deque(legs)
    _descartadas_log = []  # rastrear leis descartadas por nao definir parametros
    while fila:
      leg = fila.popleft()
      if True:
        tipo = leg.get("tipo", "")
        numero = leg.get("numero", "")
        ano = leg.get("ano", "")
        descricao = leg.get("descricao", "")
        _num_norm = numero.replace('.','').replace(' ','').strip().lstrip('0') or '0'
        chave = f"{tipo}_{_num_norm}_{ano}".lower()
        # chave reservada em analisadas apenas apos verificacoes (linha abaixo)
        # 1. Verificar por chave exata
        if chave in revogadas:
            # Buscar quem revogou na lista enriquecida
            _rev_entry = next((r for r in revogadas_lista if f"{r.get('tipo','').lower()}_{(r.get('numero','').replace('.','').replace(' ','').strip().lstrip('0') or '0')}_{r.get('ano','')}" == chave), None)
            _rev_info = _rev_entry.get("revogada_por", "legislacao mais recente") if _rev_entry else "legislacao mais recente"
            logs.append({"nivel": "aviso", "msg": f"⚠️ REVOGADA — {tipo} {numero}/{ano} foi revogada por {_rev_info} e NAO sera analisada"})
            logs.append({"nivel": "aviso", "msg": f"   Motivo: legislacao mais recente ({_rev_info}) revoga explicitamente esta"})
            _tabela_evento(logs, municipio, estado, tipo, numero, ano, pergunta=leg.get("_pergunta_label",""), status="revogada", revogado_por=[_rev_info])
            continue
        # 2. Verificar via IA com variacoes de nomenclatura
        if revogadas_lista:
            logs.append({"nivel": "info", "msg": f"Verificando se {tipo} {numero}/{ano} consta da lista de revogadas ({len(revogadas_lista)} legislacoes)..."})
            _esta_rev, _rev_por = _verificar_se_revogada_ia(tipo, numero, ano, revogadas_lista, logs, chamar_llm)
            if _esta_rev:
                logs.append({"nivel": "aviso", "msg": f"⚠️ REVOGADA (IA confirmou) — {tipo} {numero}/{ano} consta da lista de revogadas"})
                logs.append({"nivel": "aviso", "msg": f"   Revogada por: {_rev_por} — NAO sera analisada"})
                _tabela_evento(logs, municipio, estado, tipo, numero, ano, pergunta=leg.get("_pergunta_label",""), status="revogada", revogado_por=[_rev_por or ""])
                revogadas.add(chave)
                continue
        if chave in analisadas:
            logs.append({"nivel": "info", "msg": f"Duplicata normalizada ignorada: {tipo} {numero}/{ano}"})
            continue
        analisadas.add(chave)
        # Filtrar leis federais/estaduais — nao buscaveis no LeisMunicipais
        if _is_federal_ou_estadual(leg):
            logs.append({"nivel": "aviso", "msg": f"  [FEDERAL/ESTADUAL] {tipo} {numero}/{ano} — fora do escopo municipal"})
            _tabela_evento(logs, municipio, estado, tipo, numero, ano, pergunta=leg.get("_pergunta_label",""), status="nao_encontrada")
            continue
        _nivel_leg=leg.get("_nivel",0);_desc_leg=leg.get("descricao","").lower();_via_cita="citada em contexto" in _desc_leg;_limite_nivel=1 if _via_cita else 3
        if _nivel_leg>_limite_nivel:
            logs.append({"nivel":"info","msg":f"  [FILA] {tipo} {numero}/{ano} nivel={_nivel_leg} limite={_limite_nivel}"})
            _tabela_evento(logs,municipio,estado,tipo,numero,ano,pergunta=leg.get("_pergunta_label",""),status="referenciada",link="")
            continue
        _pergunta_origem = leg.get("_pergunta_label", "")
        _tabela_evento(logs, municipio, estado, tipo, numero, ano, pergunta=_pergunta_origem, status="analisando")
        enc = None
        if _fonte_funcionou and _fonte_funcionou != "lm":
            # Ja sabemos qual fonte funciona — ir direto sem tentar LM
            logs.append({"nivel": "info", "msg": f"  [{_fonte_funcionou.upper()}] Fonte conhecida para {municipio} — buscando {tipo} {numero}/{ano} diretamente..."})
            if _fonte_funcionou == "fallback1":
                enc = _buscar_fallback1(municipio, estado, tipo, numero, ano, logs, chamar_llm, analisadas, url_direta=fallback_url, page_existente=_page_mun, ctx_existente=_ctx_mun)
            if enc:
                # Verificar sentinel de municipio nao catalogado
                if isinstance(enc, dict) and enc.get("_lm_indisponivel"):
                    _lm_nao_catalogado = True
                    _lm_ja_verificado = True
                    logs.append({"nivel": "aviso", "msg": f"  [FLAG] {municipio}/{estado} marcado como NAO catalogado no LM"})
                    if enc.get("_nao_encontrada"):
                        enc = None
                    else:
                        if not _fonte_funcionou:
                            _fonte_funcionou = enc.get("_fonte", "fallback1")
                elif not _fonte_funcionou:
                    _fonte_funcionou = enc.get("_fonte", "lm")
            else:
                if not _lm_ja_verificado:
                    _lm_ja_verificado = True
                    _catalogado = _verificar_catalogado_lm(municipio, estado, logs, chamar_llm)
                    if not _catalogado:
                        _lm_nao_catalogado = True
                        logs.append({"nivel": "aviso", "msg": f"  {municipio}/{estado} NAO catalogado no LeisMunicipais — proximas leis usarao fallbacks diretamente"})
                        # Salvar no banco
                        try:
                            from app import get_db as _gdb_save
                            _conn_s = _gdb_save(); _cur_s = _conn_s.cursor()
                            _cur_s.execute("INSERT INTO municipio_fallback (municipio, estado, lm_nao_catalogado) VALUES (%s,%s,TRUE) ON CONFLICT (municipio,estado) DO UPDATE SET lm_nao_catalogado=TRUE, atualizado_em=NOW()", (municipio, estado))
                            _conn_s.commit(); _cur_s.close(); _conn_s.close()
                        except Exception as _es: pass
                    if not _fonte_funcionou: _fonte_funcionou = "fallback2"
        else:
            logs.append({"nivel": "info", "msg": f"Buscando {tipo} n {numero}/{ano} ({descricao}) no LeisMunicipais..."})
            enc = _buscar_leismunicipais(municipio, estado, tipo, numero, ano, logs, chamar_llm, analisadas, modo=leg.get("_modo_verificacao","geral"), fallback_url=fallback_url, _nivel=leg.get("_nivel",1))
            if enc:
                if not _fonte_funcionou:
                    _fonte_funcionou = enc.get("_fonte", "lm")
            else:
                if not _lm_ja_verificado:
                    _lm_ja_verificado = True
                    _catalogado = _verificar_catalogado_lm(municipio, estado, logs, chamar_llm)
                    if not _catalogado:
                        _lm_nao_catalogado = True
                        logs.append({"nivel": "aviso", "msg": f"  {municipio}/{estado} NAO catalogado no LeisMunicipais — proximas leis usarao fallbacks diretamente"})
        if enc and not _fonte_funcionou:
            _fonte_funcionou = enc.get("_fonte", "lm") if isinstance(enc, dict) else "lm"
            # Salvar fonte no banco
            try:
                from app import get_db as _gdb_sf
                _conn_sf = _gdb_sf(); _cur_sf = _conn_sf.cursor()
                _cur_sf.execute("INSERT INTO municipio_fallback (municipio, estado, fonte_funcionou) VALUES (%s,%s,%s) ON CONFLICT (municipio,estado) DO UPDATE SET fonte_funcionou=EXCLUDED.fonte_funcionou, atualizado_em=NOW()", (municipio, estado, _fonte_funcionou))
                _conn_sf.commit(); _cur_sf.close(); _conn_sf.close()
            except Exception as _esf: pass
        if not enc:
            _tabela_evento(logs, municipio, estado, tipo, numero, ano, pergunta=_pergunta_origem, status="nao_encontrada")
            _falhas_municipio += 1
            if _falhas_municipio >= 5:
                logs.append({"nivel": "aviso", "msg": f"⚠️ 5 legislações não encontradas em nenhum fallback — encerrando busca de {municipio}/{estado}"})
                break
            continue
        texto_enc = ""
        _altera_enc = []
        _revoga_enc = []
        _regulamenta_enc = []
        _alterado_por_enc = []
        _revogado_por_enc = []
        _regulamentado_por_enc = []
        _cita_enc = []
        _revoga_parcialmente_enc = []
        _revogado_parcialmente_por_enc = []
        if enc:
            enc['_altera_enc'] = _altera_enc
            enc['_alterado_por_enc'] = _alterado_por_enc
            enc['_revoga_enc'] = _revoga_enc
            enc['_revogado_por_enc'] = _revogado_por_enc
            enc['_revoga_parcialmente_enc'] = _revoga_parcialmente_enc
            enc['_revogado_parcialmente_por_enc'] = _revogado_parcialmente_por_enc
            enc['_regulamenta_enc'] = _regulamenta_enc
            enc['_regulamentado_por_enc'] = _regulamentado_por_enc
            enc['_cita_enc'] = list(set(_regulamenta_enc + _cita_enc))
            resultado["encontradas"].append(enc)

            if max_legislacoes and len(resultado["encontradas"]) >= max_legislacoes:
                logs.append({"nivel": "ok", "msg": f"  Limite de {max_legislacoes} legislacoes encontradas atingido — encerrando busca"})
                break
            # Verificar via IA se esta legislacao revoga outras da lista
            html_enc = enc.get("html", "") or ""
            # Gerar PDF a partir do HTML se nao tiver pdf_path (ex: resultado do Fallback)
            if html_enc and not enc.get("pdf_path"):
                try:
                    import tempfile as _tf_enc, os as _os_enc, subprocess as _sp_enc, re as _re_enc
                    from bs4 import BeautifulSoup as _bsr_pdf
                    _slug_enc = _re_enc.sub(r"[^a-zA-Z0-9_]", "_", f"{estado}_{municipio}_{tipo}_{numero}_{ano}")[:60]
                    _pdf_enc_path = f"/var/www/urbanlex/static/downloads/{_slug_enc}_gerado.pdf"
                    _soup_enc = _bsr_pdf(html_enc, "html.parser")
                    _cont_enc = _soup_enc.find(class_="law-container") or _soup_enc.find(class_="law-content") or _soup_enc.find(class_="ato-content") or _soup_enc.find("body") or _soup_enc
                    _html_limpo_enc = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"><style>body{{font-family:Arial,sans-serif;font-size:11pt;margin:20mm;color:#111;}}p{{margin:4px 0;text-align:justify;line-height:1.5;}}</style></head><body>{str(_cont_enc)}</body></html>"""
                    with _tf_enc.NamedTemporaryFile(suffix=".html", mode="w", encoding="utf-8", delete=False) as _tmp_enc:
                        _tmp_enc.write(_html_limpo_enc)
                        _tmp_enc_path = _tmp_enc.name
                    _proc_enc = _sp_enc.Popen(["wkhtmltopdf","--encoding","utf-8","--quiet","--disable-javascript","--no-images","--load-error-handling","ignore", _tmp_enc_path, _pdf_enc_path])
                    try:
                        _proc_enc.wait(timeout=20)
                    except _sp_enc.TimeoutExpired:
                        _proc_enc.kill()
                        _proc_enc.wait()
                        raise Exception("wkhtmltopdf timeout")
                    _os_enc.unlink(_tmp_enc_path)
                    if _os_enc.path.exists(_pdf_enc_path) and _os_enc.path.getsize(_pdf_enc_path) > 1000:
                        enc["pdf_path"] = _pdf_enc_path
                        logs.append({"nivel": "info", "msg": f"  PDF gerado via wkhtmltopdf a partir do HTML: {_os_enc.path.basename(_pdf_enc_path)}"})
                except Exception as _e_enc:
                    logs.append({"nivel": "aviso", "msg": f"  wkhtmltopdf (HTML->PDF): {str(_e_enc)[:60]}"})
            if html_enc:
                from bs4 import BeautifulSoup as _bsr
                texto_enc = _bsr(html_enc, "html.parser").get_text()
            else:
                texto_enc = ""
            # Incluir texto dos anexos no texto_enc (ZIP/PDF/rasterizado)
            _anexos_enc = enc.get("anexos_lm") or []
            # Se nao tem anexos_lm mas tem pdf_path (ex: Fallback1/2), usar pdf_path como anexo
            if not _anexos_enc and enc.get("pdf_path"):
                import os as _os_anx
                _pdf_anx = enc.get("pdf_path", "")
                if _pdf_anx and _os_anx.path.exists(_pdf_anx):
                    _nome_anx = _os_anx.path.basename(_pdf_anx)
                    _anexos_enc = [{"path": _pdf_anx, "pdf_path": _pdf_anx, "nome": _nome_anx, "texto": ""}]
                    logs.append({"nivel": "anexo", "msg": f"  [ANEXOS] pdf_path adicionado como anexo: {_nome_anx}"})
            if _anexos_enc:
                logs.append({"nivel": "relacao", "msg": f"  [ANEXOS] {len(_anexos_enc)} anexo(s): {[str(a)[:100] for a in _anexos_enc]}"})
            def _extrair_texto_arquivo(path, logs, label="Anexo", chamar_llm_fn=None):
                import subprocess as _sp_ax, os as _os_ax
                if not path or not _os_ax.path.exists(path):
                    return ""
                ext = _os_ax.path.splitext(path)[1].lower()
                # ZIP: descompactar e processar cada arquivo
                if ext == '.zip':
                    import zipfile as _zf, tempfile as _tmp_ax
                    _txt_zip = ""
                    logs.append({"nivel": "anexo", "msg": f"  📦 Descompactando anexo ZIP: {label}"})
                    try:
                        with _zf.ZipFile(path, 'r') as _z:
                            _tmpdir = _tmp_ax.mkdtemp()
                            _z.extractall(_tmpdir)
                            _all_files = []
                            for _root, _dirs, _files in _os_ax.walk(_tmpdir):
                                for _fname in sorted(_files):
                                    _all_files.append((_fname, _os_ax.path.join(_root, _fname)))
                            logs.append({"nivel": "anexo", "msg": f"  📂 {len(_all_files)} arquivo(s) encontrado(s) no ZIP"})
                            for _fname, _fpath in _all_files:
                                logs.append({"nivel": "anexo", "msg": f"  📄 Analisando: {_fname}..."})
                                _ftxt = _extrair_texto_arquivo(_fpath, logs, _fname, chamar_llm_fn)
                                if _ftxt:
                                    # Pedir ao Gemini uma descricao rapida do assunto
                                    if chamar_llm_fn:
                                        try:
                                            _desc_prompt = f"Em uma linha, descreva o assunto deste documento municipal:\n\n{_ftxt[:2000]}"
                                            _desc = chamar_llm_fn(_desc_prompt, [], f"Desc {_fname}")
                                            if _desc:
                                                logs.append({"nivel": "anexo", "msg": f"  📋 {_fname}: {_desc.strip()[:150]}"})
                                        except Exception:
                                            pass
                                    _txt_zip += f"\n\n--- {_fname} ---\n{_ftxt}"
                    except Exception as _ez:
                        logs.append({"nivel": "aviso", "msg": f"  [ANEXOS] Erro ZIP {label}: {str(_ez)[:80]}"})
                    return _txt_zip
                # PDF: Gemini Vision sempre (mais confiavel para legislacao)
                try:
                    import base64 as _b64_ax
                    import subprocess as _sp_gs
                    import tempfile as _tmp_gs, os as _os_gs
                    _tmpdir_gs = _tmp_gs.mkdtemp()
                    _png_pattern = _os_gs.path.join(_tmpdir_gs, 'page')
                    _sp_gs.run(['gs', '-dNOPAUSE', '-dBATCH', '-sDEVICE=png16m', '-r150',
                                f'-sOutputFile={_png_pattern}_%03d.png', path],
                               capture_output=True, timeout=120)
                    _pages = sorted([f for f in _os_gs.listdir(_tmpdir_gs) if f.endswith('.png')])[:10]
                    if _pages:
                        from google import genai as _gai_ax
                        _model_ax = _gai_ax.Client(api_key=__import__('os').environ.get('GEMINI_API_KEY',''))
                        _prompt_vision = "Extraia todo o texto desta pagina de documento municipal brasileiro. Retorne apenas o texto, sem comentarios."
                        _img_parts = []
                        for _pg_name in _pages:
                            _pg_path = _os_gs.path.join(_tmpdir_gs, _pg_name)
                            with open(_pg_path, "rb") as _f_pg:
                                _img_parts.append({"inline_data": {"mime_type": "image/png", "data": _b64_ax.b64encode(_f_pg.read()).decode()}})
                        _contents = [{"role": "user", "parts": [{"text": _prompt_vision}] + _img_parts}]
                        _resp_ax = _model_ax.models.generate_content(model="gemini-2.5-flash", contents=_contents)
                        _txt_vision = _resp_ax.text or ""
                        if _txt_vision:
                            logs.append({"nivel": "info", "msg": f"  [ANEXOS] {label}: {len(_txt_vision)} chars via Gemini Vision"})
                            # Limpar tmpdir
                            try:
                                import shutil as _sh_ax
                                _sh_ax.rmtree(_tmpdir_gs, ignore_errors=True)
                            except Exception:
                                pass
                            return _txt_vision
                except Exception as _ev:
                    logs.append({"nivel": "aviso", "msg": f"  [ANEXOS] Gemini Vision erro {label}: {str(_ev)[:80]}"})
                # Fallback: pdftotext se Gemini Vision falhou
                try:
                    _res = _sp_ax.run(['pdftotext', path, '-'], capture_output=True, text=True, timeout=60)
                    if _res.returncode == 0 and len(_res.stdout.strip()) > 100:
                        logs.append({"nivel": "info", "msg": f"  [ANEXOS] {label}: {len(_res.stdout)} chars via pdftotext (fallback)"})
                        return _res.stdout
                except Exception:
                    pass
                return ""
