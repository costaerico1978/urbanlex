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
                enc = _buscar_fallback1(municipio, estado, tipo, numero, ano, logs, chamar_llm, analisadas)
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
                # PDF: tentar pdftotext primeiro
                if ext == '.pdf':
                    try:
                        _res = _sp_ax.run(['pdftotext', path, '-'], capture_output=True, text=True, timeout=60)
                        if _res.returncode == 0 and len(_res.stdout.strip()) > 100:
                            logs.append({"nivel": "info", "msg": f"  [ANEXOS] {label}: {len(_res.stdout)} chars via pdftotext"})
                            return _res.stdout
                    except Exception:
                        pass
                    # Fallback: Gemini Vision para PDF rasterizado
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
                                return _txt_vision
                    except Exception as _ev:
                        logs.append({"nivel": "aviso", "msg": f"  [ANEXOS] Gemini Vision erro {label}: {str(_ev)[:80]}"})
                return ""
            if _anexos_enc:
                logs.append({"nivel": "anexo", "msg": f"  🔍 Iniciando analise de {len(_anexos_enc)} anexo(s) da {tipo} {numero}/{ano}..."})
            for _anx_e in _anexos_enc:
                _anx_nome = _anx_e.get("nome", "Anexo") if isinstance(_anx_e, dict) else "Anexo"
                _anx_txt = _anx_e.get("texto", "") if isinstance(_anx_e, dict) else ""
                if not _anx_txt:
                    _anx_path_e = _anx_e.get("path", "") or _anx_e.get("pdf_path", "") if isinstance(_anx_e, dict) else ""
                    logs.append({"nivel": "anexo", "msg": f"  📄 Processando anexo: {_anx_nome}"})
                    _anx_txt = _extrair_texto_arquivo(_anx_path_e, logs, _anx_nome, chamar_llm)
                if _anx_txt:
                    texto_enc += f"\n\nANEXO ({_anx_nome}):\n{_anx_txt}"
                    logs.append({"nivel": "anexo", "msg": f"  ✅ Anexo {_anx_nome}: {len(_anx_txt)} chars extraidos"})
                else:
                    logs.append({"nivel": "aviso", "msg": f"  ⚠️ Anexo {_anx_nome}: nao foi possivel extrair texto"})
            if _anexos_enc:
                logs.append({"nivel": "anexo", "msg": f"  📊 Total apos anexos: {len(texto_enc)} chars para analise do Gemini"})
            # Fallback: usar texto do PDF se HTML vazio
            if not texto_enc.strip():
                _pdf_path = enc.get("pdf_path", "") or ""
                if _pdf_path and __import__('os').path.exists(_pdf_path):
                    try:
                        import subprocess as _sp
                        _res = _sp.run(['pdftotext', _pdf_path, '-'], capture_output=True, text=True, timeout=30)
                        if _res.returncode == 0 and _res.stdout.strip():
                            texto_enc = _res.stdout
                            logs.append({"nivel": "info", "msg": f"  Texto extraido do PDF: {len(texto_enc)} chars"})
                    except Exception:
                        pass
            # Montar lista das demais legislacoes para verificar revogacao
            outras_legs = [l for l in legs if f"{l.get('tipo','').lower()}_{l.get('numero','').replace('.','').replace(' ','').strip()}_{l.get('ano','')}" != chave and f"{l.get('tipo','').lower()}_{l.get('numero','').replace('.','').replace(' ','').strip()}_{l.get('ano','')}" not in revogadas and f"{l.get('tipo','').lower()}_{l.get('numero','').replace('.','').replace(' ','').strip()}_{l.get('ano','')}" not in analisadas]
            if outras_legs and texto_enc:
                lista_outras = ", ".join(f"{l.get('tipo','')} {l.get('numero','')}/{l.get('ano','')}" for l in outras_legs)
                prompt_rev = (
                    f"Analise o texto COMPLETO da {tipo} {numero}/{ano} de {municipio}/{estado} abaixo.\n"
                    f"Verifique se esta legislacao REVOGA TOTALMENTE alguma das seguintes: {lista_outras}.\n\n"
                    f"ATENCAO — distinga claramente:\n"
                    f"  - REVOGACAO TOTAL: a lei anterior perde completamente a vigencia (use 'revogadas')\n"
                    f"  - ALTERACAO PARCIAL: a lei nova altera apenas alguns artigos da anterior — a lei anterior CONTINUA VIGENTE no restante (NAO inclua em 'revogadas')\n"
                    f"  - Uma lei que 'altera', 'acrescenta' ou 'modifica artigos' de outra NAO a revoga — ambas continuam vigentes\n\n"
                    f"TEXTO:\n{texto_enc}\n\n"
                    "Responda APENAS com JSON:\n"
                    "{\n"
                    "  \"revogadas\": [\"tipo numero/ano\"],\n"
                    "  \"alteradas\": [{\"lei\": \"tipo numero/ano\", \"descricao\": \"descreva o que foi alterado\"}]\n"
                    "}\n"
                    "Se nenhuma lei for revogada totalmente, use revogadas: []."
                )
                resp_rev = chamar_llm(prompt_rev, logs, f"Verif revogacao {tipo} {numero}")
                if resp_rev:
                    try:
                        import re as _re_rev
                        resp_rev_c = _re_rev.sub(r"^```json\s*|\s*```$", "", (resp_rev or "").strip())
                        dados_rev = _json.loads(resp_rev_c)
                        revogadas_ia = dados_rev.get("revogadas", [])
                        alteradas_ia = dados_rev.get("alteradas", [])
                        for alt in alteradas_ia:
                            logs.append({"nivel": "ok", "msg": f"  ℹ️ ALTERACAO PARCIAL: {tipo} {numero}/{ano} altera {alt.get('lei','')}: {alt.get('descricao','')[:120]}"})
                            logs.append({"nivel": "ok", "msg": "     Ambas continuam vigentes e serao analisadas"})
                            _altera_enc.append(alt.get('lei',''))
                        for outra in outras_legs:
                            num_outra = outra.get("numero", "").strip()
                            _num_outra_n = num_outra.replace('.','').replace(' ','').strip()
                            chave_outra = f"{outra.get('tipo','').lower()}_{_num_outra_n}_{outra.get('ano','')}".lower()
                            for rev_str in revogadas_ia:
                                if num_outra and num_outra in rev_str:
                                    revogadas.add(chave_outra)
                                    revogadas_lista.append({"tipo": outra.get("tipo",""), "numero": num_outra, "ano": outra.get("ano",""), "revogada_por": f"{tipo} {numero}/{ano}"})
                                    _revoga_enc.append(f"{outra.get('tipo','')} {num_outra}/{outra.get('ano','')}")
                                    logs.append({"nivel": "aviso", "msg": f"  ⚠️ REVOGACAO TOTAL: {tipo} {numero}/{ano} revoga integralmente {outra.get('tipo')} {num_outra}/{outra.get('ano','')}"})
                                    logs.append({"nivel": "aviso", "msg": f"     {outra.get('tipo')} {num_outra}/{outra.get('ano','')} NAO sera analisada — perdeu vigencia"})
                                    break
                    except Exception as _e_rev:
                        logs.append({"nivel": "aviso", "msg": f"  Erro verificacao revogacao: {str(_e_rev)[:60]}"})
        # Perguntar ao Gemini sobre todas as relacoes da legislacao
        logs.append({"nivel": "info", "msg": f"  [RELACOES] texto_enc={len(texto_enc)} chars, iniciando analise de relacoes...", "nivel": "relacao"})
        if texto_enc:
            try:
                prompt_rel = (
                    f"Analise o texto da {tipo} {numero}/{ano} de {municipio}/{estado} e responda:\n\n"
                    f"1. Esta lei ALTERA outra lei? Liste todas as leis que ela altera (parcialmente).\n"
                    f"2. Esta lei REVOGA TOTALMENTE outra lei? Liste apenas as que foram completamente substituidas.\n"
                    f"3. Esta lei REVOGA PARCIALMENTE outra lei? Liste as leis parcialmente revogadas e descreva QUAIS artigos, incisos ou partes foram revogados.\n"
                    f"4. Esta lei REGULAMENTA outra lei? Liste as leis que ela regulamenta.\n"
                    f"5. Esta lei e ALTERADA por alguma lei mais recente mencionada no texto? Liste.\n"
                    f"6. Esta lei e REVOGADA TOTALMENTE por alguma lei mais recente mencionada no texto? Liste.\n"
                    f"7. Esta lei e REVOGADA PARCIALMENTE por alguma lei mais recente mencionada no texto? Liste as leis e descreva QUAIS artigos, incisos ou partes foram atingidos.\n"
                    f"8. Esta lei e REGULAMENTADA por alguma lei mencionada no texto? Liste.\n"
                    f"REGRA ABSOLUTA: os campos 'revogado_por' e 'revogado_parcialmente_por' so podem conter leis com ano POSTERIOR a {ano}. Uma lei de {ano} JAMAIS pode ser revogada por lei anterior a {ano}. Se mencionar lei de ano anterior, coloque em 'alterado_por' ou 'cita', NUNCA em 'revogado_por'.\n"
                    f"9. Ha trechos tachados/riscados no texto (tags <s>, <del>, text-decoration:line-through) indicando revogacao parcial por lei posterior? Se sim, identifique qual lei posterior causou isso.\n"
                    f"10. Ha anotacoes de 'Redacao dada por', 'Acrescido por', 'Incluido por', 'Nova redacao' ou similares indicando que uma lei posterior alterou trechos desta lei? Liste todas as leis posteriores mencionadas.\n\n"
                    f"Use formato 'Tipo Numero/Ano' para cada lei (ex: 'Lei Complementar 270/2024').\n"
                    f"Responda APENAS com JSON:\n"
                    f'{{\n'
                    f'  "altera": ["Lei X/ano"],\n'
                    f'  "revoga": ["Lei X/ano"],\n'
                    f'  "revoga_parcialmente": [{{"lei": "Lei X/ano", "partes": "descricao dos artigos/partes revogados"}}],\n'
                    f'  "regulamenta": ["Lei X/ano"],\n'
                    f'  "alterado_por": ["Lei X/ano"],\n'
                    f'  "revogado_por": ["Lei X/ano"],\n'
                    f'  "revogado_parcialmente_por": [{{"lei": "Lei X/ano", "partes": "descricao dos artigos/partes atingidos"}}],\n'
                    f'  "regulamentado_por": ["Lei X/ano"],\n'
                    f'  "cita": ["Lei X/ano"],\n'
                    f'  "tachado_por": ["Lei X/ano"],\n'
                    f'  "redacao_dada_por": ["Lei X/ano"]\n'
                    f'}}\n\n'
                    f"Para o campo 'cita': liste APENAS leis citadas em contexto de definicao de zoneamento, zonas, subzonas, parametros de parcelamento do solo, uso e ocupacao do solo. Ignore citacoes em contexto de competencia, procedimento ou referencia generica.\n\n"
                    f"TEXTO:\n{texto_enc[:6000]}"
                )
                # Processar texto em blocos de 30000 chars
                _BLOCO = 30000
                _OVERLAP = 2000
                _blocos = []
                _pos = 0
                while _pos < len(texto_enc):
                    _blocos.append(texto_enc[_pos:_pos + _BLOCO])
                    _pos += _BLOCO - _OVERLAP
                logs.append({"nivel": "relacao", "msg": f"  [RELACOES] Analisando {len(_blocos)} bloco(s) de texto ({len(texto_enc)} chars total)..."})
                import re as _re_rel
                for _bi, _bloco in enumerate(_blocos):
                    _prompt_bloco = prompt_rel.replace(
                        f"TEXTO:\n{texto_enc[:6000]}",
                        f"TEXTO (bloco {_bi+1}/{len(_blocos)}):\n{_bloco}"
                    )
                    resp_rel = chamar_llm(_prompt_bloco, logs, f"Relacoes {tipo} {numero} bloco {_bi+1}/{len(_blocos)}")
                    if not resp_rel:
                        continue
                    try:
                        resp_rel_c = _re_rel.sub(r"^```json\s*|\s*```$", "", (resp_rel or "").strip())
                        _s_rl = resp_rel_c.find('{')
                        _d_rl = 0; _e_rl = _s_rl
                        for _i_rl, _c_rl in enumerate(resp_rel_c[_s_rl:], _s_rl):
                            if _c_rl == '{': _d_rl += 1
                            elif _c_rl == '}':
                                _d_rl -= 1
                                if _d_rl == 0: _e_rl = _i_rl + 1; break
                        if _s_rl < 0: raise ValueError("sem JSON na resposta Gemini")
                        dados_rel = _json.loads(resp_rel_c[_s_rl:_e_rl])
                        _altera_enc = list(set(_altera_enc + dados_rel.get("altera", [])))
                        _revoga_enc = list(set(_revoga_enc + dados_rel.get("revoga", [])))
                        # Revogação parcial — lista de objetos {lei, partes}
                        _new_rp = dados_rel.get("revoga_parcialmente", [])
                        if isinstance(_new_rp, list):
                            _chaves_rp = {r.get('lei','') for r in _revoga_parcialmente_enc}
                            for _rp in _new_rp:
                                if isinstance(_rp, dict) and _rp.get('lei','') not in _chaves_rp:
                                    _revoga_parcialmente_enc.append(_rp)
                        _regulamenta_enc = list(set(_regulamenta_enc + dados_rel.get("regulamenta", [])))
                        _ap_raw = dados_rel.get("alterado_por", [])
                        import re as _re_ano3
                        _ap_filtrado = []
                        for _ap_item in _ap_raw:
                            _m_ano3 = _re_ano3.search(r'/(\d{4})', _ap_item)
                            if _m_ano3 and int(_m_ano3.group(1)) <= int(ano):
                                logs.append({"nivel": "aviso", "msg": f"  [FILTRO] Ignorando alterado_por {_ap_item} — ano <= {ano} (impossivel)"})
                            else:
                                _ap_filtrado.append(_ap_item)
                        _alterado_por_enc = list(set(_alterado_por_enc + _ap_filtrado))
                        _rp_raw = dados_rel.get("revogado_por", [])
                        import re as _re_ano
                        _rp_filtrado = []
                        for _rp_item in _rp_raw:
                            _m_ano = _re_ano.search(r'/(\d{4})', _rp_item)
                            if _m_ano and int(_m_ano.group(1)) <= int(ano):
                                logs.append({"nivel": "aviso", "msg": f"  [FILTRO] Ignorando revogado_por {_rp_item} — ano <= {ano} (impossivel)"})
                            else:
                                _rp_filtrado.append(_rp_item)
                        _revogado_por_enc = list(set(_revogado_por_enc + _rp_filtrado))
                        # Revogado parcialmente por — lista de objetos {lei, partes}
                        _new_rpb = dados_rel.get("revogado_parcialmente_por", [])
                        if isinstance(_new_rpb, list):
                            import re as _re_rpb
                            _chaves_rpb = {r.get('lei','') for r in _revogado_parcialmente_por_enc}
                            for _rpb in _new_rpb:
                                if not isinstance(_rpb, dict): continue
                                _rpb_lei = _rpb.get('lei','')
                                _m_rpb = _re_rpb.search(r'/(\d{4})', _rpb_lei)
                                if _m_rpb and int(_m_rpb.group(1)) <= int(ano):
                                    logs.append({"nivel": "aviso", "msg": f"  [FILTRO] Ignorando revogado_parcialmente_por {_rpb_lei} — ano <= {ano} (impossivel)"})
                                    continue
                                if _rpb_lei not in _chaves_rpb:
                                    _revogado_parcialmente_por_enc.append(_rpb)
                        _rp2_raw = dados_rel.get("regulamentado_por", [])
                        import re as _re_ano2
                        _rp2_filtrado = []
                        for _rp2_item in _rp2_raw:
                            _m_ano2 = _re_ano2.search(r'/(\d{4})', _rp2_item)
                            if _m_ano2 and int(_m_ano2.group(1)) <= int(ano):
                                logs.append({"nivel": "aviso", "msg": f"  [FILTRO] Ignorando regulamentado_por {_rp2_item} — ano <= {ano} (impossivel)"})
                            else:
                                _rp2_filtrado.append(_rp2_item)
                        _regulamentado_por_enc = list(set(_regulamentado_por_enc + _rp2_filtrado))
                        _cita_enc = list(set(_cita_enc + dados_rel.get("cita", [])))
                        _tachado_por = dados_rel.get("tachado_por", [])
                        if _tachado_por:
                            logs.append({"nivel": "aviso", "msg": f"  [TACHADO] Trechos riscados identificados — revogado parcialmente por: {_tachado_por}"})
                            _revogado_por_enc = list(set(_revogado_por_enc + _tachado_por))
                        _redacao_dada_por = dados_rel.get("redacao_dada_por", [])
                        for _rdp in _redacao_dada_por:
                            logs.append({"nivel": "relacao", "msg": f"  [REDACAO] {_rdp} alterou trechos desta lei (Redacao dada por)"})
                        if _redacao_dada_por:
                            _alterado_por_enc = list(set(_alterado_por_enc + _redacao_dada_por))
                    except Exception as _eb:
                        logs.append({"nivel": "aviso", "msg": f"  [RELACOES] Erro bloco {_bi+1}: {str(_eb)[:60]}"})
                logs.append({"nivel": "relacao", "msg": f"  [RELACOES] altera={_altera_enc} revoga={_revoga_enc} regulamenta={_regulamenta_enc} alterado_por={_alterado_por_enc}"})
            except Exception as _e_rel:
                logs.append({"nivel": "aviso", "msg": f"  [RELACOES] ERRO: {str(_e_rel)[:100]}", "nivel": "relacao"})
                _regulamenta_enc = []
                _alterado_por_enc = []
                _revogado_por_enc = []
                _regulamentado_por_enc = []
                _revoga_parcialmente_enc = []
                _revogado_parcialmente_por_enc = []
        else:
            _regulamenta_enc = []
            _alterado_por_enc = []
            _revogado_por_enc = []
            _regulamentado_por_enc = []
            _revoga_parcialmente_enc = []
            _revogado_parcialmente_por_enc = []
        # Emitir evento final com todos os relacionamentos
        if not enc:
            _tabela_evento(logs, municipio, estado, tipo, numero, ano, pergunta=_pergunta_origem, status='nao_encontrada')
        if enc:
            _tabela_evento(logs, municipio, estado,
            enc.get('tipo', tipo), enc.get('numero', numero), enc.get('ano', ano),
            pergunta=_pergunta_origem, status="encontrada",
            altera=_altera_enc, alterado_por=_alterado_por_enc,
            revoga=_revoga_enc, revogado_por=_revogado_por_enc,
            revoga_parcialmente=_revoga_parcialmente_enc,
            revogado_parcialmente_por=_revogado_parcialmente_por_enc,
            cita=list(set(_regulamenta_enc + _cita_enc)), citado_em=_regulamentado_por_enc,
            link=enc.get('link',''),
            ementa=enc.get('ementa','') or leg.get('descricao','') or '')
        # Adicionar leis descobertas nas relacoes a fila dinamica
        _nivel_atual = leg.get("_nivel", 0)
        def _extrair_num_ano_fila(s):
            import re as _refa
            m = _refa.search(r'([\d\.]+)[/\-](\d{4})', s)
            return (m.group(1), m.group(2)) if m else (None, None)
        def _adicionar_na_fila(lista_leis, nivel, motivo):
            for _lei_str in (lista_leis or []):
                _num, _ano = _extrair_num_ano_fila(_lei_str)
                if not _num or not _ano:
                    continue
                _lstr_low = str(_lei_str).lower()
                if " federal" in _lstr_low or "lei federal" in _lstr_low or " estadual" in _lstr_low or "lei estadual" in _lstr_low:
                    logs.append({"nivel": "info", "msg": f"  [FILTRO] {_lei_str} federal/estadual"})
                    continue
                _tipo_f = "Lei"
                for _tp in ["Lei Complementar", "Decreto-Lei", "Decreto", "Resolucao", "Resolucao", "Lei"]:
                    if _tp.lower() in _lei_str.lower():
                        _tipo_f = _tp
                        break
                _num_norm_f = (_num.replace('.','').replace(' ','').strip().lstrip('0') or '0')
                _chave_f = f"{_tipo_f.lower()}_{_num_norm_f}_{_ano}"
                if _chave_f in analisadas or _chave_f in revogadas:
                    continue
                if any(f"{l.get('tipo','').lower()}_{l.get('numero','').replace('.','').replace(' ','').strip()}_{l.get('ano','')}" == _chave_f for l in fila):
                    continue
                logs.append({"nivel": "info", "msg": f"  [FILA] Adicionando {_tipo_f} {_num}/{_ano} nivel={nivel} — {motivo}"})
                fila.append({"tipo": _tipo_f, "numero": _num, "ano": _ano, "descricao": motivo, "_nivel": nivel, "_pergunta_label": ""})
        for _rr in (_revoga_enc or []):
            _rn,_ra=_extrair_num_ano_fila(_rr)
            if not _rn or not _ra: continue
            _rt="lei"
            for _tp in ["lei complementar","decreto-lei","decreto","resolucao"]:
                if _tp in str(_rr).lower(): _rt=_tp;break
            _rnorm=_rn.replace(".","").replace(" ","").strip().lstrip("0") or "0"
            if f"{_rt}_{_rnorm}_{_ra}" in analisadas:
                logs.append({"nivel":"aviso","msg":f"  [RETRO] {_rr} ja analisada — revogada por {tipo} {numero}/{ano}"})
                _tabela_evento(logs,municipio,estado,_rt,_rn,_ra,pergunta="",status="revogada",revogado_por=[f"{tipo} {numero}/{ano}"])
        if _nivel_atual < 2:
            # Se esta lei (nivel 1) altera uma lei de nivel 0, suas regulamentadoras
            # sao promovidas a nivel 1 (Gemini pode ter omitido na busca inicial)
            # Verificar se alguma lei em _altera_enc (strings) bate com nivel0_chaves
            _altera_nivel0 = False
            if _nivel_atual == 1:
                for _a in _altera_enc:
                    _an, _ay = _extrair_num_ano_fila(str(_a))
                    if not _an or not _ay:
                        continue
                    _at = "lei"
                    for _tp in ["lei complementar", "decreto-lei", "decreto", "resolucao", "resolução"]:
                        if _tp in str(_a).lower():
                            _at = _tp
                            break
                    _an_n = _an.replace('.','').replace(' ','').strip().lstrip('0') or '0'
                    if f"{_at}_{_an_n}_{_ay}" in nivel0_chaves:
                        _altera_nivel0 = True
                        break
            _nivel_filho = 1 if _altera_nivel0 else (_nivel_atual + 1)
            _adicionar_na_fila(_altera_enc, _nivel_filho, "alterada por lei atual")
            _adicionar_na_fila(_regulamenta_enc, _nivel_filho, "regulamentada por lei atual")
            _adicionar_na_fila(_alterado_por_enc, _nivel_filho, "altera lei atual")
            _adicionar_na_fila(_regulamentado_por_enc, _nivel_filho, "regulamenta lei atual")
            # Leis citadas: só adicionar se nivel_atual == 0 (lei principal), nunca em cascata
            _cita_enc_local = _cita_enc
            if _nivel_atual > 0:
                _cita_enc_local = []  # não propagar citações de leis descobertas
            for _cit_str in (_cita_enc_local or []):
                _num_c, _ano_c = _extrair_num_ano_fila(_cit_str)
                if not _num_c or not _ano_c:
                    continue
                _tipo_c = "Lei"
                for _tp in ["Lei Complementar", "Decreto-Lei", "Decreto", "Resolucao", "Lei"]:
                    if _tp.lower() in _cit_str.lower():
                        _tipo_c = _tp
                        break
                _num_c_n = _num_c.replace('.','').replace(' ','').strip().lstrip('0') or '0'
                _chave_c = f"{_tipo_c.lower()}_{_num_c_n}_{_ano_c}"
                if _chave_c in analisadas or _chave_c in revogadas:
                    continue
                if any(f"{l.get('tipo','').lower()}_{(l.get('numero','').replace('.','').replace(' ','').strip().lstrip('0') or '0')}_{l.get('ano','')}" == _chave_c for l in fila):
                    continue
                # IA avalia se a citacao e em contexto urbanistico relevante
                try:
                    _trecho=""
                    try:
                        _ii=texto_enc.lower().find(_num_c.lower())
                        if _ii>=0: _trecho=texto_enc[max(0,_ii-200):min(len(texto_enc),_ii+200)].replace("\n"," ").strip()
                    except: pass
                    _prompt_ctx=f"Trecho de {municipio}/{estado}:\n[TRECHO]:{_trecho or str(_cit_str)}\n\nA citacao a '{_cit_str}' condiciona parametros como gabarito, recuos, CA, TO, zoneamento, parcelamento, lote minimo, areas computaveis ou qualquer parametro que influencie ocupacao e potencial construtivo?\nResponda APENAS: sim ou nao"
                    _resp_ctx = chamar_llm(_prompt_ctx, logs, f"Ctx cita {_num_c}")
                    if _resp_ctx and "sim" in _resp_ctx.lower():
                        logs.append({"nivel": "info", "msg": f"  [FILA] {_cit_str} citada em contexto urbanistico — adicionando nivel=1"})
                        fila.append({"tipo": _tipo_c, "numero": _num_c, "ano": _ano_c, "descricao": "citada em contexto urbanistico", "_nivel": 1, "_pergunta_label": ""})
                    else:
                        logs.append({"nivel": "info", "msg": f"  [FILA] {_cit_str} citada mas contexto nao urbanistico — ignorando"})
                except Exception:
                    pass
            for _rev_str in (_revoga_enc or []):
                _num_r, _ano_r = _extrair_num_ano_fila(_rev_str)
                if _num_r and _ano_r:
                    for _tp in ["Lei Complementar", "Decreto-Lei", "Decreto", "Resolucao", "Lei"]:
                        if _tp.lower() in _rev_str.lower():
                            _num_r_n = (_num_r.replace('.','').replace(' ','').strip().lstrip('0') or '0')
                            _chave_r = f"{_tp.lower()}_{_num_r_n}_{_ano_r}"
                            revogadas.add(_chave_r)
                            revogadas_lista.append({"tipo": _tp, "numero": _num_r, "ano": _ano_r, "revogada_por": f"{tipo} {numero}/{ano}"})
                            logs.append({"nivel": "aviso", "msg": f"  [FILA] {_rev_str} marcada como revogada por {tipo} {numero}/{ano}"})
                            break

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

            if max_legislacoes and len(resultado["encontradas"]) >= max_legislacoes:
                logs.append({"nivel": "ok", "msg": f"  Limite de {max_legislacoes} legislacoes encontradas atingido — encerrando busca"})
                break

    # Resultado final — resumo por pergunta
    LABELS_PERGUNTAS = [
        "Parametros urbanisticos",
        "Zoneamento",
        "Uso e ocupacao do solo",
        "Parcelamento do solo",
        "Codigo de obras",
    ]
    logs.append({"nivel": "ok", "msg": "=" * 50})
    logs.append({"nivel": "ok", "msg": f"RESUMO DA BUSCA — {municipio}/{estado}"})
    logs.append({"nivel": "ok", "msg": "=" * 50})
    # Sumario de leis descartadas por nivel
    if _descartadas_log:
        _desc_n0 = [d for d in _descartadas_log if d["nivel"] == 0]
        _desc_n1 = [d for d in _descartadas_log if d["nivel"] == 1]
        if _desc_n0:
            logs.append({"nivel": "aviso", "msg": f"⚠️ {len(_desc_n0)} lei(s) de nivel 0 descartadas (identificadas pelo Gemini mas sem parametros urbanisticos):"})
            for _d in _desc_n0:
                logs.append({"nivel": "aviso", "msg": f"   — {_d['tipo']} {_d['numero']}/{_d['ano']}: {_d['motivo']}"})
        if _desc_n1:
            logs.append({"nivel": "info", "msg": f"ℹ️ {len(_desc_n1)} lei(s) de nivel 1 descartadas (descobertas via relacoes mas sem parametros urbanisticos):"})
            for _d in _desc_n1:
                logs.append({"nivel": "info", "msg": f"   — {_d['tipo']} {_d['numero']}/{_d['ano']}: {_d['motivo']}"})

    # Associar cada legislacao encontrada a uma pergunta pelo tipo/descricao
    KEYWORDS_PERGUNTAS = [
        ["parametro", "plano diretor", "desenvolvimento"],
        ["zoneamento", "zona", "macrozona"],
        ["uso e ocupacao", "uso do solo", "ocupacao do solo"],
        ["parcelamento", "loteamento", "subdivis"],
        ["codigo de obras", "edificacoes", "construcoes"],
    ]

    PERGUNTAS_CONFIRMACAO = [
        f"Esta legislacao define ou trata dos parametros urbanisticos de {municipio}/{estado}?",
        f"Esta legislacao define ou trata do zoneamento de {municipio}/{estado}?",
        f"Esta legislacao define ou trata do uso e ocupacao do solo de {municipio}/{estado}?",
        f"Esta legislacao define ou trata do parcelamento do solo de {municipio}/{estado}?",
        f"Esta legislacao e o codigo de obras ou edificacoes de {municipio}/{estado}?",
    ]
    for idx, (label, pergunta_conf) in enumerate(zip(LABELS_PERGUNTAS, PERGUNTAS_CONFIRMACAO)):
        leg_match = None
        for enc in resultado["encontradas"]:
            _t = enc.get("tipo", "")
            _n = enc.get("numero", "")
            _a = enc.get("ano", "")
            _link = enc.get("link", "")
            prompt_conf = (
                f"{pergunta_conf}\n\n"
                f"Legislacao: {_t} {_n}/{_a} de {municipio}/{estado}\n"
                f"Ementa/link: {_link[:200]}\n\n"
                f"Responda APENAS: sim ou nao"
            )
            try:
                resp_conf = chamar_llm(prompt_conf, logs, f"Conf {idx+1} {_t} {_n}")
                if resp_conf and "sim" in resp_conf.lower():
                    leg_match = enc
                    break
            except Exception:
                pass
        if leg_match:
            link = leg_match.get("link", "")
            pdf = leg_match.get("pdf_path", "") or ""
            logs.append({"nivel": "ok", "msg": f"{idx+1}. {label}: {leg_match.get('tipo','')} No {leg_match.get('numero','')} de {leg_match.get('ano','')} — ENCONTRADA"})
            if pdf:
                logs.append({"nivel": "ok", "msg": f"   Download PDF: {pdf}"})
            elif link:
                logs.append({"nivel": "ok", "msg": f"   Link: {link}"})
        else:
            logs.append({"nivel": "aviso", "msg": f"{idx+1}. {label}: nao encontrada"})

    logs.append({"nivel": "ok", "msg": "=" * 50})

    if not resultado["encontradas"]:
        resultado["nao_encontrada"] = True

    # Mesclar contador global (chamar_llm) com contador local (_gemini_pergunta)
    try:
        if _get_ts:
            _global = _get_ts()
            _token_stats['input'] += _global.get('input', 0)
            _token_stats['output'] += _global.get('output', 0)
    except Exception:
        pass
    # Log tokens e custo estimado (Gemini 2.5 Flash: $0.30/M input, $2.50/M output)
    _custo_i = _token_stats['input'] / 1_000_000 * 0.30
    _custo_o = _token_stats['output'] / 1_000_000 * 2.50
    _custo_t = _custo_i + _custo_o
    logs.append({"nivel": "info", "msg": f"📊 Tokens Gemini — Entrada: {_token_stats['input']:,} | Saída: {_token_stats['output']:,}"})
    logs.append({"nivel": "info", "msg": f"💰 Custo estimado — US${_custo_t:.4f} (≈ R${_custo_t*5.8:.2f})"})
    resultado["token_stats"] = _token_stats
    resultado["custo_usd"] = round(_custo_t, 6)

    # ETAPA FINAL: Gerar ZIP consolidado com subpastas por categoria
    if resultado.get("encontradas"):
        try:
            import zipfile, json as _json_zip, os as _os_zip
            import unicodedata as _ud, re as _re_zip

            def _slug(s):
                s = _ud.normalize('NFKD', s).encode('ascii', 'ignore').decode()
                return _re_zip.sub(r'[^A-Za-z0-9_]', '_', s).strip('_')[:60]

            CATEGORIAS = {
                'plano_diretor': ['plano diretor','pddua','pdm','plano de desenvolvimento'],
                'zoneamento':    ['zoneamento','macrozoneamento','zona de uso','zona especial'],
                'uso_ocupacao':  ['uso e ocupacao','uso do solo','ocupacao do solo','coeficiente'],
                'parcelamento':  ['parcelamento','loteamento','desmembramento','condominio'],
                'codigo_obras':  ['codigo de obras','codigo de edificacoes','obras e edificacoes'],
            }
            PASTAS = {
                'plano_diretor': '01_Plano_Diretor',
                'zoneamento':    '02_Zoneamento',
                'uso_ocupacao':  '03_Uso_e_Ocupacao',
                'parcelamento':  '04_Parcelamento_Solo',
                'codigo_obras':  '05_Codigo_de_Obras',
                'outros':        '06_Outros',
            }

            def _cat(leg):
                desc = (leg.get('descricao') or leg.get('ementa') or '').lower()
                for cat, palavras in CATEGORIAS.items():
                    if any(p in desc for p in palavras):
                        return cat
                return 'outros'

            mun_slug = _slug(municipio)
            est_slug = _slug(estado)
            import datetime as _dt_zip
            _agora_zip = _dt_zip.datetime.now(_dt_zip.timezone(_dt_zip.timedelta(hours=-3)))
            _ts_zip = _agora_zip.strftime('%d%m%Y_%H%M')
            zip_nome = f"legislacoes_{mun_slug}_{est_slug}_{_ts_zip}.zip"
            zip_path = f"/var/www/urbanlex/static/downloads/{zip_nome}"

            legs_json = []
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
                for leg in resultado["encontradas"]:
                    tipo = leg.get('tipo','')
                    num  = leg.get('numero','')
                    ano  = leg.get('ano','')
                    desc = leg.get('descricao') or leg.get('ementa') or ''
                    cat  = _cat(leg)
                    pasta = PASTAS[cat]
                    _tipo_abrev = tipo.replace('Lei Complementar','LC').replace('Decreto-Lei','DL').replace('Decreto Lei','DL').replace('Decreto','Dec').replace('Lei Ordinaria','Lei').replace('Lei Orgânica','LO').replace('Resolucao','Res').replace('Portaria','Port')
                    leg_slug = _slug(f"{_tipo_abrev}_{num}_{ano}")
                    base = f"{pasta}/{leg_slug}/"

                    # PDF principal
                    pdf = leg.get('pdf_path') or leg.get('caminho_pdf') or ''
                    if pdf and pdf.startswith('/static'):
                        pdf = '/var/www/urbanlex' + pdf
                    if pdf and _os_zip.path.exists(pdf):
                        ext_p = _os_zip.path.splitext(pdf)[1] or '.pdf'
                        zf.write(pdf, base + leg_slug + ext_p)

                    # Anexos (suporta 'anexos' e 'anexos_lm')
                    for anx in (leg.get('anexos') or leg.get('anexos_lm') or []):
                        ap = anx.get('path') or anx.get('caminho') or ''
                        an = anx.get('nome') or _os_zip.path.basename(ap)
                        if ap and _os_zip.path.exists(ap):
                            zf.write(ap, base + 'anexos/' + an)

                    legs_json.append({
                        'tipo': tipo, 'numero': num, 'ano': ano,
                        'descricao': desc, 'categoria': cat,
                        'status': 'vigente',
                        'link': leg.get('link') or leg.get('url') or '',
                        'pasta_zip': base,
                    })

                # JSON consolidado dentro do ZIP
                zf.writestr('legislacoes.json', _json_zip.dumps({
                    'municipio': municipio, 'estado': estado,
                    'total': len(legs_json), 'legislacoes': legs_json
                }, ensure_ascii=False, indent=2))

            resultado['zip_url']  = f"/static/downloads/{zip_nome}"
            resultado['zip_nome'] = zip_nome
            resultado['legislacoes_json'] = legs_json
            # Mapear para formato que o frontend renderizarResultadoBusca espera
            # Usar encontradas para pegar pdf_path e anexos_lm
            _enc_map = {}
            for _e in resultado.get('encontradas', []):
                _k = f"{_e.get('tipo','').lower()}_{_e.get('numero','').replace('.','').replace(' ','').strip()}_{_e.get('ano','')}"
                _enc_map[_k] = _e
            resultado['legislacoes'] = []
            for l in legs_json:
                _k2 = f"{l['tipo'].lower()}_{l['numero'].replace('.','').replace(' ','').strip()}_{l['ano']}"
                _enc_e = _enc_map.get(_k2, {})
                _pdf_path = _enc_e.get('pdf_path') or ''
                _pdf_url = f"/static/downloads/{_os_zip.path.basename(_pdf_path)}" if _pdf_path and _os_zip.path.exists(_pdf_path) else ''
                _anexos = [{'nome': a.get('nome','Anexo'), 'url': a.get('url', '')} for a in (_enc_e.get('anexos_lm') or [])]
                resultado['legislacoes'].append({
                    'nome': f"{l['tipo']} {l['numero']}/{l['ano']}",
                    'tipo': l['tipo'], 'numero': l['numero'], 'ano': l['ano'],
                    'municipio': municipio, 'estado': estado,
                    'url': l['link'], 'link': l['link'],
                    'descricao': l['descricao'], 'categoria': l['categoria'],
                    'relevancia': 1.0,
                    'texto_preview': l['descricao'][:200] if l['descricao'] else '',
                    '_fonte': 'leismunicipais',
                    'pdf_download_url': _pdf_url,
                    'anexos_lm': _anexos,
                    'ementa': _enc_e.get('ementa', ''),
                    'status': 'encontrada',
                    'altera': _enc_e.get('_altera_enc', []),
                    'alterado_por': _enc_e.get('_alterado_por_enc', []),
                    'revoga': _enc_e.get('_revoga_enc', []),
                    'revogado_por': _enc_e.get('_revogado_por_enc', []),
                    'revoga_parcialmente': _enc_e.get('_revoga_parcialmente_enc', []),
                    'revogado_parcialmente_por': _enc_e.get('_revogado_parcialmente_por_enc', []),
                    'regulamenta': _enc_e.get('_regulamenta_enc', []),
                    'regulamentado_por': _enc_e.get('_regulamentado_por_enc', []),
                    'cita': _enc_e.get('_cita_enc', []),
                    'citado_em': _enc_e.get('_citado_em_enc', []),
                })
            resultado['success'] = True
            logs.append({'nivel': 'ok', 'msg': f'📦 ZIP consolidado: {zip_nome} ({len(resultado["encontradas"])} legislações em {len(set(l["categoria"] for l in legs_json))} categorias)'})

        except Exception as _ez:
            logs.append({'nivel': 'aviso', 'msg': f'ZIP: erro — {str(_ez)[:120]}'})

    # ETAPA FINAL: Gerar relatório PDF
    try:
        from modulos.gerar_relatorio import gerar_relatorio_pdf as _gerar_pdf
        _custo = resultado.get('custo_usd')
        _tokens = resultado.get('token_stats')
        _nao_enc = [
            {'tipo': l.get('tipo',''), 'numero': l.get('numero',''), 'ano': l.get('ano',''), 'descricao': l.get('descricao','')}
            for l in legs if not any(
                e.get('tipo','').lower() == l.get('tipo','').lower() and
                e.get('numero','').replace('.','').strip() == l.get('numero','').replace('.','').strip() and
                e.get('ano','') == l.get('ano','')
                for e in resultado.get('encontradas', [])
            )
        ]
        _pdf_path, _pdf_url = _gerar_pdf(
            resultado, municipio, estado,
            custo_usd=_custo, token_stats=_tokens,
            nao_encontradas=_nao_enc, logs=logs
        )
        if _pdf_url:
            resultado['relatorio_url']  = _pdf_url
            resultado['relatorio_nome'] = _pdf_url.split('/')[-1]
    except Exception as _er:
        logs.append({'nivel': 'aviso', 'msg': f'Relatório: erro — {str(_er)[:100]}'})
    # ETAPA FINAL: Gerar tabela PDF
    try:
        from modulos.gerar_relatorio import gerar_tabela_pdf as _gerar_tabela
        # Montar lista completa com todos os status para a tabela
        _enc_set = {(e.get('tipo','').lower(), e.get('numero','').replace('.','').strip(), e.get('ano','')) for e in resultado.get('encontradas',[])}
        _tabela_todas = list(resultado.get('legislacoes') or [])
        for _l in legs:
            _k = (_l.get('tipo','').lower(), _l.get('numero','').replace('.','').strip(), _l.get('ano',''))
            if _k not in _enc_set:
                _tabela_todas.append({'tipo':_l.get('tipo',''),'numero':_l.get('numero',''),'ano':_l.get('ano',''),'municipio':municipio,'estado':estado,'status':_l.get('_status','nao_encontrada'),'ementa':_l.get('descricao',''),'link':''})
        resultado['tabela_legislacoes'] = _tabela_todas
        _tabela_path, _tabela_url = _gerar_tabela(resultado, municipio, estado, logs=logs)
        if _tabela_url:
            resultado['tabela_url']  = _tabela_url
            resultado['tabela_nome'] = _tabela_url.split('/')[-1]
    except Exception as _et:
        logs.append({'nivel': 'aviso', 'msg': f'Tabela PDF: erro — {str(_et)[:100]}'})

    return resultado


def _verificar_parametros(texto, municipio, estado, tipo, numero, ano, logs, chamar_llm, modo="parametros"):
    if modo == "parametros":
        criterio = (
            f"ATENCAO: Se o texto desta lei NAO pertencer ao municipio de {municipio}/{estado}, responda com define_parametros=false e motivo='Lei nao pertence ao municipio correto'.\n\n"
            f"Faca uma analise PROFUNDA e HOLISTICA de todo o texto da {tipo} {numero}/{ano} de {municipio}/{estado}, incluindo todos os artigos, paragrafos, incisos e anexos.\n"
            "Verifique se a lei define PELO MENOS 2 dos seguintes parametros urbanisticos:\n"
            "  - Taxa de ocupacao\n"
            "  - Indice ou coeficiente de aproveitamento\n"
            "  - Gabarito ou altura maxima de edificacao\n"
            "  - Afastamento frontal, lateral ou de fundos (recuo)\n"
            "  - Taxa de permeabilidade\n"
            "  - Densidade demografica ou construtiva\n"
            "  - Numero maximo de pavimentos\n"
            "  - Area minima de lote\n"
            "IMPORTANTE: Os parametros podem estar em qualquer parte do texto — artigos, tabelas, quadros, mapas ou anexos.\n"
            "define_parametros = true se encontrar pelo menos 2 desses parametros em qualquer parte da lei."
        )
    else:
        criterio = (
            f"ATENCAO: Se o texto desta lei NAO pertencer ao municipio de {municipio}/{estado}, responda com define_parametros=false e motivo='Lei nao pertence ao municipio correto'.\n\n"
            f"Analise de forma PROFUNDA e HOLISTICA a {tipo} {numero}/{ano} de {municipio}/{estado}.\n"
            "Leia o texto completo e compreenda o PROPOSITO GERAL da lei.\n"
            "define_parametros = true se a lei tratar de zoneamento, uso/ocupacao do solo, "
            "parcelamento do solo, codigo de obras, loteamentos ou qualquer ordenamento territorial urbano.\n"
            "EM CASO DE DUVIDA prefira true."
        )
    prompt = (
        f"Voce e um especialista em direito urbanistico brasileiro.\n"
        f"{criterio}\n\n"
        f"TEXTO COMPLETO DA LEI:\n{texto}\n\n"
        "Responda APENAS com JSON (sem markdown):\n"
        "{\n"
        "  \"define_parametros\": true ou false,\n"
        "  \"define_zoneamento\": true ou false,\n"
        "  \"parametros_encontrados\": [\"lista dos parametros ou temas encontrados\"],\n"
        "  \"referencias\": [\"Art. 10\", \"Anexo I\", ...],\n"
        "  \"leis_referenciadas\": [{\"tipo\": \"Lei\", \"numero\": \"123\", \"ano\": \"2010\", \"motivo\": \"complementa o zoneamento\"}],\n"
        "  \"ementa\": \"Ementa ou assunto da lei em ate 150 caracteres\",\n  \"motivo\": \"Explicacao clara sobre o que a lei trata e por que define_parametros e true ou false.\"\n"
        "}\n\n"
        "Regras:\n"
        "- define_zoneamento = true se a lei divide o municipio em zonas ou macrozonas\n"
        "- leis_referenciadas: outras leis citadas que complementem o ordenamento territorial. Se nenhuma, use []\n"
        "- No motivo, seja objetivo e claro"
    )
    resp = chamar_llm(prompt, logs, f"Verif {tipo} {numero}")
    if not resp:
        return False, [], ""
    try:
        import re as _re2
        resp_c = _re2.sub(r"^```json\s*|\s*```$", "", (resp or "").strip())
        # Brace-counting para evitar "Extra data"
        _s_c = resp_c.find('{')
        _d_c = 0; _e_c = _s_c
        for _i_c, _ch_c in enumerate(resp_c[_s_c:], _s_c):
            if _ch_c == '{': _d_c += 1
            elif _ch_c == '}':
                _d_c -= 1
                if _d_c == 0: _e_c = _i_c + 1; break
        dados = _json.loads(resp_c[_s_c:_e_c] if _s_c >= 0 else resp_c)
        motivo = dados.get("motivo", "")[:300]
        zoneamento = dados.get("define_zoneamento", False)
        parametros = dados.get("parametros_encontrados", [])
        referencias = dados.get("referencias", [])
        if dados.get("define_parametros"):
            logs.append({"nivel": "ok", "msg": f"  {motivo}"})
            if zoneamento:
                logs.append({"nivel": "ok", "msg": "  Define tambem o zoneamento do municipio."})
            else:
                logs.append({"nivel": "info", "msg": "  Nao define o zoneamento do municipio."})
            if parametros:
                logs.append({"nivel": "info", "msg": f"  Parametros/temas: {', '.join(parametros[:10])}"})
            if referencias:
                logs.append({"nivel": "info", "msg": f"  Referencias: {', '.join(referencias[:10])}"})
            leis_ref = dados.get("leis_referenciadas", [])
            if leis_ref:
                logs.append({"nivel": "info", "msg": f"  Leis referenciadas: {len(leis_ref)}"})
                for lr in leis_ref:
                    logs.append({"nivel": "info", "msg": f"    -> {lr.get('tipo','')} {lr.get('numero','')}/{lr.get('ano','')}: {lr.get('motivo','')[:80]}"})
            _ementa_verif = dados.get("ementa", "")[:150]
            return True, leis_ref, _ementa_verif
        else:
            logs.append({"nivel": "info", "msg": f"  {motivo}"})
            return False, [], ""
    except Exception as _e:
        logs.append({"nivel": "aviso", "msg": f"  Erro parse verificacao: {str(_e)[:60]}"})
        return False, [], ""


def _buscar_plano_diretor_lm(municipio, estado, logs, chamar_llm, analisadas):
    """Fallback: busca Plano Diretor no LeisMunicipais por palavra-chave quando Gemini nao retorna numero."""
    try:
        from modulos.navegador_universal import navegar_com_cookies_flaresolverr
        import requests as _rfs, os as _ofs
        try:
            _old = _ofs.environ.get("FLARESOLVERR_SESSION", "")
            # Destruir TODAS as sessoes acumuladas antes de criar nova
            try:
                _sl = _rfs.post("http://localhost:8191/v1", json={"cmd": "sessions.list"}, timeout=5)
                for _sid in _sl.json().get("sessions", []):
                    _rfs.post("http://localhost:8191/v1", json={"cmd": "sessions.destroy", "session": _sid}, timeout=5)
            except Exception:
                pass
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
            _ementa_lei = ""
            if html_lei:
                from bs4 import BeautifulSoup as _bs
                _soup_em = _bs(html_lei, "html.parser")
                _lc = _soup_em.find(class_='law-container')
                if _lc:
                    _p = _lc.find('p')
                    if _p:
                        _ementa_lei = _p.get_text(separator=' ', strip=True)[:300]
                texto_lei = _soup_em.get_text()
                # Extrair tipo/numero/ano da URL ou do HTML
                import re as _re
                m = _re.search(r"/([a-z-]+)/(\d{4})/\d+/(\d+)/", url_enc)
                tipo_enc = m.group(1).replace("-", " ").title() if m else "Legislacao"
                numero_enc = m.group(3) if m else "?"
                ano_enc = m.group(2) if m else "?"
                define, _leis_ref_pd = _verificar_parametros(texto_lei, municipio, estado, tipo_enc, numero_enc, ano_enc, logs, chamar_llm, modo="geral")
                if not define:
                    logs.append({"nivel": "aviso", "msg": "  IA: legislacao nao define parametros urbanisticos — descartando"})
                    return None
            return {"tipo": tipo_enc if 'tipo_enc' in dir() else "Legislacao", "numero": numero_enc if 'numero_enc' in dir() else "?", "ano": ano_enc if 'ano_enc' in dir() else "?", "link": url_enc}
        logs.append({"nivel": "info", "msg": f"  LeisMunicipais: Plano Diretor nao encontrado para {municipio}"})
    except Exception as e:
        logs.append({"nivel": "aviso", "msg": f"  Erro busca palavra-chave LM: {str(e)[:80]}"})
    return None

def _buscar_leismunicipais(municipio, estado, tipo, numero, ano, logs, chamar_llm, analisadas, modo="geral", fallback_url=None, _nivel=1):
    _fb_url_local = fallback_url
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
        _STATUS_DEF = ("nenhum_resultado_relevante", "palavra_chave_nao_encontrada", "municipio_nao_encontrado")
        _MAX_TENT = 3
        fs_result = {"encontrada": False}
        for _tent in range(_MAX_TENT):
            if _tent > 0:
                logs.append({"nivel": "aviso", "msg": f"  [Retry {_tent}/{_MAX_TENT-1}] Falha tecnica — renovando sessao e retentando LeisMunicipais..."})
                try:
                    import requests as _rfs2, os as _ofs2
                    _old2 = _ofs2.environ.get("FLARESOLVERR_SESSION", "")
                    if _old2:
                        _rfs2.post("http://localhost:8191/v1", json={"cmd": "sessions.destroy", "session": _old2}, timeout=5)
                    _rn2 = _rfs2.post("http://localhost:8191/v1", json={"cmd": "sessions.create"}, timeout=10)
                    _ns2 = _rn2.json().get("session", "")
                    if _ns2:
                        _ofs2.environ["FLARESOLVERR_SESSION"] = _ns2
                        import subprocess as _sp2
                        _sp2.run(["sed", "-i", f"s/FLARESOLVERR_SESSION=.*/FLARESOLVERR_SESSION={_ns2}/", "/var/www/urbanlex/.env"], capture_output=True)
                        logs.append({"nivel": "info", "msg": f"FlareSolverr sessao renovada: {_ns2[:8]}..."})
                except Exception as _ef2:
                    logs.append({"nivel": "aviso", "msg": f"FlareSolverr renovacao falhou: {str(_ef2)[:60]}"})
            logs.append({"nivel": "info", "msg": f"  Buscando {tipo} {numero}/{ano} no LeisMunicipais via FlareSolverr..."})
            fs_result = navegar_com_cookies_flaresolverr(url_fs, leg_dict, logs, label=f"LM {tipo} {numero}", chamar_llm=chamar_llm)
            if fs_result.get("site_fora_do_ar"):
                logs.append({"nivel": "erro", "msg": "  LeisMunicipais esta fora do ar — encerrando busca"})
                return None
            if fs_result.get("encontrada") and fs_result.get("url"):
                break
            if any(fs_result.get(s) for s in _STATUS_DEF):
                break
            if _tent < _MAX_TENT - 1:
                logs.append({"nivel": "aviso", "msg": f"  [Retry] Tentativa {_tent+1} sem resultado tecnico — tentando novamente..."})
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
            _ementa_lei = ""
            if html_lei:
                from bs4 import BeautifulSoup as _bs
                _soup_em = _bs(html_lei, "html.parser")
                _lc = _soup_em.find(class_='law-container')
                if _lc:
                    _p = _lc.find('p')
                    if _p:
                        _ementa_lei = _p.get_text(separator=' ', strip=True)[:300]
                texto_lei = _soup_em.get_text()
                # Extrair tipo/numero/ano da URL ou do HTML
                import re as _re
                m = _re.search(r"/([a-z-]+)/(\d{4})/\d+/(\d+)/", url_enc)
                tipo_enc = m.group(1).replace("-", " ").title() if m else "Legislacao"
                numero_enc = m.group(3) if m else "?"
                ano_enc = m.group(2) if m else "?"
                define, _leis_ref_pd = _verificar_parametros(texto_lei, municipio, estado, tipo_enc, numero_enc, ano_enc, logs, chamar_llm, modo="geral")
                if not define:
                    logs.append({"nivel": "aviso", "msg": "  IA: legislacao nao define parametros urbanisticos — descartando"})
                    return None
            return {"tipo": tipo_enc if 'tipo_enc' in dir() else "Legislacao", "numero": numero_enc if 'numero_enc' in dir() else "?", "ano": ano_enc if 'ano_enc' in dir() else "?", "link": url_enc}
        logs.append({"nivel": "info", "msg": f"  LeisMunicipais: Plano Diretor nao encontrado para {municipio}"})
    except Exception as e:
        logs.append({"nivel": "aviso", "msg": f"  Erro busca palavra-chave LM: {str(e)[:80]}"})
    return None

def _verificar_catalogado_lm(municipio, estado, logs, chamar_llm):
    """Verifica se municipio esta catalogado no LeisMunicipais via FlareSolverr direto (sem Playwright)."""
    import unicodedata, re as _re2
    def _slug(s):
        s = unicodedata.normalize('NFD', s.lower())
        s = ''.join(ch for ch in s if unicodedata.category(ch) != 'Mn')
        return _re2.sub(r'[^a-z0-9]+', '-', s).strip('-')
    try:
        import requests as _req2, os as _os2
        _session = _os2.environ.get('FLARESOLVERR_SESSION', '')
        _url = f"https://leismunicipais.com.br/prefeitura/{estado.lower()}/{_slug(municipio)}"
        _payload = {'cmd': 'request.get', 'url': _url, 'maxTimeout': 20000}
        if _session:
            _payload['session'] = _session
        logs.append({'nivel': 'info', 'msg': f'  [VERIF-LM] Checando catalogo: {_url}'})
        _r = _req2.post('http://localhost:8191/v1', json=_payload, timeout=25)
        if _r.status_code == 200:
            _sol = _r.json().get('solution', {})
            _html = _sol.get('response', '')
            _status = _sol.get('status', 200)
            # Catalogado = pagina retornou conteudo valido (nao 404, tem conteudo relevante)
            _catalogado = (_status == 200
                and len(_html) > 3000
                and 'leismunicipais' in _html.lower()
                and 'page not found' not in _html.lower()
                and 'nao encontrada' not in _html[:500].lower())
            logs.append({'nivel': 'info', 'msg': f'  [VERIF-LM] {municipio}/{estado}: {"CATALOGADO" if _catalogado else "NAO catalogado"} (html={len(_html)} status={_status})'})
            return _catalogado
    except Exception as _ev:
        logs.append({'nivel': 'aviso', 'msg': f'  [VERIF-LM] Erro: {str(_ev)[:60]} — assumindo catalogado'})
    return True  # conservador: em caso de erro assume catalogado

def _buscar_leismunicipais(municipio, estado, tipo, numero, ano, logs, chamar_llm, analisadas, modo="geral", fallback_url=None, _nivel=1):
    _fb_url_local = fallback_url
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
                texto_lei = _bs(html_lei, "html.parser").get_text()
                # Incluir texto dos anexos na verificacao
                _anexos_lm = fs_result.get("anexos_lm") or []
                for _anx in _anexos_lm:
                    _anx_path = _anx.get("path", "") or _anx.get("pdf_path", "") if isinstance(_anx, dict) else ""
                    _anx_texto = _anx.get("texto", "") if isinstance(_anx, dict) else ""
                    if _anx_texto:
                        texto_lei += f"\n\nANEXO: {_anx_texto}"
                if _anexos_lm:
                    logs.append({"nivel": "info", "msg": f"  Incluindo {len(_anexos_lm)} anexo(s) na verificacao"})
                define, _leis_ref, _ementa_verif = _verificar_parametros(texto_lei, municipio, estado, tipo, numero, ano, logs, chamar_llm, modo=modo)
                if not define:
                    # REGRA 2: Verificar se altera/complementa/regulamenta outra lei antes de descartar
                    prompt_altera = (
                        f"O texto abaixo e da {tipo} {numero}/{ano} de {municipio}/{estado}.\n"
                        f"Esta legislacao altera, complementa, regulamenta ou modifica artigos de outras legislacoes municipais?\n\n"
                        f"TEXTO:\n{texto_lei[:6000]}\n\n"
                        "Responda APENAS com JSON: {\"altera\": true ou false, \"leis_alteradas\": [{\"tipo\": \"\", \"numero\": \"\", \"ano\": \"\", \"descricao\": \"descreva o que altera\"}]}"
                    )
                    resp_altera = chamar_llm(prompt_altera, logs, f"Verif alteracao {tipo} {numero}")
                    _altera = False
                    if resp_altera:
                        try:
                            import re as _re_alt
                            resp_alt_c = _re_alt.sub(r"^```json\s*|\s*```$", "", (resp_altera or "").strip())
                            dados_alt = _json.loads(resp_alt_c)
                            _altera = dados_alt.get("altera", False)
                            leis_alt = dados_alt.get("leis_alteradas", [])
                            if _altera and leis_alt:
                                logs.append({"nivel": "ok", "msg": f"  ⚠️ ATENCAO: {tipo} {numero}/{ano} altera legislacoes existentes — NAO descartada!"})
                                for la in leis_alt:
                                    logs.append({"nivel": "ok", "msg": f"    -> Altera {la.get('tipo','')} {la.get('numero','')}/{la.get('ano','')}: {la.get('descricao','')[:100]}"})
                        except Exception:
                            pass
                    if not _altera:
                        if _nivel == 0:
                            logs.append({"nivel": "aviso", "msg": "  IA: legislacao nao define parametros mas e nivel 0 — mantendo"})
                        else:
                            logs.append({"nivel": "aviso", "msg": "  IA: legislacao nao define parametros e nao altera outras — descartando"})
                            return None
                    # Se altera outra lei, manter mesmo sem definir parametros diretamente
            _pdf = fs_result.get("pdf_nativo_s3") or fs_result.get("pdf_path") or ""
            if not _pdf and html_lei:
                try:
                    import tempfile as _tf, os as _os_wp, subprocess as _sp_wp, re as _re_wp
                    from bs4 import BeautifulSoup as _bs_pdf
                    _slug_pdf = _re_wp.sub(r'[^a-zA-Z0-9_]','_',f'{estado}_{municipio}_{tipo}_{numero}_{ano}')
                    _pdf_gen = f'/var/www/urbanlex/static/downloads/{_slug_pdf}_gerado.pdf'
                    _soup_pdf = _bs_pdf(html_lei, 'html.parser')
                    _container = _soup_pdf.find(class_='law-container') or _soup_pdf.find(class_='law-content') or _soup_pdf.find(class_='ato-content')
                    _html_pdf = _container if _container else _soup_pdf.find('body') or _soup_pdf
                    _html_limpo = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"><style>body{{font-family:Arial,sans-serif;font-size:11pt;margin:20mm;color:#111;}}p{{margin:4px 0;text-align:justify;line-height:1.5;}}strong,b{{font-weight:bold;}}em,i{{font-style:italic;}}h1,h2,h3{{text-align:center;margin:12px 0;}}</style></head><body>{str(_html_pdf)}</body></html>"""
                    with _tf.NamedTemporaryFile(suffix='.html', mode='w', encoding='utf-8', delete=False) as _tmp:
                        _tmp.write(_html_limpo)
                        _tmp_path = _tmp.name
                    _r_wk = _sp_wp.run(['wkhtmltopdf','--encoding','utf-8','--quiet','--disable-javascript','--no-images','--load-error-handling','ignore', _tmp_path, _pdf_gen], capture_output=True, text=True, timeout=30)
                    _os_wp.unlink(_tmp_path)
                    if _os_wp.path.exists(_pdf_gen) and _os_wp.path.getsize(_pdf_gen) > 1000:
                        _pdf = _pdf_gen
                        logs.append({'nivel': 'info', 'msg': f'  PDF gerado via wkhtmltopdf: {_os_wp.path.basename(_pdf_gen)}'})
                    else:
                        raise Exception(f'PDF vazio. stderr: {_r_wk.stderr[:200]}')
                except Exception as _ewp:
                    logs.append({'nivel': 'aviso', 'msg': f'  PDF wkhtmltopdf falhou: {str(_ewp)[:60]}'})
            _anexos = fs_result.get("anexos_lm") or []
            return {"tipo": tipo, "numero": numero, "ano": ano, "link": url_enc, "pdf_path": _pdf, "html": html_lei if "html_lei" in dir() else "", "anexos_lm": _anexos, "_leis_referenciadas": _leis_ref if "_leis_ref" in dir() else [], "ementa": _ementa_verif if "_ementa_verif" in dir() else (_ementa_lei if "_ementa_lei" in dir() else ""), "_fonte": "lm"}
        # Verificar se LM indicou municipio nao catalogado
        def _norm(s):
            import unicodedata
            return unicodedata.normalize("NFD", s.lower()).encode("ascii","ignore").decode()
        _lm_indisponivel = any(
            "nao estao disponiveis" in _norm(str(lg.get("msg",""))) or
            "nao esta disponivel" in _norm(str(lg.get("msg",""))) or
            "nao disponivel" in _norm(str(lg.get("msg",""))) or
            "not available" in _norm(str(lg.get("msg",""))) or
            "nao estao disponiveis neste portal" in _norm(str(lg.get("msg",""))) or
            "leis de" in _norm(str(lg.get("msg",""))) and "nao estao disponiveis" in _norm(str(lg.get("msg","")))
            for lg in logs[-15:]
        )
        if _lm_indisponivel:
            logs.append({"nivel": "aviso", "msg": f"  [LM] Municipio {municipio}/{estado} confirmado NAO catalogado — pulando LM para proximas leis"})
            # Retornar sentinel para o loop principal setar _lm_nao_catalogado
            enc_fb1 = _buscar_fallback1(municipio, estado, tipo, numero, ano, logs, chamar_llm, analisadas)
            if enc_fb1:
                enc_fb1["_lm_indisponivel"] = True
                return enc_fb1
            enc_fb2 = _buscar_fallback2(municipio, estado, tipo, numero, ano, logs, chamar_llm, analisadas)
            if enc_fb2:
                enc_fb2["_lm_indisponivel"] = True
            else:
                # Retornar dict vazio com sentinel para o loop setar o flag mesmo sem encontrar
                return {"_lm_indisponivel": True, "_nao_encontrada": True}
            return enc_fb2
        logs.append({"nivel": "aviso", "msg": f"  {tipo} {numero}/{ano} nao encontrada no LeisMunicipais — tentando 1º fallback..."})
        if _fb_url_local:
            logs.append({"nivel": "info", "msg": f"  [FallbackP] Tentando fonte prioritaria: {_fb_url_local[:80]}"})
            try:
                import urllib.parse as _upl
                _q = _upl.quote(f"{tipo} {numero} {ano}")
                from modulos.navegador_universal import navegar_com_cookies_flaresolverr as _ncf
                _html_fp = _ncf(f"{_fb_url_local.rstrip('/')}?q={_q}", municipio, estado, tipo, numero, ano, logs)
                if _html_fp and len(_html_fp) > 500:
                    _enc_fp = _verificar_parametros(_html_fp, municipio, estado, tipo, numero, ano, logs, chamar_llm)
                    if _enc_fp: return {"tipo": tipo, "numero": numero, "ano": ano, "link": f"{_fb_url_local}?q={_q}", "html": _html_fp, "ementa": _enc_fp.get("ementa","") if isinstance(_enc_fp,dict) else ""}
            except Exception as _efp:
                logs.append({"nivel": "aviso", "msg": f"  [FallbackP] Erro: {str(_efp)[:60]}"})
        enc = _buscar_fallback1(municipio, estado, tipo, numero, ano, logs, chamar_llm, analisadas)
        if enc:
            enc["_fonte"] = "fallback1"
            return enc
        logs.append({"nivel": "aviso", "msg": f"  1º fallback falhou — tentando 2º fallback (portal câmara/prefeitura)..."})
        _r2 = _buscar_fallback2(municipio, estado, tipo, numero, ano, logs, chamar_llm, analisadas)
        if _r2:
            _r2["_fonte"] = "fallback2"
        return _r2
    except Exception as e:
        logs.append({"nivel": "aviso", "msg": f"  Erro LeisMunicipais: {str(e)[:80]}"})
    return None

def _buscar_fallback1(municipio, estado, tipo, numero, ano, logs, chamar_llm, analisadas):
    """Fallback1: Google query formal, Gemini rankeia snippets, navega top 3 URLs com Playwright max 8 passos."""
    import urllib.parse as _upl, re as _re, requests as _req
    from bs4 import BeautifulSoup as _bs
    IGNORAR = [
        "leismunicipais.com.br", "legisweb.com.br", "jusbrasil.com",
        "facebook.com", "twitter.com", "instagram.com", "youtube.com",
        "wikipedia.org", "tiktok.com", "google.com", "bing.com",
        "escavador.com", "direitocom.com", "qconcursos.com",
        "portaldatransparencia.gov.br", "lexml.gov.br"
    ]
    query_str = f"Consulta Legislacao Prefeitura {municipio} {estado}"
    logs.append({"nivel": "info", "msg": f"  [Fallback1] Query Google: {query_str}"})
    _html_google = ""
    try:
        url_g = f"https://www.google.com/search?q={_upl.quote_plus(query_str)}&num=10&hl=pt-BR"
        _fs_resp = _req.post("http://localhost:8191/v1",
            json={"cmd": "request.get", "url": url_g, "maxTimeout": 30000}, timeout=35)
        _html_google = _fs_resp.json().get("solution", {}).get("response", "")
    except Exception as _eg:
        logs.append({"nivel": "aviso", "msg": f"  [Fallback1] FlareSolverr Google falhou: {str(_eg)[:60]}"})
    if not _html_google:
        logs.append({"nivel": "aviso", "msg": "  [Fallback1] Sem resultado do Google"})
        return None
    _soup_g = _bs(_html_google, "html.parser")
    _resultados_raw = []
    _vistos = set()
    for _div in _soup_g.find_all("div"):
        _a = _div.find("a", href=True)
        if not _a: continue
        _url = _a["href"]
        if not _url.startswith("http"): continue
        if any(ig in _url for ig in IGNORAR): continue
        if _url in _vistos: continue
        _vistos.add(_url)
        _titulo_el = _div.find("h3")
        _titulo = _titulo_el.get_text(strip=True)[:150] if _titulo_el else _url
        _snippet = _div.get_text(" ", strip=True)[:300]
        _resultados_raw.append({"url": _url, "titulo": _titulo, "snippet": _snippet})
        if len(_resultados_raw) >= 5: break
    if not _resultados_raw:
        _links = _re.findall(r'href="(https?://(?!www\.google)[^"&]{20,})"', _html_google)
        for _url in _links:
            if any(ig in _url for ig in IGNORAR): continue
            if _url in _vistos: continue
            _vistos.add(_url)
            _resultados_raw.append({"url": _url, "titulo": _url, "snippet": ""})
            if len(_resultados_raw) >= 5: break
    if not _resultados_raw:
        logs.append({"nivel": "aviso", "msg": "  [Fallback1] Nenhum resultado extraido do Google"})
        return None
    logs.append({"nivel": "info", "msg": f"  [Fallback1] {len(_resultados_raw)} resultados extraidos"})
    _linhas = []
    for i, r in enumerate(_resultados_raw):
        _linhas.append(f"{i+1}. TITULO: {r['titulo']}\n   URL: {r['url']}\n   SNIPPET: {r['snippet']}")
    _lista_para_gemini = "\n".join(_linhas)
    _prompt_rank = (
        f"Voce esta buscando a {tipo} {numero}/{ano} do municipio de {municipio}/{estado}.\n"
        f"Abaixo estao resultados do Google para 'Consulta Legislacao Prefeitura {municipio} {estado}'.\n"
        "Avalie cada resultado e decida:\n"
        "- TENTAR: site de camara municipal, prefeitura, portal de legislacao do municipio correto\n"
        "- IGNORAR: redes sociais, outros municipios, concursos, noticias, sites genericos\n\n"
        f"RESULTADOS:\n{_lista_para_gemini}\n\n"
        "Responda APENAS com JSON:\n"
        "{\"ranking\": [{\"indice\": 1, \"decisao\": \"TENTAR\"|\"IGNORAR\", \"motivo\": \"...\"}]}"
    )
    _urls_aprovadas = []
    try:
        _resp_rank = chamar_llm(_prompt_rank, logs, "fallback1_rank_snippets")
        if _resp_rank:
            import json as _json2, re as _re3
            _resp_clean = _re3.sub(r"^```json\s*|\s*```$", "", (_resp_rank or "").strip())
            _dados_rank = _json2.loads(_resp_clean)
            for item in _dados_rank.get("ranking", []):
                _idx = item.get("indice", 0) - 1
                _decisao = item.get("decisao", "IGNORAR")
                _motivo = item.get("motivo", "")
                if 0 <= _idx < len(_resultados_raw):
                    _url_item = _resultados_raw[_idx]["url"]
                    if _decisao == "TENTAR" and _url_item.lower() not in analisadas:
                        _urls_aprovadas.append(_url_item)
                        logs.append({"nivel": "info", "msg": f"  [Fallback1] TENTAR: {_url_item[:70]} — {_motivo[:60]}"})
                    else:
                        logs.append({"nivel": "info", "msg": f"  [Fallback1] IGNORAR: {_url_item[:70]} — {_motivo[:60]}"})
    except Exception as _er:
        logs.append({"nivel": "aviso", "msg": f"  [Fallback1] Ranking falhou: {str(_er)[:60]} — usando todos"})
        _urls_aprovadas = [r["url"] for r in _resultados_raw if r["url"].lower() not in analisadas]
    if not _urls_aprovadas:
        logs.append({"nivel": "aviso", "msg": "  [Fallback1] Nenhuma URL aprovada pelo Gemini"})
        return None
    _urls_aprovadas = _urls_aprovadas[:3]
    logs.append({"nivel": "info", "msg": f"  [Fallback1] {len(_urls_aprovadas)} URL(s) para navegar"})
    from modulos.navegador_universal import navegar_como_humano as _nav_humano
    from playwright.sync_api import sync_playwright as _swp
    _leg_dict = {"tipo": tipo, "numero": numero, "ano": ano, "municipio": municipio, "estado": estado, "data_publicacao": "", "assunto": ""}
    for _i, _url in enumerate(_urls_aprovadas):
        logs.append({"nivel": "info", "msg": f"  [Fallback1] Navegando {_i+1}/{len(_urls_aprovadas)}: {_url[:80]}"})
        analisadas.add(_url.lower())
        try:
            with _swp() as _pw:
                _browser = _pw.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
                _ctx = _browser.new_context(viewport={"width": 1280, "height": 800},
                    user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36")
                _page = _ctx.new_page()
                _page.goto(_url, timeout=30000, wait_until="domcontentloaded")
                _page.wait_for_timeout(2000)
                _res_nav = _nav_humano(_page, None, _leg_dict, chamar_llm, logs, label=f"FB1-{_i+1}", max_passos=15)
                _browser.close()
            if _res_nav and _res_nav.get("encontrada") and _res_nav.get("url"):
                _url_enc = _res_nav["url"]
                _html_enc = _res_nav.get("html", "")
                _pdf_enc = _res_nav.get("pdf_path", "")
                if not _html_enc and not _pdf_enc:
                    logs.append({"nivel": "aviso", "msg": f"  [Fallback1] URL {_i+1} marcada como encontrada mas sem conteudo — ignorando"})
                else:
                    logs.append({"nivel": "ok", "msg": f"  [Fallback1] Encontrada: {_url_enc[:80]}"})
                    return {"tipo": tipo, "numero": numero, "ano": ano, "link": _url_enc, "pdf_path": _pdf_enc, "html": _html_enc, "_fonte": "fallback1"}
            else:
                logs.append({"nivel": "info", "msg": f"  [Fallback1] URL {_i+1} nao encontrou em 8 passos"})
        except Exception as _en:
            logs.append({"nivel": "aviso", "msg": f"  [Fallback1] Erro navegando URL {_i+1}: {str(_en)[:80]}"})
    logs.append({"nivel": "aviso", "msg": f"  [Fallback1] {tipo} {numero}/{ano} nao encontrada nas URLs aprovadas"})
    return None

def _buscar_fallback2(municipio, estado, tipo, numero, ano, logs, chamar_llm, analisadas):
    """2º Fallback: encontra portal legislativo da câmara/prefeitura e busca a lei lá."""
    import urllib.parse, re as _re
    from bs4 import BeautifulSoup as _bs

    _tipo_lower = tipo.lower()
    _eh_decreto = 'decreto' in _tipo_lower
    headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"}
    IGNORAR = ["leismunicipais.com.br", "legisweb.com.br", "jusbrasil.com",
               "facebook.com", "twitter.com", "instagram.com", "youtube.com",
               "wikipedia.org", "tiktok.com", "google.com"]

    # Query para encontrar o portal legislativo
    if _eh_decreto:
        query_portal = f"Decretos prefeitura {municipio} {estado} legislação"
    else:
        query_portal = f"Legislação Câmara municipal {municipio} {estado}"

    logs.append({"nivel": "info", "msg": f"  [Fallback2] Buscando portal legislativo: {query_portal}"})

    try:
        url_g = f"https://www.google.com/search?q={urllib.parse.quote_plus(query_portal)}&num=10&hl=pt-BR"
        # Usar FlareSolverr para contornar bloqueio do Google
        try:
            _fs_resp2 = requests.post('http://localhost:8191/v1',
                json={"cmd":"request.get","url":url_g,"maxTimeout":30000}, timeout=35)
            _fs_data2 = _fs_resp2.json()
            _html_g2 = _fs_data2.get('solution',{}).get('response','')
        except Exception:
            _html_g2 = ''
        if not _html_g2:
            r = requests.get(url_g, headers=headers, timeout=15)
            _html_g2 = r.text
        soup = _bs(_html_g2, "html.parser")

        dominios_candidatos = []
        for a in soup.find_all("a", href=True):
            url = a["href"]
            if not url.startswith("http"):
                continue
            if any(ig in url for ig in IGNORAR):
                continue
            # Preferir domínios .leg.br, .gov.br, prefeitura, camara
            parsed = urllib.parse.urlparse(url)
            dominio = parsed.netloc
            if dominio and dominio not in [d for d, _ in dominios_candidatos]:
                score = 0
                if ".leg.br" in dominio: score += 3
                if ".gov.br" in dominio: score += 2
                if "prefeitura" in dominio or "camara" in dominio: score += 1
                if municipio.lower().replace(" ", "") in dominio.lower().replace("-", "").replace(".", ""): score += 2
                dominios_candidatos.append((dominio, score))

        dominios_candidatos.sort(key=lambda x: -x[1])
        dominios_top = [d for d, s in dominios_candidatos[:3]]

        if not dominios_top:
            logs.append({"nivel": "aviso", "msg": "  [Fallback2] Nenhum portal legislativo encontrado"})
            return None

        logs.append({"nivel": "info", "msg": f"  [Fallback2] Portais candidatos: {', '.join(dominios_top)}"})

        # Consultar cache de URL legislativa salva anteriormente
        try:
            from app import get_db as _gdb2
            _cc = _gdb2(); _ccu = _cc.cursor()
            _ccu.execute("SELECT url_legislacao FROM municipio_sites_referencia WHERE LOWER(municipio_nome)=LOWER(%s) AND LOWER(estado)=LOWER(%s) AND url_legislacao IS NOT NULL AND fallback2_funciona=TRUE LIMIT 1", (municipio, estado))
            _cache_row = _ccu.fetchone()
            _ccu.close(); _cc.close()
            if _cache_row and _cache_row[0]:
                logs.append({'nivel': 'info', 'msg': f'  [Fallback2] URL cached: {_cache_row[0]}'})
                dominios_top = [_cache_row[0].replace('https://','').replace('http://','').split('/')[0]] + dominios_top
        except: pass

        # Para cada domínio candidato, navegar com Playwright + Gemini Vision
        from modulos.navegador_universal import navegar_como_humano
        from playwright.sync_api import sync_playwright
        legislacao_fb2 = {'tipo': tipo, 'numero': numero, 'ano': ano, 'municipio': municipio, 'estado': estado}
        for dominio in dominios_top:
            logs.append({'nivel': 'info', 'msg': f'  [Fallback2] Navegando com Gemini Vision em: {dominio}'})
            url_portal = f'https://{dominio}'
            try:
                with sync_playwright() as _pw:
                    _browser = _pw.chromium.launch(headless=True, args=['--no-sandbox','--disable-dev-shm-usage'])
                    _ctx = _browser.new_context(viewport={'width':1280,'height':800},
                        user_agent='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36')
                    _page = _ctx.new_page()
                    _page.goto(url_portal, timeout=30000, wait_until='domcontentloaded')
                    _page.wait_for_timeout(2000)
                    resultado_nav = navegar_como_humano(
                        _page, None, legislacao_fb2, chamar_llm, logs,
                        label=f'[Fallback2] {dominio}', max_passos=25
                    )
                    _browser.close()
                if resultado_nav and resultado_nav.get('encontrada') and resultado_nav.get('url',''):
                    _url_found = resultado_nav.get('url','')
                    # Verificar se a URL encontrada tem conteúdo real (não loop/timeout)
                    _tem_html = bool(resultado_nav.get('html',''))
                    _tem_pdf = bool(resultado_nav.get('pdf_url',''))
                    _tem_pdf_path = bool(resultado_nav.get('pdf_path',''))
                    if not _tem_html and not _tem_pdf and not _tem_pdf_path:
                        logs.append({'nivel': 'aviso', 'msg': f'  [Fallback2] Encontrada sem conteúdo — ignorando'})
                    else:
                        logs.append({'nivel': 'ok', 'msg': f'  [Fallback2] Encontrada via Gemini Vision: {_url_found[:80]}'})
                        # Salvar domínio como fallback prioritário do município
                        _IGNORAR_CACHE = ['lexml.gov.br', 'jusbrasil.com', 'google.com', 'planalto.gov.br', 'facebook.com', 'youtube.com']
                        if not any(ig in _url_found for ig in _IGNORAR_CACHE):
                            try:
                                import urllib.parse as _upl2
                                _dominio_enc = _upl2.urlparse(_url_found)
                                _url_base = f"{_dominio_enc.scheme}://{_dominio_enc.netloc}/"
                                from app import get_db as _gdb3
                                _sc = _gdb3(); _scu = _sc.cursor()
                                _scu.execute("""INSERT INTO municipio_fallback (municipio, estado, url, atualizado_em)
                                    VALUES (%s,%s,%s,NOW())
                                    ON CONFLICT (municipio, estado) DO UPDATE SET url=%s, atualizado_em=NOW()""",
                                    (municipio, estado, _url_base, _url_base))
                                _sc.commit(); _scu.close(); _sc.close()
                                logs.append({'nivel': 'info', 'msg': f'  [Fallback2] Fallback prioritário salvo: {_url_base}'})
                            except: pass
                        return {'tipo': tipo, 'numero': numero, 'ano': ano,
                                'link': _url_found,
                                'pdf_url': resultado_nav.get('pdf_url',''),
                                'pdf_path': resultado_nav.get('pdf_path',''),
                                'html': resultado_nav.get('html','')}
            except Exception as e2:
                logs.append({'nivel': 'aviso', 'msg': f'  [Fallback2] Erro em {dominio}: {str(e2)[:80]}'})

    except Exception as e:
        logs.append({"nivel": "aviso", "msg": f"  [Fallback2] Erro geral: {str(e)[:80]}"})

    logs.append({"nivel": "aviso", "msg": f"  [Fallback2] {tipo} {numero}/{ano} não encontrada — encerrando busca desta legislação"})
    return None


# Aliases para compatibilidade com chamadas existentes
def _buscar_site_prefeitura(municipio, estado, tipo, numero, ano, logs, chamar_llm, analisadas):
    return _buscar_fallback1(municipio, estado, tipo, numero, ano, logs, chamar_llm, analisadas)


def _buscar_google(termo, municipio, estado, logs, chamar_llm, analisadas):
    # Extrai tipo/numero/ano do termo se possível
    import re as _re
    m = _re.search(r'(Lei Complementar|Lei|Decreto)[^\d]*(\d+)[^\d]*(\d{4})', termo, _re.IGNORECASE)
    if m:
        tipo, numero, ano = m.group(1), m.group(2), m.group(3)
    else:
        tipo, numero, ano = "Lei", "", ""
    return _buscar_fallback2(municipio, estado, tipo, numero, ano, logs, chamar_llm, analisadas)
