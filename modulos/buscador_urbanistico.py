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
                   revoga_parcialmente=None, revogado_parcialmente_por=None):
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
        "link": link or ""
    }
    logs.append({"nivel": "tabela", "msg": _j.dumps(dados, ensure_ascii=False)})

def buscar_legislacoes_urbanisticas(municipio, estado, logs, chamar_llm):
    resultado = {"encontradas": [], "nao_encontrada": False}
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
        f"quais sao todas as legislacoes vigentes que definem parametros urbanisticos de {municipio} {estado}? Liste todas, inclusive leis complementares, decretos regulamentadores e legislacoes especificas por zona.",
        f"quais sao todas as legislacoes vigentes de zoneamento e macrozoneamento de {municipio} {estado}? Inclua leis complementares, planos diretores e decretos relacionados.",
        f"quais sao todas as legislacoes vigentes de uso e ocupacao do solo de {municipio} {estado}? Liste todas, inclusive alteracoes parciais ainda em vigor.",
        f"quais sao todas as legislacoes vigentes de parcelamento do solo urbano de {municipio} {estado}? Inclua leis de loteamento, desmembramento e condominio.",
        f"quais sao todas as legislacoes vigentes que compoem o codigo de obras e edificacoes de {municipio} {estado}? Inclua decretos regulamentadores ainda em vigor.",
        f"qual e o Plano Diretor vigente de {municipio} {estado}? Inclua o nome oficial, numero e ano. Houve revisoes ou atualizacoes recentes?",
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
            import os as _os2
            GEMINI_KEY = _os2.environ.get("GEMINI_API_KEY", "")
            client = _genai_new.Client(api_key=GEMINI_KEY)
            google_search_tool = _types_new.Tool(google_search=_types_new.GoogleSearch())
            config = _types_new.GenerateContentConfig(tools=[google_search_tool])
            import concurrent.futures as _cf
            with _cf.ThreadPoolExecutor() as _ex:
                _fut = _ex.submit(client.models.generate_content, model="gemini-2.5-flash", contents=pergunta, config=config)
                response = _fut.result(timeout=30)
            # Contar tokens
            if hasattr(response, 'usage_metadata') and response.usage_metadata:
                _token_stats['input'] += getattr(response.usage_metadata, 'prompt_token_count', 0) or 0
                _token_stats['output'] += getattr(response.usage_metadata, 'candidates_token_count', 0) or 0
            resp_texto = (response.text or "").strip()
            if not resp_texto:
                raise ValueError("Gemini retornou texto vazio")
            logs.append({"nivel": "ok", "msg": f"Gemini respondeu ({len(resp_texto)} chars)"})
            prompt_e = (
                f"Com base na resposta abaixo, extraia as legislacoes mencionadas de {municipio}/{estado}.\n"
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
            logs.append({"nivel": "aviso", "msg": f"Gemini falhou: {str(e)[:100]} — usando DDG"})
            try:
                from modulos.buscador_legislacoes import _pesquisar_web
                resultados_ddg = _pesquisar_web(pergunta, logs, "DDG", max_results=5)
                conteudo_ddg = ""
                for res in (resultados_ddg or []):
                    conteudo_ddg += f"{res.get('title','')}\n{res.get('body','')}\n\n"
                if conteudo_ddg:
                    prompt_ddg = (
                        f"Identifique legislacoes de {municipio}/{estado} nos resultados.\n"
                        f"Nao invente numeros.\n\nRESULTADOS:\n{conteudo_ddg[:3000]}\n\n"
                        "Responda APENAS com JSON: {\"legislacoes\": [{\"tipo\": \"\", \"numero\": \"\", \"ano\": \"\", \"descricao\": \"\"}]}"
                    )
                    resp_ddg = chamar_llm(prompt_ddg, logs, "IA DDG")
                    if resp_ddg:
                        import re as _re2
                        resp_c2 = _re2.sub(r"^```json\s*|\s*```$", "", (resp_ddg or "").strip())
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
            chave = f"{leg.get('tipo','').lower()}_{numero}_{leg.get('ano','')}".lower()
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
                # Brace-counting para evitar "Extra data"
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
    fila = _deque(legs)
    while fila:
      leg = fila.popleft()
      if True:
        tipo = leg.get("tipo", "")
        numero = leg.get("numero", "")
        ano = leg.get("ano", "")
        descricao = leg.get("descricao", "")
        _num_norm = numero.replace('.','').replace(' ','').strip()
        chave = f"{tipo}_{_num_norm}_{ano}".lower()
        # 1. Verificar por chave exata
        if chave in revogadas:
            # Buscar quem revogou na lista enriquecida
            _rev_entry = next((r for r in revogadas_lista if f"{r.get('tipo','').lower()}_{r.get('numero','').strip()}_{r.get('ano','')}" == chave), None)
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
            continue
        analisadas.add(chave)
        # Nivel 2: apenas listar na tabela, sem busca completa
        _nivel_leg = leg.get("_nivel", 0)
        if _nivel_leg >= 2:
            logs.append({"nivel": "info", "msg": f"  [FILA] {tipo} {numero}/{ano} nivel=2 — apenas listada na tabela, sem busca"})
            _tabela_evento(logs, municipio, estado, tipo, numero, ano, pergunta=leg.get("_pergunta_label",""), status="referenciada", link="")
            continue
        logs.append({"nivel": "info", "msg": f"Buscando {tipo} n {numero}/{ano} ({descricao}) no LeisMunicipais..."})
        _pergunta_origem = leg.get("_pergunta_label", "")
        _tabela_evento(logs, municipio, estado, tipo, numero, ano, pergunta=_pergunta_origem, status="analisando")
        enc = _buscar_leismunicipais(municipio, estado, tipo, numero, ano, logs, chamar_llm, analisadas, modo=leg.get("_modo_verificacao","geral"))
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
            resultado["encontradas"].append(enc)
            # Verificar via IA se esta legislacao revoga outras da lista
            html_enc = enc.get("html", "") or ""
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
                            import google.generativeai as _gai_ax
                            _gai_ax.configure(api_key=__import__('os').environ.get('GEMINI_API_KEY',''))
                            _model_ax = _gai_ax.GenerativeModel('gemini-2.5-flash')
                            _parts = [f"Extraia todo o texto desta pagina de documento municipal brasileiro. Retorne apenas o texto, sem comentarios."]
                            for _pg_name in _pages:
                                _pg_path = _os_gs.path.join(_tmpdir_gs, _pg_name)
                                with open(_pg_path, 'rb') as _f_pg:
                                    _parts.append({"mime_type": "image/png", "data": _b64_ax.b64encode(_f_pg.read()).decode()})
                            _resp_ax = _model_ax.generate_content(_parts)
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
            outras_legs = [l for l in legs if f"{l.get('tipo','').lower()}_{l.get('numero','').strip()}_{l.get('ano','')}" != chave and f"{l.get('tipo','').lower()}_{l.get('numero','').strip()}_{l.get('ano','')}" not in revogadas and f"{l.get('tipo','').lower()}_{l.get('numero','').strip()}_{l.get('ano','')}" not in analisadas]
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
                        dados_rel = _json.loads(resp_rel_c)
                        _altera_enc = list(set(_altera_enc + dados_rel.get("altera", [])))
                        _revoga_enc = list(set(_revoga_enc + dados_rel.get("revoga", [])))
                        # Revogação parcial — lista de objetos {lei, partes}
                        _new_rp = dados_rel.get("revoga_parcialmente", [])
                        if isinstance(_new_rp, list):
                            _revoga_parcialmente_enc = locals().get('_revoga_parcialmente_enc', [])
                            _chaves_rp = {r.get('lei','') for r in _revoga_parcialmente_enc}
                            for _rp in _new_rp:
                                if isinstance(_rp, dict) and _rp.get('lei','') not in _chaves_rp:
                                    _revoga_parcialmente_enc.append(_rp)
                        _regulamenta_enc = list(set(_regulamenta_enc + dados_rel.get("regulamenta", [])))
                        _alterado_por_enc = list(set(_alterado_por_enc + dados_rel.get("alterado_por", [])))
                        _revogado_por_enc = list(set(_revogado_por_enc + dados_rel.get("revogado_por", [])))
                        # Revogado parcialmente por — lista de objetos {lei, partes}
                        _new_rpb = dados_rel.get("revogado_parcialmente_por", [])
                        if isinstance(_new_rpb, list):
                            _revogado_parcialmente_por_enc = locals().get('_revogado_parcialmente_por_enc', [])
                            _chaves_rpb = {r.get('lei','') for r in _revogado_parcialmente_por_enc}
                            for _rpb in _new_rpb:
                                if isinstance(_rpb, dict) and _rpb.get('lei','') not in _chaves_rpb:
                                    _revogado_parcialmente_por_enc.append(_rpb)
                        _regulamentado_por_enc = list(set(_regulamentado_por_enc + dados_rel.get("regulamentado_por", [])))
                        _cita_enc = list(set(locals().get('_cita_enc', []) + dados_rel.get("cita", [])))
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
        else:
            _regulamenta_enc = []
            _alterado_por_enc = []
            _revogado_por_enc = []
            _regulamentado_por_enc = []
            _revoga_parcialmente_enc = []
            _revogado_parcialmente_por_enc = []
        # Emitir evento final com todos os relacionamentos (sempre, independente da analise)
        if not enc:
            enc = {}
        _tabela_evento(logs, municipio, estado,
            enc.get('tipo', tipo), enc.get('numero', numero), enc.get('ano', ano),
            pergunta=_pergunta_origem, status="encontrada",
            altera=_altera_enc, alterado_por=_alterado_por_enc,
            revoga=_revoga_enc, revogado_por=_revogado_por_enc,
            revoga_parcialmente=locals().get('_revoga_parcialmente_enc', []),
            revogado_parcialmente_por=locals().get('_revogado_parcialmente_por_enc', []),
            cita=list(set(_regulamenta_enc + locals().get('_cita_enc', []))), citado_em=_regulamentado_por_enc,
            link=enc.get('link',''))
        # Adicionar leis descobertas nas relacoes a fila dinamica
        _nivel_atual = leg.get("_nivel", 0)
        def _extrair_num_ano_fila(s):
            import re as _refa
            m = _refa.search(r'(\d+)[/\-](\d{4})', s)
            return (m.group(1), m.group(2)) if m else (None, None)
        def _adicionar_na_fila(lista_leis, nivel, motivo):
            for _lei_str in (lista_leis or []):
                _num, _ano = _extrair_num_ano_fila(_lei_str)
                if not _num or not _ano:
                    continue
                _tipo_f = "Lei"
                for _tp in ["Lei Complementar", "Decreto-Lei", "Decreto", "Resolucao", "Resolucao", "Lei"]:
                    if _tp.lower() in _lei_str.lower():
                        _tipo_f = _tp
                        break
                _chave_f = f"{_tipo_f.lower()}_{_num}_{_ano}"
                if _chave_f in analisadas or _chave_f in revogadas:
                    continue
                if any(f"{l.get('tipo','').lower()}_{l.get('numero','')}_{l.get('ano','')}" == _chave_f for l in fila):
                    continue
                logs.append({"nivel": "info", "msg": f"  [FILA] Adicionando {_tipo_f} {_num}/{_ano} nivel={nivel} — {motivo}"})
                fila.append({"tipo": _tipo_f, "numero": _num, "ano": _ano, "descricao": motivo, "_nivel": nivel, "_pergunta_label": ""})
        if _nivel_atual < 2:
            _adicionar_na_fila(_altera_enc, _nivel_atual + 1, "alterada por lei atual")
            _adicionar_na_fila(_regulamenta_enc, _nivel_atual + 1, "regulamentada por lei atual")
            _adicionar_na_fila(_alterado_por_enc, _nivel_atual + 1, "altera lei atual")
            _adicionar_na_fila(_regulamentado_por_enc, _nivel_atual + 1, "regulamenta lei atual")
            # Leis citadas: avaliar contexto antes de adicionar na fila
            _cita_enc_local = locals().get('_cita_enc', [])
            for _cit_str in (_cita_enc_local or []):
                _num_c, _ano_c = _extrair_num_ano_fila(_cit_str)
                if not _num_c or not _ano_c:
                    continue
                _tipo_c = "Lei"
                for _tp in ["Lei Complementar", "Decreto-Lei", "Decreto", "Resolucao", "Lei"]:
                    if _tp.lower() in _cit_str.lower():
                        _tipo_c = _tp
                        break
                _chave_c = f"{_tipo_c.lower()}_{_num_c}_{_ano_c}"
                if _chave_c in analisadas or _chave_c in revogadas:
                    continue
                if any(f"{l.get('tipo','').lower()}_{l.get('numero','')}_{l.get('ano','')}" == _chave_c for l in fila):
                    continue
                # IA avalia se a citacao e em contexto urbanistico relevante
                try:
                    _prompt_ctx = (
                        f"No texto da {tipo} {numero}/{ano} de {municipio}/{estado}, "
                        f"a legislacao '{_cit_str}' e citada em contexto de zoneamento, zonas, subzonas, "
                        f"parametros de parcelamento do solo ou uso e ocupacao do solo?\n"
                        f"Responda APENAS: sim ou nao"
                    )
                    _resp_ctx = chamar_llm(_prompt_ctx, logs, f"Ctx cita {_num_c}")
                    if _resp_ctx and "sim" in _resp_ctx.lower():
                        logs.append({"nivel": "info", "msg": f"  [FILA] {_cit_str} citada em contexto urbanistico — adicionando nivel={_nivel_atual+1}"})
                        fila.append({"tipo": _tipo_c, "numero": _num_c, "ano": _ano_c, "descricao": "citada em contexto urbanistico", "_nivel": _nivel_atual + 1, "_pergunta_label": ""})
                    else:
                        logs.append({"nivel": "info", "msg": f"  [FILA] {_cit_str} citada mas contexto nao urbanistico — ignorando"})
                except Exception:
                    pass
            for _rev_str in (_revoga_enc or []):
                _num_r, _ano_r = _extrair_num_ano_fila(_rev_str)
                if _num_r and _ano_r:
                    for _tp in ["Lei Complementar", "Decreto-Lei", "Decreto", "Resolucao", "Lei"]:
                        if _tp.lower() in _rev_str.lower():
                            _num_r_n = _num_r.replace('.','').replace(' ','').strip()
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
            zip_nome = f"legislacoes_{mun_slug}_{est_slug}.zip"
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
                    leg_slug = _slug(f"{tipo}_{num}_{ano}")
                    base = f"{pasta}/{leg_slug}/"

                    # PDF principal
                    pdf = leg.get('pdf_path') or leg.get('caminho_pdf') or ''
                    if pdf and _os_zip.path.exists(pdf):
                        ext_p = _os_zip.path.splitext(pdf)[1] or '.pdf'
                        zf.write(pdf, base + leg_slug + ext_p)

                    # Anexos
                    for anx in (leg.get('anexos') or []):
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
                e.get('numero','') == l.get('numero','') and
                e.get('ano','') == l.get('ano','')
                for e in resultado.get('encontradas', [])
            )
        ] if 'legs' in dir() else []
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

    return resultado


def _verificar_parametros(texto, municipio, estado, tipo, numero, ano, logs, chamar_llm, modo="parametros"):
    if modo == "parametros":
        criterio = (
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
        "  \"motivo\": \"Explicacao clara sobre o que a lei trata e por que define_parametros e true ou false.\"\n"
        "}\n\n"
        "Regras:\n"
        "- define_zoneamento = true se a lei divide o municipio em zonas ou macrozonas\n"
        "- leis_referenciadas: outras leis citadas que complementem o ordenamento territorial. Se nenhuma, use []\n"
        "- No motivo, seja objetivo e claro"
    )
    resp = chamar_llm(prompt, logs, f"Verif {tipo} {numero}")
    if not resp:
        return False, []
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
            return True, leis_ref
        else:
            logs.append({"nivel": "info", "msg": f"  {motivo}"})
            return False, []
    except Exception as _e:
        logs.append({"nivel": "aviso", "msg": f"  Erro parse verificacao: {str(_e)[:60]}"})
        return False, []


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
            if html_lei:
                from bs4 import BeautifulSoup as _bs
                texto_lei = _bs(html_lei, "html.parser").get_text()
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

def _buscar_leismunicipais(municipio, estado, tipo, numero, ano, logs, chamar_llm, analisadas, modo="geral"):
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
                define, _leis_ref = _verificar_parametros(texto_lei, municipio, estado, tipo, numero, ano, logs, chamar_llm, modo=modo)
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
                        logs.append({"nivel": "aviso", "msg": "  IA: legislacao nao define parametros e nao altera outras — descartando"})
                        return None
                    # Se altera outra lei, manter mesmo sem definir parametros diretamente
            _pdf = fs_result.get("pdf_nativo_s3") or fs_result.get("pdf_path") or ""
            _anexos = fs_result.get("anexos_lm") or []
            return {"tipo": tipo, "numero": numero, "ano": ano, "link": url_enc, "pdf_path": _pdf, "html": html_lei if "html_lei" in dir() else "", "anexos_lm": _anexos, "_leis_referenciadas": _leis_ref if "_leis_ref" in dir() else []}
        logs.append({"nivel": "aviso", "msg": f"  {tipo} {numero}/{ano} nao encontrada no LeisMunicipais — tentando 1º fallback..."})
        enc = _buscar_fallback1(municipio, estado, tipo, numero, ano, logs, chamar_llm, analisadas)
        if enc:
            return enc
        logs.append({"nivel": "aviso", "msg": f"  1º fallback falhou — tentando 2º fallback (portal câmara/prefeitura)..."})
        return _buscar_fallback2(municipio, estado, tipo, numero, ano, logs, chamar_llm, analisadas)
    except Exception as e:
        logs.append({"nivel": "aviso", "msg": f"  Erro LeisMunicipais: {str(e)[:80]}"})
    return None

def _buscar_fallback1(municipio, estado, tipo, numero, ano, logs, chamar_llm, analisadas):
    """1º Fallback: busca via Google com query formal, avalia snippets, tenta até 5 resultados."""
    import urllib.parse, re as _re
    from bs4 import BeautifulSoup as _bs

    # Montar query formal dependendo do tipo
    _tipo_lower = tipo.lower()
    _eh_decreto = 'decreto' in _tipo_lower
    _tipo_label = tipo  # ex: "Decreto", "Lei Complementar", "Lei"

    if _eh_decreto:
        query_str = f'"{_tipo_label} Nº {numero}" prefeitura {municipio} {estado} {ano}'
    else:
        query_str = f'"{_tipo_label} Nº {numero}" {municipio} {estado} {ano}'

    logs.append({"nivel": "info", "msg": f"  [Fallback1] Query: {query_str}"})

    headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"}
    IGNORAR = ["leismunicipais.com.br", "legisweb.com.br", "jusbrasil.com",
               "facebook.com", "twitter.com", "instagram.com", "youtube.com",
               "wikipedia.org", "tiktok.com", "google.com", "bing.com"]

    try:
        url_g = f"https://www.google.com/search?q={urllib.parse.quote_plus(query_str)}&num=10&hl=pt-BR"
        r = requests.get(url_g, headers=headers, timeout=15)

        # Extrair resultados com snippet (título + URL + descrição)
        soup = _bs(r.text, "html.parser")
        resultados = []
        for div in soup.select("div.g, div[data-sokoban-container]"):
            a_tag = div.find("a", href=True)
            if not a_tag:
                continue
            url = a_tag["href"]
            if not url.startswith("http"):
                continue
            if any(ig in url for ig in IGNORAR):
                continue
            titulo = div.get_text(" ", strip=True)[:200]
            resultados.append({"url": url, "snippet": titulo})
        resultados = resultados[:5]

        if not resultados:
            logs.append({"nivel": "aviso", "msg": "  [Fallback1] Google não retornou resultados úteis"})
            return None

        # Avaliar snippets antes de visitar
        for i, res in enumerate(resultados):
            url = res["url"]
            snippet = res["snippet"]

            if url.lower() in analisadas:
                continue

            # Avaliação rápida do snippet — vale a pena visitar?
            snippet_lower = snippet.lower()
            tipo_ok = any(t in snippet_lower for t in [tipo.lower(), numero, ano, municipio.lower()])
            if not tipo_ok:
                logs.append({"nivel": "info", "msg": f"  [Fallback1] Resultado {i+1} ignorado pelo snippet: {snippet[:80]}"})
                continue

            analisadas.add(url.lower())
            logs.append({"nivel": "info", "msg": f"  [Fallback1] Tentando resultado {i+1}: {url[:80]}"})

            try:
                r2 = requests.get(url, headers=headers, timeout=15)
                if r2.status_code != 200:
                    continue
                texto = _bs(r2.text, "html.parser").get_text()

                # Verificar se é a legislação correta
                texto_lower = texto.lower()
                numero_ok = numero in texto_lower or numero in texto
                tipo_presente = tipo.lower() in texto_lower
                municipio_ok = municipio.lower() in texto_lower

                if numero_ok and tipo_presente:
                    logs.append({"nivel": "ok", "msg": f"  [Fallback1] Legislação encontrada: {url[:80]}"})

                    # Tentar baixar PDF se houver link
                    pdf_url = None
                    soup2 = _bs(r2.text, "html.parser")
                    for a in soup2.find_all("a", href=True):
                        href = a["href"]
                        if href.lower().endswith(".pdf"):
                            pdf_url = href if href.startswith("http") else urllib.parse.urljoin(url, href)
                            break

                    resultado = {"tipo": tipo, "numero": numero, "ano": ano, "link": url}
                    if pdf_url:
                        resultado["pdf_url"] = pdf_url
                        logs.append({"nivel": "info", "msg": f"  [Fallback1] PDF encontrado: {pdf_url[:80]}"})
                    return resultado
                else:
                    logs.append({"nivel": "info", "msg": f"  [Fallback1] Resultado {i+1} não contém a legislação esperada"})

            except Exception as e2:
                logs.append({"nivel": "aviso", "msg": f"  [Fallback1] Erro ao acessar resultado {i+1}: {str(e2)[:60]}"})

    except Exception as e:
        logs.append({"nivel": "aviso", "msg": f"  [Fallback1] Erro na busca Google: {str(e)[:80]}"})

    logs.append({"nivel": "aviso", "msg": f"  [Fallback1] {tipo} {numero}/{ano} não encontrada nos 5 primeiros resultados"})
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
        r = requests.get(url_g, headers=headers, timeout=15)
        soup = _bs(r.text, "html.parser")

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

        # Para cada domínio candidato, buscar a lei específica
        for dominio in dominios_top:
            logs.append({"nivel": "info", "msg": f"  [Fallback2] Buscando no domínio: {dominio}"})

            query_lei = f'"{tipo} {numero}" {ano} site:{dominio}'
            url_g2 = f"https://www.google.com/search?q={urllib.parse.quote_plus(query_lei)}&num=5&hl=pt-BR"

            passos = 0
            try:
                r2 = requests.get(url_g2, headers=headers, timeout=15)
                soup2 = _bs(r2.text, "html.parser")

                links_lei = []
                for a in soup2.find_all("a", href=True):
                    href = a["href"]
                    if dominio in href and href.startswith("http"):
                        links_lei.append(href)
                links_lei = list(dict.fromkeys(links_lei))[:5]

                for url_lei in links_lei:
                    if url_lei.lower() in analisadas:
                        continue
                    analisadas.add(url_lei.lower())
                    passos += 1
                    if passos > 10:
                        logs.append({"nivel": "aviso", "msg": f"  [Fallback2] Limite de 10 passos atingido em {dominio}"})
                        break

                    logs.append({"nivel": "info", "msg": f"  [Fallback2] Verificando (passo {passos}): {url_lei[:80]}"})
                    try:
                        r3 = requests.get(url_lei, headers=headers, timeout=15)
                        if r3.status_code != 200:
                            continue
                        texto = _bs(r3.text, "html.parser").get_text()
                        if numero in texto and tipo.lower() in texto.lower():
                            logs.append({"nivel": "ok", "msg": f"  [Fallback2] Legislação encontrada: {url_lei[:80]}"})
                            # Tentar PDF
                            pdf_url = None
                            soup3 = _bs(r3.text, "html.parser")
                            for a in soup3.find_all("a", href=True):
                                href = a["href"]
                                if href.lower().endswith(".pdf"):
                                    pdf_url = href if href.startswith("http") else urllib.parse.urljoin(url_lei, href)
                                    break
                            resultado = {"tipo": tipo, "numero": numero, "ano": ano, "link": url_lei}
                            if pdf_url:
                                resultado["pdf_url"] = pdf_url
                            return resultado
                    except Exception as e3:
                        logs.append({"nivel": "aviso", "msg": f"  [Fallback2] Erro passo {passos}: {str(e3)[:60]}"})

            except Exception as e2:
                logs.append({"nivel": "aviso", "msg": f"  [Fallback2] Erro ao buscar em {dominio}: {str(e2)[:60]}"})

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
