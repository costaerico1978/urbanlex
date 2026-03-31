"""
Módulo de busca automática de legislações urbanísticas por município.
"""
import re
import requests
import json as _json

def _tabela_evento(logs, municipio, estado, tipo, numero, ano, pergunta="", status="analisando",
                   altera=None, alterado_por=None, revoga=None, revogado_por=None,
                   cita=None, citado_em=None, link=None):
    """Emite evento estruturado para atualizar a tabela de legislacoes em tempo real."""
    import json as _j
    dados = {
        "municipio": municipio, "estado": estado,
        "tipo": tipo, "numero": numero, "ano": ano,
        "pergunta": pergunta, "status": status,
        "altera": altera or [], "alterado_por": alterado_por or [],
        "revoga": revoga or [], "revogado_por": revogado_por or [],
        "cita": cita or [], "citado_em": citado_em or [],
        "link": link or ""
    }
    logs.append({"nivel": "tabela", "msg": _j.dumps(dados, ensure_ascii=False)})

def buscar_legislacoes_urbanisticas(municipio, estado, logs, chamar_llm):
    resultado = {"encontradas": [], "nao_encontrada": False}
    analisadas = set()

    # ETAPA 1: 3 perguntas ao Gemini para identificar legislacoes
    PERGUNTAS = [
        f"qual legislacao define atualmente os parametros urbanisticos de {municipio} {estado}?",
        f"qual e a legislacao atual de zoneamento de {municipio} {estado}?",
        f"qual e a legislacao atual de uso e ocupacao do solo de {municipio} {estado}?",
        f"qual e atualmente a legislacao de parcelamento do solo de {municipio} {estado}?",
        f"qual e atualmente o codigo de obras de {municipio} {estado}?",
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
                dados = _json.loads(resp_c)
                if dados.get("esta_revogada"):
                    return True, dados.get("revogada_por", "legislacao mais recente")
            except Exception:
                pass
        return False, None

    # ETAPA 2: Buscar no LeisMunicipais
    for leg in legs:
        tipo = leg.get("tipo", "")
        numero = leg.get("numero", "")
        ano = leg.get("ano", "")
        descricao = leg.get("descricao", "")
        chave = f"{tipo}_{numero}_{ano}".lower()
        # 1. Verificar por chave exata
        if chave in revogadas:
            _revogador = next((l for l in legs if f"{l.get('tipo','').lower()}_{l.get('numero','').strip()}_{l.get('ano','')}" in analisadas and f"{l.get('tipo','').lower()}_{l.get('numero','').strip()}_{l.get('ano','')}" != chave), None)
            _rev_info = f"{_revogador.get('tipo','')} {_revogador.get('numero','')}/{_revogador.get('ano','')}" if _revogador else "legislacao mais recente"
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
        logs.append({"nivel": "info", "msg": f"Buscando {tipo} n {numero}/{ano} ({descricao}) no LeisMunicipais..."})
        _pergunta_origem = leg.get("_pergunta_label", "")
        _tabela_evento(logs, municipio, estado, tipo, numero, ano, pergunta=_pergunta_origem, status="analisando")
        enc = _buscar_leismunicipais(municipio, estado, tipo, numero, ano, logs, chamar_llm, analisadas, modo=leg.get("_modo_verificacao","geral"))
        if enc:
            resultado["encontradas"].append(enc)
            _altera_enc = []
            _revoga_enc = []
            # Verificar via IA se esta legislacao revoga outras da lista
            html_enc = enc.get("html", "") or ""
            if html_enc:
                from bs4 import BeautifulSoup as _bsr
                texto_enc = _bsr(html_enc, "html.parser").get_text()
            else:
                texto_enc = ""
            # Incluir texto dos anexos no texto_enc (HTML ou PDF)
            _anexos_enc = enc.get("anexos_lm") or []
            for _anx_e in _anexos_enc:
                _anx_txt = _anx_e.get("texto", "") if isinstance(_anx_e, dict) else ""
                if not _anx_txt:
                    _anx_path_e = _anx_e.get("path", "") or _anx_e.get("pdf_path", "") if isinstance(_anx_e, dict) else ""
                    if _anx_path_e and __import__('os').path.exists(_anx_path_e):
                        try:
                            import subprocess as _sp_anx
                            _res_anx = _sp_anx.run(['pdftotext', _anx_path_e, '-'], capture_output=True, text=True, timeout=30)
                            if _res_anx.returncode == 0:
                                _anx_txt = _res_anx.stdout
                        except Exception:
                            pass
                if _anx_txt:
                    texto_enc += f"\n\nANEXO:\n{_anx_txt}"
            if _anexos_enc:
                logs.append({"nivel": "info", "msg": f"  Incluindo {len(_anexos_enc)} anexo(s) na analise de relacoes"})
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
                            chave_outra = f"{outra.get('tipo','').lower()}_{num_outra}_{outra.get('ano','')}".lower()
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
                    f"2. Esta lei REVOGA outra lei? Liste todas as leis que ela revoga totalmente.\n"
                    f"3. Esta lei REGULAMENTA outra lei? Liste as leis que ela regulamenta.\n"
                    f"4. Esta lei e ALTERADA por alguma lei mais recente mencionada no texto? Liste.\n"
                    f"5. Esta lei e REVOGADA por alguma lei mais recente mencionada no texto? Liste.\n"
                    f"6. Esta lei e REGULAMENTADA por alguma lei mencionada no texto? Liste.\n\n"
                    f"Use formato 'Tipo Numero/Ano' para cada lei (ex: 'Lei Complementar 270/2024').\n"
                    f"Responda APENAS com JSON:\n"
                    f'{{\n'
                    f'  "altera": ["Lei X/ano"],\n'
                    f'  "revoga": ["Lei X/ano"],\n'
                    f'  "regulamenta": ["Lei X/ano"],\n'
                    f'  "alterado_por": ["Lei X/ano"],\n'
                    f'  "revogado_por": ["Lei X/ano"],\n'
                    f'  "regulamentado_por": ["Lei X/ano"]\n'
                    f'}}\n\n'
                    f"TEXTO:\n{texto_enc[:6000]}"
                )
                resp_rel = chamar_llm(prompt_rel, logs, f"Relacoes {tipo} {numero}")
                if resp_rel:
                    import re as _re_rel
                    resp_rel_c = _re_rel.sub(r"^```json\s*|\s*```$", "", (resp_rel or "").strip())
                    dados_rel = _json.loads(resp_rel_c)
                    _altera_enc = dados_rel.get("altera", [])
                    _revoga_enc = dados_rel.get("revoga", [])
                    _regulamenta_enc = dados_rel.get("regulamenta", [])
                    _alterado_por_enc = dados_rel.get("alterado_por", [])
                    _revogado_por_enc = dados_rel.get("revogado_por", [])
                    _regulamentado_por_enc = dados_rel.get("regulamentado_por", [])
                    logs.append({"nivel": "ok", "msg": f"  [RELACOES] altera={_altera_enc} revoga={_revoga_enc} regulamenta={_regulamenta_enc} alterado_por={_alterado_por_enc}", "nivel": "relacao"})
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
        # Emitir evento final com todos os relacionamentos (sempre, independente da analise)
        _tabela_evento(logs, municipio, estado,
            enc.get('tipo', tipo), enc.get('numero', numero), enc.get('ano', ano),
            pergunta=_pergunta_origem, status="encontrada",
            altera=_altera_enc, alterado_por=_alterado_por_enc,
            revoga=_revoga_enc, revogado_por=_revogado_por_enc,
            cita=_regulamenta_enc, citado_em=_regulamentado_por_enc,
            link=enc.get('link',''))

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

    for idx, (label, keywords) in enumerate(zip(LABELS_PERGUNTAS, KEYWORDS_PERGUNTAS)):
        # Buscar legislacao correspondente
        leg_match = None
        for enc in resultado["encontradas"]:
            desc = (enc.get("descricao", "") + " " + enc.get("tipo", "")).lower()
            if any(kw in desc for kw in keywords):
                leg_match = enc
                break
        if not leg_match and idx == 0 and resultado["encontradas"]:
            leg_match = resultado["encontradas"][0]

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
        dados = _json.loads(resp_c)
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
                    texto_lei = _bs(html_lei, "html.parser").get_text()
                    define, _leis_ref = _verificar_parametros(texto_lei, municipio, estado, tipo, numero, ano, logs, chamar_llm, modo=modo)
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
                if _verificar_parametros(texto, municipio, estado, tipo_u, num_u, ano_u, logs, chamar_llm, modo="geral")[0]:
                    return {"tipo": tipo_u, "numero": num_u, "ano": ano_u, "link": link}
            except Exception as e2:
                logs.append({"nivel": "aviso", "msg": f"  Erro link: {str(e2)[:50]}"})
    except Exception as e:
        logs.append({"nivel": "aviso", "msg": f"Erro Google: {str(e)[:60]}"})
    return None
