"""
Helpers para o Gerador de Planilha — estrategia hibrida.

Funcoes:
- chamar_ia(...)           : encapsula chamada streaming + heartbeat + extracao texto
- extrair_json(...)        : parse robusto com fallback
- filtrar_pdfs_para_zona(...) : filtra files_content baseado em mapa de identificacao
- gerar_prompt_passada(...): gera prompt especifico de cada passada (0, 1, 2, 3)
"""
import json
import time
import re
import threading
import base64

# ============================================================
# Funcao util: chamar IA com streaming + heartbeat
# ============================================================
def chamar_ia_com_retry(client, provedor, modelo, prompt_text, pdf_anexos, job_logs, label='IA',
                         max_tentativas=5, intervalo_base=15):
    """
    Wrapper de chamar_ia com retry automatico em erros transitorios.

    Erros considerados transitorios e que serao reprocessados:
    - 503 ServiceUnavailable (Gemini overload)
    - 429 RateLimit
    - 500/502/504 (server errors)
    - timeout, connection reset, network errors

    Erros NAO transitorios (propagam direto): JSON parse error, auth error, validation error.

    Backoff: intervalo_base * tentativa (linear). Padrao: 15s, 30s, 45s, 60s, 75s.
    """
    import time as _t
    erros_transitorios = ['503', '429', '500', '502', '504', 'overload',
                          'high demand', 'rate limit', 'timeout', 'unavailable',
                          'connection reset', 'network', 'temporarily',
                          'try again', 'try later', 'deadline exceeded']
    ultima_excecao = None
    for tentativa in range(1, max_tentativas + 1):
        try:
            return chamar_ia(client, provedor, modelo, prompt_text, pdf_anexos, job_logs, label)
        except Exception as e:
            ultima_excecao = e
            msg_erro = str(e).lower()
            transitorio = any(t in msg_erro for t in erros_transitorios)
            if not transitorio:
                # Erro nao transitorio: propaga imediato
                job_logs.append({'nivel':'erro','msg':f'❌ {label}: erro nao-transitorio, abortando: {str(e)[:200]}'})
                raise
            if tentativa >= max_tentativas:
                # Esgotou tentativas
                job_logs.append({'nivel':'erro','msg':f'❌ {label}: esgotou {max_tentativas} tentativas: {str(e)[:200]}'})
                raise
            espera = intervalo_base * tentativa
            job_logs.append({'nivel':'aviso','msg':f'⚠ {label}: erro transitorio (tentativa {tentativa}/{max_tentativas}), aguardando {espera}s antes de retry: {str(e)[:150]}'})
            _t.sleep(espera)
    # Nao deveria chegar aqui
    if ultima_excecao:
        raise ultima_excecao


def chamar_ia(client, provedor, modelo, prompt_text, pdf_anexos, job_logs, label='IA'):
    """
    Chama IA (Gemini ou Anthropic) com streaming e heartbeat de 15s.
    
    pdf_anexos: lista de dicts {title, data_b64} (somente PDFs ja validados)
    job_logs: a lista LogList do job
    label: prefixo para os logs (ex: "Passada 0", "Passada 1")
    
    Retorna: texto da resposta (string).
    Em caso de erro, propaga a excecao.
    """
    job_logs.append({'nivel':'info','msg':f'⏳ {label}: Aguardando IA (streaming)...'})
    t_ini = time.time()
    txt = ''
    chars = 0
    chunks = 0
    hb_state = {'stop': False, 'chars': 0}
    
    def heartbeat():
        while not hb_state['stop']:
            time.sleep(15)
            if hb_state['stop']:
                break
            dec = time.time() - t_ini
            job_logs.append({'nivel':'info','msg':f'💓 {label}: Aguardando... {dec:.0f}s decorridos, {hb_state["chars"]} chars recebidos'})
    
    th = threading.Thread(target=heartbeat, daemon=True)
    th.start()
    
    try:
        if provedor == 'anthropic':
            # Anthropic: documents como base64 em messages
            content = []
            for p in pdf_anexos:
                content.append({
                    'type': 'document',
                    'source': {'type': 'base64', 'media_type': 'application/pdf', 'data': p['data_b64']},
                    'title': p.get('title', '?')
                })
            content.append({'type': 'text', 'text': prompt_text})
            msgs = [{'role': 'user', 'content': content}]
            with client.messages.stream(model=modelo, max_tokens=64000, messages=msgs) as stream:
                for delta in stream.text_stream:
                    txt += delta
                    chunks += 1
                    chars += len(delta)
                    hb_state['chars'] = chars
        else:
            # Gemini: lista de partes (text + inline data)
            parts = [prompt_text]
            for p in pdf_anexos:
                try:
                    data = base64.b64decode(p['data_b64'])
                    parts.append({'mime_type': 'application/pdf', 'data': data})
                except Exception:
                    pass
            stream = client.generate_content(parts, stream=True, request_options={'timeout': 300})
            for chunk in stream:
                try:
                    piece = chunk.text if hasattr(chunk, 'text') else ''
                except Exception:
                    piece = ''
                if piece:
                    txt += piece
                    chunks += 1
                    chars += len(piece)
                    hb_state['chars'] = chars
        hb_state['stop'] = True
        dec = time.time() - t_ini
        job_logs.append({'nivel':'ok','msg':f'✅ {label}: IA terminou em {dec:.1f}s ({chars} chars, {chunks} chunks)'})
        return txt
    except Exception as e:
        hb_state['stop'] = True
        dec = time.time() - t_ini
        job_logs.append({'nivel':'erro','msg':f'❌ {label}: Falha apos {dec:.1f}s: {type(e).__name__}: {e}'})
        raise


# ============================================================
# Funcao util: extracao robusta de JSON
# ============================================================
def extrair_json(txt):
    """
    Tenta extrair um objeto JSON valido de uma string que pode conter
    markdown, comentarios, prefixos ou sufixos.
    
    Retorna: dict ou None se falhar.
    """
    if not txt:
        return None
    # Remover possiveis blocos de codigo
    cleaned = re.sub(r'```(?:json)?\s*', '', txt)
    cleaned = cleaned.replace('```', '')
    # Tentar achar o primeiro { e o ultimo } correspondentes
    s = cleaned.find('{')
    e = cleaned.rfind('}')
    if s < 0 or e < 0 or e <= s:
        return None
    candidate = cleaned[s:e+1]
    # Tentativa 1: parse direto
    try:
        return json.loads(candidate)
    except json.JSONDecodeError:
        pass
    # Tentativa 2: remover comentarios estilo // e /* */
    no_comments = re.sub(r'//[^\n]*', '', candidate)
    no_comments = re.sub(r'/\*.*?\*/', '', no_comments, flags=re.DOTALL)
    try:
        return json.loads(no_comments)
    except json.JSONDecodeError:
        pass
    # Tentativa 3: trocar aspas simples por duplas (caso a IA tenha usado aspas erradas)
    try:
        return json.loads(candidate.replace("'", '"'))
    except json.JSONDecodeError:
        return None


# ============================================================
# Funcao util: filtrar PDFs para uma zona especifica
# ============================================================
def filtrar_pdfs_para_zona(leis_aplicaveis, mapa_arquivos, todos_anexos):
    """
    Dada a lista de leis_aplicaveis (strings como "LC 270/2024") e o
    mapa_arquivos {nome_arquivo: identificacao}, retorna a lista de
    pdf_anexos (subset de todos_anexos) cujas identificacoes batem.
    
    leis_aplicaveis: list[str]
    mapa_arquivos: dict {nome_arquivo: identificacao}
    todos_anexos: list[{title, data_b64, nome_arquivo}]
    
    Retorna: list[anexo] filtrado. Se nada bater, retorna lista vazia.
    """
    # Normalizar leis_aplicaveis
    leis_norm = set()
    for l in leis_aplicaveis or []:
        n = re.sub(r'\s+', ' ', str(l).strip().upper())
        leis_norm.add(n)
    
    # Mapear identificacao -> nome_arquivo
    id_to_nome = {}
    for nome, ident in (mapa_arquivos or {}).items():
        n = re.sub(r'\s+', ' ', str(ident).strip().upper())
        id_to_nome[n] = nome
    
    # Achar nomes correspondentes
    nomes_relevantes = set()
    for lei in leis_norm:
        if lei in id_to_nome:
            nomes_relevantes.add(id_to_nome[lei])
        else:
            # fallback: busca parcial
            for ident_norm, nome in id_to_nome.items():
                if lei in ident_norm or ident_norm in lei:
                    nomes_relevantes.add(nome)
                    break
    
    # Filtrar anexos
    filtrados = [a for a in todos_anexos if a.get('nome_arquivo') in nomes_relevantes]
    return filtrados


# ============================================================
# Geradores de prompt para cada passada
# ============================================================
def prompt_passada_0_catalogacao_avancada(nomes_arquivos=None, metadata=None):
    """
    Versao avancada da P0 para a arquitetura PDF-driven.
    Alem de identificar cada PDF, extrai:
    - data de publicacao
    - hierarquia juridica
    - data de vigencia (inicio/fim)
    - abrangencia (municipal/AP especifica)
    - tipo de atuacao (principal/modificadora/regulamentadora/errata)
    - leis modificadas com escopo detalhado
    """
    nomes_arquivos = nomes_arquivos or []
    lista_nomes = '\n'.join(f'  {i+1}. {n}' for i, n in enumerate(nomes_arquivos))
    return (
        '\n\n=== INSTRUCAO DESTA EXECUCAO — PASSADA 0 (AVANCADA): CATALOGACAO ===\n'
        'Esta e a Passada 0. NAO preencha a planilha.\n\n'
        'Os PDFs anexados, NA ORDEM em que foram enviados, correspondem a estes nomes:\n\n'
        + lista_nomes + '\n\n'
        'Para cada PDF, leia cabecalho/ementa/preambulo + artigos finais (revogacao) e identifique:\n\n'
        '1. IDENTIFICACAO BASICA:\n'
        '   - tipo de ato (lei_complementar, lei_ordinaria, decreto, portaria, errata, retificacao)\n'
        '   - identificacao formal: "LC NNN/AAAA", "Dec NNNNN/AAAA", "Errata LC NNN/AAAA"\n'
        '   - municipio e estado\n'
        '   - data de publicacao (formato YYYY-MM-DD se possivel)\n\n'
        '2. HIERARQUIA E VIGENCIA:\n'
        '   - hierarquia_juridica: 1=Constituicao, 2=LC, 3=LO, 4=Decreto, 5=Errata, 6=Portaria\n'
        '   - data_vigencia_inicio: quando passou a valer (data publicacao + vacatio se houver)\n'
        '   - data_vigencia_fim: null se ainda valida; data se foi revogada totalmente\n\n'
        '3. ABRANGENCIA:\n'
        '   - abrangencia: "municipal_total" | "ap_especifica" | "regiao" | "bairro" | "zona_especifica"\n'
        '   - ap_atingidas: lista de APs/Regioes/Bairros se nao for municipal_total\n\n'
        '4. TIPO DE ATUACAO:\n'
        '   - "principal": lei autonoma que cria zoneamento ou parametros\n'
        '   - "modificadora": lei que altera/revoga partes de outra\n'
        '   - "regulamentadora": decreto que regulamenta lei superior\n'
        '   - "errata": correcao formal de erro material\n\n'
        '5. LEIS MODIFICADAS (CRITICO — revogacao parcial):\n'
        '   Para cada lei que ESTA lei modifica/revoga, retornar:\n'
        '   - alvo: identificacao da lei alvo (ex: "LC 270/2024")\n'
        '   - tipo_modificacao: "revogacao_total" | "revogacao_parcial" | "alteracao" | "errata"\n'
        '   - escopo: lista de partes especificamente afetadas, cada uma com:\n'
        '     * dispositivo: "Art. NN", "Art. NN §M", "Tabela X do Anexo Y", "Anexo XV"\n'
        '     * geografia: "todas" | "AP-X" | "RP-Y" | nome de bairro/regiao | nome de zona\n'
        '     * uso: "todos" | "Residencial" | "Comercial" | etc\n\n'
        'IMPORTANTE: detecte revogacao IMPLICITA tambem. Se a lei nova tem um Anexo XXI com\n'
        'a mesma finalidade do Anexo XV de uma lei anterior, marque como substituicao\n'
        '(tipo_modificacao: "revogacao_parcial" sobre o Anexo XV).\n\n'
        'Use o nome de arquivo EXATO da lista acima (mesma posicao do PDF anexado).\n\n'
        'Retorne SOMENTE este JSON, sem markdown, sem comentarios:\n'
        '{\n'
        '  "arquivos": [\n'
        '    {\n'
        '      "nome_arquivo": "LC_281_2025.pdf",\n'
        '      "identificacao": "LC 281/2025",\n'
        '      "tipo": "lei_complementar",\n'
        '      "data": "2025-03-15",\n'
        '      "hierarquia_juridica": 2,\n'
        '      "data_vigencia_inicio": "2025-04-15",\n'
        '      "data_vigencia_fim": null,\n'
        '      "abrangencia": "municipal_total",\n'
        '      "ap_atingidas": [],\n'
        '      "tipo_atuacao": "modificadora",\n'
        '      "leis_modificadas": [\n'
        '        {\n'
        '          "alvo": "LC 270/2024",\n'
        '          "tipo_modificacao": "revogacao_parcial",\n'
        '          "escopo": [\n'
        '            {"dispositivo": "Art. 47", "geografia": "todas", "uso": "todos"},\n'
        '            {"dispositivo": "Tabela XV do Anexo II", "geografia": "AP-1, AP-2.1", "uso": "todos"}\n'
        '          ]\n'
        '        }\n'
        '      ]\n'
        '    }\n'
        '  ]\n'
        '}'
    )


def prompt_passada_0_catalogacao(nomes_arquivos=None, metadata=None):
    metadata = metadata or DEFAULT_METADATA
    nomes_arquivos = nomes_arquivos or []
    chave_cat = metadata.get('chave_catalogacao', 'arquivos')
    estr = metadata.get('estrutura_catalogacao', {}) or {}
    k_nome = estr.get('nome_arquivo', 'nome_arquivo')
    k_ident = estr.get('identificacao', 'identificacao')
    k_escopo = estr.get('escopo', 'escopo')
    lista_nomes = '\n'.join(f'  {i+1}. {n}' for i, n in enumerate(nomes_arquivos))
    return (
        '\n\n=== INSTRUCAO DESTA EXECUCAO — PASSADA 0: CATALOGACAO ===\n'
        'Esta e a Passada 0. NAO preencha a planilha. Os PDFs anexados, NA ORDEM em que foram '
        'enviados, correspondem aos seguintes nomes de arquivo:\n\n'
        + lista_nomes + '\n\n'
        'Para cada PDF, leia apenas o cabecalho/ementa/preambulo e identifique:\n'
        '  - tipo de ato (Lei Complementar, Lei Ordinaria, Decreto, Portaria, Errata, Retificacao)\n'
        '  - numero e ano (ex: "LC 270/2024", "Dec 52585/2023")\n'
        '  - municipio e estado\n'
        '  - escopo geografico (municipio inteiro, AP especifica, regiao, bairro, zona, etc.)\n\n'
        'Use o nome de arquivo EXATO da lista acima (mesma posicao que o PDF foi anexado). '
        'Nao invente nomes como "anexo_1.pdf" — use os nomes da lista.\n\n'
        'Retorne SOMENTE este JSON, sem markdown, sem comentarios:\n'
        '{\n'
        f'  "{chave_cat}": [\n'
        f'    {{"{k_nome}": "LC_270_2024.pdf", "{k_ident}": "LC 270/2024", "{k_escopo}": "Municipio inteiro"}},\n'
        '    ...\n'
        '  ]\n'
        '}'
    )


def prompt_passada_1_inventario(prompt_usuario, metadata=None):
    metadata = metadata or DEFAULT_METADATA
    chave_inv = metadata.get('chave_inventario', 'zonas_canonicas')
    estr = metadata.get('estrutura_inventario', {}) or {}
    k_nome = estr.get('nome_canonico', 'nome_canonico')
    k_var = estr.get('variantes_observadas', 'variantes_observadas')
    k_ut = estr.get('unidade_territorial', 'unidade_territorial')
    k_leis = estr.get('leis_aplicaveis', 'leis_aplicaveis')
    return prompt_usuario + (
        '\n\n=== INSTRUCAO DESTA EXECUCAO — PASSADA 1: INVENTARIO ===\n'
        'Esta e a Passada 1. NAO preencha a planilha. Aplique todas as etapas e regras de '
        'identificacao, mapeamento espacial multinivel, revogacao parcial, granularidade fina '
        'e normalizacao de nomenclaturas descritas no prompt acima. Faca o inventario completo.\n\n'
        'Retorne SOMENTE este JSON, sem markdown, sem comentarios:\n'
        '{\n'
        f'  "{chave_inv}": [\n'
        '    {\n'
        f'      "{k_nome}": "ZRM2-A",\n'
        f'      "{k_var}": ["ZRM2A", "ZRM-2A"],\n'
        f'      "{k_ut}": "AP-2.1",\n'
        f'      "{k_leis}": ["LC 270/2024", "Dec 52585/2023"]\n'
        '    }\n'
        '  ],\n'
        '  "alertas": ["..."]\n'
        '}\n\n'
        'IMPORTANTE: liste TODAS as combinacoes unicas. Uma mesma zona em unidades territoriais '
        'distintas e um item separado. Use ' + k_leis + ' com a identificacao exata de cada lei '
        '(ex: "LC 270/2024"), mesmo formato da Passada 0.'
    )


def prompt_passada_2_pdf_driven_principal(prompt_usuario, lei_identificacao, zonas_canonicas,
                                            headers, instrucao_revogacao, estado_atual_resumo, metadata=None):
    """
    P2 PDF-driven (chamada principal): para cada PDF, preenche todas as celulas
    que esta lei especifica define, em todas as zonas que ela cobre.
    
    Recebe:
    - prompt_usuario: prompt v8 do usuario (regras gerais)
    - lei_identificacao: ex "LC 281/2025"
    - zonas_canonicas: lista das zonas identificadas na P1
    - headers: nomes EXATOS dos cabecalhos da planilha
    - instrucao_revogacao: texto de revogacoes ja aplicadas a esta lei (do vigencia.py)
    - estado_atual_resumo: resumo do que ja foi preenchido (zona -> [colunas])
    """
    metadata = metadata or {}
    chave_zona = metadata.get('chave_zona_individual', 'linhas')
    campo_ni = metadata.get('campo_sem_info', 'NI')
    
    headers_lista = '\n'.join(f'  • "{h}"' for h in headers[:50])
    if len(headers) > 50:
        headers_lista += f'\n  ... e mais {len(headers)-50} colunas (use o nome EXATO de cada cabecalho)'
    
    zonas_str = '\n'.join(f'  • {z.get("nome_canonico","?")} (em {z.get("unidade_territorial","?")})'
                          for z in zonas_canonicas[:50])
    if len(zonas_canonicas) > 50:
        zonas_str += f'\n  ... e mais {len(zonas_canonicas)-50} zonas'
    
    estado_str = '(planilha vazia, esta e a primeira lei)' if not estado_atual_resumo else \
                 '\n'.join(f'  • {z}: {len(cols)} colunas ja preenchidas' for z, cols in list(estado_atual_resumo.items())[:30])
    
    return prompt_usuario + (
        f'\n\n=== INSTRUCAO DESTA EXECUCAO — PASSADA 2 (PDF-DRIVEN PRINCIPAL) ===\n'
        f'Esta e a Passada 2. Voce esta lendo APENAS a lei "{lei_identificacao}".\n\n'
        f'TAREFA: Para CADA zona/unidade territorial cobertas POR ESTA LEI, preencha\n'
        f'TODAS as colunas da planilha que esta lei especifica define.\n\n'
        f'PRINCIPIO FUNDAMENTAL — SEJA AGRESSIVO NA EXTRACAO:\n'
        f'• Sua tarefa e EXTRAIR TODOS OS DADOS POSSIVEIS desta lei.\n'
        f'• Para CADA zona/UT mencionada, preencha o MAXIMO de colunas possiveis.\n'
        f'• Se a lei tem TABELA de parametros, transcreva CADA celula da tabela.\n'
        f'• Se a lei lista permissoes de uso (residencial, comercial, etc), preencha as colunas Sim/Nao para CADA uso.\n'
        f'• Se a lei define lote minimo, testada, CA, TO, gabarito, afastamentos — PREENCHA esses campos.\n'
        f'• Se a lei tem ANEXOS com tabelas, EXTRAIA TODOS os anexos.\n'
        f'• Se a lei lista regras especiais por logradouro/via/face de quadra, RETORNE LINHAS SEPARADAS para cada caso.\n\n'
        f'QUANDO OMITIR COLUNAS:\n'
        f'• Omita SOMENTE quando a lei genuinamente NAO TOCA no assunto da coluna em momento algum.\n'
        f'• Se ha qualquer mencao implicita ou explicita ao parametro, PREENCHA.\n'
        f'• Em duvida, prefira preencher (com NI ou valor) a omitir.\n'
        f'• Para zonas que esta lei NAO MENCIONA, nao retorne linha dessa zona.\n\n'
        f'COMPORTAMENTO ESPERADO:\n'
        f'• Lei urbanistica completa (LC de zoneamento) deve gerar 50-150 colunas preenchidas POR LINHA.\n'
        f'• Lei pequena (Decreto regulamentador) deve gerar 10-30 colunas POR LINHA.\n'
        f'• Errata/Retificacao deve gerar 1-5 colunas (so o que ela corrige).\n'
        f'• Se voce esta retornando menos que isso, REVISE — provavelmente esqueceu campos.\n\n'
        f'{instrucao_revogacao}\n'
        f'\n=== ZONAS CANONICAS DESTE MUNICIPIO ===\n{zonas_str}\n\n'
        f'=== HEADERS EXATOS DA PLANILHA ===\n{headers_lista}\n\n'
        f'=== ESTADO ATUAL DA PLANILHA (ja preenchido por leis anteriores) ===\n{estado_str}\n\n'
        f'OBSERVACOES SOBRE O ESTADO ATUAL:\n'
        f'• Voce PODE preencher colunas que ja estao preenchidas em zonas — o sistema decidira\n'
        f'  por prioridade (lei mais recente vence).\n'
        f'• Mas FOQUE em colunas que ainda nao foram preenchidas e que ESTA lei define.\n\n'
        f'Retorne SOMENTE este JSON, sem markdown, sem comentarios:\n'
        '{\n'
        f'  "{chave_zona}": [\n'
        '    {\n'
        '      "Zona Urbana": "<nome canonico>",\n'
        '      "Subzona Urbana": "<letra/codigo>",\n'
        '      "Area_Planejamento": "<UT>",\n'
        '      "<header_exato>": "<valor>",\n'
        '      ...\n'
        '    }\n'
        '  ]\n'
        '}\n\n'
        f'Cada chave do dicionario deve ser EXATAMENTE um header da planilha listado acima.\n'
        f'Para campos sem informacao na lei, OMITA a chave (nao use "{campo_ni}" aqui — omita).\n'
        f'Para colunas calculadas (Parte 7 do prompt), OMITA tambem.\n'
        f'Para granularidade fina (logradouros, faces de quadra), retorne MULTIPLAS linhas\n'
        f'da mesma zona com Divisoes diferentes.'
    )


def prompt_passada_2_pdf_driven_verificacao(prompt_usuario, lei_identificacao, resultado_principal,
                                             zonas_canonicas, headers, instrucao_revogacao, metadata=None):
    """
    P2 PDF-driven (chamada de verificacao): receba resultado da chamada principal
    e identifique o que foi ESQUECIDO. So retorna omissoes.
    """
    metadata = metadata or {}
    chave_zona = metadata.get('chave_zona_individual', 'linhas')
    
    import json as _json
    resultado_str = _json.dumps(resultado_principal, ensure_ascii=False, indent=2)
    if len(resultado_str) > 30000:
        resultado_str = resultado_str[:30000] + '\n... (truncado)'
    
    headers_lista = '\n'.join(f'  • "{h}"' for h in headers[:50])
    if len(headers) > 50:
        headers_lista += f'\n  ... e mais {len(headers)-50}'
    
    return prompt_usuario + (
        f'\n\n=== INSTRUCAO DESTA EXECUCAO — PASSADA 2 (VERIFICACAO DE OMISSOES) ===\n'
        f'Esta e a Passada 2 (segunda chamada). Voce esta lendo APENAS a lei "{lei_identificacao}".\n\n'
        f'A chamada anterior preencheu o seguinte (consulte abaixo). Sua tarefa AGORA e:\n'
        f'1. Identificar TUDO que a lei "{lei_identificacao}" define mas nao foi preenchido na chamada anterior.\n'
        f'2. Identificar zonas que a lei menciona mas nao apareceram no resultado.\n'
        f'3. Identificar colunas que esta lei define para zonas que ja apareceram, mas nao foram preenchidas.\n\n'
        f'{instrucao_revogacao}\n'
        f'\n=== RESULTADO DA CHAMADA ANTERIOR ===\n{resultado_str}\n\n'
        f'=== HEADERS EXATOS DA PLANILHA ===\n{headers_lista}\n\n'
        f'Retorne SOMENTE as OMISSOES. Use o mesmo formato JSON da chamada anterior:\n'
        '{\n'
        f'  "{chave_zona}": [\n'
        '    {\n'
        '      "Zona Urbana": "<zona>",\n'
        '      "Area_Planejamento": "<UT>",\n'
        '      "<header_exato>": "<valor que foi esquecido>"\n'
        '    }\n'
        '  ]\n'
        '}\n\n'
        f'Se nada foi esquecido, retorne: {{"{chave_zona}": []}}\n'
        f'Nao repita o que ja foi preenchido. So omissoes.'
    )


def prompt_passada_2_zona(prompt_usuario, zona_canonica, unidade_territorial, headers, metadata=None):
    metadata = metadata or DEFAULT_METADATA
    chave_zona = metadata.get('chave_zona_individual', 'linhas')
    campo_ni = metadata.get('campo_sem_info', 'NI')
    campo_calc = metadata.get('campo_calculado', '')
    headers_lista = '\n'.join(f'  {i+1}. "{h}"' for i, h in enumerate(headers))
    return prompt_usuario + (
        f'\n\n=== INSTRUCAO DESTA EXECUCAO — PASSADA 2: PREENCHER ZONA ESPECIFICA ===\n'
        f'Esta e a Passada 2. Os PDFs anexados sao EXCLUSIVAMENTE os relevantes para esta zona. '
        f'Aplique todas as regras do prompt acima.\n\n'
        f'Zona canonica a preencher: {zona_canonica}\n'
        f'Unidade territorial: {unidade_territorial}\n\n'
        f'Preencha APENAS uma linha (a desta zona/unidade territorial). Se a zona possui '
        f'subdivisoes finas (logradouro, lado, faixa de numeracao, etc.), retorne MULTIPLAS '
        f'linhas, uma para cada subdivisao.\n\n'
        f'Retorne SOMENTE este JSON, sem markdown, sem comentarios:\n'
        '{\n'
        f'  "{chave_zona}": [\n'
        '    {"<cabecalho>": "<valor>", ...},\n'
        '    ...\n'
        '  ]\n'
        '}\n\n'
        f'Cada chave deve ser EXATAMENTE um dos cabecalhos da planilha listados abaixo (mesma '
        f'grafia, mesmas maiusculas, mesmos acentos):\n\n{headers_lista}\n\n'
        f'Para campos sem informacao na lei, use "{campo_ni}". Para colunas de calculo automatico, '
        f'retorne string vazia "{campo_calc}".'
    )


def prompt_passada_3_validacao(json_consolidado, zonas_canonicas, metadata=None):
    metadata = metadata or DEFAULT_METADATA
    chave_falt = metadata.get('chave_validacao', 'linhas_faltantes')
    estr = metadata.get('estrutura_inventario', {}) or {}
    k_nome = estr.get('nome_canonico', 'nome_canonico')
    k_ut = estr.get('unidade_territorial', 'unidade_territorial')
    return (
        '\n\n=== INSTRUCAO DESTA EXECUCAO — PASSADA 3: VALIDACAO FINAL ===\n'
        'Esta e a Passada 3. Os PDFs anexados sao TODOS do compilado. Abaixo esta o JSON '
        'consolidado das zonas ja preenchidas nas passadas anteriores.\n\n'
        'Sua tarefa: verificar se alguma zona/unidade territorial ficou de fora ou se ha '
        'inconsistencias graves (ex: revogacao parcial nao aplicada, zona com mesmo nome em '
        'unidade territorial diferente nao listada).\n\n'
        'Lista de zonas que FORAM preenchidas:\n'
        + '\n'.join(f'  - {z.get(k_nome,"?")} ({z.get(k_ut,"?")})' for z in zonas_canonicas) +
        '\n\nJSON consolidado:\n' + json_consolidado +
        '\n\nRetorne SOMENTE este JSON, sem markdown, sem comentarios:\n'
        '{\n'
        f'  "{chave_falt}": [\n'
        '    {"<cabecalho>": "<valor>", ...}\n'
        '  ],\n'
        '  "alertas": ["..."]\n'
        '}\n\n'
        f'Se nada faltar, retorne "{chave_falt}": [] e "alertas": [].'
    )


# ============================================================
# Metadata YAML do prompt
# ============================================================
DEFAULT_METADATA = {
    'versao': 0,
    'chave_inventario': 'zonas_canonicas',
    'chave_zona_individual': 'linhas',
    'chave_validacao': 'linhas_faltantes',
    'chave_catalogacao': 'arquivos',
    'campo_sem_info': 'NI',
    'campo_calculado': '',
    'estrutura_inventario': {
        'nome_canonico': 'nome_canonico',
        'variantes_observadas': 'variantes_observadas',
        'unidade_territorial': 'unidade_territorial',
        'leis_aplicaveis': 'leis_aplicaveis'
    },
    'estrutura_catalogacao': {
        'nome_arquivo': 'nome_arquivo',
        'identificacao': 'identificacao',
        'escopo': 'escopo'
    }
}


def extrair_metadata_yaml(texto_prompt):
    """
    Extrai bloco YAML do inicio do texto do prompt.
    Aceita 3 formatos: bloco delimitado, fence yaml, ou apenas a chave inicial.
    Retorna dict com metadata. Em caso de erro, retorna DEFAULT_METADATA.
    """
    import yaml as _yaml
    import re as _re
    if not texto_prompt:
        return dict(DEFAULT_METADATA)

    yaml_text = None

    # Formato 2: dentro de fence yaml com tag URBANLEX_METADATA
    m = _re.search(
        r'```\s*yaml\s*URBANLEX_METADATA\s*\n(.*?)\n\s*```',
        texto_prompt, _re.DOTALL | _re.IGNORECASE
    )
    if m:
        yaml_text = m.group(1)

    # Formato 1: blocos delimitados
    if not yaml_text:
        m = _re.search(
            r'URBANLEX_METADATA:\s*\n(.*?)\nURBANLEX_METADATA_FIM',
            texto_prompt, _re.DOTALL | _re.IGNORECASE
        )
        if m:
            yaml_text = m.group(1)

    # Formato 3: chave URBANLEX_METADATA: + linhas indentadas seguintes
    if not yaml_text:
        m = _re.search(
            r'URBANLEX_METADATA:\s*\n((?:[ \t].*\n?)+)',
            texto_prompt
        )
        if m:
            yaml_text = m.group(1)

    if not yaml_text:
        return dict(DEFAULT_METADATA)

    try:
        # Remover possivel indentacao comum
        lines = yaml_text.split('\n')
        non_empty = [l for l in lines if l.strip()]
        if non_empty:
            min_indent = min(len(l) - len(l.lstrip()) for l in non_empty)
            yaml_text = '\n'.join(
                l[min_indent:] if len(l) >= min_indent else l for l in lines
            )

        parsed = _yaml.safe_load(yaml_text)
        if not isinstance(parsed, dict):
            return dict(DEFAULT_METADATA)

        # Mesclar com defaults
        result = dict(DEFAULT_METADATA)
        for k, v in parsed.items():
            if isinstance(v, dict) and isinstance(result.get(k), dict):
                merged = dict(result[k])
                merged.update(v)
                result[k] = merged
            else:
                result[k] = v
        return result
    except Exception:
        return dict(DEFAULT_METADATA)


def remover_bloco_yaml(texto_prompt):
    """
    Retorna o texto do prompt sem o bloco YAML, para enviar a IA apenas a prosa.
    """
    import re as _re
    if not texto_prompt:
        return texto_prompt
    cleaned = texto_prompt
    cleaned = _re.sub(
        r'```\s*yaml\s*URBANLEX_METADATA\s*\n.*?\n\s*```',
        '', cleaned, flags=_re.DOTALL | _re.IGNORECASE
    )
    cleaned = _re.sub(
        r'URBANLEX_METADATA:\s*\n.*?\nURBANLEX_METADATA_FIM\s*\n?',
        '', cleaned, flags=_re.DOTALL | _re.IGNORECASE
    )
    cleaned = _re.sub(
        r'URBANLEX_METADATA:\s*\n(?:[ \t].*\n?)+',
        '', cleaned
    )
    return cleaned.strip()

# ============================================================
# Merge nao-destrutivo de resultados
# ============================================================
def chave_zona_normalizada(linha, metadata=None):
    """
    Gera uma chave unica para a linha baseada em zona+UT+subzona+divisoes.
    Usada para identificar a mesma linha em chamadas P2 sucessivas.
    """
    metadata = metadata or {}
    estr = metadata.get('estrutura_inventario', {}) or {}
    
    # Tentar varios nomes possiveis
    candidatos = {
        'zona': ['Zona Urbana', estr.get('nome_canonico', 'nome_canonico'), 'zona'],
        'subzona': ['Subzona Urbana', 'subzona'],
        'ut': ['Area_Planejamento', estr.get('unidade_territorial', 'unidade_territorial'), 'unidade_territorial'],
        'div1': ['Divisao Subzona Urbana 1', 'divisao_1'],
        'div2': ['Divisao Subzona Urbana 2', 'divisao_2'],
    }
    
    valores = {}
    for k, possiveis in candidatos.items():
        for cand in possiveis:
            if cand in linha:
                valores[k] = str(linha[cand]).strip().lower()
                break
        if k not in valores:
            valores[k] = ''
    
    return f"{valores['ut']}||{valores['zona']}||{valores['subzona']}||{valores['div1']}||{valores['div2']}"


def merge_resultado_no_estado(resultado_iaA, lei_origem, pdf_origem,
                               estado_atual, conflitos_log, metadata=None):
    """
    Mescla resultado de uma chamada P2 (de uma lei especifica) no estado da planilha.
    NAO SOBRESCREVE celulas ja preenchidas (lei mais recente venceu).
    Loga conflitos.
    
    estado_atual: dict {chave_zona: {coluna: {valor, lei_origem, pdf_origem}}}
    conflitos_log: lista que recebe novos conflitos detectados
    
    Modifica estado_atual in-place. Retorna numero de celulas adicionadas e conflitos.
    """
    metadata = metadata or {}
    chave_zona_md = metadata.get('chave_zona_individual', 'linhas')
    
    if not isinstance(resultado_iaA, dict):
        return 0, 0
    
    linhas = resultado_iaA.get(chave_zona_md) or resultado_iaA.get('linhas') or []
    if not isinstance(linhas, list):
        return 0, 0
    
    adicionadas = 0
    conflitos = 0
    
    for linha in linhas:
        if not isinstance(linha, dict):
            continue
        chave = chave_zona_normalizada(linha, metadata)
        if not chave or chave == '||||||||':
            continue
        
        if chave not in estado_atual:
            estado_atual[chave] = {'_meta': dict(linha)}  # metadados (zona, ut, etc)
        
        for col, valor in linha.items():
            if not col:
                continue
            # Pular colunas vazias
            if valor is None or (isinstance(valor, str) and not valor.strip()):
                continue
            
            existente = estado_atual[chave].get(col)
            if existente is None or (isinstance(existente, dict) and existente.get('_eh_meta')):
                # Celula vazia: preencher
                estado_atual[chave][col] = {
                    'valor': valor,
                    'lei_origem': lei_origem,
                    'pdf_origem': pdf_origem,
                }
                adicionadas += 1
            else:
                # Celula ja preenchida: conflito (lei anterior tinha precedencia)
                if isinstance(existente, dict):
                    val_anterior = existente.get('valor')
                    lei_anterior = existente.get('lei_origem')
                    if str(val_anterior).strip() != str(valor).strip():
                        conflitos += 1
                        conflitos_log.append({
                            'chave_zona': chave,
                            'coluna': col,
                            'lei_vencedora': lei_anterior,
                            'valor_vencedor': val_anterior,
                            'lei_perdedora': lei_origem,
                            'valor_perdedor': valor,
                            'motivo': 'lei_anterior_tem_precedencia'
                        })
    
    return adicionadas, conflitos


def estado_para_resumo_para_prompt(estado_atual, max_zonas=30):
    """
    Converte estado_atual em dict simples {zona_chave: [colunas_preenchidas]}
    para incluir no prompt da proxima chamada P2.
    """
    resumo = {}
    for chave, dados in list(estado_atual.items())[:max_zonas]:
        cols = [k for k in dados.keys() if k != '_meta' and isinstance(dados.get(k), dict)]
        resumo[chave] = cols
    return resumo


def estado_para_planilha_final(estado_atual):
    """
    Converte estado_atual em lista de linhas planas {coluna: valor}
    pronta para escrever na planilha (xlsx).
    """
    linhas_finais = []
    for chave, dados in estado_atual.items():
        linha = {}
        # Recuperar metadados (zona, ut, etc) do _meta
        meta = dados.get('_meta', {})
        for k, v in meta.items():
            if v is not None and (not isinstance(v, str) or v.strip()):
                linha[k] = v
        # Adicionar valores preenchidos
        for col, info in dados.items():
            if col == '_meta':
                continue
            if isinstance(info, dict) and 'valor' in info:
                linha[col] = info['valor']
        linhas_finais.append(linha)
    return linhas_finais
