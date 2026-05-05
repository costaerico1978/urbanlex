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
            stream = client.generate_content(parts, stream=True)
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
def prompt_passada_0_catalogacao(nomes_arquivos=None):
    nomes_arquivos = nomes_arquivos or []
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
        '  "arquivos": [\n'
        '    {"nome_arquivo": "LC_270_2024.pdf", "identificacao": "LC 270/2024", "escopo": "Municipio inteiro"},\n'
        '    ...\n'
        '  ]\n'
        '}'
    )


def prompt_passada_1_inventario(prompt_usuario):
    return prompt_usuario + (
        '\n\n=== INSTRUCAO DESTA EXECUCAO — PASSADA 1: INVENTARIO ===\n'
        'Esta e a Passada 1. NAO preencha a planilha. Aplique todas as etapas 0.0 a 0.9 e a '
        'PARTE 3 do prompt acima. Faca o inventario completo das zonas/subzonas/unidades '
        'territoriais e a normalizacao de nomenclaturas (Etapa 0.9).\n\n'
        'Retorne SOMENTE este JSON, sem markdown, sem comentarios:\n'
        '{\n'
        '  "zonas_canonicas": [\n'
        '    {\n'
        '      "nome_canonico": "ZRM2-A",\n'
        '      "variantes_observadas": ["ZRM2A", "ZRM-2A"],\n'
        '      "unidade_territorial": "AP-2.1",\n'
        '      "leis_aplicaveis": ["LC 270/2024", "Dec 52585/2023"]\n'
        '    }\n'
        '  ],\n'
        '  "alertas": ["..."]\n'
        '}\n\n'
        'IMPORTANTE: liste TODAS as combinacoes unicas. Uma mesma zona em unidades territoriais '
        'distintas e um item separado. Use leis_aplicaveis com a identificacao exata de cada lei '
        '(ex: "LC 270/2024"), mesmo formato da Passada 0.'
    )


def prompt_passada_2_zona(prompt_usuario, zona_canonica, unidade_territorial, headers):
    headers_lista = '\n'.join(f'  {i+1}. "{h}"' for i, h in enumerate(headers))
    return prompt_usuario + (
        f'\n\n=== INSTRUCAO DESTA EXECUCAO — PASSADA 2: PREENCHER ZONA ESPECIFICA ===\n'
        f'Esta e a Passada 2. Os PDFs anexados sao EXCLUSIVAMENTE os relevantes para esta zona. '
        f'Aplique todas as etapas do prompt acima.\n\n'
        f'Zona canonica a preencher: {zona_canonica}\n'
        f'Unidade territorial: {unidade_territorial}\n\n'
        f'Preencha APENAS uma linha (a desta zona/unidade territorial). Se a zona possui '
        f'subdivisoes finas (logradouro, lado, faixa de numeracao, etc. — Etapa 0.4 do prompt), '
        f'retorne MULTIPLAS linhas, uma para cada subdivisao.\n\n'
        f'Retorne SOMENTE este JSON, sem markdown, sem comentarios:\n'
        f'{{\n'
        f'  "linhas": [\n'
        f'    {{"<cabecalho>": "<valor>", ...}},\n'
        f'    ...\n'
        f'  ]\n'
        f'}}\n\n'
        f'Cada chave deve ser EXATAMENTE um dos cabecalhos da planilha listados abaixo (mesma '
        f'grafia, mesmas maiusculas, mesmos acentos):\n\n{headers_lista}\n\n'
        f'Para campos sem informacao na lei, use "NI". Para colunas de calculo automatico (Parte '
        f'7 do prompt), retorne string vazia "".'
    )


def prompt_passada_3_validacao(json_consolidado, zonas_canonicas):
    return (
        '\n\n=== INSTRUCAO DESTA EXECUCAO — PASSADA 3: VALIDACAO FINAL ===\n'
        'Esta e a Passada 3. Os PDFs anexados sao TODOS do compilado. Abaixo esta o JSON '
        'consolidado das zonas ja preenchidas nas passadas anteriores.\n\n'
        'Sua tarefa: verificar se alguma zona/unidade territorial ficou de fora ou se ha '
        'inconsistencias graves (ex: revogacao parcial nao aplicada, zona com mesmo nome em '
        'unidade territorial diferente nao listada).\n\n'
        'Lista de zonas que FORAM preenchidas:\n'
        + '\n'.join(f'  - {z.get("nome_canonico","?")} ({z.get("unidade_territorial","?")})' for z in zonas_canonicas) +
        '\n\nJSON consolidado:\n' + json_consolidado +
        '\n\nRetorne SOMENTE este JSON, sem markdown, sem comentarios:\n'
        '{\n'
        '  "linhas_faltantes": [\n'
        '    {"<cabecalho>": "<valor>", ...}\n'
        '  ],\n'
        '  "alertas": ["..."]\n'
        '}\n\n'
        'Se nada faltar, retorne "linhas_faltantes": [] e "alertas": [].'
    )
