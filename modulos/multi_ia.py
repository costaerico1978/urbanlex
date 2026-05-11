"""
modulos/multi_ia.py — Adapter para multiplas IAs (Gemini, Anthropic).

Funcoes principais:
- info_modelo(provedor_modelo): retorna config (janela, tokens_por_pagina_pdf, etc)
- comprimir_pdf_se_necessario(pdfs_bytes, ia, log): comprime PDFs para caber na janela
- chamar_ia(provedor, modelo, prompt, pdfs, logs, label, retry=True): chama IA com PDFs nativos
"""
import os
import time
import io
from typing import List, Dict, Optional


# ============================================================
# Config dos modelos
# ============================================================
MODELOS = {
    'gemini-flash': {
        'provedor': 'gemini',
        'modelo': 'gemini-2.5-flash',
        'janela_tokens': 1_000_000,
        'output_max_tokens': 65_535,
        'tokens_por_pagina_pdf': 258,
        'estrategia_pdf': 'pdf_nativo',
    },
    'gemini-pro': {
        'provedor': 'gemini',
        'modelo': 'gemini-2.5-pro',
        'janela_tokens': 2_000_000,
        'output_max_tokens': 65_535,
        'tokens_por_pagina_pdf': 258,
        'estrategia_pdf': 'pdf_nativo',
    },
    'claude-sonnet': {
        'provedor': 'anthropic',
        'modelo': 'claude-sonnet-4-5',
        'janela_tokens': 200_000,
        'output_max_tokens': 8_192,
        'tokens_por_pagina_pdf': 2_300,  # Anthropic processa mais rico, mais tokens/pg
        'estrategia_pdf': 'texto_lei_principal',
    },
    'claude-opus': {
        'provedor': 'anthropic',
        'modelo': 'claude-opus-4-7',
        'janela_tokens': 200_000,
        'output_max_tokens': 8_192,
        'tokens_por_pagina_pdf': 2_300,
        'estrategia_pdf': 'texto_lei_principal',
    },
    # Modo hibrido: usa modelos diferentes por fase do pipeline.
    # Resolvido em tempo de execucao via roteamento abaixo.
    'gemini-hibrido': {
        'provedor': 'gemini',
        'modelo': 'gemini-2.5-pro',  # default se fase nao listada no roteamento
        'janela_tokens': 2_000_000,  # janela do modelo mais capaz
        'output_max_tokens': 65_535,
        'tokens_por_pagina_pdf': 258,
        'estrategia_pdf': 'pdf_nativo',
        'tipo': 'hibrido',
        'descricao': 'Pro em P2.1a/P2.2a (tabelas), Flash nas demais. ~50% custo Pro, ~95% qualidade.',
        'roteamento': {
            'P0': 'gemini-flash',
            'P1': 'gemini-flash',
            'P2.1a': 'gemini-pro',
            'P2.1b': 'gemini-flash',
            'P2.2a': 'gemini-pro',
            'P2.2b': 'gemini-flash',
            'P3': 'gemini-flash',
        },
    },
    # OCR + Gemini Pro: pipeline com pre-processamento OCR estruturado
    # dos anexos (Tesseract+Camelot/img2table) antes de mandar para Pro.
    # Atualmente comporta-se como Pro normal (placeholder); OCR real e
    # validacao serao adicionados em commits subsequentes.
    'gemini-pro-ocr': {
        'provedor': 'gemini',
        'modelo': 'gemini-2.5-pro',
        'janela_tokens': 2_000_000,
        'output_max_tokens': 65_535,
        'tokens_por_pagina_pdf': 258,
        'estrategia_pdf': 'texto_lei_principal',
        'descricao': 'Gemini Pro com pre-processamento OCR estruturado dos anexos. Maxima precisao.',
        'pre_processamento': 'ocr_tabelas',
    },
    # Plano Y: pipeline em 4 niveis com triagem + OCR + Pro + Sonnet
    # 1. Flash triagem: classifica paginas por tipo (ZONAS/PARAMETROS/TEXTO/IGNORAR)
    # 2. OCR estruturado: extrai tabelas dos anexos com parametros
    # 3. Pro: extracao principal usando markdown estruturado
    # 4. Sonnet: fallback cirurgico para paginas onde OCR falhou
    # ATUALMENTE PLACEHOLDER - comporta-se como gemini-pro-ocr ate Componente 1 (logging)
    # ser implementado. Sera ativado progressivamente em commits subsequentes.
    'triagem-ocr-pro-sonnet': {
        'provedor': 'gemini',
        'modelo': 'gemini-2.5-pro',
        'janela_tokens': 2_000_000,
        'output_max_tokens': 65_535,
        'tokens_por_pagina_pdf': 258,
        'estrategia_pdf': 'texto_lei_principal',
        'descricao': 'Pipeline completo: Flash triagem + OCR + Pro extracao + Sonnet cirurgico.',
        'pre_processamento': 'ocr_tabelas',
        'pipeline': 'triagem_ocr_pro_sonnet',  # flag para roteamento futuro
    },
}

def resolver_ia_para_fase(ia_id, fase_label):
    """Para meta-modelos hibridos, retorna o modelo real para a fase.
    Para modelos diretos, retorna o proprio ia_id."""
    cfg = MODELOS.get(ia_id, {})
    if cfg.get('tipo') == 'hibrido':
        roteamento = cfg.get('roteamento', {})
        # Match exato primeiro, depois prefixo (P2.1a.1/9 -> P2.1a)
        if fase_label in roteamento:
            return roteamento[fase_label]
        for prefixo, modelo_real in roteamento.items():
            if fase_label.startswith(prefixo):
                return modelo_real
        return ia_id  # fallback
    return ia_id


def info_modelo(ia_id):
    """Retorna config do modelo. Default = gemini-pro."""
    return MODELOS.get(ia_id, MODELOS['gemini-pro'])


# ============================================================
# Estimativa de tokens
# ============================================================
def estimar_tokens_pdf(pdf_bytes, tokens_por_pagina):
    """Estima tokens de um PDF baseado em num de paginas + tokens_por_pagina."""
    try:
        import fitz
        doc = fitz.open(stream=pdf_bytes, filetype='pdf')
        n_paginas = doc.page_count
        doc.close()
        return n_paginas * tokens_por_pagina
    except Exception:
        # Fallback: ~250 tokens por KB
        return (len(pdf_bytes) // 1024) * 250


# ============================================================
# Compressao de PDFs
# ============================================================
def comprimir_pdf(pdf_bytes):
    """
    Comprime PDF reduzindo qualidade de imagens embutidas.
    Retorna bytes do PDF comprimido (ou original se falhar).
    """
    try:
        import fitz
        doc = fitz.open(stream=pdf_bytes, filetype='pdf')
        out_buf = io.BytesIO()
        doc.save(
            out_buf,
            garbage=4,
            deflate=True,
            deflate_images=True,
            deflate_fonts=True,
            clean=True,
        )
        doc.close()
        comprimido = out_buf.getvalue()
        # So usa o comprimido se realmente reduziu
        if len(comprimido) < len(pdf_bytes):
            return comprimido
        return pdf_bytes
    except Exception:
        return pdf_bytes


def estimar_tokens_lista(pdfs, tpp):
    """Estima total de tokens para uma lista de PDFs."""
    import base64
    total = 0
    for p in pdfs:
        try:
            data = base64.b64decode(p['data_b64'])
            total += estimar_tokens_pdf(data, tpp)
        except Exception:
            pass
    return total


def dividir_em_blocos(pdfs, ia_id, logs=None):
    """
    Divide PDFs em blocos que cabem na janela da IA.
    Tenta na ordem: 1) cabe direto, 2) comprime, 3) divide.
    Sempre retorna LISTA DE BLOCOS (1+ listas de pdfs).
    """
    import base64
    info = info_modelo(ia_id)
    janela = info['janela_tokens']
    tpp = info['tokens_por_pagina_pdf']
    limite = int(janela * 0.50)
    
    total = estimar_tokens_lista(pdfs, tpp)
    if logs is not None:
        logs.append({'nivel': 'info',
                     'msg': f'  📊 Tokens estimados ({ia_id}): {total:,} / janela {janela:,}'})
    
    if total <= limite:
        return [pdfs]
    
    if logs is not None:
        logs.append({'nivel': 'aviso',
                     'msg': f'  ⚠ Estourou janela ({total:,} > {limite:,}). Comprimindo PDFs...'})
    
    pdfs_comp = []
    for p in pdfs:
        try:
            data = base64.b64decode(p['data_b64'])
            data_comp = comprimir_pdf(data)
            pdfs_comp.append({
                'title': p['title'],
                'data_b64': base64.standard_b64encode(data_comp).decode(),
                'nome_arquivo': p.get('nome_arquivo', '?')
            })
        except Exception:
            pdfs_comp.append(p)
    
    total_c = estimar_tokens_lista(pdfs_comp, tpp)
    if logs is not None:
        logs.append({'nivel': 'info',
                     'msg': f'  📦 Apos compressao: {total_c:,} tokens'})
    
    if total_c <= limite:
        return [pdfs_comp]
    
    if logs is not None:
        logs.append({'nivel': 'aviso',
                     'msg': f'  ✂ Ainda nao cabe. Dividindo em blocos de ate {limite:,} tokens...'})
    
    blocos = []
    bloco = []
    t_bloco = 0
    for p in pdfs_comp:
        try:
            data = base64.b64decode(p['data_b64'])
            t_p = estimar_tokens_pdf(data, tpp)
        except Exception:
            t_p = limite // 4
        if t_bloco + t_p > limite and bloco:
            blocos.append(bloco)
            bloco = [p]
            t_bloco = t_p
        else:
            bloco.append(p)
            t_bloco += t_p
    if bloco:
        blocos.append(bloco)
    
    if logs is not None:
        logs.append({'nivel': 'info',
                     'msg': f'  ✂ {len(blocos)} bloco(s) gerado(s)'})
        for i, b in enumerate(blocos, 1):
            tb = estimar_tokens_lista(b, tpp)
            logs.append({'nivel': 'info', 'msg': f'    Bloco {i}: {len(b)} PDF(s), ~{tb:,} tokens'})
    
    return blocos


def adequar_pdfs_para_janela(pdfs, ia_id, logs=None):
    """Compat: retorna primeiro bloco + flag se foi comprimido/dividido."""
    blocos = dividir_em_blocos(pdfs, ia_id, logs)
    return blocos[0], (len(blocos) > 1)


# ============================================================
# Chamada a IA com PDFs nativos
# ============================================================
def chamar_ia(client, ia_id, prompt_text, pdfs, logs, label='IA',
              max_tentativas=5, intervalo_base=15):
    """
    Chama IA com PDFs nativos (sem pre-processamento).
    Retry automatico em erros transitorios.
    Retorna texto da resposta (string) ou levanta excecao.
    
    pdfs: lista de {'title','data_b64','nome_arquivo'}
    """
    import threading, time as _t
    info = info_modelo(ia_id)
    provedor = info['provedor']
    modelo = info['modelo']
    
    erros_transitorios = ['503', '429', '500', '502', '504', 'overload',
                          'high demand', 'rate limit', 'timeout', 'unavailable',
                          'connection reset', 'network', 'temporarily',
                          'try again', 'try later', 'deadline exceeded']
    
    ultima_excecao = None
    for tentativa in range(1, max_tentativas + 1):
        try:
            return _executar_chamada(client, provedor, modelo, prompt_text, pdfs, logs, label)
        except Exception as e:
            ultima_excecao = e
            msg_erro = str(e).lower()
            transitorio = any(t in msg_erro for t in erros_transitorios)
            if not transitorio:
                logs.append({'nivel': 'erro',
                             'msg': f'❌ {label}: erro nao-transitorio: {str(e)[:200]}'})
                raise
            if tentativa >= max_tentativas:
                logs.append({'nivel': 'erro',
                             'msg': f'❌ {label}: esgotou {max_tentativas} tentativas: {str(e)[:200]}'})
                raise
            espera = intervalo_base * tentativa
            logs.append({'nivel': 'aviso',
                         'msg': f'⚠ {label}: erro transitorio (tent. {tentativa}/{max_tentativas}), aguardando {espera}s: {str(e)[:150]}'})
            _t.sleep(espera)
    
    if ultima_excecao:
        raise ultima_excecao


def _executar_chamada(client, provedor, modelo, prompt_text, pdfs, logs, label):
    """Executa uma chamada (sem retry). Streaming + heartbeat."""
    import base64, threading, time as _t
    
    logs.append({'nivel': 'info', 'msg': f'⏳ {label}: Aguardando IA (streaming)...'})
    inicio = _t.time()
    chars = 0
    chunks = 0
    txt = ''
    
    # Heartbeat thread
    parar_heart = threading.Event()
    def heartbeat():
        while not parar_heart.is_set():
            _t.sleep(15)
            if parar_heart.is_set(): break
            decorridos = int(_t.time() - inicio)
            logs.append({'nivel': 'info',
                         'msg': f'💓 {label}: Aguardando... {decorridos}s decorridos, {chars} chars recebidos'})
    
    hb_thread = threading.Thread(target=heartbeat, daemon=True)
    hb_thread.start()
    
    try:
        if provedor == 'anthropic':
            # Anthropic: messages.stream com PDFs como document
            content = [{'type': 'text', 'text': prompt_text}]
            for p in pdfs:
                content.append({
                    'type': 'document',
                    'source': {
                        'type': 'base64',
                        'media_type': 'application/pdf',
                        'data': p['data_b64']
                    },
                    'title': p.get('title', p.get('nome_arquivo', 'documento')),
                })
            
            with client.messages.stream(
                model=modelo,
                max_tokens=8192,
                temperature=0.3,
                messages=[{'role': 'user', 'content': content}]
            ) as stream:
                for text in stream.text_stream:
                    if text:
                        txt += text
                        chunks += 1
                        chars += len(text)
        
        else:  # gemini
            parts = [prompt_text]
            for p in pdfs:
                try:
                    data = base64.b64decode(p['data_b64'])
                    parts.append({'mime_type': 'application/pdf', 'data': data})
                except Exception:
                    pass
            
            stream = client.generate_content(parts, stream=True,
                                             generation_config={'temperature': 0.3},
                                             request_options={'timeout': 300})
            for chunk in stream:
                try:
                    piece = chunk.text if hasattr(chunk, 'text') else ''
                except Exception:
                    piece = ''
                if piece:
                    txt += piece
                    chunks += 1
                    chars += len(piece)
    finally:
        parar_heart.set()
    
    decorridos = round(_t.time() - inicio, 1)
    if not txt:
        raise Exception(f'{label}: resposta vazia da IA apos {decorridos}s')
    
    logs.append({'nivel': 'ok',
                 'msg': f'✅ {label}: IA terminou em {decorridos}s ({chars} chars, {chunks} chunks)'})
    return txt


# ============================================================
# Helpers
# ============================================================
def montar_client(ia_id):
    """Cria o client apropriado para a IA escolhida. Retorna o client ou None."""
    info = info_modelo(ia_id)
    provedor = info['provedor']
    modelo = info['modelo']
    
    if provedor == 'anthropic':
        try:
            import anthropic
            api_key = os.getenv('ANTHROPIC_API_KEY', '')
            if not api_key:
                return None
            return anthropic.Anthropic(api_key=api_key)
        except Exception:
            return None
    else:
        try:
            import google.generativeai as genai
            api_key = os.getenv('GEMINI_API_KEY', '')
            if not api_key:
                return None
            genai.configure(api_key=api_key)
            return genai.GenerativeModel(modelo)
        except Exception:
            return None

def chamar_ia_com_blocos(client, ia_id, prompt_text, pdfs, logs, label='IA',
                          chave_agregar=None, max_tentativas=5, intervalo_base=15,
                          pausa_entre_blocos=60):
    """
    Chama a IA dividindo automaticamente em blocos se necessario.
    Faz N chamadas (1 por bloco) e agrega resultados.
    
    chave_agregar: chave do JSON de saida que contem a lista a agregar.
                   Ex: 'arquivos' (P0), 'zonas_canonicas' (P1), 'linhas' (P3).
                   Se None, retorna apenas o resultado do PRIMEIRO bloco (avisando).
    
    Retorna texto-JSON consolidado (string) que pode ser parseado normalmente.
    """
    import json as _json
    blocos = dividir_em_blocos(pdfs, ia_id, logs)
    
    if len(blocos) == 1:
        # Caso comum: um unico bloco
        return chamar_ia(client, ia_id, prompt_text, blocos[0], logs, label,
                         max_tentativas=max_tentativas, intervalo_base=intervalo_base)
    
    # Multiplos blocos: fazer N chamadas e agregar
    if logs is not None:
        logs.append({'nivel': 'aviso',
                     'msg': f'⚙ {label} sera enviado em {len(blocos)} chamadas (PDFs nao cabem em uma unica janela).'})
    
    resultados = []
    for i, bloco in enumerate(blocos, 1):
        sub_label = f'{label}.{i}/{len(blocos)}'
        if logs is not None:
            logs.append({'nivel': 'info',
                         'msg': f'  ▶ {sub_label}: {len(bloco)} PDF(s)'})
        # Adicionar nota ao prompt sobre divisao
        prompt_bloco = (prompt_text +
                        f'\n\n[NOTA: Este e o BLOCO {i} de {len(blocos)} blocos de PDFs '
                        f'enviados separadamente devido ao tamanho. Voce esta vendo apenas '
                        f'os PDFs deste bloco. Outros blocos serao processados em chamadas '
                        f'separadas e os resultados serao agregados pelo backend.]')
        try:
            r = chamar_ia(client, ia_id, prompt_bloco, bloco, logs, sub_label,
                          max_tentativas=max_tentativas, intervalo_base=intervalo_base)
            resultados.append(r)
        except Exception as e:
            if logs is not None:
                logs.append({'nivel': 'erro',
                             'msg': f'  ❌ {sub_label}: falhou: {str(e)[:200]}'})
            continue
        # Pausa entre blocos para respeitar rate limit (Anthropic Sonnet: 30k tokens/min)
        if i < len(blocos) and pausa_entre_blocos > 0 and ia_id.startswith('claude'):
            if logs is not None:
                logs.append({'nivel': 'info',
                             'msg': f'  ⏸ Aguardando {pausa_entre_blocos}s antes do proximo bloco (rate limit)...'})
            import time as _tt
            _tt.sleep(pausa_entre_blocos)
    
    if not resultados:
        raise Exception(f'{label}: todos os blocos falharam')
    
    # Agregar JSONs
    if chave_agregar is None:
        # Sem chave: retornar apenas o primeiro bloco
        if logs is not None:
            logs.append({'nivel': 'aviso',
                         'msg': f'⚠ {label}: chave_agregar=None, retornando apenas o 1o bloco'})
        return resultados[0]
    
    # Parsear cada resultado e agregar a chave
    items_agregados = []
    alertas_agregados = []
    for j, r in enumerate(resultados, 1):
        try:
            # Tentar parsear JSON (mesma logica que extrair_json)
            from modulos.gerador_hibrido import extrair_json
            obj = extrair_json(r)
            if obj is None:
                continue
            if chave_agregar in obj and isinstance(obj[chave_agregar], list):
                items_agregados.extend(obj[chave_agregar])
            if 'alertas' in obj and isinstance(obj['alertas'], list):
                alertas_agregados.extend(obj['alertas'])
        except Exception as e:
            if logs is not None:
                logs.append({'nivel': 'aviso',
                             'msg': f'  ⚠ Erro parseando bloco {j}: {str(e)[:100]}'})
    
    if logs is not None:
        logs.append({'nivel': 'ok',
                     'msg': f'✅ {label}: agregado de {len(blocos)} blocos -> {len(items_agregados)} {chave_agregar}'})
    
    # Retornar JSON consolidado
    consolidado = {chave_agregar: items_agregados}
    if alertas_agregados:
        consolidado['alertas'] = alertas_agregados
    return _json.dumps(consolidado, ensure_ascii=False)
