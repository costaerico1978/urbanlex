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
    },
    'gemini-pro': {
        'provedor': 'gemini',
        'modelo': 'gemini-2.5-pro',
        'janela_tokens': 2_000_000,
        'output_max_tokens': 65_535,
        'tokens_por_pagina_pdf': 258,
    },
    'claude-sonnet': {
        'provedor': 'anthropic',
        'modelo': 'claude-sonnet-4-5',
        'janela_tokens': 200_000,
        'output_max_tokens': 8_192,
        'tokens_por_pagina_pdf': 1_500,  # Anthropic processa mais rico, mais tokens/pg
    },
    'claude-opus': {
        'provedor': 'anthropic',
        'modelo': 'claude-opus-4-7',
        'janela_tokens': 200_000,
        'output_max_tokens': 8_192,
        'tokens_por_pagina_pdf': 1_500,
    },
}


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


def adequar_pdfs_para_janela(pdfs, ia_id, logs=None):
    """
    Recebe lista de pdfs [{'title','data_b64','nome_arquivo'}] e a IA escolhida.
    Retorna lista de pdfs que cabem na janela. Se nao couberem, comprime.
    Se ainda nao couberem, retorna a lista original (chamador decide se divide).
    
    Se logs fornecido, registra o que foi feito.
    """
    import base64
    info = info_modelo(ia_id)
    janela = info['janela_tokens']
    tpp = info['tokens_por_pagina_pdf']
    
    # Reservar 15% da janela para prompt + resposta
    limite = int(janela * 0.85)
    
    # Estimar tokens totais
    total_estimado = 0
    for p in pdfs:
        try:
            data = base64.b64decode(p['data_b64'])
            total_estimado += estimar_tokens_pdf(data, tpp)
        except Exception:
            pass
    
    if logs is not None:
        logs.append({'nivel': 'info',
                     'msg': f'  📊 Tokens estimados ({ia_id}): {total_estimado:,} / janela {janela:,}'})
    
    if total_estimado <= limite:
        # Cabe sem mexer
        return pdfs, False  # (lista, foi_comprimido)
    
    # Tentar comprimir
    if logs is not None:
        logs.append({'nivel': 'aviso',
                     'msg': f'  ⚠ Estourou janela. Comprimindo {len(pdfs)} PDF(s)...'})
    
    pdfs_comp = []
    novo_total = 0
    for p in pdfs:
        try:
            data = base64.b64decode(p['data_b64'])
            data_comp = comprimir_pdf(data)
            pdfs_comp.append({
                'title': p['title'],
                'data_b64': base64.standard_b64encode(data_comp).decode(),
                'nome_arquivo': p.get('nome_arquivo', '?')
            })
            novo_total += estimar_tokens_pdf(data_comp, tpp)
        except Exception:
            pdfs_comp.append(p)
    
    if logs is not None:
        ratio = novo_total / total_estimado if total_estimado > 0 else 1.0
        logs.append({'nivel': 'info',
                     'msg': f'  📦 Apos compressao: {novo_total:,} tokens ({ratio*100:.0f}% do original)'})
    
    return pdfs_comp, True


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
