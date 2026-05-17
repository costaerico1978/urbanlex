"""
═══════════════════════════════════════════════════════════════════════════════
ETAPA 4.5 — Detecção de Anexos Citados
═══════════════════════════════════════════════════════════════════════════════

Após o organizador montar a pasta do dossiê, esta etapa:
  1. Extrai o TEXTO do corpo da lei (primeiras páginas do pdf_concatenado)
  2. Chama Haiku 4.5 pedindo lista de anexos MENCIONADOS no corpo
  3. Compara com anexos BAIXADOS (vindos do organizador)
  4. Identifica FALTANTES (citados mas não baixados)
  5. Salva tudo em dossie_legislacoes_pasta (anexos_citados / anexos_faltantes)

API:
  detectar_anexos_citados(legislacao_label, pdf_path, anexos_baixados, log_callback=None) -> dict

Custo estimado: ~$0.05 a $0.30 por legislação (depende do tamanho do corpo)
═══════════════════════════════════════════════════════════════════════════════
"""

import os
import json
import logging
import re

logger = logging.getLogger(__name__)

MODELO_HAIKU = "claude-haiku-4-5-20251001"

# Limite de páginas a enviar pro Haiku (evita explodir contexto/custo)
MAX_PAGINAS_CORPO = 80
MAX_CARACTERES_CORPO = 120000


def detectar_anexos_citados(legislacao_label, pdf_path, anexos_baixados, log_callback=None):
    """
    Detecta quais anexos são CITADOS no corpo da lei e identifica os FALTANTES.
    
    ALGORITMO DE MATCH (v2):
      Pra cada anexo citado pelo Haiku (ex: "Anexo 1.4"):
        1. Busca o texto "Anexo 1.4" DENTRO do pdf_concatenado completo
        2. Se encontrou → marca como BAIXADO ✓
        3. Senão → checa nomes dos arquivos baixados (heurística antiga)
        4. Se nada bater → FALTANTE ⚠️
    
    Args:
        legislacao_label:    'LC_148_2023' (pra logging)
        pdf_path:            path do pdf_concatenado.pdf (corpo + anexos juntos)
        anexos_baixados:     lista de dicts vinda do organizador
        log_callback:        função(msg) opcional
    
    Retorna:
        {
            'sucesso': bool,
            'anexos_citados':    [{nome_citado, contexto_no_corpo}],  # do Haiku
            'anexos_encontrados': [{nome_citado, onde}],
            'anexos_faltantes':  [{nome_citado, contexto, motivo}],
            'custo_estimado':    float,
            'tokens_input':      int,
            'tokens_output':     int,
            'erro':              str (se falhou)
        }
    """
    def _log(msg):
        logger.info(f"[etapa4.5 {legislacao_label}] {msg}")
        if log_callback:
            log_callback(msg)
    
    # ───────────────────────────────────────────────────────────────
    # 1. Extrai texto do corpo (apenas N primeiras páginas)
    # ───────────────────────────────────────────────────────────────
    if not pdf_path or not os.path.exists(pdf_path):
        return {'sucesso': False, 'erro': f'PDF nao encontrado: {pdf_path}'}
    
    _log(f"Extraindo texto de {pdf_path}")
    
    texto_corpo = _extrair_texto_corpo(pdf_path, max_paginas=MAX_PAGINAS_CORPO)
    if not texto_corpo:
        return {'sucesso': False, 'erro': 'falha ao extrair texto do PDF'}
    
    if len(texto_corpo) > MAX_CARACTERES_CORPO:
        texto_corpo = texto_corpo[:MAX_CARACTERES_CORPO]
        _log(f"Texto truncado em {MAX_CARACTERES_CORPO} chars")
    
    _log(f"Texto extraído: {len(texto_corpo)} chars")
    
    # ───────────────────────────────────────────────────────────────
    # 2. Chama Haiku 4.5
    # ───────────────────────────────────────────────────────────────
    try:
        anexos_citados, custo, tokens_in, tokens_out = _chamar_haiku_detectar(texto_corpo, log_callback=_log)
    except Exception as e:
        import traceback
        _log(f"ERRO Haiku: {e}")
        logger.error(traceback.format_exc())
        return {'sucesso': False, 'erro': f'Haiku falhou: {str(e)[:200]}'}
    
    _log(f"Haiku detectou {len(anexos_citados)} anexo(s) citado(s)")
    
    # ───────────────────────────────────────────────────────────────
    # 3. Extrai texto COMPLETO do PDF (corpo + anexos) pra busca
    # ───────────────────────────────────────────────────────────────
    _log("Extraindo texto COMPLETO do PDF (sem limite de paginas) pra match...")
    texto_completo = _extrair_texto_completo(pdf_path)
    if not texto_completo:
        # Fallback: usa o texto do corpo
        texto_completo = texto_corpo
    _log(f"Texto completo: {len(texto_completo):,} chars")
    
    # Normaliza pra busca (case-insensitive, sem acentos)
    texto_completo_norm = _normalizar_texto_busca(texto_completo)
    
    # Normaliza nomes dos baixados pra fallback
    nomes_baixados_norm = set()
    for arq in anexos_baixados:
        nome = arq.get('nome') or ''
        nomes_baixados_norm.add(_normalizar_nome_anexo(nome))
    
    # ───────────────────────────────────────────────────────────────
    # 4. Pra cada anexo citado, verifica se aparece no texto OU nos nomes
    # ───────────────────────────────────────────────────────────────
    encontrados = []
    faltantes = []
    
    for citado in anexos_citados:
        nome_c = citado.get('nome_citado') or ''
        if not nome_c:
            continue
        
        # MATCH 1: aparece literalmente no texto do PDF?
        onde_encontrou = _buscar_no_texto(nome_c, texto_completo_norm)
        if onde_encontrou:
            encontrados.append({
                'nome_citado': nome_c,
                'onde': onde_encontrou,
                'contexto_citacao': citado.get('contexto', '')[:200],
            })
            continue
        
        # MATCH 2 (fallback): nome bate com algum arquivo baixado?
        nome_c_norm = _normalizar_nome_anexo(nome_c)
        encontrou_por_nome = False
        for nome_b in nomes_baixados_norm:
            if not nome_b or not nome_c_norm:
                continue
            if nome_c_norm in nome_b or nome_b in nome_c_norm:
                encontrou_por_nome = True
                break
            if _match_anexo_referencia(nome_c, nome_b):
                encontrou_por_nome = True
                break
        
        if encontrou_por_nome:
            encontrados.append({
                'nome_citado': nome_c,
                'onde': 'nome_arquivo',
                'contexto_citacao': citado.get('contexto', '')[:200],
            })
        else:
            faltantes.append({
                'nome_citado': nome_c,
                'contexto': citado.get('contexto', '')[:200],
                'motivo': 'nao encontrado no texto do PDF nem nos nomes dos arquivos',
            })
    
    _log(f"Encontrados: {len(encontrados)}/{len(anexos_citados)} | Faltantes: {len(faltantes)}")
    
    return {
        'sucesso': True,
        'anexos_citados': anexos_citados,
        'anexos_encontrados': encontrados,
        'anexos_faltantes': faltantes,
        'custo_estimado': custo,
        'tokens_input': tokens_in,
        'tokens_output': tokens_out,
    }


def _extrair_texto_corpo(pdf_path, max_paginas=80):
    """Extrai texto das primeiras N páginas do PDF."""
    try:
        import pypdf
        reader = pypdf.PdfReader(pdf_path)
        n_paginas = min(len(reader.pages), max_paginas)
        partes = []
        for i in range(n_paginas):
            try:
                t = reader.pages[i].extract_text() or ''
                if t.strip():
                    partes.append(t)
            except Exception:
                continue
        return '\n\n'.join(partes)
    except Exception as e:
        logger.error(f"Erro pypdf: {e}")
        return None


def _extrair_texto_completo(pdf_path):
    """Extrai texto de TODAS as paginas do PDF (pra busca de anexos citados)."""
    try:
        import pypdf
        reader = pypdf.PdfReader(pdf_path)
        partes = []
        for i in range(len(reader.pages)):
            try:
                t = reader.pages[i].extract_text() or ''
                if t.strip():
                    partes.append(t)
            except Exception:
                continue
        return '\n\n'.join(partes)
    except Exception as e:
        logger.error(f"Erro pypdf (completo): {e}")
        return None


def _normalizar_texto_busca(texto):
    """Normaliza texto pra busca: lower, sem acentos, espacos compactados."""
    import unicodedata
    s = (texto or '').lower()
    s = unicodedata.normalize('NFKD', s).encode('ascii', 'ignore').decode()
    # Compacta espacos consecutivos
    s = re.sub(r'\s+', ' ', s)
    return s


def _buscar_no_texto(nome_citado, texto_norm):
    """
    Procura o nome citado no texto normalizado.
    Retorna string descrevendo onde achou, ou None.
    
    Tenta variacoes:
      - Anexo 1.4 → "anexo 1.4", "anexo 1 4", "anexo 1-4"
    """
    if not nome_citado or not texto_norm:
        return None
    
    nome_norm = _normalizar_texto_busca(nome_citado)
    
    # 1) Match literal
    if nome_norm in texto_norm:
        return f'literal: "{nome_norm}"'
    
    # 2) Variacoes com separadores diferentes
    # Extrai numeros (ex: "anexo 1.4" → "1.4")
    nums = re.findall(r'\d+(?:[.\s\-_]\d+)*', nome_norm)
    if nums:
        # Tenta com cada separador
        for num in nums:
            num_limpo = re.sub(r'[\s\-_]', '.', num)  # uniformiza pra ponto
            # Prefixo: extrai "anexo", "tabela", "mapa" etc do nome citado
            prefixo_match = re.match(r'(\w+)\s', nome_norm)
            if prefixo_match:
                prefixo = prefixo_match.group(1)
                # Tenta variacoes
                for sep in ['.', ' ', '-', '_']:
                    variacao = f'{prefixo} {num_limpo.replace(".", sep)}'
                    if variacao in texto_norm:
                        return f'variacao: "{variacao}"'
                # Tenta sem prefixo
                if f' {num_limpo}' in texto_norm:
                    return f'num: "{num_limpo}"'
    
    return None


def _chamar_haiku_detectar(texto_corpo, log_callback=None):
    """
    Chama o Haiku 4.5 pedindo lista de anexos citados no corpo.
    Retorna: (anexos_citados, custo_usd, tokens_input, tokens_output)
    """
    from anthropic import Anthropic
    
    cliente = Anthropic()
    
    prompt = f"""Você está analisando o CORPO de uma legislação urbanística municipal brasileira.

Sua tarefa: identificar TODOS os anexos, tabelas, mapas e gravames que o texto MENCIONA.

REGRAS:
1. Liste cada anexo/tabela/mapa citado uma única vez (deduplicado)
2. Use o nome EXATO como aparece no texto (ex: "Anexo 1.4", "Anexo I", "Tabela XV", "Mapa 03")
3. Para cada citação, inclua o contexto onde aparece (frase ou trecho curto)
4. NÃO invente anexos que não estão no texto
5. Inclua APENAS anexos REGULADOS pela própria lei (não cite leis externas)
6. Retorne APENAS JSON válido, sem markdown

FORMATO DE RESPOSTA (JSON estrito):
{{
  "anexos": [
    {{"nome_citado": "Anexo 1.1", "contexto": "trecho onde aparece"}},
    {{"nome_citado": "Tabela XV", "contexto": "..."}}
  ]
}}

CORPO DA LEI:
{texto_corpo}

Retorne APENAS o JSON, sem comentários ou explicações."""
    
    if log_callback:
        log_callback(f"Chamando Haiku 4.5 ({len(prompt):,} chars input)")
    
    response = cliente.messages.create(
        model=MODELO_HAIKU,
        max_tokens=4000,
        messages=[{"role": "user", "content": prompt}],
    )
    
    # Extrai texto da resposta
    if not response.content:
        raise ValueError("Resposta vazia do Haiku")
    
    texto = response.content[0].text
    
    # Custo do Haiku 4.5: $1/1M input, $5/1M output (preços aproximados)
    tokens_in = response.usage.input_tokens
    tokens_out = response.usage.output_tokens
    custo = (tokens_in / 1_000_000) * 1.0 + (tokens_out / 1_000_000) * 5.0
    
    if log_callback:
        log_callback(f"Haiku resp: {len(texto)} chars, tokens {tokens_in}→{tokens_out}, ${custo:.4f}")
    
    # Parse JSON (com brace-counting, robusto contra markdown)
    try:
        anexos = _parse_json_anexos(texto)
        return anexos, custo, tokens_in, tokens_out
    except Exception as e:
        # Salva resposta bruta no log pra debug
        logger.error(f"Resposta bruta do Haiku: {texto[:500]}")
        raise ValueError(f"falha parse JSON: {e}")


def _parse_json_anexos(texto):
    """Parse JSON com brace-counting (robusto contra markdown wrapper)."""
    # Remove possíveis markdown fences
    texto = texto.strip()
    if texto.startswith('```'):
        # Remove ```json e ``` no fim
        texto = re.sub(r'^```(?:json)?\s*', '', texto)
        texto = re.sub(r'\s*```\s*$', '', texto)
    
    # Tenta parse direto primeiro
    try:
        obj = json.loads(texto)
    except Exception:
        # Fallback: encontra { ... } balanceado
        inicio = texto.find('{')
        if inicio < 0:
            raise ValueError("nenhum '{' encontrado")
        nivel = 0
        fim = -1
        for i in range(inicio, len(texto)):
            if texto[i] == '{':
                nivel += 1
            elif texto[i] == '}':
                nivel -= 1
                if nivel == 0:
                    fim = i
                    break
        if fim < 0:
            raise ValueError("JSON nao balanceado")
        obj = json.loads(texto[inicio:fim+1])
    
    anexos = obj.get('anexos', [])
    if not isinstance(anexos, list):
        raise ValueError("'anexos' nao eh lista")
    
    # Normaliza/limpa
    resultado = []
    for a in anexos:
        if isinstance(a, dict) and a.get('nome_citado'):
            resultado.append({
                'nome_citado': str(a['nome_citado']).strip(),
                'contexto': str(a.get('contexto', '')).strip(),
            })
        elif isinstance(a, str):
            resultado.append({'nome_citado': a.strip(), 'contexto': ''})
    return resultado


def _normalizar_nome_anexo(nome):
    """Normaliza nome de anexo pra comparação. Lowercase + tira pontuação/acentos."""
    import unicodedata
    s = (nome or '').strip().lower()
    s = unicodedata.normalize('NFKD', s).encode('ascii', 'ignore').decode()
    # Remove pontuação comum
    s = re.sub(r'[^a-z0-9]+', '_', s)
    return s.strip('_')


def _match_anexo_referencia(nome_citado, nome_baixado_norm):
    """
    Match flexível: 'Anexo 1.4' deve casar com nome baixado contendo '1_4' ou '1.4'.
    """
    citado_norm = _normalizar_nome_anexo(nome_citado)
    
    # Extrai numeros do citado (ex: "Anexo 1.4" → "1_4")
    nums = re.findall(r'\d+(?:[._]\d+)*', citado_norm)
    if not nums:
        return False
    
    # Se algum desses números aparecer no baixado, é match
    for n in nums:
        if n in nome_baixado_norm:
            return True
    return False
