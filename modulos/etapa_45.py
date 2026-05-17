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

# Limites por CHUNK (1 chamada Haiku)
CHUNK_SIZE_CHARS = 250000      # ~70k tokens por chunk
CHUNK_OVERLAP_CHARS = 5000     # overlap pra nao cortar anexo no meio
MAX_TOTAL_CHARS = 2_000_000    # limite de seguranca total (8 chunks max)

# Manter compatibilidade (nao usado mais ativamente):
MAX_PAGINAS_CORPO = 500
MAX_CARACTERES_CORPO = 2_000_000


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
    # 1. Extrai texto COMPLETO do PDF (sem limite)
    # ───────────────────────────────────────────────────────────────
    if not pdf_path or not os.path.exists(pdf_path):
        return {'sucesso': False, 'erro': f'PDF nao encontrado: {pdf_path}'}
    
    _log(f"Extraindo texto de {pdf_path}")
    
    texto_corpo = _extrair_texto_completo(pdf_path)
    if not texto_corpo:
        return {'sucesso': False, 'erro': 'falha ao extrair texto do PDF'}
    
    # Limite de seguranca (PDFs muito grandes)
    if len(texto_corpo) > MAX_TOTAL_CHARS:
        texto_corpo = texto_corpo[:MAX_TOTAL_CHARS]
        _log(f"Texto truncado em {MAX_TOTAL_CHARS} chars (limite seguranca)")
    
    # Normaliza espacos: "Anexo      G01" -> "Anexo G01"
    import re as _re_norm
    texto_corpo = _re_norm.sub(r'[ \t]+', ' ', texto_corpo)
    
    _log(f"Texto extraído: {len(texto_corpo):,} chars (apos normalizacao)")
    
    # ───────────────────────────────────────────────────────────────
    # 2. Divide em chunks se necessario + chama Haiku em cada
    # ───────────────────────────────────────────────────────────────
    chunks = _dividir_em_chunks(texto_corpo, CHUNK_SIZE_CHARS, CHUNK_OVERLAP_CHARS)
    _log(f"Texto dividido em {len(chunks)} chunk(s) (chunk={CHUNK_SIZE_CHARS:,} chars, overlap={CHUNK_OVERLAP_CHARS:,})")
    
    anexos_citados = []
    nomes_vistos = set()  # dedup
    custo = 0.0
    tokens_in = 0
    tokens_out = 0
    
    for i, chunk in enumerate(chunks, 1):
        _log(f"Chunk {i}/{len(chunks)}: {len(chunk):,} chars")
        try:
            anexos_chunk, c2, ti, to = _chamar_haiku_detectar(chunk, log_callback=_log)
        except Exception as e:
            _log(f"AVISO: chunk {i} falhou ({e}). Pulando este chunk.")
            continue
        
        custo += c2
        tokens_in += ti
        tokens_out += to
        
        # Dedup: pra cada anexo do chunk, se ainda nao foi visto, adiciona
        for a in anexos_chunk:
            nome = a.get('nome_citado', '').strip()
            nome_norm = _normalizar_nome_anexo(nome)
            if nome_norm and nome_norm not in nomes_vistos:
                nomes_vistos.add(nome_norm)
                anexos_citados.append(a)
        
        _log(f"  -> {len(anexos_chunk)} anexo(s) no chunk, {len(anexos_citados)} total acumulado")
    
    _log(f"Haiku detectou {len(anexos_citados)} anexo(s) citado(s) unicos (custo total ${custo:.4f})")
    
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
    # 4. Pra cada anexo citado, monta candidatos:
    #    - MATCH 1 (nome do arquivo): encontrado direto
    #    - MATCH 2 (texto): precisa validar com Haiku se eh CONTEUDO ou MENCAO
    # ───────────────────────────────────────────────────────────────
    encontrados = []
    faltantes = []
    candidatos_validar_haiku = []   # anexos que aparecem no texto - precisam de validacao
    
    for citado in anexos_citados:
        nome_c = citado.get('nome_citado') or ''
        if not nome_c:
            continue
        
        # MATCH 1: nome bate com algum arquivo baixado? (mais confiavel)
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
            continue
        
        # MATCH 2: aparece literalmente no texto do PDF?
        onde_encontrou = _buscar_no_texto(nome_c, texto_completo_norm)
        if onde_encontrou:
            # Precisa validar: eh CONTEUDO do anexo ou apenas MENCAO?
            candidatos_validar_haiku.append({
                'nome_citado': nome_c,
                'onde': onde_encontrou,
                'contexto_citacao': citado.get('contexto', '')[:200],
            })
        else:
            faltantes.append({
                'nome_citado': nome_c,
                'contexto': citado.get('contexto', '')[:200],
                'motivo': 'nao encontrado no texto do PDF nem nos nomes dos arquivos',
            })
    
    # Se ha candidatos no texto, valida em lote com Haiku
    if candidatos_validar_haiku:
        _log(f"Validando {len(candidatos_validar_haiku)} candidato(s) com Haiku (conteudo vs mencao)...")
        try:
            validacoes, custo2, tin2, tout2 = _validar_conteudo_vs_mencao(
                candidatos_validar_haiku, texto_completo, log_callback=_log
            )
            custo += custo2
            tokens_in += tin2
            tokens_out += tout2
            
            for cand in candidatos_validar_haiku:
                nome_c = cand['nome_citado']
                veredicto = validacoes.get(nome_c, 'incerto')
                if veredicto == 'conteudo':
                    encontrados.append({
                        'nome_citado': nome_c,
                        'onde': cand['onde'] + ' (validado: conteudo presente)',
                        'contexto_citacao': cand['contexto_citacao'],
                    })
                else:
                    # 'mencao' ou 'incerto' -> trata como faltante
                    faltantes.append({
                        'nome_citado': nome_c,
                        'contexto': cand['contexto_citacao'],
                        'motivo': f'citado mas conteudo ausente (Haiku: {veredicto})',
                    })
        except Exception as e:
            _log(f"AVISO: validacao Haiku falhou ({e}). Tratando candidatos como encontrados.")
            # Fallback conservador: marca como encontrado (comportamento antigo)
            for cand in candidatos_validar_haiku:
                encontrados.append({
                    'nome_citado': cand['nome_citado'],
                    'onde': cand['onde'] + ' (validacao haiku falhou)',
                    'contexto_citacao': cand['contexto_citacao'],
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





def _dividir_em_chunks(texto, chunk_size, overlap):
    """
    Divide texto em chunks com overlap.
    
    Ex: texto de 460k, chunk_size=250k, overlap=5k
        -> chunk 1: chars 0     a 250000
        -> chunk 2: chars 245000 a 460000
    
    Tenta cortar em quebra de paragrafo pra nao quebrar anexo no meio.
    """
    if len(texto) <= chunk_size:
        return [texto]
    
    chunks = []
    inicio = 0
    while inicio < len(texto):
        fim_ideal = inicio + chunk_size
        
        if fim_ideal >= len(texto):
            # Ultimo chunk
            chunks.append(texto[inicio:])
            break
        
        # Tenta cortar numa quebra de paragrafo proxima (ate 1000 chars antes do fim_ideal)
        fim = fim_ideal
        for i in range(fim_ideal, max(fim_ideal - 1000, inicio), -1):
            if i < len(texto) and texto[i] == '\n':
                fim = i
                break
        
        chunks.append(texto[inicio:fim])
        # Proximo chunk comeca com overlap pra preservar contexto
        inicio = max(fim - overlap, inicio + 1)
    
    return chunks


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

Sua tarefa: identificar anexos COM IDENTIFICADOR ESPECIFICO que o texto cita.

REGRAS CRITICAS:
1. APENAS liste anexos com IDENTIFICADOR especifico (numero, letra, romano, codigo):
   ✅ ACEITAR: "Anexo I", "Anexo 1.4", "Anexo G01", "Tabela XV", "Mapa 03", "Anexo VII-A"
   ❌ REJEITAR: "mapas em anexo", "tabelas anexas", "do anexo", "no anexo desta lei", "anexo" (sem numero)
   ❌ REJEITAR qualquer referencia generica sem identificador unico

2. Cada anexo deve aparecer UMA UNICA VEZ na lista (deduplicado)
3. Use o nome EXATO como aparece no texto (ex: "Anexo 1.4", "Anexo I", "Tabela XV")
4. Para cada anexo, inclua o contexto onde aparece (frase curta)
5. NAO invente anexos que nao estao no texto
6. Inclua APENAS anexos REGULADOS pela propria lei (nao cite anexos de leis externas)
7. Retorne APENAS JSON valido, sem markdown nem comentarios

FORMATO DE RESPOSTA (JSON estrito):
{{
  "anexos": [
    {{"nome_citado": "Anexo 1.1", "contexto": "trecho onde aparece"}},
    {{"nome_citado": "Tabela XV", "contexto": "..."}}
  ]
}}

EXEMPLOS DE COMO PENSAR:
- Texto: "conforme Anexo I desta Lei" → ACEITAR: "Anexo I"
- Texto: "conforme mapas em anexo" → REJEITAR (sem identificador)
- Texto: "o Anexo 2.1 define os usos" → ACEITAR: "Anexo 2.1"
- Texto: "as tabelas e mapas anexos" → REJEITAR (generico)
- Texto: "Anexo G01" e "Anexo C14" e "Anexo F01" -> ACEITAR todos
- IMPORTANTE: Varra TODO o texto. Pode haver dezenas de anexos diferentes
  espalhados em paginas diferentes. Liste TODOS os que tem identificador
  especifico, mesmo que apareca apenas 1 vez.

CORPO DA LEI:
{texto_corpo}

Retorne APENAS o JSON, sem comentarios ou explicacoes."""
    
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
    """Parse JSON com brace-counting (robusto contra markdown wrapper e control chars)."""
    # Remove possíveis markdown fences
    texto = texto.strip()
    if texto.startswith('```'):
        # Remove ```json e ``` no fim
        texto = re.sub(r'^```(?:json)?\s*', '', texto)
        texto = re.sub(r'\s*```\s*$', '', texto)
    
    # Remove caracteres de controle invalidos no JSON (mantem \n, \r, \t)
    # JSON nao aceita control chars (0x00-0x1F) dentro de strings, exceto \b \f \n \r \t
    # Como o Haiku as vezes retorna esses chars em "contexto", removemos
    texto = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', ' ', texto)
    
    # Tenta parse direto primeiro (strict=False aceita control chars em strings)
    try:
        obj = json.loads(texto, strict=False)
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
        obj = json.loads(texto[inicio:fim+1], strict=False)
    
    anexos = obj.get('anexos', [])
    if not isinstance(anexos, list):
        raise ValueError("'anexos' nao eh lista")
    
    # Normaliza/limpa
    resultado = []
    for a in anexos:
        if isinstance(a, dict) and a.get('nome_citado'):
            nome = str(a['nome_citado']).strip()
            ctx = str(a.get('contexto', '')).strip()
        elif isinstance(a, str):
            nome = a.strip()
            ctx = ''
        else:
            continue
        
        # Pos-filtro: rejeita referencias genericas sem identificador especifico
        if _eh_referencia_generica(nome):
            continue
        
        resultado.append({'nome_citado': nome, 'contexto': ctx})
    return resultado


def _eh_referencia_generica(nome):
    """
    Retorna True se o nome do anexo NAO tem identificador especifico
    (numero, letra romana, codigo). Exemplos rejeitados:
      - "anexo" (sozinho)
      - "mapas em anexo"
      - "tabelas anexas"
      - "do anexo"
      - "anexo desta lei"
    """
    if not nome or len(nome.strip()) < 3:
        return True
    
    nome_lower = nome.lower().strip()
    
    # Black list de expressoes genericas
    GENERICOS = {
        'anexo', 'anexos', 'no anexo', 'do anexo', 'em anexo',
        'mapas em anexo', 'tabelas em anexo', 'tabelas anexas',
        'mapas anexos', 'tabelas e mapas', 'mapas e tabelas',
        'anexo desta lei', 'anexos desta lei',
        'tabelas', 'mapas',
    }
    if nome_lower in GENERICOS:
        return True
    
    # Tem que ter pelo menos UMA das seguintes indicacoes:
    #   - numero arabico: 1, 2, 1.4
    #   - numero romano: I, II, III, IV, V, etc
    #   - codigo: G01, A1, etc
    # Padrao: identificador apos a palavra "anexo"/"tabela"/"mapa"
    
    # Verifica se tem numero
    if re.search(r'\d', nome):
        return False
    
    # Verifica se tem romano (sequencia de I, V, X, L, C, D, M com pelo menos 1 letra)
    # Deve aparecer como palavra isolada apos a categoria
    if re.search(r'\b[IVXLCDM]+\b', nome.upper()):
        return False
    
    # Verifica se tem letra+numero (G01, A1)
    if re.search(r'[A-Z]\d', nome.upper()):
        return False
    
    # Sem identificador especifico -> generico
    return True


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


def _validar_conteudo_vs_mencao(candidatos, texto_completo, log_callback=None):
    """
    Valida se o texto contem o CONTEUDO ou apenas MENCAO de cada anexo candidato.
    Faz CHUNKING pra processar PDFs grandes.
    Combina: se algum chunk diz 'conteudo' -> 'conteudo' final.
    """
    from anthropic import Anthropic
    
    if not candidatos:
        return {}, 0.0, 0, 0
    
    chunks = _dividir_em_chunks(texto_completo, CHUNK_SIZE_CHARS, CHUNK_OVERLAP_CHARS)
    
    if log_callback:
        log_callback(f"Validacao: {len(candidatos)} candidato(s) em {len(chunks)} chunk(s)")
    
    veredictos_finais = {c['nome_citado']: 'incerto' for c in candidatos}
    custo_total = 0.0
    tokens_in_total = 0
    tokens_out_total = 0
    
    for i, chunk in enumerate(chunks, 1):
        try:
            veredictos_chunk, custo_c, tin, tout = _validar_em_um_chunk(
                candidatos, chunk, log_callback, i, len(chunks)
            )
            custo_total += custo_c
            tokens_in_total += tin
            tokens_out_total += tout
            
            for nome, val in veredictos_chunk.items():
                atual = veredictos_finais.get(nome, 'incerto')
                if val == 'conteudo':
                    veredictos_finais[nome] = 'conteudo'
                elif val == 'mencao' and atual != 'conteudo':
                    veredictos_finais[nome] = 'mencao'
        except Exception as e:
            if log_callback:
                log_callback(f"AVISO chunk validacao {i}: {e}")
            continue
    
    return veredictos_finais, custo_total, tokens_in_total, tokens_out_total


def _validar_em_um_chunk(candidatos, chunk_texto, log_callback, num_chunk, total_chunks):
    """Faz a validacao em UM chunk especifico."""
    from anthropic import Anthropic
    
    lista_anexos = "\n".join(f'- "{c["nome_citado"]}"' for c in candidatos)
    
    prompt = f"""Voce esta validando se um trecho de PDF contem o CONTEUDO de certos anexos
ou apenas MENCIONA esses anexos.

DEFINICOES:
- "conteudo" = o trecho tem o ANEXO DESENVOLVIDO (titulo + tabelas/listas/mapas dentro)
- "mencao" = o trecho apenas FAZ REFERENCIA ao anexo (ex: "conforme Anexo X"), sem desenvolver
- "incerto" = nao foi possivel determinar com certeza

ANEXOS A VALIDAR (chunk {num_chunk}/{total_chunks} do PDF):
{lista_anexos}

PARA CADA ANEXO ACIMA, busque no texto abaixo:
- Procure SECAO/TITULO comecando com o nome do anexo (ex: "ANEXO X - Titulo", seguido de conteudo)
- Verifique se o texto desenvolve aquele anexo (tabelas, listas, definicoes)

Se voce encontrar apenas "conforme Anexo X" sem desenvolvimento -> "mencao"
Se voce encontrar a SECAO do Anexo X com seu conteudo -> "conteudo"
Se o anexo NAO aparece neste chunk -> "incerto" (pode estar em outro chunk)

FORMATO DE RESPOSTA (JSON estrito):
{{
  "validacoes": {{
    "Anexo X": "conteudo",
    "Anexo Y": "mencao"
  }}
}}

TEXTO DO PDF (chunk {num_chunk}/{total_chunks}):
{chunk_texto}

Retorne APENAS o JSON, sem comentarios."""
    
    cliente = Anthropic()
    
    if log_callback:
        log_callback(f"  Validacao chunk {num_chunk}/{total_chunks}: {len(prompt):,} chars")
    
    response = cliente.messages.create(
        model=MODELO_HAIKU,
        max_tokens=2000,
        messages=[{"role": "user", "content": prompt}],
    )
    
    texto = response.content[0].text if response.content else ''
    tokens_in = response.usage.input_tokens
    tokens_out = response.usage.output_tokens
    custo = (tokens_in / 1_000_000) * 1.0 + (tokens_out / 1_000_000) * 5.0
    
    if log_callback:
        log_callback(f"    tokens {tokens_in}->{tokens_out}, ${custo:.4f}")
    
    try:
        texto_clean = texto.strip()
        if texto_clean.startswith("```"):
            texto_clean = re.sub(r"^```(?:json)?\s*", "", texto_clean)
            texto_clean = re.sub(r"\s*```\s*$", "", texto_clean)
        texto_clean = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", " ", texto_clean)
        
        try:
            obj = json.loads(texto_clean, strict=False)
        except Exception:
            inicio = texto_clean.find("{")
            nivel = 0; fim = -1
            for i in range(inicio, len(texto_clean)):
                if texto_clean[i] == "{": nivel += 1
                elif texto_clean[i] == "}":
                    nivel -= 1
                    if nivel == 0:
                        fim = i
                        break
            obj = json.loads(texto_clean[inicio:fim+1], strict=False)
        
        validacoes = obj.get("validacoes", {})
        resultado = {}
        for nome, val in validacoes.items():
            val_lower = str(val).lower().strip()
            if val_lower in ("conteudo", "conteudo", "content"):
                resultado[nome] = "conteudo"
            elif val_lower in ("mencao", "mencao", "reference", "mention"):
                resultado[nome] = "mencao"
            else:
                resultado[nome] = "incerto"
        
        return resultado, custo, tokens_in, tokens_out
    except Exception as e:
        logger.error(f"Falha parse JSON chunk {num_chunk}: {e}. Resp: {texto[:300]}")
        return {c['nome_citado']: "incerto" for c in candidatos}, custo, tokens_in, tokens_out
