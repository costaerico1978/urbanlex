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
    Detecta anexos citados no corpo da lei + identifica os FALTANTES.
    
    ALGORITMO NOVO (v3):
      1. Usa etapa2_detectar_fim_corpo + etapa3_quebrar_pdf do pipeline
         pra separar pdf_concatenado em corpo.pdf + anexos.pdf
      2. Haiku CHAMADA 1: detecta anexos APENAS no corpo.pdf
         (mais barato, texto menor)
      3. Pra cada anexo citado:
         - Match A: aparece como ARQUIVO baixado/uploaded? -> ENCONTRADO
         - Match B: aparece como SECAO no anexos.pdf? (regex) -> ENCONTRADO
         - Senao -> FALTANTE
      
      Sem Haiku validacao (era confuso).
    
    Args:
        legislacao_label:    'LC_148_2023'
        pdf_path:            path do pdf_concatenado.pdf
        anexos_baixados:     lista de dicts (sem o corpo)
        log_callback:        opcional
    
    Retorna dict com 'sucesso', 'anexos_citados', 'anexos_encontrados',
    'anexos_faltantes', 'custo_estimado', 'tokens_input', 'tokens_output'.
    """
    def _log(msg):
        logger.info(f"[etapa4.5 {legislacao_label}] {msg}")
        if log_callback:
            log_callback(msg)
    
    if not pdf_path or not os.path.exists(pdf_path):
        return {'sucesso': False, 'erro': f'PDF nao encontrado: {pdf_path}'}
    
    # ───────────────────────────────────────────────────────────────
    # 1. Separa corpo.pdf e anexos.pdf usando o pipeline existente
    # ───────────────────────────────────────────────────────────────
    _log(f"Separando corpo + anexos do PDF...")
    try:
        import pypdf
        from modulos.pipeline_extracao_lei import etapa2_detectar_fim_corpo, etapa3_quebrar_pdf
        
        reader = pypdf.PdfReader(pdf_path)
        n_paginas = len(reader.pages)
        
        # Etapa 2: detecta fim do corpo
        res_e2 = etapa2_detectar_fim_corpo(pdf_path, n_paginas, log_callback=_log)
        fim_corpo = res_e2.get('fim_corpo', 0)
        
        if fim_corpo == 0:
            # Nao identificou artigos numerados (PDF estranho?)
            _log("AVISO: nao detectou fim do corpo. Tratando PDF inteiro como corpo.")
            fim_corpo = n_paginas
        
        # Etapa 3: quebra em corpo.pdf + anexos.pdf
        work_dir = os.path.dirname(pdf_path)
        res_e3 = etapa3_quebrar_pdf(pdf_path, fim_corpo, n_paginas, work_dir, log_callback=_log)
        corpo_pdf_path = res_e3['corpo_pdf']
        anexos_pdf_path = res_e3.get('anexos_pdf')
        
        _log(f"Corpo: pg 1-{fim_corpo} ({fim_corpo} pgs)")
        _log(f"Anexos no PDF: pg {fim_corpo+1}-{n_paginas} ({n_paginas-fim_corpo} pgs)")
    except Exception as e:
        import traceback
        _log(f"ERRO separando corpo/anexos: {e}")
        logger.error(traceback.format_exc())
        return {'sucesso': False, 'erro': f'falha separando: {str(e)[:200]}'}
    
    # ───────────────────────────────────────────────────────────────
    # 2. Extrai texto APENAS do corpo (mais barato)
    # ───────────────────────────────────────────────────────────────
    texto_corpo = _extrair_texto_completo(corpo_pdf_path)
    if not texto_corpo:
        return {'sucesso': False, 'erro': 'falha extraindo texto do corpo'}
    
    # Normaliza espacos
    import re as _re_norm
    texto_corpo = _re_norm.sub(r'[ \t]+', ' ', texto_corpo)
    
    if len(texto_corpo) > MAX_TOTAL_CHARS:
        texto_corpo = texto_corpo[:MAX_TOTAL_CHARS]
        _log(f"Texto corpo truncado em {MAX_TOTAL_CHARS} chars (seguranca)")
    
    _log(f"Texto corpo: {len(texto_corpo):,} chars")
    
    # ───────────────────────────────────────────────────────────────
    # 3. Detecta anexos citados no CORPO (com chunking)
    # ───────────────────────────────────────────────────────────────
    chunks = _dividir_em_chunks(texto_corpo, CHUNK_SIZE_CHARS, CHUNK_OVERLAP_CHARS)
    _log(f"Corpo dividido em {len(chunks)} chunk(s)")
    
    anexos_citados = []
    nomes_vistos = set()
    custo = 0.0
    tokens_in = 0
    tokens_out = 0
    
    for i, chunk in enumerate(chunks, 1):
        _log(f"Chunk {i}/{len(chunks)}: {len(chunk):,} chars")
        try:
            anexos_chunk, c2, ti, to = _chamar_haiku_detectar(chunk, log_callback=_log)
        except Exception as e:
            _log(f"AVISO: chunk {i} falhou ({e}). Pulando.")
            continue
        
        custo += c2
        tokens_in += ti
        tokens_out += to
        
        for a in anexos_chunk:
            nome = a.get('nome_citado', '').strip()
            nome_norm = _normalizar_nome_anexo(nome)
            if nome_norm and nome_norm not in nomes_vistos:
                nomes_vistos.add(nome_norm)
                anexos_citados.append(a)
    
    _log(f"Haiku detectou {len(anexos_citados)} anexo(s) citado(s) unicos (custo ${custo:.4f})")
    
    # ───────────────────────────────────────────────────────────────
    # 4. Cataloga anexos no anexos.pdf E faz match com lista de citados
    #    (Haiku ve PDF visual + recebe lista de citados, retorna blocos com 'citado_como')
    # ───────────────────────────────────────────────────────────────
    catalogo_anexos = []
    custo_catalogo = 0.0
    if anexos_pdf_path and os.path.exists(anexos_pdf_path):
        try:
            lista_citados_str = [a.get('nome_citado', '') for a in anexos_citados if a.get('nome_citado')]
            _log(f"Catalogando + matchando {len(lista_citados_str)} citados no anexos.pdf...")
            res_cat = _catalogar_com_match(
                anexos_pdf_path, fim_corpo, work_dir, lista_citados_str,
                log_callback=_log
            )
            catalogo_anexos = res_cat.get('blocos', [])
            custo_catalogo = res_cat.get('custo', 0.0)
            
            # ─── SALVA catalogo em formato compativel com Etapa 4 do pipeline ───
            # IMPORTANTE: incluir bloco 'corpo_lei' no inicio - a Etapa 5 espera isso
            try:
                import json as _j_e4
                blocos_pipeline = [{
                    'nome': 'corpo_lei',
                    'titulo': 'Corpo da Lei',
                    'inicio': 1,
                    'fim': fim_corpo,
                    'tipo': 'corpo',
                }]
                # Adiciona os blocos catalogados pelo Haiku (anexos/erratas)
                # As paginas ja vieram em referencia ao PDF concatenado
                for _b in res_cat.get('blocos', []):
                    if isinstance(_b, dict):
                        blocos_pipeline.append(_b)
                cache_e4_path = os.path.join(work_dir, 'etapa4_catalogacao.json')
                with open(cache_e4_path, 'w') as _f_e4:
                    _j_e4.dump({
                        'blocos': blocos_pipeline,
                        'tokens_in': res_cat.get('tokens_in', 0),
                        'tokens_out': res_cat.get('tokens_out', 0),
                        'custo': res_cat.get('custo', 0.0),
                    }, _f_e4, ensure_ascii=False, indent=2)
                _log(f"Catalogo salvo em {cache_e4_path} ({len(blocos_pipeline)} blocos: 1 corpo + {len(blocos_pipeline)-1} anexos)")
            except Exception as _e_save_e4:
                _log(f"AVISO: nao salvou etapa4_catalogacao.json: {_e_save_e4}")
            
            # Filtra apenas tipo=anexo (descarta errata, encerramento)
            catalogo_anexos = [b for b in catalogo_anexos if b.get('tipo') == 'anexo']
            
            _log(f"Catalogo: {len(catalogo_anexos)} anexo(s) identificado(s) no PDF")
            for b in catalogo_anexos[:15]:
                cit = b.get('citado_como') or '(nao citado no corpo)'
                _log(f"  - {b.get('titulo', '?')[:60]} (pg {b.get('inicio')}-{b.get('fim')}) -> citado: {cit}")
            
            custo += custo_catalogo
        except Exception as e:
            import traceback
            _log(f"AVISO: catalogacao falhou: {str(e)[:200]}")
            logger.error(traceback.format_exc())
            catalogo_anexos = []
    else:
        _log("Sem anexos.pdf (PDF so tem corpo)")
    
    # Normaliza nomes dos arquivos baixados/uploaded pra match
    nomes_baixados_norm = set()
    for arq in anexos_baixados:
        nome = arq.get('nome') or ''
        nomes_baixados_norm.add(_normalizar_nome_anexo(nome))
    
    # ───────────────────────────────────────────────────────────────
    # 5. Classifica cada anexo citado: ENCONTRADO ou FALTANTE
    # ───────────────────────────────────────────────────────────────
    encontrados = []
    faltantes = []
    
    for citado in anexos_citados:
        nome_c = citado.get('nome_citado') or ''
        if not nome_c:
            continue
        
        nome_c_norm = _normalizar_nome_anexo(nome_c)
        
        # MATCH A: nome casa com algum arquivo baixado/uploaded?
        encontrou_por_arquivo = False
        for nome_b in nomes_baixados_norm:
            if not nome_b or not nome_c_norm:
                continue
            # Match restrito: precisa ter palavras-chave significativas
            if _match_nome_arquivo_restrito(nome_c, nome_b):
                encontrou_por_arquivo = True
                break
        
        if encontrou_por_arquivo:
            encontrados.append({
                'nome_citado': nome_c,
                'onde': 'arquivo_baixado',
                'contexto_citacao': citado.get('contexto', '')[:200],
            })
            continue
        
        # MATCH B: algum bloco do catalogo relacionou este citado em 'citado_como'?
        bloco_relacionado = None
        for b in catalogo_anexos:
            cit = b.get('citado_como')
            if cit and cit.strip().lower() == nome_c.strip().lower():
                bloco_relacionado = b
                break
        
        if bloco_relacionado:
            encontrados.append({
                'nome_citado': nome_c,
                'onde': f"catalogo: \"{bloco_relacionado.get('titulo', '')[:80]}\" (pg {bloco_relacionado.get('inicio')}-{bloco_relacionado.get('fim')})",
                'contexto_citacao': citado.get('contexto', '')[:200],
            })
            continue
        
        # Sem match -> FALTANTE
        faltantes.append({
            'nome_citado': nome_c,
            'contexto': citado.get('contexto', '')[:200],
            'motivo': 'nao encontrado entre arquivos baixados nem como secao em anexos.pdf',
        })
    
    # ───────────────────────────────────────────────────────────────
    # 6. Identifica EXTRAS: blocos no catalogo nao relacionados a citados
    # ───────────────────────────────────────────────────────────────
    extras = []
    for bloco in catalogo_anexos:
        if not bloco.get('citado_como'):
            extras.append({
                'titulo': bloco.get('titulo', bloco.get('nome', '?')),
                'paginas': f"pg {bloco.get('inicio')}-{bloco.get('fim')}",
                'observacao': 'presente no PDF mas nao citado no corpo da lei',
            })
    
    _log(f"Encontrados: {len(encontrados)}/{len(anexos_citados)} | Faltantes: {len(faltantes)} | Extras: {len(extras)}")
    
    return {
        'sucesso': True,
        'anexos_citados': anexos_citados,
        'anexos_encontrados': encontrados,
        'anexos_faltantes': faltantes,
        'anexos_extras': extras,
        'custo_estimado': custo,
        'tokens_input': tokens_in,
        'tokens_output': tokens_out,
    }


def _match_nome_arquivo_restrito(nome_citado, nome_arquivo_norm):
    """
    Match RESTRITIVO entre nome citado (ex: 'Anexo G01', 'Tabela E01') 
    e nome de arquivo normalizado (ex: 'g01_glossario', 'e01_estacionamentos').
    
    Regras:
      - Extrai o identificador especifico (ex: 'G01', 'E01', '1.4')
      - Identificador DEVE aparecer no nome do arquivo
      - Tipo (Anexo, Tabela, Mapa) NAO precisa bater (PDFs nem sempre tem)
      - Mas SE o tipo aparecer no nome do arquivo, precisa bater
    """
    import re as _re_m
    
    nome_c_norm = _normalizar_nome_anexo(nome_citado)
    
    # Extrai identificador (numero/codigo) do citado
    # ex: 'anexo_g01' -> 'g01', 'tabela_e01' -> 'e01', 'anexo_1_4' -> '1_4'
    m = _re_m.search(r'([a-z]?\d+(?:[._]\d+)*)', nome_c_norm)
    if not m:
        return False
    identificador = m.group(1)
    
    # Identificador tem que aparecer no nome do arquivo
    if identificador not in nome_arquivo_norm:
        return False
    
    # Se o identificador casa, o match e valido.
    # NAO rejeitamos por tipo diferente porque o nome do arquivo pode ter palavras
    # descritivas (ex: 'F01_TABELA_Limites_*.pdf' eh o anexo F01, nao a tabela F01)
    return True


def _match_no_catalogo(nome_citado, catalogo_anexos):
    """
    Procura se o nome_citado (ex: 'Anexo G01', 'Anexo 1.1') casa com algum
    bloco no catalogo retornado pela etapa4_catalogar_anexos.
    
    Cada bloco tem: {nome: 'anexo_1.1', titulo: 'Anexo 1.1 - ...', inicio, fim, tipo}
    
    Match: identificador do citado (G01, 1.1, E01) aparece no 'nome' ou 'titulo'
    do bloco do catalogo.
    
    Retorna string descrevendo onde, ou None.
    """
    import re as _re_c
    
    nome_c_norm = _normalizar_nome_anexo(nome_citado)
    
    # Extrai identificador do citado (G01, 1.1, E01, etc)
    m = _re_c.search(r'([a-z]?\d+(?:[._\s-]\d+)*)', nome_c_norm)
    if not m:
        return None
    identificador = m.group(1).replace('_', '.').replace(' ', '.').replace('-', '.')
    
    # Procura no catalogo
    for bloco in catalogo_anexos:
        nome_bloco = _normalizar_nome_anexo(bloco.get('nome', ''))
        titulo_bloco = _normalizar_nome_anexo(bloco.get('titulo', ''))
        
        # Gera variantes do identificador (leading zero + separadores)
        variantes = set()
        # Normaliza identificador pra partes numericas
        partes = _re_c.split(r'[._\s-]', identificador)
        partes = [p for p in partes if p]
        
        if all(p.isdigit() or (p and p[0].isalpha() and p[1:].isdigit()) for p in partes):
            # Versoes com e sem leading zero
            partes_normal = list(partes)
            partes_zero = [p.zfill(2) if p.isdigit() and len(p) == 1 else p for p in partes]
            
            # Gera com separadores: '.', '_', ' '
            for sep in ['.', '_', ' ']:
                variantes.add(sep.join(partes_normal))
                variantes.add(sep.join(partes_zero))
            # Tambem grudado
            variantes.add(''.join(partes_normal))
            variantes.add(''.join(partes_zero))
        else:
            variantes.add(identificador)
        
        # Pra cada variante, busca como subtexto no nome/titulo do bloco
        for var in variantes:
            if not var:
                continue
            # Match: precisa estar como subtexto delimitado (nao parte de numero maior)
            # Usa lookahead/lookbehind: precedido por nao-alfanumerico e seguido por nao-digito
            pattern = _re_c.compile(rf'(?:^|[^a-z0-9]){_re_c.escape(var)}(?:[^0-9]|$)')
            if pattern.search(nome_bloco) or pattern.search(titulo_bloco):
                return f"catalogo: \"{bloco.get('titulo', bloco.get('nome', ''))[:80]}\" (pg {bloco.get('inicio')}-{bloco.get('fim')})"
    
    return None


def _catalogar_com_match(anexos_pdf_path, fim_corpo, work_dir, lista_citados, log_callback=None):
    """
    Cataloga anexos do anexos.pdf usando Gemini Pro 2.5 (chamada unica ou chunks com flag continua).
    Fallback para Haiku se Gemini nao disponivel.
    """
    import subprocess, base64, os as _os_c
    from modulos.pipeline_extracao_lei import MODELO_HAIKU, calcular_custo, parse_json_robusto
    import pypdf

    def _log(msg):
        if log_callback:
            log_callback(msg)

    # Pega n_paginas
    r = subprocess.run(['pdfinfo', anexos_pdf_path], capture_output=True, text=True, timeout=30)
    n_pg_anexos = 0
    for linha in r.stdout.split('\n'):
        if linha.startswith('Pages:'):
            n_pg_anexos = int(linha.split(':')[1].strip())
            break
    if n_pg_anexos == 0:
        raise RuntimeError("Nao consegui ler n_paginas do anexos.pdf")

    # Texto-layout pagina por pagina
    texto_anexos = ""
    for pg in range(1, n_pg_anexos + 1):
        try:
            r2 = subprocess.run(
                ['pdftotext', '-layout', '-f', str(pg), '-l', str(pg), anexos_pdf_path, '-'],
                capture_output=True, text=True, errors='replace', timeout=15
            )
            texto_anexos += f"\n=== PAGINA {pg} ===\n{r2.stdout}"
        except Exception:
            pass

    lista_str = '\n'.join(f'  - "{c}"' for c in lista_citados)
    GEMINI_MAX_PGS = 150
    CHUNK_SIZE_GEM = 100

    # Verificar se Gemini disponivel
    gemini_disponivel = False
    GEMINI_KEY = _os_c.environ.get('GEMINI_API_KEY', '')
    if GEMINI_KEY:
        try:
            from google import genai as _genai
            from google.genai import types as _gtypes
            gemini_disponivel = True
        except ImportError:
            pass

    tokens_in = 0; tokens_out = 0; custo = 0.0
    blocos_raw = []

    if gemini_disponivel:
        # ── GEMINI PRO 2.5 ──
        _client_gem = _genai.Client(api_key=GEMINI_KEY)
        reader_full = pypdf.PdfReader(anexos_pdf_path)

        prompt_base = f"""Voce vai analisar este PDF (anexos de uma lei municipal) e catalogar cada bloco.

TRABALHO 1 — CATALOGAR:
Pra cada bloco (Anexo I, Anexo XVIII, etc), retorne:
  - nome: chave curta snake_case (ex: "anexo_xviii", "errata")
  - titulo: titulo completo como aparece no PDF
  - inicio: pagina de inicio (1-indexado no PDF que voce esta vendo)
  - fim: pagina de fim
  - tipo: "anexo" | "errata" | "encerramento" | "indefinido"
  - relevancia: "ALTA" (tabelas de zoneamento, usos, parametros) | "MEDIA" | "NULA"
  - continua: true se o bloco CLARAMENTE continua no proximo chunk (ultima pagina do PDF nao eh o fim do bloco), false caso contrario

TRABALHO 2 — RELACIONAR COM CITADOS:
A lei cita os seguintes anexos:
{lista_str}

Para cada bloco, adicione:
  - citado_como: texto EXATO da lista acima que corresponde, ou null

IMPORTANTE:
- Se um bloco comeca neste PDF mas claramente continua (ex: tabela cortada no meio), marque continua=true
- Paginas contam a partir de 1 neste PDF
- Retorne APENAS JSON

FORMATO:
{{
  "blocos": [
    {{
      "nome": "anexo_xxi",
      "titulo": "ANEXO XXI - PARAMETROS URBANISTICOS",
      "inicio": 1,
      "fim": 37,
      "tipo": "anexo",
      "relevancia": "ALTA",
      "continua": false,
      "citado_como": "Anexo XXI"
    }}
  ]
}}

=== TEXTO-LAYOUT (referencia) ===
{{texto_layout}}"""

        if n_pg_anexos <= GEMINI_MAX_PGS:
            # Chamada unica
            _log(f"Gemini Pro: {n_pg_anexos} pgs em chamada unica")
            with open(anexos_pdf_path, 'rb') as f:
                pdf_bytes = f.read()
            tl = texto_anexos[:80000]
            prompt = prompt_base.replace('{texto_layout}', tl)
            try:
                resp = _client_gem.models.generate_content(
                    model='gemini-2.5-pro',
                    contents=[
                        _gtypes.Part(text=prompt),
                        _gtypes.Part(inline_data=_gtypes.Blob(mime_type='application/pdf', data=pdf_bytes)),
                        _gtypes.Part(text='Retorne APENAS JSON sem markdown fences.'),
                    ],
                    config=_gtypes.GenerateContentConfig(max_output_tokens=32768, temperature=0.1)
                )
                texto_resp = resp.text or ''
                uso = resp.usage_metadata
                ti = getattr(uso, 'prompt_token_count', 0) or 0
                to = getattr(uso, 'candidates_token_count', 0) or 0
                tokens_in += ti; tokens_out += to
                custo += (ti * 1.25 + to * 10) / 1_000_000
                _log(f"  Gemini OK: {len(texto_resp)} chars, in={ti} out={to}, ${custo:.4f}")
                parsed = parse_json_robusto(texto_resp)
                if parsed and 'blocos' in parsed:
                    blocos_raw = parsed['blocos']
                else:
                    _log("  AVISO: Gemini retornou JSON invalido, usando Haiku como fallback")
                    gemini_disponivel = False
            except Exception as e_gem:
                _log(f"  ERRO Gemini: {e_gem} — usando Haiku como fallback")
                gemini_disponivel = False
        else:
            # Chunks Gemini com flag continua
            n_chunks = (n_pg_anexos + CHUNK_SIZE_GEM - 1) // CHUNK_SIZE_GEM
            _log(f"Gemini Pro: {n_pg_anexos} pgs em {n_chunks} chunk(s) de {CHUNK_SIZE_GEM}pgs")
            bloco_anterior = None
            for ci in range(n_chunks):
                pg_ini_0 = ci * CHUNK_SIZE_GEM
                pg_fim_0 = min((ci + 1) * CHUNK_SIZE_GEM, n_pg_anexos)
                offset = pg_ini_0
                chunk_path = _os_c.path.join(work_dir, f"anexos_chunk_{ci+1}.pdf")
                writer = pypdf.PdfWriter()
                for i in range(pg_ini_0, pg_fim_0):
                    writer.add_page(reader_full.pages[i])
                with open(chunk_path, 'wb') as f:
                    writer.write(f)
                # Texto-layout do chunk
                tl_chunk = ''
                for pg in range(pg_ini_0 + 1, pg_fim_0 + 1):
                    sep = f"\n=== PAGINA {pg} ==="
                    partes = texto_anexos.split(sep)
                    if len(partes) > 1:
                        tl_chunk += sep + partes[1].split("\n=== PAGINA ")[0]
                ctx = ''
                if bloco_anterior and bloco_anterior.get('continua'):
                    ctx = f"\nATENCAO: O bloco '{bloco_anterior.get('titulo','')}' vem do chunk anterior e pode continuar neste PDF. Verifique se continua e ajuste o fim."
                prompt_chunk = prompt_base.replace('{texto_layout}', tl_chunk[:50000]) + ctx
                prompt_chunk += f"\nATENCAO: As paginas deste PDF vao de {pg_ini_0+1} a {pg_fim_0}. Use essa numeracao."
                _log(f"  Chunk {ci+1}/{n_chunks} (pg {pg_ini_0+1}-{pg_fim_0}): Gemini Pro...")
                with open(chunk_path, 'rb') as f:
                    pdf_bytes_ck = f.read()
                try:
                    resp_ck = _client_gem.models.generate_content(
                        model='gemini-2.5-pro',
                        contents=[
                            _gtypes.Part(text=prompt_chunk),
                            _gtypes.Part(inline_data=_gtypes.Blob(mime_type='application/pdf', data=pdf_bytes_ck)),
                            _gtypes.Part(text='Retorne APENAS JSON sem markdown fences.'),
                        ],
                        config=_gtypes.GenerateContentConfig(max_output_tokens=16384, temperature=0.1)
                    )
                    texto_ck = resp_ck.text or ''
                    uso_ck = resp_ck.usage_metadata
                    ti = getattr(uso_ck, 'prompt_token_count', 0) or 0
                    to = getattr(uso_ck, 'candidates_token_count', 0) or 0
                    tokens_in += ti; tokens_out += to
                    custo += (ti * 1.25 + to * 10) / 1_000_000
                    parsed_ck = parse_json_robusto(texto_ck)
                    if parsed_ck and 'blocos' in parsed_ck:
                        for b in parsed_ck['blocos']:
                            b['inicio'] = b.get('inicio', 1) + pg_ini_0
                            b['fim'] = b.get('fim', 1) + pg_ini_0
                            blocos_raw.append(b)
                        if parsed_ck['blocos']:
                            bloco_anterior = parsed_ck['blocos'][-1]
                            bloco_anterior['inicio'] = bloco_anterior['inicio']
                    _log(f"    OK: {len(parsed_ck.get('blocos',[]))} blocos, ${custo:.4f}")
                except Exception as e_ck:
                    _log(f"    ERRO chung {ci+1}: {e_ck}")
            # Merge blocos com continua=true
            blocos_merged = []
            i = 0
            while i < len(blocos_raw):
                b = dict(blocos_raw[i])
                while b.get('continua') and i + 1 < len(blocos_raw):
                    i += 1
                    prox = blocos_raw[i]
                    b['fim'] = prox.get('fim', b['fim'])
                    if not prox.get('continua'):
                        break
                blocos_merged.append(b)
                i += 1
            blocos_raw = blocos_merged

    if not gemini_disponivel:
        # ── HAIKU FALLBACK ──
        import anthropic
        CHUNK_SIZE_H = 30
        reader_full = pypdf.PdfReader(anexos_pdf_path)
        n_chunks_h = (n_pg_anexos + CHUNK_SIZE_H - 1) // CHUNK_SIZE_H
        _log(f"Haiku 4.5 fallback: {n_pg_anexos} pgs em {n_chunks_h} chunk(s) de {CHUNK_SIZE_H}pgs")
        chunks_h = []
        for ci in range(n_chunks_h):
            i_ini = ci * CHUNK_SIZE_H
            i_fim = min((ci + 1) * CHUNK_SIZE_H, n_pg_anexos)
            cp = _os_c.path.join(work_dir, f"anexos_chunk_{ci+1}.pdf")
            w = pypdf.PdfWriter()
            for i in range(i_ini, i_fim):
                w.add_page(reader_full.pages[i])
            with open(cp, 'wb') as f:
                w.write(f)
            chunks_h.append({'path': cp, 'offset': i_ini, 'pg_inicio': i_ini+1, 'pg_fim': i_fim})
        prompt_h = f"""Cataloga os blocos deste PDF (anexos de lei municipal).
Pra cada bloco retorne: nome, titulo, inicio, fim, tipo, citado_como, relevancia, continua.
continua=true se o bloco claramente continua no proximo chunk.
Citados: {lista_str}
Formato: {{"blocos":[{{"nome":"...","titulo":"...","inicio":1,"fim":2,"tipo":"anexo","citado_como":null,"relevancia":"ALTA","continua":false}}]}}
Retorne APENAS JSON.
=== TEXTO-LAYOUT ===
{texto_anexos[:50000]}"""
        client_h = anthropic.Anthropic(api_key=_os_c.environ.get('ANTHROPIC_API_KEY', ''))
        bloco_anterior_h = None
        for ci, ck in enumerate(chunks_h, start=1):
            with open(ck['path'], 'rb') as fck:
                pdf_b64 = base64.b64encode(fck.read()).decode('ascii')
            prompt_ck = prompt_h
            if bloco_anterior_h and bloco_anterior_h.get('continua'):
                prompt_ck += f"\nATENCAO: '{bloco_anterior_h.get('titulo','')}' pode continuar aqui."
            _log(f"  Chunk {ci}/{len(chunks_h)} (pg {ck['pg_inicio']}-{ck['pg_fim']}): Haiku 4.5...")
            try:
                resp_h = ''
                with client_h.messages.stream(
                    model=MODELO_HAIKU, max_tokens=8000,
                    messages=[{'role': 'user', 'content': [
                        {'type': 'document', 'source': {'type': 'base64', 'media_type': 'application/pdf', 'data': pdf_b64}, 'title': f'chunk_{ci}.pdf'},
                        {'type': 'text', 'text': prompt_ck},
                    ]}]
                ) as stream:
                    for delta in stream.text_stream:
                        resp_h += delta
                    final_h = stream.get_final_message()
                ti = final_h.usage.input_tokens; to = final_h.usage.output_tokens
                tokens_in += ti; tokens_out += to
                custo += calcular_custo(ti, to, 'haiku')
                parsed_h = parse_json_robusto(resp_h)
                if parsed_h and 'blocos' in parsed_h:
                    for b in parsed_h['blocos']:
                        b['inicio'] = int(b.get('inicio', 1)) + ck['offset']
                        b['fim'] = int(b.get('fim', 1)) + ck['offset']
                        blocos_raw.append(b)
                    if parsed_h['blocos']:
                        bloco_anterior_h = dict(parsed_h['blocos'][-1])
                        bloco_anterior_h['inicio'] = bloco_anterior_h.get('inicio', 1)
            except Exception as e_h:
                _log(f"  ERRO chunk [ci]: {e_h}")
        # Merge continua
        blocos_merged_h = []
        i = 0
        while i < len(blocos_raw):
            b = dict(blocos_raw[i])
            while b.get('continua') and i + 1 < len(blocos_raw):
                i += 1
                prox = blocos_raw[i]
                b['fim'] = prox.get('fim', b['fim'])
                if not prox.get('continua'):
                    break
            blocos_merged_h.append(b)
            i += 1
        blocos_raw = blocos_merged_h

    _log(f"Catalogacao total: {len(blocos_raw)} bloco(s) | tokens {tokens_in}->{tokens_out}, ${custo:.4f}")

    # Dedup por citado_como/nome (mantém menor inicio)
    visto = {}
    for b in blocos_raw:
        chave = (b.get('citado_como') or b.get('nome') or '').lower().strip()
        if chave and chave in visto:
            if b.get('inicio', 99999) < visto[chave].get('inicio', 99999):
                visto[chave] = b
        elif chave:
            visto[chave] = b
        else:
            visto[id(b)] = b
    blocos_raw = list(visto.values())

    # Soma fim_corpo nas paginas
    blocos = []
    for b in blocos_raw:
        if not isinstance(b, dict):
            continue
        ini = b.get('inicio')
        fim = b.get('fim')
        if not isinstance(ini, int) or not isinstance(fim, int):
            continue
        blocos.append({
            'nome': b.get('nome', f'bloco_{len(blocos)}'),
            'titulo': b.get('titulo', ''),
            'inicio': ini + fim_corpo,
            'fim': fim + fim_corpo,
            'tipo': b.get('tipo', 'indefinido'),
            'citado_como': b.get('citado_como'),
            'relevancia': b.get('relevancia', 'MEDIA'),
        })
    return {
        'blocos': blocos,
        'custo': custo,
        'tokens_in': tokens_in,
        'tokens_out': tokens_out,
    }

def _buscar_secao_anexo(nome_citado, texto_anexos):
    """
    Busca se o anexo aparece como SECAO/TITULO no texto de anexos.pdf.
    
    Padrao tipico: 
      'ANEXO G01' no inicio de linha (apos quebra de pagina)
      'ANEXO G01 - Titulo da Secao'
      'Anexo G01\nGlossario'
    
    Retorna string descrevendo onde achou, ou None.
    """
    import re as _re_b
    
    if not nome_citado or not texto_anexos:
        return None
    
    # Normaliza o texto (lower, sem acento, espacos compactados)
    texto_norm = _normalizar_texto_busca(texto_anexos)
    nome_norm = _normalizar_texto_busca(nome_citado)
    
    # Extrai identificador (G01, E01, 1.4, etc) do nome citado
    nums = _re_b.findall(r'\b([a-z]?\d+(?:[.\-_\s]\d+)*)', nome_norm)
    if not nums:
        return None
    
    # Pra cada identificador, busca padroes de SECAO no texto
    # Padrao 1: 'anexo X' OU 'tabela X' OU 'mapa X' apos quebra de linha
    for num in nums:
        # Variantes do identificador
        num_clean = num.replace('_', ' ').replace('-', ' ')
        
        # Tipo do citado (anexo/tabela/mapa) - usa se houver
        tipo = None
        for t in ('anexo', 'tabela', 'mapa', 'quadro'):
            if t in nome_norm:
                tipo = t
                break
        
        # Padroes a procurar (no inicio de linha, com possivel - depois do numero)
        padroes = []
        if tipo:
            # Padrao especifico do tipo: "anexo g01" / "tabela e01"
            padroes.append(_re_b.compile(rf'\n\s*{tipo}\s+{_re_b.escape(num_clean)}\b'))
            padroes.append(_re_b.compile(rf'\n\s*{tipo}\s+{_re_b.escape(num_clean.replace(" ", "."))}\b'))
        else:
            # Generico: "anexo X" ou "tabela X" ou "mapa X"
            padroes.append(_re_b.compile(rf'\n\s*(?:anexo|tabela|mapa|quadro)\s+{_re_b.escape(num_clean)}\b'))
        
        for pat in padroes:
            m = pat.search(texto_norm)
            if m:
                return f'secao no anexos.pdf: "{m.group(0).strip()}"'
    
    return None


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
    
    prompt = f"""Voce esta validando RIGOROSAMENTE se um trecho de PDF contem o CONTEUDO 
COMPLETO de certos anexos OU apenas MENCIONA esses anexos.

═══ DEFINICOES PRECISAS ═══

"conteudo" = Voce DEVE encontrar TODAS essas evidencias juntas:
   1. Um TITULO DE SECAO claro, como "ANEXO X" ou "ANEXO X - TITULO" iniciando uma pagina/secao
   2. Conteudo DESENVOLVIDO logo apos: tabelas completas, listas numeradas, mapas, 
      glossarios, especificacoes tecnicas
   3. Volume substancial: pelo menos algumas linhas de conteudo proprio do anexo

"mencao" = Apenas REFERENCIAS ao anexo no corpo da lei, exemplos:
   - "conforme Anexo X"
   - "definido no Anexo X"  
   - "ver Anexo X"
   - "Anexo X integrante desta Lei"
   - SEM o anexo desenvolvido em si

"incerto" = Nao encontrou NEM o titulo da secao NEM referencias claras no chunk
   (pode estar em outro chunk)

═══ REGRA CRITICA ═══

VOCE NAO PODE INFERIR que um anexo esta presente apenas porque outros anexos estao.
CADA ANEXO deve ser avaliado INDIVIDUALMENTE.

Se a lei MENCIONA "Anexo C14" mas voce nao encontra a SECAO "ANEXO C14" desenvolvida 
no texto -> "mencao" (NAO "conteudo").

Se voce ve "ANEXO G01 - GLOSSARIO" seguido de termos definidos -> "conteudo" PARA G01.
Isso NAO significa que "Anexo C14" tambem esta presente. C14 precisa de PROVA propria.

═══ ANEXOS A VALIDAR (chunk {num_chunk}/{total_chunks}) ═══
{lista_anexos}

═══ TEXTO DO PDF (chunk {num_chunk}/{total_chunks}) ═══
{chunk_texto}

═══ FORMATO DE RESPOSTA (JSON estrito) ═══

Para CADA anexo da lista acima, retorne UMA das tres opcoes:
{{
  "validacoes": {{
    "Anexo X": "conteudo",
    "Anexo Y": "mencao",
    "Anexo Z": "incerto"
  }}
}}

Retorne APENAS o JSON, sem comentarios. Seja CONSERVADOR: na duvida, prefira "mencao" sobre "conteudo"."""
    
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
