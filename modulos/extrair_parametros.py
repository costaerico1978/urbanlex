"""
extrair_parametros.py
Extrai parâmetros urbanísticos de uma legislação preparada.

Recebe o ZIP gerado por preparar_legislacao.py:
  {nome}_concat_catalogo.zip
    ├── {nome}_concatenado.pdf
    └── {nome}_catalogo.json

API pública:
  extrair(zip_path, work_dir, log_callback) -> dict
  extrair_por_bloco(bloco, pdf_unico, contexto, categoria_usos, prompt, work_dir, log_callback) -> (str, float, int, int)
  dividir_bloco_grande(bloco, max_chars, pgs_por_sub) -> list
  consolidar(work_dir, zonas_validas, log_callback) -> dict
  mesclar_leis_externas(estado, log_callback) -> dict
"""
import os, re, json, time, copy, base64, logging, subprocess, zipfile
import pypdf

logger = logging.getLogger(__name__)

GEMINI_RETRIES = 3
GEMINI_RETRY_WAIT = 15
GEMINI_MAX_PAGES_PER_CALL = 150

# Siglas de zonas válidas (filtro anti-falso-positivo)
ZONAS_VALIDAS_DEFAULT = set()  # vazio = aceita tudo

CAMPOS_MERGE = [
    'usos_permitidos', 'parametros_gerais', 'parametros_por_uso',
    'variacoes', 'acrescimos_extraordinarios', 'hierarquia',
    'metodologia_area_computavel', 'afastamentos_crescentes',
]


# ─────────────────────────────────────────────────────────────────────────────
# UTILITÁRIOS INTERNOS
# ─────────────────────────────────────────────────────────────────────────────

def _log(msg, cb=None):
    logger.info(msg)
    if cb:
        cb(msg)


def _gemini_client():
    key = os.environ.get('GEMINI_API_KEY', '')
    if not key:
        raise RuntimeError("GEMINI_API_KEY nao configurada")
    try:
        from google import genai as _gd
        from google.genai import types as _gt
        return _gd.Client(api_key=key), _gt
    except ImportError:
        raise RuntimeError("google-genai nao instalado")


def _gemini_chamar(client, gt, contents, max_tokens=65536):
    ultimo_erro = None
    for tentativa in range(GEMINI_RETRIES):
        try:
            return client.models.generate_content(
                model='gemini-2.5-pro',
                contents=contents,
                config=gt.GenerateContentConfig(
                    max_output_tokens=max_tokens,
                    temperature=0.1
                )
            )
        except Exception as e:
            ultimo_erro = e
            if tentativa < GEMINI_RETRIES - 1:
                logger.warning(f"Gemini tentativa {tentativa+1} falhou ({e}), aguardando {GEMINI_RETRY_WAIT}s...")
                time.sleep(GEMINI_RETRY_WAIT)
    raise RuntimeError(f"Gemini falhou apos {GEMINI_RETRIES} tentativas: {ultimo_erro}")


def _custo_gemini(tokens_in, tokens_out):
    return (tokens_in * 1.25 + tokens_out * 10) / 1_000_000


def _parse_json(texto):
    if not texto:
        return None
    texto = texto.strip()
    for fence in ['```json', '```']:
        if fence in texto:
            texto = texto.split(fence, 1)[-1].rsplit('```', 1)[0].strip()
    try:
        return json.loads(texto)
    except Exception:
        pass
    start = texto.find('{')
    if start == -1:
        return None
    depth = 0
    for i, c in enumerate(texto[start:], start):
        if c == '{':
            depth += 1
        elif c == '}':
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(texto[start:i+1])
                except Exception:
                    pass
    return None


def _carregar_prompt():
    """Carrega prompt_v15.md do diretório do projeto."""
    bases = [
        os.path.join(os.path.dirname(__file__), '..', 'prompts', 'prompt_v15.md'),
        '/var/www/urbanlex/prompts/prompt_v15.md',
    ]
    for p in bases:
        if os.path.exists(p):
            return open(p, encoding='utf-8').read()
    raise RuntimeError("prompt_v15.md nao encontrado")


def _texto_layout_paginas(pdf_path, pg_ini, pg_fim):
    """Extrai texto -layout de um intervalo de páginas."""
    partes = []
    for pg in range(pg_ini, pg_fim + 1):
        try:
            r = subprocess.run(
                ['pdftotext', '-layout', '-f', str(pg), '-l', str(pg), pdf_path, '-'],
                capture_output=True, text=True, errors='replace', timeout=15
            )
            partes.append(f"=== PAGINA {pg} ===\n{r.stdout}")
        except Exception:
            partes.append(f"=== PAGINA {pg} ===\n")
    return '\n\n'.join(partes)


def _quebrar_pdf(origem, ini, fim, destino):
    """Quebra PDF de uma faixa de páginas (1-indexado)."""
    reader = pypdf.PdfReader(origem)
    writer = pypdf.PdfWriter()
    for i in range(ini - 1, fim):
        if i < len(reader.pages):
            writer.add_page(reader.pages[i])
    with open(destino, 'wb') as f:
        writer.write(f)


def _extrair_sigla_zona(z):
    """Extrai sigla da zona do dict."""
    if not isinstance(z, dict):
        return ''
    for campo in ('sigla', 'zona', 'nome'):
        v = z.get(campo)
        if v and isinstance(v, str):
            return v.strip().upper()
    # Suporte a hierarquia: {'UT1': 'ZCA1', 'UT2': 'A1'} -> 'ZCA1|A1'
    hier = z.get('hierarquia')
    if isinstance(hier, dict) and hier:
        partes = [str(v) for v in hier.values() if v]
        if partes:
            return '|'.join(partes).upper()
    return ''


def _eh_zona_real(sigla, zonas_validas=None):
    """Verifica se é uma sigla de zona válida."""
    if not sigla or len(sigla) < 2:
        return False
    # Rejeita strings claramente não sendo zonas
    invalidos = {'ART', 'LEI', 'DEC', 'PAR', 'CAP', 'SEC', 'SUB', 'INC', 'ALI', 'OBS', 'TAB', 'ANX'}
    if sigla in invalidos:
        return False
    if zonas_validas:
        return sigla in zonas_validas
    return True


def _eh_anexo_de_usos(titulo):
    """Detecta se o título indica anexo de usos."""
    if not titulo:
        return False
    t = titulo.lower()
    palavras = ['uso', 'atividade', 'funcao', 'função', 'permitid', 'proibid', 'admitid']
    return any(p in t for p in palavras)


def _prio_bloco(b):
    """Define ordem de processamento dos blocos (-1 = pular)."""
    nome = b['nome']
    relev = (b.get('relevancia') or '').upper().strip()
    if relev == 'NULA':
        return -1
    if nome == 'corpo_lei':
        return -1  # processado separadamente
    if nome == 'encerramento':
        return -1
    if 'errata' in nome.lower():
        return 99
    if b.get('ancora_parametros'):
        return 1   # ancora de parâmetros primeiro
    if b.get('ancora_usos'):
        return 2   # ancora de usos segundo
    if relev == 'ALTA':
        return 10
    if relev == 'MEDIA':
        return 20
    return 50


def merge_profundo(atual, novo):
    """Merge profundo de dois dicts — novo completa null do atual."""
    if not isinstance(atual, dict) or not isinstance(novo, dict):
        return
    for k, v in novo.items():
        if k not in atual or atual[k] in (None, {}, []):
            atual[k] = v
        elif isinstance(atual[k], dict) and isinstance(v, dict):
            merge_profundo(atual[k], v)


def _atualizar_estado(estado, parsed):
    """Acumula parsed no estado global."""
    if not parsed:
        return
    leg = parsed.get('legislacao')
    if leg and isinstance(leg, dict) and not estado.get('legislacao'):
        estado['legislacao'] = leg
    if leg and isinstance(leg, dict):
        for m in (leg.get('modificacoes') or []):
            if isinstance(m, dict):
                k = (m.get('alvo'), m.get('dispositivo'))
                if k not in [(x.get('alvo'), x.get('dispositivo')) for x in estado.get('modificacoes', [])]:
                    estado.setdefault('modificacoes', []).append(m)
    for z in (parsed.get('zonas') or []):
        if not isinstance(z, dict):
            continue
        sigla = _extrair_sigla_zona(z)
        if not sigla:
            continue
        if sigla not in estado['zonas']:
            estado['zonas'][sigla] = z
        else:
            ex = estado['zonas'][sigla]
            for campo in CAMPOS_MERGE:
                if ex.get(campo) in (None, {}, []) and z.get(campo) not in (None, {}, []):
                    ex[campo] = z[campo]
                elif isinstance(ex.get(campo), dict) and isinstance(z.get(campo), dict):
                    merge_profundo(ex[campo], z[campo])
    # Mapeamento de usos
    for z in (parsed.get('zonas') or []):
        if not isinstance(z, dict):
            continue
        sigla = _extrair_sigla_zona(z)
        if not sigla:
            continue
        usos = z.get('usos_permitidos')
        if usos and isinstance(usos, dict):
            usos_sim = [u for u, d in usos.items()
                        if isinstance(d, dict) and (d.get('status') or '').upper() in ('SIM', 'CONDICIONADO')]
            if usos_sim:
                estado.setdefault('usos_por_zona', {})[sigla] = usos_sim


def _gerar_contexto(estado):
    """Constrói contexto evolutivo a partir do estado acumulado."""
    linhas = ["=== CONTEXTO ACUMULADO (chamadas anteriores) ==="]
    if estado.get('legislacao'):
        l = estado['legislacao']
        linhas.append(f"Lei: {l.get('tipo')} {l.get('numero')}/{l.get('ano')} ({l.get('municipio')}/{l.get('estado')})")
    usos_zona = estado.get('usos_por_zona', {})
    if usos_zona:
        linhas.append(f"\nMapeamento de usos por zona:")
        for sigla, usos in sorted(usos_zona.items()):
            linhas.append(f"  {sigla}: {', '.join(usos)}")
    if estado.get('zonas'):
        linhas.append(f"\nZonas processadas: {len(estado['zonas'])}")
        for sigla in sorted(estado['zonas'].keys()):
            z = estado['zonas'][sigla]
            tem_params = bool(z.get('parametros_gerais') or z.get('parametros_por_uso'))
            tem_usos = sigla in usos_zona
            linhas.append(f"  {sigla}: usos={'SIM' if tem_usos else 'null'}, params={'SIM' if tem_params else 'null'}")
    linhas.append("\nINSTRUCAO: complete o que esta null. Adicione zonas novas. Para parametros, verifique se variam por uso.")
    return "\n".join(linhas)


# ─────────────────────────────────────────────────────────────────────────────
# DIVIDIR BLOCO GRANDE
# ─────────────────────────────────────────────────────────────────────────────

def dividir_bloco_grande(bloco, max_chars=50000, pgs_por_sub=10):
    """Divide bloco grande em sub-blocos de pgs_por_sub páginas."""
    if len(bloco.get('texto_layout', '')) <= max_chars:
        return [bloco]
    subs = []
    i = bloco['inicio']
    idx = 0
    while i <= bloco['fim']:
        f = min(i + pgs_por_sub - 1, bloco['fim'])
        sub = dict(bloco)
        sub['inicio'] = i
        sub['fim'] = f
        sub['nome'] = f"{bloco['nome']}_sub{idx+1}"
        sub['_sub_de'] = bloco['nome']
        sub['_sub_idx'] = idx
        subs.append(sub)
        i = f + 1
        idx += 1
    return subs


# ─────────────────────────────────────────────────────────────────────────────
# EXTRAIR POR BLOCO
# ─────────────────────────────────────────────────────────────────────────────

def extrair_por_bloco(bloco, pdf_unico, contexto, categoria_usos, prompt, work_dir, log_callback=None):
    """
    Processa um bloco com Gemini Pro.
    Retorna: (texto_resposta, tempo, tokens_in, tokens_out)
    """
    client, gt = _gemini_client()

    # Cache
    nome_safe = bloco['nome'].replace('.', '_').replace(' ', '_').replace('/', '_')
    cache_path = os.path.join(work_dir, f"extr_{nome_safe}.txt")
    if os.path.exists(cache_path) and os.path.getsize(cache_path) > 1000:
        _log(f"    [{bloco['nome']}] CACHE HIT: {os.path.getsize(cache_path)} chars", log_callback)
        return open(cache_path).read(), 0, 0, 0

    # Monta instrução de contexto
    instrucao_parts = []
    if contexto:
        instrucao_parts.append(contexto)
    if categoria_usos:
        linhas_cat = ["\n\nMAPEAMENTO DE CATEGORIAS DE USO (detectado na catalogacao):"]
        for cu in categoria_usos:
            linhas_cat.append(f"  - {cu.get('categoria','?')} = {cu.get('usos_reais',[])} (ref: {cu.get('dispositivo','?')})")
        instrucao_parts.append("\n".join(linhas_cat))
    assunto = bloco.get('assunto') or bloco.get('citado_como') or ''
    if assunto:
        instrucao_parts.append(f"\n\nATENCAO: Este bloco e '{bloco['nome']}' — assunto: {assunto}. Extraia TODAS as zonas, parametros e usos.")
    instrucao = "\n".join(instrucao_parts) if instrucao_parts else "Analise o PDF seguindo as instrucoes."
    instrucao += "\nRetorne APENAS JSON sem markdown fences. COMPACTO: omita campos null."

    # PDF do bloco
    pdf_bloco = bloco.get('pdf_path')
    if not pdf_bloco or not os.path.exists(pdf_bloco):
        nome_safe2 = bloco['nome'].replace('.', '_').replace(' ', '_').replace('/', '_')
        pdf_bloco = os.path.join(work_dir, f"bloco_{nome_safe2}.pdf")
        _quebrar_pdf(pdf_unico, bloco['inicio'], bloco['fim'], pdf_bloco)

    with open(pdf_bloco, 'rb') as f:
        pdf_bytes = f.read()

    tl = bloco.get('texto_layout') or _texto_layout_paginas(pdf_unico, bloco['inicio'], bloco['fim'])

    _log(f"    [{bloco['nome']}] Chamando Gemini Pro 2.5...", log_callback)
    t0 = time.time()

    conteudo = [gt.Part(text=prompt)]
    if tl:
        conteudo.append(gt.Part(text="=== TEXTO-LAYOUT DO PDF ===\n" + tl[:50000]))
    conteudo.append(gt.Part(inline_data=gt.Blob(mime_type='application/pdf', data=pdf_bytes)))
    conteudo.append(gt.Part(text=instrucao))

    resp = _gemini_chamar(client, gt, conteudo)
    texto = resp.text or ''
    uso = resp.usage_metadata
    tokens_in = getattr(uso, 'prompt_token_count', 0) or 0
    tokens_out = getattr(uso, 'candidates_token_count', 0) or 0
    elapsed = time.time() - t0
    custo = _custo_gemini(tokens_in, tokens_out)
    _log(f"    [{bloco['nome']}] OK: {len(texto)} chars, {elapsed:.1f}s, ${custo:.4f}", log_callback)

    if texto:
        open(cache_path, 'w', encoding='utf-8').write(texto)

    return texto, elapsed, tokens_in, tokens_out


# ─────────────────────────────────────────────────────────────────────────────
# CONSOLIDAR
# ─────────────────────────────────────────────────────────────────────────────

def consolidar(work_dir, zonas_validas=None, log_callback=None):
    """
    Merge dos JSONs de todos os blocos extraídos (arquivos extr_*.txt no work_dir).
    Aplica filtro de zonas válidas e merge profundo.
    Retorna estado consolidado.
    """
    _log("Consolidando resultados...", log_callback)
    t0 = time.time()
    if zonas_validas is None:
        zonas_validas = ZONAS_VALIDAS_DEFAULT

    # Carrega todos os arquivos extr_*.txt em ordem (corpo primeiro)
    ordem = []
    corpo_path = os.path.join(work_dir, 'extr_corpo_lei.txt')
    if os.path.exists(corpo_path):
        ordem.append('corpo_lei')

    for arq in sorted(os.listdir(work_dir)):
        if arq.startswith('extr_') and arq.endswith('.txt'):
            nome = arq[len('extr_'):-len('.txt')]
            if nome != 'corpo_lei' and nome not in ordem:
                ordem.append(nome)

    _log(f"  {len(ordem)} blocos para consolidar: {ordem}", log_callback)

    estado = {
        'legislacao': None,
        'zonas': {},
        'modificacoes': [],
        'refs_externas': [],
        'usos_por_zona': {},
    }
    descartados = set()

    for nome in ordem:
        path = os.path.join(work_dir, f'extr_{nome}.txt')
        if not os.path.exists(path):
            continue
        try:
            parsed = _parse_json(open(path, encoding='utf-8').read())
        except Exception:
            continue
        if not parsed:
            continue

        # Filtrar zonas inválidas
        zonas_filtradas = []
        for z in (parsed.get('zonas') or []):
            if not isinstance(z, dict):
                continue
            sigla = _extrair_sigla_zona(z)
            if not _eh_zona_real(sigla, zonas_validas or None):
                if sigla:
                    descartados.add(sigla)
                continue
            zonas_filtradas.append(z)
        parsed['zonas'] = zonas_filtradas

        _atualizar_estado(estado, parsed)
        n_zonas = len(estado['zonas'])
        _log(f"  {nome:30s} → zonas total: {n_zonas}", log_callback)

    _log(f"  Consolidado: {len(estado['zonas'])} zonas | descartados: {sorted(descartados) or 'nenhum'}", log_callback)
    _log(f"  Tempo: {time.time()-t0:.1f}s", log_callback)
    return estado


# ─────────────────────────────────────────────────────────────────────────────
# MESCLAR LEIS EXTERNAS
# ─────────────────────────────────────────────────────────────────────────────

def mesclar_leis_externas(estado, log_callback=None):
    """
    Identifica referências a parâmetros definidos em leis externas
    e marca como NI_LEI_EXTERNA no estado.
    """
    _log("Mesclando referências de leis externas...", log_callback)
    marcados = 0
    for sigla, zona in estado.get('zonas', {}).items():
        ppu = zona.get('parametros_por_uso') or {}
        for uso, params in ppu.items():
            if not isinstance(params, dict):
                continue
            for param, val in params.items():
                if isinstance(val, str) and ('lei' in val.lower() or 'decreto' in val.lower() or 'norma' in val.lower()):
                    params[param] = 'NI_LEI_EXTERNA'
                    marcados += 1
    _log(f"  {marcados} referência(s) marcadas como NI_LEI_EXTERNA", log_callback)
    return estado


# ─────────────────────────────────────────────────────────────────────────────
# EXPANDIR PARAMETROS POR USO
# ─────────────────────────────────────────────────────────────────────────────

def _expandir_por_uso(estado, log_callback=None):
    """Replica params gerais para cada uso permitido quando não há params específicos."""
    expandidas = 0
    for sigla, zona in estado.get('zonas', {}).items():
        usos = zona.get('usos_permitidos') or {}
        params_gerais = zona.get('parametros_gerais') or {}
        if not usos or not params_gerais:
            continue
        usos_permitidos = [
            u for u, d in usos.items()
            if isinstance(d, dict) and (d.get('status') or '').upper() in ('SIM', 'CONDICIONADO')
        ]
        if not usos_permitidos:
            continue
        ppu = zona.get('parametros_por_uso')
        if isinstance(ppu, dict) and ppu:
            for uso in usos_permitidos:
                if uso not in ppu:
                    ppu[uso] = copy.deepcopy(params_gerais)
        else:
            zona['parametros_por_uso'] = {u: copy.deepcopy(params_gerais) for u in usos_permitidos}
        expandidas += 1
    _log(f"  Zonas expandidas: {expandidas}", log_callback)
    return expandidas


# ─────────────────────────────────────────────────────────────────────────────
# EXTRAIR (ORQUESTRADOR PRINCIPAL)
# ─────────────────────────────────────────────────────────────────────────────

def extrair(zip_path, work_dir, log_callback=None):
    """
    Orquestrador principal — extrai parâmetros urbanísticos de uma legislação.

    Recebe o ZIP gerado por preparar_legislacao.py:
      {nome}_concat_catalogo.zip
        ├── {nome}_concatenado.pdf
        └── {nome}_catalogo.json

    Salva resultado_final.json em work_dir.
    Retorna dict com estado + métricas.
    """
    t0 = time.time()
    os.makedirs(work_dir, exist_ok=True)

    _log("=" * 60, log_callback)
    _log("EXTRAINDO PARÂMETROS", log_callback)
    _log("=" * 60, log_callback)

    # 1. Extrair PDF e JSON do ZIP
    _log("Extraindo ZIP...", log_callback)
    pdf_unico = None
    catalogo = None

    with zipfile.ZipFile(zip_path) as zf:
        for nome_arq in zf.namelist():
            dest = os.path.join(work_dir, os.path.basename(nome_arq))
            with zf.open(nome_arq) as src, open(dest, 'wb') as dst:
                dst.write(src.read())
            if nome_arq.endswith('_concatenado.pdf') or nome_arq.endswith('.pdf'):
                pdf_unico = dest
            elif nome_arq.endswith('_catalogo.json') or nome_arq.endswith('.json'):
                catalogo_path = dest

    if not pdf_unico or not os.path.exists(pdf_unico):
        raise RuntimeError("PDF concatenado nao encontrado no ZIP")
    if not os.path.exists(catalogo_path):
        raise RuntimeError("JSON catalogo nao encontrado no ZIP")

    with open(catalogo_path, encoding='utf-8') as f:
        catalogo = json.load(f)

    blocos = catalogo.get('blocos', [])
    categoria_usos = catalogo.get('categoria_usos', [])
    _log(f"  {len(blocos)} blocos | {len(categoria_usos)} mapeamento(s) de uso", log_callback)

    # 2. Gerar texto_por_pg (cache do layout do PDF)
    _log("Gerando texto por página...", log_callback)
    n_pgs = len(pypdf.PdfReader(pdf_unico).pages)
    texto_por_pg = {}
    for pg in range(1, n_pgs + 1):
        try:
            r = subprocess.run(
                ['pdftotext', '-layout', '-f', str(pg), '-l', str(pg), pdf_unico, '-'],
                capture_output=True, text=True, errors='replace', timeout=15
            )
            texto_por_pg[pg] = r.stdout
        except Exception:
            texto_por_pg[pg] = ''

    # 3. Preparar PDFs e textos de cada bloco
    _log("Preparando blocos...", log_callback)
    for b in blocos:
        nome_safe = b['nome'].replace('.', '_').replace(' ', '_').replace('/', '_')
        b['pdf_path'] = os.path.join(work_dir, f"bloco_{nome_safe}.pdf")
        _quebrar_pdf(pdf_unico, b['inicio'], b['fim'], b['pdf_path'])
        b['texto_layout'] = '\n'.join(
            f"=== PAGINA {pg} ===\n{texto_por_pg.get(pg, '')}"
            for pg in range(b['inicio'], b['fim'] + 1)
        )
        n_p = b['fim'] - b['inicio'] + 1
        _log(f"  {b['nome']:30s} pgs {b['inicio']:3d}-{b['fim']:3d} | {n_p}pgs | {len(b['texto_layout'])} chars", log_callback)

    # 4. Carregar prompt
    prompt = _carregar_prompt()
    _log(f"  Prompt: {len(prompt)} chars", log_callback)

    # 5. Extrair bloco por bloco (evolutivo)
    _log("Extraindo dados...", log_callback)
    estado = {
        'legislacao': None,
        'zonas': {},
        'modificacoes': [],
        'refs_externas': [],
        'usos_por_zona': {},
    }
    total_tempo = 0; total_in = 0; total_out = 0

    # Corpo primeiro
    corpo = next((b for b in blocos if b['nome'] == 'corpo_lei'), None)
    if corpo:
        resp, t, ti, to = extrair_por_bloco(corpo, pdf_unico, None, categoria_usos, prompt, work_dir, log_callback)
        total_tempo += t; total_in += ti; total_out += to
        parsed = _parse_json(resp or '')
        if parsed:
            _atualizar_estado(estado, parsed)
            _log(f"  Corpo: {len(estado['zonas'])} zonas", log_callback)

    # Demais blocos em ordem de prioridade, expandindo grandes
    blocos_processar = sorted(
        [b for b in blocos if _prio_bloco(b) >= 0],
        key=_prio_bloco
    )

    blocos_expandidos = []
    for b in blocos_processar:
        subs = dividir_bloco_grande(b)
        if len(subs) > 1:
            _log(f"  {b['nome']}: dividido em {len(subs)} sub-blocos", log_callback)
            for sub in subs:
                nome_safe = sub['nome'].replace('.', '_').replace(' ', '_').replace('/', '_')
                sub['pdf_path'] = os.path.join(work_dir, f"bloco_{nome_safe}.pdf")
                _quebrar_pdf(pdf_unico, sub['inicio'], sub['fim'], sub['pdf_path'])
                sub['texto_layout'] = '\n'.join(
                    f"=== PAGINA {pg} ===\n{texto_por_pg.get(pg, '')}"
                    for pg in range(sub['inicio'], sub['fim'] + 1)
                )
        blocos_expandidos.extend(subs)

    for b in blocos_expandidos:
        ctx = _gerar_contexto(estado) if (estado['zonas'] or estado['legislacao']) else None
        resp, t, ti, to = extrair_por_bloco(b, pdf_unico, ctx, categoria_usos, prompt, work_dir, log_callback)
        total_tempo += t; total_in += ti; total_out += to
        parsed = _parse_json(resp or '')
        if parsed:
            _atualizar_estado(estado, parsed)
            _log(f"  Zonas total: {len(estado['zonas'])}", log_callback)

    # 6. Consolidar
    estado_consolidado = consolidar(work_dir, log_callback=log_callback)

    # 7. Expandir parametros por uso
    _expandir_por_uso(estado_consolidado, log_callback)

    # 8. Mesclar leis externas
    mesclar_leis_externas(estado_consolidado, log_callback)

    # 9. Salvar resultado
    custo = _custo_gemini(total_in, total_out)
    metricas = {
        'tempo': time.time() - t0,
        'tokens_in': total_in,
        'tokens_out': total_out,
        'custo': custo,
        'n_zonas': len(estado_consolidado['zonas']),
    }

    resultado_path = os.path.join(work_dir, 'resultado_final.json')
    with open(resultado_path, 'w', encoding='utf-8') as f:
        json.dump({'estado': estado_consolidado, 'metricas': metricas}, f, ensure_ascii=False, indent=2)

    _log(f"EXTRAÇÃO CONCLUÍDA em {metricas['tempo']:.1f}s | {metricas['n_zonas']} zonas | ${custo:.4f}", log_callback)
    _log(f"Salvo: {resultado_path}", log_callback)

    return {
        'sucesso': True,
        'estado': estado_consolidado,
        'resultado_path': resultado_path,
        'metricas': metricas,
    }
def consolidar(work_dir, zonas_validas=None, log_callback=None):
    """
    Merge dos JSONs de todos os blocos extraídos (arquivos extr_*.txt no work_dir).
    Aplica filtro de zonas válidas e merge profundo.
    Retorna estado consolidado.
    """
    _log("Consolidando resultados...", log_callback)
    t0 = time.time()
    if zonas_validas is None:
        zonas_validas = ZONAS_VALIDAS_DEFAULT

    # Carrega todos os arquivos extr_*.txt em ordem (corpo primeiro)
    ordem = []
    corpo_path = os.path.join(work_dir, 'extr_corpo_lei.txt')
    if os.path.exists(corpo_path):
        ordem.append('corpo_lei')

    for arq in sorted(os.listdir(work_dir)):
        if arq.startswith('extr_') and arq.endswith('.txt'):
            nome = arq[len('extr_'):-len('.txt')]
            if nome != 'corpo_lei' and nome not in ordem:
                ordem.append(nome)

    _log(f"  {len(ordem)} blocos para consolidar: {ordem}", log_callback)

    estado = {
        'legislacao': None,
        'zonas': {},
        'modificacoes': [],
        'refs_externas': [],
        'usos_por_zona': {},
    }
    descartados = set()

    for nome in ordem:
        path = os.path.join(work_dir, f'extr_{nome}.txt')
        if not os.path.exists(path):
            continue
        try:
            parsed = _parse_json(open(path, encoding='utf-8').read())
        except Exception:
            continue
        if not parsed:
            continue

        # Filtrar zonas inválidas
        zonas_filtradas = []
        for z in (parsed.get('zonas') or []):
            if not isinstance(z, dict):
                continue
            sigla = _extrair_sigla_zona(z)
            if not _eh_zona_real(sigla, zonas_validas or None):
                if sigla:
                    descartados.add(sigla)
                continue
            zonas_filtradas.append(z)
        parsed['zonas'] = zonas_filtradas

        _atualizar_estado(estado, parsed)
        n_zonas = len(estado['zonas'])
        _log(f"  {nome:30s} → zonas total: {n_zonas}", log_callback)

    _log(f"  Consolidado: {len(estado['zonas'])} zonas | descartados: {sorted(descartados) or 'nenhum'}", log_callback)
    _log(f"  Tempo: {time.time()-t0:.1f}s", log_callback)
    return estado


# ─────────────────────────────────────────────────────────────────────────────
# MESCLAR LEIS EXTERNAS
# ─────────────────────────────────────────────────────────────────────────────

def mesclar_leis_externas(estado, log_callback=None):
    """
    Identifica referências a parâmetros definidos em leis externas
    e marca como NI_LEI_EXTERNA no estado.
    """
    _log("Mesclando referências de leis externas...", log_callback)
    marcados = 0
    for sigla, zona in estado.get('zonas', {}).items():
        ppu = zona.get('parametros_por_uso') or {}
        for uso, params in ppu.items():
            if not isinstance(params, dict):
                continue
            for param, val in params.items():
                if isinstance(val, str) and ('lei' in val.lower() or 'decreto' in val.lower() or 'norma' in val.lower()):
                    params[param] = 'NI_LEI_EXTERNA'
                    marcados += 1
    _log(f"  {marcados} referência(s) marcadas como NI_LEI_EXTERNA", log_callback)
    return estado


# ─────────────────────────────────────────────────────────────────────────────
# EXPANDIR PARAMETROS POR USO
# ─────────────────────────────────────────────────────────────────────────────

def _expandir_por_uso(estado, log_callback=None):
    """Replica params gerais para cada uso permitido quando não há params específicos."""
    expandidas = 0
    for sigla, zona in estado.get('zonas', {}).items():
        usos = zona.get('usos_permitidos') or {}
        params_gerais = zona.get('parametros_gerais') or {}
        if not usos or not params_gerais:
            continue
        usos_permitidos = [
            u for u, d in usos.items()
            if isinstance(d, dict) and (d.get('status') or '').upper() in ('SIM', 'CONDICIONADO')
        ]
        if not usos_permitidos:
            continue
        ppu = zona.get('parametros_por_uso')
        if isinstance(ppu, dict) and ppu:
            for uso in usos_permitidos:
                if uso not in ppu:
                    ppu[uso] = copy.deepcopy(params_gerais)
        else:
            zona['parametros_por_uso'] = {u: copy.deepcopy(params_gerais) for u in usos_permitidos}
        expandidas += 1
    _log(f"  Zonas expandidas: {expandidas}", log_callback)
    return expandidas


# ─────────────────────────────────────────────────────────────────────────────
# EXTRAIR (ORQUESTRADOR PRINCIPAL)
# ─────────────────────────────────────────────────────────────────────────────

def extrair(zip_path, work_dir, log_callback=None):
    """
    Orquestrador principal — extrai parâmetros urbanísticos de uma legislação.

    Recebe o ZIP gerado por preparar_legislacao.py:
      {nome}_concat_catalogo.zip
        ├── {nome}_concatenado.pdf
        └── {nome}_catalogo.json

    Salva resultado_final.json em work_dir.
    Retorna dict com estado + métricas.
    """
    t0 = time.time()
    os.makedirs(work_dir, exist_ok=True)

    _log("=" * 60, log_callback)
    _log("EXTRAINDO PARÂMETROS", log_callback)
    _log("=" * 60, log_callback)

    # 1. Extrair PDF e JSON do ZIP
    _log("Extraindo ZIP...", log_callback)
    pdf_unico = None
    catalogo = None

    with zipfile.ZipFile(zip_path) as zf:
        for nome_arq in zf.namelist():
            dest = os.path.join(work_dir, os.path.basename(nome_arq))
            with zf.open(nome_arq) as src, open(dest, 'wb') as dst:
                dst.write(src.read())
            if nome_arq.endswith('_concatenado.pdf') or nome_arq.endswith('.pdf'):
                pdf_unico = dest
            elif nome_arq.endswith('_catalogo.json') or nome_arq.endswith('.json'):
                catalogo_path = dest

    if not pdf_unico or not os.path.exists(pdf_unico):
        raise RuntimeError("PDF concatenado nao encontrado no ZIP")
    if not os.path.exists(catalogo_path):
        raise RuntimeError("JSON catalogo nao encontrado no ZIP")

    with open(catalogo_path, encoding='utf-8') as f:
        catalogo = json.load(f)

    blocos = catalogo.get('blocos', [])
    categoria_usos = catalogo.get('categoria_usos', [])
    _log(f"  {len(blocos)} blocos | {len(categoria_usos)} mapeamento(s) de uso", log_callback)

    # 2. Gerar texto_por_pg (cache do layout do PDF)
    _log("Gerando texto por página...", log_callback)
    n_pgs = len(pypdf.PdfReader(pdf_unico).pages)
    texto_por_pg = {}
    for pg in range(1, n_pgs + 1):
        try:
            r = subprocess.run(
                ['pdftotext', '-layout', '-f', str(pg), '-l', str(pg), pdf_unico, '-'],
                capture_output=True, text=True, errors='replace', timeout=15
            )
            texto_por_pg[pg] = r.stdout
        except Exception:
            texto_por_pg[pg] = ''

    # 3. Preparar PDFs e textos de cada bloco
    _log("Preparando blocos...", log_callback)
    for b in blocos:
        nome_safe = b['nome'].replace('.', '_').replace(' ', '_').replace('/', '_')
        b['pdf_path'] = os.path.join(work_dir, f"bloco_{nome_safe}.pdf")
        _quebrar_pdf(pdf_unico, b['inicio'], b['fim'], b['pdf_path'])
        b['texto_layout'] = '\n'.join(
            f"=== PAGINA {pg} ===\n{texto_por_pg.get(pg, '')}"
            for pg in range(b['inicio'], b['fim'] + 1)
        )
        n_p = b['fim'] - b['inicio'] + 1
        _log(f"  {b['nome']:30s} pgs {b['inicio']:3d}-{b['fim']:3d} | {n_p}pgs | {len(b['texto_layout'])} chars", log_callback)

    # 4. Carregar prompt
    prompt = _carregar_prompt()
    _log(f"  Prompt: {len(prompt)} chars", log_callback)

    # 5. Extrair bloco por bloco (evolutivo)
    _log("Extraindo dados...", log_callback)
    estado = {
        'legislacao': None,
        'zonas': {},
        'modificacoes': [],
        'refs_externas': [],
        'usos_por_zona': {},
    }
    total_tempo = 0; total_in = 0; total_out = 0

    # Corpo primeiro
    corpo = next((b for b in blocos if b['nome'] == 'corpo_lei'), None)
    if corpo:
        resp, t, ti, to = extrair_por_bloco(corpo, pdf_unico, None, categoria_usos, prompt, work_dir, log_callback)
        total_tempo += t; total_in += ti; total_out += to
        parsed = _parse_json(resp or '')
        if parsed:
            _atualizar_estado(estado, parsed)
            _log(f"  Corpo: {len(estado['zonas'])} zonas", log_callback)

    # Demais blocos em ordem de prioridade, expandindo grandes
    blocos_processar = sorted(
        [b for b in blocos if _prio_bloco(b) >= 0],
        key=_prio_bloco
    )

    blocos_expandidos = []
    for b in blocos_processar:
        subs = dividir_bloco_grande(b)
        if len(subs) > 1:
            _log(f"  {b['nome']}: dividido em {len(subs)} sub-blocos", log_callback)
            for sub in subs:
                nome_safe = sub['nome'].replace('.', '_').replace(' ', '_').replace('/', '_')
                sub['pdf_path'] = os.path.join(work_dir, f"bloco_{nome_safe}.pdf")
                _quebrar_pdf(pdf_unico, sub['inicio'], sub['fim'], sub['pdf_path'])
                sub['texto_layout'] = '\n'.join(
                    f"=== PAGINA {pg} ===\n{texto_por_pg.get(pg, '')}"
                    for pg in range(sub['inicio'], sub['fim'] + 1)
                )
        blocos_expandidos.extend(subs)

    for b in blocos_expandidos:
        ctx = _gerar_contexto(estado) if (estado['zonas'] or estado['legislacao']) else None
        resp, t, ti, to = extrair_por_bloco(b, pdf_unico, ctx, categoria_usos, prompt, work_dir, log_callback)
        total_tempo += t; total_in += ti; total_out += to
        parsed = _parse_json(resp or '')
        if parsed:
            _atualizar_estado(estado, parsed)
            _log(f"  Zonas total: {len(estado['zonas'])}", log_callback)

    # 6. Consolidar
    estado_consolidado = consolidar(work_dir, log_callback=log_callback)

    # 7. Expandir parametros por uso
    _expandir_por_uso(estado_consolidado, log_callback)

    # 8. Mesclar leis externas
    mesclar_leis_externas(estado_consolidado, log_callback)

    # 9. Salvar resultado
    custo = _custo_gemini(total_in, total_out)
    metricas = {
        'tempo': time.time() - t0,
        'tokens_in': total_in,
        'tokens_out': total_out,
        'custo': custo,
        'n_zonas': len(estado_consolidado['zonas']),
    }

    resultado_path = os.path.join(work_dir, 'resultado_final.json')
    with open(resultado_path, 'w', encoding='utf-8') as f:
        json.dump({'estado': estado_consolidado, 'metricas': metricas}, f, ensure_ascii=False, indent=2)

    _log(f"EXTRAÇÃO CONCLUÍDA em {metricas['tempo']:.1f}s | {metricas['n_zonas']} zonas | ${custo:.4f}", log_callback)
    _log(f"Salvo: {resultado_path}", log_callback)

    return {
        'sucesso': True,
        'estado': estado_consolidado,
        'resultado_path': resultado_path,
        'metricas': metricas,
    }
