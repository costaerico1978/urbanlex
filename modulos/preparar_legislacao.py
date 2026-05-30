"""
preparar_legislacao.py
Prepara uma legislação para extração de parâmetros urbanísticos.

Fluxo:
  1. concatenar_pdfs()  — junta todos os PDFs em tudo.pdf + dedup MD5 de arquivos
  2. dedup_hash()       — remove páginas com texto MD5 idêntico
  3. dedup_visual()     — remove páginas visualmente idênticas (Gemini)
  4. catalogar()        — identifica corpo, anexos, ancora_parametros,
                          ancora_usos, categoria_usos
  5. gerar_zip()        — empacota PDF + JSON no ZIP final

Nomenclatura:
  PDF : {tipo}_{num}_{ano}_{UF}_{municipio}_concatenado.pdf
  JSON: {tipo}_{num}_{ano}_{UF}_{municipio}_catalogo.json
  ZIP : {tipo}_{num}_{ano}_{UF}_{municipio}_concat_catalogo.zip

API pública:
  preparar(zip_path, work_dir, nome_base, output_dir, log_callback) -> dict
  concatenar_pdfs(zip_path, work_dir, log_callback) -> str
  dedup_hash(pdf_path, log_callback) -> str
  dedup_visual(pdf_path, log_callback) -> str
  catalogar(pdf_path, work_dir, log_callback) -> dict
  gerar_zip(pdf_path, json_path, nome_base, output_dir, log_callback) -> str
"""
import os, re, json, time, hashlib, zipfile, logging, subprocess
import pypdf

logger = logging.getLogger(__name__)

GEMINI_RETRIES = 3
GEMINI_RETRY_WAIT = 15
CHUNK_SIZE_CHARS = 250_000
CHUNK_OVERLAP_CHARS = 5_000
MAX_TOTAL_CHARS = 2_000_000
GEMINI_MAX_PGS_CATALOGO = 200
CHUNK_PGS_CATALOGO = 100


# ─────────────────────────────────────────────────────────────────────────────
# UTILITÁRIOS INTERNOS
# ─────────────────────────────────────────────────────────────────────────────

def _log(msg, cb=None):
    logger.info(msg)
    if cb:
        cb(msg)


def _n_paginas(pdf_path):
    r = subprocess.run(['pdfinfo', pdf_path], capture_output=True, text=True, timeout=30)
    for linha in r.stdout.split('\n'):
        if linha.startswith('Pages:'):
            return int(linha.split(':')[1].strip())
    return len(pypdf.PdfReader(pdf_path).pages)


def _texto_layout(pdf_path, pg_ini=1, pg_fim=None):
    pg_fim = pg_fim or _n_paginas(pdf_path)
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
    return '\n'.join(partes)


def _texto_completo(pdf_path):
    try:
        import subprocess as _sp
        r = _sp.run(['pdftotext', '-layout', pdf_path, '-'],
                    capture_output=True, text=True, errors='replace', timeout=120)
        if r.stdout.strip():
            return r.stdout
    except Exception as e:
        logger.warning(f"_texto_completo pdftotext falhou: {e}")
    try:
        reader = pypdf.PdfReader(pdf_path)
        partes = []
        for pg in reader.pages:
            try:
                t = pg.extract_text() or ''
                if t.strip():
                    partes.append(t)
            except Exception:
                pass
        return '\n\n'.join(partes)
    except Exception as e:
        logger.error(f"_texto_completo: {e}")
        return ''


def _dividir_chunks(texto, chunk_size=CHUNK_SIZE_CHARS, overlap=CHUNK_OVERLAP_CHARS):
    if len(texto) <= chunk_size:
        return [texto]
    chunks = []
    inicio = 0
    while inicio < len(texto):
        fim = min(inicio + chunk_size, len(texto))
        if fim < len(texto):
            for i in range(fim, max(fim - 1000, inicio), -1):
                if texto[i] == '\n':
                    fim = i
                    break
        chunks.append(texto[inicio:fim])
        if fim >= len(texto):
            break
        inicio = max(fim - overlap, inicio + 1)
    return chunks


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


# ─────────────────────────────────────────────────────────────────────────────
# 1. CONCATENAR PDFs
# ─────────────────────────────────────────────────────────────────────────────

def concatenar_pdfs(zip_path, work_dir, log_callback=None):
    _log("Concatenando PDFs...", log_callback)
    t0 = time.time()
    os.makedirs(work_dir, exist_ok=True)
    pdf_saida = os.path.join(work_dir, 'tudo.pdf')

    if os.path.exists(pdf_saida) and os.path.getsize(pdf_saida) > 1000:
        try:
            n = _n_paginas(pdf_saida)
            if n > 0:
                _log(f"  Cache: {pdf_saida} ({n} pgs)", log_callback)
                return pdf_saida
        except Exception:
            pass

    hashes = set()
    pdfs = []

    def _extrair(zp):
        with zipfile.ZipFile(zp) as zf:
            for info in zf.infolist():
                try:
                    c = zf.read(info)
                except Exception:
                    continue
                h = hashlib.md5(c).hexdigest()
                if c[:4] == b'%PDF' and h not in hashes:
                    hashes.add(h)
                    p = os.path.join(work_dir, f"pdf_{len(pdfs):03d}.pdf")
                    open(p, 'wb').write(c)
                    pdfs.append(p)
                elif c[:4] == b'PK\x03\x04' and h not in hashes:
                    hashes.add(h)
                    tz = os.path.join(work_dir, f"nest_{len(pdfs):03d}.zip")
                    open(tz, 'wb').write(c)
                    _extrair(tz)

    if zip_path.endswith('.pdf') and open(zip_path, 'rb').read(4) == b'%PDF':
        import shutil
        p = os.path.join(work_dir, 'pdf_000.pdf')
        if zip_path != p:
            shutil.copy2(zip_path, p)
        pdfs.append(p)
    else:
        _extrair(zip_path)

    if not pdfs:
        raise ValueError(f"Nenhum PDF encontrado em {zip_path}")

    _log(f"  {len(pdfs)} PDFs extraidos (dedup MD5 de arquivos)", log_callback)
    subprocess.run(
        ['pdfunite'] + pdfs + [pdf_saida],
        check=True, capture_output=True, timeout=120
    )
    n = _n_paginas(pdf_saida)
    _log(f"  PDF concatenado: {n} paginas ({time.time()-t0:.1f}s)", log_callback)
    return pdf_saida


# ─────────────────────────────────────────────────────────────────────────────
# 2. DEDUP HASH
# ─────────────────────────────────────────────────────────────────────────────

def dedup_hash(pdf_path, log_callback=None):
    _log("Dedup por hash MD5...", log_callback)
    reader = pypdf.PdfReader(pdf_path)
    n_orig = len(reader.pages)
    hashes = {}
    manter = []
    for i in range(n_orig):
        try:
            r = subprocess.run(
                ['pdftotext', '-f', str(i+1), '-l', str(i+1), pdf_path, '-'],
                capture_output=True, text=True, timeout=10
            )
            h = hashlib.md5(r.stdout.strip().encode()).hexdigest()
        except Exception:
            h = f"err_{i}"
        if h not in hashes:
            hashes[h] = i
            manter.append(i)
    removidas = n_orig - len(manter)
    if removidas > 0:
        writer = pypdf.PdfWriter()
        for i in manter:
            writer.add_page(reader.pages[i])
        with open(pdf_path, 'wb') as f:
            writer.write(f)
        _log(f"  Dedup MD5: {removidas} paginas removidas ({n_orig} -> {len(manter)})", log_callback)
    else:
        _log(f"  Dedup MD5: sem duplicatas ({n_orig} pgs)", log_callback)
    return pdf_path


# ─────────────────────────────────────────────────────────────────────────────
# 3. DEDUP VISUAL
# ─────────────────────────────────────────────────────────────────────────────

def dedup_visual(pdf_path, log_callback=None):
    _log("Dedup visual (Gemini)...", log_callback)
    client, gt = _gemini_client()
    n = _n_paginas(pdf_path)
    if n <= 1:
        _log("  Apenas 1 pagina, pulando", log_callback)
        return pdf_path

    _log(f"  Analisando {n} paginas...", log_callback)
    with open(pdf_path, 'rb') as f:
        pdf_bytes = f.read()

    prompt = (
        "Analise este PDF e identifique paginas VISUALMENTE IDENTICAS ou QUASE IDENTICAS "
        "a outra pagina (mesmo conteudo mas renderizacao diferente - nao detectavel por hash MD5). "
        "Para cada grupo de duplicatas, mantenha APENAS a primeira ocorrencia. "
        "Retorne APENAS JSON (paginas numeradas a partir de 1): "
        '{"paginas_remover": [5, 12]} '
        'Se nao houver duplicatas: {"paginas_remover": []}'
    )

    resp = _gemini_chamar(client, gt, [
        gt.Part(text=prompt),
        gt.Part(inline_data=gt.Blob(mime_type='application/pdf', data=pdf_bytes)),
        gt.Part(text='Retorne APENAS JSON sem markdown fences.'),
    ], max_tokens=2048)

    parsed = _parse_json(resp.text or '')
    if parsed and 'paginas_remover' in parsed:
        pgs_remover = set()
        for p in parsed['paginas_remover']:
            try:
                pgs_remover.add(int(p) - 1)
            except Exception:
                pass
        if pgs_remover:
            reader = pypdf.PdfReader(pdf_path)
            writer = pypdf.PdfWriter()
            for i in range(len(reader.pages)):
                if i not in pgs_remover:
                    writer.add_page(reader.pages[i])
            with open(pdf_path, 'wb') as f:
                writer.write(f)
            _log(f"  Dedup visual: {len(pgs_remover)} paginas removidas ({n} -> {n-len(pgs_remover)})", log_callback)
        else:
            _log("  Dedup visual: nenhuma duplicata encontrada", log_callback)
    return pdf_path


# ─────────────────────────────────────────────────────────────────────────────
# 4. CATALOGAR
# ─────────────────────────────────────────────────────────────────────────────

def catalogar(pdf_path, work_dir, log_callback=None):
    _log("Catalogando...", log_callback)
    t0 = time.time()

    cache_path = os.path.join(work_dir, 'etapa4_catalogacao.json')
    if os.path.exists(cache_path):
        _log(f"  Cache: {cache_path}", log_callback)
        with open(cache_path) as f:
            return json.load(f)

    fim_corpo = _detectar_fim_corpo(pdf_path, log_callback)
    n_total = _n_paginas(pdf_path)
    _log(f"  Corpo: pgs 1-{fim_corpo} | Anexos: pgs {fim_corpo+1}-{n_total}", log_callback)

    corpo_pdf = os.path.join(work_dir, 'corpo.pdf')
    anexos_pdf = os.path.join(work_dir, 'anexos.pdf')
    _separar(pdf_path, fim_corpo, n_total, corpo_pdf, anexos_pdf)

    texto_corpo = _texto_completo(corpo_pdf)
    texto_corpo = re.sub(r'[ \t]+', ' ', texto_corpo)
    if len(texto_corpo) > MAX_TOTAL_CHARS:
        texto_corpo = texto_corpo[:MAX_TOTAL_CHARS]
    _log(f"  Texto corpo: {len(texto_corpo):,} chars", log_callback)

    anexos_citados = _detectar_citados(texto_corpo, log_callback)

    lista_citados = [a.get('nome_citado', '') for a in anexos_citados if a.get('nome_citado')]
    res_cat = _catalogar_anexos(anexos_pdf, fim_corpo, work_dir, lista_citados, log_callback)

    blocos = [{'nome': 'corpo_lei', 'titulo': 'Corpo da Lei',
               'inicio': 1, 'fim': fim_corpo, 'tipo': 'corpo'}]
    for b in res_cat.get('blocos', []):
        if isinstance(b, dict):
            citado = b.get('citado_como')
            if citado:
                for ac in anexos_citados:
                    if ac.get('nome_citado', '').strip().lower() == citado.strip().lower():
                        b['assunto'] = ac.get('assunto', '')
                        break
            blocos.append(b)

    resultado = {
        'blocos': blocos,
        'tokens_in': res_cat.get('tokens_in', 0),
        'tokens_out': res_cat.get('tokens_out', 0),
        'custo': res_cat.get('custo', 0.0),
        'categoria_usos': res_cat.get('categoria_usos', []),
        'tempo': time.time() - t0,
    }

    with open(cache_path, 'w', encoding='utf-8') as f:
        json.dump(resultado, f, ensure_ascii=False, indent=2)

    _log(f"  {len(blocos)} blocos (1 corpo + {len(blocos)-1} anexos)", log_callback)
    _log(f"  categoria_usos: {len(resultado['categoria_usos'])} mapeamento(s)", log_callback)
    _log(f"  Custo: ${resultado['custo']:.4f} | Tempo: {resultado['tempo']:.1f}s", log_callback)
    return resultado


def _detectar_fim_corpo(pdf_path, log_callback=None):
    PAT_ART = re.compile(r'\bArt\.\s{0,3}(\d+)', re.IGNORECASE)
    n = _n_paginas(pdf_path)
    max_visto = 0
    fim_corpo = 0
    for pg in range(1, n + 1):
        try:
            r = subprocess.run(
                ['pdftotext', '-layout', '-f', str(pg), '-l', str(pg), pdf_path, '-'],
                capture_output=True, text=True, errors='replace', timeout=15
            )
            arts = [int(m.group(1)) for m in PAT_ART.finditer(r.stdout)]
            if arts and arts[0] >= max_visto - 5:
                max_visto = max(max_visto, arts[-1])
                fim_corpo = pg
        except Exception:
            pass
    return fim_corpo or n


def _separar(pdf_path, fim_corpo, n_total, corpo_pdf, anexos_pdf):
    reader = pypdf.PdfReader(pdf_path)
    w = pypdf.PdfWriter()
    for i in range(fim_corpo):
        w.add_page(reader.pages[i])
    with open(corpo_pdf, 'wb') as f:
        w.write(f)
    w = pypdf.PdfWriter()
    for i in range(fim_corpo, n_total):
        w.add_page(reader.pages[i])
    with open(anexos_pdf, 'wb') as f:
        w.write(f)


def _detectar_citados(texto_corpo, log_callback=None):
    _log("  Detectando citados no corpo...", log_callback)
    client, gt = _gemini_client()
    anexos_citados = []
    nomes_vistos = set()

    def _normalizar(nome):
        import unicodedata
        s = unicodedata.normalize('NFKD', nome.lower()).encode('ascii', 'ignore').decode()
        return re.sub(r'\s+', ' ', s).strip()

    def _add(parsed):
        for a in (parsed.get('anexos') or []):
            nome = a.get('nome_citado', '').strip()
            nn = _normalizar(nome)
            if nn and nn not in nomes_vistos:
                nomes_vistos.add(nn)
                anexos_citados.append(a)

    PROMPT = (
        "Voce esta analisando o CORPO de uma legislacao urbanistica municipal.\n"
        "Sua tarefa: listar TODOS os ANEXOS citados no texto.\n"
        "REGRAS:\n"
        "1. nome_citado: APENAS 'Anexo X' com numeral romano (ex: 'Anexo I', 'Anexo XVIII')\n"
        "2. assunto: descricao curta do conteudo\n"
        "3. Cada anexo uma unica vez (deduplicado)\n"
        "4. Use sempre numeral romano\n"
        "5. Inclua APENAS anexos desta propria lei\n"
        'FORMATO JSON:\n{"anexos": [{"nome_citado": "Anexo I", "assunto": "Objetivos"}, ...]}\n'
        "Retorne APENAS o JSON."
    )

    chunks = _dividir_chunks(texto_corpo)
    for i, chunk in enumerate(chunks, 1):
        _log(f"  Chunk {i}/{len(chunks)}: {len(chunk):,} chars", log_callback)
        resp = _gemini_chamar(client, gt, [
            gt.Part(text=PROMPT + '\n\n' + chunk),
            gt.Part(text='Retorne APENAS JSON.'),
        ], max_tokens=4096)
        parsed = _parse_json(resp.text or '')
        if parsed:
            _add(parsed)

    _log(f"  {len(anexos_citados)} citados detectados", log_callback)
    return anexos_citados


def _catalogar_anexos(anexos_pdf, fim_corpo, work_dir, lista_citados, log_callback=None):
    _log("  Catalogando anexos.pdf...", log_callback)
    client, gt = _gemini_client()
    tokens_in = 0; tokens_out = 0; custo = 0.0
    blocos_raw = []
    categoria_usos = []
    n_pgs = _n_paginas(anexos_pdf)
    lista_str = '\n'.join(f'  - "{c}"' for c in lista_citados)

    PROMPT_BASE = (
        "Voce vai analisar este PDF (anexos de lei municipal) e catalogar cada bloco.\n"
        "Recebe o PDF visual + texto-layout para maior precisao.\n\n"
        "TRABALHO 1 - CATALOGAR:\n"
        "Para cada bloco retorne:\n"
        "  - nome: chave snake_case (ex: 'anexo_xxi')\n"
        "  - titulo: titulo completo como aparece no PDF\n"
        "  - inicio: pagina de inicio (1-indexado neste PDF)\n"
        "  - fim: pagina de fim\n"
        "  - tipo: 'anexo' | 'errata' | 'encerramento' | 'indefinido'\n"
        "  - relevancia: 'ALTA' (parametros, usos, zoneamento) | 'MEDIA' | 'NULA'\n"
        "  - continua: true se o bloco claramente continua no proximo chunk\n"
        "  - ancora_parametros: true SE for a fonte primaria de parametros (CA, TO, gabarito, afastamentos). Apenas UM bloco.\n"
        "  - ancora_usos: true SE for a fonte primaria de usos permitidos por zona. Apenas UM bloco.\n\n"
        "TRABALHO 2 - RELACIONAR COM CITADOS:\n"
        f"A lei cita:\n{lista_str}\n"
        "Para cada bloco: citado_como = texto EXATO da lista acima, ou null.\n\n"
        "TRABALHO 3 - CATEGORIAS DE USO:\n"
        "Se a lei define usos por categorias (ex: 'Residencial I, II, III'):\n"
        '  categoria_usos: [{"categoria": "Residencial I", "usos_reais": ["residencial_unifamiliar"], "dispositivo": "Art. X"}]\n'
        "Se nao houver, omita.\n\n"
        'Retorne APENAS JSON: {"blocos": [...], "categoria_usos": [...]}\n\n'
        "=== TEXTO-LAYOUT ===\n{TEXTO_LAYOUT}"
    )

    def _processar(texto_resp, offset=0):
        parsed = _parse_json(texto_resp)
        if not parsed or 'blocos' not in parsed:
            return [], []
        blocos = []
        for b in parsed['blocos']:
            b['inicio'] = int(b.get('inicio', 1)) + offset
            b['fim'] = int(b.get('fim', 1)) + offset
            blocos.append(b)
        return blocos, parsed.get('categoria_usos', [])

    if n_pgs <= GEMINI_MAX_PGS_CATALOGO:
        _log(f"  Chamada unica ({n_pgs} pgs)...", log_callback)
        tl = _texto_layout(anexos_pdf)[:80000]
        with open(anexos_pdf, 'rb') as f:
            pdf_bytes = f.read()
        resp = _gemini_chamar(client, gt, [
            gt.Part(text=PROMPT_BASE.replace('{TEXTO_LAYOUT}', tl)),
            gt.Part(inline_data=gt.Blob(mime_type='application/pdf', data=pdf_bytes)),
            gt.Part(text='Retorne APENAS JSON sem markdown fences.'),
        ])
        uso = resp.usage_metadata
        ti = getattr(uso, 'prompt_token_count', 0) or 0
        to = getattr(uso, 'candidates_token_count', 0) or 0
        tokens_in += ti; tokens_out += to; custo += _custo_gemini(ti, to)
        bs, cu = _processar(resp.text or '', offset=fim_corpo)
        blocos_raw.extend(bs)
        if cu:
            categoria_usos = cu
    else:
        n_chunks = (n_pgs + CHUNK_PGS_CATALOGO - 1) // CHUNK_PGS_CATALOGO
        _log(f"  {n_chunks} chunks de {CHUNK_PGS_CATALOGO} pgs...", log_callback)
        reader = pypdf.PdfReader(anexos_pdf)
        bloco_anterior = None
        for ci in range(n_chunks):
            pg_ini = ci * CHUNK_PGS_CATALOGO
            pg_fim = min((ci + 1) * CHUNK_PGS_CATALOGO, n_pgs)
            chunk_path = os.path.join(work_dir, f'cat_chunk_{ci+1}.pdf')
            w = pypdf.PdfWriter()
            for i in range(pg_ini, pg_fim):
                w.add_page(reader.pages[i])
            with open(chunk_path, 'wb') as f:
                w.write(f)
            tl = _texto_layout(chunk_path)[:50000]
            ctx = ''
            if bloco_anterior and bloco_anterior.get('continua'):
                ctx = f"\nATENCAO: '{bloco_anterior.get('titulo', '')}' continua aqui."
            with open(chunk_path, 'rb') as f:
                pdf_bytes = f.read()
            _log(f"  Chunk {ci+1}/{n_chunks} (pgs {pg_ini+1}-{pg_fim})...", log_callback)
            resp = _gemini_chamar(client, gt, [
                gt.Part(text=PROMPT_BASE.replace('{TEXTO_LAYOUT}', tl) + ctx),
                gt.Part(inline_data=gt.Blob(mime_type='application/pdf', data=pdf_bytes)),
                gt.Part(text='Retorne APENAS JSON sem markdown fences.'),
            ])
            uso = resp.usage_metadata
            ti = getattr(uso, 'prompt_token_count', 0) or 0
            to = getattr(uso, 'candidates_token_count', 0) or 0
            tokens_in += ti; tokens_out += to; custo += _custo_gemini(ti, to)
            bs, cu = _processar(resp.text or '', offset=fim_corpo + pg_ini)
            blocos_raw.extend(bs)
            if cu and not categoria_usos:
                categoria_usos = cu
            if bs:
                bloco_anterior = dict(bs[-1])

    # Merge blocos com flag continua
    merged = []
    i = 0
    while i < len(blocos_raw):
        b = dict(blocos_raw[i])
        while b.get('continua') and i + 1 < len(blocos_raw):
            i += 1
            b['fim'] = blocos_raw[i].get('fim', b['fim'])
            if not blocos_raw[i].get('continua'):
                break
        merged.append(b)
        i += 1

    return {
        'blocos': merged,
        'tokens_in': tokens_in,
        'tokens_out': tokens_out,
        'custo': custo,
        'categoria_usos': categoria_usos,
    }


# ─────────────────────────────────────────────────────────────────────────────
# 5. GERAR ZIP
# ─────────────────────────────────────────────────────────────────────────────

def gerar_zip(pdf_path, json_path, nome_base, output_dir, log_callback=None):
    os.makedirs(output_dir, exist_ok=True)
    zip_path = os.path.join(output_dir, f"{nome_base}_concat_catalogo.zip")
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.write(pdf_path, f"{nome_base}_concatenado.pdf")
        zf.write(json_path, f"{nome_base}_catalogo.json")
    size_mb = os.path.getsize(zip_path) / 1_048_576
    _log(f"  ZIP: {zip_path} ({size_mb:.1f} MB)", log_callback)
    return zip_path


# ─────────────────────────────────────────────────────────────────────────────
# ORQUESTRADOR PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

def preparar(zip_path, work_dir, nome_base=None, output_dir=None, log_callback=None):
    t0 = time.time()
    os.makedirs(work_dir, exist_ok=True)
    output_dir = output_dir or work_dir
    if not nome_base:
        nome_base = os.path.splitext(os.path.basename(zip_path))[0]

    _log("=" * 60, log_callback)
    _log("PREPARANDO LEGISLACAO", log_callback)
    _log("=" * 60, log_callback)

    pdf_path = concatenar_pdfs(zip_path, work_dir, log_callback)
    dedup_hash(pdf_path, log_callback)
    dedup_visual(pdf_path, log_callback)
    catalogo = catalogar(pdf_path, work_dir, log_callback)
    json_path = os.path.join(work_dir, 'etapa4_catalogacao.json')
    zip_saida = gerar_zip(pdf_path, json_path, nome_base, output_dir, log_callback)

    tempo = time.time() - t0
    _log(f"PREPARACAO CONCLUIDA em {tempo:.1f}s | custo: ${catalogo.get('custo', 0):.4f}", log_callback)

    return {
        'sucesso': True,
        'pdf_concatenado': pdf_path,
        'json_catalogo': json_path,
        'zip_saida': zip_saida,
        'nome_base': nome_base,
        'n_blocos': len(catalogo.get('blocos', [])),
        'categoria_usos': catalogo.get('categoria_usos', []),
        'custo': catalogo.get('custo', 0.0),
        'tempo': tempo,
    }
    _log(f"  categoria_usos: {len(resultado['categoria_usos'])} mapeamento(s)", log_callback)
    _log(f"  Custo: ${resultado['custo']:.4f} | Tempo: {resultado['tempo']:.1f}s", log_callback)
    return resultado


def _detectar_fim_corpo(pdf_path, log_callback=None):
    PAT_ART = re.compile(r'\bArt\.\s{0,3}(\d+)', re.IGNORECASE)
    n = _n_paginas(pdf_path)
    max_visto = 0
    fim_corpo = 0
    for pg in range(1, n + 1):
        try:
            r = subprocess.run(
                ['pdftotext', '-layout', '-f', str(pg), '-l', str(pg), pdf_path, '-'],
                capture_output=True, text=True, errors='replace', timeout=15
            )
            arts = [int(m.group(1)) for m in PAT_ART.finditer(r.stdout)]
            if arts and arts[0] >= max_visto - 5:
                max_visto = max(max_visto, arts[-1])
                fim_corpo = pg
        except Exception:
            pass
    return fim_corpo or n


def _separar(pdf_path, fim_corpo, n_total, corpo_pdf, anexos_pdf):
    reader = pypdf.PdfReader(pdf_path)
    w = pypdf.PdfWriter()
    for i in range(fim_corpo):
        w.add_page(reader.pages[i])
    with open(corpo_pdf, 'wb') as f:
        w.write(f)
    w = pypdf.PdfWriter()
    for i in range(fim_corpo, n_total):
        w.add_page(reader.pages[i])
    with open(anexos_pdf, 'wb') as f:
        w.write(f)


def _detectar_citados(texto_corpo, log_callback=None):
    _log("  Detectando citados no corpo...", log_callback)
    client, gt = _gemini_client()
    anexos_citados = []
    nomes_vistos = set()

    def _normalizar(nome):
        import unicodedata
        s = unicodedata.normalize('NFKD', nome.lower()).encode('ascii', 'ignore').decode()
        return re.sub(r'\s+', ' ', s).strip()

    def _add(parsed):
        for a in (parsed.get('anexos') or []):
            nome = a.get('nome_citado', '').strip()
            nn = _normalizar(nome)
            if nn and nn not in nomes_vistos:
                nomes_vistos.add(nn)
                anexos_citados.append(a)

    PROMPT = (
        "Voce esta analisando o CORPO de uma legislacao urbanistica municipal.\n"
        "Sua tarefa: listar TODOS os ANEXOS citados no texto.\n"
        "REGRAS:\n"
        "1. nome_citado: APENAS 'Anexo X' com numeral romano (ex: 'Anexo I', 'Anexo XVIII')\n"
        "2. assunto: descricao curta do conteudo\n"
        "3. Cada anexo uma unica vez (deduplicado)\n"
        "4. Use sempre numeral romano\n"
        "5. Inclua APENAS anexos desta propria lei\n"
        'FORMATO JSON:\n{"anexos": [{"nome_citado": "Anexo I", "assunto": "Objetivos"}, ...]}\n'
        "Retorne APENAS o JSON."
    )

    chunks = _dividir_chunks(texto_corpo)
    for i, chunk in enumerate(chunks, 1):
        _log(f"  Chunk {i}/{len(chunks)}: {len(chunk):,} chars", log_callback)
        resp = _gemini_chamar(client, gt, [
            gt.Part(text=PROMPT + '\n\n' + chunk),
            gt.Part(text='Retorne APENAS JSON.'),
        ], max_tokens=4096)
        parsed = _parse_json(resp.text or '')
        if parsed:
            _add(parsed)

    _log(f"  {len(anexos_citados)} citados detectados", log_callback)
    return anexos_citados


def _catalogar_anexos(anexos_pdf, fim_corpo, work_dir, lista_citados, log_callback=None):
    _log("  Catalogando anexos.pdf...", log_callback)
    client, gt = _gemini_client()
    tokens_in = 0; tokens_out = 0; custo = 0.0
    blocos_raw = []
    categoria_usos = []
    n_pgs = _n_paginas(anexos_pdf)
    lista_str = '\n'.join(f'  - "{c}"' for c in lista_citados)

    PROMPT_BASE = (
        "Voce vai analisar este PDF (anexos de lei municipal) e catalogar cada bloco.\n"
        "Recebe o PDF visual + texto-layout para maior precisao.\n\n"
        "TRABALHO 1 - CATALOGAR:\n"
        "Para cada bloco retorne:\n"
        "  - nome: chave snake_case (ex: 'anexo_xxi')\n"
        "  - titulo: titulo completo como aparece no PDF\n"
        "  - inicio: pagina de inicio (1-indexado neste PDF)\n"
        "  - fim: pagina de fim\n"
        "  - tipo: 'anexo' | 'errata' | 'encerramento' | 'indefinido'\n"
        "  - relevancia: 'ALTA' (parametros, usos, zoneamento) | 'MEDIA' | 'NULA'\n"
        "  - continua: true se o bloco claramente continua no proximo chunk\n"
        "  - ancora_parametros: true SE for a fonte primaria de parametros (CA, TO, gabarito, afastamentos). Apenas UM bloco.\n"
        "  - ancora_usos: true SE for a fonte primaria de usos permitidos por zona. Apenas UM bloco.\n\n"
        "TRABALHO 2 - RELACIONAR COM CITADOS:\n"
        f"A lei cita:\n{lista_str}\n"
        "Para cada bloco: citado_como = texto EXATO da lista acima, ou null.\n\n"
        "TRABALHO 3 - CATEGORIAS DE USO:\n"
        "Se a lei define usos por categorias (ex: 'Residencial I, II, III'):\n"
        '  categoria_usos: [{"categoria": "Residencial I", "usos_reais": ["residencial_unifamiliar"], "dispositivo": "Art. X"}]\n'
        "Se nao houver, omita.\n\n"
        'Retorne APENAS JSON: {"blocos": [...], "categoria_usos": [...]}\n\n'
        "=== TEXTO-LAYOUT ===\n{TEXTO_LAYOUT}"
    )

    def _processar(texto_resp, offset=0):
        parsed = _parse_json(texto_resp)
        if not parsed or 'blocos' not in parsed:
            return [], []
        blocos = []
        for b in parsed['blocos']:
            b['inicio'] = int(b.get('inicio', 1)) + offset
            b['fim'] = int(b.get('fim', 1)) + offset
            blocos.append(b)
        return blocos, parsed.get('categoria_usos', [])

    if n_pgs <= GEMINI_MAX_PGS_CATALOGO:
        _log(f"  Chamada unica ({n_pgs} pgs)...", log_callback)
        tl = _texto_layout(anexos_pdf)[:80000]
        with open(anexos_pdf, 'rb') as f:
            pdf_bytes = f.read()
        resp = _gemini_chamar(client, gt, [
            gt.Part(text=PROMPT_BASE.replace('{TEXTO_LAYOUT}', tl)),
            gt.Part(inline_data=gt.Blob(mime_type='application/pdf', data=pdf_bytes)),
            gt.Part(text='Retorne APENAS JSON sem markdown fences.'),
        ])
        uso = resp.usage_metadata
        ti = getattr(uso, 'prompt_token_count', 0) or 0
        to = getattr(uso, 'candidates_token_count', 0) or 0
        tokens_in += ti; tokens_out += to; custo += _custo_gemini(ti, to)
        bs, cu = _processar(resp.text or '', offset=fim_corpo)
        blocos_raw.extend(bs)
        if cu:
            categoria_usos = cu
    else:
        n_chunks = (n_pgs + CHUNK_PGS_CATALOGO - 1) // CHUNK_PGS_CATALOGO
        _log(f"  {n_chunks} chunks de {CHUNK_PGS_CATALOGO} pgs...", log_callback)
        reader = pypdf.PdfReader(anexos_pdf)
        bloco_anterior = None
        for ci in range(n_chunks):
            pg_ini = ci * CHUNK_PGS_CATALOGO
            pg_fim = min((ci + 1) * CHUNK_PGS_CATALOGO, n_pgs)
            chunk_path = os.path.join(work_dir, f'cat_chunk_{ci+1}.pdf')
            w = pypdf.PdfWriter()
            for i in range(pg_ini, pg_fim):
                w.add_page(reader.pages[i])
            with open(chunk_path, 'wb') as f:
                w.write(f)
            tl = _texto_layout(chunk_path)[:50000]
            ctx = ''
            if bloco_anterior and bloco_anterior.get('continua'):
                ctx = f"\nATENCAO: '{bloco_anterior.get('titulo', '')}' continua aqui."
            with open(chunk_path, 'rb') as f:
                pdf_bytes = f.read()
            _log(f"  Chunk {ci+1}/{n_chunks} (pgs {pg_ini+1}-{pg_fim})...", log_callback)
            resp = _gemini_chamar(client, gt, [
                gt.Part(text=PROMPT_BASE.replace('{TEXTO_LAYOUT}', tl) + ctx),
                gt.Part(inline_data=gt.Blob(mime_type='application/pdf', data=pdf_bytes)),
                gt.Part(text='Retorne APENAS JSON sem markdown fences.'),
            ])
            uso = resp.usage_metadata
            ti = getattr(uso, 'prompt_token_count', 0) or 0
            to = getattr(uso, 'candidates_token_count', 0) or 0
            tokens_in += ti; tokens_out += to; custo += _custo_gemini(ti, to)
            bs, cu = _processar(resp.text or '', offset=fim_corpo + pg_ini)
            blocos_raw.extend(bs)
            if cu and not categoria_usos:
                categoria_usos = cu
            if bs:
                bloco_anterior = dict(bs[-1])

    # Merge blocos com flag continua
    merged = []
    i = 0
    while i < len(blocos_raw):
        b = dict(blocos_raw[i])
        while b.get('continua') and i + 1 < len(blocos_raw):
            i += 1
            b['fim'] = blocos_raw[i].get('fim', b['fim'])
            if not blocos_raw[i].get('continua'):
                break
        merged.append(b)
        i += 1

    return {
        'blocos': merged,
        'tokens_in': tokens_in,
        'tokens_out': tokens_out,
        'custo': custo,
        'categoria_usos': categoria_usos,
    }


# ─────────────────────────────────────────────────────────────────────────────
# 5. GERAR ZIP
# ─────────────────────────────────────────────────────────────────────────────

def gerar_zip(pdf_path, json_path, nome_base, output_dir, log_callback=None):
    os.makedirs(output_dir, exist_ok=True)
    zip_path = os.path.join(output_dir, f"{nome_base}_concat_catalogo.zip")
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.write(pdf_path, f"{nome_base}_concatenado.pdf")
        zf.write(json_path, f"{nome_base}_catalogo.json")
    size_mb = os.path.getsize(zip_path) / 1_048_576
    _log(f"  ZIP: {zip_path} ({size_mb:.1f} MB)", log_callback)
    return zip_path


# ─────────────────────────────────────────────────────────────────────────────
# ORQUESTRADOR PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

def preparar(zip_path, work_dir, nome_base=None, output_dir=None, log_callback=None):
    t0 = time.time()
    os.makedirs(work_dir, exist_ok=True)
    output_dir = output_dir or work_dir
    if not nome_base:
        nome_base = os.path.splitext(os.path.basename(zip_path))[0]

    _log("=" * 60, log_callback)
    _log("PREPARANDO LEGISLACAO", log_callback)
    _log("=" * 60, log_callback)

    pdf_path = concatenar_pdfs(zip_path, work_dir, log_callback)
    dedup_hash(pdf_path, log_callback)
    dedup_visual(pdf_path, log_callback)
    catalogo = catalogar(pdf_path, work_dir, log_callback)
    json_path = os.path.join(work_dir, 'etapa4_catalogacao.json')
    zip_saida = gerar_zip(pdf_path, json_path, nome_base, output_dir, log_callback)

    tempo = time.time() - t0
    _log(f"PREPARACAO CONCLUIDA em {tempo:.1f}s | custo: ${catalogo.get('custo', 0):.4f}", log_callback)

    return {
        'sucesso': True,
        'pdf_concatenado': pdf_path,
        'json_catalogo': json_path,
        'zip_saida': zip_saida,
        'nome_base': nome_base,
        'n_blocos': len(catalogo.get('blocos', [])),
        'categoria_usos': catalogo.get('categoria_usos', []),
        'custo': catalogo.get('custo', 0.0),
        'tempo': tempo,
    }
