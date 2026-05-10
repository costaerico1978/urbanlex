"""
modulos/ocr_tabelas.py
======================
Pipeline de OCR estruturado para anexos de planos diretores.

Estrategia:
1. pdfplumber: tenta extrair texto nativo (PDF com camada de texto).
2. Se vazio: pdf2image rasteriza + img2table+Tesseract extrai tabelas.

Saida: dict com Markdown + DIAGNOSTICO DETALHADO por pagina.

VERSAO 2 (Componente 1 do Plano Y):
- Diagnostico por pagina (qual pagina falhou, por que)
- Tracking de tempo, chars extraidos, motivo de falha
- Suporte a tipos de erro categorizados
"""
import os
import time
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

DPI_OCR = 250
TESSERACT_LANG = 'por'
MIN_CHARS_NATIVO = 50  # min chars para considerar texto nativo valido
MIN_CHARS_OCR = 20     # min chars para considerar OCR valido


def _classifica_erro(msg: str) -> str:
    """Classifica string de erro em categoria."""
    if not msg: return 'desconhecido'
    m = msg.lower()
    if 'eof' in m or 'malformed' in m: return 'pdf_corrompido'
    if 'encrypt' in m or 'password' in m: return 'pdf_protegido'
    if 'timeout' in m: return 'timeout'
    if 'memory' in m or 'oom' in m: return 'memoria'
    if 'tesseract' in m: return 'tesseract'
    if 'leptonica' in m: return 'leptonica'
    return 'outro'


def tem_texto_nativo(pdf_path: str, min_chars: int = MIN_CHARS_NATIVO) -> bool:
    """Verifica se o PDF tem camada de texto extraivel."""
    try:
        import pdfplumber
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages[:3]:
                txt = (page.extract_text() or '').strip()
                if len(txt) >= min_chars:
                    return True
        return False
    except Exception as e:
        logger.warning(f"pdfplumber falhou em {pdf_path}: {e}")
        return False


def extrair_texto_nativo(pdf_path: str) -> dict:
    """
    Extrai texto de PDF com camada de texto.
    Retorna: {conteudo, paginas_detalhe[], erro_geral}
    """
    resultado = {
        'conteudo': '',
        'paginas_detalhe': [],
        'erro_geral': None,
    }
    try:
        import pdfplumber
        partes = []
        with pdfplumber.open(pdf_path) as pdf:
            for i, page in enumerate(pdf.pages, 1):
                pagina_info = {'num': i, 'metodo': 'pdfplumber', 'chars': 0, 'ok': False, 'erro': None}
                try:
                    txt = page.extract_text() or ''
                    if txt.strip():
                        partes.append(f"--- Pagina {i} ---\n{txt}")
                        pagina_info['chars'] += len(txt)
                    # Tabelas
                    tabs = page.extract_tables()
                    for j, tab in enumerate(tabs, 1):
                        if tab:
                            md = _tabela_para_md(tab)
                            partes.append(f"\n[Tabela {i}.{j} - via pdfplumber]\n{md}")
                            pagina_info['chars'] += len(md)
                    pagina_info['ok'] = pagina_info['chars'] >= MIN_CHARS_NATIVO
                    if not pagina_info['ok']:
                        pagina_info['erro'] = f'apenas {pagina_info["chars"]} chars (min: {MIN_CHARS_NATIVO})'
                except Exception as e:
                    pagina_info['erro'] = f'{type(e).__name__}: {str(e)[:120]}'
                    pagina_info['categoria_erro'] = _classifica_erro(str(e))
                resultado['paginas_detalhe'].append(pagina_info)
        resultado['conteudo'] = "\n\n".join(partes)
    except Exception as e:
        resultado['erro_geral'] = f'{type(e).__name__}: {str(e)[:150]}'
        resultado['categoria_erro'] = _classifica_erro(str(e))
    return resultado


def _tabela_para_md(linhas) -> str:
    if not linhas: return ""
    out = []
    for i, linha in enumerate(linhas):
        cels = [str(c or '').replace('\n', ' ').strip() for c in linha]
        out.append("| " + " | ".join(cels) + " |")
        if i == 0:
            out.append("| " + " | ".join(['---'] * len(cels)) + " |")
    return "\n".join(out)


def extrair_via_ocr(pdf_path: str) -> dict:
    """
    OCR de PDF escaneado.
    Retorna: {conteudo, paginas_detalhe[], erro_geral}
    """
    resultado = {
        'conteudo': '',
        'paginas_detalhe': [],
        'erro_geral': None,
    }
    try:
        from pdf2image import convert_from_path
        from img2table.document import Image as I2TImg
        from img2table.ocr import TesseractOCR
        import tempfile
    except ImportError as e:
        resultado['erro_geral'] = f'lib OCR ausente: {e}'
        resultado['categoria_erro'] = 'dep_faltando'
        return resultado

    ocr = TesseractOCR(n_threads=1, lang=TESSERACT_LANG)
    partes = []

    try:
        with tempfile.TemporaryDirectory() as tmp:
            try:
                paginas = convert_from_path(pdf_path, dpi=DPI_OCR, output_folder=tmp,
                                              fmt='png', thread_count=1)
            except Exception as e:
                resultado['erro_geral'] = f'pdf2image falhou: {type(e).__name__}: {str(e)[:120]}'
                resultado['categoria_erro'] = _classifica_erro(str(e))
                return resultado

            for i, pagina in enumerate(paginas, 1):
                pagina_info = {'num': i, 'metodo': 'ocr', 'chars': 0, 'ok': False, 'erro': None}
                try:
                    png = os.path.join(tmp, f"pag_{i}.png")
                    pagina.save(png)

                    # Tesseract puro
                    try:
                        import pytesseract
                        texto = pytesseract.image_to_string(pagina, lang=TESSERACT_LANG)
                    except Exception:
                        import subprocess
                        r = subprocess.run(['tesseract', png, '-', '-l', TESSERACT_LANG],
                                            capture_output=True, text=True, timeout=120)
                        texto = r.stdout

                    if texto and texto.strip():
                        partes.append(f"--- Pagina {i} ---\n{texto.strip()}")
                        pagina_info['chars'] += len(texto)

                    # img2table
                    try:
                        doc = I2TImg(src=png)
                        tabelas = doc.extract_tables(ocr=ocr, implicit_rows=True,
                                                      borderless_tables=True, min_confidence=50)
                        for j, tab in enumerate(tabelas, 1):
                            df = tab.df
                            if df is not None and not df.empty:
                                md = df.to_markdown(index=False) if hasattr(df, 'to_markdown') else str(df)
                                partes.append(f"\n[Tabela {i}.{j} - via img2table]\n{md}")
                                pagina_info['chars'] += len(md)
                    except Exception as e:
                        pagina_info['erro_tabelas'] = f'{type(e).__name__}: {str(e)[:80]}'

                    pagina_info['ok'] = pagina_info['chars'] >= MIN_CHARS_OCR
                    if not pagina_info['ok']:
                        pagina_info['erro'] = f'OCR retornou apenas {pagina_info["chars"]} chars (min: {MIN_CHARS_OCR})'
                except Exception as e:
                    pagina_info['erro'] = f'{type(e).__name__}: {str(e)[:120]}'
                    pagina_info['categoria_erro'] = _classifica_erro(str(e))
                del pagina
                resultado['paginas_detalhe'].append(pagina_info)
    except Exception as e:
        resultado['erro_geral'] = f'{type(e).__name__}: {str(e)[:150]}'
        resultado['categoria_erro'] = _classifica_erro(str(e))

    resultado['conteudo'] = "\n\n".join(partes)
    return resultado


def processar_pdf(pdf_path: str) -> dict:
    """
    Pipeline principal com DIAGNOSTICO RICO.

    Returns:
        {
            'path': caminho,
            'tamanho_bytes': int,
            'paginas_total': int,
            'metodo': 'pdfplumber' | 'ocr' | 'falhou',
            'conteudo_md': string Markdown,
            'chars': int,
            'tempo_ms': int,
            'paginas_detalhe': [{num, metodo, chars, ok, erro, categoria_erro}],
            'paginas_ok': int,
            'paginas_falhou': int,
            'overall': 'sucesso' | 'parcial' | 'falhou',
            'erro_geral': string ou None,
        }
    """
    t0 = time.time()
    p = Path(pdf_path)
    base = {
        'path': pdf_path,
        'tempo_ms': 0,
        'conteudo_md': '',
        'chars': 0,
        'paginas_detalhe': [],
        'paginas_ok': 0,
        'paginas_falhou': 0,
    }

    if not p.exists():
        base.update({'metodo': 'falhou', 'overall': 'falhou',
                     'erro_geral': 'arquivo nao existe', 'tempo_ms': int((time.time()-t0)*1000)})
        return base

    try:
        base['tamanho_bytes'] = p.stat().st_size
    except Exception:
        base['tamanho_bytes'] = 0

    # Tenta extrair texto nativo
    nativo = tem_texto_nativo(pdf_path)
    r_nat = None
    if nativo:
        r_nat = extrair_texto_nativo(pdf_path)
        if r_nat['conteudo'] and len(r_nat['conteudo']) > 100:
            paginas_ok = sum(1 for p in r_nat['paginas_detalhe'] if p.get('ok'))
            paginas_falhou = sum(1 for p in r_nat['paginas_detalhe'] if not p.get('ok'))
            overall = 'sucesso' if paginas_falhou == 0 else ('parcial' if paginas_ok > 0 else 'falhou')
            base.update({
                'metodo': 'pdfplumber',
                'conteudo_md': r_nat['conteudo'],
                'chars': len(r_nat['conteudo']),
                'paginas_detalhe': r_nat['paginas_detalhe'],
                'paginas_total': len(r_nat['paginas_detalhe']),
                'paginas_ok': paginas_ok,
                'paginas_falhou': paginas_falhou,
                'overall': overall,
                'tempo_ms': int((time.time()-t0)*1000),
            })
            return base

    # PDF escaneado: tenta OCR
    r_ocr = extrair_via_ocr(pdf_path)
    paginas_ok = sum(1 for p in r_ocr['paginas_detalhe'] if p.get('ok'))
    paginas_falhou = sum(1 for p in r_ocr['paginas_detalhe'] if not p.get('ok'))
    overall = ('sucesso' if paginas_falhou == 0 and paginas_ok > 0
               else 'parcial' if paginas_ok > 0
               else 'falhou')
    base.update({
        'metodo': 'ocr' if r_ocr['conteudo'] else 'falhou',
        'conteudo_md': r_ocr['conteudo'],
        'chars': len(r_ocr['conteudo']),
        'paginas_detalhe': r_ocr['paginas_detalhe'],
        'paginas_total': len(r_ocr['paginas_detalhe']),
        'paginas_ok': paginas_ok,
        'paginas_falhou': paginas_falhou,
        'overall': overall,
        'erro_geral': r_ocr.get('erro_geral'),
        'tempo_ms': int((time.time()-t0)*1000),
    })
    return base


# Compatibilidade: nome anterior usava 'conteudo_md' diretamente — mantemos
if __name__ == '__main__':
    import sys, json
    pdf = sys.argv[1] if len(sys.argv) > 1 else None
    if not pdf:
        print("Uso: python ocr_tabelas.py <caminho.pdf>")
        sys.exit(1)
    r = processar_pdf(pdf)
    diag = {k: v for k, v in r.items() if k != 'conteudo_md'}
    print(f"=== DIAGNOSTICO ===")
    print(json.dumps(diag, indent=2, ensure_ascii=False))
    print(f"\n=== PREVIEW (1500 chars) ===")
    print((r.get('conteudo_md') or '')[:1500])
