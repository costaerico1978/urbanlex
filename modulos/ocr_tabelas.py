"""
modulos/ocr_tabelas.py
======================
Pipeline de OCR estruturado para anexos de planos diretores.

Estratégia:
1. pdfplumber: tenta extrair texto nativo (PDF com camada de texto).
2. Se vazio: pdf2image rasteriza + img2table+Tesseract extrai tabelas.

Saída: Markdown estruturado para a IA (mais confiável que pixels).
"""
import os
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

DPI_OCR = 250  # equilibrio entre qualidade e RAM
TESSERACT_LANG = 'por'


def tem_texto_nativo(pdf_path: str, min_chars: int = 50) -> bool:
    """Verifica se o PDF tem camada de texto extraivel."""
    try:
        import pdfplumber
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages[:3]:  # checa só 3 primeiras págs
                txt = (page.extract_text() or '').strip()
                if len(txt) >= min_chars:
                    return True
        return False
    except Exception as e:
        logger.warning(f"pdfplumber falhou em {pdf_path}: {e}")
        return False


def extrair_texto_nativo(pdf_path: str) -> str:
    """Extrai texto de PDF que tem camada de texto."""
    try:
        import pdfplumber
        partes = []
        with pdfplumber.open(pdf_path) as pdf:
            for i, page in enumerate(pdf.pages, 1):
                txt = page.extract_text() or ''
                if txt.strip():
                    partes.append(f"--- Página {i} ---\n{txt}")
                # Tabelas extraídas separadamente
                tabs = page.extract_tables()
                for j, tab in enumerate(tabs, 1):
                    if tab:
                        md = _tabela_para_md(tab)
                        partes.append(f"\n[Tabela {i}.{j} - via pdfplumber]\n{md}")
        return "\n\n".join(partes)
    except Exception as e:
        logger.warning(f"pdfplumber extract falhou: {e}")
        return ""


def _tabela_para_md(linhas) -> str:
    """Converte lista de listas (tabela) em Markdown."""
    if not linhas: return ""
    out = []
    for i, linha in enumerate(linhas):
        cels = [str(c or '').replace('\n', ' ').strip() for c in linha]
        out.append("| " + " | ".join(cels) + " |")
        if i == 0:
            out.append("| " + " | ".join(['---'] * len(cels)) + " |")
    return "\n".join(out)


def extrair_via_ocr(pdf_path: str) -> str:
    """OCR de PDF escaneado. Retorna Markdown com tabelas+texto."""
    try:
        from pdf2image import convert_from_path
        from img2table.document import Image as I2TImg
        from img2table.ocr import TesseractOCR
        from PIL import Image as PILImage
        import tempfile
    except ImportError as e:
        logger.error(f"Lib OCR nao disponivel: {e}")
        return ""

    ocr = TesseractOCR(n_threads=1, lang=TESSERACT_LANG)
    partes = []

    try:
        # Rasteriza páginas (uma de cada vez para economizar RAM)
        with tempfile.TemporaryDirectory() as tmp:
            paginas = convert_from_path(pdf_path, dpi=DPI_OCR, output_folder=tmp,
                                          fmt='png', thread_count=1)
            for i, pagina in enumerate(paginas, 1):
                png = os.path.join(tmp, f"pag_{i}.png")
                pagina.save(png)

                # Texto bruto da página via Tesseract
                try:
                    import pytesseract
                    texto = pytesseract.image_to_string(pagina, lang=TESSERACT_LANG)
                    if texto.strip():
                        partes.append(f"--- Página {i} ---\n{texto.strip()}")
                except Exception:
                    # Se pytesseract não tiver, OCR via subprocess
                    import subprocess
                    r = subprocess.run(['tesseract', png, '-', '-l', TESSERACT_LANG],
                                        capture_output=True, text=True, timeout=120)
                    if r.stdout.strip():
                        partes.append(f"--- Página {i} ---\n{r.stdout.strip()}")

                # Extração de tabelas estruturadas via img2table
                try:
                    doc = I2TImg(src=png)
                    tabelas = doc.extract_tables(ocr=ocr,
                                                  implicit_rows=True,
                                                  borderless_tables=True,
                                                  min_confidence=50)
                    for j, tab in enumerate(tabelas, 1):
                        df = tab.df
                        if df is not None and not df.empty:
                            md = df.to_markdown(index=False) if hasattr(df, 'to_markdown') else str(df)
                            partes.append(f"\n[Tabela {i}.{j} - via img2table]\n{md}")
                except Exception as e:
                    logger.warning(f"img2table falhou pag {i}: {e}")

                # Libera RAM
                del pagina
    except Exception as e:
        logger.error(f"OCR pipeline falhou em {pdf_path}: {e}")

    return "\n\n".join(partes)


def processar_pdf(pdf_path: str) -> dict:
    """
    Pipeline principal: detecta tipo e extrai conteúdo estruturado.

    Returns:
        {
            'path': caminho original,
            'tem_texto_nativo': bool,
            'metodo': 'pdfplumber' | 'ocr' | 'falhou',
            'conteudo_md': string Markdown estruturada,
            'chars': int,
        }
    """
    p = Path(pdf_path)
    if not p.exists():
        return {'path': pdf_path, 'metodo': 'falhou', 'erro': 'nao existe'}

    nativo = tem_texto_nativo(pdf_path)

    if nativo:
        conteudo = extrair_texto_nativo(pdf_path)
        if len(conteudo) > 100:
            return {
                'path': pdf_path,
                'tem_texto_nativo': True,
                'metodo': 'pdfplumber',
                'conteudo_md': conteudo,
                'chars': len(conteudo),
            }

    # PDF escaneado ou pdfplumber falhou
    conteudo = extrair_via_ocr(pdf_path)
    return {
        'path': pdf_path,
        'tem_texto_nativo': False,
        'metodo': 'ocr' if conteudo else 'falhou',
        'conteudo_md': conteudo,
        'chars': len(conteudo),
    }


if __name__ == '__main__':
    import sys
    pdf = sys.argv[1] if len(sys.argv) > 1 else None
    if not pdf:
        print("Uso: python ocr_tabelas.py <caminho.pdf>")
        sys.exit(1)
    r = processar_pdf(pdf)
    print(f"Método: {r.get('metodo')}")
    print(f"Chars:  {r.get('chars', 0)}")
    print(f"--- Preview ---")
    print(r.get('conteudo_md', '')[:2000])
