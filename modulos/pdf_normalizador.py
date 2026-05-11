"""
modulos/pdf_normalizador.py
============================
Normaliza PDFs para o pipeline UrbanLex:
- Detecta páginas-imagem (sem texto extraível)
- Se houver, roda OCRmyPDF para gerar PDF "searchable"
  (visualmente idêntico ao original + texto extraível)

Esse módulo é a PRÉ-PASSAGEM do pipeline em 3 níveis:
  PRÉ → Flash triagem → Pro extração → Sonnet validação

Uso típico:
    from modulos.pdf_normalizador import normalizar_pdf
    
    resultado = normalizar_pdf('/tmp/lei.pdf')
    if resultado['ocr_aplicado']:
        # PDF passou por OCR, novo path
        pdf_para_usar = resultado['path_final']
    else:
        # PDF original ja era searchable
        pdf_para_usar = resultado['path_final']
"""
import os
import time
import logging
import subprocess
import tempfile
import shutil
from pathlib import Path

logger = logging.getLogger(__name__)

# Configuracao
MIN_CHARS_POR_PAGINA = 30  # menos que isso = pagina-imagem
PERCENT_PAGINAS_TEXTO_MIN = 0.5  # 50% das paginas precisam ter texto
OCRMYPDF_TIMEOUT = 600  # 10 min max por PDF
OCR_LANG = 'por+eng'  # OCR em portugues e ingles


def _contar_chars_por_pagina(pdf_path: str) -> list:
    """Retorna lista com chars por pagina via pdfplumber."""
    try:
        import pdfplumber
        chars_por_pag = []
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                try:
                    txt = (page.extract_text() or '').strip()
                    chars_por_pag.append(len(txt))
                except Exception:
                    chars_por_pag.append(0)
        return chars_por_pag
    except Exception as e:
        logger.warning(f"pdfplumber falhou em {pdf_path}: {e}")
        return []


def diagnosticar_pdf(pdf_path: str) -> dict:
    """
    Analisa um PDF e retorna diagnostico:
        {
            'total_paginas': int,
            'paginas_com_texto': int,
            'paginas_imagem': int,
            'percent_texto': float,
            'precisa_ocr': bool,
            'chars_por_pagina': [int, ...],
        }
    """
    chars = _contar_chars_por_pagina(pdf_path)
    total = len(chars)
    com_texto = sum(1 for c in chars if c >= MIN_CHARS_POR_PAGINA)
    imagem = total - com_texto
    percent = com_texto / total if total > 0 else 0
    
    return {
        'total_paginas': total,
        'paginas_com_texto': com_texto,
        'paginas_imagem': imagem,
        'percent_texto': percent,
        'precisa_ocr': percent < PERCENT_PAGINAS_TEXTO_MIN,
        'chars_por_pagina': chars,
    }


def aplicar_ocrmypdf(pdf_path: str, output_path: str = None) -> dict:
    """
    Roda OCRmyPDF em um PDF.
    Retorna:
        {
            'sucesso': bool,
            'path_saida': str,
            'tempo_ms': int,
            'tamanho_antes': int,
            'tamanho_depois': int,
            'erro': str ou None,
        }
    """
    t0 = time.time()
    
    if output_path is None:
        # Gera path temporario unico
        base = Path(pdf_path).stem
        output_path = tempfile.mktemp(prefix=f'{base}_ocr_', suffix='.pdf')
    
    tamanho_antes = Path(pdf_path).stat().st_size
    
    # Usa --skip-text (so OCR onde nao tem texto)
    # --output-type pdf (compatibilidade ampla)
    cmd = [
        'ocrmypdf',
        '--skip-text',  # nao reprocessa paginas que ja tem texto
        '--output-type', 'pdf',
        '--language', OCR_LANG,
        '--quiet',
        pdf_path,
        output_path,
    ]
    
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True,
            timeout=OCRMYPDF_TIMEOUT
        )
        
        if result.returncode == 0 and Path(output_path).exists():
            tamanho_depois = Path(output_path).stat().st_size
            return {
                'sucesso': True,
                'path_saida': output_path,
                'tempo_ms': int((time.time() - t0) * 1000),
                'tamanho_antes': tamanho_antes,
                'tamanho_depois': tamanho_depois,
                'erro': None,
            }
        else:
            erro_msg = result.stderr[-500:] if result.stderr else 'returncode != 0'
            return {
                'sucesso': False,
                'path_saida': None,
                'tempo_ms': int((time.time() - t0) * 1000),
                'tamanho_antes': tamanho_antes,
                'tamanho_depois': 0,
                'erro': erro_msg,
            }
    except subprocess.TimeoutExpired:
        return {
            'sucesso': False,
            'path_saida': None,
            'tempo_ms': OCRMYPDF_TIMEOUT * 1000,
            'tamanho_antes': tamanho_antes,
            'tamanho_depois': 0,
            'erro': f'timeout ({OCRMYPDF_TIMEOUT}s)',
        }
    except Exception as e:
        return {
            'sucesso': False,
            'path_saida': None,
            'tempo_ms': int((time.time() - t0) * 1000),
            'tamanho_antes': tamanho_antes,
            'tamanho_depois': 0,
            'erro': f'{type(e).__name__}: {str(e)[:200]}',
        }


def normalizar_pdf(pdf_path: str, forcar_ocr: bool = False) -> dict:
    """
    Funcao principal: detecta se PDF precisa de OCR e aplica se necessario.
    
    Args:
        pdf_path: caminho do PDF original
        forcar_ocr: forca aplicacao de OCR mesmo se PDF ja tem texto
    
    Returns:
        {
            'path_original': str,
            'path_final': str,        # mesmo path ou path do PDF processado
            'ocr_aplicado': bool,
            'diagnostico': dict,
            'ocr_resultado': dict ou None,
            'erro': str ou None,
        }
    """
    if not Path(pdf_path).exists():
        return {
            'path_original': pdf_path,
            'path_final': pdf_path,
            'ocr_aplicado': False,
            'diagnostico': None,
            'ocr_resultado': None,
            'erro': 'arquivo nao existe',
        }
    
    # 1. Diagnostica
    diag = diagnosticar_pdf(pdf_path)
    
    # 2. Decide se aplica OCR
    aplicar = forcar_ocr or diag['precisa_ocr']
    
    if not aplicar:
        # PDF ja tem texto suficiente
        return {
            'path_original': pdf_path,
            'path_final': pdf_path,
            'ocr_aplicado': False,
            'diagnostico': diag,
            'ocr_resultado': None,
            'erro': None,
        }
    
    # 3. Aplica OCRmyPDF
    ocr_res = aplicar_ocrmypdf(pdf_path)
    
    if ocr_res['sucesso']:
        return {
            'path_original': pdf_path,
            'path_final': ocr_res['path_saida'],
            'ocr_aplicado': True,
            'diagnostico': diag,
            'ocr_resultado': ocr_res,
            'erro': None,
        }
    else:
        # OCR falhou: retorna PDF original como fallback
        return {
            'path_original': pdf_path,
            'path_final': pdf_path,
            'ocr_aplicado': False,
            'diagnostico': diag,
            'ocr_resultado': ocr_res,
            'erro': f'OCR falhou: {ocr_res.get("erro")}',
        }


if __name__ == '__main__':
    import sys, json
    if len(sys.argv) < 2:
        print("Uso: python pdf_normalizador.py <caminho_pdf>")
        sys.exit(1)
    
    r = normalizar_pdf(sys.argv[1])
    print(json.dumps({
        'path_original': r['path_original'],
        'path_final': r['path_final'],
        'ocr_aplicado': r['ocr_aplicado'],
        'erro': r['erro'],
        'diagnostico': r['diagnostico'],
        'ocr_resultado': r.get('ocr_resultado'),
    }, indent=2, ensure_ascii=False, default=str))
