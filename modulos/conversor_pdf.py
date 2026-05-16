"""
═══════════════════════════════════════════════════════════════════════════════
CONVERSOR PDF — UrbanLex
═══════════════════════════════════════════════════════════════════════════════

Identifica tipo de arquivo (magic bytes) e converte pra PDF se possível.

FORMATOS SUPORTADOS:
  - PDF (passa direto)
  - JPG, JPEG, PNG (via Pillow + reportlab)
  - DOC, DOCX, ODT (via libreoffice headless)
  - XLS, XLSX, ODS (via libreoffice headless)
  - HTML (via wkhtmltopdf)
  - TXT (via reportlab)

API:
  identificar_tipo(path) -> str  ('pdf', 'jpg', 'docx', 'html', 'txt', 'desconhecido')
  converter_para_pdf(path, dest_dir) -> str | None  (path do PDF ou None)
  concatenar_pdfs(lista_pdfs, dest_path) -> bool
═══════════════════════════════════════════════════════════════════════════════
"""

import os
import subprocess
import logging
import shutil
import tempfile
from pathlib import Path

logger = logging.getLogger(__name__)

# Magic bytes (primeiros bytes de cada formato)
MAGIC_SIGNATURES = {
    b'%PDF':         'pdf',
    b'\xFF\xD8\xFF': 'jpg',
    b'\x89PNG\r\n\x1a\n': 'png',
    b'PK\x03\x04':   'zip_based',   # docx, xlsx, odt, ods (todos ZIP)
    b'\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1': 'ms_office_legacy',  # doc, xls antigos
    b'<!DOCTYPE':    'html',
    b'<html':        'html',
    b'<HTML':        'html',
}


def identificar_tipo(path):
    """
    Identifica tipo real do arquivo via magic bytes + análise complementar.
    
    Retorna string: 'pdf', 'jpg', 'png', 'docx', 'doc', 'xlsx', 'xls',
                    'odt', 'ods', 'html', 'txt', 'desconhecido'.
    """
    if not os.path.exists(path):
        return 'inexistente'
    
    if os.path.getsize(path) == 0:
        return 'vazio'
    
    try:
        with open(path, 'rb') as f:
            header = f.read(16)
    except Exception as e:
        logger.error(f"Erro ao ler {path}: {e}")
        return 'erro_leitura'
    
    # Checa magic bytes
    for sig, tipo in MAGIC_SIGNATURES.items():
        if header.startswith(sig):
            if tipo == 'zip_based':
                # docx/xlsx/odt sao ZIPs - precisa olhar conteudo
                return _identificar_zip_office(path)
            if tipo == 'ms_office_legacy':
                # doc/xls antigos - chuta pela extensao
                ext = Path(path).suffix.lower()
                if ext in ('.doc',): return 'doc'
                if ext in ('.xls',): return 'xls'
                return 'doc'  # assumir doc por default
            return tipo
    
    # Sem magic bytes claros — tenta texto/HTML
    try:
        with open(path, 'rb') as f:
            sample = f.read(2048)
        # Tenta decodificar como texto
        try:
            text = sample.decode('utf-8', errors='strict')
            # Procura indicadores HTML
            lower = text.lower()
            if '<html' in lower or '<!doctype html' in lower:
                return 'html'
            # Se decodificou como UTF-8 sem erro, é texto
            return 'txt'
        except UnicodeDecodeError:
            try:
                text = sample.decode('latin-1', errors='strict')
                # latin-1 sempre decodifica, mas se eh texto plausivel
                if all(c.isprintable() or c in '\n\r\t ' for c in text[:200]):
                    return 'txt'
            except Exception:
                pass
    except Exception:
        pass
    
    return 'desconhecido'


def _identificar_zip_office(path):
    """ZIPs do Office: olha dentro pra identificar docx/xlsx/odt/ods."""
    try:
        import zipfile
        with zipfile.ZipFile(path, 'r') as z:
            names = z.namelist()
            joined = '|'.join(names[:30])
            if 'word/' in joined:
                return 'docx'
            if 'xl/' in joined:
                return 'xlsx'
            if 'mimetype' in names:
                # ODT/ODS tem arquivo 'mimetype' com identificacao
                try:
                    mime = z.read('mimetype').decode('utf-8', errors='ignore')
                    if 'opendocument.text' in mime:
                        return 'odt'
                    if 'opendocument.spreadsheet' in mime:
                        return 'ods'
                except Exception:
                    pass
            return 'zip_desconhecido'
    except Exception:
        return 'desconhecido'


def converter_para_pdf(path, dest_dir):
    """
    Tenta converter arquivo pra PDF. Retorna path do PDF ou None.
    
    Args:
        path:     arquivo de entrada
        dest_dir: diretorio onde salvar o PDF
    
    Retorna:
        str path do PDF gerado, ou None se conversao falhou
    """
    tipo = identificar_tipo(path)
    nome_base = Path(path).stem
    dest_pdf = os.path.join(dest_dir, f'{nome_base}.pdf')
    
    logger.info(f"Convertendo {path} (tipo={tipo}) -> {dest_pdf}")
    
    if tipo == 'pdf':
        # Já é PDF, só copia
        try:
            shutil.copy2(path, dest_pdf)
            return dest_pdf
        except Exception as e:
            logger.error(f"Erro ao copiar PDF: {e}")
            return None
    
    if tipo in ('jpg', 'png'):
        return _converter_imagem(path, dest_pdf)
    
    if tipo in ('docx', 'doc', 'odt', 'xlsx', 'xls', 'ods'):
        return _converter_libreoffice(path, dest_dir, dest_pdf)
    
    if tipo == 'html':
        return _converter_html(path, dest_pdf)
    
    if tipo == 'txt':
        return _converter_txt(path, dest_pdf)
    
    logger.warning(f"Tipo {tipo} nao suportado: {path}")
    return None


def _converter_imagem(path, dest_pdf):
    """JPG/PNG → PDF via Pillow."""
    try:
        from PIL import Image
        img = Image.open(path)
        # PDF nao suporta RGBA (PNG transparente)
        if img.mode == 'RGBA':
            background = Image.new('RGB', img.size, (255, 255, 255))
            background.paste(img, mask=img.split()[3])
            img = background
        elif img.mode != 'RGB':
            img = img.convert('RGB')
        img.save(dest_pdf, 'PDF', resolution=100.0)
        return dest_pdf
    except Exception as e:
        logger.error(f"Erro convertendo imagem {path}: {e}")
        return None


def _converter_libreoffice(path, dest_dir, dest_pdf):
    """DOC/DOCX/ODT/XLS/XLSX/ODS → PDF via libreoffice."""
    try:
        # Profile temporario evita conflito entre execucoes paralelas
        with tempfile.TemporaryDirectory(prefix='lo_profile_') as profile_dir:
            cmd = [
                'libreoffice',
                '--headless',
                '--norestore',
                '--nologo',
                '--nofirststartwizard',
                f'-env:UserInstallation=file://{profile_dir}',
                '--convert-to', 'pdf',
                '--outdir', dest_dir,
                path,
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            if result.returncode != 0:
                logger.error(f"libreoffice falhou: {result.stderr[:500]}")
                return None
            # libreoffice salva com nome.pdf no dest_dir
            esperado = os.path.join(dest_dir, Path(path).stem + '.pdf')
            if os.path.exists(esperado):
                if esperado != dest_pdf:
                    shutil.move(esperado, dest_pdf)
                return dest_pdf
            return None
    except subprocess.TimeoutExpired:
        logger.error(f"libreoffice timeout em {path}")
        return None
    except Exception as e:
        logger.error(f"Erro libreoffice {path}: {e}")
        return None


def _converter_html(path, dest_pdf):
    """HTML → PDF via wkhtmltopdf."""
    try:
        cmd = ['wkhtmltopdf', '-q', '--encoding', 'UTF-8', path, dest_pdf]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if result.returncode == 0 and os.path.exists(dest_pdf):
            return dest_pdf
        logger.error(f"wkhtmltopdf falhou: {result.stderr[:500]}")
        return None
    except Exception as e:
        logger.error(f"Erro wkhtmltopdf {path}: {e}")
        return None


def _converter_txt(path, dest_pdf):
    """TXT → PDF via reportlab."""
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.pdfgen import canvas
        from reportlab.lib.units import cm
        
        # Le o texto
        try:
            with open(path, 'r', encoding='utf-8') as f:
                texto = f.read()
        except UnicodeDecodeError:
            with open(path, 'r', encoding='latin-1') as f:
                texto = f.read()
        
        c = canvas.Canvas(dest_pdf, pagesize=A4)
        width, height = A4
        margin = 2 * cm
        y = height - margin
        line_height = 12
        
        c.setFont('Helvetica', 9)
        for linha in texto.split('\n'):
            # Quebra linha em pedacos se for muito longa
            while linha:
                chunk = linha[:100]
                linha = linha[100:]
                c.drawString(margin, y, chunk)
                y -= line_height
                if y < margin:
                    c.showPage()
                    c.setFont('Helvetica', 9)
                    y = height - margin
        c.save()
        return dest_pdf
    except Exception as e:
        logger.error(f"Erro reportlab {path}: {e}")
        return None


def concatenar_pdfs(lista_pdfs, dest_path):
    """
    Concatena lista de PDFs num único arquivo usando pdfunite.
    
    Args:
        lista_pdfs: lista de paths
        dest_path:  arquivo de saida
    
    Retorna:
        True se sucesso, False senao
    """
    if not lista_pdfs:
        return False
    
    # Filtra apenas PDFs que existem
    pdfs_validos = [p for p in lista_pdfs if p and os.path.exists(p) and os.path.getsize(p) > 0]
    
    if not pdfs_validos:
        logger.warning(f"Nenhum PDF valido pra concatenar em {dest_path}")
        return False
    
    if len(pdfs_validos) == 1:
        # Só 1 arquivo, copia direto
        try:
            shutil.copy2(pdfs_validos[0], dest_path)
            return True
        except Exception as e:
            logger.error(f"Erro copiando PDF unico: {e}")
            return False
    
    try:
        cmd = ['pdfunite'] + pdfs_validos + [dest_path]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if result.returncode == 0 and os.path.exists(dest_path):
            return True
        logger.error(f"pdfunite falhou: {result.stderr[:500]}")
        return False
    except Exception as e:
        logger.error(f"Erro pdfunite: {e}")
        return False
