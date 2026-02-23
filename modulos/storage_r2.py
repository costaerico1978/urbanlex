"""
modulos/storage_r2.py — Cloudflare R2 storage via S3-compatible API

Dependência: boto3 (já no requirements.txt)
Variáveis de ambiente necessárias:
  R2_ACCOUNT_ID    — ID da conta Cloudflare (ex: abc123def456...)
  R2_ACCESS_KEY    — Access Key ID do token R2
  R2_SECRET_KEY    — Secret Access Key do token R2
  R2_BUCKET_NAME   — Nome do bucket (ex: urbanlex-docs)
  R2_PUBLIC_URL    — (opcional) URL pública custom (ex: https://docs.urbanlex.com.br)
                     Se não definida, usa URL pré-assinada temporária
"""

import os
import uuid
import mimetypes
import logging
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)

# ── Lazy init do cliente boto3 ────────────────────────────────────────────────

_s3 = None

def _get_client():
    global _s3
    if _s3 is not None:
        return _s3

    try:
        import boto3
        account_id  = os.environ.get('R2_ACCOUNT_ID','')
        access_key  = os.environ.get('R2_ACCESS_KEY','')
        secret_key  = os.environ.get('R2_SECRET_KEY','')

        if not all([account_id, access_key, secret_key]):
            logger.warning('R2: variáveis de ambiente incompletas — storage desativado')
            return None

        _s3 = boto3.client(
            's3',
            endpoint_url=f'https://{account_id}.r2.cloudflarestorage.com',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name='auto',
        )
        logger.info('R2: cliente boto3 iniciado')
        return _s3

    except ImportError:
        logger.error('R2: boto3 não instalado — execute pip install boto3')
        return None
    except Exception as e:
        logger.error(f'R2: erro ao iniciar cliente: {e}')
        return None


def r2_disponivel() -> bool:
    """Retorna True se o R2 está configurado e acessível."""
    return _get_client() is not None and bool(os.environ.get('R2_BUCKET_NAME'))


# ── Upload ────────────────────────────────────────────────────────────────────

def upload_arquivo(
    arquivo_bytes: bytes,
    nome_original: str,
    leg_id: int | None = None,
    content_type: str | None = None,
) -> str | None:
    """
    Faz upload de um arquivo para o R2.

    Args:
        arquivo_bytes:  conteúdo binário do arquivo
        nome_original:  nome original (ex: "lei_16050_2014.pdf")
        leg_id:         ID da legislação (usado para organizar chave no bucket)
        content_type:   MIME type; detectado automaticamente se None

    Returns:
        URL pública/assinada do arquivo, ou None em caso de erro
    """
    client = _get_client()
    if not client:
        return None

    bucket = os.environ.get('R2_BUCKET_NAME', '')
    if not bucket:
        logger.error('R2: R2_BUCKET_NAME não definido')
        return None

    # Gerar chave única no bucket
    ext       = Path(nome_original).suffix.lower()
    uid       = uuid.uuid4().hex[:12]
    pasta     = f'legislacoes/{leg_id}' if leg_id else 'legislacoes/sem_id'
    ano_mes   = datetime.utcnow().strftime('%Y%m')
    chave     = f'{pasta}/{ano_mes}_{uid}{ext}'

    # Detectar content-type
    if not content_type:
        content_type, _ = mimetypes.guess_type(nome_original)
        content_type = content_type or 'application/octet-stream'

    try:
        extra_args = {'ContentType': content_type}

        # Se há URL pública configurada, o objeto pode ser público
        public_url_base = os.environ.get('R2_PUBLIC_URL', '')
        if public_url_base:
            extra_args['ACL'] = 'public-read'

        client.put_object(
            Bucket=bucket,
            Key=chave,
            Body=arquivo_bytes,
            **extra_args,
        )
        logger.info(f'R2: upload OK — {chave} ({len(arquivo_bytes)} bytes)')

        # Montar URL de acesso
        url = _montar_url(chave)
        return url

    except Exception as e:
        logger.error(f'R2: erro no upload de {nome_original}: {e}')
        return None


# ── Download ──────────────────────────────────────────────────────────────────

def download_arquivo(arquivo_url: str) -> bytes | None:
    """
    Baixa um arquivo do R2 a partir da URL (ou chave).

    Args:
        arquivo_url: URL completa ou chave S3 do arquivo

    Returns:
        Bytes do arquivo, ou None em caso de erro
    """
    client = _get_client()
    if not client:
        return None

    bucket = os.environ.get('R2_BUCKET_NAME', '')
    chave  = _url_para_chave(arquivo_url)

    try:
        resp = client.get_object(Bucket=bucket, Key=chave)
        dados = resp['Body'].read()
        logger.info(f'R2: download OK — {chave} ({len(dados)} bytes)')
        return dados
    except Exception as e:
        logger.error(f'R2: erro no download de {chave}: {e}')
        return None


# ── Delete ────────────────────────────────────────────────────────────────────

def deletar_arquivo(arquivo_url: str) -> bool:
    """
    Remove um arquivo do R2.

    Args:
        arquivo_url: URL completa ou chave S3 do arquivo

    Returns:
        True se deletado com sucesso, False caso contrário
    """
    client = _get_client()
    if not client:
        return False

    bucket = os.environ.get('R2_BUCKET_NAME', '')
    chave  = _url_para_chave(arquivo_url)

    try:
        client.delete_object(Bucket=bucket, Key=chave)
        logger.info(f'R2: deletado — {chave}')
        return True
    except Exception as e:
        logger.error(f'R2: erro ao deletar {chave}: {e}')
        return False


# ── URL assinada (acesso temporário) ─────────────────────────────────────────

def gerar_url_assinada(arquivo_url: str, expiracao_seg: int = 3600) -> str | None:
    """
    Gera URL pré-assinada com expiração (útil para objetos privados).

    Args:
        arquivo_url:    URL completa ou chave do arquivo
        expiracao_seg:  Segundos até a expiração (padrão: 1 hora)

    Returns:
        URL temporária de acesso, ou None em caso de erro
    """
    client = _get_client()
    if not client:
        return None

    bucket = os.environ.get('R2_BUCKET_NAME', '')
    chave  = _url_para_chave(arquivo_url)

    try:
        url = client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket, 'Key': chave},
            ExpiresIn=expiracao_seg,
        )
        return url
    except Exception as e:
        logger.error(f'R2: erro ao gerar URL assinada para {chave}: {e}')
        return None


# ── Helpers internos ──────────────────────────────────────────────────────────

def _montar_url(chave: str) -> str:
    """Monta a URL de acesso ao objeto no R2."""
    public_url_base = os.environ.get('R2_PUBLIC_URL', '').rstrip('/')
    if public_url_base:
        return f'{public_url_base}/{chave}'

    # URL pré-assinada de 7 dias para objetos privados
    client = _get_client()
    bucket = os.environ.get('R2_BUCKET_NAME', '')
    try:
        return client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket, 'Key': chave},
            ExpiresIn=7 * 24 * 3600,  # 7 dias
        )
    except Exception:
        # Fallback: retornar a chave pura (será tratada pelo endpoint)
        return chave


def _url_para_chave(url_ou_chave: str) -> str:
    """Extrai a chave S3 de uma URL completa."""
    public_url_base = os.environ.get('R2_PUBLIC_URL', '').rstrip('/')
    if public_url_base and url_ou_chave.startswith(public_url_base):
        return url_ou_chave[len(public_url_base):].lstrip('/')

    # Se for URL pré-assinada, extrair o path
    if url_ou_chave.startswith('http'):
        from urllib.parse import urlparse
        parsed = urlparse(url_ou_chave)
        return parsed.path.lstrip('/')

    # Já é uma chave
    return url_ou_chave
