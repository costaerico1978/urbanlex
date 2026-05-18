"""
═══════════════════════════════════════════════════════════════════════════════
DOSSIE -> GERADOR — Integracao Inteligente
═══════════════════════════════════════════════════════════════════════════════

Prepara o work_dir do pipeline_extracao_lei copiando arquivos do dossie,
permitindo que o gerador de planilha pule as Etapas 1-4 (que ja foram feitas
pelo organizador_dossie + etapa_45).

ECONOMIA: ~$0.30 + 5 min por legislacao processada.

USO:
    from modulos.dossie_para_gerador import preparar_work_dir_pipeline
    
    work_dir = preparar_work_dir_pipeline(
        dossie_id=415,
        busca_historico_id=511,
        legislacao_label='LC_482_2014',
        get_db=get_db,
    )
    
    # Agora o pipeline pode rodar com cache hit nas etapas 1-4:
    from modulos.pipeline_extracao_lei import processar_municipio
    resultado = processar_municipio(
        zip_path=None,  # nao usado quando cache existe
        municipio='Florianopolis',
        estado='SC',
        output_dir=work_dir,
        usar_cache=True,
    )
"""
import os
import json
import shutil
import logging
from typing import Optional

logger = logging.getLogger(__name__)


# Base onde o pipeline cria seus work_dirs
# IMPORTANTE: deve ser o MESMO PIPELINES_BASE_DIR do pipeline_extracao_lei.py
# Senao os arquivos sao copiados em pasta diferente e cache nao eh acionado.
try:
    from modulos.pipeline_extracao_lei import PIPELINES_BASE_DIR
except ImportError:
    PIPELINES_BASE_DIR = '/var/www/urbanlex/static/pipelines'


# CRITICO: usar o mesmo _slug_municipio do pipeline real
# senao o cache hit nao funciona (pasta sera diferente)
try:
    from modulos.pipeline_extracao_lei import _slug_municipio
except ImportError:
    def _slug_municipio(municipio, estado):
        import unicodedata, re
        m = unicodedata.normalize('NFKD', municipio).encode('ascii', 'ignore').decode('ascii')
        m = re.sub(r'[^A-Za-z0-9]+', '_', m).strip('_')
        return f"{m}_{estado.upper()}"


def preparar_work_dir_pipeline(dossie_id: int, busca_historico_id: int,
                                legislacao_label: str, get_db,
                                log_callback=None) -> Optional[str]:
    """
    Prepara o work_dir do pipeline copiando os arquivos do dossie.
    
    Args:
        dossie_id: ID do dossie municipal (mun_id)
        busca_historico_id: ID da busca (pasta busca_<id>)
        legislacao_label: label da legislacao (ex: 'LC_482_2014')
        get_db: funcao que retorna conexao psycopg2
        log_callback: opcional, funcao(msg) pra logs
    
    Retorna: path do work_dir preparado, ou None se erro.
    """
    def _log(msg):
        logger.info(f"[dossie->gerador] {msg}")
        if log_callback:
            log_callback(msg)
    
    import psycopg2.extras
    
    # ───────────────────────────────────────────────────────────────
    # 1. Busca dados da legislacao no banco
    # ───────────────────────────────────────────────────────────────
    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT dlp.id, dlp.pasta_path, dlp.legislacao_meta, dlp.anexos_citados,
               dm.municipio, dm.estado
        FROM dossie_legislacoes_pasta dlp
        LEFT JOIN dossie_municipios dm ON dm.id = dlp.dossie_id
        WHERE dlp.busca_historico_id = %s AND dlp.legislacao_label = %s
        LIMIT 1
    """, (busca_historico_id, legislacao_label))
    row = cur.fetchone()
    cur.close(); conn.close()
    
    if not row:
        _log(f"ERRO: legislacao nao encontrada (busca={busca_historico_id}, label={legislacao_label})")
        return None
    
    pasta_dossie = row['pasta_path']
    municipio = row['municipio'] or 'desconhecido'
    estado = row['estado'] or 'XX'
    
    if not pasta_dossie or not os.path.isdir(pasta_dossie):
        _log(f"ERRO: pasta do dossie nao existe: {pasta_dossie}")
        return None
    
    _log(f"Municipio: {municipio}/{estado}")
    _log(f"Pasta do dossie: {pasta_dossie}")
    
    # ───────────────────────────────────────────────────────────────
    # 2. Determina work_dir do pipeline
    # ───────────────────────────────────────────────────────────────
    slug = _slug_municipio(municipio, estado)
    work_dir = os.path.join(PIPELINES_BASE_DIR, slug)
    os.makedirs(work_dir, exist_ok=True)
    _log(f"Work dir do pipeline: {work_dir}")
    
    # ───────────────────────────────────────────────────────────────
    # 3. Copia arquivos: pdf_concatenado.pdf -> tudo.pdf (Etapa 1)
    # ───────────────────────────────────────────────────────────────
    src_concat = os.path.join(pasta_dossie, 'pdf_concatenado.pdf')
    dst_tudo = os.path.join(work_dir, 'tudo.pdf')
    
    if not os.path.exists(src_concat):
        _log(f"ERRO: pdf_concatenado.pdf nao existe em {pasta_dossie}")
        return None
    
    shutil.copy2(src_concat, dst_tudo)
    _log(f"  ✓ tudo.pdf ({os.path.getsize(dst_tudo)} bytes)")
    
    # ───────────────────────────────────────────────────────────────
    # 4. Copia corpo.pdf e anexos.pdf (Etapa 3)
    # ───────────────────────────────────────────────────────────────
    src_corpo = os.path.join(pasta_dossie, 'corpo.pdf')
    src_anexos = os.path.join(pasta_dossie, 'anexos.pdf')
    
    if os.path.exists(src_corpo) and os.path.exists(src_anexos):
        shutil.copy2(src_corpo, os.path.join(work_dir, 'corpo.pdf'))
        shutil.copy2(src_anexos, os.path.join(work_dir, 'anexos.pdf'))
        _log(f"  ✓ corpo.pdf + anexos.pdf")
    else:
        _log(f"  AVISO: corpo.pdf ou anexos.pdf faltando - pipeline rodara Etapa 3")
    
    # ───────────────────────────────────────────────────────────────
    # 5. Cria cache JSON da Etapa 4 a partir de anexos_citados
    #    (formato: {blocos: [{nome, titulo, inicio, fim, tipo}]})
    # ───────────────────────────────────────────────────────────────
    # NOTA: a etapa_45 nao salva o catalogo completo no banco hoje.
    # O catalogo eh gerado dinamicamente cada vez que chama Haiku.
    # Pra reusar, vamos DEIXAR o pipeline rodar a Etapa 4 normalmente
    # (custo $0.20). Em iteracao futura, salvaremos o catalogo da etapa_45.
    
    # Cache Etapa 4 (futuro): work_dir/etapa4_catalogacao.json
    # Por ora, NAO criamos esse cache. Pipeline rodara Etapa 4 do zero.
    
    _log(f"  AVISO: Etapa 4 nao tem cache do dossie ainda (rodara normalmente, custo ~USD 0.20)")
    
    # ───────────────────────────────────────────────────────────────
    # 6. Retorna work_dir preparado
    # ───────────────────────────────────────────────────────────────
    _log(f"✓ Work dir preparado: {work_dir}")
    return work_dir
