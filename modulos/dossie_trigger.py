"""
═══════════════════════════════════════════════════════════════════════════════
DOSSIE TRIGGER — UrbanLex
═══════════════════════════════════════════════════════════════════════════════

Helper centralizado pra disparar o organizador_dossie em background apos
uma busca completar com sucesso.

Usado por:
  - modulos/fila_worker.py
  - app.py rotas /api/buscador/municipio, /api/buscador/lei-especifica, /api/buscador/manual

API:
  disparar_organizador_async(municipio, estado, zip_url, get_db, origem='manual')
═══════════════════════════════════════════════════════════════════════════════
"""

import os
import json
import threading
import logging

logger = logging.getLogger(__name__)


def disparar_organizador_async(municipio, estado, zip_url, get_db, origem='manual'):
    """
    Em background:
      1. Cria/recupera dossie_id em dossie_municipios
      2. Chama organizador_dossie.processar_zip_para_dossie()
      3. Salva metadados em dossie_legislacoes_pasta
    
    Args:
        municipio:  nome do municipio
        estado:     UF
        zip_url:    URL relativa do ZIP (ex: /static/downloads/X.zip)
        get_db:     funcao pra obter conexao do banco
        origem:     'manual', 'auto', 'integracao' etc
    
    Nao bloqueia. Falhas sao apenas logadas.
    """
    def _run():
        try:
            # Resolve path absoluto do ZIP
            zip_path = None
            if zip_url and zip_url.startswith('/static'):
                zip_path = '/var/www/urbanlex' + zip_url
            elif zip_url and os.path.isabs(zip_url):
                zip_path = zip_url
            
            if not zip_path or not os.path.exists(zip_path):
                logger.warning(f"[trigger {municipio}/{estado}] ZIP nao encontrado: {zip_url}")
                return
            
            # Cria ou pega dossie_id
            dossie_id = None
            try:
                c = get_db()
                cu = c.cursor()
                cu.execute(
                    "INSERT INTO dossie_municipios (municipio, estado, origem) "
                    "VALUES (%s, %s, %s) ON CONFLICT (municipio, estado) DO NOTHING",
                    (municipio, estado, origem)
                )
                cu.execute(
                    "SELECT id FROM dossie_municipios WHERE municipio=%s AND estado=%s",
                    (municipio, estado)
                )
                row = cu.fetchone()
                if row:
                    dossie_id = row[0]
                c.commit(); cu.close(); c.close()
            except Exception as e:
                logger.error(f"[trigger {municipio}/{estado}] erro criando dossie: {e}")
                return
            
            if not dossie_id:
                logger.error(f"[trigger {municipio}/{estado}] dossie_id nao obtido")
                return
            
            logger.info(f"[trigger dossie {dossie_id}] iniciando organizador para {zip_path}")
            
            # Chama o organizador
            from modulos.organizador_dossie import processar_zip_para_dossie
            
            def _log_cb(msg):
                logger.info(f"[trigger dossie {dossie_id}] {msg}")
            
            resultado = processar_zip_para_dossie(zip_path, dossie_id, log_callback=_log_cb)
            
            if not resultado.get('sucesso'):
                logger.error(f"[trigger dossie {dossie_id}] FALHOU: {resultado.get('erro')}")
                return
            
            # Salva metadados de cada legislacao em dossie_legislacoes_pasta
            for leg in resultado.get('legislacoes', []):
                try:
                    c = get_db()
                    cu = c.cursor()
                    cu.execute("""
                        INSERT INTO dossie_legislacoes_pasta 
                        (dossie_id, legislacao_label, legislacao_meta, categoria,
                         pasta_path, pdf_concatenado_path, n_paginas, total_arquivos,
                         arquivos_originais, arquivos_falhas, duplicados_removidos)
                        VALUES (%s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s)
                        ON CONFLICT (dossie_id, legislacao_label) DO UPDATE SET
                            legislacao_meta = EXCLUDED.legislacao_meta,
                            categoria = EXCLUDED.categoria,
                            pasta_path = EXCLUDED.pasta_path,
                            pdf_concatenado_path = EXCLUDED.pdf_concatenado_path,
                            n_paginas = EXCLUDED.n_paginas,
                            total_arquivos = EXCLUDED.total_arquivos,
                            arquivos_originais = EXCLUDED.arquivos_originais,
                            arquivos_falhas = EXCLUDED.arquivos_falhas,
                            duplicados_removidos = EXCLUDED.duplicados_removidos,
                            atualizado_em = NOW()
                    """, (
                        dossie_id,
                        leg['label'],
                        json.dumps(leg.get('metadados', {})),
                        leg.get('categoria', ''),
                        os.path.dirname(leg['pdf_concatenado']) if leg.get('pdf_concatenado') else '',
                        leg.get('pdf_concatenado'),
                        leg.get('n_paginas', 0),
                        leg.get('total_arquivos', 0),
                        json.dumps(leg.get('arquivos_originais', [])),
                        json.dumps(leg.get('falhas', [])),
                        leg.get('duplicados_removidos', 0),
                    ))
                    c.commit(); cu.close(); c.close()
                    logger.info(f"[trigger dossie {dossie_id}] {leg['label']}: meta salvo")
                except Exception as e:
                    logger.error(f"[trigger dossie {dossie_id}] erro salvando {leg['label']}: {e}")
            
            # ETAPA 4.5: Detecta anexos citados (chamada Haiku ~$0.05-0.30 por legislacao)
            logger.info(f"[trigger dossie {dossie_id}] iniciando Etapa 4.5 (anexos citados)...")
            try:
                from modulos.etapa_45 import detectar_anexos_citados
                
                for leg in resultado.get('legislacoes', []):
                    if not leg.get('pdf_concatenado'):
                        continue
                    
                    # Filtra apenas os anexos (não corpo) pros dados de baixados
                    arquivos = leg.get('arquivos_originais', [])
                    label_lower = leg['label'].lower()
                    anexos_baixados = [
                        a for a in arquivos
                        if not (a.get('nome', '').lower().startswith(label_lower) and a.get('tipo_detectado') == 'pdf')
                    ]
                    
                    res_e45 = detectar_anexos_citados(
                        leg['label'],
                        leg['pdf_concatenado'],
                        anexos_baixados,
                        log_callback=lambda msg, _d=dossie_id, _l=leg['label']: logger.info(f"[trigger dossie {_d}] [{_l}] {msg}")
                    )
                    
                    if res_e45.get('sucesso'):
                        # Salva no banco
                        try:
                            c = get_db()
                            cu = c.cursor()
                            cu.execute("""
                                UPDATE dossie_legislacoes_pasta 
                                SET anexos_citados = %s::jsonb,
                                    anexos_faltantes = %s::jsonb,
                                    atualizado_em = NOW()
                                WHERE dossie_id = %s AND legislacao_label = %s
                            """, (
                                json.dumps(res_e45.get('anexos_citados', [])),
                                json.dumps(res_e45.get('anexos_faltantes', [])),
                                dossie_id,
                                leg['label'],
                            ))
                            c.commit(); cu.close(); c.close()
                            logger.info(
                                f"[trigger dossie {dossie_id}] [{leg['label']}] Etapa 4.5 OK: "
                                f"{len(res_e45.get('anexos_citados', []))} citados, "
                                f"{len(res_e45.get('anexos_faltantes', []))} faltantes, "
                                f"custo ${res_e45.get('custo_estimado', 0):.4f}"
                            )
                        except Exception as e:
                            logger.error(f"[trigger dossie {dossie_id}] erro salvando E4.5: {e}")
                    else:
                        logger.error(f"[trigger dossie {dossie_id}] [{leg['label']}] Etapa 4.5 FALHOU: {res_e45.get('erro')}")
            except Exception as e:
                import traceback
                logger.error(f"[trigger dossie {dossie_id}] EXCECAO Etapa 4.5: {e} -- {traceback.format_exc()[-300:]}")
            
            logger.info(f"[trigger dossie {dossie_id}] CONCLUIDO — {len(resultado.get('legislacoes', []))} legislacao(oes)")
        
        except Exception as e:
            import traceback
            logger.error(f"[trigger {municipio}/{estado}] EXCECAO: {e} -- {traceback.format_exc()[-500:]}")
    
    threading.Thread(target=_run, daemon=True, name=f'dossie-trigger-{municipio}-{estado}').start()
