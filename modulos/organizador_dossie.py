"""
═══════════════════════════════════════════════════════════════════════════════
ORGANIZADOR DE DOSSIÊ — UrbanLex
═══════════════════════════════════════════════════════════════════════════════

Recebe um ZIP no formato v2 (gerado pela busca automática) e organiza em
pastas separadas por legislação, com PDF concatenado pronto pro pipeline.

ENTRADA (ZIP formato v2):
   Compilado.zip
     ├── LC_X_Y.zip
     │     ├── LC_X_Y.pdf
     │     └── Anexos.zip (opcional)
     │           ├── anexo_1
     │           └── anexo_2
     └── legislacoes.json

SAÍDA (pastas no servidor):
   /static/dossies/<dossie_id>/
     ├── LC_X_Y/
     │     ├── pdf_concatenado.pdf      ← corpo + anexos juntos
     │     └── arquivos_originais.json   ← metadados
     └── (outras legislações)

API:
   processar_zip_para_dossie(zip_path, dossie_id, log_callback=None) -> dict
═══════════════════════════════════════════════════════════════════════════════
"""

import os
import json
import hashlib
import logging
import shutil
import zipfile
import tempfile
import io
import re
from pathlib import Path

from modulos.conversor_pdf import identificar_tipo, converter_para_pdf, concatenar_pdfs

logger = logging.getLogger(__name__)

DOSSIES_BASE_DIR = '/var/www/urbanlex/static/dossies'


def _slug(s):
    """Slugify pra nome de arquivo (mantém compatibilidade com busca)."""
    import unicodedata as _ud
    s = (s or '').strip()
    s = _ud.normalize('NFKD', s).encode('ascii', 'ignore').decode()
    s = re.sub(r'[^a-zA-Z0-9_\-\.]', '_', s)
    s = re.sub(r'_+', '_', s).strip('_')
    return s or 'sem_nome'


def _md5_arquivo(path):
    """MD5 streaming pra dedup."""
    h = hashlib.md5()
    try:
        with open(path, 'rb') as f:
            for chunk in iter(lambda: f.read(65536), b''):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None


def _extrair_zip_para_dir(zip_path, dest_dir, prefixo=''):
    """
    Extrai um ZIP no dest_dir, retornando lista de arquivos extraídos.
    Não faz recursão — quem chama decide se vai expandir ZIPs aninhados.
    
    Args:
        zip_path:  pode ser path em disco OU bytes (BytesIO)
        dest_dir:  onde extrair
        prefixo:   string adicionada ao nome (pra evitar colisão de nomes)
    
    Retorna:
        Lista de dicts {nome_original, path_extraido, eh_zip}
    """
    arquivos = []
    
    try:
        if isinstance(zip_path, (bytes, io.BytesIO)):
            zf = zipfile.ZipFile(zip_path if isinstance(zip_path, io.BytesIO) else io.BytesIO(zip_path))
        else:
            zf = zipfile.ZipFile(zip_path)
        
        with zf:
            for info in zf.infolist():
                if info.is_dir():
                    continue
                nome = info.filename
                # Pula lixo
                if '__MACOSX' in nome or nome.endswith('.DS_Store'):
                    continue
                
                # Nome de saída único
                nome_base = os.path.basename(nome) or nome.replace('/', '_')
                if prefixo:
                    nome_saida = f"{prefixo}__{nome_base}"
                else:
                    nome_saida = nome_base
                
                # Trata colisão (raro)
                target = os.path.join(dest_dir, _slug(nome_saida))
                base, ext = os.path.splitext(target)
                contador = 1
                while os.path.exists(target):
                    target = f"{base}_{contador}{ext}"
                    contador += 1
                
                # Extrai
                try:
                    with zf.open(info.filename) as src, open(target, 'wb') as dst:
                        shutil.copyfileobj(src, dst)
                except Exception as e:
                    logger.error(f"Erro extraindo {nome}: {e}")
                    continue
                
                arquivos.append({
                    'nome_original': nome,
                    'path_extraido': target,
                    'eh_zip': nome.lower().endswith('.zip'),
                })
    except Exception as e:
        logger.error(f"Erro abrindo ZIP: {e}")
    
    return arquivos


def _gerar_label_da_legislacao(leg):
    """
    Gera label de pasta a partir do meta de uma legislação no legislacoes.json.
    Ex: {tipo: 'Lei Complementar', numero: '148', ano: '2023'} → 'LC_148_2023'
    
    Se houver campo 'arquivo_zip' (formato v2), usa nome SEM .zip.
    """
    arquivo_zip = leg.get('arquivo_zip', '')
    if arquivo_zip and arquivo_zip.endswith('.zip'):
        return arquivo_zip[:-4]  # remove '.zip'
    
    # Fallback: monta do tipo+numero+ano
    tipo = (leg.get('tipo') or '').strip()
    numero = (leg.get('numero') or '').strip()
    ano = (leg.get('ano') or '').strip()
    tipo_abrev = {
        'Lei Complementar': 'LC',
        'Lei Ordinária': 'LO',
        'Decreto': 'Dec',
        'Decreto-Lei': 'DL',
        'Errata': 'Err',
        'Portaria': 'Port',
    }.get(tipo, _slug(tipo))
    return _slug(f"{tipo_abrev}_{numero}_{ano}") or 'sem_label'


def processar_zip_para_dossie(zip_path, dossie_id, log_callback=None, busca_id=None):
    """
    Função principal. Processa o ZIP do dossiê e organiza em /static/dossies/<id>/.
    
    Args:
        zip_path:     path do ZIP (formato v2)
        dossie_id:    ID do dossiê no banco
        log_callback: função(msg) opcional pra logging em UI
    
    Retorna:
        dict {
            sucesso: bool,
            dossie_dir: path da pasta criada,
            formato_versao: int (2 ou None se incompatível),
            legislacoes: [
                {
                    label, categoria, metadados,
                    pdf_concatenado: path,
                    n_paginas: int,
                    arquivos_originais: [...],
                    falhas: [...],
                }
            ],
            total_arquivos: int,
            erro: str (se sucesso=False)
        }
    """
    def _log(msg):
        logger.info(f"[dossie {dossie_id}] {msg}")
        if log_callback:
            log_callback(msg)
    
    if not os.path.exists(zip_path):
        return {'sucesso': False, 'erro': f'ZIP nao encontrado: {zip_path}'}
    
    _log(f"Iniciando processamento de {zip_path}")
    _log(f"Tamanho: {os.path.getsize(zip_path):,} bytes")
    
    # PASSO 1: Cria pasta do dossiê
    # Se busca_id fornecido: /static/dossies/<mun_id>/busca_<bh_id>/
    # Senao (legacy):        /static/dossies/<mun_id>/
    if busca_id:
        dossie_dir = os.path.join(DOSSIES_BASE_DIR, str(dossie_id), f'busca_{busca_id}')
    else:
        dossie_dir = os.path.join(DOSSIES_BASE_DIR, str(dossie_id))
    os.makedirs(dossie_dir, exist_ok=True)
    _log(f"Output dir: {dossie_dir}")
    
    # ──────────────────────────────────────────────────────────────────
    # PASSO 2: Ler legislacoes.json + extrair ZIPs aninhados
    # ──────────────────────────────────────────────────────────────────
    legislacoes_meta = []
    formato_versao = None
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            nomes = z.namelist()
            if 'legislacoes.json' in nomes:
                with z.open('legislacoes.json') as f:
                    meta = json.loads(f.read().decode('utf-8'))
                    legislacoes_meta = meta.get('legislacoes', [])
                    formato_versao = meta.get('formato_versao', 1)
                _log(f"legislacoes.json: {len(legislacoes_meta)} legislações (formato v{formato_versao})")
            else:
                _log("AVISO: legislacoes.json não encontrado")
    except Exception as e:
        _log(f"AVISO: erro lendo legislacoes.json: {e}")
    
    if not legislacoes_meta:
        return {
            'sucesso': False,
            'erro': 'Nenhuma legislação encontrada no ZIP (sem legislacoes.json ou vazio)',
            'dossie_dir': dossie_dir,
        }
    
    # ──────────────────────────────────────────────────────────────────
    # PASSO 3: Pra cada legislação, extrair + dedup + converter + concatenar
    # ──────────────────────────────────────────────────────────────────
    resultado_legislacoes = []
    total_arquivos = 0
    
    with zipfile.ZipFile(zip_path, 'r') as zip_principal:
        nomes_zip_principal = zip_principal.namelist()
        
        for leg in legislacoes_meta:
            label = _gerar_label_da_legislacao(leg)
            _log(f"")
            _log(f"=== Processando legislação: {label} ===")
            
            # Cria subpasta da legislação
            leg_dir = os.path.join(dossie_dir, label)
            os.makedirs(leg_dir, exist_ok=True)
            
            # Pasta temporária para extração + conversão
            with tempfile.TemporaryDirectory(prefix=f'dossie_{dossie_id}_{label}_') as tmp:
                arquivos_originais = []
                falhas = []
                pdfs_pra_concatenar = []
                
                # Identifica o ZIP da legislação no formato v2
                arquivo_zip_leg = leg.get('arquivo_zip')
                
                if formato_versao == 2 and arquivo_zip_leg and arquivo_zip_leg in nomes_zip_principal:
                    # FORMATO v2: ZIP por legislação dentro do ZIP principal
                    _log(f"  Extraindo {arquivo_zip_leg}...")
                    leg_zip_bytes = zip_principal.read(arquivo_zip_leg)
                    
                    # Extrai conteúdo do ZIP da legislação pra tmp
                    arquivos_leg = _extrair_zip_para_dir(
                        io.BytesIO(leg_zip_bytes), tmp, prefixo='corpo'
                    )
                    _log(f"  -> {len(arquivos_leg)} item(ns) no ZIP da legislação")
                    
                    # Expansão RECURSIVA de ZIPs aninhados (até 5 níveis de profundidade)
                    # Necessário porque: corpo pode ter Anexos.zip, que pode ter outros ZIPs dentro
                    todos_arquivos = []
                    pendentes = list(arquivos_leg)
                    profundidade_max = 5
                    nivel = 0
                    
                    while pendentes:
                        nivel += 1
                        if nivel > profundidade_max:
                            _log(f"  AVISO: profundidade max ({profundidade_max}) atingida")
                            todos_arquivos.extend(pendentes)
                            break
                        
                        proximos = []
                        for arq in pendentes:
                            # Tenta identificar se é ZIP (por magic bytes, não só extensão)
                            tipo_arq = identificar_tipo(arq['path_extraido'])
                            eh_zip_real = (
                                arq['eh_zip']
                                or arq['nome_original'].endswith('.zip')
                                or arq['nome_original'].endswith('/Anexos.zip')
                                or tipo_arq in ('zip_desconhecido',)
                            )
                            
                            if eh_zip_real:
                                _log(f"  [nivel {nivel}] Expandindo: {os.path.basename(arq['nome_original'])}")
                                try:
                                    sub_arquivos = _extrair_zip_para_dir(
                                        arq['path_extraido'], tmp,
                                        prefixo=f'n{nivel}_{os.path.basename(arq["nome_original"])[:30]}'
                                    )
                                    _log(f"    -> {len(sub_arquivos)} item(ns)")
                                    proximos.extend(sub_arquivos)
                                    # Remove o ZIP intermediário (já extraído)
                                    try:
                                        os.remove(arq['path_extraido'])
                                    except Exception:
                                        pass
                                except Exception as e:
                                    _log(f"    ERRO expandindo: {e}")
                                    # Se nao conseguiu expandir, mantem como arquivo normal
                                    todos_arquivos.append(arq)
                            else:
                                todos_arquivos.append(arq)
                        
                        pendentes = proximos
                else:
                    # FORMATO v1 (antigo, fallback): legislacao em pasta dentro do ZIP principal
                    _log("  Formato v1 detectado — usando pasta_zip do meta")
                    pasta_zip = (leg.get('pasta_zip') or '').strip('/')
                    if not pasta_zip:
                        _log("  AVISO: pasta_zip vazio, pulando legislação")
                        continue
                    
                    todos_arquivos = []
                    for nome in nomes_zip_principal:
                        if not nome.startswith(pasta_zip):
                            continue
                        if nome.endswith('/') or '__MACOSX' in nome or nome.endswith('.DS_Store'):
                            continue
                        # Extrai
                        nome_base = os.path.basename(nome) or nome.replace('/', '_')
                        target = os.path.join(tmp, _slug(nome_base))
                        # Trata colisão
                        base, ext = os.path.splitext(target)
                        contador = 1
                        while os.path.exists(target):
                            target = f"{base}_{contador}{ext}"
                            contador += 1
                        with zip_principal.open(nome) as src, open(target, 'wb') as dst:
                            shutil.copyfileobj(src, dst)
                        todos_arquivos.append({
                            'nome_original': nome,
                            'path_extraido': target,
                            'eh_zip': nome.lower().endswith('.zip'),
                        })
                    _log(f"  -> {len(todos_arquivos)} arquivo(s) extraído(s)")
                
                # ─── Dedup MD5 ───
                vistos_md5 = {}
                duplicados = []
                for arq in todos_arquivos:
                    md5 = _md5_arquivo(arq['path_extraido'])
                    if not md5:
                        continue
                    if md5 in vistos_md5:
                        duplicados.append(arq)
                        try:
                            os.remove(arq['path_extraido'])
                        except Exception:
                            pass
                    else:
                        vistos_md5[md5] = arq
                
                todos_arquivos = list(vistos_md5.values())
                _log(f"  Dedup MD5: {len(todos_arquivos)} únicos | {len(duplicados)} duplicados removidos")
                
                # ─── Dedup conteúdo: hash das páginas pra detectar PDFs que se sobrepõem ───
                # Caso típico LeisMunicipais: PDF principal já tem todos anexos
                # embutidos + os mesmos anexos como arquivos separados.
                # Algoritmo: extrai texto de cada página, normaliza, gera hash MD5.
                # Se TODAS as páginas de um PDF aparecem dentro de outro maior,
                # o menor é redundante e pode ser descartado.
                try:
                    import pypdf as _pp_dd
                    import re as _re_dd
                    import hashlib as _h_dd
                    
                    def _hash_pag(texto):
                        if not texto:
                            return None
                        t = _re_dd.sub(r'[^a-z0-9]+', '', (texto or '').lower())
                        if len(t) < 100:
                            return None
                        return _h_dd.md5(t[:500].encode()).hexdigest()
                    
                    def _hashes_pdf(pdf_path):
                        try:
                            r = _pp_dd.PdfReader(pdf_path)
                            hs = set()
                            for pg in r.pages:
                                try:
                                    h = _hash_pag(pg.extract_text() or '')
                                    if h:
                                        hs.add(h)
                                except Exception:
                                    pass
                            return hs, len(r.pages)
                        except Exception:
                            return set(), 0
                    
                    pdfs = [a for a in todos_arquivos
                            if a['path_extraido'].lower().endswith('.pdf')]
                    pdf_info = {}
                    for a in pdfs:
                        hs, n = _hashes_pdf(a['path_extraido'])
                        pdf_info[a['path_extraido']] = {'hashes': hs, 'n_pgs': n, 'arq': a}
                    
                    descartados_conteudo = []
                    for a in pdfs:
                        info_a = pdf_info[a['path_extraido']]
                        if not info_a['hashes']:
                            continue
                        # Procura outro PDF que CONTENHA todas as paginas deste
                        for b in pdfs:
                            if b is a:
                                continue
                            info_b = pdf_info[b['path_extraido']]
                            if not info_b['hashes']:
                                continue
                            if info_b['n_pgs'] <= info_a['n_pgs']:
                                continue  # nao pode conter se for menor
                            # Se >=80% das paginas de A estao em B, A eh redundante
                            paginas_em_b = info_a['hashes'] & info_b['hashes']
                            if len(paginas_em_b) / len(info_a['hashes']) >= 0.80:
                                descartados_conteudo.append(a)
                                _log(f"  Dedup conteudo: '{os.path.basename(a['nome_original'])}' ({info_a['n_pgs']}pgs) ja esta em '{os.path.basename(b['nome_original'])}' ({info_b['n_pgs']}pgs) - descartando")
                                break
                    
                    if descartados_conteudo:
                        _log(f"  Dedup conteudo: {len(descartados_conteudo)} PDF(s) redundante(s) descartado(s)")
                        for d in descartados_conteudo:
                            try:
                                os.remove(d['path_extraido'])
                            except Exception:
                                pass
                            duplicados.append(d)
                        todos_arquivos = [a for a in todos_arquivos if a not in descartados_conteudo]
                except Exception as _e_dd:
                    _log(f"  Aviso dedup conteudo: {str(_e_dd)[:120]}")
                
                # ─── Identifica tipo + converte ───
                tmp_pdfs = os.path.join(tmp, '_pdfs_finais')
                os.makedirs(tmp_pdfs, exist_ok=True)
                
                for arq in todos_arquivos:
                    path = arq['path_extraido']
                    nome = os.path.basename(arq['nome_original'])
                    tipo = identificar_tipo(path)
                    tamanho = os.path.getsize(path) if os.path.exists(path) else 0
                    
                    info = {
                        'nome': nome,
                        'tipo_detectado': tipo,
                        'tamanho': tamanho,
                    }
                    
                    if tipo in ('vazio', 'inexistente', 'erro_leitura'):
                        info['conversao_ok'] = False
                        info['motivo'] = f'arquivo {tipo}'
                        arquivos_originais.append(info)
                        falhas.append({'nome': nome, 'tipo': tipo, 'motivo': info['motivo']})
                        continue
                    
                    pdf_gerado = converter_para_pdf(path, tmp_pdfs)
                    if pdf_gerado:
                        info['conversao_ok'] = True
                        info['foi_convertido'] = (tipo != 'pdf')
                        pdfs_pra_concatenar.append(pdf_gerado)
                    else:
                        info['conversao_ok'] = False
                        info['motivo'] = f'falha conversao ({tipo})'
                        falhas.append({'nome': nome, 'tipo': tipo, 'motivo': info['motivo']})
                    
                    arquivos_originais.append(info)
                
                _log(f"  Conversão: {len(pdfs_pra_concatenar)} OK | {len(falhas)} falha(s)")
                
                # ─── Concatena ───
                pdf_final = os.path.join(leg_dir, 'pdf_concatenado.pdf')
                n_paginas = 0
                
                if pdfs_pra_concatenar:
                    ok = concatenar_pdfs(pdfs_pra_concatenar, pdf_final)
                    if ok and os.path.exists(pdf_final):
                        try:
                            import pypdf
                            n_paginas = len(pypdf.PdfReader(pdf_final).pages)
                        except Exception as e:
                            logger.error(f"Erro contando paginas: {e}")
                        _log(f"  ✅ PDF concatenado: {n_paginas} páginas")
                    else:
                        _log(f"  ❌ Erro ao concatenar")
                        pdf_final = None
                else:
                    pdf_final = None
                    _log(f"  ⚠️ Nenhum PDF pra concatenar")
                
                # ─── Salva metadados da legislação ───
                meta_path = os.path.join(leg_dir, 'arquivos_originais.json')
                with open(meta_path, 'w', encoding='utf-8') as f:
                    json.dump({
                        'legislacao': leg,
                        'label': label,
                        'arquivos_originais': arquivos_originais,
                        'falhas': falhas,
                        'duplicados_removidos': len(duplicados),
                        'n_paginas_concatenado': n_paginas,
                    }, f, ensure_ascii=False, indent=2)
                
                total_arquivos += len(arquivos_originais)
                
                # Copiar ZIP concat_catalogo de downloads para pasta do dossie
                try:
                    import glob as _gl, shutil as _sh_org
                    _zips_dl = _gl.glob(f"/var/www/urbanlex/static/downloads/*{label}*_concat_catalogo.zip")
                    if _zips_dl:
                        _zip_src = sorted(_zips_dl)[-1]  # mais recente
                        _zip_dst = os.path.join(leg_dir, os.path.basename(_zip_src))
                        if not os.path.exists(_zip_dst):
                            _sh_org.copy2(_zip_src, _zip_dst)
                            _log(f"  ✓ ZIP concat_catalogo copiado para dossiê")
                except Exception as _ez:
                    pass
                resultado_legislacoes.append({
                    'label': label,
                    'categoria': leg.get('categoria', ''),
                    'metadados': leg,
                    'pdf_concatenado': pdf_final,
                    'n_paginas': n_paginas,
                    'total_arquivos': len(arquivos_originais),
                    'total_pdfs_ok': len(pdfs_pra_concatenar),
                    'arquivos_originais': arquivos_originais,
                    'falhas': falhas,
                    'duplicados_removidos': len(duplicados),
                })
    
    _log("")
    _log(f"✅ CONCLUÍDO: {len(resultado_legislacoes)} legislação(ões) | {total_arquivos} arquivo(s) processado(s)")
    
    return {
        'sucesso': True,
        'dossie_dir': dossie_dir,
        'formato_versao': formato_versao,
        'legislacoes': resultado_legislacoes,
        'total_arquivos': total_arquivos,
    }
