"""
modulos/triagem_flash.py
=========================
1a passada do pipeline 'triagem_ocr_pro_sonnet' (Plano Y v2).

Recebe lista de PDFs (ja normalizados pela pre-passagem) e usa Gemini Flash
para classificar cada PDF/pagina por tipo de conteudo e mapear zonas urbanas
para suas paginas de definicao.

Saida estrutural permite que a 2a passada (Pro) receba so paginas relevantes
em vez de processar todos os ~134 PDFs.

Categorias de pagina:
  TABELA_ZONA     - Tabela de parametros urbanisticos por zona
  TABELA_USO      - Quadro de usos permitidos/condicionados/proibidos
  TEXTO_LEI       - Artigos, paragrafos, incisos de lei
  MAPA            - Mapa de zoneamento
  DIAGRAMA_VIARIO - Gabaritos viarios, perfis de via
  CAPA_INDICE     - Capas, sumarios, paginas decorativas
  ILUSTRACAO      - Outros desenhos tecnicos
  OUTRO           - Nao se encaixa nas categorias acima

Uso:
    from modulos.triagem_flash import triagem_anexos
    
    resultado = triagem_anexos(
        pdfs_list=todos_anexos,
        client=cliente_gemini_flash,
        ia_id='gemini-flash',
        logs=job['logs']
    )
    
    # resultado:
    # {
    #     'paginas': [...],
    #     'zonas_identificadas': [...],
    #     'indice_zona_paginas': {...},
    #     'distribuicao_tipos': {...},
    #     'metadados': {...}
    # }
"""
import os
import json
import time
import logging
import base64
import tempfile
from pathlib import Path

logger = logging.getLogger(__name__)


# Categorias canonicas que a Flash deve usar
CATEGORIAS_VALIDAS = [
    'TABELA_ZONA',
    'TABELA_USO',
    'TEXTO_LEI',
    'MAPA',
    'DIAGRAMA_VIARIO',
    'CAPA_INDICE',
    'ILUSTRACAO',
    'OUTRO',
]

# Categorias relevantes para extracao de parametros (usadas pela 2a passada)
CATEGORIAS_RELEVANTES = ['TABELA_ZONA', 'TABELA_USO', 'TEXTO_LEI']

# Limite de tamanho por batch (Flash aceita 20MB mas reduzimos para garantir robustez)
MAX_BATCH_MB = 12

# Padrao regex para validar siglas de zona urbana
# Aceita: ZR1, ZC2, ZI3, ZD, ZCS, ZCA, ZEIS, ZEPC, ZIE, ZII, ZEAT, ZEIHC, ZEIAN
# Rejeita: MZ1 (macrozona), UGPA X (unidade de gestao), AP X (area de planejamento)
import re as _re_zona
PADRAO_ZONA_VALIDA = _re_zona.compile(r'^(Z[RCI]\d+[A-Z]?|ZD|ZCS|ZCA|ZE[A-Z]+|ZI[IE])$', _re_zona.IGNORECASE)

def _validar_zona(sigla: str) -> bool:
    """Verifica se sigla e uma zona urbana valida (nao macrozona/UGPA)."""
    if not sigla:
        return False
    s = sigla.strip().upper().replace(' ', '')
    return bool(PADRAO_ZONA_VALIDA.match(s))



PROMPT_TRIAGEM_FLASH = """Voce e um analista de legislacao urbanistica brasileira.

TAREFA: Para cada PDF abaixo, classifique:
1. TIPO do conteudo (use UMA das categorias listadas)
2. ZONAS URBANAS mencionadas (siglas como ZR1, ZC2, ZI3, ZCS, ZCA, ZD, etc)

CATEGORIAS (use exatamente uma destas):
- TABELA_ZONA: tabela com parametros (TO, TP, recuos, altura) para zonas
- TABELA_USO: quadro de usos permitidos/condicionados/proibidos por zona
- TEXTO_LEI: artigos, paragrafos, texto normativo
- MAPA: mapa de zoneamento ou perimetro urbano
- DIAGRAMA_VIARIO: gabaritos viarios, perfis de via, secao de rua
- CAPA_INDICE: capa, sumario, indice, pagina decorativa
- ILUSTRACAO: outros desenhos tecnicos sem dados de parametros
- OUTRO: nao se encaixa nas categorias acima

REGRAS:
- Se PDF tem multiplas paginas, classifique CADA pagina separadamente
- Zonas: cite TODAS as siglas explicitas na pagina (ex: 'ZR1', 'ZC2'), nao nomes longos
- Se nao tem zona explicita, retorne array vazio
- Se nao tem certeza, use OUTRO com confianca baixa (50-70)
- Confianca 0-100 (90+ = certeza, 70-89 = alta, 50-69 = media, <50 = baixa)

FORMATO DE RESPOSTA (JSON estrito, sem markdown):
{
  "paginas": [
    {
      "pdf": "<USE O NOME EXATO DO ARQUIVO PDF QUE VOCE RECEBEU>",
      "pagina": 1,
      "tipo": "TABELA_ZONA",
      "confianca": 95,
      "zonas": ["ZR1", "ZR2"]
    }
  ]
}

CRITICO: O campo "pdf" DEVE ser o NOME EXATO do arquivo PDF que voce esta
analisando. NUNCA escreva placeholders como 'nome_do_arquivo.pdf' ou
'arquivo.pdf'. Use SEMPRE o filename real do PDF (ex: 'LC 148.pdf',
'Anexo 02.4.pdf', 'PDDUA-186.pdf'). Se um PDF tem multiplas paginas,
use o MESMO nome de arquivo em todas as entradas dele.

IMPORTANTE: Retorne SOMENTE o JSON, sem comentarios ou markdown."""


def _construir_indice_zona_paginas(paginas: list) -> dict:
    """
    Inverte o mapeamento: de paginas->zonas para zonas->paginas.
    
    Retorna:
        {
            'ZR1': {
                'tabela_zona': [(pdf, n_pag, confianca), ...],
                'tabela_uso': [...],
                'texto_lei': [...],
                'todas': [...]
            },
            ...
        }
    """
    indice = {}
    for p in paginas:
        zonas = p.get('zonas') or []
        if not zonas:
            continue
        tipo = (p.get('tipo') or 'OUTRO').upper()
        for zona in zonas:
            zona_normalizada = zona.strip().upper().replace(' ', '')
            # Filtra zonas invalidas (macrozonas, UGPAs, etc - nao tem parametros urbanisticos diretos)
            if not _validar_zona(zona_normalizada):
                continue
            if zona_normalizada not in indice:
                indice[zona_normalizada] = {
                    'tabela_zona': [],
                    'tabela_uso': [],
                    'texto_lei': [],
                    'outros': [],
                    'todas': [],
                }
            ref = {
                'pdf': p.get('pdf'),
                'pagina': p.get('pagina'),
                'confianca': p.get('confianca', 0),
            }
            if tipo == 'TABELA_ZONA':
                indice[zona_normalizada]['tabela_zona'].append(ref)
            elif tipo == 'TABELA_USO':
                indice[zona_normalizada]['tabela_uso'].append(ref)
            elif tipo == 'TEXTO_LEI':
                indice[zona_normalizada]['texto_lei'].append(ref)
            else:
                indice[zona_normalizada]['outros'].append(ref)
            indice[zona_normalizada]['todas'].append(ref)
    return indice


def _calcular_distribuicao(paginas: list) -> dict:
    """Calcula histograma de tipos de pagina."""
    dist = {cat: 0 for cat in CATEGORIAS_VALIDAS}
    for p in paginas:
        tipo = (p.get('tipo') or 'OUTRO').upper()
        if tipo in dist:
            dist[tipo] += 1
        else:
            dist['OUTRO'] += 1
    return dist


def _filtrar_paginas_relevantes(paginas: list) -> list:
    """Retorna so paginas das categorias relevantes (para a 2a passada)."""
    return [p for p in paginas if (p.get('tipo') or '').upper() in CATEGORIAS_RELEVANTES]


def _dividir_em_batches(pdfs_list: list, max_mb: float = None) -> list:
    """
    Divide PDFs em batches respeitando MAX_BATCH_MB de tamanho total.
    """
    if max_mb is None:
        max_mb = MAX_BATCH_MB
    max_bytes = max_mb * 1024 * 1024
    
    batches = []
    batch_atual = []
    bytes_atual = 0
    
    for p in pdfs_list:
        try:
            tamanho = len(base64.b64decode(p['data_b64']))
        except Exception:
            tamanho = 0
        
        if batch_atual and (bytes_atual + tamanho) > max_bytes:
            batches.append(batch_atual)
            batch_atual = [p]
            bytes_atual = tamanho
        else:
            batch_atual.append(p)
            bytes_atual += tamanho
    
    if batch_atual:
        batches.append(batch_atual)
    
    return batches


def triagem_anexos(pdfs_list: list, client, ia_id: str, logs: list, persistir_em: str = None) -> dict:
    """
    Funcao principal: classifica PDFs via Gemini Flash.
    Aplica batching automatico para nao estourar limite de contexto.
    
    Args:
        pdfs_list: lista de dicts com {'data_b64', 'nome_arquivo', ...}
        client: cliente Gemini Flash (montado por montar_client)
        ia_id: 'gemini-flash' ou outro modelo de triagem
        logs: lista de logs do job (para append)
        persistir_em: path opcional para salvar JSON de debug
    
    Returns:
        dict com paginas classificadas, zonas, indice, distribuicao
    """
    from modulos.multi_ia import chamar_ia
    
    t0 = time.time()
    
    # Loga inicio
    logs.append({'nivel': 'info', 'msg': f'======= TRIAGEM FLASH (1a passada) ======='})
    
    # Calcula tamanho total e decide se precisa fragmentar
    tamanho_total_mb = sum(len(base64.b64decode(p['data_b64'])) for p in pdfs_list) / (1024 * 1024)
    batches = _dividir_em_batches(pdfs_list)
    
    logs.append({'nivel': 'info', 'msg': f'Classificando {len(pdfs_list)} PDFs ({tamanho_total_mb:.1f}MB) em {len(batches)} batch(es)...'})
    
    # Itera batches, agregando resultados
    paginas = []
    falhas_batch = 0
    
    try:
        for bi, batch in enumerate(batches, 1):
            t_batch = time.time()
            batch_mb = sum(len(base64.b64decode(p['data_b64'])) for p in batch) / (1024 * 1024)
            
            try:
                resp = chamar_ia(
                    client=client,
                    ia_id=ia_id,
                    prompt_text=PROMPT_TRIAGEM_FLASH,
                    pdfs=batch,
                    logs=logs,
                    label=f'TRIAGEM.{bi}/{len(batches)}',
                )
                
                # Parsing
                if isinstance(resp, dict):
                    texto_resp = resp.get('texto') or resp.get('content') or ''
                else:
                    texto_resp = str(resp)
                
                # Limpa markdown
                texto_limpo = texto_resp.strip()
                if texto_limpo.startswith('```'):
                    linhas = texto_limpo.split('\n')
                    linhas = [l for l in linhas if not l.startswith('```')]
                    texto_limpo = '\n'.join(linhas)
                
                # Parse JSON
                try:
                    dados = json.loads(texto_limpo)
                except json.JSONDecodeError:
                    logs.append({'nivel': 'aviso', 'msg': f'  Batch {bi}: JSON invalido, brace-counting fallback'})
                    dados = _parse_json_brace_counting(texto_limpo)
                
                paginas_batch = dados.get('paginas') or []
                
                # POS-PROCESSAMENTO: corrige nomes 'nome_do_arquivo.pdf' (placeholder do exemplo)
                # mapeando para os nomes reais dos PDFs do batch
                nomes_reais_batch = [p.get('nome_arquivo', '') for p in batch]
                paginas_batch = _corrigir_nomes_pdf(paginas_batch, nomes_reais_batch)
                
                paginas.extend(paginas_batch)
                
                dt_batch = int(time.time() - t_batch)
                logs.append({
                    'nivel': 'info',
                    'msg': f'  Batch {bi}/{len(batches)}: {len(batch)} PDFs ({batch_mb:.1f}MB) -> {len(paginas_batch)} paginas em {dt_batch}s'
                })
                
            except Exception as e_batch:
                falhas_batch += 1
                logs.append({
                    'nivel': 'aviso',
                    'msg': f'  Batch {bi}/{len(batches)} FALHOU: {type(e_batch).__name__}: {str(e_batch)[:200]}'
                })
        
        if not paginas:
            raise Exception(f'Triagem retornou 0 paginas em todos os {len(batches)} batches')
        
        # Constroi estruturas derivadas
        indice = _construir_indice_zona_paginas(paginas)
        distribuicao = _calcular_distribuicao(paginas)
        zonas_id = sorted(indice.keys())
        paginas_relevantes = _filtrar_paginas_relevantes(paginas)
        
        # Loga sumario
        dt = int(time.time() - t0)
        logs.append({'nivel': 'ok', 'msg': f'Triagem concluida em {dt}s: {len(paginas)} paginas, {len(zonas_id)} zonas, {len(paginas_relevantes)} relevantes'})
        
        # Distribuicao
        dist_str = ', '.join([f'{k}:{v}' for k, v in distribuicao.items() if v > 0])
        logs.append({'nivel': 'info', 'msg': f'  Distribuicao: {dist_str}'})
        
        # Top 5 zonas com mais paginas
        zonas_top = sorted(indice.items(), key=lambda x: len(x[1]['todas']), reverse=True)[:5]
        for zona, dados_zona in zonas_top:
            n_tab = len(dados_zona['tabela_zona'])
            n_uso = len(dados_zona['tabela_uso'])
            n_txt = len(dados_zona['texto_lei'])
            logs.append({'nivel': 'info', 'msg': f'  {zona}: {n_tab} tab_param + {n_uso} tab_uso + {n_txt} texto = {len(dados_zona["todas"])} pag'})
        
        # Monta resultado
        resultado = {
            'paginas': paginas,
            'paginas_relevantes': paginas_relevantes,
            'zonas_identificadas': zonas_id,
            'indice_zona_paginas': indice,
            'distribuicao_tipos': distribuicao,
            'metadados': {
                'total_pdfs': len(pdfs_list),
                'total_paginas': len(paginas),
                'total_zonas': len(zonas_id),
                'total_relevantes': len(paginas_relevantes),
                'tempo_ms': int((time.time() - t0) * 1000),
                'ia_usada': ia_id,
            },
        }
        
        # Persiste se solicitado
        if persistir_em:
            try:
                Path(persistir_em).parent.mkdir(parents=True, exist_ok=True)
                with open(persistir_em, 'w', encoding='utf-8') as f:
                    json.dump(resultado, f, indent=2, ensure_ascii=False, default=str)
                logs.append({'nivel': 'info', 'msg': f'  Triagem persistida em {persistir_em}'})
            except Exception as e:
                logs.append({'nivel': 'aviso', 'msg': f'  Falha ao persistir triagem: {e}'})
        
        return resultado
        
    except Exception as e:
        dt = int(time.time() - t0)
        logs.append({'nivel': 'erro', 'msg': f'Triagem FALHOU em {dt}s: {type(e).__name__}: {str(e)[:200]}'})
        # Retorna estrutura vazia para nao quebrar pipeline
        return {
            'paginas': [],
            'paginas_relevantes': [],
            'zonas_identificadas': [],
            'indice_zona_paginas': {},
            'distribuicao_tipos': {},
            'metadados': {
                'erro': str(e)[:500],
                'tempo_ms': int((time.time() - t0) * 1000),
            },
        }


def _parse_json_brace_counting(texto: str) -> dict:
    """
    Parse JSON robusto via contagem de braces.
    Usa quando json.loads falha por causa de ruido extra.
    """
    inicio = texto.find('{')
    if inicio < 0:
        return {'paginas': []}
    
    depth = 0
    fim = inicio
    in_string = False
    escape = False
    
    for i in range(inicio, len(texto)):
        c = texto[i]
        if escape:
            escape = False
            continue
        if c == '\\':
            escape = True
            continue
        if c == '"' and not escape:
            in_string = not in_string
            continue
        if in_string:
            continue
        if c == '{':
            depth += 1
        elif c == '}':
            depth -= 1
            if depth == 0:
                fim = i + 1
                break
    
    try:
        return json.loads(texto[inicio:fim])
    except Exception:
        return {'paginas': []}




def _corrigir_nomes_pdf(paginas_lista: list, nomes_reais_batch: list) -> list:
    """
    Corrige campo 'pdf' de paginas classificadas pela IA.
    Se a IA retornou placeholder generico ou nome que nao bate com PDFs
    do batch, tenta mapear pelo nome mais proximo. Se o batch tem so 1
    PDF, todas as paginas vao para ele.
    """
    PLACEHOLDERS = {'nome_do_arquivo.pdf', 'arquivo.pdf', 'documento.pdf', 'pdf.pdf', ''}
    nomes_set = set(nomes_reais_batch)
    
    if len(nomes_reais_batch) == 1:
        for p in paginas_lista:
            p['pdf'] = nomes_reais_batch[0]
        return paginas_lista
    
    for p in paginas_lista:
        pdf_nome = (p.get('pdf') or '').strip()
        
        if pdf_nome in nomes_set:
            continue
        
        if pdf_nome.lower() in PLACEHOLDERS:
            p['pdf'] = '__AMBIGUO__'
            continue
        
        match_parcial = None
        for nome_real in nomes_reais_batch:
            base_retornado = pdf_nome.replace('.pdf', '').lower()
            base_real = nome_real.replace('.pdf', '').lower()
            if base_retornado and (base_retornado in base_real or base_real.startswith(base_retornado[:20])):
                match_parcial = nome_real
                break
        
        if match_parcial:
            p['pdf'] = match_parcial
        else:
            p['pdf'] = '__AMBIGUO__'
    
    return paginas_lista

def filtrar_anexos_por_zonas(anexos: list, zonas_dev: list, indice_triagem: dict) -> list:
    """
    Modo dev: filtra lista de anexos para conter apenas PDFs que tem
    paginas relevantes para as zonas especificadas.
    
    Args:
        anexos: lista completa de anexos (cada um com 'nome_arquivo')
        zonas_dev: lista de siglas de zonas (ex: ['ZR1', 'ZC1'])
        indice_triagem: indice_zona_paginas da triagem
    
    Returns:
        Subconjunto dos anexos
    """
    if not zonas_dev or not indice_triagem:
        return anexos
    
    pdfs_relevantes = set()
    for zona in zonas_dev:
        zn = zona.strip().upper()
        if zn in indice_triagem:
            for ref in indice_triagem[zn]['todas']:
                if ref.get('pdf'):
                    pdfs_relevantes.add(ref['pdf'])
    
    return [a for a in anexos if a.get('nome_arquivo') in pdfs_relevantes]
