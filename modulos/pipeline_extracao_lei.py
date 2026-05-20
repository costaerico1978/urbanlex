"""
═══════════════════════════════════════════════════════════════════════════════
PIPELINE DE EXTRAÇÃO DE LEI URBANÍSTICA — UrbanLex
═══════════════════════════════════════════════════════════════════════════════

Processa um ZIP de legislação municipal e extrai zonas, parâmetros urbanísticos,
usos permitidos, variações e modificações da errata num JSON estruturado pronto
pra alimentar a planilha-padrão (300 colunas) ou o banco PostgreSQL.

INPUT:  ZIP contendo PDFs da lei principal + anexos + erratas
OUTPUT: dict com chaves: sucesso, resultado | erro_etapa, erro_msg, resultado_parcial

═══════════════════════════════════════════════════════════════════════════════
ETAPAS DO PIPELINE
═══════════════════════════════════════════════════════════════════════════════

ETAPA 1 — EXTRAÇÃO E CONCATENAÇÃO                            [$0    | ~5s]
  Extrai PDFs do ZIP (deduplicando por MD5), descompacta ZIPs aninhados,
  e concatena tudo num PDF único usando pdfunite.

ETAPA 2 — DETECÇÃO DO FIM DO CORPO DA LEI                    [$0    | ~25s]
  Analisa cada página com pdftotext -layout e usa regex pra detectar
  "Art. N" no início de linha. Onde a sequência de artigos PARA de crescer
  é o fim do corpo da lei (começam os anexos).

ETAPA 3 — QUEBRA EM CORPO + ANEXOS                           [$0    | <1s]
  Usa pypdf pra separar o PDF concatenado em 2 partes:
  - corpo.pdf (artigos da lei)
  - anexos.pdf (quadros, mapas, erratas)

ETAPA 4 — CATALOGAÇÃO DOS ANEXOS                             [$0.20 | ~25s]
  IA: Claude Haiku 4.5 (visão + texto)
  Lê anexos.pdf inteiro e identifica cada bloco (Anexo 1.X, Anexo 2.X,
  errata, encerramento) com suas páginas iniciais e finais.

ETAPA 5 — EXTRAÇÃO DE DADOS POR BLOCO                        [$1-3  | ~10-17min]
  IA: Claude Sonnet 4.6 (contexto evolutivo + prompt v13)
  Faz N chamadas sequenciais (uma por bloco), cada uma recebendo:
  - PDF do bloco + texto -layout
  - Contexto acumulado do que já foi descoberto
  - Prompt v13 com regras de extração
  
  FASE 1A: lê CORPO procurando usos permitidos por zona
  FASE 1B: se não achou no corpo, busca anexo com nome tipo
           "Usos Permitidos" ou "Quadro de Atividades"
  FASE 2:  processa demais blocos (anexos 2.X, 1.X, errata) com
           contexto acumulado, preenchendo o que está null

ETAPA 6 — RECONSOLIDAÇÃO COM MERGE PROFUNDO                  [$0    | <1s]
  Python aplica merge profundo (campo a campo) sobre os JSONs gerados
  pelo Sonnet. Prioriza valores reais sobre null/NI. Filtra falsos
  positivos (AV, UGPAB, ZIE, etc).

ETAPA 7 — EXPANSÃO DE parametros_por_uso                     [$0    | <1s]
  Pra cada zona, se Sonnet detectou que params são uniformes
  (parametros_por_uso: null), Python expande replicando os params
  gerais pra cada uso permitido. Garante estrutura universal.

ETAPA 8 — APLICAÇÃO DA ERRATA NAS ZONAS                      [$0.10 | ~30s]
  IA: Claude Sonnet 4.6 (output enxuto, formato {zona, campo_path, valor_novo})
  Manda zonas afetadas + modificações relevantes pro Sonnet.
  Sonnet retorna lista de alterações. Python aplica via dot-notation.

═══════════════════════════════════════════════════════════════════════════════
CUSTO TOTAL ESTIMADO: ~$3.30 (primeira execução) ou ~$1.30 (com cache)
TEMPO TOTAL ESTIMADO: ~18min (primeira) ou ~10min (com cache)
═══════════════════════════════════════════════════════════════════════════════

USO:
    from modulos.pipeline_extracao_lei import processar_municipio
    
    resultado = processar_municipio(
        zip_path='/path/to/legislacoes_Xangri_la_RS.zip',
        municipio='Xangri-Lá',
        estado='RS',
        usar_cache=True,
        log_callback=lambda msg: print(msg),
    )
    # Sucesso: {'sucesso': True, 'resultado': {legislacao, zonas, ...}, 'metricas': {...}}
    # Erro:    {'sucesso': False, 'erro_etapa': N, 'erro_msg': '...', 'resultado_parcial': {...}}

═══════════════════════════════════════════════════════════════════════════════
"""

import os
import sys
import re
import json
import time
import base64
import zipfile
import hashlib
import logging
import subprocess
import shutil
import tempfile
import copy
import traceback
from datetime import datetime

# Dependências externas
import pypdf
import anthropic

# Logger
logger = logging.getLogger(__name__)

# Carrega .env se ainda não foi carregado (não sobrescreve variáveis existentes)
try:
    from dotenv import load_dotenv
    load_dotenv('/var/www/urbanlex/.env', override=False)
except ImportError:
    pass

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURAÇÕES
# ═══════════════════════════════════════════════════════════════════════════════

# Caminhos
PROMPT_V13_PATH = "/var/www/urbanlex/prompts/prompt_v14.md"  # default: v14
PIPELINES_BASE_DIR = "/var/www/urbanlex/static/pipelines"

# Modelos
MODELO_HAIKU = "claude-haiku-4-5-20251001"
MODELO_SONNET = "claude-sonnet-4-6"

# Zonas válidas do Art. 277 (Xangri-Lá-compatível, expandir conforme necessário)
ZONAS_VALIDAS_DEFAULT = {
    'ZR1', 'ZR2', 'ZR3', 'ZR4', 'ZR5', 'ZR6', 'ZR7',
    'ZC1', 'ZC2', 'ZCA', 'ZCS',
    'ZD', 'ZI1', 'ZI2',
    'ZEII', 'ZEIS', 'ZEIHC', 'ZEIAN', 'ZEAT', 'ZEPC',
}

# Filtros de falsos positivos
ZONAS_BLACKLIST = {'AV', 'RUA', 'UGPAB', 'UEU', 'PEIP', 'PEIU', 'ZIE', 'ZII'}

# Palavras-chave pra identificar anexo de usos permitidos
PALAVRAS_ANEXO_USOS = [
    "usos permitidos", "usos aceitos",
    "quadro de usos", "quadro de usos permitidos", "quadro de usos aceitos",
    "tabela de usos", "tabela de usos permitidos", "tabela de usos aceitos",
    "relacao de usos", "relacao de usos permitidos", "relacao de usos aceitos",
    "lista de usos", "lista de usos permitidos", "lista de usos aceitos",
    "atividades permitidas", "atividades aceitas",
    "quadro de atividades", "quadro de atividades permitidas",
    "quadro de atividades aceitas",
    "tabela de atividades", "tabela de atividades permitidas",
    "tabela de atividades aceitas",
    "relacao de atividades", "relacao de atividades permitidas",
    "relacao de atividades aceitas",
    "lista de atividades", "lista de atividades permitidas",
    "lista de atividades aceitas",
]

# Regex para detectar artigos no INICIO de linha
PAT_ART_INICIO = re.compile(
    r'(?:^|\n)\s*Art(?:igo)?\.?\s*(\d+)\s*[ºo°]?',
    re.IGNORECASE | re.MULTILINE
)

# Preços por 1M tokens (USD)
PRECO_HAIKU_IN = 1.00
PRECO_HAIKU_OUT = 5.00
PRECO_SONNET_IN = 3.00
PRECO_SONNET_OUT = 15.00


# ═══════════════════════════════════════════════════════════════════════════════
# FUNÇÕES AUXILIARES
# ═══════════════════════════════════════════════════════════════════════════════

def _log(msg, log_callback=None):
    """Log unificado: callback (se fornecido) + logger padrão."""
    if log_callback:
        try: log_callback(msg)
        except Exception: pass
    logger.info(msg)


def _slug_municipio(municipio, estado):
    """Cria slug seguro para nome de pasta: 'Xangri-Lá', 'RS' -> 'Xangri-La_RS'."""
    import unicodedata
    s = unicodedata.normalize('NFKD', f"{municipio}_{estado}")
    s = ''.join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r'[^A-Za-z0-9_\-]+', '_', s).strip('_')
    return s


def parse_json_robusto(texto):
    """
    Parser de JSON tolerante a:
    - Texto antes/depois do JSON
    - Strings com chaves dentro
    - Caracteres especiais (escape, aspas)
    
    Usa contador de profundidade pra achar o JSON top-level.
    Retorna dict ou None.
    """
    if not texto:
        return None
    
    inicio = texto.find('{')
    if inicio == -1:
        return None
    
    depth = 0
    in_string = False
    escape = False
    fim = -1
    
    for i in range(inicio, len(texto)):
        c = texto[i]
        if escape:
            escape = False
            continue
        if in_string:
            if c == '\\':
                escape = True
                continue
            if c == '"':
                in_string = False
            continue
        if c == '"':
            in_string = True
            continue
        if c == '{':
            depth += 1
        elif c == '}':
            depth -= 1
            if depth == 0:
                fim = i + 1
                break
    
    if fim == -1:
        return None
    
    try:
        return json.loads(texto[inicio:fim], strict=False)
    except Exception as e:
        logger.warning(f"parse_json_robusto falhou: {e}")
        return None


def eh_vazio_ou_NI(v):
    """
    Verifica se um valor é 'vazio' para fins de merge profundo.
    
    Considera vazio:
    - None
    - String "NI", "NULL", "" (case-insensitive)
    - Dict com valor=null ou valor="NI"
    """
    if v is None:
        return True
    if isinstance(v, str) and v.strip().upper() in ('NI', 'NULL', ''):
        return True
    if isinstance(v, dict):
        valor = v.get('valor')
        if valor is None:
            return True
        if isinstance(valor, str) and valor.strip().upper() in ('NI', 'NULL', ''):
            return True
    return False


def merge_profundo(atual, novo):
    """
    Mescla 2 estruturas (dicts/listas) campo a campo.
    Prioriza valores REAIS sobre None/NI/vazio.
    
    Exemplos:
    - merge_profundo({a: None}, {a: 5}) -> {a: 5}
    - merge_profundo({a: "NI"}, {a: 9}) -> {a: 9}
    - merge_profundo({a: 7}, {a: 9}) -> {a: 7} (preserva o atual)
    - merge_profundo({a: {x: None}}, {a: {x: 5}}) -> {a: {x: 5}} (recursivo)
    """
    if novo is None:
        return atual
    if atual is None:
        return novo
    if not isinstance(atual, dict) or not isinstance(novo, dict):
        return atual
    
    for chave, valor_novo in novo.items():
        valor_atual = atual.get(chave)
        if eh_vazio_ou_NI(valor_atual) and not eh_vazio_ou_NI(valor_novo):
            atual[chave] = valor_novo
        elif isinstance(valor_atual, dict) and isinstance(valor_novo, dict):
            merge_profundo(valor_atual, valor_novo)
    
    return atual


def set_dot_path(obj, path, valor):
    """
    Aplica valor em obj.path.nested usando dot-notation.
    Cria dicts intermediários se não existirem.
    
    Exemplo:
    set_dot_path(zona, "parametros_gerais.altura.valor", "25")
    -> zona["parametros_gerais"]["altura"]["valor"] = "25"
    
    Retorna True se aplicou, False se path inválido.
    """
    partes = path.split('.')
    atual = obj
    for p in partes[:-1]:
        if isinstance(atual, dict):
            if p not in atual or not isinstance(atual[p], dict):
                atual[p] = {}
            atual = atual[p]
        else:
            return False
    if isinstance(atual, dict):
        atual[partes[-1]] = valor
        return True
    return False


def eh_zona_real(sigla, zonas_validas=None):
    """
    Filtro de falsos positivos.
    Aceita siglas em zonas_validas, rejeita as do blacklist.
    """
    if not sigla:
        return False
    s = sigla.strip().upper()
    if s in ZONAS_BLACKLIST:
        return False
    if zonas_validas and s in zonas_validas:
        return True
    # Heurística: começa com Z e tem só letras/números
    if s.startswith('Z') and re.match(r'^Z[A-Z0-9]+$', s):
        return True
    return False


def eh_anexo_de_usos(titulo):
    """
    Verifica se o título de um anexo indica que é tabela de usos permitidos.
    Normaliza acentos antes de comparar.
    """
    if not titulo:
        return False
    import unicodedata
    t = unicodedata.normalize('NFKD', titulo.lower())
    t = ''.join(c for c in t if not unicodedata.combining(c))
    for palavra in PALAVRAS_ANEXO_USOS:
        if palavra in t:
            return True
    return False


def calcular_custo(tokens_in, tokens_out, modelo='sonnet'):
    """Calcula custo em USD a partir de tokens."""
    if modelo == 'haiku':
        return (tokens_in * PRECO_HAIKU_IN + tokens_out * PRECO_HAIKU_OUT) / 1_000_000
    return (tokens_in * PRECO_SONNET_IN + tokens_out * PRECO_SONNET_OUT) / 1_000_000


def carregar_prompt_v13():
    """Carrega o prompt v13 do disco. Lança FileNotFoundError se não existir."""
    with open(PROMPT_V13_PATH, 'r', encoding='utf-8') as f:
        return f.read()


# ═══════════════════════════════════════════════════════════════════════════════
# ETAPA 1 — EXTRAÇÃO E CONCATENAÇÃO
# ═══════════════════════════════════════════════════════════════════════════════

def etapa1_extrair_e_concatenar(zip_path, work_dir, log_callback=None, usar_cache=True):
    """
    Extrai PDFs do ZIP (deduplicando por MD5), descompacta ZIPs aninhados,
    e concatena tudo num PDF único usando pdfunite.
    
    Retorna: caminho do PDF concatenado (work_dir/tudo.pdf)
    
    CACHE: se usar_cache=True e tudo.pdf ja existe no work_dir, reusa direto.
    """
    _log("ETAPA 1/8 — Extraindo e concatenando PDFs", log_callback)
    t0 = time.time()
    
    # Cache: se tudo.pdf ja existe, pula extracao
    pdf_unico_cache = os.path.join(work_dir, "tudo.pdf")
    if usar_cache and os.path.exists(pdf_unico_cache) and os.path.getsize(pdf_unico_cache) > 1000:
        try:
            r_cache = subprocess.run(['pdfinfo', pdf_unico_cache], capture_output=True, text=True, timeout=30)
            n_pg_cache = 0
            for linha in r_cache.stdout.split('\n'):
                if linha.startswith('Pages:'):
                    n_pg_cache = int(linha.split(':')[1].strip())
                    break
            if n_pg_cache > 0:
                _log(f"  ETAPA 1 CACHE HIT: usando {pdf_unico_cache} ({n_pg_cache} pgs)", log_callback)
                return {
                    'pdf_unico': pdf_unico_cache,
                    'n_paginas': n_pg_cache,
                    'pdfs_extraidos': 0,
                    'tempo': time.time() - t0,
                    'cached': True,
                }
        except Exception:
            pass  # Cache falhou, segue execucao normal
    
    hashes = set()
    pdfs = []
    
    def _extrair_recursivo(zp, dest):
        with zipfile.ZipFile(zp) as zf:
            for info in zf.infolist():
                nome = os.path.basename(info.filename) or info.filename
                try:
                    c = zf.read(info)
                except Exception:
                    continue
                h = hashlib.md5(c).hexdigest()
                if c[:4] == b'%PDF' and h not in hashes:
                    hashes.add(h)
                    caminho = os.path.join(dest, f"pdf_{len(pdfs):03d}.pdf")
                    with open(caminho, 'wb') as f:
                        f.write(c)
                    pdfs.append({'nome': nome, 'path': caminho})
                elif c[:4] == b'PK\x03\x04' and h not in hashes:
                    hashes.add(h)
                    tz = os.path.join(dest, f"nest_{len(pdfs):03d}.zip")
                    with open(tz, 'wb') as f:
                        f.write(c)
                    _extrair_recursivo(tz, dest)
    
    _extrair_recursivo(zip_path, work_dir)
    
    if not pdfs:
        raise ValueError(f"Nenhum PDF encontrado em {zip_path}")
    
    _log(f"  PDFs extraídos (dedup MD5): {len(pdfs)}", log_callback)
    
    # Concatena com pdfunite
    pdf_unico = os.path.join(work_dir, "tudo.pdf")
    try:
        subprocess.run(
            ['pdfunite'] + [p['path'] for p in pdfs] + [pdf_unico],
            check=True, capture_output=True, timeout=120
        )
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"pdfunite falhou: {e.stderr.decode('utf-8', errors='replace')}")
    
    # Pega total de páginas
    r = subprocess.run(['pdfinfo', pdf_unico], capture_output=True, text=True, timeout=30)
    n_pg = 0
    for linha in r.stdout.split('\n'):
        if linha.startswith('Pages:'):
            n_pg = int(linha.split(':')[1].strip())
            break
    
    _log(f"  PDF concatenado: {pdf_unico} ({n_pg} páginas)", log_callback)
    _log(f"  Etapa 1 concluída em {time.time()-t0:.1f}s", log_callback)
    
    return {
        'pdf_unico': pdf_unico,
        'n_paginas': n_pg,
        'pdfs_extraidos': len(pdfs),
        'tempo': time.time() - t0,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# ETAPA 2 — DETECÇÃO DO FIM DO CORPO DA LEI
# ═══════════════════════════════════════════════════════════════════════════════

def etapa2_detectar_fim_corpo(pdf_unico, n_paginas, log_callback=None, work_dir=None, usar_cache=True):
    """
    Analisa cada página com pdftotext -layout e detecta onde termina o corpo
    da lei (artigos com numeração crescente).
    
    Retorna: dict com texto_por_pg (cache) + fim_corpo + max_visto
    
    CACHE: se usar_cache=True e work_dir/etapa2_fim_corpo.json existe, reusa.
    """
    _log("ETAPA 2/8 — Detectando fim do corpo da lei", log_callback)
    t0 = time.time()
    
    # Cache via JSON no work_dir
    if usar_cache and work_dir:
        cache_path = os.path.join(work_dir, 'etapa2_fim_corpo.json')
        if os.path.exists(cache_path):
            try:
                with open(cache_path) as f:
                    cached = json.load(f)
                if cached.get('fim_corpo') is not None and 'texto_por_pg' in cached:
                    txt_pg = {int(k): v for k, v in cached['texto_por_pg'].items()}
                    _log(f"  ETAPA 2 CACHE HIT: fim_corpo={cached['fim_corpo']}", log_callback)
                    return {
                        'fim_corpo': cached['fim_corpo'],
                        'max_artigo_visto': cached.get('max_artigo_visto', 0),
                        'texto_por_pg': txt_pg,
                        'tempo': time.time() - t0,
                        'cached': True,
                    }
            except Exception as e:
                _log(f"  Cache etapa2 invalido: {e}", log_callback)
    
    texto_por_pg = {}
    for pg in range(1, n_paginas + 1):
        try:
            r = subprocess.run(
                ['pdftotext', '-layout', '-f', str(pg), '-l', str(pg), pdf_unico, '-'],
                capture_output=True, text=True, errors='replace', timeout=15
            )
            texto_por_pg[pg] = r.stdout
        except Exception as e:
            logger.warning(f"pdftotext pg {pg} falhou: {e}")
            texto_por_pg[pg] = ''
    
    max_visto = 0
    fim_corpo = 0
    
    for pg in range(1, n_paginas + 1):
        arts = [int(m.group(1)) for m in PAT_ART_INICIO.finditer(texto_por_pg[pg])]
        if arts:
            primeiro = arts[0]
            if primeiro >= max_visto - 5:
                max_visto = max(max_visto, arts[-1])
                fim_corpo = pg
    
    _log(f"  Total: {n_paginas} pgs | Corpo: 1-{fim_corpo} (Art 1-{max_visto}) | Anexos: {fim_corpo+1}-{n_paginas}", log_callback)
    _log(f"  Etapa 2 concluída em {time.time()-t0:.1f}s", log_callback)
    
    resultado_e2 = {
        'fim_corpo': fim_corpo,
        'max_artigo_visto': max_visto,
        'texto_por_pg': texto_por_pg,
        'tempo': time.time() - t0,
    }
    
    # Salva cache pra proxima execucao
    if work_dir:
        try:
            cache_path = os.path.join(work_dir, 'etapa2_fim_corpo.json')
            cache_data = {
                'fim_corpo': fim_corpo,
                'max_artigo_visto': max_visto,
                'texto_por_pg': {str(k): v for k, v in texto_por_pg.items()},
            }
            with open(cache_path, 'w') as f:
                json.dump(cache_data, f, ensure_ascii=False)
            _log(f"  Cache etapa2 salvo: {cache_path}", log_callback)
        except Exception as e:
            _log(f"  AVISO: falhou salvando cache etapa2: {e}", log_callback)
    
    return resultado_e2


# ═══════════════════════════════════════════════════════════════════════════════
# ETAPA 3 — QUEBRA EM CORPO + ANEXOS
# ═══════════════════════════════════════════════════════════════════════════════

def etapa3_quebrar_pdf(pdf_unico, fim_corpo, n_paginas, work_dir, log_callback=None, usar_cache=True):
    """
    Usa pypdf pra dividir o PDF em corpo.pdf + anexos.pdf.
    
    Retorna: dict com paths e info sobre quebra
    
    CACHE: se usar_cache=True e ambos corpo.pdf e anexos.pdf existem, reusa.
    """
    _log("ETAPA 3/8 — Quebrando PDF em corpo + anexos", log_callback)
    t0 = time.time()
    
    # Cache: se corpo.pdf e anexos.pdf ja existem, pula split
    corpo_cache = os.path.join(work_dir, "corpo.pdf")
    anexos_cache = os.path.join(work_dir, "anexos.pdf")
    if (usar_cache and os.path.exists(corpo_cache) and os.path.getsize(corpo_cache) > 500
        and os.path.exists(anexos_cache) and os.path.getsize(anexos_cache) > 500):
        try:
            n_pg_corpo = len(pypdf.PdfReader(corpo_cache).pages)
            n_pg_anexos = len(pypdf.PdfReader(anexos_cache).pages)
            _log(f"  ETAPA 3 CACHE HIT: corpo {n_pg_corpo}pgs + anexos {n_pg_anexos}pgs", log_callback)
            return {
                'corpo_pdf': corpo_cache,
                'anexos_pdf': anexos_cache,
                'corpo_n_pgs': n_pg_corpo,
                'anexos_n_pgs': n_pg_anexos,
                'tempo': time.time() - t0,
                'cached': True,
            }
        except Exception:
            pass  # Cache invalido, segue execucao normal
    
    corpo_pdf = os.path.join(work_dir, "corpo.pdf")
    anexos_pdf = os.path.join(work_dir, "anexos.pdf")
    
    reader = pypdf.PdfReader(pdf_unico)
    
    # Corpo: pgs 1 ate fim_corpo
    writer_corpo = pypdf.PdfWriter()
    for i in range(0, fim_corpo):
        writer_corpo.add_page(reader.pages[i])
    with open(corpo_pdf, 'wb') as f:
        writer_corpo.write(f)
    
    # Anexos: pgs fim_corpo+1 ate fim
    writer_anexos = pypdf.PdfWriter()
    for i in range(fim_corpo, n_paginas):
        writer_anexos.add_page(reader.pages[i])
    with open(anexos_pdf, 'wb') as f:
        writer_anexos.write(f)
    
    n_pg_anexos = n_paginas - fim_corpo
    
    _log(f"  Corpo: {corpo_pdf} ({fim_corpo} pgs)", log_callback)
    _log(f"  Anexos: {anexos_pdf} ({n_pg_anexos} pgs)", log_callback)
    _log(f"  Etapa 3 concluída em {time.time()-t0:.1f}s", log_callback)
    
    return {
        'corpo_pdf': corpo_pdf,
        'anexos_pdf': anexos_pdf,
        'corpo_n_pgs': fim_corpo,
        'anexos_n_pgs': n_pg_anexos,
        'tempo': time.time() - t0,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# ETAPA 4 — CATALOGAÇÃO DOS ANEXOS (HAIKU)
# ═══════════════════════════════════════════════════════════════════════════════

PROMPT_CATALOGACAO_HAIKU = """Voce vai analisar este PDF (anexos de uma lei municipal) e catalogar cada bloco.

Cada bloco é uma seção logica: Anexo 1.1, Anexo 1.2, Anexo 2.1, Anexo 2.2... ou
errata, encerramento, etc.

Para cada bloco, retorne:
- nome: chave curta (ex: "anexo_1.1", "anexo_2.4", "errata", "encerramento")
- titulo: título completo conforme aparece no PDF
- inicio: número da página onde COMEÇA o bloco (1-indexado)
- fim: número da página onde TERMINA o bloco
- tipo: "anexo" | "errata" | "encerramento" | "indefinido"

Retorne JSON:
{
  "blocos": [
    {"nome": "anexo_1.1", "titulo": "Anexo 1.1 - Mapa de Zoneamento", "inicio": 1, "fim": 2, "tipo": "anexo"},
    ...
  ]
}

ATENÇÃO: as paginas que voce ve no PDF começam em 1. Use essa numeração."""


def etapa4_catalogar_anexos(anexos_pdf, corpo_n_pgs, work_dir, log_callback=None):
    """
    Usa Claude Haiku 4.5 (visão + texto) pra catalogar os blocos do PDF de anexos.
    
    Retorna: lista de blocos com {nome, titulo, inicio, fim, tipo}.
    inicio/fim são páginas NO PDF CONCATENADO (não no anexos.pdf).
    """
    _log("ETAPA 4/8 — Catalogando anexos com Haiku 4.5", log_callback)
    t0 = time.time()
    
    # Pega n_paginas do anexos.pdf
    r = subprocess.run(['pdfinfo', anexos_pdf], capture_output=True, text=True, timeout=30)
    n_pg_anexos = 0
    for linha in r.stdout.split('\n'):
        if linha.startswith('Pages:'):
            n_pg_anexos = int(linha.split(':')[1].strip())
            break
    
    if n_pg_anexos == 0:
        raise RuntimeError("Não consegui ler número de páginas do anexos.pdf")
    
    # Refaz texto -layout do anexos.pdf (numeração coerente com o que IA vê)
    texto_anexos = ""
    for pg in range(1, n_pg_anexos + 1):
        try:
            r = subprocess.run(
                ['pdftotext', '-layout', '-f', str(pg), '-l', str(pg), anexos_pdf, '-'],
                capture_output=True, text=True, errors='replace', timeout=15
            )
            texto_anexos += f"\n=== PAGINA {pg} ===\n{r.stdout}"
        except Exception:
            pass
    
    # Se anexos > 100 pgs, precisa quebrar (limite da API)
    if n_pg_anexos > 100:
        _log(f"  Anexos tem {n_pg_anexos} pgs (> 100), quebrando em sub-PDFs", log_callback)
        # TODO: implementar split em sub-PDFs e chamadas múltiplas
        # Por ora, mandamos só as 100 primeiras
        anexos_pdf_uso = os.path.join(work_dir, "anexos_100.pdf")
        reader = pypdf.PdfReader(anexos_pdf)
        writer = pypdf.PdfWriter()
        for i in range(100):
            writer.add_page(reader.pages[i])
        with open(anexos_pdf_uso, 'wb') as f:
            writer.write(f)
    else:
        anexos_pdf_uso = anexos_pdf
    
    # Carrega PDF em base64
    with open(anexos_pdf_uso, 'rb') as f:
        pdf_b64 = base64.b64encode(f.read()).decode('ascii')
    
    client = anthropic.Anthropic(api_key=os.environ['ANTHROPIC_API_KEY'])
    
    prompt = PROMPT_CATALOGACAO_HAIKU + "\n\n=== TEXTO -LAYOUT (referência) ===\n" + texto_anexos[:50000]
    
    _log(f"  Chamando Haiku 4.5...", log_callback)
    
    resposta = ""
    try:
        with client.messages.stream(
            model=MODELO_HAIKU,
            max_tokens=8000,
            messages=[{
                "role": "user",
                "content": [
                    {"type": "document", "source": {
                        "type": "base64", "media_type": "application/pdf", "data": pdf_b64
                    }, "title": "anexos.pdf"},
                    {"type": "text", "text": prompt},
                ]
            }]
        ) as stream:
            for delta in stream.text_stream:
                resposta += delta
            final = stream.get_final_message()
    except Exception as e:
        raise RuntimeError(f"Haiku falhou: {e}")
    
    tokens_in = final.usage.input_tokens
    tokens_out = final.usage.output_tokens
    custo = calcular_custo(tokens_in, tokens_out, 'haiku')
    
    parsed = parse_json_robusto(resposta)
    if not parsed or 'blocos' not in parsed:
        raise RuntimeError(f"Haiku retornou JSON inválido: {resposta[:300]}")
    
    blocos_raw = parsed['blocos']
    
    # Converte paginas: como anexos.pdf comeca na pg 1, somar corpo_n_pgs pra obter pg no PDF concatenado
    # Adiciona bloco "corpo_lei" no início e ajusta páginas dos demais
    blocos = [{
        'nome': 'corpo_lei',
        'titulo': 'Corpo da Lei',
        'inicio': 1,
        'fim': corpo_n_pgs,
        'tipo': 'corpo',
    }]
    
    for b in blocos_raw:
        if not isinstance(b, dict):
            continue
        ini = b.get('inicio')
        fim = b.get('fim')
        if not isinstance(ini, int) or not isinstance(fim, int):
            continue
        blocos.append({
            'nome': b.get('nome', f'bloco_{len(blocos)}'),
            'titulo': b.get('titulo', ''),
            'inicio': ini + corpo_n_pgs,
            'fim': fim + corpo_n_pgs,
            'tipo': b.get('tipo', 'indefinido'),
        })
    
    _log(f"  Catalogados {len(blocos)} blocos (corpo + {len(blocos_raw)} anexos/erratas)", log_callback)
    _log(f"  Tokens: in={tokens_in}, out={tokens_out} | Custo: ${custo:.3f}", log_callback)
    _log(f"  Etapa 4 concluída em {time.time()-t0:.1f}s", log_callback)
    
    return {
        'blocos': blocos,
        'tempo': time.time() - t0,
        'tokens_in': tokens_in,
        'tokens_out': tokens_out,
        'custo': custo,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# ETAPA 5 — EXTRAÇÃO DE DADOS POR BLOCO (SONNET)
# ═══════════════════════════════════════════════════════════════════════════════

def _quebrar_pdf(origem, ini, fim, destino):
    """Quebra PDF de uma faixa de páginas (1-indexado) com pypdf."""
    reader = pypdf.PdfReader(origem)
    writer = pypdf.PdfWriter()
    for i in range(ini - 1, fim):
        if i < len(reader.pages):
            writer.add_page(reader.pages[i])
    with open(destino, 'wb') as f:
        writer.write(f)


def _texto_layout(pdf_unico, ini, fim, texto_por_pg=None):
    """
    Pega texto -layout das páginas ini-fim do PDF concatenado.
    Se texto_por_pg fornecido (cache da etapa 2), usa direto.
    """
    partes = []
    for pg in range(ini, fim + 1):
        if texto_por_pg and pg in texto_por_pg:
            partes.append(f"=== PAGINA {pg} ===\n{texto_por_pg[pg]}")
        else:
            try:
                r = subprocess.run(
                    ['pdftotext', '-layout', '-f', str(pg), '-l', str(pg), pdf_unico, '-'],
                    capture_output=True, text=True, errors='replace', timeout=15
                )
                partes.append(f"=== PAGINA {pg} ===\n{r.stdout}")
            except Exception:
                pass
    return "\n\n".join(partes)


def _preparar_blocos(blocos, pdf_unico, texto_por_pg, work_dir, log_callback=None):
    """Quebra cada bloco em PDF próprio e gera texto -layout coerente."""
    _log(f"  Preparando PDFs e textos de {len(blocos)} blocos...", log_callback)
    for b in blocos:
        nome_safe = b['nome'].replace('.', '_').replace(' ', '_').replace('/', '_')
        b['pdf_path'] = os.path.join(work_dir, f"bloco_{nome_safe}.pdf")
        _quebrar_pdf(pdf_unico, b['inicio'], b['fim'], b['pdf_path'])
        b['texto_layout'] = _texto_layout(pdf_unico, b['inicio'], b['fim'], texto_por_pg)
        n_p = b['fim'] - b['inicio'] + 1
        _log(f"    {b['nome']:25s} pgs {b['inicio']:3d}-{b['fim']:3d} | {n_p} pgs | {len(b['texto_layout'])} chars", log_callback)


def chamar_sonnet_extracao(pdf_path, texto_layout, prompt_extra, prompt_v13,
                           label, client, max_tokens=32000, log_callback=None):
    """
    Chama Sonnet 4.6 com PDF + texto -layout + prompt evolutivo + prompt v13.
    
    Retorna: (texto_resposta, tempo, tokens_in, tokens_out) ou (None, 0, 0, 0) se erro
    """
    with open(pdf_path, 'rb') as f:
        pdf_b64 = base64.b64encode(f.read()).decode('ascii')
    
    prompt_final = (
        prompt_extra
        + "\n\n=== TEXTO -LAYOUT ===\n"
        + texto_layout[:30000]
        + "\n\n=== PROMPT V13 ===\n"
        + prompt_v13
    )
    
    _log(f"    [{label}] Chamando Sonnet 4.6...", log_callback)
    t0 = time.time()
    texto = ""
    
    try:
        with client.messages.stream(
            model=MODELO_SONNET,
            max_tokens=max_tokens,
            messages=[{
                "role": "user",
                "content": [
                    {"type": "document", "source": {
                        "type": "base64", "media_type": "application/pdf", "data": pdf_b64
                    }, "title": f"{label}.pdf"},
                    {"type": "text", "text": prompt_final},
                ]
            }],
            extra_headers={"anthropic-beta": "context-1m-2025-08-07"}
        ) as stream:
            last = 0
            for delta in stream.text_stream:
                texto += delta
                now = time.time() - t0
                if now - last >= 30:
                    _log(f"      {now:.0f}s, {len(texto)} chars", log_callback)
                    last = now
            final = stream.get_final_message()
    except Exception as e:
        _log(f"    ERRO Sonnet [{label}]: {e}", log_callback)
        return None, time.time() - t0, 0, 0
    
    return texto, time.time() - t0, final.usage.input_tokens, final.usage.output_tokens


def _extrair_usos_da_resposta(parsed):
    """Extrai mapeamento {sigla -> [usos]} do JSON do Sonnet."""
    mapa = {}
    for z in (parsed.get('zonas') or []):
        if not isinstance(z, dict):
            continue
        sigla = z.get('sigla_canonica')
        if not sigla:
            continue
        usos = z.get('usos_permitidos')
        if usos and isinstance(usos, dict):
            usos_sim = []
            for uso, det in usos.items():
                if isinstance(det, dict):
                    status = (det.get('status') or '').upper()
                    if status in ('SIM', 'CONDICIONADO'):
                        usos_sim.append(uso)
            if usos_sim:
                mapa[sigla] = usos_sim
    return mapa


def _atualizar_estado(estado, parsed):
    """Acumula resposta no estado global (sem merge profundo — só completa null)."""
    if not parsed:
        return
    
    # Legislacao
    leg = parsed.get('legislacao')
    if leg and isinstance(leg, dict) and not estado['legislacao']:
        estado['legislacao'] = leg
    
    # Modificacoes (da errata)
    if leg and isinstance(leg, dict):
        for m in (leg.get('modificacoes') or []):
            if isinstance(m, dict):
                k = (m.get('alvo'), m.get('dispositivo'))
                if k not in [(x.get('alvo'), x.get('dispositivo'))
                             for x in estado['modificacoes']]:
                    estado['modificacoes'].append(m)
    
    # Zonas
    for z in (parsed.get('zonas') or []):
        if not isinstance(z, dict):
            continue
        sigla = z.get('sigla_canonica')
        if not sigla:
            continue
        if sigla not in estado['zonas']:
            estado['zonas'][sigla] = z
        else:
            ex = estado['zonas'][sigla]
            for campo in ['usos_permitidos', 'parametros_gerais',
                          'parametros_por_uso', 'variacoes',
                          'acrescimos_extraordinarios', 'hierarquia',
                          'metodologia_area_computavel', 'afastamentos_crescentes']:
                if ex.get(campo) in (None, {}, []) and z.get(campo) not in (None, {}, []):
                    ex[campo] = z[campo]
    
    # Mapeamento de usos
    novos_usos = _extrair_usos_da_resposta(parsed)
    for sigla, usos in novos_usos.items():
        if sigla not in estado['usos_por_zona']:
            estado['usos_por_zona'][sigla] = usos


def _gerar_contexto(estado):
    """Constrói contexto evolutivo a partir do estado."""
    linhas = ["=== CONTEXTO ACUMULADO (do que ja foi descoberto em chamadas anteriores) ==="]
    
    if estado['legislacao']:
        l = estado['legislacao']
        linhas.append(f"Lei: {l.get('tipo')} {l.get('numero')}/{l.get('ano')} ({l.get('municipio')}/{l.get('estado')})")
    
    if estado['usos_por_zona']:
        linhas.append(f"\nMapeamento de usos por zona ja descoberto:")
        for sigla, usos in sorted(estado['usos_por_zona'].items()):
            linhas.append(f"  {sigla}: {', '.join(usos)}")
    
    if estado['zonas']:
        linhas.append(f"\nZonas ja processadas: {len(estado['zonas'])}")
        for sigla in sorted(estado['zonas'].keys()):
            z = estado['zonas'][sigla]
            tem_params = bool(z.get('parametros_gerais') or z.get('parametros_por_uso'))
            tem_usos = sigla in estado['usos_por_zona']
            linhas.append(f"  {sigla}: usos={'SIM' if tem_usos else 'null'}, params={'SIM' if tem_params else 'null'}")
    
    linhas.append("\nINSTRUCAO: complete o que esta null. Adicione zonas novas se houver. "
                  "Para parametros, verifique se variam por uso permitido.")
    return "\n".join(linhas)


def _prio_bloco(b):
    """Define ordem de processamento dos blocos."""
    nome = b['nome']
    if nome == 'corpo_lei': return -1  # ja processado na FASE 1A
    if nome == 'encerramento': return -1
    if 'errata' in nome.lower(): return 99  # errata por ultimo
    if nome.startswith('anexo_2.'): return 10  # params primeiro
    if nome.startswith('anexo_1.'): return 20  # mapas depois
    return 50


def etapa5_extrair_dados(blocos, pdf_unico, texto_por_pg, work_dir,
                         usar_cache=True, log_callback=None):
    """
    Faz N chamadas Sonnet sequenciais com contexto evolutivo.
    
    FASE 1A: lê CORPO procurando usos
    FASE 1B: se não achou no corpo, busca anexo com nome indicativo
    FASE 2:  processa demais blocos (anexos 2.X > 1.X > errata)
    
    Retorna: dict com estado consolidado + métricas
    """
    _log("ETAPA 5/8 — Extração de dados por bloco (Sonnet 4.6)", log_callback)
    t0 = time.time()
    
    # Prepara PDFs e textos
    _preparar_blocos(blocos, pdf_unico, texto_por_pg, work_dir, log_callback)
    
    # Inicializa estado
    estado = {
        'legislacao': None,
        'usos_por_zona': {},
        'zonas': {},
        'modificacoes': [],
        'refs_externas': [],
    }
    
    total_tempo = 0
    total_in = 0
    total_out = 0
    
    prompt_v13 = carregar_prompt_v13()
    client = anthropic.Anthropic(api_key=os.environ['ANTHROPIC_API_KEY'])
    
    # ────────────────────────────────────────────────────────────────────────
    # FASE 1A — CORPO
    # ────────────────────────────────────────────────────────────────────────
    _log("  FASE 1A: Lendo CORPO procurando usos por zona", log_callback)
    
    corpo_bloco = next((b for b in blocos if b['nome'] == 'corpo_lei'), None)
    if not corpo_bloco:
        raise RuntimeError("Bloco corpo_lei não encontrado")
    
    contexto_corpo = """OBJETIVO ESPECIAL DESTA CHAMADA:
Voce esta vendo o CORPO da lei municipal. Sua tarefa principal:
1. Identificar a legislacao (tipo, numero, ano, municipio, estado)
2. Identificar TODAS as zonas urbanisticas declaradas
3. Para CADA ZONA, identificar os USOS PERMITIDOS
4. Extrair quaisquer parametros que apareçam no corpo da lei

Os parametros detalhados podem estar em anexos separados.
Concentre-se em USOS PERMITIDOS por zona.

Aplique o prompt v13 abaixo para gerar o JSON."""
    
    cache_corpo = os.path.join(work_dir, "etapa5_corpo.txt")
    if usar_cache and os.path.exists(cache_corpo) and os.path.getsize(cache_corpo) > 10000:
        _log(f"    [CORPO] CACHE HIT: {os.path.getsize(cache_corpo)} chars", log_callback)
        resp_corpo = open(cache_corpo).read()
        t, ti, to = 0, 0, 0
    else:
        resp_corpo, t, ti, to = chamar_sonnet_extracao(
            corpo_bloco['pdf_path'], corpo_bloco['texto_layout'],
            contexto_corpo, prompt_v13, 'CORPO', client, log_callback=log_callback
        )
        if resp_corpo:
            open(cache_corpo, 'w').write(resp_corpo)
    
    total_tempo += t; total_in += ti; total_out += to
    
    parsed_corpo = parse_json_robusto(resp_corpo or '')
    if parsed_corpo:
        _atualizar_estado(estado, parsed_corpo)
        _log(f"    Parse OK | Zonas: {len(estado['zonas'])} | Usos mapeados: {len(estado['usos_por_zona'])}", log_callback)
    else:
        _log(f"    PARSE FALHOU no CORPO", log_callback)
    
    # ────────────────────────────────────────────────────────────────────────
    # FASE 1B — Anexo de usos (se não achou no corpo)
    # ────────────────────────────────────────────────────────────────────────
    if not estado['usos_por_zona']:
        _log("  FASE 1B: Procurando anexo de usos pelo nome", log_callback)
        anexo_usos = None
        for b in blocos:
            if b['nome'] in ('corpo_lei', 'encerramento'):
                continue
            if eh_anexo_de_usos(b.get('titulo', '')):
                anexo_usos = b
                _log(f"    Encontrado: {b['nome']} - {b.get('titulo')}", log_callback)
                break
        
        if anexo_usos:
            ctx = "OBJETIVO: Voce esta vendo o anexo que lista USOS PERMITIDOS por zona.\n\n" + _gerar_contexto(estado)
            cache_path = os.path.join(work_dir, f"etapa5_{anexo_usos['nome'].replace('.', '_')}.txt")
            if usar_cache and os.path.exists(cache_path) and os.path.getsize(cache_path) > 1000:
                resp = open(cache_path).read()
                t, ti, to = 0, 0, 0
            else:
                resp, t, ti, to = chamar_sonnet_extracao(
                    anexo_usos['pdf_path'], anexo_usos['texto_layout'],
                    ctx, prompt_v13, anexo_usos['nome'], client, log_callback=log_callback
                )
                if resp:
                    open(cache_path, 'w').write(resp)
            total_tempo += t; total_in += ti; total_out += to
            parsed = parse_json_robusto(resp or '')
            if parsed:
                _atualizar_estado(estado, parsed)
    else:
        _log("  FASE 1B pulada (usos ja descobertos no corpo)", log_callback)
    
    # ────────────────────────────────────────────────────────────────────────
    # FASE 2 — Demais blocos com contexto evolutivo
    # ────────────────────────────────────────────────────────────────────────
    _log("  FASE 2: Processando demais blocos com contexto evolutivo", log_callback)
    
    blocos_processar = sorted(
        [b for b in blocos if _prio_bloco(b) >= 0],
        key=_prio_bloco
    )
    
    for b in blocos_processar:
        _log(f"    >>> Bloco: {b['nome']} ({b['fim']-b['inicio']+1} pgs)", log_callback)
        cache_path = os.path.join(work_dir, f"etapa5_{b['nome'].replace('.', '_')}.txt")
        
        if usar_cache and os.path.exists(cache_path) and os.path.getsize(cache_path) > 1000:
            _log(f"      [{b['nome']}] CACHE HIT: {os.path.getsize(cache_path)} chars", log_callback)
            resp = open(cache_path).read()
            t, ti, to = 0, 0, 0
        else:
            ctx = _gerar_contexto(estado)
            resp, t, ti, to = chamar_sonnet_extracao(
                b['pdf_path'], b['texto_layout'],
                ctx, prompt_v13, b['nome'], client, log_callback=log_callback
            )
            if resp:
                open(cache_path, 'w').write(resp)
        
        total_tempo += t; total_in += ti; total_out += to
        parsed = parse_json_robusto(resp or '')
        if parsed:
            _atualizar_estado(estado, parsed)
            _log(f"      Parse OK | Zonas total: {len(estado['zonas'])}", log_callback)
    
    custo = calcular_custo(total_in, total_out, 'sonnet')
    _log(f"  Etapa 5 concluída | Tempo: {time.time()-t0:.0f}s | "
         f"Tokens: in={total_in}, out={total_out} | Custo: ${custo:.2f}", log_callback)
    
    return {
        'estado': estado,
        'tempo': time.time() - t0,
        'tokens_in': total_in,
        'tokens_out': total_out,
        'custo': custo,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# ETAPA 6 — RECONSOLIDAÇÃO COM MERGE PROFUNDO
# ═══════════════════════════════════════════════════════════════════════════════

def etapa6_reconsolidar(estado_run, work_dir, zonas_validas=None, log_callback=None):
    """Aplica merge profundo, filtra falsos positivos."""
    _log("ETAPA 6/8 — Reconsolidação com merge profundo", log_callback)
    t0 = time.time()
    
    if zonas_validas is None:
        zonas_validas = ZONAS_VALIDAS_DEFAULT
    
    # FIX: ler TODOS os etapa5_*.txt da pasta dinamicamente
    # (antes era lista hardcoded com nomes antigos do Xangri-La,
    #  causando perda de zonas quando blocos tinham outros nomes)
    ordem = []
    # 'corpo' SEMPRE primeiro pra garantir prioridade no merge
    if os.path.exists(os.path.join(work_dir, 'etapa5_corpo.txt')):
        ordem.append('corpo')
    # Adiciona TODOS os outros etapa5_*.txt em ordem alfabetica
    try:
        for arq in sorted(os.listdir(work_dir)):
            if arq.startswith('etapa5_') and arq.endswith('.txt'):
                nome_bloco = arq[len('etapa5_'):-len('.txt')]
                if nome_bloco != 'corpo' and nome_bloco not in ordem:
                    ordem.append(nome_bloco)
    except Exception as _e_ls:
        _log(f"  WARN listdir work_dir: {_e_ls}", log_callback)
    _log(f"  Etapa 6 processando {len(ordem)} blocos: {ordem}", log_callback)
    
    estado = {'legislacao': None, 'zonas': {}, 'modificacoes': [], 'refs_externas': []}
    descartados = set()
    
    for nome in ordem:
        path = os.path.join(work_dir, f'etapa5_{nome}.txt')
        if not os.path.exists(path):
            continue
        try:
            with open(path) as f:
                parsed = parse_json_robusto(f.read())
        except Exception:
            continue
        if not parsed:
            continue
        
        leg = parsed.get('legislacao')
        if leg and isinstance(leg, dict) and not estado['legislacao']:
            estado['legislacao'] = leg
        if leg and isinstance(leg, dict):
            for m in (leg.get('modificacoes') or []):
                if isinstance(m, dict):
                    k = (m.get('alvo'), m.get('dispositivo'))
                    if k not in [(x.get('alvo'), x.get('dispositivo')) for x in estado['modificacoes']]:
                        estado['modificacoes'].append(m)
        
        novas = atualizadas = 0
        for z in (parsed.get('zonas') or []):
            if not isinstance(z, dict):
                continue
            sigla = (z.get('sigla_canonica') or '').strip().upper()
            if not eh_zona_real(sigla, zonas_validas):
                if sigla:
                    descartados.add(sigla)
                continue
            if sigla not in estado['zonas']:
                estado['zonas'][sigla] = z
                novas += 1
            else:
                ex = estado['zonas'][sigla]
                for campo in ['usos_permitidos','parametros_gerais','parametros_por_uso',
                              'variacoes','acrescimos_extraordinarios','hierarquia',
                              'metodologia_area_computavel','afastamentos_crescentes']:
                    if ex.get(campo) is None and z.get(campo) is not None:
                        ex[campo] = z[campo]
                    elif ex.get(campo) and z.get(campo):
                        merge_profundo(ex[campo], z[campo])
                atualizadas += 1
        _log(f"  {nome:20s} -> {novas} novas, {atualizadas} merge", log_callback)
    
    _log(f"  Zonas: {len(estado['zonas'])} | Descartados: {sorted(descartados) or 'nenhum'}", log_callback)
    _log(f"  Modificações: {len(estado['modificacoes'])} | Etapa 6 em {time.time()-t0:.1f}s", log_callback)
    return {'estado': estado, 'descartados': sorted(descartados), 'tempo': time.time()-t0}


# ═══════════════════════════════════════════════════════════════════════════════
# ETAPA 7 — EXPANSÃO DE parametros_por_uso
# ═══════════════════════════════════════════════════════════════════════════════

def etapa7_expandir_por_uso(estado, log_callback=None):
    """Replica params gerais pra cada uso permitido."""
    _log("ETAPA 7/8 — Expansão de parametros_por_uso", log_callback)
    t0 = time.time()
    expandidas = 0
    for sigla, zona in estado['zonas'].items():
        usos = zona.get('usos_permitidos') or {}
        params_gerais = zona.get('parametros_gerais') or {}
        if not usos or not params_gerais:
            continue
        usos_permitidos = []
        for nome_uso, det in usos.items():
            if isinstance(det, dict):
                status = (det.get('status') or '').strip().upper()
                if status in ('SIM', 'CONDICIONADO'):
                    usos_permitidos.append(nome_uso)
        if not usos_permitidos:
            continue
        ppu_atual = zona.get('parametros_por_uso')
        if isinstance(ppu_atual, dict) and ppu_atual:
            for uso in usos_permitidos:
                if uso not in ppu_atual:
                    ppu_atual[uso] = copy.deepcopy(params_gerais)
        else:
            zona['parametros_por_uso'] = {uso: copy.deepcopy(params_gerais) for uso in usos_permitidos}
        expandidas += 1
    _log(f"  Zonas expandidas: {expandidas} | Etapa 7 em {time.time()-t0:.1f}s", log_callback)
    return {'expandidas': expandidas, 'tempo': time.time()-t0}


# ═══════════════════════════════════════════════════════════════════════════════
# ETAPA 8 — APLICAÇÃO DA ERRATA NAS ZONAS (SONNET)
# ═══════════════════════════════════════════════════════════════════════════════

def etapa8_aplicar_errata(estado, log_callback=None):
    """Sonnet identifica alteracoes em formato {zona, campo_path, valor_novo}."""
    _log("ETAPA 8/8 — Aplicação da errata (Sonnet 4.6)", log_callback)
    t0 = time.time()
    
    mods_relevantes = []
    for i, mod in enumerate(estado.get('modificacoes', []), 1):
        tipo = (mod.get('tipo') or '').lower()
        if 'revoga' in tipo or 'mantida' in tipo:
            continue
        mod_copia = dict(mod)
        mod_copia['_mod_num'] = i
        mods_relevantes.append(mod_copia)
    
    if not mods_relevantes:
        _log("  Nenhuma modificação relevante.", log_callback)
        return {'alteracoes_aplicadas': 0, 'tempo': time.time()-t0,
                'tokens_in': 0, 'tokens_out': 0, 'custo': 0.0}
    
    zonas_afetadas = set()
    for mod in mods_relevantes:
        texto = str(mod.get('escopo_geografico','')) + ' ' + str(mod.get('dispositivo',''))
        for sigla in estado['zonas'].keys():
            if re.search(r'\b' + re.escape(sigla) + r'\b', texto):
                zonas_afetadas.add(sigla)
    
    if not zonas_afetadas:
        _log("  Nenhuma zona afetada.", log_callback)
        return {'alteracoes_aplicadas': 0, 'tempo': time.time()-t0,
                'tokens_in': 0, 'tokens_out': 0, 'custo': 0.0}
    
    zonas_compactas = {
        sigla: {
            'parametros_gerais': estado['zonas'][sigla].get('parametros_gerais'),
            'usos_permitidos': estado['zonas'][sigla].get('usos_permitidos'),
            'variacoes': estado['zonas'][sigla].get('variacoes'),
        }
        for sigla in zonas_afetadas if sigla in estado['zonas']
    }
    
    prompt = ("Voce analisa modificacoes de uma ERRATA e identifica quais campos do JSON de cada zona devem ser alterados.\n"
              "Retorne JSON enxuto com {alteracoes: [{zona, campo_path, valor_antigo, valor_novo, mod_num, descricao}]}.\n"
              "Use dot-notation no campo_path (ex: 'parametros_gerais.altura.valor').\n"
              "NUNCA retorne a zona inteira. Apenas alteracoes.\n\n"
              f"=== ZONAS AFETADAS ===\n{json.dumps(zonas_compactas, ensure_ascii=False, indent=2)}\n\n"
              f"=== MODIFICACOES ===\n{json.dumps(mods_relevantes, ensure_ascii=False, indent=2)}\n\n"
              "Responda APENAS o JSON.")
    
    client = anthropic.Anthropic(api_key=os.environ['ANTHROPIC_API_KEY'])
    _log(f"  Chamando Sonnet (prompt: {len(prompt)} chars)...", log_callback)
    
    texto = ""
    try:
        with client.messages.stream(model=MODELO_SONNET, max_tokens=8000,
                                     messages=[{"role":"user","content":prompt}]) as stream:
            for delta in stream.text_stream:
                texto += delta
            final = stream.get_final_message()
    except Exception as e:
        raise RuntimeError(f"Sonnet falhou: {e}")
    
    tokens_in = final.usage.input_tokens
    tokens_out = final.usage.output_tokens
    custo = calcular_custo(tokens_in, tokens_out, 'sonnet')
    
    parsed = parse_json_robusto(texto)
    if not parsed:
        _log("  PARSE FALHOU", log_callback)
        return {'alteracoes_aplicadas': 0, 'tempo': time.time()-t0,
                'tokens_in': tokens_in, 'tokens_out': tokens_out, 'custo': custo}
    
    alteracoes = parsed.get('alteracoes', [])
    aplicadas = 0
    for alt in alteracoes:
        zona = alt.get('zona')
        path = alt.get('campo_path')
        valor_novo = alt.get('valor_novo')
        if zona in estado['zonas'] and path and valor_novo is not None:
            if set_dot_path(estado['zonas'][zona], path, valor_novo):
                aplicadas += 1
    
    _log(f"  Aplicadas: {aplicadas}/{len(alteracoes)} | Custo: ${custo:.3f}", log_callback)
    return {'alteracoes_aplicadas': aplicadas, 'alteracoes_propostas': len(alteracoes),
            'tempo': time.time()-t0, 'tokens_in': tokens_in, 'tokens_out': tokens_out, 'custo': custo}


# ═══════════════════════════════════════════════════════════════════════════════
# FUNÇÃO PRINCIPAL
# ═══════════════════════════════════════════════════════════════════════════════

def processar_municipio(zip_path, municipio, estado, output_dir=None,
                        usar_cache=True, log_callback=None, zonas_validas=None):
    """
    Executa pipeline completo de extração de lei urbanística.
    
    Args:
        zip_path:      caminho do ZIP com PDFs da legislação
        municipio:     nome do município (ex: 'Xangri-Lá')
        estado:        UF (ex: 'RS')
        output_dir:    diretório de intermediários (default: PIPELINES_BASE_DIR/Slug)
        usar_cache:    reaproveita arquivos no output_dir
        log_callback:  função(msg) opcional
        zonas_validas: set de siglas aceitas (default: ZONAS_VALIDAS_DEFAULT)
    
    Retorna:
        Sucesso: {sucesso: True, resultado: {...}, metricas: {...}}
        Erro:    {sucesso: False, erro_etapa: N, erro_msg: '...', resultado_parcial: {...}}
    """
    _log("="*80, log_callback)
    _log(f"PIPELINE DE EXTRAÇÃO — {municipio}/{estado}", log_callback)
    _log(f"Início: {datetime.now().isoformat()}", log_callback)
    _log("="*80, log_callback)
    
    t_inicio = time.time()
    if output_dir is None:
        # Cache por LEGISLACAO: /static/pipelines/<Municipio>_<UF>/leg_<md5short>/
        _slug = _slug_municipio(municipio, estado)
        _md5_zip = calcular_md5_zip(zip_path)
        if _md5_zip:
            _leg_dir = f"leg_{_md5_zip[:12]}"
        else:
            # fallback: usa basename do zip se md5 falhar
            _leg_dir = "leg_" + (os.path.basename(zip_path or 'sem_zip')[:30].replace('.', '_').replace(' ', '_'))
        output_dir = os.path.join(PIPELINES_BASE_DIR, _slug, _leg_dir)
    os.makedirs(output_dir, exist_ok=True)
    _log(f"Output dir: {output_dir}", log_callback)
    
    metricas = {'inicio': datetime.now().isoformat(), 'output_dir': output_dir,
                'etapas': {}, 'custo_total': 0.0, 'tokens_in': 0, 'tokens_out': 0}
    resultado_parcial = {'legislacao': None, 'zonas': {}, 'modificacoes': []}
    
    def _fim(etapa_num, msg, exc=None):
        metricas['fim'] = datetime.now().isoformat()
        metricas['tempo_total'] = time.time() - t_inicio
        _log(f"ERRO Etapa {etapa_num}: {msg}", log_callback)
        if exc:
            _log(f"Trace: {traceback.format_exc()[:500]}", log_callback)
        return {'sucesso': False, 'erro_etapa': etapa_num, 'erro_msg': msg,
                'resultado_parcial': resultado_parcial, 'metricas': metricas}
    
    try:
        r1 = etapa1_extrair_e_concatenar(zip_path, output_dir, log_callback, usar_cache=usar_cache)
        metricas['etapas']['1'] = {'tempo': r1['tempo'], 'n_pgs': r1['n_paginas']}
    except Exception as e:
        return _fim(1, str(e), e)
    
    try:
        r2 = etapa2_detectar_fim_corpo(r1['pdf_unico'], r1['n_paginas'], log_callback, work_dir=output_dir, usar_cache=usar_cache)
        metricas['etapas']['2'] = {'tempo': r2['tempo'], 'fim_corpo': r2['fim_corpo']}
    except Exception as e:
        return _fim(2, str(e), e)
    
    try:
        r3 = etapa3_quebrar_pdf(r1['pdf_unico'], r2['fim_corpo'], r1['n_paginas'], output_dir, log_callback, usar_cache=usar_cache)
        metricas['etapas']['3'] = {'tempo': r3['tempo']}
    except Exception as e:
        return _fim(3, str(e), e)
    
    try:
        cache_cat = os.path.join(output_dir, 'etapa4_catalogacao.json')
        if usar_cache and os.path.exists(cache_cat):
            with open(cache_cat) as f:
                r4 = json.load(f)
            _log(f"ETAPA 4/8 — CACHE HIT ({len(r4['blocos'])} blocos)", log_callback)
        else:
            r4 = etapa4_catalogar_anexos(r3['anexos_pdf'], r3['corpo_n_pgs'], output_dir, log_callback)
            with open(cache_cat, 'w') as f:
                json.dump({'blocos': r4['blocos'], 'tokens_in': r4['tokens_in'],
                          'tokens_out': r4['tokens_out'], 'custo': r4['custo']},
                         f, ensure_ascii=False, indent=2)
        metricas['etapas']['4'] = {'tempo': r4.get('tempo', 0), 'blocos': len(r4['blocos']),
                                    'custo': r4.get('custo', 0)}
        metricas['custo_total'] += r4.get('custo', 0)
        metricas['tokens_in'] += r4.get('tokens_in', 0)
        metricas['tokens_out'] += r4.get('tokens_out', 0)
    except Exception as e:
        return _fim(4, str(e), e)
    
    try:
        r5 = etapa5_extrair_dados(r4['blocos'], r1['pdf_unico'], r2['texto_por_pg'],
                                   output_dir, usar_cache, log_callback)
        metricas['etapas']['5'] = {'tempo': r5['tempo'], 'tokens_in': r5['tokens_in'],
                                    'tokens_out': r5['tokens_out'], 'custo': r5['custo'],
                                    'zonas_brutas': len(r5['estado']['zonas'])}
        metricas['custo_total'] += r5['custo']
        metricas['tokens_in'] += r5['tokens_in']
        metricas['tokens_out'] += r5['tokens_out']
        resultado_parcial = r5['estado']
    except Exception as e:
        return _fim(5, str(e), e)
    
    try:
        r6 = etapa6_reconsolidar(r5, output_dir, zonas_validas, log_callback)
        metricas['etapas']['6'] = {'tempo': r6['tempo'],
                                    'zonas_filtradas': len(r6['estado']['zonas']),
                                    'descartados': r6['descartados']}
        estado_final = r6['estado']
        resultado_parcial = estado_final
    except Exception as e:
        return _fim(6, str(e), e)
    
    try:
        r7 = etapa7_expandir_por_uso(estado_final, log_callback)
        metricas['etapas']['7'] = {'tempo': r7['tempo'], 'expandidas': r7['expandidas']}
    except Exception as e:
        return _fim(7, str(e), e)
    
    try:
        r8 = etapa8_aplicar_errata(estado_final, log_callback)
        metricas['etapas']['8'] = {'tempo': r8['tempo'],
                                    'alteracoes': r8.get('alteracoes_aplicadas', 0),
                                    'custo': r8.get('custo', 0)}
        metricas['custo_total'] += r8.get('custo', 0)
        metricas['tokens_in'] += r8.get('tokens_in', 0)
        metricas['tokens_out'] += r8.get('tokens_out', 0)
    except Exception as e:
        return _fim(8, str(e), e)
    
    metricas['fim'] = datetime.now().isoformat()
    metricas['tempo_total'] = time.time() - t_inicio
    
    resultado_final = os.path.join(output_dir, 'resultado_final.json')
    with open(resultado_final, 'w') as f:
        json.dump({'estado': estado_final, 'metricas': metricas},
                  f, ensure_ascii=False, indent=2, default=str)
    
    _log("="*80, log_callback)
    _log(f"PIPELINE CONCLUÍDO", log_callback)
    _log(f"  Zonas: {len(estado_final['zonas'])}", log_callback)
    _log(f"  Modificações: {len(estado_final['modificacoes'])}", log_callback)
    _log(f"  Custo: ${metricas['custo_total']:.2f}", log_callback)
    _log(f"  Tempo: {metricas['tempo_total']:.0f}s ({metricas['tempo_total']/60:.1f}min)", log_callback)
    _log(f"  Salvo: {resultado_final}", log_callback)
    _log("="*80, log_callback)
    
    return {'sucesso': True, 'resultado': estado_final, 'metricas': metricas}

# ═══════════════════════════════════════════════════════════════════════════════
# PARTE 6 — PERSISTÊNCIA EM POSTGRESQL
# ═══════════════════════════════════════════════════════════════════════════════
#
# 3 funções pra integrar com o banco:
#   - salvar_processamento(): INSERT em legislacao_processamentos
#   - consolidar_municipio_db(): aplica todas leis em ordem cronológica
#   - buscar_consolidado(): SELECT do estado atual
# ═══════════════════════════════════════════════════════════════════════════════

import psycopg2
from psycopg2.extras import Json, RealDictCursor


def _conectar_db():
    """Conecta no banco usando credentials do .env."""
    db_host = os.environ.get('DB_HOST', 'localhost')
    db_name = os.environ.get('DB_NAME', 'urbanlex')
    db_user = os.environ.get('DB_USER', 'urbanlex')
    db_pass = os.environ.get('DB_PASS', 'urbanlex123')
    return psycopg2.connect(host=db_host, database=db_name,
                             user=db_user, password=db_pass)


def calcular_md5_zip(zip_path):
    """
    Calcula MD5 de um arquivo ZIP. Usado pra cache inteligente.
    
    Retorna: string hexa de 32 chars, ou None se erro.
    """
    if not os.path.exists(zip_path):
        return None
    try:
        h = hashlib.md5()
        with open(zip_path, 'rb') as f:
            for chunk in iter(lambda: f.read(65536), b''):
                h.update(chunk)
        return h.hexdigest()
    except Exception as e:
        logger.error(f"calcular_md5_zip falhou: {e}")
        return None


def buscar_processamento_por_md5(municipio, estado_uf, zip_md5):
    """
    Busca processamento anterior com mesmo município E mesmo MD5.
    
    Retorna: dict do processamento (id, resultado_json, ...) ou None.
    """
    if not zip_md5:
        return None
    try:
        conn = _conectar_db()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT id, municipio, estado, resultado_json, metricas,
                   processado_em, sucesso, zip_md5
            FROM legislacao_processamentos
            WHERE municipio = %s 
              AND estado = %s 
              AND zip_md5 = %s
              AND sucesso = TRUE
            ORDER BY processado_em DESC
            LIMIT 1
        """, (municipio, estado_uf, zip_md5))
        row = cur.fetchone()
        cur.close()
        conn.close()
        return dict(row) if row else None
    except Exception as e:
        logger.error(f"buscar_processamento_por_md5 falhou: {e}")
        return None


def salvar_processamento(resultado_pipeline, legislacao_id=None,
                         processado_por=None, log_callback=None,
                         legislacao_label=None):
    """
    Salva o resultado do pipeline em legislacao_processamentos.
    
    Args:
        resultado_pipeline: dict retornado por processar_municipio()
        legislacao_id:      FK para legislacoes (opcional)
        processado_por:     FK para users (opcional)
        log_callback:       função(msg) opcional
    
    Retorna:
        id do registro criado, ou None se falhou
    """
    _log("Salvando processamento no banco...", log_callback)
    
    if not isinstance(resultado_pipeline, dict):
        _log("  ERRO: resultado_pipeline deve ser dict", log_callback)
        return None
    
    sucesso = resultado_pipeline.get('sucesso', False)
    metricas = resultado_pipeline.get('metricas', {})
    
    # Determina municipio/estado a partir de metricas ou resultado
    if sucesso:
        estado_extr = resultado_pipeline.get('resultado', {})
        leg = estado_extr.get('legislacao') or {}
        municipio = leg.get('municipio') or metricas.get('municipio')
        estado_uf = leg.get('estado') or metricas.get('estado')
    else:
        rp = resultado_pipeline.get('resultado_parcial', {}) or {}
        leg = rp.get('legislacao') or {}
        municipio = leg.get('municipio')
        estado_uf = leg.get('estado')
    
    if not municipio or not estado_uf:
        _log("  ERRO: municipio ou estado não identificados no resultado", log_callback)
        return None
    
    resultado_json = (resultado_pipeline.get('resultado')
                      if sucesso else resultado_pipeline.get('resultado_parcial'))
    
    # Calcula MD5 do ZIP se temos o path
    zip_path_local = metricas.get('zip_path')
    zip_md5 = calcular_md5_zip(zip_path_local) if zip_path_local else None
    
    try:
        conn = _conectar_db()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO legislacao_processamentos
                (legislacao_id, municipio, estado, resultado_json, metricas,
                 pipeline_versao, prompt_versao, sucesso, erro_etapa, erro_msg,
                 zip_path, zip_md5, output_dir, processado_por, legislacao_label)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            legislacao_id, municipio, estado_uf,
            Json(resultado_json), Json(metricas),
            '1.0', 'v13',
            sucesso,
            resultado_pipeline.get('erro_etapa'),
            resultado_pipeline.get('erro_msg'),
            zip_path_local,
            zip_md5,
            metricas.get('output_dir'),
            processado_por,
            legislacao_label,
        ))
        novo_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        _log(f"  Salvo em legislacao_processamentos.id={novo_id}", log_callback)
        return novo_id
    except Exception as e:
        _log(f"  ERRO ao salvar: {e}", log_callback)
        logger.error(f"salvar_processamento falhou: {e}\n{traceback.format_exc()}")
        return None


def consolidar_municipio_db(municipio, estado_uf, consolidado_por=None,
                            log_callback=None):
    """
    Consolida TODAS as leis processadas de um município em ordem cronológica.
    
    Lógica:
    1. SELECT todos legislacao_processamentos (sucesso=true) do município
    2. Ordena por data_publicacao da legislacao (mais antiga primeiro)
    3. Aplica merge sequencial: lei mais recente sobrepõe campos
    4. Gera audit_log: pra cada zona, lista quais leis a afetaram
    5. UPSERT em municipios_consolidado
    
    Retorna:
        dict com {sucesso, zonas, total_zonas, total_modificacoes, leis_aplicadas}
    """
    _log(f"Consolidando {municipio}/{estado_uf}...", log_callback)
    
    try:
        conn = _conectar_db()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Busca TODOS processamentos do municipio (com data da legislacao se houver)
        cur.execute("""
            SELECT lp.id, lp.legislacao_id, lp.resultado_json, lp.processado_em,
                   l.data_publicacao, l.ano
            FROM legislacao_processamentos lp
            LEFT JOIN legislacoes l ON lp.legislacao_id = l.id
            WHERE lp.municipio = %s AND lp.estado = %s AND lp.sucesso = TRUE
            ORDER BY COALESCE(l.data_publicacao, lp.processado_em::date) ASC,
                     lp.processado_em ASC
        """, (municipio, estado_uf))
        
        processamentos = cur.fetchall()
        if not processamentos:
            _log(f"  Nenhum processamento encontrado para {municipio}/{estado_uf}", log_callback)
            cur.close()
            conn.close()
            return {'sucesso': False, 'msg': 'sem processamentos'}
        
        _log(f"  Processamentos encontrados: {len(processamentos)}", log_callback)
        
        # Estado consolidado
        zonas_consolidadas = {}
        audit_log = {}  # {sigla: {campo: [leis_que_afetaram]}}
        legislacoes_aplicadas = []
        total_modificacoes = 0
        
        for p in processamentos:
            leg_id = p.get('legislacao_id')
            data_pub = p.get('data_publicacao') or p.get('processado_em')
            resultado = p.get('resultado_json') or {}
            
            if leg_id:
                legislacoes_aplicadas.append(leg_id)
            
            # Identifica a lei (pra audit_log)
            leg_info = resultado.get('legislacao') or {}
            label_lei = (f"{leg_info.get('tipo', 'Lei')} "
                         f"{leg_info.get('numero', '?')}/{leg_info.get('ano', '?')}")
            
            # Aplica cada zona
            for sigla, zona_nova in (resultado.get('zonas') or {}).items():
                if not isinstance(zona_nova, dict):
                    continue
                if sigla not in zonas_consolidadas:
                    zonas_consolidadas[sigla] = copy.deepcopy(zona_nova)
                    audit_log[sigla] = {'origem': label_lei}
                else:
                    # Merge profundo: lei mais recente sobrepõe
                    zona_atual = zonas_consolidadas[sigla]
                    for campo in ['usos_permitidos', 'parametros_gerais',
                                  'parametros_por_uso', 'variacoes',
                                  'acrescimos_extraordinarios', 'hierarquia',
                                  'metodologia_area_computavel',
                                  'afastamentos_crescentes']:
                        if zona_nova.get(campo) is not None:
                            # Lei mais recente: SOBREPÕE (não merge inteligente, sobrepõe direto)
                            zona_atual[campo] = zona_nova[campo]
                            # Registra no audit
                            audit_log.setdefault(sigla, {}).setdefault('alteracoes', []).append({
                                'lei': label_lei, 'campo': campo
                            })
            
            total_modificacoes += len(resultado.get('modificacoes') or [])
        
        # UPSERT em municipios_consolidado
        cur.execute("""
            INSERT INTO municipios_consolidado
                (municipio, estado, zonas_consolidadas, legislacoes_aplicadas,
                 audit_log, total_zonas, total_modificacoes, consolidado_por)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (municipio, estado) DO UPDATE SET
                zonas_consolidadas = EXCLUDED.zonas_consolidadas,
                legislacoes_aplicadas = EXCLUDED.legislacoes_aplicadas,
                audit_log = EXCLUDED.audit_log,
                total_zonas = EXCLUDED.total_zonas,
                total_modificacoes = EXCLUDED.total_modificacoes,
                consolidado_em = NOW(),
                consolidado_por = EXCLUDED.consolidado_por
            RETURNING id
        """, (
            municipio, estado_uf,
            Json(zonas_consolidadas),
            legislacoes_aplicadas or None,
            Json(audit_log),
            len(zonas_consolidadas),
            total_modificacoes,
            consolidado_por,
        ))
        
        mc_id = cur.fetchone()['id']
        conn.commit()
        cur.close()
        conn.close()
        
        _log(f"  Consolidado salvo (id={mc_id})", log_callback)
        _log(f"  Zonas: {len(zonas_consolidadas)} | Modificações: {total_modificacoes}", log_callback)
        _log(f"  Leis aplicadas: {len(legislacoes_aplicadas)}", log_callback)
        
        return {
            'sucesso': True,
            'consolidado_id': mc_id,
            'zonas': zonas_consolidadas,
            'total_zonas': len(zonas_consolidadas),
            'total_modificacoes': total_modificacoes,
            'leis_aplicadas': legislacoes_aplicadas,
            'audit_log': audit_log,
        }
    except Exception as e:
        _log(f"  ERRO ao consolidar: {e}", log_callback)
        logger.error(f"consolidar_municipio_db falhou: {e}\n{traceback.format_exc()}")
        return {'sucesso': False, 'msg': str(e)}


def buscar_consolidado(municipio, estado_uf):
    """
    Busca o estado consolidado de um município.
    
    Retorna:
        dict com {zonas, audit_log, total_zonas, ...} ou None se não existir
    """
    try:
        conn = _conectar_db()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT id, municipio, estado, zonas_consolidadas,
                   legislacoes_aplicadas, audit_log,
                   total_zonas, total_modificacoes,
                   consolidado_em
            FROM municipios_consolidado
            WHERE municipio = %s AND estado = %s
            LIMIT 1
        """, (municipio, estado_uf))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            return None
        return dict(row)
    except Exception as e:
        logger.error(f"buscar_consolidado falhou: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# PARTE 7 — FILA DE EXTRAÇÃO (enfileiramento)
# ═══════════════════════════════════════════════════════════════════════════════
#
# Função pra adicionar items na fila_extracao.
# O processamento em background é feito por fila_extracao_worker.py
# ═══════════════════════════════════════════════════════════════════════════════

def enfileirar_extracao(zip_path, municipio, estado_uf, legislacao_id=None,
                        usar_cache=True, consolidar_apos=True, ordem=0,
                        criado_por=None, legislacao_label=None):
    """
    Adiciona um item na fila_extracao para processamento em background.
    
    Args:
        zip_path:         caminho absoluto do ZIP com PDFs
        municipio:        nome do município
        estado_uf:        UF (2 letras)
        legislacao_id:    FK para legislacoes (opcional)
        usar_cache:       reaproveitar TXTs salvos
        consolidar_apos:  consolidar municipios_consolidado depois
        ordem:            prioridade (menor = mais cedo)
        criado_por:       FK users (opcional)
    
    Retorna:
        id do item criado, ou None se erro
    """
    if not os.path.exists(zip_path):
        logger.error(f"enfileirar_extracao: ZIP não existe: {zip_path}")
        return None
    
    try:
        conn = _conectar_db()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO fila_extracao
                (municipio, estado, zip_path, legislacao_id,
                 usar_cache, consolidar_apos, ordem, criado_por, legislacao_label)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (municipio, estado_uf, zip_path, legislacao_id,
              usar_cache, consolidar_apos, ordem, criado_por, legislacao_label))
        novo_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Item enfileirado: id={novo_id} ({municipio}/{estado_uf})")
        return novo_id
    except Exception as e:
        logger.error(f"enfileirar_extracao falhou: {e}")
        return None
