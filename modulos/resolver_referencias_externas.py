"""
resolver_referencias_externas.py

Modulo responsavel por:
1. Coletar referencias externas de todas as zonas de um resultado_final.json
2. Buscar essas leis no banco (tabela `legislacoes`)
3. Disparar pipeline secundario pra extrair subzonas (Fase B.2)
4. Mergear resultados no JSON principal (Fase B.3)

Fase B.1 (este commit): apenas funcoes 1 e 2.
"""
import os
import json
import logging
from typing import Dict, List, Optional, Tuple
from collections import defaultdict

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)


def coletar_refs_unicas(resultado_json: Dict) -> List[Dict]:
    """
    Coleta todas as `referencias_externas` das zonas e retorna lista DEDUPLICADA
    de leis referenciadas, com as zonas que apontam pra cada uma.

    Args:
        resultado_json: dict completo lido de resultado_final.json
                        (formato: {"estado": {"zonas": {...}, ...}, ...})

    Returns:
        Lista de dicts:
        [
          {
            "ref": {esfera, municipio, estado, tipo_nome, numero, ano, texto_original},
            "zonas_afetadas": ["ZPP|AP-4", ...],
            "parametros_pendentes": [...]  # union dos parametros_afetados
          },
          ...
        ]
    """
    estado = resultado_json.get('estado', {}) if 'estado' in resultado_json else resultado_json
    zonas = estado.get('zonas', {})

    # Agrupa por chave (esfera, tipo_nome, numero, ano)
    agrupado = defaultdict(lambda: {
        'ref': None,
        'zonas_afetadas': [],
        'parametros_pendentes': set(),
    })

    # zonas pode ser dict ou list
    if isinstance(zonas, dict):
        iterator = zonas.items()
    else:
        iterator = [(z.get('sigla_canonica', f'idx_{i}'), z) for i, z in enumerate(zonas)]

    # ----- 1. Refs no nivel ZONA (formato novo, pos-commit 5c80482) -----
    for chave_zona, z in iterator:
        if not isinstance(z, dict):
            continue
        refs = z.get('referencias_externas') or []
        if not refs:
            continue
        for ref_item in refs:
            if not isinstance(ref_item, dict):
                continue
            lei = ref_item.get('lei_referenciada') or {}
            if not isinstance(lei, dict):
                continue

            # Chave de agrupamento: esfera + tipo_nome + numero + ano (+ municipio se municipal)
            esfera = (lei.get('esfera') or '').lower()
            tipo_nome = lei.get('tipo_nome') or ''
            numero = str(lei.get('numero') or '').strip()
            ano = lei.get('ano')
            municipio = lei.get('municipio') or ''
            estado_uf = lei.get('estado') or ''

            chave_agg = (esfera, tipo_nome, numero, str(ano), municipio if esfera == 'municipal' else '', estado_uf)

            agg = agrupado[chave_agg]
            if agg['ref'] is None:
                agg['ref'] = {
                    'esfera': esfera,
                    'tipo_nome': tipo_nome,
                    'numero': numero,
                    'ano': ano,
                    'municipio': municipio,
                    'estado': estado_uf,
                    'texto_original': lei.get('texto_original'),
                }
            agg['zonas_afetadas'].append(chave_zona)
            params_afetados = ref_item.get('parametros_afetados') or []
            agg['parametros_pendentes'].update(params_afetados)

    # ----- 2. Refs no nivel LEGISLACAO (formato antigo agregado) -----
    # estado.legislacao.referencias_externas: lista de {tipo, numero, ano, contexto}
    refs_alto = (estado.get('legislacao') or {}).get('referencias_externas') or []
    municipio_leg = (estado.get('legislacao') or {}).get('municipio') or ''
    estado_leg = (estado.get('legislacao') or {}).get('estado') or ''
    for ref_item in refs_alto:
        if not isinstance(ref_item, dict):
            continue
        # Pode estar em formato simples (tipo+numero+ano direto) ou com lei_referenciada
        if 'lei_referenciada' in ref_item:
            lei = ref_item['lei_referenciada'] or {}
        else:
            # Formato antigo: tipo/numero/ano no proprio item
            lei = {
                'tipo_nome': ref_item.get('tipo'),
                'numero': ref_item.get('numero'),
                'ano': ref_item.get('ano'),
                'esfera': 'municipal' if municipio_leg else None,
                'municipio': municipio_leg,
                'estado': estado_leg,
                'texto_original': ref_item.get('contexto'),
            }
        if not lei.get('numero') or not lei.get('ano'):
            continue
        esfera = (lei.get('esfera') or '').lower()
        tipo_nome = lei.get('tipo_nome') or ''
        numero = str(lei.get('numero') or '').strip()
        ano = lei.get('ano')
        municipio = lei.get('municipio') or municipio_leg
        estado_uf = lei.get('estado') or estado_leg
        chave_agg = (esfera, tipo_nome, numero, str(ano), municipio if esfera == 'municipal' else '', estado_uf)
        agg = agrupado[chave_agg]
        if agg['ref'] is None:
            agg['ref'] = {
                'esfera': esfera,
                'tipo_nome': tipo_nome,
                'numero': numero,
                'ano': ano,
                'municipio': municipio,
                'estado': estado_uf,
                'texto_original': lei.get('texto_original') or ref_item.get('contexto'),
            }
        # Nao tem zona especifica nesse formato; nao adiciona zonas_afetadas

    # Converte set pra lista
    resultado = []
    for k, agg in agrupado.items():
        resultado.append({
            'ref': agg['ref'],
            'zonas_afetadas': agg['zonas_afetadas'],
            'parametros_pendentes': sorted(agg['parametros_pendentes']),
        })

    return resultado


def buscar_lei_no_banco(ref: Dict, db_conn) -> Optional[Dict]:
    """
    Busca uma lei referenciada no banco usando os campos do `ref`.

    Args:
        ref: dict com esfera, tipo_nome, numero, ano, municipio, estado
        db_conn: conexao psycopg2 ao banco

    Returns:
        dict com dados da legislacao encontrada, ou None se nao existir.
        Campos retornados: id, esfera, municipio_nome, estado, tipo_nome,
                          numero, ano, ementa, data_publicacao, status,
                          arquivo_url, conteudo_texto (truncado), texto_integral_size
    """
    cur = db_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Monta WHERE baseado em esfera
    where_clauses = ["tipo_nome = %s", "numero = %s", "ano = %s"]
    params = [ref.get('tipo_nome'), str(ref.get('numero')), ref.get('ano')]

    esfera = (ref.get('esfera') or '').lower()
    if esfera:
        where_clauses.append("LOWER(esfera) = %s")
        params.append(esfera)

    if esfera == 'municipal':
        # Match por municipio + estado
        if ref.get('municipio'):
            where_clauses.append("municipio_nome = %s")
            params.append(ref.get('municipio'))
        if ref.get('estado'):
            where_clauses.append("estado = %s")
            params.append(ref.get('estado'))
    elif esfera == 'estadual':
        if ref.get('estado'):
            where_clauses.append("estado = %s")
            params.append(ref.get('estado'))

    sql = f"""
        SELECT id, esfera, municipio_nome, estado, tipo_nome, numero, ano,
               ementa, data_publicacao, status, arquivo_url,
               LENGTH(COALESCE(texto_integral, conteudo_texto, '')) AS texto_size
        FROM legislacoes
        WHERE {' AND '.join(where_clauses)}
        ORDER BY id DESC
        LIMIT 1
    """

    cur.execute(sql, params)
    row = cur.fetchone()
    if row:
        cur.close()
        return dict(row)

    # FALLBACK: procurar tambem em dossie_legislacoes_pasta (leis trazidas pelo buscador)
    # Match por legislacao_meta JSONB: tipo + numero + ano
    cur.execute("""
        SELECT dlp.id AS dossie_pasta_id, dlp.dossie_id, dlp.legislacao_label,
               dlp.legislacao_meta, dlp.pdf_concatenado_path
        FROM dossie_legislacoes_pasta dlp
        WHERE LOWER(COALESCE(dlp.legislacao_meta->>'tipo', dlp.legislacao_meta->>'tipo_nome', '')) = LOWER(%s)
          AND (dlp.legislacao_meta->>'numero') = %s
          AND (dlp.legislacao_meta->>'ano') = %s
        ORDER BY dlp.id DESC
        LIMIT 1
    """, (ref.get('tipo_nome') or '', str(ref.get('numero') or ''), str(ref.get('ano') or '')))
    drow = cur.fetchone()
    cur.close()
    if drow:
        # Retorna um dict sintetico simulando uma legislacao
        return {
            'id': None,  # nao tem id em legislacoes
            'fonte': 'dossie_pasta',
            'dossie_pasta_id': drow['dossie_pasta_id'],
            'dossie_id': drow['dossie_id'],
            'legislacao_label': drow['legislacao_label'],
            'esfera': ref.get('esfera'),
            'municipio_nome': ref.get('municipio'),
            'estado': ref.get('estado'),
            'tipo_nome': ref.get('tipo_nome'),
            'numero': str(ref.get('numero')),
            'ano': ref.get('ano'),
            'ementa': (drow['legislacao_meta'] or {}).get('descricao', ''),
            'data_publicacao': None,
            'status': (drow['legislacao_meta'] or {}).get('status', 'vigente'),
            'arquivo_url': (drow['legislacao_meta'] or {}).get('link'),
            'texto_size': 0,
            'pdf_concatenado_path': drow['pdf_concatenado_path'],
        }
    return None


def buscar_processamento_da_lei(ref: Dict, db_conn) -> Optional[Dict]:
    """
    Verifica se existe processamento BEM-SUCEDIDO desta lei externa em
    `legislacao_processamentos`, matchando pelo conteudo do resultado_json:
    estado.legislacao.{tipo, numero, ano}.

    Returns:
        {id, processado_em, municipio, output_dir} ou None se nao existir.
    """
    cur = db_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    sql = """
        SELECT id, processado_em, municipio, estado, output_dir
        FROM legislacao_processamentos
        WHERE sucesso=TRUE
          AND resultado_json -> 'estado' -> 'legislacao' ->> 'tipo' = %s
          AND resultado_json -> 'estado' -> 'legislacao' ->> 'numero' = %s
          AND (resultado_json -> 'estado' -> 'legislacao' ->> 'ano')::int = %s
        ORDER BY id DESC
        LIMIT 1
    """
    cur.execute(sql, (ref.get('tipo_nome'), str(ref.get('numero')), int(ref.get('ano'))))
    row = cur.fetchone()
    cur.close()
    return dict(row) if row else None


def status_lei_externa(ref: Dict, db_conn) -> Dict:
    """
    Determina o status da lei externa referenciada:
      - 'vermelho': lei AUSENTE em `legislacoes`
      - 'amarelo': existe em `legislacoes` mas sem JSON processado
      - 'verde': existe + tem processamento com sucesso

    Returns:
        {
          'status': 'vermelho'|'amarelo'|'verde',
          'legislacao_id': int|None,    # id em `legislacoes`
          'processamento_id': int|None, # id em `legislacao_processamentos`
          'detalhes_banco': dict|None,
          'detalhes_processamento': dict|None,
        }
    """
    legislacao = buscar_lei_no_banco(ref, db_conn)
    if not legislacao:
        return {
            'status': 'vermelho',
            'legislacao_id': None,
            'processamento_id': None,
            'detalhes_banco': None,
            'detalhes_processamento': None,
        }
    processamento = buscar_processamento_da_lei(ref, db_conn)
    if not processamento:
        return {
            'status': 'amarelo',
            'legislacao_id': legislacao['id'],
            'processamento_id': None,
            'detalhes_banco': legislacao,
            'detalhes_processamento': None,
        }
    return {
        'status': 'verde',
        'legislacao_id': legislacao['id'],
        'processamento_id': processamento['id'],
        'detalhes_banco': legislacao,
        'detalhes_processamento': processamento,
    }


def relatorio_refs(resultado_json: Dict, db_conn) -> List[Dict]:
    """
    Gera relatorio de todas as referencias externas:
    - Quais leis sao referenciadas
    - Quais zonas as referenciam
    - Quais existem no banco (resolviveis automaticamente)
    - Quais NAO existem (precisam de intervencao manual)
    """
    refs_unicas = coletar_refs_unicas(resultado_json)
    relatorio = []
    for item in refs_unicas:
        st = status_lei_externa(item['ref'], db_conn)
        relatorio.append({
            **item,
            'status': st['status'],   # 'vermelho'|'amarelo'|'verde'
            'encontrada_no_banco': st['legislacao_id'] is not None,
            'legislacao_id': st['legislacao_id'],
            'processamento_id': st['processamento_id'],
            'detalhes_banco': st['detalhes_banco'],
            'detalhes_processamento': st['detalhes_processamento'],
        })
    return relatorio


# ─── TESTE ad-hoc quando executado standalone ────────────────────
if __name__ == '__main__':
    import sys
    from dotenv import load_dotenv

    load_dotenv('/var/www/urbanlex/.env')

    # Aceita caminho do JSON via argv
    if len(sys.argv) < 2:
        print("Uso: python3 resolver_referencias_externas.py <caminho/para/resultado_final.json>")
        sys.exit(1)
    json_path = sys.argv[1]
    if not os.path.exists(json_path):
        print(f"ERRO: {json_path} nao existe")
        sys.exit(1)

    with open(json_path, 'r', encoding='utf-8') as f:
        d = json.load(f)

    print(f"\n=== Coletando referencias externas ===")
    refs = coletar_refs_unicas(d)
    print(f"Referencias unicas encontradas: {len(refs)}\n")

    for i, item in enumerate(refs, 1):
        r = item['ref']
        print(f"{i}. {r['tipo_nome']} {r['numero']}/{r['ano']} ({r['esfera']})")
        if r.get('municipio'):
            print(f"   municipio: {r['municipio']}/{r['estado']}")
        print(f"   zonas afetadas ({len(item['zonas_afetadas'])}): {item['zonas_afetadas'][:5]}{'...' if len(item['zonas_afetadas']) > 5 else ''}")
        print(f"   parametros pendentes: {len(item['parametros_pendentes'])} ({item['parametros_pendentes'][:3]}...)")

    # Busca cada uma no banco
    print(f"\n=== Buscando no banco ===")
    conn = psycopg2.connect(os.environ['DATABASE_URL'])
    relatorio = relatorio_refs(d, conn)
    icones = {'vermelho': '🔴', 'amarelo': '🟡', 'verde': '🟢'}
    for i, item in enumerate(relatorio, 1):
        r = item['ref']
        ico = icones.get(item['status'], '?')
        print(f"\n{i}. {ico} {r['tipo_nome']} {r['numero']}/{r['ano']} ({item['status'].upper()})")
        if item['detalhes_banco']:
            db = item['detalhes_banco']
            print(f"   legislacao_id={db['id']} | texto={db['texto_size']} chars | data={db['data_publicacao']}")
        if item['detalhes_processamento']:
            p = item['detalhes_processamento']
            print(f"   processamento_id={p['id']} | processado_em={p['processado_em']} | output_dir={p.get('output_dir')}")
    conn.close()

    n_verm = sum(1 for r in relatorio if r['status'] == 'vermelho')
    n_amar = sum(1 for r in relatorio if r['status'] == 'amarelo')
    n_verd = sum(1 for r in relatorio if r['status'] == 'verde')
    print(f"\n=== RESUMO ===")
    print(f"  Total refs: {len(refs)}")
    print(f"  🔴 vermelho (ausente no urbanlex): {n_verm}")
    print(f"  🟡 amarelo (no banco, sem JSON):    {n_amar}")
    print(f"  🟢 verde (com JSON gerado):         {n_verd}")
