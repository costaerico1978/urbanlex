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
    cur.close()
    return dict(row) if row else None


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
        lei_banco = buscar_lei_no_banco(item['ref'], db_conn)
        relatorio.append({
            **item,
            'encontrada_no_banco': lei_banco is not None,
            'legislacao_id': lei_banco['id'] if lei_banco else None,
            'detalhes_banco': lei_banco,
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
    for i, item in enumerate(relatorio, 1):
        r = item['ref']
        status = "✅ ENCONTRADA" if item['encontrada_no_banco'] else "❌ NAO encontrada"
        print(f"\n{i}. {r['tipo_nome']} {r['numero']}/{r['ano']} → {status}")
        if item['detalhes_banco']:
            db = item['detalhes_banco']
            print(f"   id={db['id']} | data={db['data_publicacao']} | texto={db['texto_size']} chars")
            print(f"   ementa: {(db['ementa'] or '')[:80]}")
    conn.close()

    encontradas = sum(1 for r in relatorio if r['encontrada_no_banco'])
    print(f"\n=== RESUMO ===")
    print(f"  Total refs: {len(refs)}")
    print(f"  No banco: {encontradas}")
    print(f"  Faltam: {len(refs) - encontradas}")
