# -*- coding: utf-8 -*-
"""
modulos/mesclar_leis_externas.py - Fase B.7

Mescla leis externas referenciadas em zonas da lei principal.

Contexto:
- Quando uma lei principal (ex: LC 270/2024 do Rio) tem uma zona como ZPP|AP4
  que diz 'parametros conforme Dec 3046/1981', a extracao marca
  NI_LEI_EXTERNA nessa zona e adiciona em 'referencias_externas'.
- Quando o JSON da lei externa (Dec 3046) tambem esta entre os JSONs
  selecionados pra geracao da planilha, podemos COMBINAR: pegar as
  subzonas que a externa define e renomeia-las concatenando com a zona pai
  da principal.

Exemplo:
  Principal LC 270: zonas = {'ZPP|AP4': {referencias_externas: [{lei: Dec 3046}]}}
  Externa Dec 3046: zonas = {'A-1': {...}, 'A-4': {...}, 'A-7': {...}}

  Apos merge na principal:
  zonas = {
    'ZPP|AP4': {marcado como expandido},
    'ZPP|AP4|A-1': {dados de A-1},
    'ZPP|AP4|A-4': {dados de A-4},
    'ZPP|AP4|A-7': {dados de A-7},
  }

Uso:
  jsons_carregados = preenchedor_planilha.carregar_jsons(ids, get_db)
  jsons_mesclados = mesclar_leis_externas(jsons_carregados)
  # ... segue pra consolidar normalmente
"""
import copy
import logging
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


def _normalizar_chave(s) -> str:
    """Normaliza string pra comparacao (lowercase, sem espacos extras)."""
    return str(s or '').strip().lower()


def _extrair_chave_lei(lei_ref: Dict) -> Optional[Tuple]:
    """
    Cria chave canonica de uma lei pra comparacao.
    Retorna (tipo_lowercase, numero, ano) ou None se faltar info.
    """
    if not isinstance(lei_ref, dict):
        return None
    tipo = _normalizar_chave(lei_ref.get('tipo_nome') or lei_ref.get('tipo'))
    numero = str(lei_ref.get('numero') or '').strip()
    ano = str(lei_ref.get('ano') or '').strip()
    if not tipo or not numero or not ano:
        return None
    return (tipo, numero, ano)


def _coletar_refs_externas_zona(zona_dados: Dict) -> List[Dict]:
    """
    Coleta TODAS as referencias externas dentro de uma zona da lei principal.
    Suporta os 2 formatos:
      - novo (pos-PARTE 0): referencias_externas dentro da zona
      - obs: o formato antigo (refs no nivel legislacao) NAO pode ser
        mesclado aqui pq nao se sabe qual zona referencia qual lei.
    """
    if not isinstance(zona_dados, dict):
        return []
    refs = zona_dados.get('referencias_externas') or []
    if not isinstance(refs, list):
        return []
    return [r for r in refs if isinstance(r, dict)]


def _indexar_leis_carregadas(jsons_carregados: List[Dict]) -> Dict[Tuple, Dict]:
    """
    Cria indice {chave_lei: json_carregado} pra busca rapida.
    chave_lei = (tipo, numero, ano) extraido de estado.legislacao
    """
    indice = {}
    for j in jsons_carregados:
        data = j.get('data') or {}
        leg_meta = (data.get('estado') or {}).get('legislacao') or {}
        # Pode ter 'tipo' (antigo) ou 'tipo_nome' (novo)
        ref = {
            'tipo_nome': leg_meta.get('tipo_nome') or leg_meta.get('tipo'),
            'numero': leg_meta.get('numero'),
            'ano': leg_meta.get('ano'),
        }
        chave = _extrair_chave_lei(ref)
        if chave:
            indice[chave] = j
    return indice


def _gerar_nova_chave_zona(zona_pai: str, sigla_filha: str) -> str:
    """
    Combina o nome da zona pai com o da filha.
    Exemplo:
      _gerar_nova_chave_zona('ZPP|AP4', 'A-1') -> 'ZPP|AP4|A-1'
      _gerar_nova_chave_zona('ZPP', 'A-1') -> 'ZPP|A-1'
    """
    zona_pai = str(zona_pai or '').strip()
    sigla_filha = str(sigla_filha or '').strip()
    if not zona_pai:
        return sigla_filha
    if not sigla_filha:
        return zona_pai
    return f"{zona_pai}|{sigla_filha}"


def mesclar_leis_externas(jsons_carregados: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
    """
    Para cada JSON na lista, varre suas zonas procurando referencias_externas.
    Se a lei referenciada tambem esta entre os JSONs carregados:
      - Pega as zonas da lei externa
      - Mescla na lei principal como subzonas hierarquicas
        (renomeia chave: 'ZPP|AP4' + 'A-1' -> 'ZPP|AP4|A-1')

    Returns:
        (jsons_mesclados, log_merges)
        log_merges = lista de {lei_pai, zona_pai, lei_externa, subzonas_adicionadas}
    """
    # Faz copia profunda pra nao mexer no original
    jsons = copy.deepcopy(jsons_carregados)
    indice = _indexar_leis_carregadas(jsons)
    log_merges = []

    for j in jsons:
        data = j.get('data') or {}
        estado = data.get('estado') or {}
        zonas = estado.get('zonas') or {}
        leg_meta_principal = estado.get('legislacao') or {}
        rotulo_principal = j.get('legislacao_label') or _extrair_chave_lei({
            'tipo_nome': leg_meta_principal.get('tipo_nome') or leg_meta_principal.get('tipo'),
            'numero': leg_meta_principal.get('numero'),
            'ano': leg_meta_principal.get('ano'),
        })

        # Itera sobre zonas (copia das chaves pq vamos modificar)
        chaves_zonas_originais = list(zonas.keys()) if isinstance(zonas, dict) else []

        for chave_zona in chaves_zonas_originais:
            zona = zonas.get(chave_zona)
            if not isinstance(zona, dict):
                continue
            refs = _coletar_refs_externas_zona(zona)
            if not refs:
                continue

            for ref_item in refs:
                # Pega a lei referenciada (suporta {lei_referenciada: {...}} ou flat)
                if 'lei_referenciada' in ref_item:
                    lei_ref = ref_item['lei_referenciada'] or {}
                else:
                    lei_ref = ref_item
                chave_ext = _extrair_chave_lei(lei_ref)
                if not chave_ext:
                    continue

                lei_externa_json = indice.get(chave_ext)
                if not lei_externa_json:
                    # Lei externa nao esta entre os JSONs selecionados, skip
                    continue

                # Pega zonas da lei externa
                zonas_externa = ((lei_externa_json.get('data') or {})
                                  .get('estado') or {}).get('zonas') or {}

                if not zonas_externa or not isinstance(zonas_externa, dict):
                    continue

                subzonas_adicionadas = []
                for sigla_ext, dados_ext in zonas_externa.items():
                    if not isinstance(dados_ext, dict):
                        continue
                    # Gera nova chave: 'ZPP|AP4|A-1'
                    nova_chave = _gerar_nova_chave_zona(chave_zona, sigla_ext)

                    # Evita sobrescrever se ja existe (pode acontecer em re-merge)
                    if nova_chave in zonas:
                        logger.warning(
                            f"Zona '{nova_chave}' ja existe ao mesclar "
                            f"{chave_ext} em {chave_zona}, pulando"
                        )
                        continue

                    # Copia subzona profundamente + acrescenta marcadores
                    subzona_copy = copy.deepcopy(dados_ext)
                    # Anota a origem do merge
                    subzona_copy['_origem_merge'] = {
                        'zona_pai': chave_zona,
                        'lei_origem': dict(lei_ref),
                        'sigla_original': sigla_ext,
                    }
                    zonas[nova_chave] = subzona_copy
                    subzonas_adicionadas.append(nova_chave)

                # Marca a zona pai como "expandida via merge"
                if subzonas_adicionadas:
                    zona['_expandida_por_merge'] = {
                        'lei_externa': dict(lei_ref),
                        'subzonas_geradas': subzonas_adicionadas,
                    }
                    log_merges.append({
                        'lei_pai': rotulo_principal,
                        'zona_pai': chave_zona,
                        'lei_externa': dict(lei_ref),
                        'subzonas_adicionadas': subzonas_adicionadas,
                    })
                    logger.info(
                        f"Merge: {rotulo_principal} zona '{chave_zona}' expandida "
                        f"via {chave_ext} -> {len(subzonas_adicionadas)} subzonas"
                    )

        # Atualiza zonas no JSON
        estado['zonas'] = zonas
        data['estado'] = estado
        j['data'] = data

    return jsons, log_merges


if __name__ == '__main__':
    """Smoke test rapido."""
    import json
    # Cria 2 jsons falsos pra testar
    principal = {
        'proc_id': 1,
        'legislacao_label': 'LC 270/2024',
        'data': {
            'estado': {
                'legislacao': {'tipo_nome': 'Lei Complementar', 'numero': '270', 'ano': '2024'},
                'zonas': {
                    'ZPP|AP4': {
                        'descricao': 'Zona Preservacao Predio AP4',
                        'referencias_externas': [{
                            'lei_referenciada': {
                                'tipo_nome': 'Decreto', 'numero': '3046', 'ano': '1981'
                            }
                        }]
                    }
                }
            }
        }
    }
    externa = {
        'proc_id': 2,
        'legislacao_label': 'Dec 3046/1981',
        'data': {
            'estado': {
                'legislacao': {'tipo_nome': 'Decreto', 'numero': '3046', 'ano': '1981'},
                'zonas': {
                    'A-1': {'CAM': 1.25, 'TO': 50},
                    'A-4': {'CAM': 1.25, 'TO': 50},
                    'A-7': {'CAM': 0, 'TO': 0},
                }
            }
        }
    }
    jsons_mesclados, log = mesclar_leis_externas([principal, externa])
    print("=== JSON principal apos merge ===")
    zonas_pp = jsons_mesclados[0]['data']['estado']['zonas']
    for k in zonas_pp:
        print(f"  {k}")
    print(f"\n=== Log de merges ({len(log)}) ===")
    for m in log:
        print(f"  {m['zona_pai']} + {m['lei_externa']['tipo_nome']} {m['lei_externa']['numero']}/{m['lei_externa']['ano']} -> {len(m['subzonas_adicionadas'])} subzonas")
