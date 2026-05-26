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



def _extrair_subzonas_de_texto(texto):
    """Extrai siglas tipo 'A-4', 'A4', 'Subzona A 5' do texto livre.
    Retorna lista de siglas normalizadas (lowercase, sem hifen)."""
    import re
    if not texto:
        return []
    # Padrao: letra + opcional hifen/espaco + digito(s)
    # Ex: 'A-4', 'A4', 'A 5', 'Subzona A-12'
    # Mas evita capturar 'Art. 1', 'Cap.III' etc
    # Estrategia: procura 'A' (maiusculo ou nao) seguido de opcional separador e numero
    pattern = re.compile(r'\b([A-Z]+)\s*[-\s]?\s*(\d{1,3})\b')
    siglas = set()
    for m in pattern.finditer(texto):
        letra = m.group(1).upper()
        numero = m.group(2)
        # Filtro: ignora prefixos que nao sao zonas
        if letra in ('ART', 'CAP', 'INC', 'I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X'):
            continue
        # Tamanho razoavel de sigla (1-4 letras)
        if len(letra) > 4:
            continue
        sigla = f"{letra}{numero}".lower()
        siglas.add(sigla)
    return sorted(siglas)


def _coletar_revogacoes_zonas_externas(jsons_carregados: List[Dict]) -> set:
    """
    Varre todos os JSONs carregados procurando em estado.legislacao.revogacoes_zonas_externas
    e retorna set de chaves (tipo_lower, numero, ano, sigla_zona) pra match O(1) durante o merge.

    A sigla_zona vem normalizada (lowercase, sem espacos).
    """
    revogacoes = set()
    for j in jsons_carregados:
        leg = ((j.get('data') or {}).get('estado') or {}).get('legislacao') or {}
        revogs = leg.get('revogacoes_zonas_externas') or []
        if not isinstance(revogs, list):
            continue
        for item in revogs:
            if not isinstance(item, dict):
                continue
            lei_origem = item.get('lei_origem') or {}
            chave_lei = _extrair_chave_lei(lei_origem)
            if not chave_lei:
                continue
            # v15: usa hierarquia_zona (dict) - extrai UT mais profunda
            # v14 compat: aceita sigla_zona (string) tambem
            sigla = ''
            hier_zona = item.get('hierarquia_zona')
            if isinstance(hier_zona, dict):
                sigla = _ut_mais_profunda(hier_zona).lower()
            if not sigla:
                sigla = (item.get('sigla_zona') or '').strip().lower()
            if not sigla:
                continue
            # Chave de match: (tipo, numero, ano, sigla)
            revogacoes.add((chave_lei[0], chave_lei[1], chave_lei[2], sigla))
    # FALLBACK v15: tambem le overrides_de_leis_anteriores com tipo_alteracao=revogacao*
    # (caso o Sonnet tenha posto revogacoes de zonas no campo errado)
    for j in jsons_carregados:
        leg = ((j.get('data') or {}).get('estado') or {}).get('legislacao') or {}
        overs = leg.get('overrides_de_leis_anteriores') or []
        if not isinstance(overs, list):
            continue
        for item in overs:
            if not isinstance(item, dict):
                continue
            tipo_alt = str(item.get('tipo_alteracao') or '').lower()
            if not tipo_alt.startswith('revogacao'):
                continue
            disp = str(item.get('dispositivo_alterado') or '')
            # So considera se mencionar subzona/zona (evita confundir com revogacao de artigos)
            if not any(s in disp.lower() for s in ['subzona', 'zona ', 'zonas']):
                continue
            # Lei alterada (pode estar como tipo ou tipo_nome)
            lei_alt = item.get('lei_alterada') or {}
            chave_lei = _extrair_chave_lei({
                'tipo_nome': lei_alt.get('tipo_nome') or lei_alt.get('tipo'),
                'numero': lei_alt.get('numero'),
                'ano': lei_alt.get('ano'),
            })
            if not chave_lei:
                continue
            # Extrai siglas do dispositivo (texto livre)
            siglas = _extrair_subzonas_de_texto(disp)
            for sigla in siglas:
                revogacoes.add((chave_lei[0], chave_lei[1], chave_lei[2], sigla))
                logger.info(f"FALLBACK revogacao via overrides: {chave_lei} subzona {sigla}")
    return revogacoes


def _normalizar_sigla_p8(s: str) -> str:
    """Normaliza sigla pra comparacao: maiuscula, sem hifen, sem espaco."""
    if not s:
        return ''
    return ''.join(c for c in str(s).upper() if c.isalnum())


def _ut_mais_profunda(hierarquia: Dict) -> str:
    """Retorna o valor da UT preenchida mais profunda em uma hierarquia."""
    if not isinstance(hierarquia, dict):
        return ''
    for ut in ['UT7', 'UT6', 'UT5', 'UT4', 'UT3', 'UT2', 'UT1']:
        v = hierarquia.get(ut)
        if v:
            return str(v).strip()
    return ''


def _filtrar_zonas_aplicaveis(zonas_externa: Dict, subzonas_aplicaveis: List[str]) -> Dict:
    """
    Filtra dict de zonas externas mantendo so as que estao em subzonas_aplicaveis.
    Match feito pela UT mais profunda da hierarquia (v15) ou pela chave do dict (fallback).
    Se subzonas_aplicaveis contem '*' ou eh vazio/None, mantem TODAS.
    """
    if not subzonas_aplicaveis or '*' in subzonas_aplicaveis:
        return zonas_externa
    aplicaveis_norm = {_normalizar_sigla_p8(s) for s in subzonas_aplicaveis}
    resultado = {}
    for chave_zona, dados in zonas_externa.items():
        if not isinstance(dados, dict):
            continue
        # v15: usa UT mais profunda da hierarquia
        hier = dados.get('hierarquia') or {}
        sigla_via_hier = _ut_mais_profunda(hier)
        # Fallback: chave do dict (v14 compat)
        sigla_canonica = dados.get('sigla_canonica') or sigla_via_hier or chave_zona
        if _normalizar_sigla_p8(sigla_canonica) in aplicaveis_norm:
            resultado[chave_zona] = dados
    return resultado


def _ordenar_cronologicamente(jsons: List[Dict]) -> List[Dict]:
    """Ordena jsons por ano desc (mais recente primeiro)."""
    def chave_data(j):
        leg = ((j.get('data') or {}).get('estado') or {}).get('legislacao') or {}
        ano = leg.get('ano') or '0'
        try:
            return int(ano)
        except (ValueError, TypeError):
            return 0
    return sorted(jsons, key=chave_data, reverse=True)


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
    jsons = _ordenar_cronologicamente(jsons)  # mais recente primeiro
    indice = _indexar_leis_carregadas(jsons)
    revogacoes_set = _coletar_revogacoes_zonas_externas(jsons)
    log_merges = []
    if revogacoes_set:
        logger.info(f"Coletadas {len(revogacoes_set)} revogacoes de zonas externas que serao puladas no merge")

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
                # GUARD: pula refs auto-referentes (lei externa == lei principal)
                chave_propria = _extrair_chave_lei({
                    'tipo_nome': leg_meta_principal.get('tipo_nome') or leg_meta_principal.get('tipo'),
                    'numero': leg_meta_principal.get('numero'),
                    'ano': leg_meta_principal.get('ano'),
                })
                if chave_propria and chave_propria == chave_ext:
                    logger.warning(f"Pulando ref auto-referente: zona '{chave_zona}' aponta pra propria lei {chave_ext}")
                    continue

                # Pega zonas da lei externa
                zonas_externa = ((lei_externa_json.get('data') or {})
                                  .get('estado') or {}).get('zonas') or {}

                if not zonas_externa or not isinstance(zonas_externa, dict):
                    continue
                # Filtra por subzonas_aplicaveis se especificado na ref
                subzonas_aplicaveis = ref_item.get('subzonas_aplicaveis')
                if subzonas_aplicaveis:
                    antes = len(zonas_externa)
                    zonas_externa = _filtrar_zonas_aplicaveis(zonas_externa, subzonas_aplicaveis)
                    if antes != len(zonas_externa):
                        logger.info(f"Filtrou {chave_ext}: {antes} -> {len(zonas_externa)} subzonas (aplicaveis: {subzonas_aplicaveis})")

                subzonas_adicionadas = []
                zonas_externa_puladas_por_revogacao = []
                for sigla_ext, dados_ext in list(zonas_externa.items()):
                    if not isinstance(dados_ext, dict):
                        continue
                    # Verifica se esta zona externa foi revogada
                    # v15: compara pelo ultimo segmento da chave (ex: 'ZE5|A4' -> 'a4')
                    sigla_norm = str(sigla_ext or '').strip().split('|')[-1].lower()
                    chave_rev = (chave_ext[0], chave_ext[1], chave_ext[2], sigla_norm)
                    if chave_rev in revogacoes_set:
                        zonas_externa_puladas_por_revogacao.append(sigla_ext)
                        logger.info(
                            f"Pulando zona '{sigla_ext}' de {chave_ext} no merge "
                            f"(REVOGADA conforme alguma lei do conjunto)"
                        )
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
                    # v15: concatenar hierarquia da pai com hierarquia da subzona externa
                    hier_pai = zona.get('hierarquia') or {}
                    hier_ext = subzona_copy.get('hierarquia') or {}
                    hier_nova = {}
                    idx = 1
                    for ut in ['UT1','UT2','UT3','UT4','UT5','UT6','UT7']:
                        v = hier_pai.get(ut)
                        if v:
                            hier_nova[f'UT{idx}'] = v
                            idx += 1
                    for ut in ['UT1','UT2','UT3','UT4','UT5','UT6','UT7']:
                        v = hier_ext.get(ut)
                        if v and idx <= 7:
                            hier_nova[f'UT{idx}'] = v
                            idx += 1
                    for n in range(idx, 8):
                        hier_nova[f'UT{n}'] = None
                    subzona_copy['hierarquia'] = hier_nova
                    # Anota a origem do merge
                    subzona_copy['_origem_merge'] = {
                        'zona_pai': chave_zona,
                        'hierarquia_pai': hier_pai,
                        'lei_origem': dict(lei_ref),
                        'sigla_original': sigla_ext,
                    }
                    zonas[nova_chave] = subzona_copy
                    subzonas_adicionadas.append(nova_chave)

                # Marca a zona pai como "expandida via merge"
                if subzonas_adicionadas or zonas_externa_puladas_por_revogacao:
                    zona['_expandida_por_merge'] = {
                        'lei_externa': dict(lei_ref),
                        'subzonas_geradas': subzonas_adicionadas,
                        'subzonas_puladas_por_revogacao': zonas_externa_puladas_por_revogacao,
                    }
                    log_merges.append({
                        'lei_pai': rotulo_principal,
                        'zona_pai': chave_zona,
                        'lei_externa': dict(lei_ref),
                        'subzonas_adicionadas': subzonas_adicionadas,
                        'subzonas_puladas_por_revogacao': zonas_externa_puladas_por_revogacao,
                    })
                    logger.info(
                        f"Merge: {rotulo_principal} zona '{chave_zona}' expandida "
                        f"via {chave_ext} -> {len(subzonas_adicionadas)} subzonas"
                    )

        # Atualiza zonas no JSON
        estado['zonas'] = zonas
        data['estado'] = estado
        j['data'] = data

    # v15: pos-processamento - remove zonas das leis EXTERNAS que ja foram
    # mescladas em outras leis principais (evita duplicacao na planilha final)
    # Coleta sigla+lei das zonas mescladas
    siglas_mescladas = {}  # {chave_lei: set(siglas)}
    for j in jsons:
        zonas = ((j.get('data') or {}).get('estado') or {}).get('zonas') or {}
        for chave_z, z in zonas.items():
            origem = z.get('_origem_merge')
            if not origem:
                continue
            lei_orig = origem.get('lei_origem') or {}
            chave_lei = _extrair_chave_lei({
                'tipo_nome': lei_orig.get('tipo_nome') or lei_orig.get('tipo'),
                'numero': lei_orig.get('numero'),
                'ano': lei_orig.get('ano'),
            })
            if not chave_lei:
                continue
            # Sigla original (ultimo segmento da chave da zona externa)
            sigla_orig = str(origem.get('sigla_original') or '').strip().split('|')[-1].lower()
            if sigla_orig:
                siglas_mescladas.setdefault(chave_lei, set()).add(sigla_orig)
    # Remove das leis externas as zonas ja mescladas
    n_removidas = 0
    for j in jsons:
        leg = ((j.get('data') or {}).get('estado') or {}).get('legislacao') or {}
        chave_propria = _extrair_chave_lei({
            'tipo_nome': leg.get('tipo_nome') or leg.get('tipo'),
            'numero': leg.get('numero'),
            'ano': leg.get('ano'),
        })
        if not chave_propria:
            continue
        siglas_remover = siglas_mescladas.get(chave_propria, set())
        if not siglas_remover:
            continue
        zonas = ((j.get('data') or {}).get('estado') or {}).get('zonas') or {}
        if not isinstance(zonas, dict):
            continue
        for chave_z in list(zonas.keys()):
            sigla_ult = chave_z.split('|')[-1].lower()
            if sigla_ult in siglas_remover:
                del zonas[chave_z]
                n_removidas += 1
    if n_removidas:
        logger.info(f"Pos-processamento v15: removidas {n_removidas} zonas duplicadas (mescladas em outras leis)")

    # Tambem remove zonas REVOGADAS das leis externas (nao devem aparecer nem standalone)
    n_revogadas_removidas = 0
    for j in jsons:
        leg = ((j.get('data') or {}).get('estado') or {}).get('legislacao') or {}
        chave_propria = _extrair_chave_lei({
            'tipo_nome': leg.get('tipo_nome') or leg.get('tipo'),
            'numero': leg.get('numero'),
            'ano': leg.get('ano'),
        })
        if not chave_propria:
            continue
        zonas = ((j.get('data') or {}).get('estado') or {}).get('zonas') or {}
        if not isinstance(zonas, dict):
            continue
        for chave_z in list(zonas.keys()):
            sigla_ult = chave_z.split('|')[-1].lower()
            chave_rev = (chave_propria[0], chave_propria[1], chave_propria[2], sigla_ult)
            if chave_rev in revogacoes_set:
                del zonas[chave_z]
                n_revogadas_removidas += 1
    if n_revogadas_removidas:
        logger.info(f"Pos-processamento v15: removidas {n_revogadas_removidas} zonas REVOGADAS da lei externa standalone")

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
