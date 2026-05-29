"""
consolidar_leis.py
Consolida múltiplas leis em um único JSON de estado.

Recebe uma lista de JSONs de resultado_final.json (gerados por extrair_parametros.py)
e faz merge por (zona, parâmetro), com lei mais recente ganhando.

API pública:
  consolidar_multiplas(lista_jsons, log_callback) -> dict
"""
import json
import logging

logger = logging.getLogger(__name__)

CAMPOS_MERGE = [
    'usos_permitidos', 'parametros_gerais', 'parametros_por_uso',
    'variacoes', 'acrescimos_extraordinarios', 'hierarquia',
    'metodologia_area_computavel', 'afastamentos_crescentes',
]


def _log(msg, cb=None):
    logger.info(msg)
    if cb:
        cb(msg)


def _extrair_sigla(z):
    if not isinstance(z, dict):
        return ''
    for campo in ('sigla', 'zona', 'nome'):
        v = z.get(campo)
        if v and isinstance(v, str):
            return v.strip().upper()
    return ''


def _merge_profundo(atual, novo):
    """Merge profundo — novo sobrescreve atual (lei mais recente ganha)."""
    if not isinstance(atual, dict) or not isinstance(novo, dict):
        return
    for k, v in novo.items():
        if v in (None, {}, []):
            continue  # novo vazio não sobrescreve
        if k not in atual or atual[k] in (None, {}, []):
            atual[k] = v
        elif isinstance(atual[k], dict) and isinstance(v, dict):
            _merge_profundo(atual[k], v)
        else:
            atual[k] = v  # lei mais recente ganha


def _merge_zona(atual, nova):
    """Aplica nova zona sobre a atual — nova lei ganha campo a campo."""
    for campo in CAMPOS_MERGE:
        v_novo = nova.get(campo)
        if v_novo in (None, {}, []):
            continue
        v_atual = atual.get(campo)
        if v_atual in (None, {}, []):
            atual[campo] = v_novo
        elif isinstance(v_atual, dict) and isinstance(v_novo, dict):
            _merge_profundo(v_atual, v_novo)
        else:
            atual[campo] = v_novo  # lei mais recente ganha


def consolidar_multiplas(lista_jsons, log_callback=None):
    """
    Consolida múltiplos JSONs de resultado_final.json em um único estado.

    lista_jsons: lista de dicts OU paths de arquivos JSON.
    Os JSONs devem estar ordenados do mais antigo para o mais recente
    (lei mais recente ganha por zona/parâmetro).

    Retorna: dict com estado consolidado.
    """
    _log(f"Consolidando {len(lista_jsons)} lei(s)...", log_callback)

    estado = {
        'legislacao': None,
        'zonas': {},
        'modificacoes': [],
        'refs_externas': [],
        'usos_por_zona': {},
    }

    for i, item in enumerate(lista_jsons, 1):
        # Aceita path de arquivo ou dict
        if isinstance(item, str):
            with open(item, encoding='utf-8') as f:
                data = json.load(f)
        else:
            data = item

        # Suporta formato {estado: {...}} ou direto {...}
        if 'estado' in data:
            estado_lei = data['estado']
        else:
            estado_lei = data

        leg = estado_lei.get('legislacao')
        label = f"Lei {i}"
        if leg and isinstance(leg, dict):
            label = f"{leg.get('tipo','')} {leg.get('numero','')}/{leg.get('ano','')}".strip()
            # Primeira legislação define o cabeçalho; demais sobrescrevem
            estado['legislacao'] = leg

        # Modificações: acumula sem duplicar
        for m in (estado_lei.get('modificacoes') or []):
            if isinstance(m, dict):
                k = (m.get('alvo'), m.get('dispositivo'))
                if k not in [(x.get('alvo'), x.get('dispositivo')) for x in estado['modificacoes']]:
                    estado['modificacoes'].append(m)

        # Zonas: merge com lei mais recente ganhando
        novas = 0; atualizadas = 0
        for z in (estado_lei.get('zonas') or []):
            if not isinstance(z, dict):
                continue
            sigla = _extrair_sigla(z)
            if not sigla:
                continue
            if sigla not in estado['zonas']:
                estado['zonas'][sigla] = z
                novas += 1
            else:
                _merge_zona(estado['zonas'][sigla], z)
                atualizadas += 1

        # Usos por zona
        for sigla, usos in (estado_lei.get('usos_por_zona') or {}).items():
            if usos:
                estado['usos_por_zona'][sigla] = usos

        _log(f"  {label}: {novas} zonas novas, {atualizadas} atualizadas → total {len(estado['zonas'])}", log_callback)

    _log(f"Consolidado: {len(estado['zonas'])} zonas de {len(lista_jsons)} lei(s)", log_callback)
    return estado
