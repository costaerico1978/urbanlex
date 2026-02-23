#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
modulos/bridge_integracao.py
────────────────────────────
MÓDULO DE INTEGRAÇÃO v1.3 → v2.3

Quando o monitor do v1.3 detecta uma alteração em legislação de zoneamento,
este módulo:
  1. Identifica se a alteração afeta zonas urbanísticas
  2. Dispara o extrator IA do v2.3 com o novo texto da lei
  3. Salva o resultado na tabela integracao_atualizacoes
  4. Cria notificação para aprovação humana
  5. Após aprovação: atualiza zonas_urbanas automaticamente
"""

import os
import json
import logging
from datetime import datetime
from typing import Optional, Dict, Any

import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Conexão com banco
# ─────────────────────────────────────────────

def get_db():
    import os, psycopg2
    from psycopg2.extras import RealDictCursor
    url = os.getenv('DATABASE_URL')
    if url:
        return psycopg2.connect(url, cursor_factory=RealDictCursor)
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=int(os.getenv('DB_PORT', 5432)),
        dbname=os.getenv('DB_NAME', 'urbanlex'),
        user=os.getenv('DB_USER', 'postgres'),
        password=os.getenv('DB_PASSWORD', ''),
        cursor_factory=RealDictCursor
    )


# ─────────────────────────────────────────────
# PASSO 1: Checar se alteração é urbanística
# ─────────────────────────────────────────────

PALAVRAS_URBANISTICAS = [
    'zoneamento', 'zona', 'coeficiente de aproveitamento', 'ca básico',
    'ca máximo', 'taxa de ocupação', 'gabarito', 'recuo', 'afastamento',
    'uso do solo', 'plano diretor', 'lei de uso', 'parcelamento',
    'habitação de interesse social', 'his', 'outorga onerosa'
]

def e_alteracao_urbanistica(titulo: str, conteudo: str = '') -> bool:
    """
    Retorna True se a alteração legislativa toca parâmetros urbanísticos.
    Usado para filtrar o que vale a pena re-extrair via IA.
    """
    texto = (titulo + ' ' + conteudo).lower()
    return any(palavra in texto for palavra in PALAVRAS_URBANISTICAS)


# ─────────────────────────────────────────────
# PASSO 2: Disparar extração IA
# ─────────────────────────────────────────────

def extrair_parametros_com_ia(
    legislacao_texto: str,
    municipio: str,
    zona: str
) -> Dict[str, Any]:
    """
    Chama o ExtratorIAComConsenso do v2.3.
    Retorna dict com parâmetros extraídos + metadata de consenso.
    """
    try:
        from extrator_ia_legislacao_v2_3 import ExtratorIAComConsenso

        extrator = ExtratorIAComConsenso(
            groq_api_key=os.getenv('GROQ_API_KEY'),
            gemini_api_key=os.getenv('GEMINI_API_KEY'),
            max_rodadas_debate=2
        )

        resultado = extrator.extrair_parametros(
            legislacao=legislacao_texto,
            municipio=municipio,
            zona=zona
        )
        return resultado

    except Exception as e:
        logger.error(f"Erro na extração IA: {e}")
        return {'erro': str(e), 'status': 'falha'}


# ─────────────────────────────────────────────
# PASSO 3: Registrar na tabela de integração
# ─────────────────────────────────────────────

def registrar_integracao(
    alteracao_id: int,
    legislacao_id: int,
    municipio_id: int,
    zona_nome: str,
    resultado_extracao: Dict,
    divergencias: Optional[Dict] = None
) -> int:
    """
    Salva o resultado da extração em integracao_atualizacoes.
    Retorna o ID do registro criado.
    """
    conn = get_db()
    try:
        cur = conn.cursor()

        # Tenta encontrar zona_urbana_id existente
        cur.execute("""
            SELECT id FROM zonas_urbanas
            WHERE municipio = (SELECT nome FROM municipios WHERE id = %s)
            AND zona = %s
        """, (municipio_id, zona_nome))
        zona_row = cur.fetchone()
        zona_id = zona_row['id'] if zona_row else None

        cur.execute("""
            INSERT INTO integracao_atualizacoes
                (alteracao_id, legislacao_id, municipio_id, zona_urbana_id,
                 zona_nome, status, resultado_extracao, divergencias_ia, extraido_em)
            VALUES (%s, %s, %s, %s, %s, 'aguardando_aprovacao', %s, %s, NOW())
            RETURNING id
        """, (
            alteracao_id, legislacao_id, municipio_id, zona_id,
            zona_nome,
            json.dumps(resultado_extracao),
            json.dumps(divergencias) if divergencias else None
        ))
        integracao_id = cur.fetchone()['id']
        conn.commit()

        logger.info(f"Integração registrada: ID={integracao_id}, zona={zona_nome}")
        return integracao_id

    finally:
        conn.close()


# ─────────────────────────────────────────────
# PASSO 4: Criar notificação para admin
# ─────────────────────────────────────────────

def criar_notificacao_aprovacao(
    municipio: str,
    zona: str,
    integracao_id: int,
    tem_divergencias: bool
) -> None:
    """
    Cria notificação no painel do v1.3 para o admin revisar.
    """
    nivel = 'alerta' if tem_divergencias else 'info'
    titulo = f"Parâmetros atualizados: {municipio} / {zona}"
    mensagem = (
        f"A IA extraiu novos parâmetros urbanísticos para {municipio} - {zona}. "
        f"{'⚠️ Há divergências entre GROQ e Gemini que precisam de revisão. ' if tem_divergencias else ''}"
        f"Acesse /admin/integracao/{integracao_id} para aprovar ou rejeitar."
    )

    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO notificacoes_admin (tipo, nivel, titulo, mensagem)
            VALUES ('parametros_urbanisticos', %s, %s, %s)
        """, (nivel, titulo, mensagem))
        conn.commit()
    finally:
        conn.close()


# ─────────────────────────────────────────────
# PASSO 5: Aplicar parâmetros aprovados → zonas_urbanas
# ─────────────────────────────────────────────

def aplicar_parametros_aprovados(
    integracao_id: int,
    usuario_id: int
) -> Dict[str, Any]:
    """
    Chamado quando admin aprova uma extração.
    Atualiza (ou cria) a linha em zonas_urbanas com os novos parâmetros.
    Retorna {'success': True/False, 'msg': '...'}
    """
    conn = get_db()
    try:
        cur = conn.cursor()

        # Buscar dados da integração
        cur.execute("""
            SELECT i.*, m.nome as municipio_nome, m.estado
            FROM integracao_atualizacoes i
            JOIN municipios m ON m.id = i.municipio_id
            WHERE i.id = %s AND i.status = 'aguardando_aprovacao'
        """, (integracao_id,))
        integracao = cur.fetchone()

        if not integracao:
            return {'success': False, 'msg': 'Integração não encontrada ou já processada'}

        parametros = integracao['resultado_extracao']
        if isinstance(parametros, str):
            parametros = json.loads(parametros)

        municipio = integracao['municipio_nome']
        estado    = integracao['estado']
        zona      = integracao['zona_nome']

        # Monta os campos para upsert (apenas os que existem na tabela)
        CAMPOS_ZONA = [
            'estado','lote_minimo','lote_maximo','area_a_ser_doada',
            'gabarito_varia_altitude','afastamento_entre_blocos','usos_permitidos',
            'uso_residencial_unifamiliar','uso_residencial_multifamiliar',
            'uso_residencial_his','uso_comercial','uso_servicos',
            'uso_misto','uso_industrial','uso_institucional',
            'no_pavimentos_calculo_area_computavel_maxima',
            'ca_basico_resunif','ca_maximo_resunif','to_basica_resunif','to_maxima_resunif',
            'gabarito_pavtos_basico_resunif','gabarito_pavtos_maximo_resunif',
            'gabarito_metros_basico_resunif','gabarito_metros_maximo_resunif',
            'afastamento_frontal_resunif','afastamento_lateral_resunif','afastamento_fundos_resunif',
            'metodologia_area_computavel_resunif','fator_privativa_computavel_resunif',
            'ca_basico_resmult','ca_maximo_resmult','to_basica_resmult','to_maxima_resmult',
            'gabarito_pavtos_basico_resmult','gabarito_pavtos_maximo_resmult',
            'gabarito_metros_basico_resmult','gabarito_metros_maximo_resmult',
            'afastamento_frontal_resmult','afastamento_lateral_resmult','afastamento_fundos_resmult',
            'metodologia_area_computavel_resmult','fator_privativa_computavel_resmult',
            'ca_basico_reshis','ca_maximo_reshis','to_basica_reshis','to_maxima_reshis',
            'gabarito_pavtos_basico_reshis','gabarito_pavtos_maximo_reshis',
            'gabarito_metros_basico_reshis','gabarito_metros_maximo_reshis',
            'afastamento_frontal_reshis','afastamento_lateral_reshis','afastamento_fundos_reshis',
            'metodologia_area_computavel_reshis','fator_privativa_computavel_reshis',
            'ca_basico_com','ca_maximo_com','to_basica_com','to_maxima_com',
            'gabarito_pavtos_basico_com','gabarito_pavtos_maximo_com',
            'gabarito_metros_basico_com','gabarito_metros_maximo_com',
            'afastamento_frontal_com','afastamento_lateral_com','afastamento_fundos_com',
            'metodologia_area_computavel_com','fator_privativa_computavel_com',
            'ca_basico_serv','ca_maximo_serv','to_basica_serv','to_maxima_serv',
            'gabarito_pavtos_basico_serv','gabarito_pavtos_maximo_serv',
            'gabarito_metros_basico_serv','gabarito_metros_maximo_serv',
            'afastamento_frontal_serv','afastamento_lateral_serv','afastamento_fundos_serv',
            'metodologia_area_computavel_serv','fator_privativa_computavel_serv',
            'ca_basico_misto','ca_maximo_misto','to_basica_misto','to_maxima_misto',
            'ca_basico_ind','ca_maximo_ind','to_basica_ind','to_maxima_ind',
            'ca_basico_inst','ca_maximo_inst','to_basica_inst','to_maxima_inst',
            'formula_area_computavel_basica','formula_area_computavel_maxima','observacoes'
        ]

        # Filtra só os campos que vieram na extração
        dados = {
            k: v for k, v in parametros.items()
            if k in CAMPOS_ZONA and v is not None
        }
        dados['municipio'] = municipio
        dados['zona']      = zona
        dados['estado']    = estado
        dados['data_ultima_modificacao'] = datetime.now()

        if not dados:
            return {'success': False, 'msg': 'Nenhum parâmetro válido para salvar'}

        # UPSERT em zonas_urbanas
        campos    = list(dados.keys())
        valores   = list(dados.values())
        placeholders = ', '.join(['%s'] * len(campos))
        set_clause = ', '.join([f"{c} = EXCLUDED.{c}" for c in campos if c not in ('municipio','zona')])

        sql = f"""
            INSERT INTO zonas_urbanas ({', '.join(campos)})
            VALUES ({placeholders})
            ON CONFLICT (municipio, zona, subzona, divisao_subzona)
            DO UPDATE SET {set_clause}, atualizado_em = NOW()
            RETURNING id
        """
        cur.execute(sql, valores)
        zona_id = cur.fetchone()['id']

        # Atualiza integracao_atualizacoes
        cur.execute("""
            UPDATE integracao_atualizacoes
            SET status = 'aprovado', zona_urbana_id = %s,
                aprovado_em = NOW(), aprovado_por = %s
            WHERE id = %s
        """, (zona_id, usuario_id, integracao_id))

        conn.commit()
        logger.info(f"Zona {zona} de {municipio} atualizada (ID={zona_id})")
        return {'success': True, 'msg': f'Zona {zona} atualizada com sucesso', 'zona_id': zona_id}

    except Exception as e:
        conn.rollback()
        logger.error(f"Erro ao aplicar parâmetros: {e}")
        return {'success': False, 'msg': str(e)}
    finally:
        conn.close()


# ─────────────────────────────────────────────
# PONTO DE ENTRADA PRINCIPAL
# Chamado pelo scheduler do v1.3 quando detecta alteração
# ─────────────────────────────────────────────

def processar_alteracao_detectada(
    alteracao_id: int,
    legislacao_id: int,
    municipio_id: int,
    municipio_nome: str,
    titulo_alteracao: str,
    conteudo_legislacao: str,
    zona_nome: Optional[str] = None
) -> Dict[str, Any]:
    """
    Função principal do bridge.
    Chamada automaticamente pelo scheduler do v1.3.

    Exemplo de chamada no scheduler.py do v1.3:
        from modulos.bridge_integracao import processar_alteracao_detectada
        processar_alteracao_detectada(
            alteracao_id=alt.id,
            legislacao_id=leg.id,
            municipio_id=mun.id,
            municipio_nome=mun.nome,
            titulo_alteracao=alt.descricao,
            conteudo_legislacao=leg.conteudo,
            zona_nome=zona_detectada  # ex: 'ZRM2'
        )
    """
    logger.info(f"Bridge iniciado: municipio={municipio_nome}, alteracao={alteracao_id}")

    # 1. Verificar se é alteração urbanística
    if not e_alteracao_urbanistica(titulo_alteracao, conteudo_legislacao):
        logger.info("Alteração não é urbanística — ignorando bridge")
        return {'processado': False, 'motivo': 'nao_urbanistica'}

    # 2. Se não soubermos a zona, tentamos extrair para o município inteiro
    if not zona_nome:
        zona_nome = 'ZONA_IDENTIFICADA_PELA_IA'

    # 3. Extrair parâmetros com IA
    logger.info(f"Extraindo parâmetros para {municipio_nome} / {zona_nome}...")
    resultado = extrair_parametros_com_ia(
        legislacao_texto=conteudo_legislacao,
        municipio=municipio_nome,
        zona=zona_nome
    )

    if 'erro' in resultado:
        logger.error(f"Extração falhou: {resultado['erro']}")
        return {'processado': False, 'motivo': 'falha_extracao', 'erro': resultado['erro']}

    # 4. Checar divergências
    divergencias = resultado.get('divergencias_nao_resolvidas', {})
    tem_divergencias = bool(divergencias)

    # 5. Registrar na tabela de integração
    integracao_id = registrar_integracao(
        alteracao_id=alteracao_id,
        legislacao_id=legislacao_id,
        municipio_id=municipio_id,
        zona_nome=zona_nome,
        resultado_extracao=resultado,
        divergencias=divergencias
    )

    # 6. Criar notificação para admin
    criar_notificacao_aprovacao(
        municipio=municipio_nome,
        zona=zona_nome,
        integracao_id=integracao_id,
        tem_divergencias=tem_divergencias
    )

    return {
        'processado': True,
        'integracao_id': integracao_id,
        'tem_divergencias': tem_divergencias,
        'status': 'aguardando_aprovacao'
    }
