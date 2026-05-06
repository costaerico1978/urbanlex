"""
modulos/vigencia.py — Calculo da matriz de vigencia de legislacoes.

Funcoes principais:
- ordenar_pdfs_por_prioridade(catalogacao): ordena PDFs por hierarquia + data + especificidade
- calcular_matriz_vigencia(catalogacao): mapeia revogacoes
- gerar_instrucao_revogacao_para_pdf(pdf_id, matriz): texto explicativo para o prompt P2
"""
from datetime import datetime


# ============================================================
# Hierarquia juridica brasileira (1 = mais alto, 6 = mais baixo)
# ============================================================
HIERARQUIA = {
    'constituicao': 1,
    'lei_complementar': 2,
    'lc': 2,
    'lei_ordinaria': 3,
    'lei': 3,
    'lo': 3,
    'decreto': 4,
    'dec': 4,
    'errata': 5,
    'retificacao': 5,
    'portaria': 6,
}


def hierarquia_de(tipo):
    """Retorna nivel hierarquico (1-6) ou 99 se desconhecido."""
    if not tipo:
        return 99
    t = str(tipo).lower().strip()
    for chave, nivel in HIERARQUIA.items():
        if chave in t:
            return nivel
    return 99


def parsear_data(data_str):
    """Retorna datetime ou None. Aceita ISO, DD/MM/YYYY, YYYY-MM-DD, etc."""
    if not data_str:
        return None
    s = str(data_str).strip()
    formatos = ['%Y-%m-%d', '%d/%m/%Y', '%Y/%m/%d', '%d-%m-%Y', '%Y']
    for f in formatos:
        try:
            return datetime.strptime(s, f)
        except Exception:
            continue
    return None


# ============================================================
# Ordenacao de PDFs por prioridade
# ============================================================
def ordenar_pdfs_por_prioridade(catalogacao):
    """
    Ordena lista de PDFs catalogados por:
    1. Hierarquia juridica (LC > LO > Decreto > Errata > Portaria)
    2. Data (mais recente primeiro)
    3. Especificidade (lei modificadora vem antes da modificada)
    
    Recebe lista de dicts com chaves: nome_arquivo, identificacao, tipo, data,
                                       hierarquia_juridica, tipo_atuacao, leis_modificadas.
    Retorna a mesma lista ordenada.
    """
    if not catalogacao:
        return []
    
    def chave(item):
        # Hierarquia: usar campo se existe, senao deduzir do tipo
        h = item.get('hierarquia_juridica')
        if h is None:
            h = hierarquia_de(item.get('tipo'))
        # Data: mais recente primeiro = data invertida (entao usar -timestamp)
        dt = parsear_data(item.get('data'))
        ts = dt.timestamp() if dt else 0
        # Especificidade: modificadoras antes das principais (ordem inversa)
        atuacao = (item.get('tipo_atuacao') or '').lower()
        prio_atuacao = {'modificadora': 0, 'errata': 1, 'regulamentadora': 2,
                        'principal': 3}.get(atuacao, 4)
        # Ordenar por: hierarquia ASC, data DESC, atuacao ASC
        return (h, -ts, prio_atuacao)
    
    return sorted(catalogacao, key=chave)


# ============================================================
# Calculo da matriz de vigencia
# ============================================================
def calcular_matriz_vigencia(catalogacao):
    """
    Recebe a catalogacao da P0 e calcula:
    {
      'leis_revogadas_totalmente': [identificacao, ...],
      'revogacoes_parciais': [
          {
            'lei_revogadora': 'LC 281/2025',
            'lei_alvo': 'LC 270/2024',
            'escopo': [
              {'dispositivo': 'Art. 47', 'geografia': 'todas', 'uso': 'todos'},
              {'dispositivo': 'Tabela XV do Anexo II', 'geografia': 'AP-1, AP-2.1', 'uso': 'todos'},
            ]
          }
      ],
      'pdf_para_revogacoes_a_aplicar': {
        # para cada PDF (pelo identificacao), lista de revogacoes que devem ser aplicadas
        'LC 270/2024': [
            {'fonte': 'LC 281/2025', 'dispositivo': 'Art. 47', 'geografia': 'todas'},
            ...
        ]
      }
    }
    """
    if not catalogacao:
        return {
            'leis_revogadas_totalmente': [],
            'revogacoes_parciais': [],
            'pdf_para_revogacoes_a_aplicar': {}
        }
    
    leis_revogadas_totalmente = []
    revogacoes_parciais = []
    pdf_para_revogacoes_a_aplicar = {}
    
    for it in catalogacao:
        ident = it.get('identificacao')
        modifs = it.get('leis_modificadas') or []
        for mod in modifs:
            alvo = mod.get('alvo')
            if not alvo:
                continue
            tipo_mod = (mod.get('tipo_modificacao') or '').lower()
            escopo = mod.get('escopo') or []
            
            if 'total' in tipo_mod:
                # Revogacao total
                leis_revogadas_totalmente.append({
                    'lei_revogadora': ident,
                    'lei_alvo': alvo,
                })
            elif 'parcial' in tipo_mod or escopo:
                # Revogacao parcial
                revogacoes_parciais.append({
                    'lei_revogadora': ident,
                    'lei_alvo': alvo,
                    'escopo': escopo
                })
                # Adiciona ao mapa pdf->revogacoes
                if alvo not in pdf_para_revogacoes_a_aplicar:
                    pdf_para_revogacoes_a_aplicar[alvo] = []
                for esc in escopo:
                    pdf_para_revogacoes_a_aplicar[alvo].append({
                        'fonte': ident,
                        'dispositivo': esc.get('dispositivo', '?'),
                        'geografia': esc.get('geografia', 'todas'),
                        'uso': esc.get('uso', 'todos'),
                    })
    
    return {
        'leis_revogadas_totalmente': leis_revogadas_totalmente,
        'revogacoes_parciais': revogacoes_parciais,
        'pdf_para_revogacoes_a_aplicar': pdf_para_revogacoes_a_aplicar,
    }


# ============================================================
# Filtrar PDFs revogados totalmente
# ============================================================
def filtrar_pdfs_revogados_totalmente(catalogacao, matriz):
    """
    Remove da lista catalogacao os PDFs cujas leis foram TOTALMENTE revogadas.
    Retorna nova lista (catalogacao filtrada).
    """
    revogadas = {r['lei_alvo'] for r in matriz.get('leis_revogadas_totalmente', [])}
    if not revogadas:
        return catalogacao
    return [it for it in catalogacao
            if it.get('identificacao') not in revogadas]


# ============================================================
# Gerar texto de instrucao para prompt P2
# ============================================================
def gerar_instrucao_revogacao_para_pdf(pdf_identificacao, matriz):
    """
    Para um PDF especifico, gera texto que sera incluido no prompt P2
    explicando o que ja foi revogado dessa lei.
    
    Retorna string vazia se a lei nao tem revogacoes ativas.
    """
    revogs = matriz.get('pdf_para_revogacoes_a_aplicar', {}).get(pdf_identificacao, [])
    if not revogs:
        return ''
    
    linhas = [
        '',
        '=== INSTRUCAO DE REVOGACOES PARCIAIS ===',
        f'A lei "{pdf_identificacao}" sofreu as seguintes revogacoes parciais por leis posteriores:',
        ''
    ]
    
    for r in revogs:
        geo = r.get('geografia', 'todas')
        uso = r.get('uso', 'todos')
        disp = r.get('dispositivo', '?')
        fonte = r.get('fonte', '?')
        partes = [f'  • Dispositivo "{disp}" foi REVOGADO por {fonte}']
        if geo and geo.lower() != 'todas':
            partes.append(f'    (apenas para: {geo})')
        if uso and uso.lower() != 'todos':
            partes.append(f'    (apenas uso: {uso})')
        linhas.extend(partes)
    
    linhas.extend([
        '',
        'REGRA: ao preencher dados desta lei, IGNORE os conteudos dos dispositivos',
        'revogados acima. Use APENAS o conteudo nao-revogado. Para os dispositivos',
        'revogados, NAO retorne dados (deixa que a lei revogadora preencha).',
        '',
    ])
    
    return '\n'.join(linhas)


# ============================================================
# Helper: filtrar dispositivo revogado para zona
# ============================================================
def dispositivo_revogado_para_zona(pdf_identificacao, dispositivo, zona, unidade_territorial, matriz):
    """
    Retorna True se o dispositivo X da lei Y foi revogado PARA a zona/UT em questao.
    Usado para validar in-flight (sistema pode reforcar a regra apos resposta da IA).
    """
    revogs = matriz.get('pdf_para_revogacoes_a_aplicar', {}).get(pdf_identificacao, [])
    for r in revogs:
        if r.get('dispositivo') != dispositivo:
            continue
        geo = (r.get('geografia') or 'todas').lower()
        if geo == 'todas':
            return True
        if zona and zona.lower() in geo:
            return True
        if unidade_territorial and unidade_territorial.lower() in geo:
            return True
    return False
