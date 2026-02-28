#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
teste_agente_e2e.py
────────────────────
Teste end-to-end do agente autônomo.

Simula: "Chegou o município Niterói/RJ da plataforma externa"
E roda o pipeline completo:
  1. Detecta que é município novo
  2. Descobre o site do diário oficial
  3. Descobre legislações urbanísticas (Plano Diretor, LUOS, etc.)
  4. Cadastra na biblioteca + ativa monitoramento
  5. Agente navega o diário oficial buscando publicações
  6. Analisa resultados e aplica regras automáticas

USO:
  # Teste completo (precisa de GEMINI_API_KEY e DATABASE_URL)
  python teste_agente_e2e.py

  # Teste só de descoberta (sem banco)
  python teste_agente_e2e.py --sem-banco

  # Teste só do navegador
  python teste_agente_e2e.py --navegador-apenas

  # Testar com outro município
  python teste_agente_e2e.py --municipio "Campinas" --estado "SP"
"""

import os
import sys
import json
import asyncio
import logging
import argparse
from datetime import date, datetime, timedelta

# Logging colorido
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Cores
G = '\033[92m'  # verde
Y = '\033[93m'  # amarelo
R = '\033[91m'  # vermelho
B = '\033[94m'  # azul
C = '\033[96m'  # cyan
W = '\033[0m'   # reset
BOLD = '\033[1m'


def banner(texto):
    print(f"\n{B}{'═'*70}")
    print(f"  {BOLD}{texto}{W}")
    print(f"{B}{'═'*70}{W}\n")


def ok(msg):
    print(f"  {G}✓{W} {msg}")


def warn(msg):
    print(f"  {Y}⚠{W} {msg}")


def erro(msg):
    print(f"  {R}✗{W} {msg}")


def info(msg):
    print(f"  {C}→{W} {msg}")


def separador(titulo):
    print(f"\n  {B}── {titulo} ──{W}")


# ─────────────────────────────────────────────────────────────────────────────
# VERIFICAÇÕES DE AMBIENTE
# ─────────────────────────────────────────────────────────────────────────────

def verificar_ambiente():
    """Verifica dependências e variáveis de ambiente."""
    banner("VERIFICAÇÃO DE AMBIENTE")

    checks = {
        'GEMINI_API_KEY': os.getenv('GEMINI_API_KEY', ''),
        'DATABASE_URL': os.getenv('DATABASE_URL', ''),
        'ADMIN_EMAIL': os.getenv('ADMIN_EMAIL', ''),
    }

    for k, v in checks.items():
        if v:
            ok(f"{k}: {'*' * 8}...{v[-6:]}" if 'KEY' in k or 'URL' in k else f"{k}: {v}")
        else:
            warn(f"{k}: NÃO CONFIGURADA")

    separador("Dependências Python")
    deps = {
        'playwright': False, 'google.generativeai': False,
        'requests': False, 'psycopg2': False,
    }
    for dep in deps:
        try:
            __import__(dep)
            deps[dep] = True
            ok(f"{dep}")
        except ImportError:
            warn(f"{dep} — NÃO INSTALADO")

    separador("Módulos UrbanLex")
    modulos = [
        'modulos.browser_pool', 'modulos.navegador_agente',
        'modulos.descobridor_diario', 'modulos.descobridor_legislacoes',
        'modulos.integrador_plataforma', 'modulos.regras_automaticas',
    ]
    for mod in modulos:
        try:
            __import__(mod)
            ok(f"{mod}")
        except Exception as e:
            warn(f"{mod} — {e}")

    return checks, deps


# ─────────────────────────────────────────────────────────────────────────────
# ETAPA 1: DESCOBRIR DIÁRIO OFICIAL
# ─────────────────────────────────────────────────────────────────────────────

def teste_descobrir_diario(municipio, estado):
    banner(f"ETAPA 1: DESCOBRIR DIÁRIO OFICIAL — {municipio}/{estado}")

    try:
        from modulos.descobridor_diario import descobrir_diario
    except ImportError as e:
        erro(f"Import falhou: {e}")
        return None

    info(f"Buscando diário oficial de {municipio}/{estado}...")
    info("Estratégia: cache → base conhecida → Google+LLM → Querido Diário")
    print()

    resultado = descobrir_diario(municipio, estado, 'municipal')

    if resultado.get('url'):
        ok(f"URL encontrada: {BOLD}{resultado['url']}{W}")
        ok(f"Nome: {resultado.get('nome', '—')}")
        ok(f"Plataforma: {resultado.get('tipo_plataforma', '—')}")
        ok(f"Origem: {resultado.get('origem', '—')}")
        if resultado.get('codigo_ibge'):
            ok(f"IBGE: {resultado['codigo_ibge']}")
    else:
        erro(f"Não foi possível descobrir diário")
        warn(f"Origem: {resultado.get('origem', '—')}")

    return resultado


# ─────────────────────────────────────────────────────────────────────────────
# ETAPA 2: DESCOBRIR LEGISLAÇÕES URBANÍSTICAS
# ─────────────────────────────────────────────────────────────────────────────

def teste_descobrir_legislacoes(municipio, estado):
    banner(f"ETAPA 2: DESCOBRIR LEGISLAÇÕES — {municipio}/{estado}")

    if not os.getenv('GEMINI_API_KEY'):
        warn("GEMINI_API_KEY não configurada — pulando")
        return []

    try:
        from modulos.descobridor_legislacoes import descobrir_legislacoes_municipio
    except ImportError as e:
        erro(f"Import falhou: {e}")
        return []

    info(f"Perguntando ao Gemini sobre legislações urbanísticas de {municipio}/{estado}...")
    info("Buscando: Plano Diretor, LUOS, Código de Obras, Parcelamento, Posturas")
    print()

    try:
        legislacoes = descobrir_legislacoes_municipio(municipio, estado)
    except Exception as e:
        erro(f"Erro: {e}")
        return []

    if legislacoes:
        ok(f"{BOLD}{len(legislacoes)} legislação(ões) encontrada(s):{W}")
        for i, leg in enumerate(legislacoes, 1):
            conf = leg.get('confianca', 0)
            cor_conf = G if conf >= 0.8 else (Y if conf >= 0.6 else R)
            print(f"    {i}. {leg.get('tipo', '?')} nº {leg.get('numero', '?')}/{leg.get('ano', '?')}")
            print(f"       Assunto: {leg.get('assunto', '—')}")
            print(f"       Ementa: {(leg.get('ementa', '—'))[:80]}")
            print(f"       Confiança: {cor_conf}{conf:.0%}{W}")
            print()
    else:
        warn("Nenhuma legislação encontrada pelo LLM")

    return legislacoes


# ─────────────────────────────────────────────────────────────────────────────
# ETAPA 3: NAVEGADOR AUTÔNOMO
# ─────────────────────────────────────────────────────────────────────────────

def teste_navegador(url_diario, termos_busca):
    banner(f"ETAPA 3: AGENTE AUTÔNOMO NAVEGANDO")

    if not os.getenv('GEMINI_API_KEY'):
        warn("GEMINI_API_KEY não configurada — pulando navegação")
        return None

    try:
        from modulos.browser_pool import playwright_disponivel
        if not playwright_disponivel():
            warn("Playwright não instalado — pulando navegação")
            info("Instale com: pip install playwright && playwright install chromium")
            return None
    except ImportError:
        warn("browser_pool não importável")
        return None

    info(f"Alvo: {BOLD}{url_diario}{W}")
    info(f"Buscando: {termos_busca}")
    info(f"Modo: Observar screenshot → Gemini decide → Playwright age")
    print()

    try:
        from modulos.navegador_agente import executar_busca_agente

        data_inicio = date.today() - timedelta(days=30)
        data_fim = date.today()

        info(f"Período: {data_inicio} → {data_fim}")
        info("Iniciando browser...")
        print()

        resultado = asyncio.run(
            executar_busca_agente(url_diario, termos_busca, data_inicio, data_fim)
        )

        separador("Resultado da navegação")

        if resultado.get('sucesso'):
            ok(f"SUCESSO — {resultado.get('total', 0)} publicação(ões)")
        else:
            warn(f"Sem resultados: {resultado.get('mensagem', '?')}")

        ok(f"Passos executados: {resultado.get('passos_executados', 0)}")
        ok(f"Método: {resultado.get('metodo', '?')}")

        if resultado.get('log_passos'):
            separador("Log de passos do agente")
            for p in resultado['log_passos']:
                acao = p.get('acao', '?')
                rac = p.get('raciocinio', '')[:80]
                print(f"    Passo {p.get('passo', '?')}: {BOLD}{acao}{W}")
                if rac:
                    print(f"      💭 {rac}")

        if resultado.get('publicacoes'):
            separador("Publicações encontradas")
            for pub in resultado['publicacoes'][:5]:
                print(f"    📰 {pub.get('titulo', '?')}")
                print(f"       Data: {pub.get('data', '?')} | Tipo: {pub.get('tipo', '?')}")
                if pub.get('conteudo'):
                    print(f"       {pub['conteudo'][:100]}...")
                print()

        return resultado

    except Exception as e:
        erro(f"Erro na navegação: {e}")
        import traceback
        traceback.print_exc()
        return None


# ─────────────────────────────────────────────────────────────────────────────
# ETAPA 4: CADASTRO + MONITORAMENTO (precisa de banco)
# ─────────────────────────────────────────────────────────────────────────────

def teste_cadastro(legislacoes, municipio, estado):
    banner(f"ETAPA 4: CADASTRO NA BIBLIOTECA")

    if not os.getenv('DATABASE_URL'):
        warn("DATABASE_URL não configurada — simulando cadastro")
        for leg in legislacoes:
            info(f"[SIMULADO] Cadastraria: {leg.get('tipo', '?')} "
                 f"nº {leg.get('numero', '?')}/{leg.get('ano', '?')}")
            info(f"[SIMULADO] Monitoramento ativo desde: {leg.get('data_publicacao', 'hoje')}")
        return []

    try:
        from modulos.descobridor_legislacoes import cadastrar_legislacoes_descobertas
    except ImportError as e:
        erro(f"Import falhou: {e}")
        return []

    info(f"Cadastrando {len(legislacoes)} legislação(ões) com monitoramento ativo...")

    ids = cadastrar_legislacoes_descobertas(
        legislacoes, municipio, estado,
        ativar_monitoramento=True
    )

    if ids:
        ok(f"{BOLD}{len(ids)} legislação(ões) cadastrada(s){W}")
        for leg_id in ids:
            ok(f"  ID: {leg_id}")
    else:
        warn("Nenhuma legislação cadastrada")

    return ids


# ─────────────────────────────────────────────────────────────────────────────
# ETAPA 5: REGRAS AUTOMÁTICAS (simulação)
# ─────────────────────────────────────────────────────────────────────────────

def teste_regras_automaticas():
    banner("ETAPA 5: TESTE DE REGRAS AUTOMÁTICAS (simulação)")

    try:
        from modulos.regras_automaticas import aplicar_regras_analise
    except ImportError as e:
        warn(f"Import: {e}")
        return

    # Simular detecção de alteração
    separador("Cenário A: Decreto altera Plano Diretor")
    info("Simulando: IA detectou que Decreto 456/2026 altera LC 123/2020")

    if os.getenv('DATABASE_URL'):
        # Precisaria de IDs reais — pular se não tem dados
        warn("Teste de regras precisa de legislações reais no banco — use após etapa 4")
    else:
        info("[SIMULADO] Regra acionada: nova_legislacao_detectada")
        info("[SIMULADO] Ação: cadastrar Decreto 456/2026 + criar relação + monitorar")

    print()

    separador("Cenário B: Lei revoga Código de Obras")
    info("Simulando: IA detectou que Lei 789/2026 revoga Lei 100/2015")

    if os.getenv('DATABASE_URL'):
        warn("Teste de regras precisa de legislações reais no banco")
    else:
        info("[SIMULADO] Regra acionada: revogacao_detectada")
        info("[SIMULADO] Ação: desativar monitoramento de Lei 100/2015")
        info("[SIMULADO] Ação: cadastrar Lei 789/2026 automaticamente")


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE COMPLETO
# ─────────────────────────────────────────────────────────────────────────────

def pipeline_completo(municipio, estado, args):
    banner(f"PIPELINE COMPLETO: {municipio}/{estado}")

    inicio = datetime.now()
    info(f"Início: {inicio.strftime('%H:%M:%S')}")
    info(f"Simulando: plataforma externa enviou '{municipio}/{estado}' como município novo")
    print()

    # ── ETAPA 1: Descobrir diário oficial ──
    diario = teste_descobrir_diario(municipio, estado)

    # ── ETAPA 2: Descobrir legislações ──
    legislacoes = teste_descobrir_legislacoes(municipio, estado)

    # ── ETAPA 3: Navegador autônomo ──
    if diario and diario.get('url') and not args.sem_navegador:
        # Montar termos de busca a partir das legislações descobertas
        termos = []
        for leg in legislacoes[:3]:
            t = f"{leg.get('tipo', 'Lei')} {leg.get('numero', '')}"
            if t.strip():
                termos.append(t.strip())
        if not termos:
            termos = ['plano diretor', 'uso e ocupação do solo']

        resultado_nav = teste_navegador(diario['url'], termos)
    else:
        if args.sem_navegador:
            warn("Navegação pulada (--sem-navegador)")
        resultado_nav = None

    # ── ETAPA 4: Cadastro ──
    if legislacoes and not args.sem_banco:
        ids = teste_cadastro(legislacoes, municipio, estado)
    else:
        ids = []

    # ── ETAPA 5: Regras automáticas ──
    teste_regras_automaticas()

    # ── RESUMO ──
    duracao = (datetime.now() - inicio).total_seconds()

    banner("RESUMO DO PIPELINE")
    print(f"  Município:     {BOLD}{municipio}/{estado}{W}")
    print(f"  Duração:       {duracao:.1f}s")
    print(f"  Diário:        {diario.get('url', 'não encontrado') if diario else 'não encontrado'}")
    print(f"  Legislações:   {len(legislacoes)} descoberta(s)")
    print(f"  Cadastradas:   {len(ids)}")
    print(f"  Navegação:     {'sucesso' if resultado_nav and resultado_nav.get('sucesso') else 'sem resultados ou não executada'}")
    print()

    # Gerar relatório JSON
    relatorio = {
        'municipio': municipio,
        'estado': estado,
        'duracao_seg': duracao,
        'diario': diario,
        'legislacoes_encontradas': legislacoes,
        'legislacoes_cadastradas': ids,
        'navegacao': resultado_nav,
        'timestamp': datetime.now().isoformat(),
    }

    relatorio_path = f'teste_e2e_{municipio.lower().replace(" ", "_")}_{estado}.json'
    with open(relatorio_path, 'w', encoding='utf-8') as f:
        json.dump(relatorio, f, ensure_ascii=False, indent=2, default=str)
    ok(f"Relatório salvo em: {relatorio_path}")

    return relatorio


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Teste E2E do Agente Autônomo UrbanLex')
    parser.add_argument('--municipio', default='Niterói', help='Nome do município')
    parser.add_argument('--estado', default='RJ', help='UF do estado')
    parser.add_argument('--sem-banco', action='store_true', help='Não tentar acessar banco de dados')
    parser.add_argument('--sem-navegador', action='store_true', help='Pular teste de navegação')
    parser.add_argument('--navegador-apenas', action='store_true', help='Testar só a navegação')
    parser.add_argument('--url-diario', default='', help='URL específica para testar navegação')
    parser.add_argument('--termo', default='plano diretor', help='Termo para buscar na navegação')
    args = parser.parse_args()

    banner("🤖 URBANLEX — TESTE E2E DO AGENTE AUTÔNOMO v6.0")
    print(f"  Data: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"  Município: {args.municipio}/{args.estado}")
    print()

    # Verificar ambiente
    checks, deps = verificar_ambiente()

    if args.navegador_apenas:
        url = args.url_diario
        if not url:
            # Tentar descobrir
            diario = teste_descobrir_diario(args.municipio, args.estado)
            url = diario.get('url', '') if diario else ''
        if url:
            teste_navegador(url, [args.termo])
        else:
            erro("Nenhuma URL disponível para navegação")
        return

    # Pipeline completo
    pipeline_completo(args.municipio, args.estado, args)


if __name__ == '__main__':
    main()
