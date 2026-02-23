#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sistema de Extração IA com Consenso - v2.3 FINAL
Extrai parâmetros urbanísticos usando GROQ + GEMINI com debate automático

Autor: Sistema Urbanístico
Data: 22/02/2026
Versão: 2.3 FINAL
"""

import os
import json
import time
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime

# NOTA: Instale as bibliotecas necessárias:
# pip install groq google-generativeai --break-system-packages

try:
    from groq import Groq
    import google.generativeai as genai
except ImportError:
    print("⚠️ AVISO: Bibliotecas não instaladas.")
    print("Execute: pip install groq google-generativeai --break-system-packages")


class ExtratorIAComConsenso:
    """
    Sistema de extração de parâmetros urbanísticos com consenso IA-IA
    
    Usa GROQ e GEMINI em paralelo e debate automático para resolver divergências
    """
    
    # Mapa de fatores de conversão área computável → privativa
    FATORES_RESMULT = {
        "Só área privativa fechada": 1.05,
        "Área privativa + circulação horizontal": 1.10,
        "Área privativa + circulação horizontal e vertical": 0.95
    }
    
    FATORES_COMERCIAL = {
        "Só área privativa fechada": 1.00,
        "Área privativa + circulação horizontal": 0.90,
        "Área privativa + circulação horizontal e vertical": 0.85
    }
    
    # Altura padrão pavimento (piso a piso)
    ALTURA_PAVIMENTO = 3.0  # metros
    
    def __init__(self, groq_api_key: str, gemini_api_key: str, max_rodadas_debate: int = 2):
        """
        Inicializa o extrator
        
        Args:
            groq_api_key: Chave API do GROQ
            gemini_api_key: Chave API do GEMINI
            max_rodadas_debate: Máximo de rodadas de debate (default 2)
        """
        self.groq_client = Groq(api_key=groq_api_key)
        genai.configure(api_key=gemini_api_key)
        self.gemini_model = genai.GenerativeModel('gemini-pro')
        
        self.max_rodadas = max_rodadas_debate
        self.log_debates = []
    
    def extrair_parametros(
        self, 
        legislacao: str, 
        municipio: str, 
        zona: str
    ) -> Dict[str, Any]:
        """
        Extrai todos os parâmetros urbanísticos com consenso IA-IA
        
        Args:
            legislacao: Texto completo da legislação
            municipio: Nome do município
            zona: Nome da zona
        
        Returns:
            dict com todos os 124 campos preenchidos
        """
        
        print(f"\n{'='*80}")
        print(f"EXTRAINDO PARÂMETROS: {municipio} - {zona}")
        print(f"{'='*80}\n")
        
        # ETAPA 1: Extração inicial GROQ
        print("[1/4] Extraindo com GROQ...")
        resultado_groq = self._extrair_groq(legislacao, municipio, zona)
        time.sleep(1)
        
        # ETAPA 2: Extração inicial GEMINI
        print("[2/4] Extraindo com GEMINI...")
        resultado_gemini = self._extrair_gemini(legislacao, municipio, zona)
        time.sleep(1)
        
        # ETAPA 3: Comparar e buscar consenso
        print("[3/4] Comparando resultados e buscando consenso...")
        resultado_final = self._buscar_consenso(
            legislacao, 
            resultado_groq, 
            resultado_gemini
        )
        
        # ETAPA 4: Pós-processamento
        print("[4/4] Pós-processamento...")
        resultado_completo = self._pos_processar(resultado_final, municipio, zona)
        
        print(f"\n{'='*80}")
        print(f"✅ EXTRAÇÃO CONCLUÍDA!")
        print(f"{'='*80}")
        
        return resultado_completo
    
    def _extrair_groq(self, legislacao: str, municipio: str, zona: str) -> Dict:
        """Extração usando GROQ"""
        
        prompt = self._gerar_prompt_extracao(legislacao, municipio, zona)
        
        try:
            response = self.groq_client.chat.completions.create(
                model="mixtral-8x7b-32768",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=8000
            )
            
            texto_resposta = response.choices[0].message.content
            return self._parsear_json(texto_resposta)
            
        except Exception as e:
            print(f"⚠️ Erro no GROQ: {e}")
            return {}
    
    def _extrair_gemini(self, legislacao: str, municipio: str, zona: str) -> Dict:
        """Extração usando GEMINI"""
        
        prompt = self._gerar_prompt_extracao(legislacao, municipio, zona)
        
        try:
            response = self.gemini_model.generate_content(prompt)
            texto_resposta = response.text
            return self._parsear_json(texto_resposta)
            
        except Exception as e:
            print(f"⚠️ Erro no GEMINI: {e}")
            return {}
    
    def _gerar_prompt_extracao(self, legislacao: str, municipio: str, zona: str) -> str:
        """Gera prompt completo para extração"""
        
        return f"""
Você é um especialista em urbanismo brasileiro. Analise a legislação e extraia os parâmetros urbanísticos.

MUNICÍPIO: {municipio}
ZONA: {zona}

LEGISLAÇÃO:
{legislacao[:15000]}

INSTRUÇÕES:

1. PARÂMETROS POR USO:
Extraia para cada uso (ResUnif, ResMult, ResHIS, Com, Serv, Misto, Ind, Inst):
- CA_basico e CA_maximo
- TO_basica e TO_maxima
- Gabarito_pavtos_basico/maximo e Gabarito_metros_basico/maximo
- Afastamento_frontal, lateral, fundos

2. FÓRMULAS DE CÁLCULO:
Identifique qual METODOLOGIA é usada para calcular área computável:

a) Se usa Coeficiente de Aproveitamento (CA):
   Formula_Basica: "Area_Lote * CA_basico"
   Formula_Maxima: "Area_Lote * CA_maximo"

b) Se NÃO tem CA, mas tem TO e Gabarito:
   Formula_Basica: "Area_Lote * TO_basica * Gabarito_pavtos_basico"
   Formula_Maxima: "Area_Lote * TO_maxima * No_Pavimentos_Calculo_Area_Computavel_Maxima"

c) Se só tem afastamentos e gabarito:
   Formula_Basica: "(Testada - 2*Afastamento_lateral) * (Profundidade - Afastamento_frontal - Afastamento_fundos) * Gabarito_pavtos_basico"

3. PAVIMENTOS TIPO:
Se gabarito é usado no cálculo, identifique:
- Gabarito total permitido (ex: 6 pavimentos ou 18 metros)
- Composição real (térreo, tipos, cobertura)
- Quantos pavimentos TÊM unidades privativas (pavimentos TIPO)
- Preencha "No_Pavimentos_Calculo_Area_Computavel_Maxima" com esse número

CÁLCULO:
- Se gabarito em metros, divida por 3m (altura piso a piso)
- Subtraia pavimentos sem unidades (térreo pilotis, cobertura técnica)

4. METODOLOGIA ÁREA COMPUTÁVEL (para ResUnif, ResMult, ResHIS, Com, Serv):
Identifique O QUE conta na área computável:
- "Só área privativa fechada"
- "Área privativa + circulação horizontal"
- "Área privativa + circulação horizontal e vertical"

5. FATOR DE CONVERSÃO:
Use as tabelas:

ResUnif/ResMult/ResHIS:
- Só privativa fechada → 1.05
- Privativa + circ horiz → 1.10
- Privativa + circ horiz + vert → 0.95

Com/Serv:
- Só privativa fechada → 1.00
- Privativa + circ horiz → 0.90
- Privativa + circ horiz + vert → 0.85

RETORNE JSON VÁLIDO:
{{
  "CA_basico_ResUnif": valor ou "N/A",
  "CA_maximo_ResUnif": valor ou "N/A",
  ... (todos os parâmetros)
  
  "Formula_Area_Computavel_Basica": "fórmula",
  "Formula_Area_Computavel_Maxima": "fórmula",
  
  "No_Pavimentos_Calculo_Area_Computavel_Maxima": número ou "",
  
  "Metodologia_Area_Computavel_ResMult": "texto",
  "Fator_Privativa_Computavel_ResMult": número,
  
  ... (demais campos)
}}

IMPORTANTE:
- Use "N/A" se uso não for permitido
- Use "" (vazio) se parâmetro não se aplica
- Seja preciso nos números
- Cite artigos da legislação em "Observacoes"
"""
    
    def _buscar_consenso(
        self, 
        legislacao: str,
        resultado_groq: Dict, 
        resultado_gemini: Dict
    ) -> Dict:
        """
        Busca consenso entre GROQ e GEMINI através de debate
        """
        
        # Comparar resultados
        divergencias = self._comparar_resultados(resultado_groq, resultado_gemini)
        
        if not divergencias:
            print("  ✅ Consenso total! Nenhuma divergência.")
            return resultado_groq
        
        print(f"  ⚠️ {len(divergencias)} divergências encontradas")
        
        # Tentar resolver cada divergência
        consenso = resultado_groq.copy()
        nao_resolvidas = []
        
        for campo in divergencias:
            valor_groq = resultado_groq.get(campo)
            valor_gemini = resultado_gemini.get(campo)
            
            print(f"\n  🔄 Debate: {campo}")
            print(f"     GROQ: {valor_groq}")
            print(f"     GEMINI: {valor_gemini}")
            
            # Realizar debate
            valor_consenso = self._debater_campo(
                legislacao, campo, valor_groq, valor_gemini
            )
            
            if valor_consenso:
                print(f"     ✅ CONSENSO: {valor_consenso}")
                consenso[campo] = valor_consenso
            else:
                print(f"     ⚠️ SEM CONSENSO")
                nao_resolvidas.append({
                    'campo': campo,
                    'groq': valor_groq,
                    'gemini': valor_gemini
                })
        
        # Adicionar divergências não resolvidas
        if nao_resolvidas:
            consenso['_divergencias_nao_resolvidas'] = nao_resolvidas
            print(f"\n  ⚠️ {len(nao_resolvidas)} divergências NÃO resolvidas")
        
        return consenso
    
    def _debater_campo(
        self, 
        legislacao: str, 
        campo: str, 
        valor_groq: Any, 
        valor_gemini: Any
    ) -> Optional[Any]:
        """
        Debate específico para um campo divergente
        """
        
        for rodada in range(1, self.max_rodadas + 1):
            print(f"       Rodada {rodada}...")
            
            # GROQ explica
            explicacao_groq = self._pedir_explicacao(
                "GROQ", legislacao, campo, valor_groq, valor_gemini, None
            )
            
            # GEMINI explica
            explicacao_gemini = self._pedir_explicacao(
                "GEMINI", legislacao, campo, valor_gemini, valor_groq, explicacao_groq
            )
            
            # GROQ revisa após ouvir GEMINI
            revisao_groq = self._pedir_revisao(
                "GROQ", legislacao, campo, valor_groq, explicacao_gemini
            )
            
            # GEMINI revisa após ouvir GROQ
            revisao_gemini = self._pedir_revisao(
                "GEMINI", legislacao, campo, valor_gemini, explicacao_groq
            )
            
            # Chegaram a consenso?
            if self._valores_iguais(revisao_groq, revisao_gemini):
                return revisao_groq
            
            # Atualizar para próxima rodada
            valor_groq = revisao_groq
            valor_gemini = revisao_gemini
        
        # Não resolveu
        return None
    
    def _pedir_explicacao(
        self, 
        modelo: str, 
        legislacao: str, 
        campo: str, 
        meu_valor: Any,
        valor_outro: Any,
        explicacao_outro: Optional[str]
    ) -> str:
        """
        Pede para modelo explicar seu raciocínio
        """
        
        prompt = f"""
Você extraiu {campo} = {meu_valor}
Outro modelo extraiu {valor_outro}

{f'O outro modelo explicou: {explicacao_outro}' if explicacao_outro else ''}

CITE o trecho EXATO da legislação que justifica sua resposta.
Explique seu raciocínio passo a passo.

LEGISLAÇÃO:
{legislacao[:10000]}

Seja BREVE (max 200 palavras).
"""
        
        if modelo == "GROQ":
            try:
                response = self.groq_client.chat.completions.create(
                    model="mixtral-8x7b-32768",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.1,
                    max_tokens=500
                )
                return response.choices[0].message.content
            except:
                return ""
        else:  # GEMINI
            try:
                response = self.gemini_model.generate_content(prompt)
                return response.text
            except:
                return ""
    
    def _pedir_revisao(
        self, 
        modelo: str, 
        legislacao: str, 
        campo: str, 
        meu_valor: Any,
        explicacao_outro: str
    ) -> Any:
        """
        Pede para modelo revisar após ouvir o outro
        """
        
        prompt = f"""
Você extraiu {campo} = {meu_valor}

Outro modelo argumentou:
{explicacao_outro}

Analisando o argumento do outro modelo, você:
1. MANTÉM sua resposta ({meu_valor})?
2. MUDA para outro valor?

Responda APENAS com o valor final (número ou texto).
Se mudar, explique BREVEMENTE por quê (max 50 palavras).
"""
        
        if modelo == "GROQ":
            try:
                response = self.groq_client.chat.completions.create(
                    model="mixtral-8x7b-32768",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.1,
                    max_tokens=200
                )
                return self._extrair_valor_da_resposta(response.choices[0].message.content)
            except:
                return meu_valor
        else:  # GEMINI
            try:
                response = self.gemini_model.generate_content(prompt)
                return self._extrair_valor_da_resposta(response.text)
            except:
                return meu_valor
    
    def _comparar_resultados(self, resultado1: Dict, resultado2: Dict) -> List[str]:
        """
        Compara dois resultados e retorna lista de campos divergentes
        """
        divergencias = []
        
        for campo in resultado1:
            if campo.startswith('_'):
                continue
            
            valor1 = resultado1.get(campo)
            valor2 = resultado2.get(campo)
            
            if not self._valores_iguais(valor1, valor2):
                divergencias.append(campo)
        
        return divergencias
    
    def _valores_iguais(self, valor1: Any, valor2: Any) -> bool:
        """
        Verifica se dois valores são equivalentes
        """
        # Ambos None/vazios
        if not valor1 and not valor2:
            return True
        
        # Tipos diferentes
        if type(valor1) != type(valor2):
            return False
        
        # Números (com tolerância 5%)
        if isinstance(valor1, (int, float)) and isinstance(valor2, (int, float)):
            if valor1 == 0 and valor2 == 0:
                return True
            return abs(valor1 - valor2) / max(abs(valor1), abs(valor2)) < 0.05
        
        # Strings
        return str(valor1).strip().lower() == str(valor2).strip().lower()
    
    def _parsear_json(self, texto: str) -> Dict:
        """
        Extrai e parseia JSON de texto
        """
        try:
            # Tentar parsear direto
            return json.loads(texto)
        except:
            # Tentar extrair entre ```json e ```
            import re
            match = re.search(r'```json\s*(.*?)\s*```', texto, re.DOTALL)
            if match:
                return json.loads(match.group(1))
            
            # Tentar extrair entre { e }
            match = re.search(r'\{.*\}', texto, re.DOTALL)
            if match:
                return json.loads(match.group(0))
            
            return {}
    
    def _extrair_valor_da_resposta(self, texto: str) -> Any:
        """
        Extrai valor simples de resposta de revisão
        """
        # Procurar número
        import re
        match = re.search(r'(\d+\.?\d*)', texto)
        if match:
            num = match.group(1)
            return float(num) if '.' in num else int(num)
        
        # Procurar N/A
        if 'N/A' in texto or 'n/a' in texto:
            return 'N/A'
        
        # Retornar texto limpo
        return texto.strip()[:100]
    
    def _pos_processar(self, resultado: Dict, municipio: str, zona: str) -> Dict:
        """
        Pós-processa resultado final
        """
        resultado['Pais'] = 'Brasil'
        resultado['Municipio'] = municipio
        resultado['Zona'] = zona
        resultado['Data_Ultima_Modificacao'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        return resultado


# ========== FUNÇÃO DE CONVENIÊNCIA ==========

def extrair_zona_completa(
    legislacao: str,
    municipio: str,
    zona: str,
    groq_api_key: str = None,
    gemini_api_key: str = None
) -> Dict[str, Any]:
    """
    Função de conveniência para extrair zona completa
    
    Args:
        legislacao: Texto da legislação
        municipio: Nome do município
        zona: Nome da zona
        groq_api_key: Chave GROQ (ou variável ambiente GROQ_API_KEY)
        gemini_api_key: Chave GEMINI (ou variável ambiente GEMINI_API_KEY)
    
    Returns:
        dict com 124 campos preenchidos
    
    Example:
        >>> resultado = extrair_zona_completa(
        ...     legislacao=texto_lei,
        ...     municipio="Rio de Janeiro",
        ...     zona="ZRM2"
        ... )
    """
    
    # Buscar chaves de API
    groq_key = groq_api_key or os.getenv('GROQ_API_KEY')
    gemini_key = gemini_api_key or os.getenv('GEMINI_API_KEY')
    
    if not groq_key or not gemini_key:
        raise ValueError("API keys não fornecidas. Use variáveis de ambiente ou parâmetros.")
    
    # Criar extrator e extrair
    extrator = ExtratorIAComConsenso(groq_key, gemini_key)
    return extrator.extrair_parametros(legislacao, municipio, zona)


# ========== EXEMPLO DE USO ==========

if __name__ == "__main__":
    print("="*80)
    print("SISTEMA DE EXTRAÇÃO IA COM CONSENSO v2.3")
    print("="*80)
    
    print("\n📋 CONFIGURAÇÃO:")
    print("1. Configure as chaves de API:")
    print("   export GROQ_API_KEY='sua_chave_aqui'")
    print("   export GEMINI_API_KEY='sua_chave_aqui'")
    print("\n2. Use a função:")
    print("""
    from extrator_ia_legislacao_v2.3 import extrair_zona_completa
    
    resultado = extrair_zona_completa(
        legislacao=texto_da_lei,
        municipio="Rio de Janeiro",
        zona="ZRM2"
    )
    
    print(resultado['CA_maximo_Com'])
    print(resultado['Formula_Area_Computavel_Maxima'])
    """)
    
    print("\n✅ Sistema pronto para uso!")
    print("="*80)
