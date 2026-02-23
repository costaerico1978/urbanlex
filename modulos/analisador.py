#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
modulos/analisador.py
─────────────────────
Módulo de análise IA de Diários Oficiais.
Adaptado do v1.3 para UrbanLex unificado (sem dependência de config.py).
Usa GROQ por padrão; suporta também Claude e Gemini.
"""

import os
import logging
import json
import re
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

# Configurações via variáveis de ambiente
AI_PROVIDER    = os.getenv('AI_PROVIDER', 'groq')
GROQ_API_KEY   = os.getenv('GROQ_API_KEY', '')
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY', '')
ANTHROPIC_KEY  = os.getenv('ANTHROPIC_API_KEY', '')
MAX_TOKENS     = int(os.getenv('AI_MAX_TOKENS', '4000'))

AI_MODELS = {
    'groq':   'llama-3.1-70b-versatile',
    'gemini': 'gemini-1.5-pro',
    'claude': 'claude-sonnet-4-6',
}
MODEL_NAME = AI_MODELS.get(AI_PROVIDER, 'llama-3.1-70b-versatile')

KEYWORDS_ALTERACAO = [
    "altera", "modifica", "revoga", "derroga", "cancela",
    "substitui", "acrescenta", "suprime", "retifica",
    "dá nova redação", "nova redação", "fica alterado",
    "fica revogado", "ficam revogados", "fica modificado"
]

import logging
from typing import List, Dict, Optional
import json
import re


logger = logging.getLogger(__name__)

class AnalisadorIA:
    def __init__(self, api_key: str = None, provider: str = None):
        self.provider = provider or AI_PROVIDER
        self.model = MODEL_NAME
        
        # Inicializar cliente baseado no provider
        if self.provider == 'groq':
            from groq import Groq
            self.api_key = api_key or GROQ_API_KEY
            if not self.api_key:
                raise ValueError("Groq API Key não configurada. Obtenha gratuitamente em: https://console.groq.com/")
            self.client = Groq(api_key=self.api_key)
            
        elif self.provider == 'claude':
            from anthropic import Anthropic
            self.api_key = api_key or ANTHROPIC_KEY
            if not self.api_key:
                raise ValueError("Anthropic API Key não configurada")
            self.client = Anthropic(api_key=self.api_key)
            
        elif self.provider == 'gemini':
            import google.generativeai as genai
            self.api_key = api_key or GEMINI_API_KEY
            if not self.api_key:
                raise ValueError("Gemini API Key não configurada")
            genai.configure(api_key=self.api_key)
            self.client = genai.GenerativeModel(self.model)
        
        else:
            raise ValueError(f"Provider '{self.provider}' não suportado. Use: groq, claude ou gemini")
        
        logger.info(f"AnalisadorIA inicializado com provider: {self.provider} (modelo: {self.model})")
    
    def analisar_diario(self, conteudo_diario: str, legislacoes_monitoradas: List[Dict]) -> List[Dict]:
        """
        Analisa um diário oficial procurando por alterações nas legislações monitoradas
        
        Args:
            conteudo_diario: Texto completo do diário oficial
            legislacoes_monitoradas: Lista de legislações para monitorar
        
        Returns:
            Lista de alterações encontradas
        """
        
        if not legislacoes_monitoradas:
            return []
        
        # Criar prompt para IA
        prompt = self._criar_prompt_analise(conteudo_diario, legislacoes_monitoradas)
        
        try:
            logger.info(f"Enviando diário para análise ({self.provider})...")
            
            # Chamar IA conforme provider
            if self.provider == 'groq':
                resposta_texto = self._chamar_groq(prompt)
            elif self.provider == 'claude':
                resposta_texto = self._chamar_claude(prompt)
            elif self.provider == 'gemini':
                resposta_texto = self._chamar_gemini(prompt)
            
            # Parse da resposta JSON
            alteracoes = self._parse_resposta(resposta_texto)
            
            logger.info(f"Análise concluída. {len(alteracoes)} alteração(ões) encontrada(s).")
            
            return alteracoes
            
        except Exception as e:
            logger.error(f"Erro ao analisar diário com IA: {e}", exc_info=True)
            return []
    
    def _chamar_groq(self, prompt: str) -> str:
        """Chama API do Groq (gratuita)"""
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": "Você é um especialista em análise de legislação municipal brasileira. Responda apenas com JSON válido."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            max_tokens=MAX_TOKENS
        )
        return response.choices[0].message.content
    
    def _chamar_claude(self, prompt: str) -> str:
        """Chama API do Claude"""
        response = self.client.messages.create(
            model=self.model,
            max_tokens=MAX_TOKENS,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        return response.content[0].text
    
    def _chamar_gemini(self, prompt: str) -> str:
        """Chama API do Gemini"""
        response = self.client.generate_content(prompt)
        return response.text
    
    def _criar_prompt_analise(self, conteudo_diario: str, legislacoes: List[Dict]) -> str:
        """Cria o prompt para análise do diário"""
        
        # Limitar tamanho do conteúdo para não exceder limites da API
        if len(conteudo_diario) > 180000:  # ~45k tokens
            conteudo_diario = conteudo_diario[:180000] + "\n\n[CONTEÚDO TRUNCADO]"
        
        # Montar lista de legislações monitoradas
        lista_legislacoes = []
        for leg in legislacoes:
            lista_legislacoes.append(
                f"- {leg['tipo']} nº {leg['numero']}/{leg['ano']}"
            )
        
        legislacoes_texto = "\n".join(lista_legislacoes)
        
        prompt = f"""Você é um assistente especializado em análise de legislação municipal brasileira.

Sua tarefa é analisar o conteúdo de um Diário Oficial Municipal e identificar se há QUALQUER menção, alteração, revogação, modificação ou referência às seguintes legislações que estão sendo monitoradas:

LEGISLAÇÕES MONITORADAS:
{legislacoes_texto}

CONTEÚDO DO DIÁRIO OFICIAL:
{conteudo_diario}

INSTRUÇÕES:
1. Leia TODO o conteúdo do diário oficial cuidadosamente
2. Procure por qualquer menção às legislações listadas acima
3. Identifique especialmente atos que ALTERAM, MODIFICAM, REVOGAM, DERROGAM, ACRESCENTAM, SUPRIMEM, SUBSTITUEM ou DÃO NOVA REDAÇÃO às legislações monitoradas
4. Para cada alteração encontrada, você deve:
   - Identificar o tipo e número da legislação ALTERADORA (a nova lei/decreto)
   - Identificar qual legislação ORIGINAL está sendo alterada
   - Classificar o tipo de alteração (alteração, revogação, acréscimo, etc.)
   - Criar um resumo claro e objetivo da alteração
   - Extrair o texto completo do ato alterador
   - **IMPORTANTE: Extrair a EMENTA completa da legislação alteradora**

PALAVRAS-CHAVE IMPORTANTES:
{', '.join(KEYWORDS_ALTERACAO)}

FORMATO DE RESPOSTA:
Responda APENAS com um JSON válido no seguinte formato (array vazio se nada for encontrado):

[
  {{
    "legislacao_original": {{
      "tipo": "Lei",
      "numero": "1234",
      "ano": 2020
    }},
    "legislacao_alteradora": {{
      "tipo": "Decreto",
      "numero": "5678",
      "ano": 2024,
      "ementa": "Texto completo da ementa da nova legislação"
    }},
    "tipo_alteracao": "alteração|revogação|acréscimo|modificação",
    "resumo": "Breve resumo objetivo da alteração (2-3 frases)",
    "conteudo_completo": "Texto completo do ato que faz a alteração",
    "data_publicacao": "YYYY-MM-DD"
  }}
]

IMPORTANTE:
- Se NÃO encontrar NENHUMA alteração, retorne um array vazio: []
- Não inclua comentários ou explicações, apenas o JSON
- Seja rigoroso: só inclua se houver CERTEZA de que é uma alteração à legislação monitorada
- Ignore menções que apenas citam a lei sem alterá-la
- SEMPRE extraia a ementa completa da legislação alteradora
"""
        
        return prompt
    
    def _parse_resposta(self, resposta_texto: str) -> List[Dict]:
        """Parse da resposta JSON da IA"""
        
        try:
            # Remover possíveis marcadores de código
            resposta_limpa = resposta_texto.strip()
            if resposta_limpa.startswith('```json'):
                resposta_limpa = resposta_limpa[7:]
            if resposta_limpa.startswith('```'):
                resposta_limpa = resposta_limpa[3:]
            if resposta_limpa.endswith('```'):
                resposta_limpa = resposta_limpa[:-3]
            
            resposta_limpa = resposta_limpa.strip()
            
            # Parse JSON
            alteracoes = json.loads(resposta_limpa)
            
            # Validar estrutura
            if not isinstance(alteracoes, list):
                logger.warning("Resposta da IA não é uma lista")
                return []
            
            return alteracoes
            
        except json.JSONDecodeError as e:
            logger.error(f"Erro ao fazer parse da resposta JSON: {e}")
            logger.error(f"Resposta recebida: {resposta_texto[:500]}")
            return []
    
    def extrair_ementa_de_texto(self, texto_legislacao: str) -> Optional[str]:
        """
        Extrai a ementa de um texto de legislação
        Útil para quando precisar extrair ementa de PDFs/HTML
        """
        
        # Padrões comuns de ementa
        padroes_ementa = [
            r'Ementa[:\s]+(.*?)(?:\n\n|\n[A-Z])',
            r'(?:Dispõe sobre|Estabelece|Institui|Regulamenta|Altera|Revoga)(.*?)(?:\n\n|\n[A-Z])',
        ]
        
        for padrao in padroes_ementa:
            match = re.search(padrao, texto_legislacao, re.IGNORECASE | re.DOTALL)
            if match:
                ementa = match.group(1).strip()
                # Limitar tamanho
                if len(ementa) > 500:
                    ementa = ementa[:500] + "..."
                return ementa
        
        return None
    
    def gerar_resumo_legislacao(self, conteudo: str) -> str:
        """Gera um resumo de uma legislação usando IA"""
        
        prompt = f"""Você é um assistente especializado em legislação brasileira.

Crie um resumo objetivo e claro da seguinte legislação:

{conteudo}

O resumo deve ter 2-3 parágrafos e cobrir:
1. O que a legislação estabelece (objetivo principal)
2. Principais disposições ou mudanças
3. A quem se aplica ou quem é afetado

Seja claro, objetivo e use linguagem acessível.
"""
        
        try:
            if self.provider == 'groq':
                return self._chamar_groq(prompt)
            elif self.provider == 'claude':
                return self._chamar_claude(prompt)
            elif self.provider == 'gemini':
                return self._chamar_gemini(prompt)
                
        except Exception as e:
            logger.error(f"Erro ao gerar resumo: {e}")
            return "Não foi possível gerar o resumo automaticamente."
