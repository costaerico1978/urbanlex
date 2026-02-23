#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
modulos/scraper.py
──────────────────
Módulo de scraping de Diários Oficiais.
Adaptado do v1.3 para UrbanLex unificado (sem dependência de config.py).
"""

import os
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import time
import logging
import io

logger = logging.getLogger(__name__)

# Configurações (via env ou defaults)
USER_AGENT      = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
REQUEST_TIMEOUT = int(os.getenv('SCRAPER_TIMEOUT', '30'))
RETRY_ATTEMPTS  = int(os.getenv('SCRAPER_RETRIES', '3'))
RETRY_DELAY     = int(os.getenv('SCRAPER_DELAY',   '5'))

import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import time
import logging
from pathlib import Path
import PyPDF2
import io


logger = logging.getLogger(__name__)

class DiarioScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': USER_AGENT})
    
    def analisar_site(self, url: str) -> Dict:
        """
        Analisa um site de diário oficial e tenta descobrir como extrair conteúdo
        Retorna configuração e tipo do site
        """
        logger.info(f"Analisando site: {url}")
        
        try:
            response = self.session.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Detectar plataforma conhecida
            tipo_detectado = self._detectar_plataforma(url, soup)
            
            if tipo_detectado:
                logger.info(f"Plataforma detectada: {tipo_detectado}")
                return {
                    'tipo': tipo_detectado,
                    'url_base': url,
                    'sucesso': True,
                    'mensagem': f'Plataforma {tipo_detectado} detectada automaticamente'
                }
            
            # Tentar análise genérica
            config_generica = self._analisar_genericamente(url, soup)
            
            if config_generica['encontrou_padroes']:
                return {
                    'tipo': 'generico',
                    'config': config_generica,
                    'sucesso': True,
                    'mensagem': 'Padrões detectados automaticamente'
                }
            
            # Se não conseguiu detectar automaticamente
            return {
                'tipo': 'manual',
                'sucesso': False,
                'mensagem': 'Não foi possível detectar automaticamente. Necessária configuração manual.',
                'html_sample': str(soup.prettify())[:2000]
            }
            
        except Exception as e:
            logger.error(f"Erro ao analisar site: {e}")
            return {
                'tipo': 'erro',
                'sucesso': False,
                'mensagem': f'Erro ao acessar site: {str(e)}'
            }
    
    def _detectar_plataforma(self, url: str, soup: BeautifulSoup) -> Optional[str]:
        """Detecta se é uma plataforma conhecida"""
        
        # DOM - Diário Oficial dos Municípios
        if 'dom.pmp.sp.gov.br' in url or 'diariomunicipal.com.br' in url:
            return 'dom'
        
        # IOBNET
        if 'iobnet' in url or 'imprensaoficial' in url:
            return 'iobnet'
        
        # AMM - Associação Mineira de Municípios
        if 'diariomunicipal.org' in url or 'portalamm' in url:
            return 'amm'
        
        # Detectar por conteúdo HTML
        html_text = str(soup).lower()
        
        if 'diariomunicipal' in html_text and 'dom-ml' in html_text:
            return 'dom'
        
        if 'iobnet' in html_text:
            return 'iobnet'
        
        return None
    
    def _analisar_genericamente(self, url: str, soup: BeautifulSoup) -> Dict:
        """Tenta analisar site genérico e encontrar padrões"""
        
        config = {
            'encontrou_padroes': False,
            'links_pdf': [],
            'links_edicao': [],
            'seletores_data': [],
            'estrutura': 'desconhecida'
        }
        
        # Procurar por links de PDF
        links_pdf = soup.find_all('a', href=re.compile(r'\.pdf$', re.I))
        if links_pdf:
            config['links_pdf'] = [link.get('href') for link in links_pdf[:5]]
            config['encontrou_padroes'] = True
            config['estrutura'] = 'links_pdf_diretos'
        
        # Procurar por padrões de data
        padroes_data = [
            r'\d{2}/\d{2}/\d{4}',  # dd/mm/yyyy
            r'\d{4}-\d{2}-\d{2}',  # yyyy-mm-dd
            r'data-date',
            r'data-publicacao'
        ]
        
        for padrao in padroes_data:
            elementos = soup.find_all(text=re.compile(padrao))
            if elementos:
                config['seletores_data'].append(padrao)
        
        # Procurar por estrutura de edições
        possiveis_edicoes = soup.find_all(['div', 'li', 'tr'], 
                                         class_=re.compile(r'edicao|diario|publicacao', re.I))
        if possiveis_edicoes:
            config['estrutura'] = 'lista_edicoes'
            config['encontrou_padroes'] = True
        
        return config
    
    def baixar_diario(self, url: str, tipo_site: str, config_extracao: Dict, 
                     data_alvo: datetime) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Baixa o diário oficial para uma data específica
        Retorna: (sucesso, conteúdo_texto, mensagem_erro)
        """
        
        try:
            if tipo_site == 'dom':
                return self._baixar_dom(url, data_alvo)
            elif tipo_site == 'iobnet':
                return self._baixar_iobnet(url, data_alvo)
            elif tipo_site == 'amm':
                return self._baixar_amm(url, data_alvo)
            elif tipo_site == 'generico':
                return self._baixar_generico(url, config_extracao, data_alvo)
            else:
                return False, None, "Tipo de site não suportado"
                
        except Exception as e:
            logger.error(f"Erro ao baixar diário: {e}")
            return False, None, str(e)
    
    def _baixar_dom(self, url: str, data: datetime) -> Tuple[bool, Optional[str], Optional[str]]:
        """Baixa diário da plataforma DOM"""
        # Implementação específica para DOM
        # Este é um exemplo - você precisaria ajustar para a URL específica
        
        try:
            # Formato típico: https://diariomunicipal.com.br/{municipio}/edicoes/{ano}/{mes}/{dia}
            data_str = data.strftime("%Y/%m/%d")
            url_edicao = f"{url}/edicoes/{data_str}"
            
            response = self.session.get(url_edicao, timeout=REQUEST_TIMEOUT)
            
            if response.status_code == 404:
                return False, None, "Diário não disponível para esta data"
            
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Procurar link do PDF
            link_pdf = soup.find('a', href=re.compile(r'\.pdf$', re.I))
            
            if link_pdf:
                pdf_url = link_pdf.get('href')
                if not pdf_url.startswith('http'):
                    pdf_url = url + pdf_url
                
                conteudo = self._extrair_texto_pdf(pdf_url)
                return True, conteudo, None
            
            # Se não tem PDF, tentar extrair HTML
            conteudo_html = soup.get_text(separator='\n', strip=True)
            return True, conteudo_html, None
            
        except Exception as e:
            return False, None, str(e)
    
    def _baixar_iobnet(self, url: str, data: datetime) -> Tuple[bool, Optional[str], Optional[str]]:
        """Baixa diário da plataforma IOBNET"""
        # Implementação específica para IOBNET
        return False, None, "Plataforma IOBNET ainda não implementada. Configure manualmente."
    
    def _baixar_amm(self, url: str, data: datetime) -> Tuple[bool, Optional[str], Optional[str]]:
        """Baixa diário da plataforma AMM"""
        # Implementação específica para AMM
        return False, None, "Plataforma AMM ainda não implementada. Configure manualmente."
    
    def _baixar_generico(self, url: str, config_extracao: Dict, 
                        data: datetime) -> Tuple[bool, Optional[str], Optional[str]]:
        """Baixa diário de site genérico usando configuração fornecida"""
        
        try:
            response = self.session.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Tentar encontrar PDF para a data
            if config_extracao.get('estrutura') == 'links_pdf_diretos':
                links_pdf = soup.find_all('a', href=re.compile(r'\.pdf$', re.I))
                
                # Procurar PDF da data específica
                data_str_formatos = [
                    data.strftime("%d-%m-%Y"),
                    data.strftime("%d/%m/%Y"),
                    data.strftime("%Y-%m-%d"),
                    data.strftime("%d%m%Y")
                ]
                
                for link in links_pdf:
                    href = link.get('href', '')
                    texto = link.get_text()
                    
                    for formato in data_str_formatos:
                        if formato in href or formato in texto:
                            pdf_url = href if href.startswith('http') else url + href
                            conteudo = self._extrair_texto_pdf(pdf_url)
                            return True, conteudo, None
            
            # Se não encontrou, retornar todo o conteúdo HTML
            conteudo = soup.get_text(separator='\n', strip=True)
            return True, conteudo, "Conteúdo completo da página (verificar se é o diário correto)"
            
        except Exception as e:
            return False, None, str(e)
    
    def _extrair_texto_pdf(self, url: str) -> str:
        """Extrai texto de um PDF"""
        try:
            response = self.session.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            
            pdf_file = io.BytesIO(response.content)
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            
            texto = ""
            for page in pdf_reader.pages:
                texto += page.extract_text() + "\n"
            
            return texto
            
        except Exception as e:
            logger.error(f"Erro ao extrair texto do PDF: {e}")
            return f"[ERRO ao extrair PDF: {str(e)}]"
    
    def buscar_diarios_periodo(self, url: str, tipo_site: str, config_extracao: Dict,
                               data_inicio: datetime, data_fim: datetime) -> List[Dict]:
        """
        Busca diários oficiais em um período de datas
        Retorna lista com informações de cada diário encontrado
        """
        
        diarios = []
        data_atual = data_inicio
        
        while data_atual <= data_fim:
            sucesso, conteudo, erro = self.baixar_diario(url, tipo_site, config_extracao, data_atual)
            
            if sucesso and conteudo:
                diarios.append({
                    'data': data_atual.strftime("%Y-%m-%d"),
                    'conteudo': conteudo,
                    'sucesso': True
                })
            else:
                diarios.append({
                    'data': data_atual.strftime("%Y-%m-%d"),
                    'conteudo': None,
                    'sucesso': False,
                    'erro': erro
                })
            
            data_atual += timedelta(days=1)
            time.sleep(1)  # Evitar sobrecarga no servidor
        
        return diarios
