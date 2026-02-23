#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Calculador de Área Computável - v2.2 FINAL
Sistema para calcular área computável básica e máxima de terrenos

Autor: Sistema Urbanístico
Data: 22/02/2026
Versão: 2.2 FINAL
"""

import math
from typing import Dict, Optional, Any


class CalculadorAreaComputavel:
    """
    Calcula área computável básica e máxima para terrenos
    
    Suporta múltiplas metodologias:
    - CA direto (simples)
    - TO × Gabarito (CA implícito)
    - Geométrico (afastamentos)
    """
    
    # Usos válidos
    USOS_VALIDOS = [
        'ResUnif',   # Residencial Unifamiliar
        'ResMult',   # Residencial Multifamiliar
        'ResHIS',    # Residencial HIS
        'Com',       # Comercial
        'Serv',      # Serviços
        'Misto',     # Misto
        'Ind',       # Industrial
        'Inst'       # Institucional
    ]
    
    # Proporção padrão para cálculo de testada/profundidade
    PROPORCAO_PADRAO = 2.5  # 1:2.5 (testada:profundidade)
    
    def __init__(self, proporcao_lote: float = 2.5):
        """
        Inicializa o calculador
        
        Args:
            proporcao_lote: Proporção padrão testada:profundidade (default 2.5)
        """
        self.proporcao_lote = proporcao_lote
    
    def calcular(
        self, 
        lote: Dict[str, Any], 
        zona: Dict[str, Any], 
        uso: str
    ) -> Dict[str, Optional[float]]:
        """
        Calcula área computável básica e máxima
        
        Args:
            lote (dict): Dados do lote
                {
                    'area': 1000,        # m² (obrigatório)
                    'testada': 20,       # m (opcional)
                    'profundidade': 50   # m (opcional)
                }
            
            zona (dict): Dados da zona (linha da planilha)
                Deve conter:
                - Formula_Area_Computavel_Basica
                - Formula_Area_Computavel_Maxima
                - Parâmetros específicos do uso (CA_basico_[uso], etc)
            
            uso (str): Tipo de uso
                Valores: 'ResUnif', 'ResMult', 'ResHIS', 'Com', 
                         'Serv', 'Misto', 'Ind', 'Inst'
        
        Returns:
            dict: {
                'area_basica': float ou None,
                'area_maxima': float ou None,
                'outorga_necessaria': float ou None,
                'mensagem': str (opcional, se erro)
            }
        
        Raises:
            ValueError: Se parâmetros inválidos
        
        Examples:
            >>> calc = CalculadorAreaComputavel()
            >>> lote = {'area': 1000}
            >>> zona = {
            ...     'Formula_Area_Computavel_Basica': 'Area_Lote * CA_basico',
            ...     'Formula_Area_Computavel_Maxima': 'Area_Lote * CA_maximo',
            ...     'CA_basico_Com': 2.0,
            ...     'CA_maximo_Com': 3.5
            ... }
            >>> resultado = calc.calcular(lote, zona, 'Com')
            >>> print(resultado)
            {'area_basica': 2000.0, 'area_maxima': 3500.0, 'outorga_necessaria': 1500.0}
        """
        
        # ===== 1. VALIDAÇÕES =====
        try:
            self._validar_lote(lote)
            self._validar_uso(uso)
        except ValueError as e:
            return {
                'area_basica': None,
                'area_maxima': None,
                'outorga_necessaria': None,
                'mensagem': str(e)
            }
        
        # ===== 2. BUSCAR FÓRMULAS =====
        formula_basica = zona.get('Formula_Area_Computavel_Basica')
        formula_maxima = zona.get('Formula_Area_Computavel_Maxima')
        
        if not formula_basica or not formula_maxima:
            return {
                'area_basica': None,
                'area_maxima': None,
                'outorga_necessaria': None,
                'mensagem': 'Fórmulas não definidas para esta zona'
            }
        
        # ===== 3. PREPARAR VARIÁVEIS =====
        parametros = self._preparar_parametros(lote, zona, uso)
        
        # ===== 4. VERIFICAR SE USO É PERMITIDO =====
        if not self._uso_permitido(parametros):
            return {
                'area_basica': None,
                'area_maxima': None,
                'outorga_necessaria': None,
                'mensagem': 'Uso não permitido nesta zona'
            }
        
        # ===== 5. CALCULAR ÁREA BÁSICA =====
        area_basica, erro_basica = self._calcular_area(
            formula_basica, parametros, 'básica'
        )
        
        if erro_basica:
            return {
                'area_basica': None,
                'area_maxima': None,
                'outorga_necessaria': None,
                'mensagem': erro_basica
            }
        
        # ===== 6. CALCULAR ÁREA MÁXIMA =====
        area_maxima, erro_maxima = self._calcular_area(
            formula_maxima, parametros, 'máxima'
        )
        
        if erro_maxima:
            return {
                'area_basica': area_basica,
                'area_maxima': None,
                'outorga_necessaria': None,
                'mensagem': erro_maxima
            }
        
        # ===== 7. VALIDAR RESULTADOS =====
        try:
            self._validar_resultados(area_basica, area_maxima, lote['area'])
        except ValueError as e:
            return {
                'area_basica': area_basica,
                'area_maxima': area_maxima,
                'outorga_necessaria': None,
                'mensagem': f'Validação falhou: {str(e)}'
            }
        
        # ===== 8. CALCULAR OUTORGA =====
        outorga = self._calcular_outorga(area_basica, area_maxima)
        
        # ===== 9. CALCULAR ÁREA PRIVATIVA =====
        area_privativa_basica = self._calcular_area_privativa(area_basica, zona, uso)
        area_privativa_maxima = self._calcular_area_privativa(area_maxima, zona, uso)
        
        # ===== 10. RETORNAR RESULTADO =====
        return {
            'area_basica': area_basica,
            'area_maxima': area_maxima,
            'outorga_necessaria': outorga,
            'area_privativa_basica': area_privativa_basica,
            'area_privativa_maxima': area_privativa_maxima,
            'metodologia': zona.get(f'Metodologia_Area_Computavel_{uso}'),
            'fator_conversao': zona.get(f'Fator_Privativa_Computavel_{uso}')
        }
    
    def _validar_lote(self, lote: Dict[str, Any]) -> None:
        """Valida dados do lote"""
        if not isinstance(lote, dict):
            raise ValueError("Lote deve ser um dicionário")
        
        if 'area' not in lote:
            raise ValueError("Área do lote é obrigatória")
        
        if not isinstance(lote['area'], (int, float)) or lote['area'] <= 0:
            raise ValueError("Área deve ser um número positivo")
        
        if lote.get('testada') is not None:
            if not isinstance(lote['testada'], (int, float)) or lote['testada'] <= 0:
                raise ValueError("Testada deve ser um número positivo")
        
        if lote.get('profundidade') is not None:
            if not isinstance(lote['profundidade'], (int, float)) or lote['profundidade'] <= 0:
                raise ValueError("Profundidade deve ser um número positivo")
        
        # Validar coerência área vs testada×profundidade
        if lote.get('testada') and lote.get('profundidade'):
            area_calculada = lote['testada'] * lote['profundidade']
            tolerancia = lote['area'] * 0.05  # 5%
            if abs(area_calculada - lote['area']) > tolerancia:
                raise ValueError(
                    f"Área informada ({lote['area']}) não bate com "
                    f"testada × profundidade ({area_calculada})"
                )
    
    def _validar_uso(self, uso: str) -> None:
        """Valida tipo de uso"""
        if uso not in self.USOS_VALIDOS:
            raise ValueError(
                f"Uso inválido: {uso}. "
                f"Valores válidos: {', '.join(self.USOS_VALIDOS)}"
            )
    
    def _preparar_parametros(
        self, 
        lote: Dict[str, Any], 
        zona: Dict[str, Any], 
        uso: str
    ) -> Dict[str, Any]:
        """
        Prepara dicionário de variáveis para avaliar fórmula
        
        Returns:
            dict com todas as variáveis disponíveis
        """
        area_lote = lote['area']
        testada = lote.get('testada')
        profundidade = lote.get('profundidade')
        
        # Calcular testada e profundidade se não fornecidas
        if not testada and not profundidade:
            # Usar proporção padrão
            testada = math.sqrt(area_lote / self.proporcao_lote)
            profundidade = testada * self.proporcao_lote
        elif testada and not profundidade:
            profundidade = area_lote / testada
        elif profundidade and not testada:
            testada = area_lote / profundidade
        # else: ambos fornecidos, usar como estão
        
        # Montar dicionário de parâmetros
        parametros = {
            # Dados do lote
            'Area_Lote': area_lote,
            'Testada': testada,
            'Profundidade': profundidade,
            
            # Parâmetros BÁSICOS do uso
            'CA_basico': zona.get(f'CA_basico_{uso}'),
            'TO_basica': zona.get(f'TO_basica_{uso}'),
            'Gabarito_pavtos_basico': zona.get(f'Gabarito_pavtos_basico_{uso}'),
            'Gabarito_metros_basico': zona.get(f'Gabarito_metros_basico_{uso}'),
            
            # Parâmetros MÁXIMOS do uso
            'CA_maximo': zona.get(f'CA_maximo_{uso}'),
            'TO_maxima': zona.get(f'TO_maxima_{uso}'),
            'Gabarito_pavtos_maximo': zona.get(f'Gabarito_pavtos_maximo_{uso}'),
            'Gabarito_metros_maximo': zona.get(f'Gabarito_metros_maximo_{uso}'),
            
            # Afastamentos (geralmente iguais para básico e máximo)
            'Afastamento_frontal': zona.get(f'Afastamento_frontal_{uso}'),
            'Afastamento_lateral': zona.get(f'Afastamento_lateral_{uso}'),
            'Afastamento_fundos': zona.get(f'Afastamento_fundos_{uso}'),
        }
        
        # Converter N/A e vazios para None
        for key in parametros:
            if parametros[key] in ['N/A', '', None]:
                parametros[key] = None
        
        return parametros
    
    def _uso_permitido(self, parametros: Dict[str, Any]) -> bool:
        """Verifica se uso é permitido na zona"""
        # Se todos os parâmetros principais são None, uso não é permitido
        principais = [
            parametros.get('CA_basico'),
            parametros.get('CA_maximo'),
            parametros.get('TO_basica'),
            parametros.get('TO_maxima'),
            parametros.get('Gabarito_pavtos_basico'),
            parametros.get('Gabarito_pavtos_maximo'),
            parametros.get('Afastamento_frontal'),
        ]
        
        # Se pelo menos um parâmetro principal existe, uso é permitido
        return any(p is not None for p in principais)
    
    def _calcular_area(
        self, 
        formula: str, 
        parametros: Dict[str, Any],
        tipo: str
    ) -> tuple[Optional[float], Optional[str]]:
        """
        Calcula área usando a fórmula
        
        Returns:
            (area, erro): Tupla com área calculada e mensagem de erro (se houver)
        """
        try:
            # Contexto seguro para eval (sem builtins perigosos)
            contexto = {
                "__builtins__": {},
                # Funções matemáticas permitidas
                "sqrt": math.sqrt,
                "pow": math.pow,
                "abs": abs,
                "min": min,
                "max": max,
            }
            
            # Avaliar fórmula
            resultado = eval(formula, contexto, parametros)
            
            # Converter para float
            if resultado is not None:
                resultado = float(resultado)
            
            return resultado, None
            
        except ZeroDivisionError:
            return None, f'Divisão por zero ao calcular área {tipo}'
        
        except NameError as e:
            return None, f'Variável não encontrada na fórmula {tipo}: {e}'
        
        except SyntaxError:
            return None, f'Fórmula {tipo} inválida (erro de sintaxe)'
        
        except TypeError as e:
            return None, f'Erro de tipo ao calcular área {tipo}: {e}'
        
        except Exception as e:
            return None, f'Erro ao calcular área {tipo}: {e}'
    
    def _validar_resultados(
        self, 
        area_basica: Optional[float], 
        area_maxima: Optional[float],
        area_lote: float
    ) -> None:
        """Valida coerência dos resultados"""
        
        # Áreas não podem ser negativas
        if area_basica is not None and area_basica < 0:
            raise ValueError("Área básica negativa (verificar afastamentos)")
        
        if area_maxima is not None and area_maxima < 0:
            raise ValueError("Área máxima negativa (verificar afastamentos)")
        
        # Máxima >= Básica
        if (area_basica is not None and area_maxima is not None and 
            area_maxima < area_basica):
            raise ValueError(
                f"Área máxima ({area_maxima:.1f}) menor que básica ({area_basica:.1f})"
            )
        
        # Não pode ser absurdamente grande (>15x área lote)
        if area_basica is not None and area_basica > area_lote * 15:
            raise ValueError(
                f"Área básica muito grande ({area_basica:.1f} m² = "
                f"{area_basica/area_lote:.1f}x lote)"
            )
        
        if area_maxima is not None and area_maxima > area_lote * 20:
            raise ValueError(
                f"Área máxima muito grande ({area_maxima:.1f} m² = "
                f"{area_maxima/area_lote:.1f}x lote)"
            )
    
    def _calcular_outorga(
        self, 
        area_basica: Optional[float], 
        area_maxima: Optional[float]
    ) -> Optional[float]:
        """Calcula valor da outorga onerosa necessária"""
        if area_basica is not None and area_maxima is not None:
            # Outorga nunca é negativa
            return max(0, area_maxima - area_basica)
        return None
    
    def _calcular_area_privativa(
        self,
        area_computavel: Optional[float],
        zona: Dict[str, Any],
        uso: str
    ) -> Optional[float]:
        """
        Calcula área privativa estimada a partir da área computável
        
        Args:
            area_computavel: Área computável em m²
            zona: Dados da zona
            uso: Tipo de uso
        
        Returns:
            Área privativa estimada ou None
        """
        if area_computavel is None:
            return None
        
        # Buscar fator de conversão
        fator = zona.get(f'Fator_Privativa_Computavel_{uso}')
        
        if fator is None or fator == 'N/A' or fator == '':
            return None
        
        try:
            fator_num = float(fator)
            return area_computavel * fator_num
        except (ValueError, TypeError):
            return None


# ========== FUNÇÕES DE CONVENIÊNCIA ==========

def calcular_areas_computaveis(
    lote: Dict[str, Any], 
    zona: Dict[str, Any], 
    uso: str,
    proporcao_lote: float = 2.5
) -> Dict[str, Optional[float]]:
    """
    Função de conveniência para calcular áreas computáveis
    
    Args:
        lote: Dados do lote {'area': 1000, 'testada': 20}
        zona: Dados da zona (da planilha)
        uso: Tipo de uso ('Com', 'ResUnif', etc)
        proporcao_lote: Proporção testada:profundidade (default 2.5)
    
    Returns:
        dict com area_basica, area_maxima e outorga_necessaria
    
    Example:
        >>> resultado = calcular_areas_computaveis(
        ...     lote={'area': 1000},
        ...     zona={'Formula_Area_Computavel_Basica': 'Area_Lote * CA_basico',
        ...           'Formula_Area_Computavel_Maxima': 'Area_Lote * CA_maximo',
        ...           'CA_basico_Com': 2.0,
        ...           'CA_maximo_Com': 3.5},
        ...     uso='Com'
        ... )
        >>> print(resultado)
        {'area_basica': 2000.0, 'area_maxima': 3500.0, 'outorga_necessaria': 1500.0}
    """
    calc = CalculadorAreaComputavel(proporcao_lote=proporcao_lote)
    return calc.calcular(lote, zona, uso)


# ========== TESTES ==========

if __name__ == "__main__":
    print("="*80)
    print("TESTES DO CALCULADOR DE ÁREA COMPUTÁVEL v2.2")
    print("="*80)
    
    calc = CalculadorAreaComputavel()
    
    # TESTE 1: CA Direto (RJ ZRM2)
    print("\n[TESTE 1] Zona com CA Direto (RJ ZRM2 - Comercial)")
    zona_zrm2 = {
        'Formula_Area_Computavel_Basica': 'Area_Lote * CA_basico',
        'Formula_Area_Computavel_Maxima': 'Area_Lote * CA_maximo',
        'CA_basico_Com': 2.0,
        'CA_maximo_Com': 3.5,
        'Metodologia_Area_Computavel_Com': 'Área privativa + circulação horizontal e vertical',
        'Fator_Privativa_Computavel_Com': 0.85,
    }
    lote1 = {'area': 1000}
    
    resultado1 = calc.calcular(lote1, zona_zrm2, 'Com')
    print(f"  Área Básica: {resultado1['area_basica']} m²")
    print(f"  Área Máxima: {resultado1['area_maxima']} m²")
    print(f"  Outorga: {resultado1['outorga_necessaria']} m²")
    print(f"  Privativa Básica: {resultado1['area_privativa_basica']} m²")
    print(f"  Privativa Máxima: {resultado1['area_privativa_maxima']} m²")
    assert resultado1['area_basica'] == 2000.0
    assert resultado1['area_maxima'] == 3500.0
    assert resultado1['outorga_necessaria'] == 1500.0
    assert resultado1['area_privativa_basica'] == 1700.0  # 2000 * 0.85
    assert resultado1['area_privativa_maxima'] == 2975.0  # 3500 * 0.85
    print("  ✅ PASSOU")
    
    # TESTE 2: TO × Gabarito (SP ZC)
    print("\n[TESTE 2] Zona com TO × Gabarito (SP ZC - Comercial)")
    zona_zc = {
        'Formula_Area_Computavel_Basica': 'Area_Lote * TO_basica * Gabarito_pavtos_basico',
        'Formula_Area_Computavel_Maxima': 'Area_Lote * TO_maxima * Gabarito_pavtos_maximo',
        'TO_basica_Com': 0.6,
        'Gabarito_pavtos_basico_Com': 4,
        'TO_maxima_Com': 0.8,
        'Gabarito_pavtos_maximo_Com': 6,
    }
    
    resultado2 = calc.calcular(lote1, zona_zc, 'Com')
    print(f"  Área Básica: {resultado2['area_basica']} m²")
    print(f"  Área Máxima: {resultado2['area_maxima']} m²")
    print(f"  Outorga: {resultado2['outorga_necessaria']} m²")
    assert resultado2['area_basica'] == 2400.0
    assert resultado2['area_maxima'] == 4800.0
    assert resultado2['outorga_necessaria'] == 2400.0
    print("  ✅ PASSOU")
    
    # TESTE 3: Geométrico (Curitiba ZR3)
    print("\n[TESTE 3] Zona Geométrica (Curitiba ZR3 - ResUnif)")
    zona_zr3 = {
        'Formula_Area_Computavel_Basica': '(Testada - 2*Afastamento_lateral) * (Profundidade - Afastamento_frontal - Afastamento_fundos) * Gabarito_pavtos_basico',
        'Formula_Area_Computavel_Maxima': '(Testada - 2*Afastamento_lateral) * (Profundidade - Afastamento_frontal - Afastamento_fundos) * Gabarito_pavtos_maximo',
        'Afastamento_frontal_ResUnif': 5,
        'Afastamento_lateral_ResUnif': 3,
        'Afastamento_fundos_ResUnif': 5,
        'Gabarito_pavtos_basico_ResUnif': 2,
        'Gabarito_pavtos_maximo_ResUnif': 3,
    }
    lote3 = {'area': 1000, 'testada': 20, 'profundidade': 50}
    
    resultado3 = calc.calcular(lote3, zona_zr3, 'ResUnif')
    print(f"  Testada: 20m, Profundidade: 50m")
    print(f"  Área edificável: (20-6) × (50-10) = 14m × 40m = 560 m²")
    print(f"  Área Básica: {resultado3['area_basica']} m² (560 × 2)")
    print(f"  Área Máxima: {resultado3['area_maxima']} m² (560 × 3)")
    print(f"  Outorga: {resultado3['outorga_necessaria']} m²")
    assert resultado3['area_basica'] == 1120.0
    assert resultado3['area_maxima'] == 1680.0
    assert resultado3['outorga_necessaria'] == 560.0
    print("  ✅ PASSOU")
    
    # TESTE 4: Uso não permitido
    print("\n[TESTE 4] Uso Não Permitido (Industrial com N/A)")
    zona_sem_ind = {
        'Formula_Area_Computavel_Basica': 'Area_Lote * CA_basico',
        'Formula_Area_Computavel_Maxima': 'Area_Lote * CA_maximo',
        'CA_basico_Ind': 'N/A',
        'CA_maximo_Ind': 'N/A',
    }
    
    resultado4 = calc.calcular(lote1, zona_sem_ind, 'Ind')
    print(f"  Resultado: {resultado4['mensagem']}")
    assert resultado4['area_basica'] is None
    assert resultado4['area_maxima'] is None
    assert 'não permitido' in resultado4['mensagem']
    print("  ✅ PASSOU")
    
    print("\n" + "="*80)
    print("TODOS OS TESTES PASSARAM! ✅")
    print("="*80)
    print("\nSistema pronto para uso em produção!")
