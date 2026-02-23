#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
importar_excel_para_db.py — UrbanLex v2.3
Importa planilha de zonas urbanas para PostgreSQL (Railway).

Uso no Railway Shell:
    python importar_excel_para_db.py ARQUIVO.xlsx
    python importar_excel_para_db.py ARQUIVO.xlsx --sobrescrever

Dependências:
    pip install pandas openpyxl psycopg2-binary --break-system-packages
"""

import os
import sys
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from typing import Dict, List, Any

# ─── Conexão: usa DATABASE_URL do Railway automaticamente ───────────────────

def _get_conn():
    database_url = os.getenv('DATABASE_URL')
    if database_url:
        return psycopg2.connect(database_url)
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        database=os.getenv('DB_NAME', 'urbanismo'),
        user=os.getenv('DB_USER', 'postgres'),
        password=os.getenv('DB_PASSWORD', ''),
    )


class ImportadorZonas:
    """Importa zonas urbanas do Excel para PostgreSQL"""

    MAPEAMENTO_COLUNAS = {
        'Pais': 'pais', 'Estado': 'estado', 'Municipio': 'municipio',
        'Area_Planejamento': 'area_planejamento',
        'Zona': 'zona', 'Subzona': 'subzona', 'Divisao_Subzona': 'divisao_subzona',
        'Lote_Minimo': 'lote_minimo', 'Lote_Maximo': 'lote_maximo',
        'Area_a_ser_doada': 'area_a_ser_doada',
        'Afastamento_Entre_Blocos': 'afastamento_entre_blocos',
        'Usos_Permitidos': 'usos_permitidos',
        'Uso_Residencial_Unifamiliar':   'uso_residencial_unifamiliar',
        'Uso_Residencial_Multifamiliar': 'uso_residencial_multifamiliar',
        'Uso_Residencial_HIS':           'uso_residencial_his',
        'Uso_Comercial':                 'uso_comercial',
        'Uso_Servicos':                  'uso_servicos',
        'Uso_Misto':                     'uso_misto',
        'Uso_Industrial':                'uso_industrial',
        'Uso_Institucional':             'uso_institucional',
        'Formula_Area_Computavel_Basica': 'formula_area_computavel_basica',
        'Formula_Area_Computavel_Maxima': 'formula_area_computavel_maxima',
        'Observacoes':                   'observacoes',
        'Data_Ultima_Modificacao':       'data_vigencia',
    }

    USOS = ['ResUnif', 'ResMult', 'ResHIS', 'Com', 'Serv', 'Misto', 'Ind', 'Inst']
    PARAMS_USO = [
        'CA_basico', 'CA_maximo', 'TO_basica', 'TO_maxima',
        'Gabarito_pavtos_maximo', 'Gabarito_metros_maximo',
        'Afastamento_frontal', 'Afastamento_lateral', 'Afastamento_fundos',
        'Taxa_permeabilidade',
    ]

    def importar(self, arquivo: str, sobrescrever: bool = True):
        print(f"\n{'='*60}")
        print(f"IMPORTANDO: {arquivo}")
        print(f"{'='*60}\n")

        # Ler Excel
        print("[1/4] Lendo arquivo Excel...")
        try:
            df = pd.read_excel(arquivo, sheet_name=0)
            print(f"  ✅ {len(df)} linhas encontradas")
        except Exception as e:
            print(f"  ❌ Erro: {e}"); return

        # Processar
        print("[2/4] Processando dados...")
        registros = self._processar(df)
        print(f"  ✅ {len(registros)} zonas prontas")

        # Conectar
        print("[3/4] Conectando ao banco...")
        conn = _get_conn()
        cur = conn.cursor()

        # Importar
        print("[4/4] Importando...")
        ok = erro = 0
        for dados in registros:
            if not dados.get('municipio') or not dados.get('zona'):
                continue
            try:
                if sobrescrever:
                    self._upsert(cur, dados)
                else:
                    self._insert(cur, dados)
                ok += 1
                print(f"  ✅ {dados['municipio']} — {dados['zona']}")
            except Exception as e:
                erro += 1
                conn.rollback()
                print(f"  ❌ {dados.get('municipio')} — {dados.get('zona')}: {e}")

        conn.commit()
        conn.close()

        print(f"\n{'='*60}")
        print(f"RESULTADO: {ok} importadas | {erro} erros")
        print(f"{'='*60}\n")

    def _processar(self, df: pd.DataFrame) -> List[Dict]:
        result = []
        for _, row in df.iterrows():
            reg = {}
            for col_ex, col_sql in self.MAPEAMENTO_COLUNAS.items():
                if col_ex in df.columns:
                    reg[col_sql] = self._limpar(row[col_ex])

            for uso in self.USOS:
                u = uso.lower().replace('his', 'his')
                for param in self.PARAMS_USO:
                    col_ex = f'{param}_{uso}'
                    col_sql = f'{param.lower()}_{u}'
                    if col_ex in df.columns:
                        reg[col_sql] = self._limpar(row[col_ex])

            result.append(reg)
        return result

    def _limpar(self, v: Any) -> Any:
        if pd.isna(v) if not isinstance(v, (list, dict)) else False:
            return None
        if isinstance(v, str):
            v = v.strip()
            if v.upper() in ('N/A', 'NA', ''):
                return None
            if v.lower() in ('sim', 'yes', 's', 'y', '1'):
                return True
            if v.lower() in ('não', 'nao', 'no', 'n', '0'):
                return False
        return v

    def _insert(self, cur, dados: Dict):
        cols = ', '.join(dados.keys())
        ph   = ', '.join(['%s'] * len(dados))
        cur.execute(f"INSERT INTO zonas_urbanas ({cols}) VALUES ({ph})", list(dados.values()))

    def _upsert(self, cur, dados: Dict):
        municipio       = dados.get('municipio')
        zona            = dados.get('zona')
        subzona         = dados.get('subzona') or ''
        divisao_subzona = dados.get('divisao_subzona') or ''

        cur.execute("""
            SELECT id FROM zonas_urbanas
            WHERE municipio = %s AND zona = %s
              AND COALESCE(subzona,'')         = %s
              AND COALESCE(divisao_subzona,'') = %s
        """, (municipio, zona, subzona, divisao_subzona))
        row = cur.fetchone()

        if row:
            set_cl = ', '.join([f"{k} = %s" for k in dados.keys()])
            cur.execute(f"UPDATE zonas_urbanas SET {set_cl} WHERE id = %s",
                        list(dados.values()) + [row[0]])
        else:
            self._insert(cur, dados)


def main():
    if len(sys.argv) < 2:
        print("Uso: python importar_excel_para_db.py ARQUIVO.xlsx [--sobrescrever]")
        sys.exit(1)

    arquivo = sys.argv[1]
    sobrescrever = '--sobrescrever' in sys.argv or '-s' in sys.argv

    ImportadorZonas().importar(arquivo, sobrescrever=sobrescrever)


if __name__ == '__main__':
    main()
