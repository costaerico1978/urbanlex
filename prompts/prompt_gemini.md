# Extrator de Parâmetros Urbanísticos — Gemini Pro 2.5

Você receberá páginas selecionadas de uma lei urbanística municipal brasileira em PDF. Sua tarefa é extrair e estruturar todos os dados urbanísticos em formato JSON.

---

## INSTRUÇÕES GERAIS

1. Leia o PDF integralmente antes de responder.
2. Retorne APENAS o JSON — sem texto antes ou depois, sem markdown fences.
3. Se um dado não está no PDF, use `null`. Nunca invente valores.
4. Seja exaustivo: registre TODAS as zonas que aparecem na lei.

---

## HIERARQUIA TERRITORIAL (OBRIGATÓRIO)

Toda zona deve ter sua posição na hierarquia da lei identificada:

- **UT1**: nível mais alto — geralmente Área de Planejamento (AP1, AP2, AP3, AP4, AP5) ou Macrozona. Se a lei não divide o município em áreas maiores antes das zonas, use `null`.
- **UT2**: segundo nível — geralmente o tipo de zona (ZRM, ZRU, ZCA, ZPP, etc.)
- **UT3**: terceiro nível — subzona ou variante (ZRM1D, ZRM2A, ZRU1B, etc.), se existir

Exemplos:
- AP4 > ZRM > ZRM1D → `{"UT1": "AP4", "UT2": "ZRM", "UT3": "ZRM1D"}`
- AP4 > ZPP → `{"UT1": "AP4", "UT2": "ZPP", "UT3": null}`
- AP2.1 > ZRM2 > ZRM2D → `{"UT1": "AP2.1", "UT2": "ZRM2", "UT3": "ZRM2D"}`

**COMO IDENTIFICAR UMA ZONA/VARIANTE REAL:**

Antes de registrar qualquer sigla como zona, verifique DOIS critérios:

**Critério 1 — Contexto tabular:**
A sigla aparece numa tabela ou lista onde OUTRAS ZONAS também aparecem com seus próprios valores? (ex: tabela do Anexo XXI com colunas ZRM2A, ZRM2B, ZRM2D cada uma com CA e TO próprios)
→ SIM: é uma zona real, registre como UT.

**Critério 2 — Parâmetros ou remissão:**
A sigla tem parâmetros urbanísticos definidos (CA, TO, gabarito, recuo...) OU remete os parâmetros a outra legislação (ex: "conforme Decreto 3046/1981")?
→ SIM: é uma zona real, registre como UT.

Se NENHUM dos dois critérios for atendido → NÃO registre como zona (provavelmente é apenas menção textual ou tipo base do Anexo de usos).

**ATENÇÃO — Não confunda as duas tabelas:**
- **Tabela de USOS (Anexo XVIII)**: colunas = tipos base (ZRM, ZRU, ZCA...). Os tipos base NÃO são zonas — são agrupadores para herança de usos.
- **Tabela de PARÂMETROS (Anexo XXI)**: colunas = variantes reais (ZRM2A, ZRM2B...). Estas SÃO zonas — registre cada uma como UT.

**DEFINIÇÃO — Tipo base vs Variante:**
- **Tipo base**: sigla genérica que aparece no Anexo de usos como cabeçalho (ex: ZRM, ZRU, ZCA). Agrupa variantes com usos similares. NÃO tem parâmetros próprios — serve só como referência de usos.
- **Variante**: sigla específica com parâmetros urbanísticos próprios definidos na lei (ex: ZRM2A, ZRU1B, ZCA2E). É o que realmente existe no território. Sempre entra como UT.

Regra prática: se a sigla aparece numa tabela de parâmetros (Anexo XXI, tabela de gabaritos, etc.) com valores próprios — é variante. Se aparece só no Anexo de usos como cabeçalho de coluna — é tipo base.

**REGRA CRÍTICA — Hierarquia e expansão de variantes:**
O nível só vira UT se a lei define parâmetros urbanísticos EM FUNÇÃO desse nível.

- Se os parâmetros são definidos por variante (ZRM2A, ZRM2B...) → a variante é o UT, NÃO o tipo base (ZRM).
- O tipo base (ZRM, ZRU, ZCA...) serve APENAS para identificar de qual linha do Anexo de usos as variantes herdam seus usos permitidos — ele NÃO entra como nível na hierarquia.

Exemplos CORRETOS:
  AP4 + ZRM2A com parâmetros próprios → UT1=AP4, UT2=ZRM2A (não UT2=ZRM, UT3=ZRM2A)
  AP4 + ZPP com parâmetros próprios   → UT1=AP4, UT2=ZPP

Procedimento:
1. Identifique TODAS as variantes MENCIONADAS na lei (mesmo sem parâmetros explícitos no batch — parâmetros podem ser null)
2. Para os USOS PERMITIDOS: verifique se o Anexo XVIII define usos para o tipo base (ZRM, ZRU...) e herde para cada variante
3. Registre cada variante como UT2 direto (UT1=AP se houver, UT2=variante, UT3=null)
4. Crie ENTRADAS SEPARADAS para cada variante (não agrupe ZRM2A + ZRM2B numa entrada)

---

## IDENTIFICAÇÃO DA LEGISLAÇÃO

```json
{
  "legislacao": {
    "tipo": "Lei Complementar",
    "numero": "270",
    "ano": 2024,
    "municipio": "Rio de Janeiro",
    "estado": "RJ",
    "ementa": "Lei de Uso e Ocupação do Solo"
  }
}
```

---

## ZONAS — ESTRUTURA DE CADA ENTRADA

Para cada zona ou subzona identificada, crie um objeto com esta estrutura:

```json
{
  "hierarquia": {
    "UT1": "AP4",
    "UT2": "ZRM",
    "UT3": "ZRM2A"
  },
  "dispositivo_definidor": "Art. 115 / Anexo I",
  "descricao_zona": "Zona Residencial Multifamiliar variante 2A",
  "usos_permitidos": {
    "residencial_unifamiliar": {"status": "SIM", "condicao": null, "fonte": "Anexo XVIII"},
    "residencial_multifamiliar": {"status": "SIM", "condicao": null, "fonte": "Anexo XVIII"},
    "residencial_his": {"status": "SIM", "condicao": null, "fonte": "Anexo XVIII"},
    "residencial_transitorio_hotel": {"status": "NI", "condicao": null, "fonte": null},
    "comercial": {"status": "SIM", "condicao": null, "fonte": "Anexo XVIII"},
    "servicos": {"status": "SIM", "condicao": null, "fonte": "Anexo XVIII"},
    "uso_misto": {"status": "NÃO", "condicao": null, "fonte": "Anexo XVIII"},
    "industrial": {"status": "NÃO", "condicao": null, "fonte": "Anexo XVIII"},
    "institucional": {"status": "SIM", "condicao": null, "fonte": "Anexo XVIII"}
  },
  "parametros_gerais": {
    "area_lote_minimo_m2": {"valor": null, "fonte": null, "dispositivo": null},
    "testada_minima_m": {"valor": null, "fonte": null, "dispositivo": null},
    "coeficiente_aproveitamento_maximo": {"valor": null, "fonte": null, "dispositivo": null},
    "coeficiente_aproveitamento_basico": {"valor": null, "fonte": null, "dispositivo": null},
    "taxa_ocupacao_percentual": {"valor": null, "fonte": null, "dispositivo": null},
    "taxa_permeabilidade_percentual": {"valor": null, "fonte": null, "dispositivo": null},
    "gabarito_pavimentos": {"valor": null, "fonte": null, "dispositivo": null},
    "altura_maxima_m": {"valor": null, "fonte": null, "dispositivo": null},
    "recuo_frontal_m": {"valor": null, "fonte": null, "dispositivo": null},
    "recuo_lateral_m": {"valor": null, "fonte": null, "dispositivo": null},
    "recuo_fundos_m": {"valor": null, "fonte": null, "dispositivo": null}
  }
}
```

**Status de usos:**
- `"SIM"` — permitido sem restrição
- `"NÃO"` — proibido
- `"CONDICIONADO"` — permitido com condição (preencha "condicao")
- `"NI"` — lei não menciona este uso nesta zona

---

## HERANÇA DE USOS (OBRIGATÓRIO)

O Anexo XVIII define usos para TIPOS BASE (ZRM, ZRU, ZCA...). As VARIANTES com parâmetros próprios herdam esses usos — mas cada variante entra como UT2 direto (não como UT3).

| Tipo Base no Anexo XVIII | Variantes que herdam os usos (cada uma vira UT2) |
|--------------------------|--------------------------------------------------|
| ZRM | ZRM1, ZRM1D, ZRM2, ZRM2A, ZRM2B, ZRM2D, ZRM2E, ZRM2F, ZRM2G, ZRM2H, ZRM2M, ZRM3, ZRM3A, ZRM3B, ZRM3D, ZRM3F, ZRM3G |
| ZRU | ZRU1, ZRU1A, ZRU1B, ZRU1C, ZRU2, ZRU2A |
| ZCS | ZCSA, ZCSB |
| ZCA | ZCA1, ZCA2, ZCA2A, ZCA2B, ZCA2C, ZCA2D, ZCA2E |
| ZUPI | ZUPIA |
| ZEI | ZEIS |

Cada variante vira uma entrada com UT1=AP (se houver) e UT2=variante:
  {"UT1": "AP4", "UT2": "ZRM2A", "UT3": null}  ← CORRETO
  {"UT1": "AP4", "UT2": "ZRM", "UT3": "ZRM2A"} ← ERRADO

Se o Anexo tiver entradas distintas para ZRU1 e ZRU2 (usos diferentes), use os usos específicos de cada uma.

---

## REVOGAÇÕES DE ZONAS EXTERNAS

Se a lei revogar explicitamente zonas ou subzonas de outra lei (ex: Art. 535 revogando subzonas do Decreto 3046/1981), registre em `revogacoes_zonas_externas`:

```json
{
  "revogacoes_zonas_externas": [
    {
      "lei_revogada": "Decreto 3046/1981",
      "zona_revogada": "A-4",
      "artigo_revogador": "Art. 535, II"
    }
  ]
}
```

---

## ESTRUTURA FINAL DO JSON

```json
{
  "legislacao": { ... },
  "zonas": [ ... ],
  "revogacoes_zonas_externas": [ ... ],
  "observacoes_gerais": "..."
}
```

---

## NORMALIZAÇÃO DE SIGLAS

Remova hífens internos das siglas de zona ao registrar no JSON:
- `ZA-A` → `ZAA`
- `ZA-B` → `ZAB`
- `ZRM-2A` → `ZRM2A`
- `ZRU-1B` → `ZRU1B`

Exceção: hífens em subzonas do Decreto 3046 (A-1, A-2, B-3...) são mantidos pois fazem parte do nome oficial.

---

## DEDUPLICAÇÃO

Se a mesma zona aparecer mais de uma vez no PDF (texto repetido ou seções duplicadas), crie apenas UMA entrada consolidando todas as informações:
- Use os parâmetros mais completos encontrados
- Não crie entradas duplicadas para a mesma zona

---

## CHECKLIST ANTES DE FINALIZAR

- [ ] Toda zona tem UT1 preenchido (AP ou null se lei não divide por área)?
- [ ] Variantes expandidas (ZRM1D, ZRM2A etc. têm entradas próprias)?
- [ ] Anexo XVIII lido e usos aplicados a todas as zonas relevantes?
- [ ] Revogações do Art. 535 capturadas?
- [ ] Parâmetros do Anexo XXI AP4 preenchidos?
- [ ] Nenhum campo inventado — apenas null se ausente?
