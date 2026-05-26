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

**REGRA CRÍTICA — Expansão de variantes:**
Se o Anexo de usos define usos para ZRM (tipo base) e a lei menciona ZRM1D, ZRM2A, ZRM2B, ZRM3F como variantes desse tipo, crie ENTRADAS SEPARADAS para cada variante, herdando os usos do tipo base. Não agrupe variantes numa única entrada.

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

Quando o Anexo de usos lista um TIPO BASE (ex: ZRM, ZRU, ZCS), aplique esses usos a TODAS as variantes desse tipo presentes na lei:

| Tipo Base | Variantes que herdam |
|-----------|---------------------|
| ZRM | ZRM1, ZRM1D, ZRM2, ZRM2A, ZRM2B, ZRM2D, ZRM2E, ZRM2F, ZRM2G, ZRM2H, ZRM2M, ZRM3, ZRM3A, ZRM3B, ZRM3D, ZRM3F, ZRM3G |
| ZRU | ZRU1, ZRU1A, ZRU1B, ZRU1C, ZRU2, ZRU2A |
| ZCS | ZCSA, ZCSB |
| ZCA | ZCA1, ZCA2, ZCA2A, ZCA2B, ZCA2C, ZCA2D, ZCA2E |
| ZUPI | ZUPIA |
| ZEI | ZEIS |

Se o Anexo tiver entradas distintas para ZRU1 e ZRU2 (com usos diferentes), use as entradas específicas — não herde do tipo base.

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

## CHECKLIST ANTES DE FINALIZAR

- [ ] Toda zona tem UT1 preenchido (AP ou null se lei não divide por área)?
- [ ] Variantes expandidas (ZRM1D, ZRM2A etc. têm entradas próprias)?
- [ ] Anexo XVIII lido e usos aplicados a todas as zonas relevantes?
- [ ] Revogações do Art. 535 capturadas?
- [ ] Parâmetros do Anexo XXI AP4 preenchidos?
- [ ] Nenhum campo inventado — apenas null se ausente?
