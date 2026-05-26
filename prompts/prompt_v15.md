# PROMPT v15 — Análise de Legislação Urbanística

## PARTE 0 — Antes de começar

### Sobre o conjunto de documentos

Você pode estar vendo apenas uma parte do conjunto total de documentos relacionados a esta legislação. Outros PDFs podem estar sendo processados em chamadas separadas.

#### IMPORTANTE — Identificação do corpo da lei

Em conjuntos com muitos PDFs, é comum que **um arquivo seja o CORPO PRINCIPAL da lei** (texto integral com Partes, Títulos, Capítulos, Artigos) e os outros sejam ANEXOS (tabelas, mapas, errata).

O corpo principal geralmente tem nome similar ao número da lei (Ex: `LC_148_2023.pdf`) e contém o texto completo com Art. 1º, Art. 2º, etc.

**Os anexos COMPLEMENTAM o corpo, NÃO o substituem.** Mesmo que os anexos sejam mais numerosos ou visuais, é o corpo da lei que define o `legislacao.tipo` e a maior parte das regras gerais.

Se você identificar o corpo da lei no conjunto: **leia-o integralmente** antes de classificar. Não classifique como "Errata" um conjunto que contém o corpo da lei + uma errata — a lei é a lei, a errata é uma modificação dela.

### Tarefa geral

1. Examine TODOS os PDFs — especialmente o corpo da lei se ele estiver presente
2. Identifique o que cada um é
3. Responda APENAS sobre o conteúdo que você vê
4. NÃO invente dados
5. Se uma zona é mencionada mas seus parâmetros não estão neste batch, registre a zona e deixe parâmetros como null

### Como ler

Leia a legislação inteira (corpo, todos os artigos, parágrafos, incisos, anexos, tabelas e mapas) que você consegue ver. Não pule anexos por serem longos — eles geralmente contêm as tabelas de parâmetros mais importantes.

Se houver errata ou retificação no batch, leia a errata **antes** da lei original — a errata prevalece.

---

## PARTE 0 — REGRAS DE FORMATO CANONICO (LEIA PRIMEIRO, NAO VIOLE)

Estas regras sao OBRIGATORIAS e tem prioridade sobre QUALQUER exemplo que voce veja adiante neste prompt. Se um exemplo neste prompt contradiz uma regra desta PARTE 0, siga a regra (nao o exemplo).

**REGRA 0.1 — Formato canonico das siglas em cada UT da hierarquia:**

Cada nivel da hierarquia (UT1, UT2, ..., UT7) deve conter siglas com APENAS letras maiusculas (A-Z) e digitos (0-9). SEM hifens, espacos, pontos, acentos ou outros caracteres.

| Sigla na lei (variantes) | Valor canonico da UT |
|--------------------------|-----------------------|
| `ZCA-1`, `ZCA 1`, `ZCA1` | `ZCA1` |
| `ZRM 2 A`, `ZRM2-A`, `ZRM2A` | `ZRM2A` |
| `ZRM 1 D`, `ZRM1-D` | `ZRM1D` |
| `ZUM-A`, `ZUM A` | `ZUMA` |
| `ZUPI-A` | `ZUPIA` |
| `ZEIS-1` | `ZEIS1` |
| `ZCS A`, `ZCS-A` | `ZCSA` |
| `ZPP` | `ZPP` |

**REGRA 0.2 — A sigla NUNCA contem hierarquia (AP, Macrozona, Setor, etc):**

PROIBIDO usar sufixos hierarquicos na sigla. A diferenciacao entre zonas com mesmo nome em hierarquias diferentes deve ser feita via `hierarquia` (UT1/UT2/UT3).

ERRADO:
- `ZRM2A-AP1`, `ZRM2AAP1`, `ZRM2A_AP1`, `ZRM2A.AP1`
- `ZRM-2-A-AP-2.1`
- `ZCA1-AP1`

CORRETO (mesma sigla, hierarquia diferente):
```
{"hierarquia": {"UT1": "AP1", "UT2": "ZRM2A"}, ...}
{"hierarquia": {"UT1": "AP2", "UT2": "ZRM2A"}, ...}
```

Sao DUAS entradas separadas no array `zonas`, com mesma sigla "ZRM2A" em UT2 mas UT1 distinta.

**REGRA 0.3 — Formato canonico da hierarquia UT1/UT2/UT3:**

Use forma compacta sem hifen nem espaco.

| Variante na lei | UT1 canonico |
|----------------|--------------|
| `AP-1`, `AP 1`, `AP1`, `Área de Planejamento 1`, `Area de Planejamento 1` | `AP1` |
| `AP-4`, `Area de Planejamento 4` | `AP4` |
| `AP-2.1`, `Área de Planejamento 2.1` | `AP2.1` |
| `Setor 3` | `Setor3` |

**REGRA 0.4 — O que NAO eh uma zona (NAO incluir no array `zonas`):**

NAO crie entrada no array `zonas` para:

1. **Macrozonas e niveis nao-hierarquicos estritos** (Macrozona de Proteção Integral, Estruturação Urbana, Redução da Vulnerabilidade, etc) — sao niveis amplos que tipicamente atravessam multiplas areas/zonas. NAO sao hierarquicamente estritos. NAO devem aparecer em nenhuma UT. Ignore-as completamente. Ver REGRA 0.5 (criterio de hierarquia estrita).

2. **Conceitos sem parametros** (zonas mencionadas no texto da lei mas sem `usos_permitidos` ou `parametros_gerais`/`parametros_por_uso` preenchidos) — se voce nao tem PARAMETROS REAIS pra preencher (mesmo que NI), nao crie a entrada.

3. **Artefatos de figuras/legendas** (Zona A da Figura 10, Zona B do mapa X) — nao sao zonas urbanas reais, sao apenas referencias visuais.

4. **Pais-mae sem subdivisao real** (ZRM, ZCA sem numero, quando todas as instancias reais sao subzonas tipo ZRM1A, ZRM2A) — nao crie entrada generica.

5. **Subzonas de leis EXTERNAS referenciadas** (Ex: Subzona A-4 do Dec 3046/1981 mencionada por LC 270/2024 na ZPP) — NAO crie entrada no array `zonas` para essas subzonas. Elas serao trazidas via merge da lei externa. Voce DEVE capturar a referencia em `referencias_externas` (campo `subzonas_aplicaveis`), mas NAO crie entradas filhas no array `zonas` extendendo a hierarquia (Ex: nao criar `{UT1:'AP4', UT2:'ZPP', UT3:'A2'}`). A zona referenciante (ZPP) fica registrada apenas com sua propria hierarquia (Ex: `{UT1:'AP4', UT2:'ZPP'}`).

**REGRA 0.5 — Criterio de HIERARQUIA ESTRITA pras UTs:**

Um nivel territorial so deve entrar como UT na `hierarquia` se atende ao criterio:

> **Cada elemento desse nivel deve estar INTEIRAMENTE CONTIDO em UM UNICO elemento do nivel imediatamente superior.**

Se um nivel atravessa outros niveis (sobrepoe sem ser parte integral), NAO o inclua em UT — ignore-o.

**Exemplo (LC 270 do Rio de Janeiro):**
- Macrozona (ex: Estruturação Urbana) atravessa varias APs → NAO eh UT, IGNORE
- AP (Area de Planejamento): cada zona esta inteiramente em UMA AP → SIM, eh UT
- Zona (ZRM2A, ZPP) → SIM, eh UT
- Subzona/Setor → SIM, eh UT

**Exemplo (Dec 3046):**
- ZE5 (zona-mae) → SIM, eh UT
- A1, A2, A3 (subzonas dentro de ZE5) → SIM, sao UTs

**Como decidir:** ao identificar um nivel na lei, pergunte: "toda zona desse nivel pertence a UM UNICO elemento do nivel superior?". Se SIM, eh UT. Se NAO (atravessa/sobrepoe), nao eh UT.

Niveis informacionais como zoneamento ambiental sobreposto (APA, APP, ZPA) ja sao tratados no campo `zoneamento_ambiental_sobreposto`, NAO em UT.

---

**REGRA 0.6 — Filtragem por ter parametros:**

Uma zona so entra no array `zonas` se atende A PELO MENOS UMA destas:
- Tem pelo menos um uso em `usos_permitidos` com `status: "SIM"` ou `"CONDICIONADO"`, OU
- Tem ao menos um parametro em `parametros_gerais` ou `parametros_por_uso` com valor (mesmo que `NI` ou `NI_LEI_EXTERNA`)

Se nada disso se aplica, NAO inclua a zona no JSON.


---

## PARTE 1 — Sobre esta legislação

**1.1** Qual é a identificação formal desta legislação?

Informe: tipo de ato (Lei Complementar, Lei Ordinária, Decreto, Errata, Portaria), número, ano, município, estado.

**1.2** Qual é a data de publicação? Há vacatio legis? Quando começou a vigorar?

**1.3** Esta lei altera, revoga ou substitui dispositivos de outras leis?

Se nao, pule para 1.4.

Se sim, para CADA modificacao, capture estruturadamente em `overrides_de_leis_anteriores` (no topo da seccao `legislacao`):
- `lei_alterada`: {tipo, numero, ano}
- `tipo_alteracao`: "nova_redacao_integral" (artigo INTEIRO reescrito) / "alteracao_cirurgica" (so inciso/paragrafo mudou) / "revogacao_total" / "revogacao_parcial" / "errata"
- `dispositivo_alterado`: Ex: "art. 32, inciso III"
- `escopo_geografico`: Ex: "todo o municipio", "AP-1"
- `escopo_uso`: Ex: "todos os usos", "apenas Comercial"
- `zonas_afetadas`: lista de hierarquias parciais (Ex: [{"UT1": "AP4", "UT2": "ZRM2S"}]) ou null
- `redacao_nova`: texto da nova redacao SE houver
- `parametros_novos`: lista de objetos {hierarquia, parametros} se a alteracao define novos valores (Ex: [{"hierarquia": {"UT1": "AP4", "UT2": "ZRM2S"}, "parametros": {"altura_maxima_m": "20"}}])

Exemplo:
```json
"overrides_de_leis_anteriores": [
  {
    "lei_alterada": {"tipo": "Lei Complementar", "numero": "270", "ano": "2024"},
    "tipo_alteracao": "nova_redacao_integral",
    "dispositivo_alterado": "art. 32",
    "zonas_afetadas": [{"UT1": "AP4", "UT2": "ZRM2S"}],
    "parametros_novos": [{"hierarquia": {"UT1": "AP4", "UT2": "ZRM2S"}, "parametros": {"taxa_ocupacao_maxima_pct": "60", "altura_maxima_m": "20"}}]
  }
]
```

**1.4** Esta lei referencia outras leis que NAO estao neste conjunto de documentos?

Se sim, em `referencias_externas`, para CADA referencia capture:
- `tipo`, `numero`, `ano`, `contexto` (basico)
- `zona_referenciante`: hierarquia da zona desta lei que faz a referencia (Ex: {"UT1": "AP4", "UT2": "ZPP"})
- `subzonas_aplicaveis`: lista de SIGLAS (strings) das zonas/subzonas externas referenciadas
  - Cada item eh uma string com a sigla canonica da subzona como aparece na lei externa (Ex: ["A4", "A5", "A6", "A7"])
  - Formato canonico (REGRA 0.1): so letras+digitos, sem hifen
  - Voce so conhece os NOMES das subzonas externas (nao a hierarquia interna da lei externa). O merge fara o match das siglas com as zonas da lei externa quando combinar
  - Referencia generica ("aplicam-se TODAS as normas do Dec 3046") -> use [] (lista vazia = sem filtro, pega todas as zonas da lei externa)
- `dispositivo`: artigo/inciso desta lei onde a referencia aparece

Exemplo:
```json
"referencias_externas": [
  {
    "tipo": "Decreto", "numero": "3046", "ano": "1981",
    "contexto": "Parametros urbanisticos ZPP (AP-4)",
    "zona_referenciante": {"UT1": "AP4", "UT2": "ZPP"},
    "subzonas_aplicaveis": ["A4", "A5", "A6", "A7"],
    "dispositivo": "art. 32, inciso II"
  }
]
```

Marque essas leis externas como "pendencia externa" - nao invente o conteudo delas.

---

## PARTE 2 — Mapeamento territorial

**REGRA 2.D — Dispositivo definidor da zona (OBRIGATORIO):**

Cada zona deve incluir o campo `dispositivo_definidor`, indicando o artigo PRINCIPAL onde a zona eh definida nesta lei:

```json
{
  "hierarquia": {"UT1": "AP4", "UT2": "ZRM2S", ...},
  "dispositivo_definidor": "art. 32",
  ...
}
```

Use formato curto: "art. N" ou "art. N, paragrafo M" ou "Anexo X". Pra que serve: facilita rastreabilidade e overrides por leis posteriores.

**2.1** Esta legislação define zonas, subzonas ou áreas específicas onde determinados usos são permitidos?

Se não, pule para a PARTE 8.

Se sim, continue.

**2.2** O município é dividido hierarquicamente antes de chegar nas zonas?

Exemplos de hierarquia possível (varia por município):
- Área de Planejamento → Região Administrativa → Bairro → Zona (Macrozonas NAO entram em UTs — ver REGRA 0.5)
- Setor → Unidade de Planejamento → Zona
- Só Zona (sem hierarquia)

Identifique a cadeia que ESTA lei usa. Não invente níveis que a lei não cita.

**2.3** Para cada zona identificada, liste:
- Sigla canônica (Ex: "ZRM2-A")
- Variantes de grafia observadas (Ex: "ZRM2A", "ZRM-2A" — todas a mesma zona)
- Subzonas existentes, se houver
- Cadeia hierárquica completa onde se encaixa
- Legislação fonte (Ex: "LC 148/2023, Art. 70, II")

**Atenção:** se a mesma sigla (Ex: "ZUM-1") aparece em hierarquias diferentes (Ex: AP-1 e AP-3), trate como duas zonas distintas.

**2.4** Há áreas com zoneamento ambiental sobreposto (APP, APA, ZPA, ZEPC, ZEIS)?

Se sim, identifique quais zonas têm essa sobreposição e qual lei a define.

---

## PARTE 3 — Usos permitidos por zona

**3.1** Para cada zona/subzona identificada na PARTE 2, esta lei define quais usos são permitidos?

Se não viu essa informação neste batch, deixe como null na saída — outro batch pode ter.

Se sim, para cada zona, informe o status de cada um dos 9 usos:

- Residencial Unifamiliar
- Residencial Multifamiliar
- Residencial HIS (Habitação Social)
- Residencial Transitório / Hotel (hotéis, pousadas, motéis, flats, apart-hotéis, hospedagem)
- Comercial
- Serviços
- Uso Misto
- Industrial
- Institucional

**IMPORTANTE — Residencial Transitório/Hotel:**
- A lei pode chamar de várias formas: "hospedagem", "hotel", "uso turístico", "uso transitório", "alojamento", "pousada", "flat", "apart-hotel", "motel". Todas essas variações se agrupam neste uso.
- Se a lei NÃO menciona NENHUMA dessas categorias EM NENHUMA zona da municipalidade, deixe `residencial_transitorio_hotel.status` = `"NI"` em todas as zonas (lei silenciou — Python preencherá NI nos parâmetros).
- Se a lei menciona em alguma zona mas não nesta zona específica, use `"NÃO"` (proibido por silêncio na zona).

Status possíveis:
- **SIM** — uso permitido sem restrição
- **NÃO** — uso proibido
- **CONDICIONADO** — permitido sob certas condições (descreva a condição)

Cite a fonte legal para cada uso definido (artigo/anexo).

---

**REGRA 3.H — Herança de usos por nomenclatura (UNIVERSAL):**

Quando um Anexo ou tabela de usos define usos para um TIPO DE ZONA (ex: ZRM, ZRU, ZCS, ZCA),
todas as VARIANTES desse tipo HERDAM os mesmos usos, SALVO disposicao explicita em contrario.

Prefixo alfabético contínuo define o tipo base:
  ZRM  → ZRM1, ZRM1D, ZRM2, ZRM2A, ZRM2B, ZRM2D, ZRM3, ZRM3F herdam usos de ZRM
  ZRU  → ZRU1, ZRU1A, ZRU1B, ZRU2, ZRU2A herdam usos de ZRU
  ZCS  → ZCSA, ZCSB herdam usos de ZCS
  ZCA  → ZCA1, ZCA2, ZCA2A, ZCA2B, ZCA2C herdam usos de ZCA
  ZUPI → ZUPIA herda usos de ZUPI
  ZEI  → ZEIS herda usos de ZEI
  ZPP  → sem variantes; segue Nota 8 (Dec 3046 por subzona)

Ordem de prioridade:
1. Usos EXPLICITOS para a variante na lei/Anexo
2. Usos do TIPO BASE herdados do Anexo/tabela de usos
3. null (nem variante nem tipo base encontrados neste batch)

IMPORTANTE: nao deixe usos_permitidos vazio para zona com tipo base no Anexo.
Se o Anexo tem ZRM, preencha ZRM1D, ZRM2A, ZRM2B etc com os usos de ZRM.

---

## PARTE 4 — Parâmetros gerais por zona

**REGRA 4.D — Dispositivo legal por parametro (OBRIGATORIO):**

Cada parametro em `parametros_gerais` e `parametros_por_uso` deve incluir o campo `dispositivo`, ALEM do campo `fonte`, indicando o artigo/inciso/paragrafo ESPECIFICO:

```json
"area_lote_minimo_m2": {
  "valor": "360",
  "fonte": "LC 148/2023, Art. 70, II",
  "dispositivo": "art. 70, inciso II"
}
```

Formato curto: "art. N", "art. N, inciso M", "art. N, paragrafo M, inciso K", "Anexo X". Pra que serve: permite que leis posteriores facam alteracoes cirurgicas em dispositivos especificos.

**4.1** Para cada zona/subzona, esta lei define parâmetros gerais (que valem para a zona toda, sem distinguir por uso)?
Se não viu essa informação neste batch, deixe como null — outro batch pode ter.
Se sim, informe os valores definidos.

**ATENCAO: USE EXATAMENTE estas chaves JSON em `parametros_gerais`. NAO INVENTE NOMES.**
Se um valor existir na lei mas o nome do parametro for diferente, normalize para a chave abaixo. Nao use abreviacoes da lei (TO, CAM, TP, etc) — sempre use o nome canonico.

PARAMETROS DO LOTE (registrar em `parametros_gerais`)

| Parametro                          | Chave JSON               |
|------------------------------------|--------------------------|
| Area Lote Minimo (m2)              | `area_lote_minimo_m2`    |
| Area Lote Maximo (m2)              | `area_lote_maximo_m2`    |
| Testada Minima (m)                 | `testada_minima_m`       |
| Area a ser doada em loteamento (%) | `area_doacao_pct`        |

PARAMETROS GERAIS DA ZONA (registrar em `parametros_gerais`)

| Parametro                                | Chave JSON                                |
|------------------------------------------|-------------------------------------------|
| Taxa de Permeabilidade Minima (%)        | `permeabilidade_minima_pct`               |
| Quota Ideal (m2/economia)                | `quota_ideal_m2_economia`                 |
| Afastamento entre blocos                 | `afastamento_entre_blocos`                |
| Gabarito maximo Nao Afastado — pavimentos| `gabarito_max_nao_afastado_pavimentos`    |
| Gabarito maximo Nao Afastado — altura (m)| `gabarito_max_nao_afastado_altura_m`      |
| Isencao de Outorga Onerosa               | `isencao_outorga_onerosa`                 |

Para cada parametro: valor + legislacao fonte (Ex: "LC 148/2023, Art. 70, II").
Se a lei nao define um parametro, use "NI" (Nao Informado).

**Nao crie campos fora desta lista em `parametros_gerais`. Coeficiente de Aproveitamento (CAM/CAB) e Taxa de Ocupacao (TO) sao REGISTRADOS por uso (PARTE 5), nunca em `parametros_gerais`.**

---
## PARTE 5 — Parâmetros por uso

**5.1** Para cada zona, os parametros urbanisticos variam dependendo do uso?
Se nao viu essa informacao neste batch, deixe como null — outro batch pode ter.
Se sim, para cada combinacao (zona x uso permitido), informe os 11 parametros abaixo.

**ATENCAO: USE EXATAMENTE estas chaves JSON em `parametros_por_uso[<uso>]`. NAO INVENTE NOMES.**
Se a lei usar abreviacoes (CAM, CAB, TO, TP, etc), normalize para a chave canonica abaixo:

| Parametro                                          | Chave JSON                              |
|----------------------------------------------------|-----------------------------------------|
| Coeficiente de Aproveitamento BASICO (sem outorga) | `coeficiente_aproveitamento_basico`     |
| Coeficiente de Aproveitamento MAXIMO (com outorga) | `coeficiente_aproveitamento_maximo`     |
| Taxa de Ocupacao BASICA                            | `taxa_ocupacao_basica_pct`              |
| Taxa de Ocupacao MAXIMA                            | `taxa_ocupacao_maxima_pct`              |
| Gabarito BASICO em pavimentos                      | `gabarito_basico_pavimentos`            |
| Gabarito MAXIMO em pavimentos                      | `gabarito_max_nao_afastado_pavimentos`  |
| Gabarito BASICO em altura (m)                      | `gabarito_basico_altura_m`              |
| Gabarito MAXIMO em altura (m)                      | `altura_maxima_absoluta_m`              |
| Afastamento frontal                                | `recuo_frontal_m`                       |
| Afastamento lateral                                | `recuo_lateral_m`                       |
| Afastamento de fundos                              | `recuo_fundos_m`                        |

**Aliases comuns nas leis brasileiras (NORMALIZAR para a chave canonica):**
- "CAM", "Coef. Aprov. Maximo", "CA max" -> `coeficiente_aproveitamento_maximo`
- "CAB", "Coef. Aprov. Basico", "CA basico" -> `coeficiente_aproveitamento_basico`
- "TO", "TO max", "Taxa Ocup." -> `taxa_ocupacao_maxima_pct`
- "TP" -> `permeabilidade_minima_pct` (vai em `parametros_gerais`)
- "Afast. frontal", "Recuo Frontal", "Recuo F" -> `recuo_frontal_m`

Para cada um: valor + legislacao fonte.

**5.2** Se a lei NAO distingue parametros por uso (define so valores gerais da zona), confirme: "Parametros sao gerais da zona, valem para todos os usos permitidos."

**5.3** Para zonas onde o uso eh NAO (proibido), confirme: "Uso X proibido na zona Y."

**5.4 — Parametros remetidos a OUTRA LEGISLACAO (importante)**

Algumas zonas tem parametros que NAO sao definidos na lei principal — a lei remete a OUTRA norma (Decreto antigo, LC anterior, etc).

Exemplos comuns na tabela: "ver Dec. no 3046/1981", "conforme LC 89/2005", "definido em legislacao especifica".

Quando isso ocorrer, faca DOIS PASSOS:

**Passo 1 — Marcar o valor:** use NI_LEI_EXTERNA (em vez de NI) no campo valor do parametro. Isso sinaliza que o valor existe, so esta em outra norma.

Exemplo:
"coeficiente_aproveitamento_maximo": {
  "valor": "NI_LEI_EXTERNA",
  "fonte": "LC 270/2024, Anexo XXI — remete a Dec. 3046/1981"
}

**Passo 2 — Adicionar referencias_externas na zona (formato ESTRUTURADO obrigatorio):**

```
"referencias_externas": [
  {
    "lei_referenciada": {
      "esfera": "municipal",
      "municipio": "Rio de Janeiro",
      "estado": "RJ",
      "tipo_nome": "Decreto",
      "numero": "3046",
      "ano": 1981,
      "texto_original": "Dec. nº 3046, de 27 de abril de 1981"
    },
    "dispositivo": "Subzonas A-4, A-5, A-6, A-7, ...",
    "parametros_afetados": [
      "coeficiente_aproveitamento_maximo",
      "taxa_ocupacao_maxima_pct"
    ],
    "contexto": "ZPP - Plano Piloto Jacarepagua, parametros por subzona do Dec 3046"
  }
]
```

**REGRAS de preenchimento de lei_referenciada (esquema do Buscador):**

- `esfera` (obrigatorio): use EXATAMENTE um destes valores: `"federal"`, `"estadual"`, `"municipal"`
- `municipio` (so se esfera=municipal): nome completo do municipio (ex: "Rio de Janeiro", "Sao Paulo")
- `estado` (obrigatorio se esfera=municipal ou estadual): sigla UF (ex: "RJ", "SP")
- `tipo_nome` (obrigatorio): use EXATAMENTE um destes 14 tipos canonicos:
  - "Lei Ordinária"
  - "Lei Complementar"
  - "Decreto"
  - "Decreto-Lei"
  - "Portaria"
  - "Resolução"
  - "Instrução Normativa"
  - "Instrução Técnica"
  - "Medida Provisória"
  - "Emenda Constitucional"
  - "Plano Diretor"
  - "Código de Obras"
  - "Código de Posturas"
  - "Regulamento"
- `numero` (obrigatorio): string, sem o ano (ex: "3046", "270", "148")
- `ano` (obrigatorio): integer (ex: 1981, 2024)
- `texto_original` (opcional, recomendado): citacao literal da lei principal, util pra auditoria

**Como o sistema usa isso:**

1. Apos a extracao, o sistema busca cada lei referenciada no banco de dossies usando esfera+tipo+numero+ano
2. Se a lei JA EXISTE no dossie (foi encontrada pelo Buscador automaticamente), o sistema processa ela em pipeline secundario
3. Se NAO existe, o operador pode anexar o PDF manualmente
4. Apos processar a lei externa, faz MERGE: cada zona com referencia vira N linhas adicionais na planilha:
   - UT1, UT2, UT3 mantem hierarquia da lei principal (ex: AP-4)
   - Zona Urbana mantem (ex: ZPP)
   - UT4 recebe a subdivisao da lei externa (ex: "Subzona A-4")
   - Parametros sao preenchidos com valores reais
   - Fonte cita "Dec. 3046/1981, Subzona A-4"

**Hierarquia UT4-UT6 (regra estrita):**
- UT1, UT2, UT3, ... UT7 = hierarquia da LEI PRINCIPAL (AP, Setor, Zona, etc — niveis estritamente hierarquicos, ver REGRA 0.5)
- UT4, UT5, UT6 = APENAS para hierarquia da LEI EXTERNA (quando aplicavel)
- Sem lei externa: UT4, UT5, UT6 ficam null

### 5.5 — Revogacoes de Zonas/Subzonas de Leis EXTERNAS  CRITICO

REGRA DE ROTEAMENTO (LEIA ANTES):
- Revogacao de ZONA ou SUBZONA inteira de lei externa -> SEMPRE em
  `revogacoes_zonas_externas` (campo desta secao). NUNCA em
  `overrides_de_leis_anteriores`.
- `overrides_de_leis_anteriores` eh para alteracao de REDACAO de artigos,
  PARAMETROS, ou DISPOSITIVOS de outras leis - NUNCA para revogacao de
  zonas/subzonas.
- Mesmo que o dispositivo seja "Art. 535 - Ficam revogados parcialmente:
  II - Dec 3046, Cap III, Subzonas A-4, A-5...", esta SECAO 5.5 eh o
  destino correto. Gere UM ITEM POR SUBZONA na lista, mesmo que sejam
  dezenas.

A lei principal sob analise pode REVOGAR zonas ou subzonas inteiras
definidas em leis externas (anteriores). Esta secao monta a LISTA NEGRA
da lei principal: zonas/subzonas que a lei externa AINDA define no
proprio texto dela, mas que FORAM REVOGADAS pela lei principal e
portanto NAO podem aparecer na planilha final.

Por que critico: o sistema mescla automaticamente todas as zonas das
leis externas. Se voce NAO capturar as revogacoes aqui, a planilha
sairah COM ZONAS QUE NAO EXISTEM MAIS.

Isso eh COMUM em:
- Planos diretores novos que substituem subzonas obsoletas de decretos antigos
- Leis municipais que extinguem zonas especiais criadas por leis anteriores
- Atos que reclassificam parte de uma area, deixando a subzona original sem efeito

**Quando capturar:** se a lei explicitamente diz "fica revogada a zona X
do Decreto Y", "a Subzona A-4 do Dec 3046/1981 fica extinta", "Suprime-se
a area destinada a Z definida pela Lei W", ou similar.

**Como capturar:** preencher campo `estado.legislacao.revogacoes_zonas_externas`
(LISTA) com o seguinte schema:

```
"revogacoes_zonas_externas": [
  {
    "lei_origem": {
      "esfera": "municipal",
      "municipio": "Rio de Janeiro",
      "estado": "RJ",
      "tipo_nome": "Decreto",
      "numero": "3046",
      "ano": 1981
    },
    "hierarquia_zona": {"UT1": "ZE5", "UT2": "A4"},
    "dispositivo_revogador": "Art. 250, inciso II",
    "motivo": "Subzona A-4 do Dec 3046 fica revogada por reclassificacao da area pela LC 270/2024"
  }
]
```

**REGRAS:**
- `lei_origem`: mesmo schema de `lei_referenciada` (esfera, municipio, estado, tipo_nome, numero, ano)
- `hierarquia_zona` (obrigatorio): hierarquia parcial da zona externa revogada, no contexto da LEI EXTERNA (ex: {"UT1": "ZE5", "UT2": "A4"}). Formato segue REGRA 0.1 (so letras+digitos por UT).
  - Se a lei revoga UM CONJUNTO de zonas/subzonas, criar UM ITEM por hierarquia
- `dispositivo_revogador` (opcional, recomendado): artigo/anexo da lei principal que efetua a revogacao
- `motivo` (opcional, recomendado): explicacao breve

**Importante:** capture APENAS revogacoes EXPLICITAS de zonas. Nao capture
modificacoes parciais (alteracoes de parametros) — essas ficam em outros
campos. Apenas revogacao INTEIRA da zona/subzona.

**EXEMPLO PRATICO — Lista revogada num unico dispositivo:**

Se voce ler na lei algo como:
"Art. 535. Ficam revogados parcialmente: II - o Decreto no 3046, de
27 de abril de 1981 - Capitulo III, Subzonas A-4, A-5, A-6, A-7, A-8,
A-9, A-10, A-11, A-12, A-23, A-24, A-25, A-26, A-27, A-28, A-29, A-30,
A-31, A-32, A-33, A-34, A-38, A-41, A-42, A-43, A-44 e A-45"

Voce DEVE gerar UM ITEM da lista `revogacoes_zonas_externas` PARA CADA
SUBZONA citada. Mesmo que sejam dezenas. Exemplo (resumido):
```
"revogacoes_zonas_externas": [
  {"lei_origem": {"tipo_nome":"Decreto","numero":"3046","ano":1981},
   "hierarquia_zona": {"UT1": "ZE5", "UT2": "A4"},
   "dispositivo_revogador": "Art. 535, inciso II",
   "motivo": "Subzona A-4 do Cap. III do Dec 3046 revogada parcialmente"},
  {"lei_origem": {"tipo_nome":"Decreto","numero":"3046","ano":1981},
   "hierarquia_zona": {"UT1": "ZE5", "UT2": "A5"},
   "dispositivo_revogador": "Art. 535, inciso II"},
  ... e assim por diante para CADA subzona da lista ...
]
```

**Cuidado com formato canonico (REGRA 0.1):**
- "A-4" no texto da lei -> "A4" na hierarquia_zona (sem hifen)
- "Subzona A 5" no texto -> "A5"
- Sempre letras+digitos por UT, sem espacos, sem hifens

**Como o sistema usa isso:** durante a geracao da planilha, quando uma lei
externa eh mesclada (etapa Fase B do preenchedor), o sistema verifica se
ha revogacoes_zonas_externas em QUALQUER um dos JSONs carregados. Se sim,
as zonas revogadas SAO OMITIDAS da planilha final.

---

## PARTE 6 — Variações condicionais

**6.1a** Os parâmetros variam dependendo da HIERARQUIA VIÁRIA da via (arterial, coletora, local, expressa, etc)?

Se sim, para cada zona afetada, informe:
- Nome da hierarquia EXATAMENTE como a lei a chama (ex: "arterial", "coletora", "local", "primária", "secundária")
- Parâmetros que variam e seus valores para essa hierarquia
- Fonte (artigo/anexo)

Formato sugerido:

ZD (Zona Dinâmica):
  Taxa de Ocupação:
    - Geral: 65%
    - Via arterial: 80%
    - Via coletora: 70%

**Atenção:** capture o nome EXATO usado pela lei. Se a lei diz "vias estruturadoras", use "estruturadora", não traduza para "arterial".

**6.1b** Os parâmetros variam dependendo de uma VIA ESPECÍFICA NOMEADA (ex: "Av. Beira-Mar", "Rua General Rabelo")?

Se sim, para cada zona afetada, informe:
- Nome exato da via
- Lado (PAR / IMPAR / AMBOS)
- Trecho (range numérico ou cruzamentos, ou null)
- Parâmetros que variam e seus valores
- Fonte

Formato sugerido:

ZRM1-M (AP2.1):
  Gabarito (pav):
    - Geral: 8
    - Av. Afranio de Melo Franco | AMBOS | * : 11
    - Rua General Rabelo | IMPAR | * : 3

**6.2** Os parâmetros variam se o lote for de esquina?

Se sim, para cada zona afetada, informe os valores para meio-de-quadra vs. esquina.

Exemplo: "ZR5: Área Lote Mínimo = 360m² em meio de quadra, 480m² em esquina."

**6.3** Os parâmetros variam conforme a declividade do terreno?

Se sim, para cada zona afetada, informe:
- Threshold de declividade (Ex: 20%)
- Valores para terrenos com declividade ≤ threshold
- Valores para terrenos com declividade > threshold
- Legislação que define a regra

**6.4** Os parâmetros variam conforme a altitude/cota do terreno?

Se sim, identifique se é por FAIXAS ou por COTA_MAX:

FAIXAS: o valor muda conforme a faixa de altitude.
  ≤ cota 20m : 15
  cota 20m – cota 40m : 10
  cota 40m – cota 50m : 6
  > cota 50m : 3

COTA_MAX: valor único + teto absoluto.
  Gabarito: 7 pavimentos / Teto: ≤ cota 25m acima do nível do mar

---

## PARTE 6.5 — Hierarquia viária do município

Independente de variações por zona (PARTE 6.1a/6.1b), responda sobre o município COMO UM TODO:

**6.5.1** A lei DEFINE uma hierarquia viária para o município? (sim/não/NI)

**6.5.2** Se sim, quais são as hierarquias existentes?
Liste cada hierarquia com o nome EXATO usado pela lei.
Não normalize nem traduza: se a lei chama de "vias estruturadoras", escreva "estruturadora". Se chama "primária/secundária", use isso.
Exemplos de vocabulários comuns (apenas referência — use o que a lei usar):
  - arterial / coletora / local
  - principal / secundária / terciária
  - expressa / arterial / coletora / local
  - estruturadora / coletora / vicinal
Para cada hierarquia, informe a fonte (artigo/anexo onde está definida).

**6.5.3** A lei mapeia VIAS específicas do município para hierarquias? (sim/não/NI)
Se sim, liste todas as vias mapeadas:
- Nome exato da via
- Hierarquia à qual pertence
- Trecho (se aplicável)
- Fonte

**6.5.4** Existe alguma característica geométrica/funcional definida para cada hierarquia? (largura mínima, número de faixas, velocidade, etc)
Se sim, informe por hierarquia.

---

## PARTE 7 — Acréscimos extraordinários

**7.1** A lei prevê acréscimos de área construtiva extraordinária (TDC, OUC, HIS, etc)?

Se sim, para cada acréscimo previsto (até 3), informe:
- Acréscimo permitido (%)
- Base de cálculo
- Fator motivador (TDC / OUC / HIS / outro)
- Condição de aplicabilidade
- Legislação (Art. / § / Inciso)

---

## PARTE 8 — Regras críticas

### REGRA 1 — Replicação de parâmetros gerais por uso

A legislação raramente diz "para uso Comercial, TO = X%". Quase sempre ela diz "Na zona ZR1, TO máxima = 50%" sem nomear o uso.

**Quando a lei dá um parâmetro GERAL da zona** (sem distinguir uso):
- Esse parâmetro vale para TODOS os usos permitidos
- Registre como parametros_gerais da zona
- Não duplique em parametros_por_uso — Python aplicará a replicação

**Quando a lei dá um parâmetro ESPECÍFICO por uso:**
- Registre em parametros_por_uso apenas o valor específico daquele uso
- O valor geral fica em parametros_gerais

**Para usos NÃO permitidos:**
- Não registre parâmetros — Python preencherá "Not Allowed" depois

### REGRA 2 — Quando criar registro separado vs. valor descritivo

**Crie registro de zona separado quando:**
- Subzonas diferentes (ZRM2-A e ZRM2-B → 2 registros)
- Mesma zona em hierarquias diferentes (ZRM2 em AP-2.1 e em AP-3.5 → 2 registros)
- Faixas geográficas estruturais (ZCS comum vs ZCS na "Faixa 80m da BR-101" → 2 registros)

**NÃO crie registro separado quando:**
- Variação por via → registre dentro de variacoes.por_via
- Variação por esquina → registre dentro de variacoes.por_esquina
- Variação por declividade → registre dentro de variacoes.por_declividade

### REGRA 3 — Citação de legislação

Para cada parâmetro preenchido, cite a fonte legal no formato:
- "LC NNN/AAAA, Art. X"
- "LC NNN/AAAA, Art. X, §Y, II"
- "LC NNN/AAAA, Anexo II, Tabela XV"
- "LC NNN/AAAA alterada por LC MMM/AAAA, Art. X"

### REGRA 4 — Valores ausentes

- **Lei não define o parâmetro** → "NI" (Não Informado)
- **Não viu nos PDFs deste batch** → null (outro batch pode ter)
- **Uso não permitido** → não registre (Python preenche depois)

### REGRA 4.1 — SEMPRE REGISTRAR ZONAS MENCIONADAS

Se você identificar uma zona mencionada nos PDFs (mesmo que apenas pelo nome, em um artigo, anexo ou mapa) — **SEMPRE registre essa zona** na lista zonas da saída, mesmo que você não tenha encontrado parâmetros, usos ou variações para ela.

Use null em todos os campos para os quais você não tem dados:
- usos_permitidos: null se não viu usos no batch
- parametros_gerais: null se não viu parâmetros no batch
- parametros_por_uso: null se não viu
- variacoes: null se não viu

**NÃO OMITA zonas** apenas porque seus parâmetros estão em outro PDF (ex: Anexo 2.4 ausente). Outro batch pode complementar essa informação. A zona existir é informação valiosa por si só.

Exemplo: se a lei menciona "ZR5 esta na AP-2 conforme Art. 277" mas o Anexo 2.4 (que tem os parametros) nao esta neste batch, ainda assim registre ZR5 na lista com fonte_definicao preenchida e todos os campos de parametros/usos como null.

### REGRA 4.5 — Resolução de referências cruzadas

Quando a lei diz coisas como **"idêntico à zona base"**, **"ver zona ZR1"**, **"mesmos valores da ZC2"**, **"conforme a zona principal"**, **"varia conforme a hierarquia da via"** ou similar:

- **NUNCA** copie o texto literal como valor do parâmetro
- **RESOLVA a referência**: vá até a zona/contexto referenciado, pegue o valor numérico real, e use esse valor
- Se a referência aponta pra outra zona que NÃO existe no texto deste batch, deixe `"valor": "NI"` (não invente)
- Se a referência é circular ou ambígua, deixe `"valor": "NI"` e descreva no campo `fonte` qual era a referência (ex: `"fonte": "LC X, Art. Y — referencia 'idêntico à zona base' mas zona base não definida neste batch"`)

Exemplos do que NÃO fazer:
- ❌ `"taxa_ocupacao_maxima_pct": {"valor": "Idêntico à zona base", "fonte": "..."}`
- ❌ `"recuo_frontal_m": {"valor": "Varia conforme via", "fonte": "..."}`
- ❌ `"area_lote_minimo_m2": {"valor": "Ver ZR1", "fonte": "..."}`

Exemplos do que fazer:
- ✅ `"taxa_ocupacao_maxima_pct": {"valor": "60", "fonte": "LC X, Art. Y — herda da zona base ZR1"}`
- ✅ `"recuo_frontal_m": {"valor": "NI", "fonte": null}` (se referência não resolvível)

Variações por via/hierarquia/esquina/topografia/lote-corner devem ir SEMPRE em `variacoes` (PARTE 6), NUNCA como texto livre no valor de um parâmetro.

### REGRA 5 — Normalização de grafias

Variações apenas de pontuação/espaço/hífen são a mesma zona:
- ZRM2-A = ZRM2A = ZRM-2A = ZRM 2-A → escolha UMA grafia canônica
- Preferir a grafia da lei mais recente
- Registrar as variantes em variantes_observadas

**Não consolide** sigla parcial vs completa (ZRM2 ≠ ZRM2-A).

---

## PARTE 9 — Formato de saída

Responda em JSON com a estrutura abaixo. Use null para campos que você não conseguiu identificar neste batch.

EXEMPLO DE ESTRUTURA:

{
  "batch_observado": {
    "documentos_vistos": ["LC_148_2023.pdf (corpo da lei)", "anexo_pag_130.pdf (mapa)"],
    "documentos_nao_vistos_referenciados": ["Anexo 2.4 (mencionado mas não presente)"]
  },
  "legislacao": {
    "tipo": "Lei Complementar",
    "numero": "148",
    "ano": "2023",
    "municipio": "Xangri-Lá",
    "estado": "RS",
    "data_publicacao": "2023-12-01",
    "vigencia_inicio": "2024-03-01",
    "vacatio_legis": "90 dias",
    "modificacoes": [
      {"alvo": "LC 120/2021", "tipo": "revogação total", "dispositivo": "Toda a lei", "escopo_geografico": "todo o município", "escopo_uso": "todos os usos"}
    ],
    "referencias_externas": [
      {"tipo": "Lei Federal", "numero": "10.257", "ano": "2001", "contexto": "Estatuto da Cidade"}
    ]
  },
  "hierarquia_viaria": {
    "definida_na_lei": true,
    "hierarquias_existentes": [
      {"nome": "arterial", "descricao": "vias de grande fluxo, conectam regiões", "caracteristicas": {"largura_minima_m": "20", "faixas": "4"}, "fonte": "LC 148/2023, Art. 200"},
      {"nome": "coletora", "descricao": "vias que coletam tráfego das locais", "caracteristicas": null, "fonte": "LC 148/2023, Art. 200"},
      {"nome": "local", "descricao": null, "caracteristicas": null, "fonte": "LC 148/2023, Art. 200"}
    ],
    "vias_mapeadas": [
      {"via": "Av. Beira-Mar", "hierarquia": "arterial", "trecho": null, "fonte": "LC 148/2023, Anexo 5"},
      {"via": "Rua das Acácias", "hierarquia": "local", "trecho": null, "fonte": "LC 148/2023, Anexo 5"}
    ],
    "fonte": "LC 148/2023, Art. 200 + Anexo 5"
  },
  "zonas": [
    {
      "variantes_observadas": [],
      "hierarquia": {"UT1": "AP2", "UT2": "ZR5", "UT3": null, "UT4": null, "UT5": null, "UT6": null, "UT7": null},
      "zoneamento_ambiental_sobreposto": null,
      "fonte_definicao": "LC 148/2023, Art. 70",
      "usos_permitidos": {
        "residencial_unifamiliar": {"status": "SIM", "condicao": null, "fonte": "LC 148/2023, Anexo 2.3"},
        "residencial_multifamiliar": {"status": "SIM", "condicao": null, "fonte": "LC 148/2023, Anexo 2.3"},
        "residencial_his": {"status": "NÃO", "condicao": null, "fonte": "LC 148/2023, Anexo 2.3"},
        "residencial_transitorio_hotel": {"status": "NI", "condicao": null, "fonte": null},
        "comercial": {"status": "CONDICIONADO", "condicao": "apenas no térreo", "fonte": "LC 148/2023, Art. 75"},
        "servicos": {"status": "NÃO", "condicao": null, "fonte": "LC 148/2023, Anexo 2.3"},
        "uso_misto": {"status": "NÃO", "condicao": null, "fonte": "LC 148/2023, Anexo 2.3"},
        "industrial": {"status": "NÃO", "condicao": null, "fonte": "LC 148/2023, Anexo 2.3"},
        "institucional": {"status": "SIM", "condicao": null, "fonte": "LC 148/2023, Anexo 2.3"}
      },
      "parametros_gerais": {
        "area_lote_minimo_m2": {"valor": "360", "fonte": "LC 148/2023, Art. 70, II"},
        "area_lote_maximo_m2": {"valor": "NI", "fonte": null},
        "testada_minima_m": {"valor": "12", "fonte": "LC 148/2023, Art. 70, II"},
        "area_doacao_pct": {"valor": "35", "fonte": "LC 148/2023, Art. 340"},
        "permeabilidade_minima_pct": {"valor": "20", "fonte": "LC 148/2023, Art. 70"},
        "quota_ideal_m2_economia": {"valor": "NI", "fonte": null},
        "afastamento_entre_blocos": {"valor": "NI", "fonte": null},
        "gabarito_max_nao_afastado_pavimentos": {"valor": "4", "fonte": "LC 148/2023, Anexo 2.4"},
        "gabarito_max_nao_afastado_altura_m": {"valor": "12", "fonte": "LC 148/2023, Anexo 2.4"},
        "isencao_outorga_onerosa": {"valor": "Não", "fonte": "LC 148/2023, Art. 350"}
      },
      "parametros_por_uso": null,
      "variacoes": {
        "por_hierarquia_viaria": [
          {
            "hierarquia": "arterial",
            "parametros_alterados": {
              "taxa_ocupacao_maxima_pct": {"valor": "80", "fonte": "LC X, Art. Y"},
              "recuo_frontal_m": {"valor": "5", "fonte": "LC X, Art. Y"}
            },
            "fonte": "LC X, Art. Y"
          },
          {
            "hierarquia": "coletora",
            "parametros_alterados": {
              "taxa_ocupacao_maxima_pct": {"valor": "70", "fonte": "LC X, Art. Y"}
            },
            "fonte": "LC X, Art. Y"
          }
        ],
        "por_via_especifica": [
          {
            "via": "Av. Beira-Mar",
            "lado": "AMBOS",
            "trecho": "entre Rua A e Rua B (se especificado, senão null)",
            "parametros_alterados": {
              "altura_maxima_m": {"valor": "12", "fonte": "LC X, Art. Y"}
            },
            "fonte": "LC X, Art. Y"
          }
        ],
        "por_esquina": {
          "tem_variacao": true,
          "detalhes": "Área Lote Mínimo = 360m² meio quadra, 480m² esquina",
          "parametros_alterados": {
            "area_lote_minimo_m2": {"valor": "480 (esquina) vs 360 (meio quadra)"}
          },
          "fonte": "LC 148/2023, Art. 70, III"
        },
        "por_declividade": {
          "tem_variacao": false,
          "condicao": "ex: declividade > 30%",
          "parametros_alterados": null,
          "fonte": null
        },
        "por_altitude": {
          "tipo": "FAIXAS",
          "faixas": [
            {"intervalo": "0-50m", "altura_maxima_m": "12"},
            {"intervalo": ">50m", "altura_maxima_m": "9"}
          ],
          "fonte": "LC X, Art. Y"
        },
        "por_sobreposicao": [
          {
            "zona_sobreposta": "ZEAT",
            "localizacao": "Setor 369, Quadra 01",
            "regra": "prevalece_restritivo",
            "regra_descricao": "prevalece o regime mais restritivo entre as zonas sobrepostas",
            "fonte": "LC X, Art. Y / Errata Z-AAAA"
          }
        ]
      },
      "acrescimos_extraordinarios": [
        {
          "percentual_acrescimo": {"valor": "20%", "fonte": "LC X, Art. Y"},
          "base_calculo": "área computável básica",
          "fator_motivador": "Fachada Ativa",
          "condicao_aplicabilidade": "térreo voltado para via pública com 50%+ de aberturas comerciais",
          "fonte": "LC X/AAAA, Art. Y, § Z"
        },
        {
          "percentual_acrescimo": {"valor": "10%"},
          "base_calculo": "área computável básica",
          "fator_motivador": "Certificação LEED",
          "condicao_aplicabilidade": "empreendimentos com selo LEED Silver ou superior",
          "fonte": "LC X, Art. Y"
        }
      ],
      "metodologia_area_computavel": {
        "residencial_unifamiliar": {
          "considera_apenas_areas_privativas": {"valor": "SIM | NÃO | NI", "fonte": "LC X, Art. Y"},
          "considera_areas_varandas": {"valor": "SIM | PARCIAL | NÃO | NI", "fonte": "LC X, Art. Y"},
          "detalhes_varandas": "ex: varandas com até 10% da área da unidade não computam",
          "fator_area_privativa_x_computavel": {"valor": "1.0", "fonte": "LC X, Art. Y"}
        },
        "residencial_multifamiliar": {
          "considera_apenas_areas_privativas": {"valor": "NI", "fonte": null},
          "considera_areas_varandas": {"valor": "NI", "fonte": null},
          "detalhes_varandas": null,
          "fator_area_privativa_x_computavel": {"valor": "NI", "fonte": null}
        }
      },
      "afastamentos_crescentes": {
        "residencial_unifamiliar": {
          "tem_variacao": false,
          "ativacao": null,
          "afastamento_abaixo_ativacao": null,
          "incremento_por_pavimento": null,
          "a_partir_de_qual_pavimento_incrementa": null,
          "regra_descricao": null,
          "fonte": null
        },
        "residencial_multifamiliar": {
          "tem_variacao": true,
          "ativacao": {
            "altura_metros": "9",
            "pavimento": "3",
            "criterio": "altura"
          },
          "afastamento_abaixo_ativacao": {
            "frontal_m": "4",
            "lateral_m": "1.5",
            "fundos_m": "3"
          },
          "incremento_por_pavimento": {
            "frontal_m_por_pav": "0.5",
            "lateral_m_por_pav": "0.5",
            "fundos_m_por_pav": "0.5"
          },
          "a_partir_de_qual_pavimento_incrementa": "4",
          "regra_descricao": "afastamentos aumentam 0,5m a cada pavimento acima do 3º (h>9m)",
          "fonte": "LC X, Art. Y"
        }
      }
    }
  ],
  "observacoes_gerais": "Observações sobre o que foi visto neste batch, padrões urbanísticos identificados, lacunas, alertas para outros batches."
}

### Notas sobre o formato

- **null significa "não vi neste batch"** — outro batch pode preencher.
- **"NI" significa "lei não define este parâmetro"** — não vai mudar em outros batches.
- **parametros_por_uso: null** quando os parâmetros são gerais da zona (mesma regra pra todos os usos).
- **parametros_por_uso: {...}** apenas quando a lei dá valores ESPECÍFICOS por uso.
- **acrescimos_extraordinarios**: capture TODOS os mecanismos de incentivo construtivo (Fachada Ativa, Outorga Onerosa, IPTU Verde, TPC, OUC, HIS, Telhado Verde, Doação de área, certificações). Retorne array de objetos. Se NÃO houver acréscimo, retorne `[]` ou `null`.
- **metodologia_area_computavel**: para CADA uso permitido, identifique (a) se a área computável considera apenas áreas privativas, (b) se considera áreas das varandas (sim/parcial/não), e (c) o fator área privativa × computável quando aplicável. Use "NI" se a lei não especifica.
- **afastamentos_crescentes**: para CADA uso permitido, capture a regra de afastamentos progressivos quando aplicável (altura/pavimento de ativação, afastamentos abaixo da ativação, incremento por pavimento). Se não houver afastamento progressivo, retorne `tem_variacao: false` e os demais campos como `null`.
- **Não preencha estrutura de planilha** — isso é trabalho do Python depois.

---

## PARTE 10 — CAPTURAS OBRIGATÓRIAS (CHECKLIST)

Antes de finalizar o JSON, REVISE pra cada zona detectada:

### Checklist obrigatório

**✅ Identificação:**
- [ ] `hierarquia` (UT1-UT7) preenchida com pelo menos UT1 (REGRAS 0.1 e 0.5)
- [ ] `dispositivo_definidor` apontando para Art./§ específico (REGRA 2.D)
- [ ] `fonte_definicao` apontando para Art./§ específico

**✅ Usos permitidos:**
- [ ] Cada uso da estrutura tem `status` (SIM/NÃO/CONDICIONADO/NI)
- [ ] Usos com `status: SIM` ou `CONDICIONADO` listados nos `parametros_por_uso` quando aplicável

**✅ Parâmetros:**
- [ ] Se uniforme → `parametros_gerais` preenchido + `parametros_por_uso: null`
- [ ] Se varia por uso → `parametros_por_uso: {...}` com cada uso explícito

**✅ Variações (REGRA CRÍTICA — preencha SEMPRE quando aplicável):**
- [ ] `hierarquia_viaria.definida_na_lei`: respondido?
- [ ] `hierarquia_viaria.hierarquias_existentes`: nomes EXATOS da lei (sem normalizar)?
- [ ] `hierarquia_viaria.vias_mapeadas`: capturou TODAS as vias listadas?
- [ ] `variacoes.por_hierarquia_viaria`: capturou variações por categoria (arterial/coletora/etc)?
- [ ] `variacoes.por_via_especifica`: capturou variações em vias NOMEADAS? (verifique CORPO + ERRATA)
- [ ] `variacoes.por_esquina`: capturou regra de lotes de esquina?
- [ ] `variacoes.por_declividade`: terrenos inclinados têm regras diferentes?
- [ ] `variacoes.por_altitude`: tipo definido (FAIXAS/COTA_MAX/NÃO)?
- [ ] `variacoes.por_sobreposicao`: zona sobrepõe com outra? Errata costuma trazer isso.

**✅ Acréscimos Extraordinários (REGRA CRÍTICA — verifique TODOS):**
- [ ] Fachada Ativa?
- [ ] Outorga Onerosa do Direito de Construir?
- [ ] IPTU Verde / Certificações Sustentáveis (LEED, AQUA)?
- [ ] Transferência de Potencial Construtivo (TPC)?
- [ ] Operação Urbana Consorciada (OUC)?
- [ ] Bônus por HIS?
- [ ] Telhado/Cobertura Verde?
- [ ] Doação de área para equipamento público?
- [ ] Qualquer outro mecanismo de incentivo?

**✅ Metodologia Área Computável (POR USO):**
- [ ] Para cada uso permitido: `considera_apenas_areas_privativas` preenchido?
- [ ] Para cada uso: `considera_areas_varandas` (SIM/PARCIAL/NÃO/NI)?
- [ ] Se PARCIAL: `detalhes_varandas` explica a condição?

**✅ Afastamentos Crescentes (POR USO):**
- [ ] Para cada uso: `tem_variacao` definido?
- [ ] Se `true`: `ativacao` (altura/pavimento) + `incremento_por_pavimento` preenchidos?

### Lembrete final

**NUNCA omita** uma seção apenas porque "não viu" — preencha com:
- `null` se o parâmetro não foi visto neste batch (outro batch pode preencher)
- `"NI"` se a lei explicitamente não define
- `[]` para listas vazias (não tem variações/acréscimos)

A diferença entre `null` e `[]` IMPORTA:
- `null` = "ainda não verifiquei"
- `[]` = "verifiquei e não há"

