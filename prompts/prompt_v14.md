# PROMPT v13 — Análise de Legislação Urbanística

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

## PARTE 1 — Sobre esta legislação

**1.1** Qual é a identificação formal desta legislação?

Informe: tipo de ato (Lei Complementar, Lei Ordinária, Decreto, Errata, Portaria), número, ano, município, estado.

**1.2** Qual é a data de publicação? Há vacatio legis? Quando começou a vigorar?

**1.3** Esta lei altera, revoga ou substitui dispositivos de outras leis?

Se não, pule para 1.4.

Se sim, para cada lei afetada, informe:
- Identificação da lei alvo (Ex: LC 270/2024)
- Tipo de modificação: revogação total / revogação parcial / alteração / errata
- Dispositivo afetado (Ex: "Art. 47", "Tabela XV do Anexo II")
- Escopo geográfico da alteração (Ex: "todo o município", "apenas AP-1", "Bairro X")
- Escopo de uso (Ex: "todos os usos", "apenas Comercial")

**1.4** Esta lei referencia outras leis que NÃO estão neste conjunto de documentos?

Se sim, liste cada referência: tipo de ato, número, ano, contexto onde aparece. Marque essas leis como "pendência externa" — não invente o conteúdo delas.

---

## PARTE 2 — Mapeamento territorial

**2.1** Esta legislação define zonas, subzonas ou áreas específicas onde determinados usos são permitidos?

Se não, pule para a PARTE 8.

Se sim, continue.

**2.2** O município é dividido hierarquicamente antes de chegar nas zonas?

Exemplos de hierarquia possível (varia por município):
- Macrozona → Área de Planejamento → Região Administrativa → Bairro → Zona
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

## PARTE 4 — Parâmetros gerais por zona

**4.1** Para cada zona/subzona, esta lei define parâmetros gerais (que valem para a zona toda, sem distinguir por uso)?

Se não viu essa informação neste batch, deixe como null — outro batch pode ter.

Se sim, informe os valores definidos:

PARÂMETROS DO LOTE
- Área Lote Mínimo (m²)
- Área Lote Máximo (m²)
- Testada Mínima (m)
- Área a ser doada em loteamento (%)

PARÂMETROS GERAIS DA ZONA
- Taxa de Permeabilidade Mínima (%)
- Quota Ideal (m²/economia)
- Afastamento entre blocos
- Gabarito máximo Não Afastado — em pavimentos
- Gabarito máximo Não Afastado — em altura (m)
- Isenção de Outorga Onerosa (Sim/Não/Parcial)

Para cada parâmetro: valor + legislação fonte (Ex: "LC 148/2023, Art. 70, II").

Se a lei não define um parâmetro, use "NI" (Não Informado).

---

## PARTE 5 — Parâmetros por uso

**5.1** Para cada zona, os parâmetros urbanísticos variam dependendo do uso?

Se não viu essa informação neste batch, deixe como null — outro batch pode ter.

Se sim, para cada combinação (zona × uso permitido), informe os 11 parâmetros abaixo:

- Coeficiente de Aproveitamento BÁSICO (sem outorga)
- Coeficiente de Aproveitamento MÁXIMO (com outorga onerosa)
- Taxa de Ocupação BÁSICA
- Taxa de Ocupação MÁXIMA
- Gabarito BÁSICO em pavimentos
- Gabarito MÁXIMO em pavimentos
- Gabarito BÁSICO em altura (m)
- Gabarito MÁXIMO em altura (m)
- Afastamento frontal
- Afastamento lateral
- Afastamento de fundos

Para cada um: valor + legislação fonte.

**5.2** Se a lei NÃO distingue parâmetros por uso (define só valores gerais da zona), confirme: "Parâmetros são gerais da zona, valem para todos os usos permitidos."

**5.3** Para zonas onde o uso é NÃO (proibido), confirme: "Uso X proibido na zona Y."

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

Exemplo: se a lei menciona "ZR5 está localizada na Macrozona 2 conforme Art. 277" mas o Anexo 2.4 (que tem os parâmetros) não está neste batch, ainda assim registre ZR5 na lista com fonte_definicao preenchida e todos os campos de parâmetros/usos como null.

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
      "sigla_canonica": "ZR5",
      "variantes_observadas": [],
      "hierarquia": {"UT1": "Macrozona 2", "UT2": null, "UT3": null, "UT4": null, "UT5": null, "UT6": null},
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
- [ ] `sigla_canonica` definida
- [ ] `hierarquia` (UT1-UT6) preenchida quando aplicável
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
