-- ============================================================
-- UrbanLex — Schema PostgreSQL Completo v3.0
-- 25 tabelas
-- ============================================================

-- ── AUTENTICAÇÃO ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS users (
    id              SERIAL PRIMARY KEY,
    nome            VARCHAR(200) NOT NULL,
    email           VARCHAR(200) UNIQUE NOT NULL,
    senha_hash      VARCHAR(256) NOT NULL,
    role            VARCHAR(20) DEFAULT 'apenas_leitura',
    ativo           BOOLEAN DEFAULT FALSE,
    aprovado        BOOLEAN DEFAULT FALSE,
    criado_em       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ultimo_acesso   TIMESTAMP
);

CREATE TABLE IF NOT EXISTS aprovacao_tokens (
    id          SERIAL PRIMARY KEY,
    user_id     INTEGER REFERENCES users(id) ON DELETE CASCADE,
    token       VARCHAR(100) UNIQUE NOT NULL,
    tipo        VARCHAR(20) DEFAULT 'ativacao',
    usado       BOOLEAN DEFAULT FALSE,
    expira_em   TIMESTAMP NOT NULL,
    criado_em   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS password_reset_tokens (
    id          SERIAL PRIMARY KEY,
    user_id     INTEGER REFERENCES users(id) ON DELETE CASCADE,
    token       VARCHAR(100) UNIQUE NOT NULL,
    usado       BOOLEAN DEFAULT FALSE,
    expira_em   TIMESTAMP NOT NULL,
    criado_em   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ── SUPORTE GERAL ───────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS municipios (
    id               SERIAL PRIMARY KEY,
    nome             VARCHAR(200) NOT NULL,
    estado           VARCHAR(50),
    url_diario       TEXT,
    tipo_site        VARCHAR(50),
    config_extracao  JSONB,
    ativo            BOOLEAN DEFAULT TRUE,
    ultimo_monitoramento TIMESTAMP,
    perfil_detectado_em  TIMESTAMP,
    criado_em        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    atualizado_em    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- ── PERFIS DE DIÁRIO OFICIAL (aprendizado do scraper inteligente) ───────────

CREATE TABLE IF NOT EXISTS perfis_diario (
    id                  SERIAL PRIMARY KEY,
    municipio_id        INTEGER REFERENCES municipios(id) ON DELETE CASCADE,
    -- Identificação do site
    url_base            TEXT NOT NULL,
    plataforma          VARCHAR(50),      -- iobnet | dom | amm | custom | desconhecido
    -- Resultado da detecção
    status_deteccao     VARCHAR(20) DEFAULT 'pendente',  -- pendente | ok | falhou | captcha | login
    erro_deteccao       TEXT,
    -- Perfil de navegação (gerado pela IA)
    perfil_json         JSONB,            -- seletores, parâmetros, passos de navegação
    screenshot_b64      TEXT,             -- screenshot da página principal (base64, comprimido)
    -- Controle
    detectado_em        TIMESTAMP,
    ultima_execucao_ok  TIMESTAMP,
    falhas_consecutivas INTEGER DEFAULT 0,
    requer_playwright   BOOLEAN DEFAULT FALSE,
    requer_login        BOOLEAN DEFAULT FALSE,
    requer_captcha      BOOLEAN DEFAULT FALSE,
    criado_em           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    atualizado_em       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(municipio_id)
);

-- ── BIBLIOTECA — TABELAS DE SUPORTE ──────────────────────────

CREATE TABLE IF NOT EXISTS tipos_legislacao (
    id          SERIAL PRIMARY KEY,
    nome        VARCHAR(100) UNIQUE NOT NULL,
    descricao   TEXT,
    criado_por  INTEGER REFERENCES users(id),
    criado_em   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Valores padrão de tipos
INSERT INTO tipos_legislacao (nome) VALUES
    ('Lei Ordinária'),
    ('Lei Complementar'),
    ('Decreto'),
    ('Decreto-Lei'),
    ('Portaria'),
    ('Resolução'),
    ('Instrução Normativa'),
    ('Instrução Técnica'),
    ('Medida Provisória'),
    ('Emenda Constitucional'),
    ('Plano Diretor'),
    ('Código de Obras'),
    ('Código de Posturas'),
    ('Regulamento')
ON CONFLICT (nome) DO NOTHING;

CREATE TABLE IF NOT EXISTS assuntos_legislacao (
    id          SERIAL PRIMARY KEY,
    nome        VARCHAR(100) UNIQUE NOT NULL,
    descricao   TEXT,
    criado_por  INTEGER REFERENCES users(id),
    criado_em   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO assuntos_legislacao (nome) VALUES
    ('Zoneamento e Uso do Solo'),
    ('Plano Diretor'),
    ('Código de Obras'),
    ('Código Tributário'),
    ('Meio Ambiente'),
    ('Mobilidade Urbana'),
    ('Habitação de Interesse Social'),
    ('Patrimônio Histórico'),
    ('Licenciamento Ambiental'),
    ('Saneamento Básico'),
    ('Regularização Fundiária'),
    ('Outorga Onerosa'),
    ('Transferência do Direito de Construir')
ON CONFLICT (nome) DO NOTHING;

-- ── BIBLIOTECA — LEGISLAÇÕES ──────────────────────────────────

CREATE TABLE IF NOT EXISTS legislacoes (
    id                   SERIAL PRIMARY KEY,
    -- Identificação
    pais                 VARCHAR(10) DEFAULT 'BR',
    esfera               VARCHAR(20) DEFAULT 'municipal',  -- federal, estadual, municipal
    estado               VARCHAR(50),
    municipio_id         INTEGER REFERENCES municipios(id),
    municipio_nome       VARCHAR(200),
    tipo_id              INTEGER REFERENCES tipos_legislacao(id),
    tipo_nome            VARCHAR(100),
    numero               VARCHAR(50),
    ano                  INTEGER,
    data_publicacao      DATE,
    -- Conteúdo
    ementa               TEXT,
    assunto_id           INTEGER REFERENCES assuntos_legislacao(id),
    assunto_nome         VARCHAR(100),
    palavras_chave       TEXT,   -- JSON array de strings
    -- Documento
    arquivo_url          TEXT,   -- URL no Cloudflare R2
    arquivo_nome         VARCHAR(500),
    arquivo_tipo         VARCHAR(20),  -- pdf, doc, jpg, etc
    conteudo_texto       TEXT,   -- texto extraído
    url_original         TEXT,   -- URL pública da fonte original
    -- Status
    status               VARCHAR(20) DEFAULT 'vigente',  -- vigente, revogada
    em_monitoramento     BOOLEAN DEFAULT FALSE,
    data_inicio_monitoramento DATE,  -- data a partir da qual monitorar diários
    data_fim_monitoramento DATE,    -- até quando monitorar (NULL = até hoje)
    ultima_verificacao_monitoramento DATE,  -- até onde já verificou
    -- Origem e aprovação
    origem               VARCHAR(20) DEFAULT 'manual',  -- manual, busca_ia, monitoramento
    pendente_aprovacao   BOOLEAN DEFAULT FALSE,
    aprovado_em          TIMESTAMP,
    aprovado_por         INTEGER REFERENCES users(id),
    -- Controle
    hash_conteudo        VARCHAR(64),
    processado           BOOLEAN DEFAULT FALSE,
    criado_em            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    atualizado_em        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_leg_estado ON legislacoes(estado);
CREATE INDEX IF NOT EXISTS idx_leg_municipio ON legislacoes(municipio_id);
CREATE INDEX IF NOT EXISTS idx_leg_tipo ON legislacoes(tipo_id);
CREATE INDEX IF NOT EXISTS idx_leg_ano ON legislacoes(ano);
CREATE INDEX IF NOT EXISTS idx_leg_status ON legislacoes(status);
CREATE INDEX IF NOT EXISTS idx_leg_pendente ON legislacoes(pendente_aprovacao);
CREATE INDEX IF NOT EXISTS idx_leg_monitoramento ON legislacoes(em_monitoramento);

-- Árvore genealógica: relações entre legislações
CREATE TABLE IF NOT EXISTS legislacao_relacoes (
    id                   SERIAL PRIMARY KEY,
    legislacao_pai_id    INTEGER REFERENCES legislacoes(id) ON DELETE CASCADE,
    legislacao_filha_id  INTEGER REFERENCES legislacoes(id) ON DELETE CASCADE,
    tipo_relacao         VARCHAR(50),  -- altera, revoga, regulamenta, complementa, acrescenta
    descricao            TEXT,
    data_relacao         DATE,
    criado_em            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(legislacao_pai_id, legislacao_filha_id)
);

-- ── MONITORAMENTO (v1.3) ─────────────────────────────────────

CREATE TABLE IF NOT EXISTS categorias (
    id          SERIAL PRIMARY KEY,
    nome        VARCHAR(100) NOT NULL,
    descricao   TEXT,
    cor         VARCHAR(20) DEFAULT '#3d9be9',
    criado_em   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS alteracoes (
    id                      SERIAL PRIMARY KEY,
    legislacao_id           INTEGER REFERENCES legislacoes(id),
    tipo_alteracao          VARCHAR(50),
    descricao               TEXT,
    hash_anterior           VARCHAR(64),
    hash_novo               VARCHAR(64),
    data_deteccao           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    aprovado                BOOLEAN DEFAULT FALSE,
    aprovado_em             TIMESTAMP,
    aprovado_por            INTEGER REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS analise_zonas (
    id              SERIAL PRIMARY KEY,
    alteracao_id    INTEGER REFERENCES alteracoes(id),
    zona            VARCHAR(50),
    parametro       VARCHAR(100),
    valor_anterior  TEXT,
    valor_novo      TEXT,
    criado_em       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS divergencias (
    id              SERIAL PRIMARY KEY,
    alteracao_id    INTEGER REFERENCES alteracoes(id),
    campo           VARCHAR(100),
    valor_groq      TEXT,
    valor_gemini    TEXT,
    resolvido       BOOLEAN DEFAULT FALSE,
    resolucao       TEXT,
    criado_em       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS legislacoes_nao_encontradas (
    id              SERIAL PRIMARY KEY,
    municipio_id    INTEGER REFERENCES municipios(id),
    referencia      VARCHAR(200),
    tentativas      INTEGER DEFAULT 1,
    ultima_tentativa TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    criado_em       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS scheduler_config (
    id                  SERIAL PRIMARY KEY,
    horario_execucao    VARCHAR(5) DEFAULT '02:00',
    status              VARCHAR(30) DEFAULT 'ativo',
    proxima_execucao    TIMESTAMP,
    ultima_execucao     TIMESTAMP,
    motivo_pausa        TEXT,
    pausado_por         INTEGER REFERENCES users(id),
    atualizado_em       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    debug_ativo         BOOLEAN DEFAULT TRUE,
    email_relatorio     BOOLEAN DEFAULT TRUE
);

INSERT INTO scheduler_config (horario_execucao, status, debug_ativo, email_relatorio)
VALUES ('02:00', 'ativo', TRUE, TRUE)
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS scheduler_execucoes (
    id                      SERIAL PRIMARY KEY,
    iniciada_em             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finalizada_em           TIMESTAMP,
    status                  VARCHAR(20) DEFAULT 'rodando',   -- rodando | concluido | erro
    disparado_por           INTEGER REFERENCES users(id),
    -- Contadores
    municipios_processados  INTEGER DEFAULT 0,
    municipios_ok           INTEGER DEFAULT 0,
    municipios_erro         INTEGER DEFAULT 0,
    alteracoes_detectadas   INTEGER DEFAULT 0,
    -- Log e debug
    log                     TEXT,          -- resumo do ciclo
    log_erros               TEXT,          -- stack traces e erros por município
    erros                   INTEGER DEFAULT 0,
    -- Notificação
    email_enviado           BOOLEAN DEFAULT FALSE
);

-- Log detalhado por legislação por execução
CREATE TABLE IF NOT EXISTS monitoramento_legislacao_log (
    id                      SERIAL PRIMARY KEY,
    execucao_id             INTEGER REFERENCES scheduler_execucoes(id) ON DELETE CASCADE,
    legislacao_id           INTEGER REFERENCES legislacoes(id) ON DELETE CASCADE,
    municipio_id            INTEGER REFERENCES municipios(id) ON DELETE CASCADE,
    data                    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    iniciada_em             TIMESTAMP,
    finalizada_em           TIMESTAMP,
    status                  VARCHAR(20) DEFAULT 'ok',   -- ok | parcial | erro
    sucesso                 BOOLEAN DEFAULT TRUE,
    -- Contadores detalhados
    publicacoes_encontradas INTEGER DEFAULT 0,  -- total retornado pelo scraper
    publicacoes_analisadas  INTEGER DEFAULT 0,  -- enviadas à IA para análise
    alteracoes_detectadas   INTEGER DEFAULT 0,  -- alterações encontradas pela IA
    publicacoes_duplicadas  INTEGER DEFAULT 0,  -- já existiam no banco
    -- Detalhes
    metodo_busca            VARCHAR(30),        -- scraper | fallback | nenhum
    url_acessada            TEXT,
    mensagem                TEXT,               -- resumo legível
    erro                    TEXT                -- mensagem de erro se houver
);
CREATE INDEX IF NOT EXISTS idx_mll_legislacao ON monitoramento_legislacao_log(legislacao_id);
CREATE INDEX IF NOT EXISTS idx_mll_municipio ON monitoramento_legislacao_log(municipio_id);
CREATE INDEX IF NOT EXISTS idx_mll_data ON monitoramento_legislacao_log(data DESC);

CREATE TABLE IF NOT EXISTS legislacoes_versoes (
    id              SERIAL PRIMARY KEY,
    legislacao_id   INTEGER REFERENCES legislacoes(id),
    hash_conteudo   VARCHAR(64),
    conteudo_texto  TEXT,
    versao          INTEGER DEFAULT 1,
    criado_em       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS alteracoes_pendentes (
    id              SERIAL PRIMARY KEY,
    municipio_id    INTEGER REFERENCES municipios(id),
    legislacao_id   INTEGER REFERENCES legislacoes(id),
    tipo_alteracao  VARCHAR(50),
    descricao       TEXT,
    conteudo_novo   TEXT,
    status          VARCHAR(20) DEFAULT 'pendente',
    detectada_em    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    validada_em     TIMESTAMP,
    validada_por    INTEGER REFERENCES users(id),
    observacoes     TEXT
);

CREATE TABLE IF NOT EXISTS notificacoes_admin (
    id          SERIAL PRIMARY KEY,
    tipo        VARCHAR(50),
    titulo      VARCHAR(200),
    mensagem    TEXT,
    lida        BOOLEAN DEFAULT FALSE,
    lida_em     TIMESTAMP,
    lida_por    INTEGER REFERENCES users(id),
    criada_em   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ── PARÂMETROS URBANÍSTICOS (v2.3) ───────────────────────────

CREATE TABLE IF NOT EXISTS zonas_urbanas (
    id                          SERIAL PRIMARY KEY,
    municipio                   VARCHAR(200),
    estado                      VARCHAR(50),
    zona                        VARCHAR(100),
    subzona                     VARCHAR(100) NOT NULL DEFAULT '',
    descricao                   TEXT,
    legislacao_referencia       VARCHAR(500),
    legislacao_id               INTEGER REFERENCES legislacoes(id),

    -- ResUnif
    res_unif_ca_basico          NUMERIC(6,2),
    res_unif_ca_maximo          NUMERIC(6,2),
    res_unif_to_max             NUMERIC(6,2),
    res_unif_gabarito_max       NUMERIC(6,1),
    res_unif_recuo_frontal      NUMERIC(5,2),
    res_unif_recuo_lateral      NUMERIC(5,2),
    res_unif_recuo_fundos       NUMERIC(5,2),
    res_unif_lote_min           NUMERIC(10,2),
    res_unif_lote_frente_min    NUMERIC(6,2),
    res_unif_formula_area       TEXT,
    res_unif_observacoes        TEXT,

    -- ResMult
    res_mult_ca_basico          NUMERIC(6,2),
    res_mult_ca_maximo          NUMERIC(6,2),
    res_mult_to_max             NUMERIC(6,2),
    res_mult_gabarito_max       NUMERIC(6,1),
    res_mult_recuo_frontal      NUMERIC(5,2),
    res_mult_recuo_lateral      NUMERIC(5,2),
    res_mult_recuo_fundos       NUMERIC(5,2),
    res_mult_lote_min           NUMERIC(10,2),
    res_mult_formula_area       TEXT,
    res_mult_fator_privativa    NUMERIC(4,2),
    res_mult_observacoes        TEXT,

    -- HIS
    his_ca_basico               NUMERIC(6,2),
    his_ca_maximo               NUMERIC(6,2),
    his_to_max                  NUMERIC(6,2),
    his_gabarito_max            NUMERIC(6,1),
    his_recuo_frontal           NUMERIC(5,2),
    his_recuo_lateral           NUMERIC(5,2),
    his_recuo_fundos            NUMERIC(5,2),
    his_formula_area            TEXT,
    his_observacoes             TEXT,

    -- Comercial
    com_ca_basico               NUMERIC(6,2),
    com_ca_maximo               NUMERIC(6,2),
    com_to_max                  NUMERIC(6,2),
    com_gabarito_max            NUMERIC(6,1),
    com_recuo_frontal           NUMERIC(5,2),
    com_recuo_lateral           NUMERIC(5,2),
    com_recuo_fundos            NUMERIC(5,2),
    com_formula_area            TEXT,
    com_fator_privativa         NUMERIC(4,2),
    com_observacoes             TEXT,

    -- Serviços
    serv_ca_basico              NUMERIC(6,2),
    serv_ca_maximo              NUMERIC(6,2),
    serv_to_max                 NUMERIC(6,2),
    serv_gabarito_max           NUMERIC(6,1),
    serv_recuo_frontal          NUMERIC(5,2),
    serv_recuo_lateral          NUMERIC(5,2),
    serv_recuo_fundos           NUMERIC(5,2),
    serv_formula_area           TEXT,
    serv_observacoes            TEXT,

    -- Misto
    misto_ca_basico             NUMERIC(6,2),
    misto_ca_maximo             NUMERIC(6,2),
    misto_to_max                NUMERIC(6,2),
    misto_gabarito_max          NUMERIC(6,1),
    misto_recuo_frontal         NUMERIC(5,2),
    misto_recuo_lateral         NUMERIC(5,2),
    misto_recuo_fundos          NUMERIC(5,2),
    misto_formula_area          TEXT,
    misto_observacoes           TEXT,

    -- Industrial
    ind_ca_basico               NUMERIC(6,2),
    ind_ca_maximo               NUMERIC(6,2),
    ind_to_max                  NUMERIC(6,2),
    ind_gabarito_max            NUMERIC(6,1),
    ind_recuo_frontal           NUMERIC(5,2),
    ind_recuo_lateral           NUMERIC(5,2),
    ind_recuo_fundos            NUMERIC(5,2),
    ind_formula_area            TEXT,
    ind_observacoes             TEXT,

    -- Institucional
    inst_ca_basico              NUMERIC(6,2),
    inst_ca_maximo              NUMERIC(6,2),
    inst_to_max                 NUMERIC(6,2),
    inst_gabarito_max           NUMERIC(6,1),
    inst_recuo_frontal          NUMERIC(5,2),
    inst_recuo_lateral          NUMERIC(5,2),
    inst_recuo_fundos           NUMERIC(5,2),
    inst_formula_area           TEXT,
    inst_observacoes            TEXT,

    -- Outorga onerosa
    outorga_ct                  NUMERIC(6,2),
    outorga_fp                  NUMERIC(6,2),
    outorga_formula             TEXT,
    outorga_observacoes         TEXT,

    -- Controle
    criado_em                   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    atualizado_em               TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    atualizado_por              INTEGER REFERENCES users(id),
    UNIQUE(municipio, zona, subzona)
);

-- ── INTEGRAÇÃO v1.3 → v2.3 ───────────────────────────────────

CREATE TABLE IF NOT EXISTS integracao_atualizacoes (
    id                  SERIAL PRIMARY KEY,
    alteracao_id        INTEGER REFERENCES alteracoes(id),
    legislacao_id       INTEGER REFERENCES legislacoes(id),
    municipio           VARCHAR(200),
    zona                VARCHAR(100),
    subzona             VARCHAR(100) NOT NULL DEFAULT '',
    parametros_json     JSONB,
    status              VARCHAR(20) DEFAULT 'pendente',
    revisado_em         TIMESTAMP,
    revisado_por        INTEGER REFERENCES users(id),
    motivo_rejeicao     TEXT,
    criado_em           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ── BUSCA POR REFERÊNCIA (fila de busca IA) ───────────────────

CREATE TABLE IF NOT EXISTS buscas_ia (
    id              SERIAL PRIMARY KEY,
    consulta        TEXT NOT NULL,
    status          VARCHAR(20) DEFAULT 'pendente',  -- pendente, buscando, encontrado, nao_encontrado, erro
    resultado_url   TEXT,
    resultado_nome  TEXT,
    legislacao_id   INTEGER REFERENCES legislacoes(id),
    erro            TEXT,
    solicitado_por  INTEGER REFERENCES users(id),
    criado_em       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finalizado_em   TIMESTAMP
);

-- ── AUDITORIA ─────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS auditoria (
    id          SERIAL PRIMARY KEY,
    user_id     INTEGER REFERENCES users(id),
    acao        VARCHAR(100),
    tabela      VARCHAR(50),
    registro_id INTEGER,
    dados_antes JSONB,
    dados_apos  JSONB,
    ip          VARCHAR(45),
    criado_em   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
