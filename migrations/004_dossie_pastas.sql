-- ═══════════════════════════════════════════════════════════════════════════════
-- Migration 004: Suporte ao novo fluxo de dossiês (formato v2)
-- ═══════════════════════════════════════════════════════════════════════════════
-- Data: 17/05/2026
-- Objetivos:
--   1. Vincular processamentos a dossiês (FK)
--   2. Saber quais etapas do pipeline já rodaram (etapas_concluidas)
--   3. Salvar metadados das pastas geradas pelo organizador
--   4. Estrutura pra uploads pendentes de anexos faltantes (Fase 3)
-- ═══════════════════════════════════════════════════════════════════════════════

-- ─── 1) Adiciona FK e etapas_concluidas em legislacao_processamentos ───────────

ALTER TABLE legislacao_processamentos 
  ADD COLUMN IF NOT EXISTS dossie_id INTEGER REFERENCES dossie_municipios(id) ON DELETE SET NULL;

ALTER TABLE legislacao_processamentos
  ADD COLUMN IF NOT EXISTS etapas_concluidas INTEGER[] DEFAULT '{}';

ALTER TABLE legislacao_processamentos
  ADD COLUMN IF NOT EXISTS legislacao_label VARCHAR(100);

CREATE INDEX IF NOT EXISTS idx_leg_proc_dossie ON legislacao_processamentos(dossie_id);
CREATE INDEX IF NOT EXISTS idx_leg_proc_etapas ON legislacao_processamentos USING GIN(etapas_concluidas);

COMMENT ON COLUMN legislacao_processamentos.dossie_id IS 'FK para dossie_municipios. NULL se processamento avulso (manual).';
COMMENT ON COLUMN legislacao_processamentos.etapas_concluidas IS 'Array de etapas do pipeline ja concluidas. Ex: {1,2,3,4} ou {1,2,3,4,4.5} ou {1,2,3,4,5,6,7,8}';
COMMENT ON COLUMN legislacao_processamentos.legislacao_label IS 'Label da legislacao (ex: LC_148_2023). Identifica qual legislacao dentro do dossie.';

-- ─── 2) Tabela com metadados das pastas geradas pelo organizador ──────────────

CREATE TABLE IF NOT EXISTS dossie_legislacoes_pasta (
    id SERIAL PRIMARY KEY,
    dossie_id INTEGER NOT NULL REFERENCES dossie_municipios(id) ON DELETE CASCADE,
    legislacao_label VARCHAR(100) NOT NULL,       -- 'LC_148_2023'
    legislacao_meta JSONB NOT NULL,                -- meta original do legislacoes.json
    categoria VARCHAR(50),                          -- 'plano_diretor', 'outros', etc
    pasta_path TEXT NOT NULL,                       -- '/var/www/urbanlex/static/dossies/5/LC_148_2023'
    pdf_concatenado_path TEXT,                      -- pdf_concatenado.pdf
    n_paginas INTEGER DEFAULT 0,
    total_arquivos INTEGER DEFAULT 0,
    arquivos_originais JSONB DEFAULT '[]'::JSONB,   -- lista de arquivos da pasta
    arquivos_falhas JSONB DEFAULT '[]'::JSONB,      -- arquivos que falharam conversao
    duplicados_removidos INTEGER DEFAULT 0,
    -- Etapa 4.5: lista de anexos CITADOS no corpo (preenchido depois do pipeline)
    anexos_citados JSONB DEFAULT '[]'::JSONB,
    anexos_faltantes JSONB DEFAULT '[]'::JSONB,
    criado_em TIMESTAMP DEFAULT NOW(),
    atualizado_em TIMESTAMP DEFAULT NOW(),
    UNIQUE (dossie_id, legislacao_label)
);

CREATE INDEX IF NOT EXISTS idx_dlp_dossie ON dossie_legislacoes_pasta(dossie_id);
CREATE INDEX IF NOT EXISTS idx_dlp_label ON dossie_legislacoes_pasta(legislacao_label);

COMMENT ON TABLE dossie_legislacoes_pasta IS 'Metadados da pasta de cada legislacao em /static/dossies/<id>/<label>/';
COMMENT ON COLUMN dossie_legislacoes_pasta.arquivos_originais IS 'Lista de arquivos da pasta: [{nome, tipo, tamanho, conversao_ok, foi_convertido}]';
COMMENT ON COLUMN dossie_legislacoes_pasta.anexos_citados IS 'Etapa 4.5: anexos detectados pelo Haiku como citados no corpo da lei';
COMMENT ON COLUMN dossie_legislacoes_pasta.anexos_faltantes IS 'Anexos citados que NAO foram baixados pela busca automatica';

-- ─── 3) Tabela pra uploads pendentes de anexos (Fase 3 do plano) ──────────────

CREATE TABLE IF NOT EXISTS dossie_anexos_uploads (
    id SERIAL PRIMARY KEY,
    dossie_id INTEGER NOT NULL REFERENCES dossie_municipios(id) ON DELETE CASCADE,
    legislacao_label VARCHAR(100) NOT NULL,
    arquivo_path TEXT NOT NULL,                     -- /var/www/urbanlex/static/dossies/5/LC_X/upload_pendente/file.pdf
    nome_original VARCHAR(500),                     -- nome com que o operador enviou
    refere_a VARCHAR(200),                           -- ex: "Anexo 1.4" (campo opcional)
    tamanho_bytes BIGINT DEFAULT 0,
    md5_hash VARCHAR(32),
    aplicado BOOLEAN DEFAULT FALSE,                 -- TRUE depois que reroda Etapa 4
    aplicado_em TIMESTAMP,
    criado_em TIMESTAMP DEFAULT NOW(),
    criado_por INTEGER REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_dau_dossie ON dossie_anexos_uploads(dossie_id);
CREATE INDEX IF NOT EXISTS idx_dau_pendentes ON dossie_anexos_uploads(dossie_id) WHERE aplicado=FALSE;

COMMENT ON TABLE dossie_anexos_uploads IS 'Anexos enviados manualmente pelo operador (uploads aguardando aplicacao via re-catalogacao)';
COMMENT ON COLUMN dossie_anexos_uploads.aplicado IS 'TRUE depois de clicar "Adicionar arquivos e atualizar catalogacao"';

-- ─── Verificacao ───────────────────────────────────────────────────────────────

\d legislacao_processamentos
\d dossie_legislacoes_pasta
\d dossie_anexos_uploads
