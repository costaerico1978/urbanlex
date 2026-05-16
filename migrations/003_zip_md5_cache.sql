-- ═══════════════════════════════════════════════════════════════════════════════
-- MIGRATION 003 — Cache Inteligente via MD5 do ZIP
-- ═══════════════════════════════════════════════════════════════════════════════
-- Adiciona zip_md5 em legislacao_processamentos pra permitir cache
-- automático: se mesmo município processado com MESMO ZIP (MD5 igual),
-- reaproveita resultado em vez de re-rodar o pipeline.
--
-- Data: 2026-05-16
-- ═══════════════════════════════════════════════════════════════════════════════

ALTER TABLE legislacao_processamentos
    ADD COLUMN IF NOT EXISTS zip_md5 VARCHAR(32);

CREATE INDEX IF NOT EXISTS idx_lp_zip_md5 
    ON legislacao_processamentos(municipio, estado, zip_md5);

COMMENT ON COLUMN legislacao_processamentos.zip_md5 IS 
    'MD5 do ZIP processado. Usado pra cache inteligente: se mesmo município e mesmo MD5, reaproveita resultado.';

