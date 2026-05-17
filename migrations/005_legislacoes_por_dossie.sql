-- ═══════════════════════════════════════════════════════════════════════════════
-- Migration 005: Cada DOSSIÊ (busca) tem seu próprio conjunto de legislações
-- ═══════════════════════════════════════════════════════════════════════════════
-- Data: 17/05/2026
-- 
-- Mudança conceitual:
--   ANTES: legislações organizadas eram POR MUNICÍPIO (1 versão consolidada)
--   AGORA: legislações organizadas são POR BUSCA (snapshot histórico)
--
-- Estrutura de pastas muda de:
--   /static/dossies/<mun_id>/<label>/
-- para:
--   /static/dossies/<mun_id>/busca_<bh_id>/<label>/
-- ═══════════════════════════════════════════════════════════════════════════════

-- ─── 1) Adiciona busca_historico_id em dossie_legislacoes_pasta ────────────────

ALTER TABLE dossie_legislacoes_pasta
  ADD COLUMN IF NOT EXISTS busca_historico_id INTEGER REFERENCES buscas_historico(id) ON DELETE CASCADE;

CREATE INDEX IF NOT EXISTS idx_dlp_busca ON dossie_legislacoes_pasta(busca_historico_id);

COMMENT ON COLUMN dossie_legislacoes_pasta.busca_historico_id IS 'FK para buscas_historico. Cada dossie tem seu proprio conjunto de legislacoes organizadas.';

-- ─── 2) Remove a UNIQUE antiga e cria nova UNIQUE por (busca, label) ──────────

ALTER TABLE dossie_legislacoes_pasta
  DROP CONSTRAINT IF EXISTS dossie_legislacoes_pasta_dossie_id_legislacao_label_key;

-- Cria nova UNIQUE CONSTRAINT (nao parcial, pra suportar ON CONFLICT)
ALTER TABLE dossie_legislacoes_pasta
  ADD CONSTRAINT IF NOT EXISTS dossie_legislacoes_pasta_busca_label_key 
  UNIQUE (busca_historico_id, legislacao_label);

-- ─── 3) Limpa dados antigos (operador confirmou: pode apagar) ─────────────────

DELETE FROM dossie_anexos_uploads;
DELETE FROM dossie_legislacoes_pasta;

-- ─── Verificacao ───────────────────────────────────────────────────────────────

\d dossie_legislacoes_pasta
