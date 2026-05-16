-- ═══════════════════════════════════════════════════════════════════════════════
-- MIGRATION 002 — Fila de Extração
-- ═══════════════════════════════════════════════════════════════════════════════
-- Cria fila para processamento em background do pipeline_extracao_lei.
--
-- Fluxo:
--   1. Operador enfileira: INSERT em fila_extracao (status='aguardando')
--   2. fila_extracao_worker pega itens em ordem (mais antigos primeiro)
--   3. Roda pipeline_extracao_lei.processar_municipio()
--   4. Salva resultado via salvar_processamento() + consolidar_municipio_db()
--   5. Atualiza status: 'concluido' ou 'erro'
--
-- Data: 2026-05-15
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS fila_extracao (
    id SERIAL PRIMARY KEY,
    
    -- Identificação
    municipio VARCHAR(200) NOT NULL,
    estado VARCHAR(2) NOT NULL,
    zip_path TEXT NOT NULL,
    legislacao_id INTEGER REFERENCES legislacoes(id) ON DELETE SET NULL,
    
    -- Configuração
    usar_cache BOOLEAN DEFAULT TRUE,
    consolidar_apos BOOLEAN DEFAULT TRUE,  -- consolida municipios_consolidado após processar?
    
    -- Status do job
    status VARCHAR(20) DEFAULT 'aguardando',  -- aguardando | rodando | concluido | erro
    job_id VARCHAR(100),
    ordem INTEGER DEFAULT 0,
    
    -- Resultado
    processamento_id INTEGER REFERENCES legislacao_processamentos(id) ON DELETE SET NULL,
    progresso_atual VARCHAR(500),  -- "ETAPA 5/8: extraindo anexo_2.4..."
    erro_etapa INTEGER,
    erro_msg TEXT,
    
    -- Audit
    criado_em TIMESTAMP DEFAULT NOW(),
    iniciado_em TIMESTAMP,
    concluido_em TIMESTAMP,
    criado_por INTEGER REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_fe_status ON fila_extracao(status);
CREATE INDEX IF NOT EXISTS idx_fe_municipio ON fila_extracao(municipio, estado);
CREATE INDEX IF NOT EXISTS idx_fe_ordem_criado ON fila_extracao(ordem ASC, criado_em ASC) WHERE status = 'aguardando';

COMMENT ON TABLE fila_extracao IS 'Fila de extração — itens processados em background pelo fila_extracao_worker';
COMMENT ON COLUMN fila_extracao.status IS 'aguardando | rodando | concluido | erro';
COMMENT ON COLUMN fila_extracao.consolidar_apos IS 'Se TRUE, consolida municipios_consolidado depois do pipeline';

