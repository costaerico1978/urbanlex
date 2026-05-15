-- ═══════════════════════════════════════════════════════════════════════════════
-- MIGRATION 001 — Pipeline de Extração de Lei Urbanística
-- ═══════════════════════════════════════════════════════════════════════════════
-- Cria 2 tabelas pra suportar o pipeline_extracao_lei.py:
--
--   1. legislacao_processamentos
--      → Resultado bruto do pipeline (1 registro por LEI processada)
--      → JSONB com zonas, modificações, métricas
--
--   2. municipios_consolidado
--      → Estado consolidado (1 registro por MUNICÍPIO)
--      → Resultado de aplicar leis em ordem cronológica
--      → Fonte para gerar planilhas/exportações
--
-- Data: 2026-05-15
-- ═══════════════════════════════════════════════════════════════════════════════

-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║ NIVEL 1 — legislacao_processamentos                                       ║
-- ║ Resultado bruto de UMA execução do pipeline (1 lei processada)            ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝
CREATE TABLE IF NOT EXISTS legislacao_processamentos (
    id SERIAL PRIMARY KEY,
    
    -- Vínculo com legislação cadastrada (opcional, pode ser nula em testes)
    legislacao_id INTEGER REFERENCES legislacoes(id) ON DELETE SET NULL,
    
    -- Identificação direta (caso nao tenha legislacao_id ainda)
    municipio VARCHAR(200) NOT NULL,
    estado VARCHAR(2) NOT NULL,
    
    -- Resultado bruto do pipeline
    -- {legislacao: {...}, zonas: {ZR1: {...}, ZR2: {...}}, modificacoes: [], refs_externas: []}
    resultado_json JSONB NOT NULL,
    
    -- Métricas do pipeline
    -- {tempo_total, custo_total, tokens_in, tokens_out, etapas: {1: {...}, ...}}
    metricas JSONB,
    
    -- Versionamento
    pipeline_versao VARCHAR(20) DEFAULT '1.0',
    prompt_versao VARCHAR(20),
    
    -- Status
    sucesso BOOLEAN NOT NULL,
    erro_etapa INTEGER,
    erro_msg TEXT,
    
    -- Caminhos
    zip_path TEXT,
    output_dir TEXT,
    
    -- Auditoria
    processado_em TIMESTAMP DEFAULT NOW(),
    processado_por INTEGER REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_lp_municipio_estado ON legislacao_processamentos(municipio, estado);
CREATE INDEX IF NOT EXISTS idx_lp_legislacao_id ON legislacao_processamentos(legislacao_id);
CREATE INDEX IF NOT EXISTS idx_lp_processado_em ON legislacao_processamentos(processado_em DESC);
CREATE INDEX IF NOT EXISTS idx_lp_sucesso ON legislacao_processamentos(sucesso) WHERE sucesso = true;
-- Index GIN pra queries dentro do JSONB
CREATE INDEX IF NOT EXISTS idx_lp_zonas_gin ON legislacao_processamentos USING GIN ((resultado_json->'zonas'));

COMMENT ON TABLE legislacao_processamentos IS 'Resultado bruto de uma execução do pipeline_extracao_lei.py — uma linha por lei processada';
COMMENT ON COLUMN legislacao_processamentos.resultado_json IS 'JSON do pipeline: {legislacao, zonas, modificacoes, refs_externas}';
COMMENT ON COLUMN legislacao_processamentos.metricas IS 'Métricas: {tempo_total, custo_total, tokens_in, tokens_out, etapas}';


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║ NIVEL 2 — municipios_consolidado                                          ║
-- ║ Estado consolidado de TODAS as leis de um município (1 reg/município)     ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝
CREATE TABLE IF NOT EXISTS municipios_consolidado (
    id SERIAL PRIMARY KEY,
    
    municipio VARCHAR(200) NOT NULL,
    estado VARCHAR(2) NOT NULL,
    
    -- Estado consolidado das zonas (após aplicar todas as leis em ordem cronológica)
    -- {ZR1: {parametros_gerais, usos_permitidos, variacoes, ...}}
    zonas_consolidadas JSONB NOT NULL,
    
    -- IDs das leis que contribuíram (em ordem de aplicação)
    legislacoes_aplicadas INTEGER[],
    
    -- Audit trail: log de quais leis afetaram cada campo
    -- {ZR1.altura: ['LC 434/1999', 'LC 198/2023'], ...}
    audit_log JSONB,
    
    -- Metadados
    total_zonas INTEGER,
    total_modificacoes INTEGER,
    
    -- Auditoria
    consolidado_em TIMESTAMP DEFAULT NOW(),
    consolidado_por INTEGER REFERENCES users(id),
    
    UNIQUE(municipio, estado)
);

CREATE INDEX IF NOT EXISTS idx_mc_municipio_estado ON municipios_consolidado(municipio, estado);
CREATE INDEX IF NOT EXISTS idx_mc_consolidado_em ON municipios_consolidado(consolidado_em DESC);
CREATE INDEX IF NOT EXISTS idx_mc_zonas_gin ON municipios_consolidado USING GIN (zonas_consolidadas);

COMMENT ON TABLE municipios_consolidado IS 'Estado consolidado de um município — resultado de aplicar todas as leis em ordem cronológica';
COMMENT ON COLUMN municipios_consolidado.audit_log IS 'Trilha de auditoria: quais leis afetaram cada campo de cada zona';

