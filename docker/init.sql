-- Script de Inicialização da Base de Dados PIX
-- Banco de Dados: bank_db

CREATE TABLE IF NOT EXISTS pix_transactions (
    transaction_id VARCHAR(64) PRIMARY KEY,
    amount NUMERIC(15, 2) NOT NULL,
    pix_key VARCHAR(128) NOT NULL,
    description VARCHAR(255),
    status VARCHAR(30) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    version BIGINT DEFAULT 0 NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pix_tx_created_at ON pix_transactions(created_at);
CREATE INDEX IF NOT EXISTS idx_pix_tx_status ON pix_transactions(status);

CREATE TABLE IF NOT EXISTS outbox (
    id BIGSERIAL PRIMARY KEY,
    aggregate_type VARCHAR(50) NOT NULL,
    aggregate_id VARCHAR(64) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    payload TEXT NOT NULL,
    status VARCHAR(20) DEFAULT 'PENDING' NOT NULL,
    retry_count INT DEFAULT 0 NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_outbox_status_created ON outbox(status, created_at) WHERE status = 'PENDING';
