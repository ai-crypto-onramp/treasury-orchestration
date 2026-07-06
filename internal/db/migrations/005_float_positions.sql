-- +migrate Up
CREATE TABLE IF NOT EXISTS float_positions (
    id                  BIGSERIAL PRIMARY KEY,
    fiat_currency       TEXT          NOT NULL,
    short_fiat_amount   NUMERIC(20,8) NOT NULL DEFAULT 0,
    long_crypto_amount  NUMERIC(20,8) NOT NULL DEFAULT 0,
    settlement_due_at   TIMESTAMPTZ,
    updated_at          TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_float_positions_fiat_currency ON float_positions (fiat_currency);

-- +migrate Down
DROP TABLE IF EXISTS float_positions;