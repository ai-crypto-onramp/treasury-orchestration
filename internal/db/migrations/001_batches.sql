-- +migrate Up
CREATE TABLE IF NOT EXISTS batches (
    id            BIGSERIAL PRIMARY KEY,
    asset_pair    TEXT        NOT NULL,
    status        TEXT        NOT NULL DEFAULT 'open',
    notional_usd  NUMERIC(20,8) NOT NULL DEFAULT 0,
    opened_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    closed_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_batches_asset_pair ON batches (asset_pair);
CREATE INDEX IF NOT EXISTS idx_batches_status     ON batches (status);

-- +migrate Down
DROP TABLE IF EXISTS batches;