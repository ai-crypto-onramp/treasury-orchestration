-- +migrate Up
CREATE TABLE IF NOT EXISTS batch_memberships (
    id            BIGSERIAL PRIMARY KEY,
    batch_id      BIGINT       NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
    tx_id         TEXT         NOT NULL UNIQUE,
    amount        NUMERIC(20,8) NOT NULL,
    asset         TEXT         NOT NULL,
    fiat_currency TEXT         NOT NULL,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_memberships_batch_id ON batch_memberships (batch_id);
CREATE INDEX IF NOT EXISTS idx_memberships_asset_pair ON batch_memberships (asset, fiat_currency);

-- +migrate Down
DROP TABLE IF EXISTS batch_memberships;