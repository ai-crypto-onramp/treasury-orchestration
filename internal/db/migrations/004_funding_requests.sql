-- +migrate Up
CREATE TABLE IF NOT EXISTS funding_requests (
    id            BIGSERIAL PRIMARY KEY,
    wallet_id     TEXT          NOT NULL,
    asset         TEXT          NOT NULL,
    amount        NUMERIC(20,8) NOT NULL,
    status        TEXT          NOT NULL DEFAULT 'pending',
    source_venue  TEXT          NOT NULL,
    created_at    TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_funding_requests_status ON funding_requests (status);
CREATE INDEX IF NOT EXISTS idx_funding_requests_asset  ON funding_requests (asset);

-- +migrate Down
DROP TABLE IF EXISTS funding_requests;