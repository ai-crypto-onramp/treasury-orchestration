-- +migrate Up
CREATE TABLE IF NOT EXISTS rebalancing_jobs (
    id         BIGSERIAL PRIMARY KEY,
    from_ref   TEXT          NOT NULL,
    to_ref     TEXT          NOT NULL,
    asset      TEXT          NOT NULL,
    amount     NUMERIC(20,8) NOT NULL,
    status     TEXT          NOT NULL DEFAULT 'pending',
    reason     TEXT          NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_rebalancing_jobs_status ON rebalancing_jobs (status);
CREATE INDEX IF NOT EXISTS idx_rebalancing_jobs_asset  ON rebalancing_jobs (asset);

-- +migrate Down
DROP TABLE IF EXISTS rebalancing_jobs;