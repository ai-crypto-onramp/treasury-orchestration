-- +migrate Up
CREATE TABLE IF NOT EXISTS aggregate_orders (
    id           BIGSERIAL PRIMARY KEY,
    batch_id     BIGINT        NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
    venue_routes JSONB         NOT NULL DEFAULT '[]'::jsonb,
    fill_price   NUMERIC(20,8) NOT NULL DEFAULT 0,
    total_filled NUMERIC(20,8) NOT NULL DEFAULT 0,
    status       TEXT          NOT NULL DEFAULT 'executing',
    created_at   TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_aggregate_orders_batch_id ON aggregate_orders (batch_id);
CREATE INDEX IF NOT EXISTS idx_aggregate_orders_status   ON aggregate_orders (status);

-- +migrate Down
DROP TABLE IF EXISTS aggregate_orders;