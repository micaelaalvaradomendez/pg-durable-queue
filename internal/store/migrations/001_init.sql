-- Scans survive page reloads: the operator's buffer lives in the database.
CREATE TABLE IF NOT EXISTS scan_buffer (
    id          uuid PRIMARY KEY,
    session_id  text        NOT NULL,
    order_ref   text        NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    deleted_at  timestamptz
);

CREATE UNIQUE INDEX IF NOT EXISTS scan_buffer_active_uq
    ON scan_buffer (session_id, order_ref) WHERE deleted_at IS NULL;

CREATE TABLE IF NOT EXISTS batch (
    id               uuid PRIMARY KEY,
    idempotency_key  text        NOT NULL UNIQUE,
    session_id       text        NOT NULL,
    target_warehouse text        NOT NULL,
    note             text,
    created_by       text        NOT NULL,
    created_at       timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS batch_item (
    id              uuid PRIMARY KEY,
    batch_id        uuid        NOT NULL REFERENCES batch (id),
    order_ref       text        NOT NULL,
    status          text        NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'in_progress', 'retry', 'applied', 'dlq')),
    attempts        integer     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    lease_until     timestamptz,
    last_error      text,
    updated_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (batch_id, order_ref),
    CHECK ((status = 'in_progress') = (lease_until IS NOT NULL))
);

-- Partial indexes: workers only ever scan claimable or leased rows.
CREATE INDEX IF NOT EXISTS batch_item_claimable_idx
    ON batch_item (next_attempt_at) WHERE status IN ('pending', 'retry');
CREATE INDEX IF NOT EXISTS batch_item_lease_idx
    ON batch_item (lease_until) WHERE status = 'in_progress';
CREATE INDEX IF NOT EXISTS batch_item_batch_status_idx
    ON batch_item (batch_id, status);

CREATE TABLE IF NOT EXISTS item_attempt (
    id         uuid PRIMARY KEY,
    item_id    uuid        NOT NULL REFERENCES batch_item (id),
    attempt_no integer     NOT NULL,
    worker_id  text        NOT NULL,
    outcome    text        NOT NULL
               CHECK (outcome IN ('applied', 'already_applied', 'transient', 'permanent')),
    detail     text,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS item_attempt_item_idx ON item_attempt (item_id);

-- Domain events written in the same transaction as the state change they describe.
CREATE TABLE IF NOT EXISTS outbox (
    id           uuid PRIMARY KEY,
    aggregate_id uuid        NOT NULL,
    event_type   text        NOT NULL,
    actor        text        NOT NULL,
    payload      jsonb       NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);

CREATE OR REPLACE FUNCTION forbid_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is append-only', TG_TABLE_NAME
        USING ERRCODE = 'insufficient_privilege';
END;
$$;

DROP TRIGGER IF EXISTS item_attempt_append_only ON item_attempt;
CREATE TRIGGER item_attempt_append_only
    BEFORE UPDATE OR DELETE ON item_attempt
    FOR EACH ROW EXECUTE FUNCTION forbid_mutation();
