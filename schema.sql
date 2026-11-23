CREATE TABLE IF NOT EXISTS customer_history (
    customer_id VARCHAR NOT NULL,
    version BIGINT NOT NULL,
    segment VARCHAR NOT NULL,
    region VARCHAR NOT NULL,
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP,
    is_deleted BOOLEAN NOT NULL,
    PRIMARY KEY (customer_id, version)
);
CREATE TABLE IF NOT EXISTS customer_events (
    event_id VARCHAR PRIMARY KEY,
    payload VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id VARCHAR PRIMARY KEY,
    customer_id VARCHAR NOT NULL,
    occurred_at TIMESTAMP NOT NULL,
    amount_cents BIGINT NOT NULL CHECK (amount_cents > 0),
    payload VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS exceptions (
    batch_id VARCHAR NOT NULL,
    source VARCHAR NOT NULL,
    row_number INTEGER NOT NULL,
    reason VARCHAR NOT NULL,
    payload VARCHAR NOT NULL,
    PRIMARY KEY (batch_id, source, row_number)
);
CREATE TABLE IF NOT EXISTS outcomes (
    batch_id VARCHAR NOT NULL,
    source VARCHAR NOT NULL,
    row_number INTEGER NOT NULL,
    decision VARCHAR NOT NULL,
    PRIMARY KEY (batch_id, source, row_number)
);
CREATE TABLE IF NOT EXISTS batches (
    batch_id VARCHAR PRIMARY KEY,
    content_hash VARCHAR NOT NULL,
    controls VARCHAR NOT NULL
);
