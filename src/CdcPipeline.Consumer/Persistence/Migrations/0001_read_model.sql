-- Read model for the shop entities, fed by the CDC consumer.
--
-- Every projected table carries the same bookkeeping columns:
--   source              which capture pipeline the row came from ('pg' or 'ora'); part of the primary key, so the
--                       PostgreSQL and Oracle shops can share one read model without colliding ids
--   pos_major/pos_minor SourcePosition of the last change applied; a change is applied only if its position is
--                       strictly greater, which makes broker redelivery and connector replays harmless
--   deleted             deletes are kept as tombstones so a late redelivery of an older change cannot resurrect a row
--   extra               source fields the contract does not (yet) know, e.g. a freshly added column
--   tx_id               source transaction of the last applied change; the indexer waits for that transaction to be
--                       complete so the search index never shows half a transaction
--   apply_count         how many changes have actually been applied (tests compare it with the source row version)
--   applied_at          clock of the read-model database when the change landed (lag measurement)
--   index_dirty         the search index does not reflect this row's current position yet

CREATE TABLE rm_customer (
    source        text           NOT NULL,
    id            bigint         NOT NULL,
    full_name     text,
    email         text,
    city          text,
    loyalty_tier  text,
    version       integer,
    updated_at    timestamptz,
    extra         jsonb          NOT NULL DEFAULT '{}',
    deleted       boolean        NOT NULL DEFAULT false,
    pos_major     numeric(20,0)  NOT NULL,
    pos_minor     numeric(20,0)  NOT NULL,
    tx_id         text,
    apply_count   integer        NOT NULL DEFAULT 1,
    applied_at    timestamptz    NOT NULL DEFAULT clock_timestamp(),
    index_dirty   boolean        NOT NULL DEFAULT true,
    indexed_at    timestamptz,
    PRIMARY KEY (source, id)
);

CREATE TABLE rm_order (
    source        text           NOT NULL,
    id            bigint         NOT NULL,
    customer_id   bigint,
    status        text,
    total_amount  numeric(14,2),
    version       integer,
    updated_at    timestamptz,
    extra         jsonb          NOT NULL DEFAULT '{}',
    deleted       boolean        NOT NULL DEFAULT false,
    pos_major     numeric(20,0)  NOT NULL,
    pos_minor     numeric(20,0)  NOT NULL,
    tx_id         text,
    apply_count   integer        NOT NULL DEFAULT 1,
    applied_at    timestamptz    NOT NULL DEFAULT clock_timestamp(),
    index_dirty   boolean        NOT NULL DEFAULT true,
    indexed_at    timestamptz,
    PRIMARY KEY (source, id)
);

CREATE TABLE rm_order_line (
    source        text           NOT NULL,
    order_id      bigint         NOT NULL,
    line_no       integer        NOT NULL,
    sku           text,
    -- numeric from the start: contract v2 widened quantity from integer to a decimal (sold by weight).
    quantity      numeric(14,3),
    unit_price    numeric(14,2),
    version       integer,
    updated_at    timestamptz,
    extra         jsonb          NOT NULL DEFAULT '{}',
    deleted       boolean        NOT NULL DEFAULT false,
    pos_major     numeric(20,0)  NOT NULL,
    pos_minor     numeric(20,0)  NOT NULL,
    tx_id         text,
    apply_count   integer        NOT NULL DEFAULT 1,
    applied_at    timestamptz    NOT NULL DEFAULT clock_timestamp(),
    index_dirty   boolean        NOT NULL DEFAULT true,
    indexed_at    timestamptz,
    PRIMARY KEY (source, order_id, line_no)
);

CREATE INDEX rm_customer_dirty   ON rm_customer   (applied_at) WHERE index_dirty;
CREATE INDEX rm_order_dirty      ON rm_order      (applied_at) WHERE index_dirty;
CREATE INDEX rm_order_line_dirty ON rm_order_line (applied_at) WHERE index_dirty;

-- Transaction completeness. applied_events counts distinct data events (deduplicated through rm_tx_event);
-- expected_events comes from the connector's END marker. Markers and data events travel on different topics and
-- partitions, so they may arrive in any order; completion is decided whenever either side moves.
CREATE TABLE rm_tx (
    source           text         NOT NULL,
    tx_id            text         NOT NULL,
    expected_events  integer,
    applied_events   integer      NOT NULL DEFAULT 0,
    first_seen_at    timestamptz  NOT NULL DEFAULT clock_timestamp(),
    completed_at     timestamptz,
    PRIMARY KEY (source, tx_id)
);
CREATE INDEX rm_tx_open ON rm_tx (first_seen_at) WHERE completed_at IS NULL;

CREATE TABLE rm_tx_event (
    source       text    NOT NULL,
    tx_id        text    NOT NULL,
    total_order  bigint  NOT NULL,
    PRIMARY KEY (source, tx_id, total_order)
);

-- Durable counters, updated in the same transaction as the rows they describe.
CREATE TABLE rm_counter (
    name   text    PRIMARY KEY,
    value  bigint  NOT NULL DEFAULT 0
);
INSERT INTO rm_counter (name) VALUES ('events_applied'), ('events_skipped'), ('batches');
