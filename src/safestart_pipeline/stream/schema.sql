-- The streaming store. The envelope of every event has its own columns; only the kind's own values stay as JSON.
CREATE SCHEMA IF NOT EXISTS stream;

CREATE TABLE IF NOT EXISTS stream.events (
    event_id text PRIMARY KEY,
    schema_version smallint NOT NULL CHECK (schema_version = 1),
    kind text NOT NULL CHECK (kind IN ('inspection', 'violation', 'accident', 'order_107a',
                                       'dataset_version', 'page_change', 'regulation')),
    mode text NOT NULL CHECK (mode IN ('replay', 'live')),
    occurred_at timestamptz NOT NULL,
    emitted_at timestamptz NOT NULL,
    mine_id text,
    district text,
    state text,
    coal_metal text CHECK (coal_metal IN ('C', 'M')),
    title text NOT NULL,
    details jsonb NOT NULL,
    source jsonb NOT NULL,
    kafka_topic text NOT NULL,
    kafka_partition integer NOT NULL,
    kafka_offset bigint NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS events_kind_time ON stream.events (kind, occurred_at DESC);
CREATE INDEX IF NOT EXISTS events_mine_time ON stream.events (mine_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS events_received ON stream.events (received_at DESC);

-- Events per source day, kind and district, kept by the analytics consumer ('' when an event has no district).
-- "serious" counts significant-and-substantial violations and fatal or lost-time accidents.
CREATE TABLE IF NOT EXISTS stream.daily_counts (
    day date NOT NULL,
    kind text NOT NULL,
    district text NOT NULL DEFAULT '',
    events integer NOT NULL CHECK (events >= 0),
    serious integer NOT NULL DEFAULT 0 CHECK (serious >= 0),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (day, kind, district)
);

-- One-step-ahead forecast of each complete day's count, with the residual and an anomaly flag.
CREATE TABLE IF NOT EXISTS stream.forecasts (
    day date NOT NULL,
    kind text NOT NULL,
    district text NOT NULL DEFAULT '',
    observed double precision NOT NULL,
    predicted double precision NOT NULL,
    residual double precision NOT NULL,
    anomaly boolean NOT NULL,
    method text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (day, kind, district)
);

-- Every live check of a source by a watcher: when, the outcome, how many new events it found.
CREATE TABLE IF NOT EXISTS stream.source_checks (
    source text NOT NULL,
    checked_at timestamptz NOT NULL,
    status text NOT NULL CHECK (status IN ('ok', 'unchanged', 'error', 'skipped')),
    found integer NOT NULL DEFAULT 0 CHECK (found >= 0),
    note text NOT NULL DEFAULT '',
    PRIMARY KEY (source, checked_at)
);
