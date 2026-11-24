# Architecture

SafeStart's data pipeline collects the public U.S. mine and workplace safety record, keeps every version it downloads
(except the cloud server's own copies of withdrawn official files, deleted after 30 days), and serves two kinds of
consumers: analysts (a DuckDB warehouse and its SQL marts) and a live dashboard fed by Kafka.

The diagram of the whole system is in the [Architecture section](DOCUMENTATION.md#architecture) of the documentation.
This page gives the detail behind it: the packages, the shared rules, the settings, the event contract, the streaming
services, the stores, the orchestration, the entry points, the deployment and the constraints.

## Packages (`src/safestart_pipeline/`)

| Package | Role |
|---|---|
| `definitions.py` | The rules every component shares: injury, serious accident, cited section, commodity |
| `crawl/`, `acquire/` | Robots-compliant crawls of the three websites; bulk downloads of MSHA's open data and eCFR, every version kept |
| `lake/` | Typed Parquet of every version of MSHA's 20 datasets (`ssp lake`) |
| `warehouse/` | DuckDB warehouse built from the lake, the eCFR XML and the crawl catalogs; marts, documented queries, exports |
| `stream/` | The event contract (`events.py`), the record-to-event mapping (`records.py`), PostgreSQL schema (`schema.sql`), Kafka clients, replay producer, storage and analytics consumers |
| `watch/` | Live checks of lake versions, sitemaps, signed official manifests and the Federal Register |
| `ml/` | Features from the lake with Polars and a mine accident-risk model (`docs/ML.md`) |
| `dashboard/` | Streamlit app over DuckDB, PostgreSQL, official run records and the local feed |
| `flows/` | Prefect flows that run and schedule the batch, the model and the official files (`python -m safestart_pipeline.flows`) |
| `official/` | The daily update of the official files (`run.py`, `daily.py`, `sources.py`), its record (`state.py`), the signed feed for SafeStart (`feed.py`, docs/OFFICIAL_FILES.md) and the purge of retired originals (`purge.py`) |
| `verify.py` | Completeness reports of the crawls and the bulk downloads (`ssp verify`) |
| `sample.py` | A small synthetic data root the whole stack runs on, for CI and for readers without the data (`python -m safestart_pipeline.sample`) |

## Shared rules (`definitions.py`)

Each rule is stated once, in [`definitions.py`](../src/safestart_pipeline/definitions.py), whose docstring gives the
evidence from the real data. The warehouse, the replay and the version watcher (through `stream/records.py`), the
model and the dashboard use its constants, its Python functions and its DuckDB SQL forms (`serious_sql`, `injury_sql`,
`cfr_section_sql`, `commodity_sql`) instead of restating a rule.

- An **injury** is an accident of degree of injury 01 to 06, the injuries MSHA's injury rates count.
- A **serious accident** is degree 01 to 05: a death, a permanent disability, or days away from work or of restricted
  work. The rule reads the degree, never the day counts, which arrive only once the miner is back at work and also
  appear on illnesses and other cases. A **fatality** is degree 01.
- The **cited section** of a violation is the eCFR section its citation names, without the paragraph:
  `56.14107(a)` is 56.14107, `75.1100-2(b)` is 75.1100-2, `56.5001(a)/.5005` is 56.5001, and `104(d)(1)` names none.
- The **commodity** (coal `C` or metal/non-metal `M`) is the mine's flag in MSHA's mines dataset, and a record's own
  flag only when its mine is missing there, so a mine's violations, accidents and hours worked share one commodity.
- A violation is **S&S** (significant and substantial) when its `sig_sub` flag is `Y` (`SIG_SUB`).

## Settings (`config.py`)

`data_root()`, `postgres_url()`, `kafka_bootstrap()`, `warehouse_path()`, and `setting(name, default)`: the environment
variable `SAFESTART_<NAME>` wins, then `<name>` in `pipeline.local.toml`, then the default. Secrets come only from the
environment. The defaults address the local Compose stack as `127.0.0.1` (PostgreSQL 15432, Kafka 19092): its host
ports listen on IPv4 only, and on Windows `localhost` tries IPv6 first, which stalls every connection.

- `SAFESTART_DATA_ROOT`: defaults to `./data` (or `data_root` in `pipeline.local.toml`); used by every stage.
- `SAFESTART_WAREHOUSE_PATH`: defaults to `<data root>/warehouse/safestart.duckdb`; used by the warehouse, the exports
  and the dashboard.
- `SAFESTART_WAREHOUSE_MEMORY_LIMIT`: defaults to `1GB`; used by the warehouse build's DuckDB.
- `SAFESTART_POSTGRES_URL`: defaults to `postgresql://safestart:safestart@127.0.0.1:15432/safestart`; used by the
  stream services and the dashboard.
- `SAFESTART_KAFKA_BOOTSTRAP`: defaults to `127.0.0.1:19092`; used by the Kafka clients.
- `SAFESTART_KAFKA_PARTITIONS`: defaults to `3`; used for the partitions of a new topic (`kafka_partitions`; the
  initializer's `--partitions` wins).
- `SAFESTART_KAFKA_SECURITY_PROTOCOL`: defaults to `SASL_SSL` once a user name is set; used by the Kafka clients, for a
  managed broker.
- `SAFESTART_KAFKA_SASL_MECHANISM`: defaults to `PLAIN` (or `SCRAM-SHA-256`, `SCRAM-SHA-512`); used by the Kafka
  clients.
- `SAFESTART_KAFKA_SASL_USERNAME`, `SAFESTART_KAFKA_SASL_PASSWORD`: no default; used by the Kafka clients, which read
  them from the environment only.
- `SAFESTART_KAFKA_SSL_CA_LOCATION`: defaults to the system's certificates; used by the Kafka clients.
- `SAFESTART_WATCH_STATE`: defaults to `<data root>/state/watch.sqlite` (`/state/watch.sqlite` in Compose); used by
  the watchers for their state.
- `SAFESTART_WATCH_LOG`: defaults to `<data root>/logs/watch.log`, and `-` means standard error alone (`-` in Compose);
  used by the watchers for their log.
- `PREFECT_API_URL`, `PREFECT_API_KEY`: unset by default, so the orchestrator keeps the schedule itself; used by the
  orchestrator (Prefect's own settings).
- `POLARS_MAX_THREADS`, `OMP_NUM_THREADS`: default to all cores (2 in Compose, as on the e2-medium); used by the batch
  and the model.

## The event contract

`stream/events.py` defines one validated envelope for every event. Topics: `msha.enforcement` (kinds `inspection`,
`violation`, `accident`, `order_107a`) and `safety.sources` (kinds `dataset_version`, `page_change`, `regulation`).
The Kafka message key is the mine ID, so a mine's events stay in order on one partition, or the kind for an event
without a mine. `event_id` is `<kind>:<source key>`, fixed for a given source record, so a replay or a repeated check
is idempotent downstream. `mode` is `replay` (history pushed through the stream in source-time order) or `live` (found
by a watcher). `stream/records.py` maps a lake record to its event for both the replay and the version watcher, so a
record becomes the same event whichever path finds it.

| Kind | Source key | Details (JSON scalars only) |
|---|---|---|
| `inspection` | `event_no` | `activity_code`, `activity`, `begin_date`, `end_date` |
| `violation` | `violation_no` | `event_no`, `cfr_section` (the shared rule, e.g. `75.400`), `part_section` (as cited, e.g. `56.14107(a)`), `sig_sub`, `likelihood`, `injury_illness`, `negligence`, `proposed_penalty` |
| `accident` | `document_no` | `degree_injury_cd`, `degree_injury`, `serious` (the shared rule), `classification`, `accident_type`, `no_injuries`, `days_lost` |
| `order_107a` | the order's `violation_no` | `termination_date` |
| `dataset_version` | `<dataset>:<version>`, the version being the lake file's stem (the first 16 hex digits of the source zip's SHA-256) | `dataset`, `version`, `previous_version`, `rows`, `new_rows` |
| `page_change` (sitemap) | `<url>\|<change>\|<lastmod>` | `site`, `url`, `lastmod`, `change` |
| `page_change` (feed) | `official\|<url>\|<change>\|<content hash>\|<observedAt>` | Feed entry details (below) |
| `regulation` | Federal Register document number | `agency`, `document_number`, `type`, `html_url`, `publication_date` |

Feed details carry the site, URL, change, hashes, type, HTTP status, bytes, redirect target, manifest sequence and
origin. A sitemap event reports an added, changed or dropped listing. Its lastmod is empty when none was supplied.
The change and lastmod distinguish later edits of the same page. A page the crawl never recorded, or first recorded
after the watcher's previous read, is `added` once. This accounts for new sitemap URLs recorded by the daily update.
A later lastmod is `changed`. An event counts as serious when it is a violation with `sig_sub` true or an accident
with `serious` true. A replayed event's `source` holds its dataset, the lake version it was read from and `through`,
the last source day of its replay pass, which the analytics consumer's clock reads.

## Streaming

- **Replay** (`python -m safestart_pipeline.stream.replay`): the current lake version of inspections, violations,
  107(a) orders and accidents, one month at a time in source-time order. `--start` (default 2024-01-01) and `--until`
  bound the source days; `--day-seconds` (default 5) is the wall time of one source day, 0 sending as fast as the
  broker takes them; `--max-rate` caps the events per second; `--loop` starts again from `--start` after the newest
  record, and the unchanged event ids keep the consumers from storing or counting anything twice. Each event carries
  the last source day of its pass (`source.through`), known before the first is sent. Without a lake (a data root the
  orchestrator's first batch has not filled yet) the replay says so once and looks again every 5 minutes, honoring a
  stop, instead of failing and being restarted over and over; Compose restarts it only after a failure.
- **Storage consumer** (group `safestart-storage`): every valid event into `stream.events`, keyed by `event_id`, with
  the topic, partition and offset where it first arrived. Offsets are committed after the database transaction.
- **Analytics consumer** (group `safestart-analytics`): counts per source day, kind and district into
  `stream.daily_counts`, exactly once. An event is added to the counts only by the statement that first writes its
  `event_id` to the ledger `stream.counted_events`, in the same transaction, so a replayed or redelivered event moves
  no count; `daily_counts` is always `counted_events` grouped by day, kind and district.
- **Forecasts** (`stream.forecasts`, method `ewma-weekday(alpha=0.1,weeks=26,k=3.0)`): enforcement follows the work
  week, so each day of a series (kind, district) is forecast one step ahead from the same weekday in the previous 26
  weeks by an exponentially weighted moving average (smoothing 0.1). The residual is observed minus predicted; a day is
  an anomaly when |observed - predicted| exceeds three scales, so unusually low days such as public holidays are
  flagged as well as high ones, the scale being the weighted standard deviation of those weeks but at least that of a
  Poisson count (the square root of the forecast) and at least 1, and only after four weeks of history. A late event
  recomputes the forecasts that depend on its day. Evaluated on the lake's 52 enforcement series (4 kinds by 13
  districts, January 2024 to November 2026): a mean absolute error of 3.65 events per series-day, against 6.3 for the
  best plain day-to-day EWMA (smoothing 1, yesterday's count; 7.1 with smoothing 0.1) and 3.9 to 4.0 for shorter
  memories; 1.5% of the judged series-days are flagged, such as 2024-05-22, when MSHA opened 1,664 Special Emphasis
  Program inspections.
- **The forecasts' clock**: a day is forecast once every partition has moved past it or been read to its end. While
  a replay pass is under way (the topic has not carried its stamped last day yet), a partition stands at its newest
  replayed day, and the watchers' live events, often far ahead of the replay, are counted but never move the clock;
  once the pass has reached its last day, a partition stands at the newer of its replayed and live days, so the days
  the watchers find afterwards are forecast without a restart. An empty store waits for the replay's first event on
  `msha.enforcement`; `safety.sources`, which no replay feeds, follows its live days throughout.
  `scripts/smoke_test.py` dates its synthetic events on the stream's first day, so they never move the clock.
- **Watchers** (`python -m safestart_pipeline.watch once|loop`): new dataset versions every 6 hours (record diffs of
  consecutive lake versions), MSHA's sitemaps every 6 hours and OSHA's once a day (against the crawl inventories, not
  while that site's crawl runs), and hourly checks of the Federal Register and local official feed. A version in which
  too many records look new (its keys or format changed) is refused: an error in that check, nothing announced, and the
  baseline of the next version. Where the watcher can write the data root's state folder, it holds the crawl's lock
  while it reads a site's sitemaps. Otherwise (including containers with a read-only mount), the watcher skips a
  site while that crawl's inventory, its `-wal` or a download in progress (`raw/<site>/objects/tmp`) changed in the
  last 15 minutes. A skipped check is due again an hour later, so the daily update of the official files, which writes
  OSHA's inventory from 16:30 to 04:30 UTC, never keeps OSHA's check from running. Every request obeys robots.txt and
  the host's pacing, redirects and failed requests included
  (`watch/polite.py`). Their state (`SAFESTART_WATCH_STATE`, a small SQLite file) remembers every event published, the
  versions compared (refused ones marked) and when each source was checked; the version check spills DuckDB's
  overflow into a folder beside it. They log to standard error and to `SAFESTART_WATCH_LOG` unless it is `-`. Every
  check is recorded in `stream.source_checks`.
  The feed check verifies each numbered manifest's Ed25519 signature, sequence and link to the previous manifest
  when it is available. It advances its sequence mark after publishing the entries. A sitemap addition and a feed
  discovery of the same URL share one announcement. A sitemap drop and an observed HTTP disappearance remain distinct.

## Stores

- PostgreSQL, schema `stream` (`stream/schema.sql`, every statement idempotent): `events` (one row per event, envelope
  in columns, details and source as JSON, Kafka position, with indexes by kind, by mine, by arrival, and by mode and
  arrival for the Live page), `daily_counts`, `counted_events` (the analytics consumer's ledger), `forecasts`,
  `source_checks`.
- DuckDB warehouse (`warehouse_path()`): built into a new file and swapped in, so readers never see a half-built
  warehouse; its build and its exports hold a lock beside it (`warehouse/lock.py`), so one writer works at a time
  whoever starts it, and a build first removes what a killed build left behind. Core tables `mine`, `inspection`,
  `violation`, `accident`, `production_quarter`, `cfr_section`, `document`, `dataset_version`; marts as views; what
  the build found goes to `build_check`, every repeated key to `duplicate_row` and every row without a key to
  `null_key_row`. The ERD is in `docs/ERD.md`.
  The dashboard opens it read-only, with a memory limit and a spill folder of its own: DuckDB's default spill folder
  sits beside the database file, which the containers mount read-only.

The dashboard reads stored totals and arrival times from `stream.events`, and its 14-day tile trends from
`stream.daily_counts` across both modes. Live compares each viewer's successive totals per mode. It separates replay
and live activity on independent linear scales and shows the last 48 hours of watcher checks, official runs, publish
attempts and the latest warehouse build. Future markers use the shared UTC schedules. Sources puts the run's recorded
progress, feed history and latest changed URLs ahead of the inventories.

## Orchestration

The orchestrator service (`python -m safestart_pipeline.flows serve`) runs the batch flow every day at 14:00 UTC
(bulk downloads, lake, warehouse, exports), the model flow every Sunday at 15:00 UTC and the official flow (the daily
update of the official files, `official/`) every day at 16:30 UTC until 04:30 UTC, one at a time. With
`PREFECT_API_URL` and `PREFECT_API_KEY` set it serves the flows as Prefect deployments (Prefect Cloud shows their runs
and can start one); without them it keeps the same schedule in its own process. `python -m safestart_pipeline.flows
run batch|model|official` (or `ssp pipeline [--model]`, `ssp official run`) runs one now. It is the only service
that writes the data root, so Compose starts it only with its profile (`COMPOSE_PROFILES=orchestrator`,
`scripts/start.py --with-orchestrator`): the server turns it on, a workstation when asked. Served runs execute in
processes of their own; the package's progress lines reach their Prefect logs through
`PREFECT_LOGGING_EXTRA_LOGGERS`.

The batch and model flows and `ssp lake` hold one writer lock on the data root (`state/pipeline.lock`) from start to
end, so a second run started meanwhile, by hand or by the schedule, fails at once instead of writing the same files.
The official flow writes none of those files and does not take that lock: it holds each crawl site's lock
(`state/crawl-<site>.lock`) while it asks that site, and its deadline keeps it clear of the batch's and the model's
starts.
Without a Prefect API, a run that raises before it can start (Prefect's local server not answering, for example) is
tried again 15 minutes later, up to three times, and the schedule goes on; the orchestrator gives that local server
120 seconds to start.

## Entry points

- `ssp lake`: builds the Parquet lake.
- `ssp pipeline [--no-download] [--model]`: runs the batch flow once (downloads, lake, warehouse, exports), then, with
  `--model`, the model flow.
- `ssp official run [--site] [--until] [--budget]`, `ssp official status`: runs the daily update of the official files
  now, without Prefect, then publishes its manifest; shows its latest run, queue, re-check cycle, feed and purge.
- `ssp official manifest`, `ssp official purge [--delete]`, `ssp official keygen --out <file>`: publishes the feed's
  next manifest now; reports, or with `--delete` deletes, the originals of URLs retired more than 30 days ago; writes
  a new signing key pair (docs/OFFICIAL_FILES.md).
- `ssp warehouse build|query [name]|export`, `ssp ml train|analyze`: the warehouse and model commands below.
- `python -m safestart_pipeline.warehouse build`: builds the DuckDB warehouse.
- `python -m safestart_pipeline.stream.init`: creates the Kafka topics and the PostgreSQL schema.
- `python -m safestart_pipeline.stream.replay`: replays enforcement history through Kafka.
- `python -m safestart_pipeline.stream.storage` / `.analytics`: the two consumer groups.
- `python -m safestart_pipeline.watch once|loop`: runs the due live watchers once, or on their schedules.
- `python -m safestart_pipeline.flows serve`: runs the batch, model and official flows on their schedules.
- `python -m safestart_pipeline.ml train`: builds features and trains the accident-risk model.
- `streamlit run src/safestart_pipeline/dashboard/app.py`: the dashboard.
- `python -m safestart_pipeline.sample <folder>`: writes the synthetic sample data root (docs/DEPLOYMENT.md).

## Deployment

`docker-compose.yml` runs every service above on one host from one image, and `docs/DEPLOYMENT.md` explains it. Every
service mounts the data root read-only at `/data` except the orchestrator; the watchers keep their state in a volume
of their own and log to standard error, which Docker keeps and rotates. A read-only mount does not mean that no crawl
runs: on a workstation the crawls run on the host into the folder the stack mounts. The containerized sitemap watcher
cannot take the crawl's lock there, and a file lock would not cross Docker Desktop's file sharing anyway, so it skips
a site whose crawl wrote its inventory or a download in the last 15 minutes (see Watchers).

## Constraints

- The cloud host is an e2-medium (2 shared vCPUs, 4 GB) with a 2 GB swap file: Kafka runs with a small heap,
  PostgreSQL with a small configuration, every service has a memory limit (the budget is in `docker-compose.yml`), and
  batch work streams through files instead of holding whole datasets in memory.
- Every request to a website follows its robots.txt and pacing (reuse `crawl/robots.py`), redirect hops included; a
  refusal is never worked around.
- Tests use small fixtures and temporary directories; they never write under the real data root.
