# Deployment

The pipeline runs in Docker Compose: Kafka and PostgreSQL, the stream initializer, the replay producer, the storage and
analytics consumer groups, the live watchers, the dashboard and the orchestrator of the batch flows. The same files
start it on a workstation and on a server; on a server an HTTPS gateway is added in front of the dashboard and the
signed feed of the official files. The crawls that build an inventory stay on the workstation (they run for days under
each site's pacing); the daily update of the official files ([OFFICIAL_FILES.md](OFFICIAL_FILES.md)) and everything
else runs in the stack. Every service mounts the data root at `/data`, read-only, except the orchestrator, which runs
the flows that write it.

## What runs

| Service | Image | Does | Memory limit | Port |
|---|---|---|---|---|
| `kafka` | `apache/kafka:4.1.2` | Broker and KRaft controller, heap 256-384 MiB | 768 MiB | 127.0.0.1:19092 |
| `postgres` | `postgres:16-alpine` | The `stream` schema, with settings for a small host | 384 MiB | 127.0.0.1:15432 |
| `init` | pipeline | Creates the Kafka topics and the PostgreSQL schema, then exits | 192 MiB | |
| `replay` | pipeline | Replays the enforcement history from the lake through Kafka | 384 MiB | |
| `storage-consumer` | pipeline | Stores every event in `stream.events` | 192 MiB | |
| `analytics-consumer` | pipeline | Daily counts and forecasts | 256 MiB | |
| `watcher` | pipeline | Live watchers; their state lives in the `watch-state` volume | 512 MiB | |
| `dashboard` | pipeline | Streamlit, health at `/_stcore/health` | 448 MiB | 127.0.0.1:8501 |
| `orchestrator` | pipeline | The daily batch (downloads, lake, warehouse, exports), the weekly model and the daily update of the official files with its signed feed, through Prefect; only with the `orchestrator` profile | 1,920 MiB | |
| `gateway` (server only) | `caddy:2.11-alpine` | HTTPS for a domain name or an IP address: the dashboard, and the feed at `/feed/official/` | 96 MiB | 80, 443 |

- One image (`Dockerfile`) runs every pipeline service. It is built from `uv.lock` with all extras; the dependencies
  sit in their own layer, so a code change rebuilds in seconds. The services run as the unprivileged user `safestart`
  (uid 10001) without Linux capabilities and cannot change their own code.
- Memory is budgeted for an e2-medium (4 GiB) with a 2 GiB swap file, leaving 1 GiB to the system and Docker
  (`tests/test_deployment_files.py` holds the files to it). The services that keep running may use 2.6 GiB together,
  the gateway included, so they never need swap. The orchestrator's limit covers its largest run, the weekly model
  training, which peaked at up to 1,380 MiB in this image with two threads, beside Prefect and its local server
  (0.4 GiB); with it and the replay at their limits as well, the host may swap but never runs out of memory. Measured
  in the image on the real lake: the warehouse build peaks at 1.0 GiB, the watcher's comparison of two versions of the
  3.1 million violations at 343 MiB (DuckDB held to 256 MB), and the dashboard at 300 MiB rendering every page from the
  real warehouse. At rest, Kafka, PostgreSQL, both consumers, the dashboard and the orchestrator used about 600 MiB
  together, 370 MiB of it Kafka's.
- The services start in order: Kafka and PostgreSQL until healthy, then `init`, then the stream services. `init` runs
  once per start and the replay is restarted only after a failure; every other service restarts unless stopped. The
  orchestrator runs only with its Compose profile (`COMPOSE_PROFILES=orchestrator`), since it writes the data root;
  it needs neither Kafka nor PostgreSQL and starts at once. Container logs rotate at 10 MB, three files per service.
- Kafka's log, the stream store and the watcher state are named volumes (`kafka-data`, `postgres-data`,
  `watch-state`); they survive `docker compose down` and are deleted only with `--volumes`.

### Where each service writes

| Service | Writes | Where |
|---|---|---|
| `orchestrator` | downloads, bulk store, lake, warehouse, exports, model; the official files' crawl stores and new originals (`state/crawl-<site>.sqlite`, `raw/<site>/objects/`), their record (`state/official.sqlite`) and the feed (`exports/official/`) | the data root, mounted read-write; Prefect's settings in its home folder |
| `watcher` | its state (`SAFESTART_WATCH_STATE=/state/watch.sqlite`) and the version check's DuckDB spill folder | the `watch-state` volume |
| `watcher` | its log (`SAFESTART_WATCH_LOG=-`) | standard error, kept by Docker |
| `dashboard` | DuckDB's spill folder, should a query outgrow its 192 MiB | `/tmp` in the container |
| `replay` | DuckDB's spill folder | `/tmp` in the container |
| `kafka`, `postgres` | the log, the stream store | their volumes |

The read-only services read the data root's SQLite stores (the bulk store, the crawl inventories) even when they
cannot create those databases' `-wal` and `-shm` files beside them: a database no process has open is then read as
immutable (`verify.read_only`). The gateway mounts only `exports/official/` of the data root, read-only, to serve the
feed.

## Start on a workstation

Install Docker Desktop (Windows, macOS) or Docker Engine with the Compose plugin (Linux), then, from the repository:

```bash
python scripts/start.py --data-root <folder with the data root>
```

`--data-root` names the folder that holds the data root (`raw/`, `state/`, `lake/`, `warehouse/`, `exports/`); without
it the empty `./data` is used: the replay then waits for a lake, which only the orchestrator's batch writes, and the
dashboard has no warehouse to show ([the sample data root](#without-the-data-the-sample-data-root) fills both in a
minute). The first run writes `.env` with a random PostgreSQL password, builds the image, starts every service but the
orchestrator and waits until the dashboard answers. The orchestrator writes the data root, so a workstation runs it only
when asked: `--with-orchestrator` turns it on and `--without-orchestrator` off again, and the choice is kept in `.env`.
Later runs reuse `.env`: PostgreSQL keeps the password its database was created with, so the launcher never replaces
it, and it refuses to invent a new one while the database of a lost `.env` still exists. The launcher needs only the
standard library of Python 3. On Linux the orchestrator writes the data root as uid 10001; with the orchestrator on,
the launcher says when the folder belongs to someone else (`sudo chown -R 10001:10001 <folder>` hands it over).

| `.env` setting | Meaning |
|---|---|
| `POSTGRES_PASSWORD` | Password of the `safestart` database user (random, written once) |
| `DATA_ROOT` | Host folder mounted at `/data`: read-only, but read-write in the orchestrator |
| `PUBLIC_DOMAIN` | Domain name or public IPv4 address for the HTTPS gateway; empty on a workstation |
| `COMPOSE_PROFILES` | `orchestrator` to run the orchestrator (`--with-orchestrator`); empty, the default, leaves it off |
| `DASHBOARD_HOST_PORT`, `KAFKA_HOST_PORT`, `POSTGRES_HOST_PORT` | Host ports (8501, 19092, 15432), on 127.0.0.1 only |
| `PREFECT_API_URL`, `PREFECT_API_KEY` | Optional, commented out by default: a Prefect API for the orchestrator (below) |
| `OFFICIAL_FEED_SIGNING_KEY` | On the server only, commented out by default: the key that signs the official-files feed (below); without it the daily update publishes nothing |

Then:

- Dashboard: `http://localhost:8501` on the machine running the stack.
- Kafka for local tools: `127.0.0.1:19092`; PostgreSQL: `postgresql://safestart:<password>@127.0.0.1:15432/safestart`.
  Use `127.0.0.1` rather than `localhost`: the host ports are open on IPv4 only, and on Windows `localhost` tries IPv6
  first, which costs two seconds per connection. The settings' defaults (`config.py`) do the same.
- End-to-end check: `uv run --extra stream python scripts/smoke_test.py` sends four synthetic events of a district
  named after the run through Kafka, waits until the storage consumer has stored each one and the analytics consumer
  has counted each one exactly once, checks the dashboard, and removes its rows again, the counted-event ledger
  included. The events are dated on the stream's first source day (today on an empty store), so they never move the
  analytics consumer's clock.
- State and logs: `docker compose ps`, `docker compose logs -f replay`, `docker compose logs -f watcher`.
- One writer of the data root at a time. A batch or model run, whoever starts it, and `ssp lake` hold the data root's
  writer lock (`state/pipeline.lock`) from start to end, so a second one started meanwhile, by hand or by the
  schedule, fails at once instead of writing the same files. The official flow writes none of those files and does not
  take that lock: it holds each crawl site's lock (`state/crawl-<site>.lock`) while it asks that site. A lock keeps
  apart the processes of one host or one container, but file locks do not reach across Docker Desktop's file sharing:
  on a workstation that runs the orchestrator and also runs the batch or the model by hand on the host (`ssp pipeline`,
  `python -m safestart_pipeline.flows run`) on the same folder, stop the orchestrator first (`docker compose stop
  orchestrator`).
  For the same reason the containerized sitemap watcher cannot see a host crawl's lock; it leaves a site alone while
  that site's crawl, or the daily update of the official files, has written its inventory or a download in the last 15
  minutes, and asks again an hour later.
- Stop with `docker compose stop` or `docker compose down`; `docker compose down --volumes` also deletes Kafka's log,
  the stream store and the watcher state (the next start creates them again with the password in `.env`).

### Without the data: the sample data root

The part of the real data root the stack reads is 4.5 GB, gathered over days of polite crawling and downloading. To see the whole system without
it, write the sample data root, a few hundred kilobytes of invented records in MSHA's own formats
(`src/safestart_pipeline/sample.py`), and start the stack on it:

```bash
uv sync --all-extras
uv run python -m safestart_pipeline.sample sample-data   # a new or empty folder; the lake is built from its zips
python scripts/start.py --data-root sample-data
docker compose --profile orchestrator run --rm orchestrator python -m safestart_pipeline.flows run batch --no-download
docker compose run --rm replay python -m safestart_pipeline.stream.replay --day-seconds 0
uv run --extra stream python scripts/smoke_test.py
```

It holds five mines, two weekly versions of the inspections, violations, 107(a) orders, accidents and mines, five
quarters of hours worked, the eCFR sections the violations cite and the legacy host's crawl catalog. The batch, run
once without downloads (on Linux, first hand the folder to the image's user: `sudo chown -R 10001:10001 sample-data`),
finds the lake current and builds the warehouse and the SafeStart exports; the dashboard then shows them. The replay
service sends the history at the stack's pace, 5 seconds per source day from 2024 (about 90 minutes for the sample),
so the command above sends it at once (the consumers keep each event once, whoever sends it); the watcher announces
the second version's new week as live events within a minute of the start. The watcher still asks the Federal
Register for its newest documents; the MSHA and OSHA crawl catalogs are left out, so it never compares their live
sitemaps with a tiny inventory. CI runs the same steps and checks every count (`.github/workflows/ci.yml`).

### The orchestrator and Prefect

`python scripts/start.py --with-orchestrator` turns the orchestrator on (the server's startup script does). Without a
Prefect API it keeps the schedule in its own process (batch every day at 14:00 UTC, model every Sunday at 15:00 UTC,
the daily update of the official files every day at 16:30 UTC, stopping by 04:30 UTC) and logs each run:
`docker compose logs -f orchestrator`. With Prefect Cloud, put the workspace's
API URL and an API key in `.env` and recreate the service:

```bash
PREFECT_API_URL=https://api.prefect.cloud/api/accounts/<account>/workspaces/<workspace>
PREFECT_API_KEY=<API key>
```

```bash
docker compose up -d orchestrator
```

It then serves the batch, model and official flows as deployments of that workspace, on the same schedule, and a run
can also be started from Prefect Cloud. Without Prefect Cloud, leave both lines commented out rather than empty: with an
empty URL Prefect logs errors on every run. Without a Prefect API, the first run of each process starts Prefect's
local API server, which may take 20 seconds on shared vCPUs, so the orchestrator gives it 120
(`PREFECT_SERVER_EPHEMERAL_STARTUP_TIMEOUT_SECONDS`); a scheduled run that still raises before it starts is logged with
its traceback and tried again 15 minutes later, up to three times, and the scheduler keeps going. Polars and the
model's libraries are held to two threads (`POLARS_MAX_THREADS`, `OMP_NUM_THREADS`), the e2-medium's two vCPUs. To run
the batch now: `docker compose exec orchestrator python -m safestart_pipeline.flows run batch`; it fails at once if a
scheduled run holds the data root's writer lock, and a scheduled run that finds a manual one under way fails likewise
and waits for its next time.

## Serve it over HTTPS

On a Linux server with Docker, Python 3 and ports 80 and 443 open:

```bash
python3 scripts/start.py --public-domain pipeline.example.org --data-root /srv/safestart-data
python3 scripts/start.py --public-domain 203.0.113.10 --data-root /srv/safestart-data   # an IP address works too
```

`--public-domain` adds `docker-compose.public.yml`: Caddy listens on ports 80 and 443 and obtains a certificate from
Let's Encrypt for the domain (point its DNS A record at the server first) or for the IP address itself. Both use Let's
Encrypt's short-lived profile, six-day certificates that Caddy renews on its own; it is the only profile that
certifies an IP address (`deploy/Caddyfile`). Kafka, PostgreSQL and the dashboard stay on 127.0.0.1. The setting is
kept in `.env`; clear `PUBLIC_DOMAIN` there to go back to a local start.

## Google Cloud

The server is a Compute Engine e2-medium (2 shared vCPUs, 4 GB) with a 50 GB balanced disk, a 2 GB swap file on it and
a reserved IPv4 address. Its network admits only HTTP and HTTPS; there is no SSH from the internet. The data root
travels through a Cloud Storage bucket: the workstation uploads it, and the server copies from it what it needs and
lacks: `raw/bulk/`, `state/`, `lake/`, `warehouse/` and `exports/` (about 5 GB), never the crawls' 43 GB raw archive.
From then on the orchestrator keeps the server's copy current, the OSHA and legacy-host inventories included, which its
daily update of the official files writes. The server's service account holds no project role and may only read that
bucket.

Run steps 1 to 3 and 5 to 6 in Cloud Shell (the terminal icon in the Google Cloud console); step 4 runs on the
workstation that holds the data. `<project>`, `<bucket>`, `<billing account>` and `<address>` stand for your own.

1. Project. Choose a project ID (lowercase, globally unique) and a billing account:

   ```bash
   gcloud projects create <project> --name="SafeStart pipeline"
   gcloud config set project <project>
   gcloud billing accounts list
   gcloud billing projects link <project> --billing-account=<billing account>
   ```

2. Budget alerts. A budget sends e-mail to the billing administrators at 50, 90 and 100 percent of the amount; it does
   not stop spending.

   ```bash
   gcloud services enable billingbudgets.googleapis.com
   NUMBER=$(gcloud projects describe <project> --format='value(projectNumber)')
   gcloud billing budgets create --billing-account=<billing account> --display-name="SafeStart pipeline" \
     --budget-amount=50USD --filter-projects="projects/$NUMBER" \
     --threshold-rule=percent=0.5 --threshold-rule=percent=0.9 --threshold-rule=percent=1.0
   ```

3. Bucket, in the server's region (copies inside a region are free), private:

   ```bash
   gcloud storage buckets create gs://<bucket> --location=us-central1 --uniform-bucket-level-access \
     --public-access-prevention
   ```

4. Upload, on the Windows workstation, with the Google Cloud CLI installed and signed in (`gcloud auth login`,
   `gcloud config set project <project>`). Upload `state/` while no crawl, download or lake build is writing to it, so
   that each database is copied whole (`scripts/stop-crawl.ps1` stops a crawl; the next start resumes it). Look at the
   dry run first:

   ```powershell
   deploy/gcp/upload-to-gcs.ps1 -Bucket <bucket> -DataRoot <folder with the data root> -DryRun
   deploy/gcp/upload-to-gcs.ps1 -Bucket <bucket> -DataRoot <folder with the data root>
   deploy/gcp/upload-to-gcs.ps1 -Bucket <bucket> -DataRoot <folder with the data root> -Only raw   # optional
   ```

   `-DataRoot` names the folder that holds the data root; without it the script takes the environment variable
   `SAFESTART_DATA_ROOT`, and with neither it prints its usage and stops. It also stops with an error when that folder
   does not exist or holds none of the folders asked for, so a run that finds nothing to copy never ends as if it had
   worked. By default the script copies `raw/bulk/`, `state/`, `lake/`, `warehouse/` and `exports/`, each with
   `gcloud storage rsync`; `-Only` picks folders, and `raw` names the whole raw archive, crawls included, which the
   server never reads. A rerun skips what already arrived and resumes an interrupted large file; nothing in the bucket
   is ever deleted. Left out: partial downloads of a running crawl, the work files a lake build in progress or killed
   leaves (`*.partial`, `*.tmp`, and the zip member it transcodes to `.<16 hex digits>.txt`, up to 1.5 GB), and the
   journal files of a database in use (SQLite's `-wal`, `-shm` and `-journal`; the warehouse's `.wal`, a build in
   progress and its staging folder), which must never meet another copy of their database, and the official-files feed
   and its record (`exports/official/`, `state/official.sqlite`), which belong to the server. Where PowerShell refuses
   to run scripts, call it as `powershell -ExecutionPolicy Bypass -File deploy/gcp/upload-to-gcs.ps1 -Bucket <bucket>
   -DataRoot <folder with the data root>`; from Linux, macOS or Git Bash,
   `deploy/gcp/upload-to-gcs.sh <bucket> --data-root <folder with the data root>` does the same.

5. Server:

   ```bash
   git clone https://github.com/USERZEROA/safestart-data-pipeline.git
   cd safestart-data-pipeline
   BUCKET=<bucket> bash deploy/gcp/create-vm.sh
   ```

   `create-vm.sh` enables the APIs, creates the network and its firewall rule (TCP 80 and 443 only), reserves the
   address, creates the service account and its read access to the bucket, and creates the VM with
   `deploy/gcp/startup.sh` as its startup script. Each step reuses what an earlier run made. `DOMAIN=<name>` serves a
   domain instead of the address; `REGION`, `ZONE`, `NAME`, `REPO` and `BRANCH` override the defaults.

6. Verify. The first boot adds the swap file, installs Docker, copies the data, builds the image and starts the stack,
   the orchestrator included, in about ten minutes:

   ```bash
   gcloud compute instances get-serial-port-output safestart-pipeline --zone=us-central1-a | grep safestart:
   curl -fsS https://<address>/_stcore/health        # prints ok
   ```

   Then open `https://<address>` in a browser; the stream's figures grow while the replay runs.

At every boot `startup.sh` copies from the bucket only the files the server lacks (`gcloud storage rsync
--no-clobber`) and deletes nothing: the orchestrator keeps the server's downloads, lake, warehouse, exports and crawl
inventories current, so an older copy in the bucket never replaces them. It never copies the official-files feed or
its record (`exports/official/`, `state/official.sqlite`), which only the server writes, nor the crawls' `raw/<site>/`
folders, so an original the daily update purged is not brought back. A crawl inventory is replaced from the bucket
through `state-replace/` (below); any other file of `state/` whose server copy was deleted comes back from the bucket
at the next boot. The script then makes `exports/official/` and gives the data root to uid 10001, the image's user,
for the orchestrator to write.

Operating the server:

- Update after a push to `main` by stopping and starting the server (or `sudo reboot` in a maintenance shell):

  ```bash
  gcloud compute instances stop safestart-pipeline --zone=us-central1-a
  gcloud compute instances start safestart-pipeline --zone=us-central1-a
  ```

  A stop shuts the guest down cleanly, so Docker stops every container within its grace period and Kafka writes out
  the events it has acknowledged. A `gcloud compute instances reset` skips the shutdown, and live events the watcher
  has already recorded as sent could be lost from Kafka. Every boot brings the checkout up to the branch, copies what
  the server lacks from the bucket and rebuilds what changed; `.env` stays.
- A shell for maintenance (to add the Prefect settings to `.env`, for example) needs a temporary firewall rule for
  Google's IAP range, removed afterwards:

  ```bash
  gcloud compute firewall-rules create safestart-pipeline-iap-ssh --network=safestart-pipeline-net \
    --allow=tcp:22 --source-ranges=35.235.240.0/20 --target-tags=safestart-pipeline-web
  gcloud compute ssh safestart-pipeline --zone=us-central1-a --tunnel-through-iap
  # on the server: cd /opt/safestart-data-pipeline && sudo docker compose ps
  #   sudo nano .env && sudo docker compose up -d orchestrator
  #   sudo docker compose exec analytics-consumer python /app/scripts/smoke_test.py
  gcloud compute firewall-rules delete safestart-pipeline-iap-ssh
  ```

- Pause: `gcloud compute instances stop safestart-pipeline --zone=us-central1-a` (the disk and the address are still
  billed). Remove everything: delete the instance, the address `safestart-pipeline-ip`, the firewall rule, the network
  and, when the data is no longer needed, the bucket.

### Placing a crawl inventory on the server

Once its daily update runs, the server owns its OSHA and legacy-host inventories: it records every change in them. To
put a newer inventory in their place without SSH (the first time: the workstation's OSHA inventory, once its content
hashes are recorded), upload it to the bucket's `state-replace/` and restart the server. Until then the daily update
leaves a site whose inventory still holds copies without a content hash alone: it records the site as `skipped` ("not
backfilled", with how many such copies there are), since the server holds none of the crawls' originals to compare an
answer with, and every page that came back in other bytes would otherwise be reported as changed.

1. On the workstation, finish the inventory and close it: `uv run ssp official backfill-hashes osha` (resumable; the
   originals are only read) until `uv run ssp status osha` shows `"copies_without_content_hash": 0`, with no crawl of
   the site running (`scripts/stop-crawl.ps1`), so that the database is whole in its one file.
2. Upload it:

   ```powershell
   gcloud storage cp <folder with the data root>/state/crawl-osha.sqlite gs://<bucket>/state-replace/crawl-osha.sqlite
   ```

3. Stop and start the server, as for an update. At boot, `deploy/gcp/replace-state.sh` downloads the upload beside the
   server's copy and checks it (SQLite's `quick_check` and its URLs); then it stops the stack's containers, removes the
   old copy's journal files (`-wal`, `-shm`) so that they never meet the new one, and moves the upload into place, and
   the stack starts as usual.
4. Check the serial console: `gcloud compute instances get-serial-port-output safestart-pipeline --zone=us-central1-a
   | grep "safestart: state/"` shows `state/crawl-osha.sqlite is now the uploaded inventory (<n> URLs)`, and the
   server's `state/state-replace.log` records each upload, applied or refused.

Each upload (each generation of the object) is applied once: later boots leave the server's inventory alone though the
upload stays in the bucket, and only a newer upload replaces it again. An upload that fails its check is refused,
logged and never retried, and the server's copy stays. A replacement discards what the server recorded in that
inventory since the last one (what the feed published stays published), so upload it before the server's first daily
update where possible. Only crawl inventories (`crawl-<site>.sqlite`) are taken from `state-replace/`. The legacy
host's (`crawl-arlweb.sqlite`) is placed the same way after `uv run ssp official backfill-hashes arlweb`; until then
the daily update skips the legacy host as well.

### The official-files feed

The orchestrator's daily update publishes the signed feed for SafeStart at `https://<address>/feed/official/`
([OFFICIAL_FILES.md](OFFICIAL_FILES.md) is the contract). It signs with a key in the server's `.env`:

1. On the workstation: `uv run ssp official keygen --out <file outside the repository>`. It writes the key pair to that
   file only and prints the public key, for SafeStart's configuration.
2. In the maintenance shell above: add the file's `OFFICIAL_FEED_SIGNING_KEY=...` line to
   `/opt/safestart-data-pipeline/.env` and run `sudo docker compose up -d orchestrator`; Compose gives the key to the
   orchestrator alone. Delete the key file on the workstation once the line is in place.
3. After the next run (16:30 UTC), or at once with `sudo docker compose exec orchestrator ssp official manifest`:
   `curl -fsS https://<address>/feed/official/latest.json` shows the newest manifest and `.../public-key.txt` the public
   key; the dashboard's Sources page shows the feed's sequence and age.

Without the key the daily update still records what it finds, and the dashboard warns that nothing was published; the
first signed manifest then covers everything since the first run. Manifests are kept 45 days. After each run the purge
deletes from the server's disk the originals of URLs retired more than 30 days ago, and only where the key is set:
anywhere else, a workstation that runs the orchestrator on its archive among them, it only reports what it would
delete. The bucket and the workstation keep their copies, since the server can only read the bucket, but they never
receive what the server downloads: an edition only the server fetched is gone once purged.

## Costs

An estimate from Google's published on-demand list prices for us-central1, before taxes and credits; the prices are
list prices to confirm with the [Google Cloud pricing calculator](https://cloud.google.com/products/calculator) before
relying on them.

| Item | List price (to confirm) | Per month |
|---|---|---|
| e2-medium VM, running all month (730 hours) | $0.0335 per hour | about $24.50 |
| 50 GB balanced persistent disk (the swap file included) | $0.10 per GB | $5.00 |
| Reserved IPv4 address attached to a running VM | $0.005 per hour | about $3.65 |
| Cloud Storage Standard, about 5 GB for the server (raw/bulk, state, lake, warehouse, exports) | $0.020 per GB | about $0.10 |
| Cloud Storage Standard, the optional raw archive (about 45 GB) | $0.020 per GB | about $0.90 |
| Uploading the raw archive, about 85,000 files (class A operations, once) | $0.005 per 1,000 | about $0.45 once |
| The server's copy from the bucket (same region) | free | $0 |
| Dashboard traffic to the internet | $0.12 per GB after the first GB | under $1 for a demo |
| **Running** | | **about $34** |
| Stopped: disk, unattached address ($0.01 per hour), bucket | | about $13 |

The free tier does not cover an e2-medium; a new account's trial credit does. Sustained-use discounts do not apply to
E2 machines.
