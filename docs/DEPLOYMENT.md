# Deployment

The pipeline runs in Docker Compose: Kafka and PostgreSQL, the stream initializer, the replay producer, the storage and
analytics consumer groups, the live watchers, the dashboard and the orchestrator of the batch flows. The same files
start it on a workstation and on a server; on a server an HTTPS gateway is added in front of the dashboard. The crawls
stay on the workstation (they run for days under each site's pacing); everything else runs in the stack. Every service
mounts the data root at `/data`, read-only, except the orchestrator, which runs the batch that writes it.

## What runs

| Service | Image | Does | Memory limit | Port |
|---|---|---|---|---|
| `kafka` | `apache/kafka:4.1.2` | Broker and KRaft controller, heap 256-384 MiB | 768 MiB | 127.0.0.1:19092 |
| `postgres` | `postgres:16-alpine` | The `stream` schema, with settings for a small host | 384 MiB | 127.0.0.1:15432 |
| `init` | pipeline | Creates the Kafka topics and the PostgreSQL schema, then exits | 192 MiB | |
| `replay` | pipeline | Replays the enforcement history from the lake through Kafka | 384 MiB | |
| `storage-consumer` | pipeline | Stores every event in `stream.events` | 192 MiB | |
| `analytics-consumer` | pipeline | Daily counts and forecasts | 256 MiB | |
| `watcher` | pipeline | Live watchers; their state lives in the `watch-state` volume | 640 MiB | |
| `dashboard` | pipeline | Streamlit, health at `/_stcore/health` | 448 MiB | 127.0.0.1:8501 |
| `orchestrator` | pipeline | The daily batch (downloads, lake, warehouse, exports) and the weekly model, through Prefect; only with the `orchestrator` profile | 1,920 MiB | |
| `gateway` (server only) | `caddy:2.11-alpine` | HTTPS for a domain name or an IP address | 96 MiB | 80, 443 |

- One image (`Dockerfile`) runs every pipeline service. It is built from `uv.lock` with all extras; the dependencies
  sit in their own layer, so a code change rebuilds in seconds. The services run as the unprivileged user `safestart`
  (uid 10001) without Linux capabilities and cannot change their own code.
- Memory is budgeted for an e2-medium (4 GiB) with a 2 GiB swap file, leaving 1 GiB to the system and Docker
  (`tests/test_deployment_files.py` holds the files to it). The services that keep running may use 2.7 GiB together,
  the gateway included, so they never need swap. The orchestrator's limit covers its largest run, the weekly model
  training, which peaked at 1.35 GiB in this image with two threads, beside Prefect and its local server (0.4 GiB);
  with it and the replay at their limits as well, the host may swap but never runs out of memory. Measured in the
  image on the real lake: the warehouse build peaks at 1.0 GiB, the watcher's comparison of two versions of the 3.1
  million violations at 607 MiB, and the dashboard at 300 MiB rendering every page from the real warehouse. At rest,
  Kafka, PostgreSQL, both consumers, the dashboard and the orchestrator used about 600 MiB together, 370 MiB of it
  Kafka's.
- The services start in order: Kafka and PostgreSQL until healthy, then `init`, then the stream services. `init` runs
  once per start and the replay is restarted only after a failure; every other service restarts unless stopped. The
  orchestrator runs only with its Compose profile (`COMPOSE_PROFILES=orchestrator`), since it writes the data root;
  it needs neither Kafka nor PostgreSQL and starts at once. Container logs rotate at 10 MB, three files per service.
- Kafka's log, the stream store and the watcher state are named volumes (`kafka-data`, `postgres-data`,
  `watch-state`); they survive `docker compose down` and are deleted only with `--volumes`.

### Where each service writes

| Service | Writes | Where |
|---|---|---|
| `orchestrator` | downloads, bulk store, lake, warehouse, exports, model | the data root, mounted read-write; Prefect's settings in its home folder |
| `watcher` | its state (`SAFESTART_WATCH_STATE=/state/watch.sqlite`) and the version check's DuckDB spill folder | the `watch-state` volume |
| `watcher` | its log (`SAFESTART_WATCH_LOG=-`) | standard error, kept by Docker |
| `dashboard` | DuckDB's spill folder, should a query outgrow its 192 MiB | `/tmp` in the container |
| `replay` | DuckDB's spill folder | `/tmp` in the container |
| `kafka`, `postgres` | the log, the stream store | their volumes |

The read-only services read the data root's SQLite stores (the bulk store, the crawl inventories) even when they
cannot create those databases' `-wal` and `-shm` files beside them: a database no process has open is then read as
immutable (`verify.read_only`).

## Start on a workstation

Install Docker Desktop (Windows, macOS) or Docker Engine with the Compose plugin (Linux), then, from the repository:

```bash
python scripts/start.py --data-root F:/SafeStartData
```

`--data-root` names the folder that holds the data root (`raw/`, `state/`, `lake/`, `warehouse/`, `exports/`); without
it the empty `./data` is used and the stream services start without history until the orchestrator's first batch. The
first run writes `.env` with a random PostgreSQL password, builds the image, starts every service but the
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

Then:

- Dashboard: <http://localhost:8501>.
- Kafka for local tools: `127.0.0.1:19092`; PostgreSQL: `postgresql://safestart:<password>@127.0.0.1:15432/safestart`.
  Use `127.0.0.1` rather than `localhost`: the host ports are open on IPv4 only, and on Windows `localhost` tries IPv6
  first, which costs two seconds per connection. The settings' defaults (`config.py`) do the same.
- End-to-end check: `uv run --extra stream python scripts/smoke_test.py` sends four synthetic events of a district
  named after the run through Kafka, waits until the storage consumer has stored each one and the analytics consumer
  has counted each one exactly once, checks the dashboard, and removes its rows again, the counted-event ledger
  included.
- State and logs: `docker compose ps`, `docker compose logs -f replay`, `docker compose logs -f watcher`.
- One writer of the data root at a time: on a workstation that runs the orchestrator and also runs the batch or the
  model by hand (`ssp pipeline`, `python -m safestart_pipeline.flows run`) on the same folder, stop the orchestrator
  first with `docker compose stop orchestrator`. File locks do not reach across Docker Desktop's file sharing, so the warehouse
  lock cannot keep a process on the host and one in a container apart.
- Stop with `docker compose stop` or `docker compose down`; `docker compose down --volumes` also deletes Kafka's log,
  the stream store and the watcher state (the next start creates them again with the password in `.env`).

### The orchestrator and Prefect

`python scripts/start.py --with-orchestrator` turns the orchestrator on (the server's startup script does). Without a
Prefect API it keeps the schedule in its own process (batch every day at 14:00 UTC, model every Sunday at 15:00 UTC)
and logs each run: `docker compose logs -f orchestrator`. With Prefect Cloud, put the workspace's
API URL and an API key in `.env` and recreate the service:

```bash
PREFECT_API_URL=https://api.prefect.cloud/api/accounts/<account>/workspaces/<workspace>
PREFECT_API_KEY=<API key>
```

```bash
docker compose up -d orchestrator
```

It then serves the batch and model flows as deployments of that workspace, on the same schedule, and a run can also be
started from Prefect Cloud. Without Prefect Cloud, leave both lines commented out rather than empty: with an empty URL
Prefect logs errors on every run. Polars and the model's libraries are held to two threads (`POLARS_MAX_THREADS`,
`OMP_NUM_THREADS`), the e2-medium's two vCPUs. To run the batch now:
`docker compose exec orchestrator python -m safestart_pipeline.flows run batch`.

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
From then on the orchestrator keeps the server's copy current. The server's service account holds no project role and
may only read that bucket.

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
   deploy/gcp/upload-to-gcs.ps1 -Bucket <bucket> -DryRun
   deploy/gcp/upload-to-gcs.ps1 -Bucket <bucket>
   deploy/gcp/upload-to-gcs.ps1 -Bucket <bucket> -Only raw   # optional: the whole raw archive, crawls included
   ```

   By default the script copies `raw/bulk/`, `state/`, `lake/`, `warehouse/` and `exports/`, each with
   `gcloud storage rsync`; `-Only` picks folders, and `raw` names the whole raw archive, an archive the server never
   reads. A rerun skips what already arrived and resumes an interrupted large file; nothing in the bucket is ever
   deleted. Left out: partial downloads of a running crawl, half-written lake files, and the journal files of a
   database in use (SQLite's `-wal`, `-shm` and `-journal`; the warehouse's `.wal`, a build in progress and its
   staging folder), which must never meet another copy of their database. Where PowerShell refuses to run scripts,
   call it as `powershell -ExecutionPolicy Bypass -File deploy/gcp/upload-to-gcs.ps1 -Bucket <bucket>`.
   `deploy/gcp/upload-to-gcs.sh` does the same from Linux, macOS or Git Bash.

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
--no-clobber`) and deletes nothing: the orchestrator keeps the server's downloads, lake, warehouse and exports current,
so an older copy in the bucket never replaces them. To take a newer copy of a file from the bucket (a crawl inventory
in `state/` after a new crawl), delete the server's copy and reboot. The script then gives the data root to uid 10001,
the image's user, for the orchestrator to write.

Operating the server:

- Update after a push to `main`: `gcloud compute instances reset safestart-pipeline --zone=us-central1-a`. Every boot
  brings the checkout up to the branch, copies what the server lacks from the bucket and rebuilds what changed; `.env`
  stays.
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
