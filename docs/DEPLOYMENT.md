# Deployment

The streaming side of the pipeline runs in Docker Compose: Kafka and PostgreSQL, the stream initializer, the replay
producer, the storage and analytics consumer groups, the live watchers and the dashboard. The same files start it on
a workstation and on a server; on a server an HTTPS gateway is added in front of the dashboard. The batch stages (crawl,
lake, warehouse) run outside Compose and leave their results in the data root, which every service reads through a
read-only mount.

## What runs

| Service | Image | Does | Memory limit | Port |
|---|---|---|---|---|
| `kafka` | `apache/kafka:4.1.2` | Broker and KRaft controller, heap 256-384 MiB | 768 MiB | 127.0.0.1:19092 |
| `postgres` | `postgres:16-alpine` | The `stream` schema, with settings for a small host | 384 MiB | 127.0.0.1:15432 |
| `init` | pipeline | Creates the Kafka topics and the PostgreSQL schema, then exits | 192 MiB | |
| `replay` | pipeline | Replays the enforcement history from the lake through Kafka | 384 MiB | |
| `storage-consumer` | pipeline | Stores every event in `stream.events` | 192 MiB | |
| `analytics-consumer` | pipeline | Daily counts and forecasts | 256 MiB | |
| `watcher` | pipeline | Live watchers; their state lives in the `watch-state` volume | 320 MiB | |
| `dashboard` | pipeline | Streamlit, health at `/_stcore/health` | 640 MiB | 127.0.0.1:8501 |
| `gateway` (server only) | `caddy:2.11-alpine` | HTTPS for a domain name or an IP address | 96 MiB | 80, 443 |

- One image (`Dockerfile`) runs every pipeline service. It is built from `uv.lock` with all extras; the dependencies
  sit in their own layer, so a code change rebuilds in seconds. The services run as the unprivileged user `safestart`
  without Linux capabilities and cannot change their own code.
- The running services may use 2.9 GiB together (3.0 GiB with the gateway), which leaves about 1 GiB of an e2-medium's
  4 GiB to the system and Docker. Kafka measured 320 MiB at rest and 400 MiB while its health check ran.
- The services start in order: Kafka and PostgreSQL until healthy, then `init`, then the rest. `init` runs once per
  start and the replay is restarted only after a failure; every other service restarts unless stopped. Container logs
  rotate at 10 MB, three files per service.
- Kafka's log, the stream store and the watcher state are named volumes (`kafka-data`, `postgres-data`,
  `watch-state`); they survive `docker compose down` and are deleted only with `--volumes`.

## Start on a workstation

Install Docker Desktop (Windows, macOS) or Docker Engine with the Compose plugin (Linux), then, from the repository:

```bash
python scripts/start.py --data-root F:/SafeStartData
```

`--data-root` names the folder that holds `lake/` and `warehouse/` (the replay and the dashboard read them; without it
the empty `./data` is used and the stream services start without history). The first run writes `.env` with a random
PostgreSQL password, builds the image, starts every service and waits until the dashboard answers. Later runs reuse
`.env`: PostgreSQL keeps the password its database was created with, so the launcher never replaces it, and it refuses
to invent a new one while the database of a lost `.env` still exists. The launcher needs only the standard library of
Python 3.

| `.env` setting | Meaning |
|---|---|
| `POSTGRES_PASSWORD` | Password of the `safestart` database user (random, written once) |
| `DATA_ROOT` | Host folder mounted read-only at `/data` |
| `PUBLIC_DOMAIN` | Domain name or public IPv4 address for the HTTPS gateway; empty on a workstation |
| `DASHBOARD_HOST_PORT`, `KAFKA_HOST_PORT`, `POSTGRES_HOST_PORT` | Host ports (8501, 19092, 15432), on 127.0.0.1 only |

Then:

- Dashboard: <http://localhost:8501>.
- Kafka for local tools: `127.0.0.1:19092`; PostgreSQL: `postgresql://safestart:<password>@127.0.0.1:15432/safestart`.
  Use `127.0.0.1` rather than `localhost`: the host ports are open on IPv4 only, and on Windows `localhost` tries IPv6
  first, which costs two seconds per connection.
- End-to-end check: `uv run --extra stream python scripts/smoke_test.py` sends four synthetic events of a district
  named after the run through Kafka, waits until the storage consumer has stored each one and the analytics consumer
  has counted each one exactly once, checks the dashboard, and removes its rows again.
- State and logs: `docker compose ps`, `docker compose logs -f replay`.
- Stop with `docker compose stop` or `docker compose down`; `docker compose down --volumes` also deletes Kafka's log,
  the stream store and the watcher state (the next start creates them again with the password in `.env`).

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

The server is a Compute Engine e2-medium (2 shared vCPUs, 4 GB) with a 50 GB balanced disk and a reserved IPv4
address. Its network admits only HTTP and HTTPS; there is no SSH from the internet. The data root travels through a
Cloud Storage bucket: the workstation uploads it, and the server copies `lake/`, `warehouse/` and `exports/` from it at
every boot (the raw archive stays in the bucket). The server's service account holds no project role and may only
read that bucket.

Run steps 1 to 3 and 5 to 6 in Cloud Shell (the terminal icon in the Google Cloud console); step 4 runs on the
workstation that holds the data.

1. Project. Choose a project ID (lowercase, globally unique) and a billing account:

   ```bash
   PROJECT=safestart-pipeline-<suffix>
   gcloud projects create "$PROJECT" --name="SafeStart pipeline"
   gcloud config set project "$PROJECT"
   gcloud billing accounts list
   gcloud billing projects link "$PROJECT" --billing-account=<ACCOUNT_ID>
   ```

2. Budget alerts. A budget sends e-mail to the billing administrators at 50, 90 and 100 percent of the amount; it does
   not stop spending.

   ```bash
   gcloud services enable billingbudgets.googleapis.com
   NUMBER=$(gcloud projects describe "$PROJECT" --format='value(projectNumber)')
   gcloud billing budgets create --billing-account=<ACCOUNT_ID> --display-name="SafeStart pipeline" \
     --budget-amount=50USD --filter-projects="projects/$NUMBER" \
     --threshold-rule=percent=0.5 --threshold-rule=percent=0.9 --threshold-rule=percent=1.0
   ```

3. Bucket, in the server's region (copies inside a region are free), private:

   ```bash
   BUCKET=safestart-data-<suffix>
   gcloud storage buckets create "gs://$BUCKET" --location=us-central1 --uniform-bucket-level-access \
     --public-access-prevention
   ```

4. Upload, on the Windows workstation, with the Google Cloud CLI installed and signed in (`gcloud auth login`,
   `gcloud config set project <PROJECT>`). Look at the dry run first; the server needs `lake/` and `warehouse/`, so they
   can go before the large `raw/`:

   ```powershell
   deploy/gcp/upload-to-gcs.ps1 -Bucket <BUCKET> -DryRun
   deploy/gcp/upload-to-gcs.ps1 -Bucket <BUCKET> -Only lake,warehouse,exports
   deploy/gcp/upload-to-gcs.ps1 -Bucket <BUCKET>
   ```

   The script runs `gcloud storage rsync` per folder. A rerun skips what already arrived and resumes an interrupted
   large file; nothing in the bucket is ever deleted. Partial downloads of a running crawl and half-written lake files
   are left out. Where PowerShell refuses to run scripts, call it as
   `powershell -ExecutionPolicy Bypass -File deploy/gcp/upload-to-gcs.ps1 -Bucket <BUCKET>`.
   `deploy/gcp/upload-to-gcs.sh` does the same from Linux, macOS or Git Bash.

5. Server:

   ```bash
   git clone https://github.com/USERZEROA/safestart-data-pipeline.git
   cd safestart-data-pipeline
   BUCKET=<BUCKET> bash deploy/gcp/create-vm.sh
   ```

   `create-vm.sh` enables the APIs, creates the network and its firewall rule (TCP 80 and 443 only), reserves the
   address, creates the service account and its read access to the bucket, and creates the VM with
   `deploy/gcp/startup.sh` as its startup script. Each step reuses what an earlier run made. `DOMAIN=<name>` serves a
   domain instead of the address; `REGION`, `ZONE`, `NAME`, `REPO` and `BRANCH` override the defaults.

6. Verify. The first boot installs Docker, copies the data, builds the image and starts the stack in about ten minutes:

   ```bash
   gcloud compute instances get-serial-port-output safestart-pipeline --zone=us-central1-a | grep safestart:
   curl -fsS https://<ADDRESS>/_stcore/health        # prints ok
   ```

   Then open `https://<ADDRESS>` in a browser; the stream's figures grow while the replay runs.

Operating the server:

- Update after a push to `main`: `gcloud compute instances reset safestart-pipeline --zone=us-central1-a`. Every boot
  brings the checkout up to the branch, copies new data from the bucket and rebuilds what changed; `.env` stays.
- A shell for maintenance needs a temporary firewall rule for Google's IAP range, removed afterwards:

  ```bash
  gcloud compute firewall-rules create safestart-pipeline-iap-ssh --network=safestart-pipeline-net \
    --allow=tcp:22 --source-ranges=35.235.240.0/20 --target-tags=safestart-pipeline-web
  gcloud compute ssh safestart-pipeline --zone=us-central1-a --tunnel-through-iap
  # on the server: cd /opt/safestart-data-pipeline && sudo docker compose ps
  #   sudo docker compose exec analytics-consumer python /app/scripts/smoke_test.py
  gcloud compute firewall-rules delete safestart-pipeline-iap-ssh
  ```

- Pause: `gcloud compute instances stop safestart-pipeline --zone=us-central1-a` (the disk and the address are still
  billed). Remove everything: delete the instance, the address `safestart-pipeline-ip`, the firewall rule, the network
  and, when the data is no longer needed, the bucket.

## Costs

An estimate from Google's published on-demand prices for us-central1, before taxes and credits. Prices change; check
them with the [Google Cloud pricing calculator](https://cloud.google.com/products/calculator) before relying on them.

| Item | Price | Per month |
|---|---|---|
| e2-medium VM, running all month (730 hours) | $0.0335 per hour | about $24.50 |
| 50 GB balanced persistent disk | $0.10 per GB | $5.00 |
| Reserved IPv4 address attached to a running VM | $0.005 per hour | about $3.65 |
| Cloud Storage Standard, about 48 GB (raw 46 GB, lake 0.8 GB, warehouse and exports) | $0.020 per GB | about $1 |
| Upload of about 85,000 files (class A operations, once) | $0.005 per 1,000 | about $0.45 once |
| Dashboard traffic to the internet | $0.12 per GB after the first GB | under $1 for a demo |
| **Running** | | **about $34** |
| Stopped: disk, unattached address ($0.01 per hour), bucket | | about $13 |

The free tier does not cover an e2-medium; a new account's trial credit does. Sustained-use discounts do not apply to
E2 machines.
