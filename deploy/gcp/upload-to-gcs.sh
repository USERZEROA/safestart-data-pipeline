#!/usr/bin/env bash
# Copies the parts of the data root the server needs (raw/bulk/, state/, lake/, warehouse/, exports/) to
# gs://<bucket>/<folder>/ with `gcloud storage rsync`; `--only raw` copies the whole raw archive instead, the crawls'
# 43 GB included, which the server never reads. Resumable: a rerun skips what already arrived and continues an
# interrupted large file. It never deletes anything in the bucket. --dry-run lists what would be copied without copying
# it. (On Windows, upload-to-gcs.ps1 does the same.)
#
#   deploy/gcp/upload-to-gcs.sh <bucket> [--data-root DIR] [--only "lake warehouse"] [--dry-run]
#
# The data root defaults to $SAFESTART_DATA_ROOT, else F:/SafeStartData. A folder the data root lacks is skipped.
# Upload state/ while no crawl, download or lake build is writing to it, so that each database is copied whole.
set -Eeuo pipefail

usage() {
  echo "usage: $0 <bucket> [--data-root DIR] [--only \"raw/bulk state lake warehouse exports raw\"] [--dry-run]" >&2
  exit 2
}

bucket=
data_root=${SAFESTART_DATA_ROOT:-F:/SafeStartData}
folders="raw/bulk state lake warehouse exports"
dry_run=
while (($#)); do
  case $1 in
    --data-root) [[ $# -ge 2 ]] || usage; data_root=$2; shift 2 ;;
    --only) [[ $# -ge 2 ]] || usage; folders=$2; shift 2 ;;
    --dry-run) dry_run=--dry-run; shift ;;
    -*) usage ;;
    *) [[ -z $bucket ]] || usage; bucket=${1#gs://}; bucket=${bucket%/}; shift ;;
  esac
done
[[ $bucket =~ ^[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]$ ]] || usage
if ! command -v gcloud >/dev/null; then
  echo "Install the Google Cloud CLI and sign in with 'gcloud auth login' first." >&2
  exit 1
fi

# Not data: partial downloads of a running crawl (objects/tmp/), half-written lake files (*.partial), and the journal
# files of a database in use (SQLite's -wal, -shm and -journal; the warehouse's DuckDB .wal, a build in progress and its
# staging folder), which must never meet another copy of their database. The patterns avoid backslashes and '|' so
# they pass unchanged through any shell.
exclude='.*objects.tmp.*,.*[.]partial$,.*[.]sqlite-[a-z]+$,.*[.]duckdb[.-].*'
for folder in $folders; do
  case $folder in
    raw/bulk | state | lake | warehouse | exports | raw) ;;
    *) echo "unknown folder $folder (raw/bulk, state, lake, warehouse, exports or raw)" >&2; exit 2 ;;
  esac
  if [[ ! -d $data_root/$folder ]]; then
    echo "Skipped $folder/: not in $data_root."
    continue
  fi
  echo "Copying $data_root/$folder/ to gs://$bucket/$folder/ ${dry_run:+(dry run)}"
  # shellcheck disable=SC2086  # $dry_run is empty or one flag
  gcloud storage rsync --recursive $dry_run --exclude="$exclude" "$data_root/$folder" "gs://$bucket/$folder"
done
