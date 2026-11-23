#!/usr/bin/env bash
# Copies the data root's raw/, lake/, warehouse/ and exports/ to gs://<bucket>/<folder>/ with `gcloud storage rsync`.
# Resumable: a rerun skips what already arrived and continues an interrupted large file. It never deletes anything in
# the bucket. --dry-run lists what would be copied without copying it. (On Windows, upload-to-gcs.ps1 does the same.)
#
#   deploy/gcp/upload-to-gcs.sh <bucket> [--data-root DIR] [--only "lake warehouse"] [--dry-run]
#
# The data root defaults to $SAFESTART_DATA_ROOT, else F:/SafeStartData. A folder the data root lacks is skipped.
set -Eeuo pipefail

usage() {
  echo "usage: $0 <bucket> [--data-root DIR] [--only \"raw lake warehouse exports\"] [--dry-run]" >&2
  exit 2
}

bucket=
data_root=${SAFESTART_DATA_ROOT:-F:/SafeStartData}
folders="raw lake warehouse exports"
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

# Partial downloads of a running crawl (objects/tmp/) and half-written lake files (*.partial) are not data. The
# patterns avoid backslashes and '|' so they pass unchanged through any shell.
exclude='.*objects.tmp.*,.*[.]partial$'
for folder in $folders; do
  case $folder in
    raw | lake | warehouse | exports) ;;
    *) echo "unknown folder $folder (raw, lake, warehouse or exports)" >&2; exit 2 ;;
  esac
  if [[ ! -d $data_root/$folder ]]; then
    echo "Skipped $folder/: not in $data_root."
    continue
  fi
  echo "Copying $data_root/$folder/ to gs://$bucket/$folder/ ${dry_run:+(dry run)}"
  # shellcheck disable=SC2086  # $dry_run is empty or one flag
  gcloud storage rsync --recursive $dry_run --exclude="$exclude" "$data_root/$folder" "gs://$bucket/$folder"
done
