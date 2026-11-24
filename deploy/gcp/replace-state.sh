#!/usr/bin/env bash
# Puts a crawl inventory the operator uploaded to gs://<bucket>/state-replace/ in place of the server's copy in state/,
# once per upload, without SSH (docs/DEPLOYMENT.md, "Placing a crawl inventory on the server"). startup.sh runs it at
# every boot, after the bucket copy and before the stack starts:
#
#   deploy/gcp/replace-state.sh <bucket> <data root>
#
# Only crawl inventories (crawl-<site>.sqlite) are taken. A new upload (a generation of the object not seen before) is
# downloaded beside the server's copy and checked (SQLite's quick_check and its urls table); then, with every container
# of the stack stopped, the old copy's -wal, -shm and -journal are removed, so that no journal of the old copy meets the
# new one, and the upload is moved into place. Each upload's outcome is appended to state/state-replace.log as
# "<name> <generation> applied|refused <time>", and an upload found there is never taken again: from then on the
# inventory is the server's own, and only a newer upload replaces it. An upload that fails its check is refused and the
# server's copy stays. PYTHON names the interpreter for the check (python3 by default).
set -Eeuo pipefail

bucket=${1:?usage: replace-state.sh <bucket> <data root>}
data=${2:?usage: replace-state.sh <bucket> <data root>}
python=${PYTHON:-python3}
log=$data/state/state-replace.log
install -d "$data/state"
touch "$log"

# Prints the number of URLs of a whole SQLite inventory; fails for anything else.
check() {
  "$python" -c 'import sqlite3, sys, urllib.request
db = sqlite3.connect("file:" + urllib.request.pathname2url(sys.argv[1]) + "?mode=ro&immutable=1", uri=True)
if db.execute("pragma quick_check").fetchone()[0] != "ok":
    sys.exit("quick_check failed")
print(db.execute("select count(*) from urls").fetchone()[0])' "$1"
}

for object in $(gcloud storage ls "gs://$bucket/state-replace/" 2>/dev/null || true); do
  name=${object##*/}
  if [[ ! $name =~ ^crawl-[a-z0-9]+[.]sqlite$ ]]; then
    [[ -z $name ]] || echo "safestart: $object is not a crawl inventory, left alone"
    continue
  fi
  generation=$(gcloud storage objects describe "$object" --format='value(generation)')
  if [[ ! $generation =~ ^[0-9]+$ ]]; then  # without it the upload could not be recorded, and would come back
    echo "safestart: $object has no generation to record, left alone"
    continue
  fi
  if awk -v name="$name" -v generation="$generation" '$1 == name && $2 == generation { found = 1 }
       END { exit !found }' "$log"; then
    continue  # applied or refused at an earlier boot
  fi
  echo "safestart: placing $object (generation $generation) as state/$name"
  incoming=$data/state/.$name.incoming
  rm -f "$incoming"
  gcloud storage cp "$object" "$incoming"
  if ! urls=$(check "$incoming"); then
    rm -f "$incoming"
    echo "$name $generation refused $(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$log"
    echo "safestart: $object failed its check; the server's state/$name stays"
    continue
  fi
  running=$(docker ps --quiet --filter label=com.docker.compose.project=safestart)
  if [[ -n $running ]]; then
    echo "safestart: stopping the stack while state/$name is replaced"
    # shellcheck disable=SC2086  # one container id per word
    docker stop $running >/dev/null
  fi
  rm -f "$data/state/$name-wal" "$data/state/$name-shm" "$data/state/$name-journal"
  mv -f "$incoming" "$data/state/$name"
  echo "$name $generation applied $(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$log"
  echo "safestart: state/$name is now the uploaded inventory ($urls URLs)"
done
