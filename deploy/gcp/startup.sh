#!/usr/bin/env bash
# Startup script of the pipeline's Compute Engine VM (create-vm.sh installs it); it runs as root at every boot.
# It adds a 2 GB swap file, installs Docker from Docker's own repository (Ubuntu's package can be too old for the
# Compose file), brings the checkout up to date, copies from the data bucket what the services need and the server
# lacks (raw/bulk/, state/, lake/, warehouse/, exports/; not the crawls' raw archive), and starts the stack behind the
# HTTPS gateway for the VM's address or its domain. Every message starts with "safestart:", so the serial console shows
# the progress: gcloud compute instances get-serial-port-output <name> | grep safestart:
set -Eeuo pipefail
exec > >(tee -a /var/log/safestart-startup.log) 2>&1
trap 'echo "safestart: startup failed at line $LINENO"' ERR
export PATH=$PATH:/snap/bin DEBIAN_FRONTEND=noninteractive

metadata=http://metadata.google.internal/computeMetadata/v1/instance
query() { curl -fsS --retry 5 -H 'Metadata-Flavor: Google' "$metadata/$1"; }
setting() { query "attributes/$1" 2>/dev/null || true; }

repo=$(setting safestart-repo)
repo=${repo:-https://github.com/USERZEROA/safestart-data-pipeline.git}
branch=$(setting safestart-branch)
branch=${branch:-main}
bucket=$(setting safestart-bucket)
domain=$(setting safestart-domain)
domain=${domain:-$(query network-interfaces/0/access-configs/0/external-ip)}
app=/opt/safestart-data-pipeline
data=/srv/safestart-data
echo "safestart: starting for $domain from $repo ($branch)"

# 2 GB of swap: the services that keep running fit in the 4 GB of memory, and the orchestrator's daily batch and weekly
# model training may overflow into it (the memory budget in docker-compose.yml).
if [[ -z $(swapon --show=NAME --noheadings) ]]; then
  fallocate -l 2G /swapfile && chmod 600 /swapfile && mkswap /swapfile >/dev/null && swapon /swapfile
  grep -q '^/swapfile ' /etc/fstab || echo '/swapfile none swap sw 0 0' >> /etc/fstab
fi

if ! docker compose version >/dev/null 2>&1; then
  echo "safestart: installing Docker"
  apt-get -q -o DPkg::Lock::Timeout=300 update
  apt-get -qy -o DPkg::Lock::Timeout=300 install ca-certificates curl git python3
  install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
  chmod a+r /etc/apt/keyrings/docker.asc
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc]" \
    "https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
    > /etc/apt/sources.list.d/docker.list
  apt-get -q -o DPkg::Lock::Timeout=300 update
  apt-get -qy -o DPkg::Lock::Timeout=300 install docker-ce docker-ce-cli containerd.io docker-buildx-plugin \
    docker-compose-plugin
fi
systemctl enable --now docker

# The VM runs the branch as published; .env (the database password) is not tracked, so an update keeps it.
if [[ ! -d $app/.git ]]; then
  git clone --branch "$branch" "$repo" "$app"
fi
git -C "$app" fetch --quiet origin "$branch"
git -C "$app" checkout --quiet --force -B "$branch" "origin/$branch"
echo "safestart: code at $(git -C "$app" rev-parse --short HEAD)"

# The bucket seeds the data root. A boot copies only the files the server lacks: once there, the orchestrator keeps
# them current (new downloads, the lake, the warehouse, the exports), so an older copy in the bucket never replaces
# them, and nothing on the server is deleted. A database's journal files (SQLite -wal/-shm, DuckDB .wal) and work in
# progress are never copied, so a journal cannot meet another copy of its database.
install -d -m 0755 "$data"
if [[ -n $bucket ]]; then
  command -v gcloud >/dev/null || snap install google-cloud-cli --classic
  for part in raw/bulk state lake warehouse exports; do
    if gcloud storage ls "gs://$bucket/$part/" >/dev/null 2>&1; then
      echo "safestart: copying what the server lacks of gs://$bucket/$part/"
      install -d "$data/$part"
      gcloud storage rsync --recursive --no-clobber \
        --exclude='.*objects.tmp.*,.*[.]partial$,.*[.]sqlite-[a-z]+$,.*[.]duckdb[.-].*' \
        "gs://$bucket/$part" "$data/$part"
    else
      echo "safestart: gs://$bucket/$part/ is empty, skipped"
    fi
  done
fi
# Every service runs as the image's user, uid 10001: the orchestrator writes the data root, the others read it.
chown -R 10001:10001 "$data"

cd "$app"
python3 scripts/start.py --public-domain "$domain" --data-root "$data"
echo "safestart: running at https://$domain"
