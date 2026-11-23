#!/usr/bin/env bash
# Startup script of the pipeline's Compute Engine VM (create-vm.sh installs it); it runs as root at every boot.
# It installs Docker from Docker's own repository (Ubuntu's package can be too old for the Compose file), brings the
# checkout up to date, copies lake/, warehouse/ and exports/ from the data bucket, and starts the stack behind the HTTPS
# gateway for the VM's address or its domain. Every message starts with "safestart:", so the serial console shows the
# progress: gcloud compute instances get-serial-port-output <name> | grep safestart:
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

# Swap as a safety margin on a 4 GB machine; the services' memory limits already keep them within the RAM.
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

install -d -m 0755 "$data"
if [[ -n $bucket ]]; then
  command -v gcloud >/dev/null || snap install google-cloud-cli --classic
  for part in lake warehouse exports; do
    if gcloud storage ls "gs://$bucket/$part/" >/dev/null 2>&1; then
      echo "safestart: copying gs://$bucket/$part/"
      gcloud storage rsync --recursive "gs://$bucket/$part" "$data/$part"
    else
      echo "safestart: gs://$bucket/$part/ is empty, skipped"
    fi
  done
fi
chmod -R a+rX "$data"  # the services read it as an unprivileged user

cd "$app"
python3 scripts/start.py --public-domain "$domain" --data-root "$data"
echo "safestart: running at https://$domain"
