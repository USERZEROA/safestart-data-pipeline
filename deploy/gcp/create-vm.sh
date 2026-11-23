#!/usr/bin/env bash
# Creates the pipeline's server on Google Cloud: an e2-medium VM (2 shared vCPUs, 4 GB) with a 50 GB balanced disk and a
# reserved public IPv4 address, on a network of its own whose firewall admits only HTTP and HTTPS. Its startup script
# (startup.sh) adds swap, installs Docker, copies raw/bulk/, state/, lake/, warehouse/ and exports/ from the bucket and
# starts the stack behind the HTTPS gateway. Run it in Cloud Shell from a checkout of the repository; docs/DEPLOYMENT.md
# has the whole procedure.
#
#   BUCKET=<bucket> bash deploy/gcp/create-vm.sh
#
# Settings (environment): PROJECT (default: the gcloud project), REGION (us-central1), ZONE (<region>-a), NAME
# (safestart-pipeline), BUCKET (the data bucket the VM may read; without it the stack starts with no data), DOMAIN (a
# domain name whose DNS points at the address; default: HTTPS for the address itself), REPO and BRANCH (what the VM
# runs).
# Each step reuses what an earlier run created, so a rerun finishes an interrupted one; on an existing VM it updates
# the settings and the startup script, which take effect at the next boot.
set -Eeuo pipefail

project=${PROJECT:-$(gcloud config get-value project 2>/dev/null)}
region=${REGION:-us-central1}
zone=${ZONE:-$region-a}
name=${NAME:-safestart-pipeline}
bucket=${BUCKET:-}
domain=${DOMAIN:-}
repo=${REPO:-https://github.com/USERZEROA/safestart-data-pipeline.git}
branch=${BRANCH:-main}
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

if [[ -z $project ]]; then
  echo "No project: run 'gcloud config set project <project id>' or set PROJECT." >&2
  exit 1
fi
gc() { gcloud --project="$project" --quiet "$@"; }
missing() { ! gc "$@" --format='value(name)' >/dev/null 2>&1; }

echo "Project $project, zone $zone, server $name"
gc services enable compute.googleapis.com iam.googleapis.com storage.googleapis.com

# The default network admits SSH and RDP from anywhere; this one admits web traffic and nothing else.
network=$name-net
if missing compute networks describe "$network"; then
  gc compute networks create "$network" --subnet-mode=custom
fi
if missing compute networks subnets describe "$network-$region" --region="$region"; then
  gc compute networks subnets create "$network-$region" --network="$network" --region="$region" --range=10.20.0.0/24
fi
if missing compute firewall-rules describe "$name-web"; then
  gc compute firewall-rules create "$name-web" --network="$network" --direction=INGRESS --action=ALLOW \
    --rules=tcp:80,tcp:443 --source-ranges=0.0.0.0/0 --target-tags="$name-web"
fi

# A reserved address keeps the URL, and the certificate issued for it, across restarts.
if missing compute addresses describe "$name-ip" --region="$region"; then
  gc compute addresses create "$name-ip" --region="$region"
fi
address=$(gc compute addresses describe "$name-ip" --region="$region" --format='value(address)')

# The VM runs as its own service account, which holds no project role; with a bucket it may read that bucket and
# nothing else. (Giving a VM an account later means stopping it, so it gets one from the start.)
account=$name-vm@$project.iam.gserviceaccount.com
if missing iam service-accounts describe "$account"; then
  gc iam service-accounts create "$name-vm" --display-name="SafeStart pipeline server"
fi
if [[ -n $bucket ]]; then
  bucket=${bucket#gs://}
  bucket=${bucket%/}
  if missing storage buckets describe "gs://$bucket"; then
    echo "No bucket gs://$bucket in $project: create it and upload the data first (docs/DEPLOYMENT.md)." >&2
    exit 1
  fi
  for attempt in 1 2 3 4 5 6; do  # a new service account takes a few seconds to become usable in policies
    if gc storage buckets add-iam-policy-binding "gs://$bucket" --member="serviceAccount:$account" \
      --role=roles/storage.objectViewer >/dev/null; then
      break
    fi
    [[ $attempt -lt 6 ]] || exit 1
    sleep 10
  done
fi

settings="safestart-repo=$repo,safestart-branch=$branch,enable-oslogin=TRUE,block-project-ssh-keys=TRUE"
[[ -z $bucket ]] || settings+=",safestart-bucket=$bucket"
[[ -z $domain ]] || settings+=",safestart-domain=$domain"
if missing compute instances describe "$name" --zone="$zone"; then
  gc compute instances create "$name" --zone="$zone" --machine-type=e2-medium \
    --image-family=ubuntu-2404-lts-amd64 --image-project=ubuntu-os-cloud \
    --boot-disk-size=50GB --boot-disk-type=pd-balanced \
    --subnet="$network-$region" --address="$address" --tags="$name-web" \
    --service-account="$account" --scopes=storage-ro \
    --shielded-secure-boot --shielded-vtpm --shielded-integrity-monitoring \
    --metadata="$settings" --metadata-from-file=startup-script="$here/startup.sh" \
    --labels=app=safestart-pipeline
else
  gc compute instances add-metadata "$name" --zone="$zone" --metadata="$settings" \
    --metadata-from-file=startup-script="$here/startup.sh"
  # A stop shuts the guest down, so every container stops within its grace period; a reset would cut them off.
  echo "$name exists; its settings are updated for the next boot (gcloud compute instances stop $name --zone=$zone" \
    "&& gcloud compute instances start $name --zone=$zone)."
fi

site=${domain:-$address}
echo
echo "Server $name has the address $address."
[[ -z $domain ]] || echo "Point the DNS A record of $domain at $address; the certificate is issued once it resolves."
cat <<EOF
The server now installs Docker, copies the data and starts the stack (about ten minutes on the first boot).
Follow it with
  gcloud compute instances get-serial-port-output $name --zone=$zone --project=$project | grep safestart:
then check
  curl -fsS https://$site/_stcore/health
and open https://$site in a browser.
EOF
