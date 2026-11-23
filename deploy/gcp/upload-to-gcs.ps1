# Windows: copies the parts of the data root the server needs (raw/bulk/, state/, lake/, warehouse/, exports/) to
# gs://<bucket>/<folder>/ with `gcloud storage rsync`; `-Only raw` copies the whole raw archive instead, the crawls'
# 43 GB included, which the server never reads. Resumable: a rerun skips what already arrived and continues an
# interrupted large file. It never deletes anything in the bucket. -DryRun lists what would be copied without copying
# it.
#
#   deploy/gcp/upload-to-gcs.ps1 -Bucket <bucket> [-DataRoot F:/SafeStartData] [-Only lake,warehouse] [-DryRun]
#
# The data root defaults to $env:SAFESTART_DATA_ROOT, else F:/SafeStartData. A folder the data root lacks is skipped.
# Upload state/ while no crawl, download or lake build is writing to it, so that each database is copied whole.
param(
  [Parameter(Mandatory = $true)][string]$Bucket,
  [string]$DataRoot = $(if ($env:SAFESTART_DATA_ROOT) { $env:SAFESTART_DATA_ROOT } else { "F:/SafeStartData" }),
  [string[]]$Only = @("raw/bulk", "state", "lake", "warehouse", "exports"),
  [switch]$DryRun
)
$ErrorActionPreference = "Stop"

$Bucket = $Bucket -replace '^gs://', '' -replace '/+$', ''
if ($Bucket -cnotmatch '^[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]$') { throw "Not a Cloud Storage bucket name: $Bucket" }
# "-Only lake,warehouse" arrives as two values from a PowerShell prompt and as one from powershell -File.
$Only = @($Only | ForEach-Object { $_ -split ',' } | ForEach-Object { $_.Trim() } | Where-Object { $_ })
$folders = @("raw/bulk", "state", "lake", "warehouse", "exports", "raw")
foreach ($folder in $Only) {
  if ($folder -cnotin $folders) {
    throw "Unknown folder $folder (raw/bulk, state, lake, warehouse, exports or raw)"
  }
}
if (-not (Get-Command gcloud -ErrorAction SilentlyContinue)) {
  throw "Install the Google Cloud CLI and sign in with 'gcloud auth login' first."
}

# Not data: partial downloads of a running crawl (objects/tmp/), the work files of a lake build in progress or killed
# (*.partial, *.tmp, and the text it transcodes a zip's member into, .<16 hex digits>.txt, up to 1.5 GB), and the
# journal files of a database in use (SQLite's -wal, -shm and -journal; the warehouse's DuckDB .wal, a build in
# progress and its staging folder), which must never meet another copy of their database. The patterns avoid
# backslashes and '|' so they pass unchanged through gcloud.cmd.
$exclude = '.*objects.tmp.*,.*[.]partial$,.*[.]sqlite-[a-z]+$,.*[.]duckdb[.-].*,.*[.]tmp$,.*[.][0-9a-f]{16}[.]txt$'
foreach ($folder in $Only) {
  $source = Join-Path $DataRoot $folder
  if (-not (Test-Path -LiteralPath $source -PathType Container)) {
    "Skipped ${folder}/: not in $DataRoot."
    continue
  }
  "Copying $source to gs://$Bucket/$folder/" + $(if ($DryRun) { " (dry run)" } else { "" })
  $arguments = @("storage", "rsync", "--recursive", "--exclude=$exclude", $source, "gs://$Bucket/$folder")
  if ($DryRun) { $arguments += "--dry-run" }
  & gcloud @arguments
  if ($LASTEXITCODE -ne 0) {
    throw "gcloud storage rsync stopped on ${folder}/ (exit $LASTEXITCODE); run the script again to resume."
  }
}
