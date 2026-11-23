# Windows: copies the data root's raw/, lake/, warehouse/ and exports/ to gs://<bucket>/<folder>/ with
# `gcloud storage rsync`. Resumable: a rerun skips what already arrived and continues an interrupted large file. It
# never deletes anything in the bucket. -DryRun lists what would be copied without copying it.
#
#   deploy/gcp/upload-to-gcs.ps1 -Bucket <bucket> [-DataRoot F:/SafeStartData] [-Only lake,warehouse] [-DryRun]
#
# The data root defaults to $env:SAFESTART_DATA_ROOT, else F:/SafeStartData. A folder the data root lacks is skipped.
param(
  [Parameter(Mandatory = $true)][string]$Bucket,
  [string]$DataRoot = $(if ($env:SAFESTART_DATA_ROOT) { $env:SAFESTART_DATA_ROOT } else { "F:/SafeStartData" }),
  [string[]]$Only = @("raw", "lake", "warehouse", "exports"),
  [switch]$DryRun
)
$ErrorActionPreference = "Stop"

$Bucket = $Bucket -replace '^gs://', '' -replace '/+$', ''
if ($Bucket -cnotmatch '^[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]$') { throw "Not a Cloud Storage bucket name: $Bucket" }
# "-Only lake,warehouse" arrives as two values from a PowerShell prompt and as one from powershell -File.
$Only = @($Only | ForEach-Object { $_ -split ',' } | ForEach-Object { $_.Trim() } | Where-Object { $_ })
foreach ($folder in $Only) {
  if ($folder -cnotin @("raw", "lake", "warehouse", "exports")) {
    throw "Unknown folder $folder (raw, lake, warehouse or exports)"
  }
}
if (-not (Get-Command gcloud -ErrorAction SilentlyContinue)) {
  throw "Install the Google Cloud CLI and sign in with 'gcloud auth login' first."
}

# Partial downloads of a running crawl (objects/tmp/) and half-written lake files (*.partial) are not data. The
# patterns avoid backslashes and '|' so they pass unchanged through gcloud.cmd.
$exclude = '.*objects.tmp.*,.*[.]partial$'
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
