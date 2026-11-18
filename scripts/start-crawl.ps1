# Windows: start a detached, resumable crawl of one site. It keeps running after this shell closes; progress goes to
# <data root>/logs/<site>.log and `uv run ssp status <site>` reports counts. scripts/stop-crawl.ps1 stops it; starting
# again resumes where it stopped.
#
# The crawl runs from its own environment in .runtime/ (the crawler's three dependencies and a snapshot of this
# repository), so the development environment can add or upgrade packages while a crawl that takes days keeps going.
# -Refresh rebuilds that snapshot from the working tree; it refuses while any crawl is running.
param(
  [Parameter(Mandatory = $true)][ValidateSet("osha", "msha", "arlweb")][string]$Site,
  [switch]$Refresh
)

$repo = Split-Path -Parent $PSScriptRoot
$runtime = Join-Path $repo ".runtime"
$python = Join-Path $runtime "Scripts/python.exe"
$all = @(Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -like "*safestart_pipeline.cli crawl*" })

if ($Refresh -or -not (Test-Path -LiteralPath $python)) {
  if ($all.Count -gt 0) { throw "Stop the running crawls before refreshing the crawler environment." }
  if (-not (Test-Path -LiteralPath $python)) { uv venv --quiet --python 3.13 $runtime; if ($LASTEXITCODE) { throw "uv venv failed." } }
  uv pip install --quiet --python $python --reinstall-package safestart-data-pipeline $repo
  if ($LASTEXITCODE) { throw "Installing the crawler snapshot failed." }
}

$running = @($all | Where-Object { $_.CommandLine -like "*crawl $Site*" })
if ($running.Count -gt 0) {
  "A $Site crawl is already running (process $($running[0].ProcessId))."
  return
}

# Created through WMI so the crawl does not belong to this shell and survives it, with its console window hidden
# (progress is in the log file). The working directory is the repository, where pipeline.local.toml names the data root.
$startup = New-CimInstance -ClassName Win32_ProcessStartup -ClientOnly -Property @{ ShowWindow = [uint16]0 }
$result = Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments @{
  CommandLine               = "`"$python`" -m safestart_pipeline.cli crawl $Site"
  CurrentDirectory          = $repo
  ProcessStartupInformation = $startup
}
if ($result.ReturnValue -ne 0) { throw "Could not start the crawl (Win32_Process.Create returned $($result.ReturnValue))." }
"Started the $Site crawl as process $($result.ProcessId)."
