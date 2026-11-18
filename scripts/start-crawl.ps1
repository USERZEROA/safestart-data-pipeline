# Windows: start a detached, resumable crawl of one site. It keeps running after this shell closes; progress goes to
# <data root>/logs/<site>.log and `uv run ssp status <site>` reports counts. scripts/stop-crawl.ps1 stops it; starting
# again resumes where it stopped.
param([Parameter(Mandatory = $true)][ValidateSet("osha", "msha")][string]$Site)

$repo = Split-Path -Parent $PSScriptRoot
$ssp = Join-Path $repo ".venv/Scripts/ssp.exe"
if (-not (Test-Path -LiteralPath $ssp)) { throw "Run 'uv sync' in the repository first." }

$running = Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -like "*ssp*crawl $Site*" }
if ($running) {
  "A $Site crawl is already running (process $(@($running)[0].ProcessId))."
  return
}

# Created through WMI so the crawl does not belong to this shell and survives it.
$result = Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments @{
  CommandLine      = "`"$ssp`" crawl $Site"
  CurrentDirectory = $repo
}
if ($result.ReturnValue -ne 0) { throw "Could not start the crawl (Win32_Process.Create returned $($result.ReturnValue))." }
"Started the $Site crawl as process $($result.ProcessId)."
