# Windows: stop a crawl started by start-crawl.ps1. The state is committed after every URL, so at most the URL in
# flight is fetched again on the next start.
param([Parameter(Mandatory = $true)][ValidateSet("osha", "msha")][string]$Site)

$targets = @(Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -like "*ssp*crawl $Site*" })
if (-not $targets) {
  "No $Site crawl is running."
  return
}
foreach ($process in $targets) {
  Stop-Process -Id $process.ProcessId -ErrorAction SilentlyContinue
  "Stopped process $($process.ProcessId) ($($process.Name))."
}
