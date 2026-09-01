# reconcile.ps1 - the convergence guarantee.
#
# Runs every 30 minutes regardless of whether any session exists. This is the
# piece that makes "always synced" actually true: it catches sessions killed
# mid-turn, pushes that failed because the peer was asleep, edits made outside
# a session, and multi-day blackouts like 2026-08-25 to 08-31.
#
# THE VERDICT COMES FROM THIS RUN, NEVER FROM THE STATUS FILE ALONE.
# On 2026-09-01 an earlier version read a status file left by a previous manual
# run and cheerfully reported OK while the sync it had just attempted failed
# outright. A stale verdict is indistinguishable from a fresh one unless you
# check, so this checks: the status must be newer than this run started, or it
# is treated as evidence of nothing.
# ASCII-only on purpose.
$ErrorActionPreference = "SilentlyContinue"

$canon  = "$env:USERPROFILE\.claude\projects\C--WINDOWS-system32\memory"
$py     = "$env:USERPROFILE\vajra-memory-sync\.venv\Scripts\python.exe"
$toast  = "$env:USERPROFILE\vajra-memory-sync\scripts\toast.ps1"
$stat   = "$env:USERPROFILE\.local\memory-sync\memory-sync.json"
$log    = "$env:USERPROFILE\.local\memory-sync\memory-sync.log"
$prevF  = "$env:USERPROFILE\.local\memory-sync\memory-sync.prev"
$started = Get-Date
$stamp   = $started.ToString("yyyy-MM-dd HH:mm:ss")

if (-not (Test-Path $py)) {
    Add-Content -Path $log -Value "$stamp BAD venv missing at $py"
    exit 0
}

$out = & $py -m memory_sync.cli sync --root $canon --machine winpc --status-file $stat 2>&1
$rc = $LASTEXITCODE
$outText = ($out | Out-String).Trim()

# Derive the verdict from evidence produced BY THIS RUN.
$verdict = "BAD"
$detail = $outText
try {
    $j = Get-Content $stat -Raw -ErrorAction Stop | ConvertFrom-Json
    $written = [datetime]::ParseExact($j.t, "yyyy-MM-dd HH:mm:ss", $null)
    if ($written -ge $started.AddSeconds(-5)) {
        $verdict = $j.verdict
        $detail = "$($j.checks.completeness.detail); $($j.checks.lint.detail)"
    } else {
        $verdict = "BAD"
        $detail = "status file is STALE (written $($j.t), run started $stamp) - the sync did not complete: $outText"
    }
} catch {
    $verdict = "BAD"
    $detail = "no usable status written this run: $outText"
}

Add-Content -Path $log -Value "$stamp rc=$rc verdict=$verdict $detail"

# Toast only on a state change, so a healthy link stays quiet.
if (Test-Path $prevF) { $prev = (Get-Content $prevF -TotalCount 1).Trim() } else { $prev = "" }
if ($verdict -ne $prev) {
    [System.IO.File]::WriteAllText($prevF, $verdict, (New-Object System.Text.UTF8Encoding($false)))
    Add-Content -Path $log -Value "$stamp STATE-CHANGE '$prev' -> '$verdict'"
    $short = $detail
    if ($short.Length -gt 160) { $short = $short.Substring(0, 160) }
    # Windows PowerShell, not pwsh 7: only 5.1 can project the WinRT toast types.
    $r = & "$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe" `
            -NonInteractive -ExecutionPolicy Bypass -File $toast `
            -Title "VAJRA memory sync: $verdict" -Message $short 2>&1
    Add-Content -Path $log -Value "$stamp toast: $($r | Out-String).Trim()"
}
