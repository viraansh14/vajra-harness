# hook-pull.ps1 - SessionStart: refresh memory from the peer, then regenerate
# the index.
#
# MUST FAIL OPEN. A sync fault can never be allowed to block a session from
# starting, so every network call is bounded and the script always exits 0.
# ASCII-only: PS 5.1 parses BOM-less files as ANSI and curly quotes break strings.
$ErrorActionPreference = "SilentlyContinue"

$canon = "$env:USERPROFILE\.claude\projects\C--WINDOWS-system32\memory"
$py    = "$env:USERPROFILE\vajra-memory-sync\.venv\Scripts\python.exe"
$stat  = "$env:USERPROFILE\.local\memory-sync\memory-sync.json"

foreach ($r in @("_shared", "_local")) {
    $repo = Join-Path $canon $r
    if (-not (Test-Path (Join-Path $repo ".git"))) { continue }
    # Bounded pull: a wedged ssh must not hang session start.
    $job = Start-Job -ScriptBlock {
        param($p)
        git -C $p pull --rebase peer main 2>&1
    } -ArgumentList $repo
    if (Wait-Job $job -Timeout 20) { Receive-Job $job | Out-Null } else { Stop-Job $job -Force }
    Remove-Job $job -Force
}

if (Test-Path $py) {
    & $py -m memory_sync.cli index --root $canon --machine winpc --status-file $stat 2>&1 | Out-Null
}

exit 0
