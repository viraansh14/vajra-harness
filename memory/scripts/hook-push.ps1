# hook-push.ps1 - Stop / SessionEnd: commit what the session learned and push it
# to the peer, then regenerate the index.
#
# Best effort by design: the session is ending, so a failed push must not error
# out. It is recorded in the status file instead, and the 30-minute reconciler
# is what guarantees eventual convergence.
# ASCII-only on purpose.
$ErrorActionPreference = "SilentlyContinue"

$canon = "$env:USERPROFILE\.claude\projects\C--WINDOWS-system32\memory"
$py    = "$env:USERPROFILE\vajra-memory-sync\.venv\Scripts\python.exe"
$stat  = "$env:USERPROFILE\.local\memory-sync\memory-sync.json"

if (Test-Path $py) {
    $job = Start-Job -ScriptBlock {
        param($p, $c, $s)
        & $p -m memory_sync.cli sync --root $c --machine winpc --status-file $s 2>&1
    } -ArgumentList $py, $canon, $stat
    if (Wait-Job $job -Timeout 90) { Receive-Job $job | Out-Null } else { Stop-Job $job -Force }
    Remove-Job $job -Force
}

exit 0
