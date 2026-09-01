# hook-commit-on-write.ps1 - PostToolUse (Write|Edit): commit immediately after
# any memory write.
#
# This is the O2 fix. `receive.denyCurrentBranch=updateInstead` inspects the
# RECEIVING worktree, and the receiver is dirty exactly when it is mid-turn
# writing a memory - so an inbound peer push would be refused precisely when the
# peer is most active. Committing on write keeps the worktree clean between
# operations, so that window essentially closes.
#
# Cheap and silent by design: it must never slow down or interrupt a tool call.
# ASCII-only on purpose.
$ErrorActionPreference = "SilentlyContinue"

$canon = "$env:USERPROFILE\.claude\projects\C--WINDOWS-system32\memory"

foreach ($r in @("_shared", "_local")) {
    $repo = Join-Path $canon $r
    if (-not (Test-Path (Join-Path $repo ".git"))) { continue }
    git -C $repo add -A 2>$null
    git -C $repo diff --cached --quiet 2>$null
    if ($LASTEXITCODE -ne 0) {
        git -C $repo commit -qm "memory: autocommit on write" 2>$null
    }
}

exit 0
