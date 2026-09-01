#!/bin/sh
# claudebus SessionStart hook - V3 shim.
# All hook logic now lives in hook.py (session-id identity; the old
# CLAUDEBUS_ID / CLAUDE_CODE_CHILD_SESSION env gates are gone: CC 2.1.x sets
# CLAUDE_CODE_CHILD_SESSION=1 for every subprocess including hooks, which had
# silently disabled this hook for all sessions). Kept as a shim for anything
# still wired to this path; settings.json points at hook.py directly.
exec python3 "$HOME/.claude/claudebus/hook.py" session-start
