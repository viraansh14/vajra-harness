#!/bin/sh
# claudebus UserPromptSubmit hook - V3 shim (see hook-join.sh for why).
exec python3 "$HOME/.claude/claudebus/hook.py" prompt
