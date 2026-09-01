#!/usr/bin/env python3
"""Idempotently install the Codex lifecycle side of Claude Bus v5."""

import argparse
import json
import os
import shlex
import sys
import tempfile


DEFAULT_HOOKS = os.path.expanduser("~/.codex/hooks.json")
DEFAULT_BRIDGE = os.path.expanduser("~/.claude/claudebus/codex_hook.py")
EVENTS = (
    "SessionStart", "UserPromptSubmit", "PostToolUse",
    "SubagentStart", "SubagentStop", "Stop",
)


def command_for(bridge, event):
    return " ".join((shlex.quote(sys.executable), shlex.quote(bridge), shlex.quote(event)))


def install_document(doc, bridge):
    hooks = doc.setdefault("hooks", {})
    changed = []
    for event in EVENTS:
        groups = hooks.setdefault(event, [])
        command = command_for(bridge, event)
        present = any(
            item.get("command") == command
            for group in groups
            for item in group.get("hooks", [])
            if item.get("type") == "command"
        )
        if present:
            continue
        groups.append({
            "hooks": [{
                "type": "command",
                "command": command,
                "timeout": 8,
                "statusMessage": "Synchronizing cross-provider session bus...",
            }]
        })
        changed.append(event)
    return changed


def uninstall_document(doc, bridge, owned_events):
    """Remove only definitions this installer recorded as newly added."""
    hooks = doc.get("hooks", {})
    changed = []
    for event in owned_events:
        groups = hooks.get(event, [])
        command = command_for(bridge, event)
        kept_groups = []
        removed = False
        for group in groups:
            items = group.get("hooks", [])
            kept_items = [
                item for item in items
                if not (item.get("type") == "command" and item.get("command") == command)
            ]
            removed = removed or len(kept_items) != len(items)
            if kept_items:
                updated = dict(group)
                updated["hooks"] = kept_items
                kept_groups.append(updated)
        if removed:
            hooks[event] = kept_groups
            changed.append(event)
    return changed


def load(path):
    if not os.path.exists(path):
        return {"hooks": {}}
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def write_atomic(path, doc):
    directory = os.path.dirname(path)
    os.makedirs(directory, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix="hooks.", suffix=".json", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(doc, handle, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def state_path_for(hooks_path):
    return os.path.join(os.path.dirname(os.path.abspath(hooks_path)),
                        ".claudebus-v5-hooks-state.json")


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--hooks", default=DEFAULT_HOOKS)
    parser.add_argument("--bridge", default=DEFAULT_BRIDGE)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--uninstall", action="store_true")
    parser.add_argument("--state")
    args = parser.parse_args(argv)
    bridge = os.path.abspath(args.bridge)
    hooks_path = os.path.abspath(args.hooks)
    state_path = os.path.abspath(args.state or state_path_for(hooks_path))
    bridge_error = None
    if not args.uninstall and not os.path.isfile(bridge):
        bridge_error = "bridge-not-found"
    elif not args.uninstall and not os.access(bridge, os.R_OK):
        bridge_error = "bridge-not-readable"
    elif not args.uninstall:
        try:
            with open(bridge, encoding="utf-8") as handle:
                compile(handle.read(), bridge, "exec")
        except (OSError, SyntaxError) as exc:
            bridge_error = f"bridge-invalid:{type(exc).__name__}"
    if bridge_error:
        print(json.dumps({
            "ok": False, "changed": [], "hooks": args.hooks,
            "bridge": bridge, "bridge_error": bridge_error,
            "requires_trust": False,
        }, sort_keys=True))
        return 2
    doc = load(hooks_path)
    action = "uninstall" if args.uninstall else "install"
    if args.uninstall:
        state = load(state_path) if os.path.exists(state_path) else {}
        matching_state = (
            state.get("format") == 1
            and state.get("hooks") == hooks_path
            and state.get("bridge") == bridge
        )
        owned_events = state.get("added_events", []) if matching_state else []
        changed = uninstall_document(doc, bridge, owned_events)
        if changed and not args.check:
            write_atomic(hooks_path, doc)
        if not args.check and matching_state and os.path.exists(state_path):
            os.unlink(state_path)
    else:
        changed = install_document(doc, bridge)
        if changed and not args.check:
            write_atomic(hooks_path, doc)
            prior = load(state_path) if os.path.exists(state_path) else {}
            prior_owned = prior.get("added_events", []) if (
                prior.get("format") == 1
                and prior.get("hooks") == hooks_path
                and prior.get("bridge") == bridge
            ) else []
            write_atomic(state_path, {
                "format": 1,
                "hooks": hooks_path,
                "bridge": bridge,
                "added_events": sorted(set(prior_owned) | set(changed)),
            })
    print(json.dumps({
        "ok": not (args.check and changed),
        "action": action,
        "changed": changed,
        "hooks": args.hooks,
        "bridge": bridge,
        "bridge_error": None,
        "state": state_path,
        "requires_trust": bool(changed),
        "trust_status": ("review-required-for-new-or-changed-hooks" if changed
                         else "not-verified-by-installer"),
        "trust_action": "Open /hooks in Codex and trust the exact definitions",
        "vetted_automation_bypass": "--dangerously-bypass-hook-trust",
    }, sort_keys=True))
    return 1 if args.check and changed else 0


if __name__ == "__main__":
    raise SystemExit(main())
