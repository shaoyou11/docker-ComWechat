#!/usr/bin/python3
import json
import os
import sys
from urllib import request


CORE_PROCESS_LIMITS = {
    "WeChat.exe": 1,
    "WeChatHook.exe": 1,
    "explorer.exe": 1,
    "winedevice.exe": 2,
    "wineserver": 1,
}


def bridge_enabled():
    return os.environ.get("COMWECHAT_BRIDGE_ENABLED", "false").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def check_url(url):
    with request.urlopen(url, timeout=3) as response:
        payload = json.loads(response.read().decode("utf-8"))
    return response.status == 200 and isinstance(payload, dict)


def process_state(pid):
    try:
        stat = open(f"/proc/{pid}/stat", encoding="utf-8").read()
    except OSError:
        return None
    end = stat.rfind(")")
    return stat[end + 2 : end + 3] if end >= 0 else None


def core_processes_are_sane():
    counts = {name: 0 for name in CORE_PROCESS_LIMITS}
    try:
        entries = os.listdir("/proc")
    except OSError:
        return False
    for entry in entries:
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/comm", encoding="utf-8") as source:
                name = source.read().strip()
        except OSError:
            continue
        if name not in counts:
            continue
        state = process_state(entry)
        if state == "D":
            print(f"healthcheck failed: {name}[{entry}] is uninterruptible", file=sys.stderr)
            return False
        if state != "Z":
            counts[name] += 1
    for name, limit in CORE_PROCESS_LIMITS.items():
        if counts[name] > limit:
            print(
                f"healthcheck failed: duplicate {name} processes ({counts[name]})",
                file=sys.stderr,
            )
            return False
    return True


def main():
    if not core_processes_are_sane():
        return 1
    if bridge_enabled():
        port = os.environ.get("COMWECHAT_BRIDGE_API_PORT", "19088")
        ok = check_url(f"http://127.0.0.1:{port}/healthz")
    else:
        port = os.environ.get("COMWECHAT_API_PORT", "18888")
        ok = check_url(f"http://127.0.0.1:{port}/api/?type=0")
    return 0 if ok else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"healthcheck failed: {error}", file=sys.stderr)
        raise SystemExit(1)
