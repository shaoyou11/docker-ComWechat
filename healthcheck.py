#!/usr/bin/python3
import json
import os
import sys
from urllib import request


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


def main():
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
