"""Provide the private system and session buses required by Wine."""

import os
import subprocess
from pathlib import Path


BUSES = {
    "system": "/run/dbus/system_bus_socket",
    "session": "/run/comwechat/session_bus_socket",
}


def bus_ready(kind):
    try:
        result = subprocess.run(
            ["dbus-send", "--bus=unix:path=" + BUSES[kind],
             "--print-reply", "--reply-timeout=1500",
             "--dest=org.freedesktop.DBus", "/", "org.freedesktop.DBus.ListNames"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=2,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return result.returncode == 0


def ensure_dbus():
    for kind, socket in BUSES.items():
        os.environ[f"DBUS_{kind.upper()}_BUS_ADDRESS"] = "unix:path=" + socket
        if bus_ready(kind):
            continue
        Path(socket).parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(
            ["dbus-daemon", "--" + kind, "--fork", "--nopidfile",
             "--address=unix:path=" + socket],
            check=True, timeout=10,
        )
        if not bus_ready(kind):
            raise RuntimeError(f"D-Bus {kind} bus did not become ready")


if __name__ == "__main__":
    ensure_dbus()
