#!/usr/bin/python3
import datetime
import heapq
import json
import logging
import os
import socketserver
import threading
import time
from collections import deque
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib import request
from urllib.error import URLError, HTTPError
from typing import Any, Dict, Optional, Tuple


LOGGER = logging.getLogger("comwechat_bridge")
if not LOGGER.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name, str(default))
    try:
        return int(value)
    except ValueError:
        LOGGER.warning("Invalid integer env %s=%s, fallback=%s", name, value, default)
        return default


def _env_float(name: str, default: float) -> float:
    value = os.environ.get(name, str(default))
    try:
        return float(value)
    except ValueError:
        LOGGER.warning("Invalid float env %s=%s, fallback=%s", name, value, default)
        return default


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in ("1", "true", "yes", "on")


def extract_sort_ts(msg: Dict[str, Any], received_ts: float) -> float:
    timestamp = msg.get("timestamp")
    if isinstance(timestamp, (int, float)):
        return float(timestamp)
    if isinstance(timestamp, str):
        try:
            return float(timestamp)
        except ValueError:
            pass

    text_time = msg.get("time")
    if isinstance(text_time, str):
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
            try:
                dt = datetime.datetime.strptime(text_time, fmt)
                return dt.timestamp()
            except ValueError:
                continue
        LOGGER.debug("Failed to parse msg['time']=%s", text_time)

    return float(received_ts)


def is_fast_path(msg: Dict[str, Any]) -> bool:
    return msg.get("isSendMsg") == 1 and msg.get("isSendByPhone") == 0


def is_login_reorder_anchor(msg: Dict[str, Any]) -> bool:
    message = msg.get("message")
    return isinstance(message, str) and '<sysmsg type="SafeModuleCfg"' in message


@dataclass
class BridgeConfig:
    enabled: bool
    ingress_host: str
    ingress_port: int
    api_host: str
    api_port: int
    comwechat_api_port: int
    hook_save_path: str
    boot_reorder_window_seconds: int
    login_anchor_probe_window_seconds: float
    login_state_probe_interval_seconds: float
    consume_rate_per_sec: float
    max_buffer: int
    hook_retry_times: int
    hook_retry_interval_seconds: float
    metrics_interval_seconds: int

    @classmethod
    def from_env(cls) -> "BridgeConfig":
        return cls(
            enabled=_env_bool("COMWECHAT_BRIDGE_ENABLED", False),
            ingress_host=os.environ.get("COMWECHAT_BRIDGE_IN_HOST", "0.0.0.0"),
            ingress_port=_env_int("COMWECHAT_BRIDGE_IN_PORT", 23456),
            api_host=os.environ.get("COMWECHAT_BRIDGE_API_HOST", "0.0.0.0"),
            api_port=_env_int("COMWECHAT_BRIDGE_API_PORT", 19088),
            comwechat_api_port=_env_int("COMWECHAT_API_PORT", 18888),
            hook_save_path=os.environ.get(
                "COMWECHAT_HOOK_SAVE_PATH", "C:\\Users\\user\\My Documents\\WeChat Files"
            ),
            boot_reorder_window_seconds=_env_int("COMWECHAT_BOOT_REORDER_WINDOW_SECONDS", 30),
            login_anchor_probe_window_seconds=max(
                0.0, _env_float("COMWECHAT_LOGIN_ANCHOR_PROBE_WINDOW_SECONDS", 1.0)
            ),
            login_state_probe_interval_seconds=max(
                0.1, _env_float("COMWECHAT_LOGIN_STATE_PROBE_INTERVAL_SECONDS", 1.0)
            ),
            consume_rate_per_sec=max(0.1, _env_float("COMWECHAT_CONSUME_RATE_PER_SEC", 5.0)),
            max_buffer=max(100, _env_int("COMWECHAT_BRIDGE_MAX_BUFFER", 20000)),
            hook_retry_times=max(1, _env_int("COMWECHAT_HOOK_RETRY_TIMES", 20)),
            hook_retry_interval_seconds=max(
                0.1, _env_float("COMWECHAT_HOOK_RETRY_INTERVAL_SECONDS", 1.0)
            ),
            metrics_interval_seconds=max(
                1, _env_int("COMWECHAT_BRIDGE_METRICS_INTERVAL_SECONDS", 10)
            ),
        )


class MessageBuffer:
    def __init__(self, config: BridgeConfig):
        self.config = config
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)

        self._reorder_started_at: Optional[float] = None
        self._reorder_active = False
        self._probe_started_at: Optional[float] = None
        self._seq = 0

        self._reorder_heap: list[Tuple[float, int, Dict[str, Any]]] = []
        self._probe_queue: deque[Tuple[float, int, Dict[str, Any]]] = deque()
        self._normal_queue: deque[Dict[str, Any]] = deque()
        self._ready_queue: deque[Dict[str, Any]] = deque()

        self._stats: Dict[str, int] = {
            "ingress_total": 0,
            "ready_total": 0,
            "pulled_total": 0,
            "fast_path_total": 0,
            "login_anchor_total": 0,
            "probe_timeout_total": 0,
            "reordered_total": 0,
            "overflow_drop_total": 0,
        }

    def _phase_locked(self) -> str:
        if self._probe_started_at is not None:
            return "login_probe"
        return "login_reordering" if self._reorder_active else "steady"

    def phase(self) -> str:
        with self._lock:
            return self._phase_locked()

    def queue_size(self) -> int:
        with self._lock:
            return (
                len(self._reorder_heap)
                + len(self._probe_queue)
                + len(self._normal_queue)
                + len(self._ready_queue)
            )

    def ingest(self, msg: Dict[str, Any]) -> None:
        received_ts = time.time()
        ingress_ts = time.monotonic()
        with self._cond:
            self._stats["ingress_total"] += 1
            self._flush_probe_if_expired_locked(ingress_ts)
            if is_fast_path(msg):
                self._ready_queue.appendleft(msg)
                self._stats["fast_path_total"] += 1
                self._stats["ready_total"] += 1
            elif is_login_reorder_anchor(msg):
                self._close_reorder_window_locked("reset")
                self._move_probe_into_reorder_locked()
                self._ready_queue.append(msg)
                self._stats["login_anchor_total"] += 1
                self._stats["ready_total"] += 1
                self._open_reorder_window_locked(ingress_ts)
            elif self._reorder_active:
                sort_ts = extract_sort_ts(msg, received_ts)
                heapq.heappush(self._reorder_heap, (sort_ts, self._seq, msg))
                self._seq += 1
            elif self._probe_started_at is not None:
                sort_ts = extract_sort_ts(msg, received_ts)
                self._probe_queue.append((sort_ts, self._seq, msg))
                self._seq += 1
            else:
                self._normal_queue.append(msg)

            self._drop_if_overflow_locked()
            self._cond.notify_all()

    def _open_reorder_window_locked(self, started_at: float) -> None:
        self._reorder_started_at = started_at
        self._reorder_active = True
        LOGGER.info(
            "Bridge login reorder window opened for %s seconds.",
            self.config.boot_reorder_window_seconds,
        )

    def begin_login_anchor_probe(self) -> bool:
        if self.config.login_anchor_probe_window_seconds <= 0:
            return False
        with self._cond:
            if self._probe_started_at is not None:
                return False
            self._probe_started_at = time.monotonic()
            LOGGER.info(
                "Bridge login anchor probe opened for %.1f seconds after login-state probe confirmed login.",
                self.config.login_anchor_probe_window_seconds,
            )
            self._cond.notify_all()
            return True

    def _move_probe_into_reorder_locked(self) -> None:
        while self._probe_queue:
            sort_ts, seq, msg = self._probe_queue.popleft()
            heapq.heappush(self._reorder_heap, (sort_ts, seq, msg))
        self._probe_started_at = None

    def _flush_probe_if_expired_locked(self, now: float) -> None:
        if self._probe_started_at is None:
            return
        elapsed = now - self._probe_started_at
        if elapsed < self.config.login_anchor_probe_window_seconds:
            return

        moved = 0
        while self._probe_queue:
            _, _, msg = self._probe_queue.popleft()
            self._normal_queue.append(msg)
            moved += 1
        self._probe_started_at = None
        self._stats["probe_timeout_total"] += moved
        LOGGER.info("Bridge login anchor probe expired, released=%s messages.", moved)

    def _close_reorder_window_locked(self, reason: str) -> int:
        if not self._reorder_active:
            return 0

        moved = 0
        while self._reorder_heap:
            _, _, msg = heapq.heappop(self._reorder_heap)
            self._normal_queue.append(msg)
            moved += 1

        self._reorder_started_at = None
        self._reorder_active = False
        self._stats["reordered_total"] += moved
        LOGGER.info("Bridge login reorder window %s, moved=%s messages.", reason, moved)
        return moved

    def _drop_if_overflow_locked(self) -> None:
        while (
            len(self._reorder_heap)
            + len(self._probe_queue)
            + len(self._normal_queue)
            + len(self._ready_queue)
        ) > self.config.max_buffer:
            dropped = None
            if self._normal_queue:
                dropped = self._normal_queue.popleft()
            elif self._probe_queue:
                _, _, dropped = self._probe_queue.popleft()
            elif self._reorder_heap:
                _, _, dropped = heapq.heappop(self._reorder_heap)
            elif self._ready_queue:
                dropped = self._ready_queue.popleft()
            if dropped is None:
                break
            self._stats["overflow_drop_total"] += 1

    def maybe_flush_reorder(self) -> None:
        with self._cond:
            now = time.monotonic()
            self._flush_probe_if_expired_locked(now)
            if not self._reorder_active or self._reorder_started_at is None:
                return
            elapsed = now - self._reorder_started_at
            if elapsed < self.config.boot_reorder_window_seconds:
                return
            self._close_reorder_window_locked("closed")
            self._cond.notify_all()

    def emit_ready(self, limit: int) -> int:
        moved = 0
        with self._cond:
            while self._normal_queue and moved < limit:
                self._ready_queue.append(self._normal_queue.popleft())
                moved += 1
                self._stats["ready_total"] += 1
            if moved:
                self._cond.notify_all()
        return moved

    def pull(self, max_items: int, wait_ms: int) -> Dict[str, Any]:
        timeout = max(0, wait_ms) / 1000.0
        deadline = time.monotonic() + timeout
        with self._cond:
            while not self._ready_queue and timeout > 0:
                self._cond.wait(timeout=timeout)
                timeout = deadline - time.monotonic()

            items = []
            for _ in range(max(1, max_items)):
                if not self._ready_queue:
                    break
                items.append(self._ready_queue.popleft())
            self._stats["pulled_total"] += len(items)
            return {
                "messages": items,
                "queue_size": len(self._reorder_heap)
                + len(self._probe_queue)
                + len(self._normal_queue)
                + len(self._ready_queue),
                "phase": self._phase_locked(),
            }

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "phase": self._phase_locked(),
                "probe_queue_size": len(self._probe_queue),
                "reorder_queue_size": len(self._reorder_heap),
                "normal_queue_size": len(self._normal_queue),
                "ready_queue_size": len(self._ready_queue),
                "ingress_total": self._stats["ingress_total"],
                "ready_total": self._stats["ready_total"],
                "pulled_total": self._stats["pulled_total"],
                "fast_path_total": self._stats["fast_path_total"],
                "login_anchor_total": self._stats["login_anchor_total"],
                "probe_timeout_total": self._stats["probe_timeout_total"],
                "reordered_total": self._stats["reordered_total"],
                "overflow_drop_total": self._stats["overflow_drop_total"],
            }


class _ThreadingIngressServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True


class IngressSocketServer:
    def __init__(self, config: BridgeConfig, buffer: MessageBuffer):
        self.config = config
        self.buffer = buffer
        self.server: Optional[_ThreadingIngressServer] = None
        self.thread: Optional[threading.Thread] = None

    def start(self) -> None:
        class BridgeHandler(socketserver.BaseRequestHandler):
            def handle(inner_self):
                bridge_server = inner_self.server.bridge_server  # type: ignore[attr-defined]
                conn = inner_self.request
                pending = b""
                while True:
                    try:
                        chunk = conn.recv(4096)
                    except OSError:
                        break
                    if not chunk:
                        break
                    pending += chunk
                    while b"\n" in pending:
                        line, pending = pending.split(b"\n", 1)
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            msg = json.loads(line.decode("utf-8"))
                        except (UnicodeDecodeError, json.JSONDecodeError):
                            LOGGER.warning("Invalid ingress payload dropped.")
                            continue
                        bridge_server.buffer.ingest(msg)
                        try:
                            conn.sendall(b"200 OK")
                        except OSError:
                            break
                try:
                    conn.close()
                except OSError:
                    pass

        addr = (self.config.ingress_host, self.config.ingress_port)
        self.server = _ThreadingIngressServer(addr, BridgeHandler)
        self.server.bridge_server = self  # type: ignore[attr-defined]
        self.thread = threading.Thread(
            target=self.server.serve_forever,
            kwargs={"poll_interval": 0.5},
            daemon=True,
            name="bridge-ingress-server",
        )
        self.thread.start()
        LOGGER.info("Bridge ingress listening on %s:%s", self.config.ingress_host, self.config.ingress_port)

    def stop(self) -> None:
        if self.server is not None:
            self.server.shutdown()
            self.server.server_close()
        if self.thread is not None:
            self.thread.join(timeout=1.5)


class BridgeApiServer:
    def __init__(self, config: BridgeConfig, buffer: MessageBuffer, state: Dict[str, Any]):
        self.config = config
        self.buffer = buffer
        self.state = state
        self.httpd: Optional[ThreadingHTTPServer] = None
        self.thread: Optional[threading.Thread] = None

    def start(self) -> None:
        api_server = self

        class Handler(BaseHTTPRequestHandler):
            def _send_json(inner_self, code: int, payload: Dict[str, Any]) -> None:
                data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                inner_self.send_response(code)
                inner_self.send_header("Content-Type", "application/json; charset=utf-8")
                inner_self.send_header("Content-Length", str(len(data)))
                inner_self.end_headers()
                inner_self.wfile.write(data)

            def do_GET(inner_self):
                if inner_self.path != "/healthz":
                    inner_self._send_json(404, {"ok": False, "error": "not_found"})
                    return
                queue_size = api_server.buffer.queue_size()
                inner_self._send_json(
                    200,
                    {
                        "ok": True,
                        "hooks_ready": bool(api_server.state.get("hooks_ready", False)),
                        "queue_size": queue_size,
                    },
                )

            def do_POST(inner_self):
                if inner_self.path != "/v1/messages/pull":
                    inner_self._send_json(404, {"ok": False, "error": "not_found"})
                    return

                try:
                    content_len = int(inner_self.headers.get("Content-Length", "0"))
                except ValueError:
                    content_len = 0

                body = inner_self.rfile.read(content_len) if content_len > 0 else b"{}"
                try:
                    payload = json.loads(body.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError):
                    inner_self._send_json(400, {"ok": False, "error": "invalid_json"})
                    return

                try:
                    max_items = int(payload.get("max_items", 50))
                    wait_ms = int(payload.get("wait_ms", 15000))
                except (TypeError, ValueError):
                    inner_self._send_json(400, {"ok": False, "error": "invalid_arguments"})
                    return
                result = api_server.buffer.pull(max_items=max_items, wait_ms=wait_ms)
                inner_self._send_json(200, result)

            def log_message(inner_self, format: str, *args: Any) -> None:
                LOGGER.debug("BridgeApiServer: " + format, *args)

        addr = (self.config.api_host, self.config.api_port)
        self.httpd = ThreadingHTTPServer(addr, Handler)
        self.thread = threading.Thread(
            target=self.httpd.serve_forever,
            kwargs={"poll_interval": 0.5},
            daemon=True,
            name="bridge-api-server",
        )
        self.thread.start()
        LOGGER.info("Bridge API listening on %s:%s", self.config.api_host, self.config.api_port)

    def stop(self) -> None:
        if self.httpd is not None:
            self.httpd.shutdown()
            self.httpd.server_close()
        if self.thread is not None:
            self.thread.join(timeout=1.5)


class BridgeService:
    def __init__(self, config: BridgeConfig):
        self.config = config
        self.buffer = MessageBuffer(config)
        self.state: Dict[str, Any] = {"hooks_ready": False, "is_login": None}

        self.ingress = IngressSocketServer(config, self.buffer)
        self.api = BridgeApiServer(config, self.buffer, self.state)
        self.stop_event = threading.Event()
        self.threads: list[threading.Thread] = []

    def _wechat_post(self, hook_type: int, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"http://127.0.0.1:{self.config.comwechat_api_port}/api/?type={hook_type}"
        data = json.dumps(payload).encode("utf-8")
        req = request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with request.urlopen(req, timeout=2) as resp:
                content = resp.read().decode("utf-8")
        except (URLError, HTTPError) as e:
            raise RuntimeError(f"ComWeChat API request failed: {e}") from e
        return json.loads(content, strict=False)

    def _wechat_get(self, hook_type: int) -> Dict[str, Any]:
        url = f"http://127.0.0.1:{self.config.comwechat_api_port}/api/?type={hook_type}"
        req = request.Request(url, method="GET")
        try:
            with request.urlopen(req, timeout=2) as resp:
                content = resp.read().decode("utf-8")
        except (URLError, HTTPError) as e:
            raise RuntimeError(f"ComWeChat API request failed: {e}") from e
        return json.loads(content, strict=False)

    def _start_hooks(self) -> None:
        hooks = [
            (9, {"port": self.config.ingress_port}, "StartMsgHook"),
            (11, {"save_path": self.config.hook_save_path}, "StartImageHook"),
            (13, {"save_path": self.config.hook_save_path}, "StartVoiceHook"),
        ]
        for hook_type, payload, name in hooks:
            success = False
            for attempt in range(1, self.config.hook_retry_times + 1):
                try:
                    result = self._wechat_post(hook_type, payload)
                    LOGGER.info("%s success on attempt %s: %s", name, attempt, result)
                    success = True
                    break
                except Exception as e:
                    LOGGER.warning(
                        "%s failed on attempt %s/%s: %s",
                        name,
                        attempt,
                        self.config.hook_retry_times,
                        e,
                    )
                    time.sleep(self.config.hook_retry_interval_seconds)
            if not success:
                raise RuntimeError(f"{name} failed after retries")

        self.state["hooks_ready"] = True

    def _reorder_worker(self) -> None:
        while not self.stop_event.is_set():
            self.buffer.maybe_flush_reorder()
            self.stop_event.wait(0.2)

    def _login_state_worker(self) -> None:
        last_is_login: Optional[bool] = None
        while not self.stop_event.is_set():
            try:
                payload = self._wechat_get(0)
                is_login = bool(payload.get("is_login"))
                self.state["is_login"] = is_login
                if last_is_login is False and is_login is True:
                    self.buffer.begin_login_anchor_probe()
                last_is_login = is_login
            except Exception as e:
                LOGGER.warning("Bridge login-state probe failed: %s", e)
            self.stop_event.wait(self.config.login_state_probe_interval_seconds)

    def _rate_worker(self) -> None:
        tick_seconds = 0.1
        batch_size = max(1, int(round(self.config.consume_rate_per_sec * tick_seconds)))
        interval = max(0.01, batch_size / self.config.consume_rate_per_sec)
        while not self.stop_event.is_set():
            moved = self.buffer.emit_ready(limit=batch_size)
            if moved == 0:
                self.stop_event.wait(0.05)
                continue
            self.stop_event.wait(interval)

    def _metrics_worker(self) -> None:
        while not self.stop_event.wait(self.config.metrics_interval_seconds):
            snap = self.buffer.snapshot()
            LOGGER.info(
                "Bridge stats phase=%s probe=%s reorder=%s normal=%s ready=%s ingress=%s ready_total=%s pulled=%s fast=%s anchors=%s probe_timeouts=%s reordered=%s dropped=%s hooks_ready=%s is_login=%s",
                snap["phase"],
                snap["probe_queue_size"],
                snap["reorder_queue_size"],
                snap["normal_queue_size"],
                snap["ready_queue_size"],
                snap["ingress_total"],
                snap["ready_total"],
                snap["pulled_total"],
                snap["fast_path_total"],
                snap["login_anchor_total"],
                snap["probe_timeout_total"],
                snap["reordered_total"],
                snap["overflow_drop_total"],
                self.state["hooks_ready"],
                self.state["is_login"],
            )

    def start(self) -> None:
        if not self.config.enabled:
            return
        self.ingress.start()
        self._start_hooks()
        self.api.start()

        self.threads = [
            threading.Thread(target=self._login_state_worker, daemon=True, name="bridge-login-state"),
            threading.Thread(target=self._reorder_worker, daemon=True, name="bridge-reorder"),
            threading.Thread(target=self._rate_worker, daemon=True, name="bridge-rate"),
            threading.Thread(target=self._metrics_worker, daemon=True, name="bridge-metrics"),
        ]
        for thread in self.threads:
            thread.start()
        LOGGER.info("Bridge service started.")

    def stop(self) -> None:
        self.stop_event.set()
        self.api.stop()
        self.ingress.stop()
        for thread in self.threads:
            thread.join(timeout=1.5)
        LOGGER.info("Bridge service stopped.")
