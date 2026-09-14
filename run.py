#!/usr/bin/python3
import datetime
import json
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from comwechat_bridge import BridgeConfig, BridgeService


VERSION = os.environ.get("COMWECHAT_VERSION", "3.9.12.16")
VERSION_CHANGE_ATTEMPTS = int(os.environ.get("COMWECHAT_VERSION_CHANGE_ATTEMPTS", "20"))
VERSION_CHANGE_RETRY_SECONDS = int(os.environ.get("COMWECHAT_VERSION_CHANGE_RETRY_SECONDS", "2"))
VERSION_CHANGE_CONNECT_TIMEOUT_SECONDS = int(
    os.environ.get("COMWECHAT_VERSION_CHANGE_CONNECT_TIMEOUT_SECONDS", "3")
)
VERSION_CHANGE_MAX_TIME_SECONDS = int(
    os.environ.get("COMWECHAT_VERSION_CHANGE_MAX_TIME_SECONDS", "10")
)
VERSION_CHANGE_ENABLED = os.environ.get(
    "COMWECHAT_VERSION_CHANGE_ENABLED", "false"
).strip().lower() in {"1", "true", "yes", "on"}
CHILD_RECOVERY_ATTEMPTS = int(os.environ.get("COMWECHAT_CHILD_RECOVERY_ATTEMPTS", "3"))
CHILD_RECOVERY_BACKOFF_SECONDS = int(os.environ.get("COMWECHAT_CHILD_RECOVERY_BACKOFF_SECONDS", "5"))
CHILD_RECOVERY_RESET_SECONDS = int(os.environ.get("COMWECHAT_CHILD_RECOVERY_RESET_SECONDS", "0"))
CHILD_RECOVERY_STABLE_SECONDS = int(
    os.environ.get("COMWECHAT_CHILD_RECOVERY_STABLE_SECONDS", "300")
)
WINE_CLEANUP_TIMEOUT_SECONDS = int(
    os.environ.get("COMWECHAT_WINE_CLEANUP_TIMEOUT_SECONDS", "15")
)
CHILD_UNRESPONSIVE_SECONDS = int(
    os.environ.get("COMWECHAT_CHILD_UNRESPONSIVE_SECONDS", "30")
)
SUPERVISOR_CONTROL_PORT = int(
    os.environ.get("COMWECHAT_SUPERVISOR_CONTROL_PORT", "19089")
)
WINE_PROCESS_NAMES = {
    "WeChat.exe",
    "WeChatHook.exe",
    "explorer.exe",
    "plugplay.exe",
    "rpcss.exe",
    "services.exe",
    "svchost.exe",
    "wineboot.exe",
    "winedevice.exe",
    "wineserver",
}


class ChildProcessStopped(RuntimeError):
    def __init__(self, name, status):
        super().__init__(f"{name} process stopped with code {status}")
        self.name = name
        self.status = status


class WechatStackStartupFailed(RuntimeError):
    pass


class WechatStackRecoveryRequested(RuntimeError):
    pass


class WineSessionCleanupFailed(RuntimeError):
    pass


def recovery_failures_should_reset(now, last_failure):
    """Only reset the failure budget when an explicit positive window is set."""
    return (
        CHILD_RECOVERY_RESET_SECONDS > 0
        and now - last_failure >= CHILD_RECOVERY_RESET_SECONDS
    )


def recovery_failures_after_stable_run(failures, run_seconds, stable_seconds):
    if stable_seconds > 0 and run_seconds >= stable_seconds:
        return 0
    return failures


class DockerWechatHook:
    def __init__(self):
        self.vnc = None
        self.wechat = None
        self.reg_hook = None
        self.bridge = None
        self.exiting = False
        self.recovery_requested = threading.Event()
        self.supervisor_state = "starting"
        self.supervisor_server = None
        self.recovery_lock = threading.Lock()
        self.pending_recovery = None
        self.single_recovery_launch = False
        self.last_recovery_error = ""
        self.confirmed_online_since = None
        signal.signal(signal.SIGINT, self.now_exit)
        signal.signal(signal.SIGHUP, self.now_exit)
        signal.signal(signal.SIGTERM, self.now_exit)

    def now_exit(self, signum, frame):
        self.exit_container()

    def prepare(self):
        subprocess.run(["unzip", "-o", "-d", "comwechat", "comwechat.zip"], check=True)
        source = "/WeChatHook.exe"
        target = "/comwechat/http/WeChatHook.exe"
        if os.path.exists(source):
            subprocess.run(["cp", "-p", source, target], check=True)
        elif not os.path.exists(target):
            raise RuntimeError("WeChatHook.exe 不存在")

    def run_vnc(self):
        os.makedirs("/root/.vnc", mode=0o755, exist_ok=True)
        passwd_output = subprocess.run(
            ["/usr/bin/vncpasswd", "-f"],
            input=os.environ["VNCPASS"].encode(),
            capture_output=True,
            check=True,
        )
        with open("/root/.vnc/passwd", "wb") as passwd_file:
            passwd_file.write(passwd_output.stdout)
        os.chmod("/root/.vnc/passwd", 0o700)
        self.vnc = subprocess.Popen(
            [
                "/usr/bin/vncserver",
                "-localhost",
                "no",
                "-xstartup",
                "/usr/bin/openbox",
                ":5",
            ],
            start_new_session=True,
        )

    def run_wechat(self):
        self.wechat = subprocess.Popen(
            [
                "wine",
                "/home/user/.wine/drive_c/Program Files/Tencent/WeChat/WeChat.exe",
            ],
            start_new_session=True,
        )

    def manual_scan_active(self):
        path = Path(os.environ.get("COMWECHAT_MANUAL_LOGIN_SESSION_PATH",
                                   "/var/lib/wechat-session/manual-login-session.json"))
        if not path.parent.is_dir():
            return True  # No shared protection state: fail closed.
        try:
            return float(json.loads(path.read_text())["expires_at"]) > time.time()
        except FileNotFoundError:
            return False
        except (OSError, ValueError, TypeError, KeyError):
            return True

    def recovery_status(self):
        return {"ok": True, "state": self.supervisor_state,
                "recovery_protocol": 1, "last_error": self.last_recovery_error,
                "request_pending": self.pending_recovery is not None,
                "manual_retry_required": self.supervisor_state == "paused"}

    def accept_recovery(self, payload):
        source = payload.get("source")
        key = str(payload.get("request_id", ""))
        if source not in ("manual", "automatic") or not key or len(key) > 160:
            return 400, {"ok": False, "reason": "explicit_bounded_request_required"}
        with self.recovery_lock:
            if self.pending_recovery is not None or self.supervisor_state == "recovering":
                return 409, {"ok": False, "reason": "recovery_in_progress"}
            if source == "automatic" and self.manual_scan_active():
                return 409, {"ok": False, "reason": "manual_scan_active"}
            state = self.bridge.state if self.bridge is not None else {}
            if source == "manual":
                if self.supervisor_state != "paused":
                    return 409, {"ok": False, "reason": "manual_retry_only_when_paused"}
                if self._wine_processes():
                    return 409, {"ok": False, "reason": "previous_process_cleanup_incomplete"}
            elif (self.supervisor_state != "running" or state.get("is_login") is not False
                  or state.get("hooks_ready") is not True
                  or not payload.get("stack_generation")
                  or state.get("stack_generation") != payload.get("stack_generation")):
                return 409, {"ok": False, "reason": "offline_generation_not_confirmed"}
            ledger = Path(os.environ.get("COMWECHAT_RECOVERY_LEDGER_PATH",
                                         "/var/lib/comwechat-bridge/recovery-requests.json"))
            try:
                ids = json.loads(ledger.read_text()) if ledger.exists() else []
                if not isinstance(ids, list):
                    raise ValueError("invalid ledger")
                if key in ids:
                    return 200, {"ok": True, "accepted": False, "reason": "duplicate_request"}
                ledger.parent.mkdir(parents=True, exist_ok=True)
                temporary = ledger.with_suffix(".tmp")
                with temporary.open("w") as handle:
                    json.dump((ids + [key])[-100:], handle)
                    handle.flush()
                    os.fsync(handle.fileno())
                temporary.replace(ledger)
            except (OSError, ValueError):
                return 503, {"ok": False, "reason": "recovery_record_unavailable"}
            self.pending_recovery = dict(payload)
            self.recovery_requested.set()
            return 202, {"ok": True, "accepted": True, "recovery_protocol": 1}

    def consume_recovery(self):
        with self.recovery_lock:
            payload = self.pending_recovery
            self.pending_recovery = None
            self.recovery_requested.clear()
            if not payload:
                return False
            previous = self.supervisor_state
            self.supervisor_state = "recovering"  # Block new QR generation first.
            state = self.bridge.state if self.bridge is not None else {}
            if payload["source"] == "automatic" and (self.manual_scan_active() or (
                state.get("is_login") is not False or
                state.get("stack_generation") != payload.get("stack_generation")
            )):
                self.supervisor_state = previous
                return False
            self.single_recovery_launch = True
            self.confirmed_online_since = None
            self.last_recovery_error = ""
            return True

    def start_supervisor_server(self):
        owner = self

        class SupervisorHandler(BaseHTTPRequestHandler):
            def _send_json(self, status, payload):
                body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                if self.path != "/healthz":
                    self.send_error(404)
                    return
                self._send_json(200, owner.recovery_status())

            def do_POST(self):
                if self.path != "/recover":
                    self.send_error(404)
                    return
                try:
                    size = int(self.headers.get("Content-Length", "0"))
                    if not 0 < size <= 2048:
                        raise ValueError("invalid size")
                    payload = json.loads(self.rfile.read(size))
                    if not isinstance(payload, dict):
                        raise ValueError("invalid payload")
                except (ValueError, TypeError):
                    self._send_json(400, {"ok": False, "reason": "explicit_bounded_request_required"})
                    return
                status, response = owner.accept_recovery(payload)
                self._send_json(status, response)

            def log_message(self, _format, *_args):
                return

        self.supervisor_server = ThreadingHTTPServer(
            ("127.0.0.1", SUPERVISOR_CONTROL_PORT), SupervisorHandler
        )
        threading.Thread(
            target=self.supervisor_server.serve_forever,
            daemon=True,
        ).start()
        print(
            f"微信主管控制接口已启动: 127.0.0.1:{SUPERVISOR_CONTROL_PORT}",
            flush=True,
        )

    def run_hook(self):
        print("等待 5 秒再 hook", flush=True)
        time.sleep(5)
        self._clear_hook_port()
        self.reg_hook = subprocess.Popen(
            ["wine", "/comwechat/http/WeChatHook.exe"],
            start_new_session=True,
        )

    @staticmethod
    def _clear_hook_port():
        port = os.environ.get("COMWECHAT_API_PORT", "18888")
        try:
            subprocess.run(
                ["fuser", "-k", f"{port}/tcp"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
        except OSError:
            pass

    def change_version(
        self,
        attempts=VERSION_CHANGE_ATTEMPTS,
        retry_seconds=VERSION_CHANGE_RETRY_SECONDS,
    ):
        time.sleep(5)
        result = None
        for attempt in range(1, attempts + 1):
            result = subprocess.run(
                [
                    "curl",
                    "--fail",
                    "--connect-timeout",
                    str(VERSION_CHANGE_CONNECT_TIMEOUT_SECONDS),
                    "--max-time",
                    str(VERSION_CHANGE_MAX_TIME_SECONDS),
                    "-X",
                    "POST",
                    "http://127.0.0.1:18888/api/?type=35",
                    "-H",
                    "Content-Type: application/json",
                    "-d",
                    json.dumps(
                        {
                            "path": "/comwechat/http/WeChatHook.exe",
                            "version": VERSION,
                        }
                    ),
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            if result.returncode == 0:
                print("版本已经修改", flush=True)
                return
            if attempt < attempts:
                print(
                    f"版本修改接口暂未就绪，第 {attempt} 次重试。",
                    flush=True,
                )
                time.sleep(retry_seconds)
        print(
            f"Curl command failed with error: {result.stderr.decode()}",
            flush=True,
        )
        raise WechatStackStartupFailed("版本修改接口连续失败")

    def maybe_change_version(self):
        if not VERSION_CHANGE_ENABLED:
            print("版本修改已关闭，跳过版本修改。", flush=True)
            return
        try:
            self.change_version()
        except WechatStackStartupFailed as error:
            print(f"版本修改失败，继续启动微信栈: {error}", flush=True)

    def start_bridge(self):
        config = BridgeConfig.from_env()
        if not config.enabled:
            print("Bridge API 未启用，继续使用原有 TCP 消息方式。", flush=True)
            return
        self.bridge = BridgeService(config)
        try:
            self.bridge.start()
        except Exception as error:
            # A slow WeChat/Hook API must use the existing in-container
            # recovery path instead of terminating the Docker main process.
            bridge = self.bridge
            self.bridge = None
            try:
                bridge.stop()
            except Exception as stop_error:
                print(f"Bridge 启动失败后的清理失败: {stop_error}", flush=True)
            raise WechatStackStartupFailed(f"Bridge 启动失败: {error}") from error

    def monitor_children(self, poll_interval=1):
        unresponsive_since = {}
        while not self.exiting:
            if self.recovery_requested.is_set() and self.consume_recovery():
                raise WechatStackRecoveryRequested("Explicit single-attempt recovery")
            state = self.bridge.state if self.bridge is not None else {}
            if state.get("is_login") is True and state.get("hooks_ready") is True:
                if self.confirmed_online_since is None:
                    self.confirmed_online_since = time.monotonic()
                if time.monotonic() - self.confirmed_online_since >= 30:
                    self.single_recovery_launch = False
            else:
                self.confirmed_online_since = None
            for name, process in (
                ("WeChat", self.wechat),
                ("Hook", self.reg_hook),
            ):
                if process is None:
                    continue
                status = process.poll()
                if status is not None:
                    raise ChildProcessStopped(name, status)
                state = self._process_state(getattr(process, "pid", None))
                if state == "D":
                    started = unresponsive_since.setdefault(name, time.monotonic())
                    if time.monotonic() - started >= CHILD_UNRESPONSIVE_SECONDS:
                        raise ChildProcessStopped(name, "uninterruptible")
                else:
                    unresponsive_since.pop(name, None)
            time.sleep(poll_interval)

    def stop_wechat_stack(self):
        if self.bridge is not None:
            try:
                self.bridge.stop()
            except Exception as error:
                print(f"Bridge 停止失败: {error}", flush=True)
            self.bridge = None
        self._terminate("Hook程序", self.reg_hook)
        self._terminate("微信", self.wechat)
        self.reg_hook = None
        self.wechat = None
        if not self.cleanup_wine_session():
            raise WineSessionCleanupFailed(
                "旧 Wine 会话仍有不可终止进程，已禁止叠加启动新微信"
            )

    @staticmethod
    def _process_state(pid):
        if not pid:
            return None
        try:
            stat = open(f"/proc/{pid}/stat", encoding="utf-8").read()
        except (OSError, ValueError):
            return None
        end = stat.rfind(")")
        return stat[end + 2 : end + 3] if end >= 0 else None

    @classmethod
    def _wine_processes(cls):
        processes = []
        try:
            entries = os.listdir("/proc")
        except OSError:
            return processes
        for entry in entries:
            if not entry.isdigit():
                continue
            pid = int(entry)
            try:
                with open(f"/proc/{pid}/comm", encoding="utf-8") as source:
                    name = source.read().strip()
            except OSError:
                continue
            if name not in WINE_PROCESS_NAMES:
                continue
            state = cls._process_state(pid)
            if state != "Z":
                processes.append((pid, name, state))
        return processes

    @classmethod
    def cleanup_wine_session(cls, timeout=WINE_CLEANUP_TIMEOUT_SECONDS):
        """Stop the whole Wine generation before another WeChat is launched."""
        try:
            subprocess.run(
                ["wineserver", "-k"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=min(5, max(1, timeout)),
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired):
            pass

        deadline = time.monotonic() + max(1, timeout)
        while time.monotonic() < deadline:
            remaining = cls._wine_processes()
            if not remaining:
                return True
            time.sleep(0.5)

        remaining = cls._wine_processes()
        for pid, _name, _state in remaining:
            try:
                os.kill(pid, signal.SIGKILL)
            except (OSError, ProcessLookupError):
                pass
        time.sleep(1)
        remaining = cls._wine_processes()
        if remaining:
            details = ", ".join(
                f"{name}[{pid}]({state or '?'})" for pid, name, state in remaining
            )
            print(f"Wine 会话清理失败，残留进程: {details}", flush=True)
            return False
        return True

    def wait_for_manual_restart(self):
        self.supervisor_state = "paused"
        print(
            "微信栈已停止自动恢复，等待用户主动请求一次新的恢复。",
            flush=True,
        )
        while not self.exiting:
            if self.recovery_requested.wait(timeout=60) and self.consume_recovery():
                print("收到用户请求，仅尝试启动一次微信。", flush=True)
                return True
        return False

    @staticmethod
    def _terminate(name, process):
        if process is None:
            return
        try:
            if process.poll() is None:
                print(
                    datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    + f" 退出{name}...",
                    flush=True,
                )
                DockerWechatHook._signal_process(process, signal.SIGTERM)
                wait = getattr(process, "wait", None)
                if wait is None:
                    return
                try:
                    wait(timeout=10)
                except subprocess.TimeoutExpired:
                    DockerWechatHook._signal_process(process, signal.SIGKILL)
                    try:
                        wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        pass
        except (OSError, ProcessLookupError):
            pass

    @staticmethod
    def _signal_process(process, signum):
        pid = getattr(process, "pid", None)
        if pid is not None:
            try:
                os.killpg(pid, signum)
                return
            except (OSError, ProcessLookupError):
                pass
        if signum == signal.SIGKILL:
            process.kill()
        else:
            process.terminate()

    def exit_container(self, exit_code=0):
        if self.exiting:
            return
        self.exiting = True
        print(
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            + " 正在退出容器...",
            flush=True,
        )
        try:
            self.stop_wechat_stack()
        except WineSessionCleanupFailed as error:
            print(f"容器退出时 Wine 会话未完全结束: {error}", flush=True)
        finally:
            self._terminate("VNC", self.vnc)
            if self.supervisor_server is not None:
                self.supervisor_server.shutdown()
                self.supervisor_server.server_close()
                self.supervisor_server = None
        if exit_code:
            raise SystemExit(exit_code)

    def run_all_in_one(self):
        print(
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            + " 启动容器中...",
            flush=True,
        )
        try:
            self.prepare()
            self.start_supervisor_server()
            self.run_vnc()
            recovery_failures = 0
            last_failure = 0.0
            while not self.exiting:
                stack_ready_at = None
                try:
                    self.supervisor_state = "recovering"
                    self.run_wechat()
                    self.run_hook()
                    self.maybe_change_version()
                    self.start_bridge()
                    stack_ready_at = time.monotonic()
                    self.supervisor_state = "running"
                    self.monitor_children()
                except (
                    ChildProcessStopped,
                    WechatStackStartupFailed,
                    WechatStackRecoveryRequested,
                ) as error:
                    now = time.monotonic()
                    if recovery_failures_should_reset(now, last_failure):
                        recovery_failures = 0
                    if stack_ready_at is not None:
                        previous_failures = recovery_failures
                        recovery_failures = recovery_failures_after_stable_run(
                            recovery_failures,
                            run_seconds=now - stack_ready_at,
                            stable_seconds=CHILD_RECOVERY_STABLE_SECONDS,
                        )
                        if previous_failures and not recovery_failures:
                            print(
                                "微信栈已稳定运行，旧的恢复失败计数已清零。",
                                flush=True,
                            )
                    requested = isinstance(error, WechatStackRecoveryRequested)
                    stop_after_failure = self.single_recovery_launch and not requested
                    self.last_recovery_error = str(error)[:240]
                    recovery_failures = 0 if requested else recovery_failures + 1
                    last_failure = now
                    print(f"微信栈需要恢复: {error}", flush=True)
                    try:
                        self.stop_wechat_stack()
                    except WineSessionCleanupFailed as cleanup_error:
                        print(f"微信栈恢复已停止: {cleanup_error}", flush=True)
                        self.wait_for_manual_restart()
                        return
                    if stop_after_failure or recovery_failures > CHILD_RECOVERY_ATTEMPTS:
                        if self.wait_for_manual_restart():
                            recovery_failures = 0
                            last_failure = 0.0
                            continue
                        return
                    delay = CHILD_RECOVERY_BACKOFF_SECONDS * recovery_failures
                    print(
                        f"将在 {delay} 秒后进行第 {recovery_failures} 次容器内恢复。",
                        flush=True,
                    )
                    time.sleep(delay)
        except KeyboardInterrupt:
            self.exit_container()
        except Exception as error:
            print(f"容器主进程异常: {error}", flush=True)
            self.exit_container(exit_code=1)


if __name__ == "__main__":
    from dbus_runtime import ensure_dbus

    ensure_dbus()
    print("---All in one 微信 ComRobot 容器---", flush=True)
    DockerWechatHook().run_all_in_one()
