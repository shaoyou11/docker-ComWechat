#!/usr/bin/python3
import datetime
import json
import os
import signal
import subprocess
import sys
import time

from comwechat_bridge import BridgeConfig, BridgeService


VERSION = os.environ.get("COMWECHAT_VERSION", "3.9.12.16")
VERSION_CHANGE_ATTEMPTS = int(os.environ.get("COMWECHAT_VERSION_CHANGE_ATTEMPTS", "6"))
VERSION_CHANGE_RETRY_SECONDS = int(os.environ.get("COMWECHAT_VERSION_CHANGE_RETRY_SECONDS", "2"))
CHILD_RECOVERY_ATTEMPTS = int(os.environ.get("COMWECHAT_CHILD_RECOVERY_ATTEMPTS", "3"))
CHILD_RECOVERY_BACKOFF_SECONDS = int(os.environ.get("COMWECHAT_CHILD_RECOVERY_BACKOFF_SECONDS", "5"))
CHILD_RECOVERY_RESET_SECONDS = int(os.environ.get("COMWECHAT_CHILD_RECOVERY_RESET_SECONDS", "300"))


class ChildProcessStopped(RuntimeError):
    def __init__(self, name, status):
        super().__init__(f"{name} process stopped with code {status}")
        self.name = name
        self.status = status


class WechatStackStartupFailed(RuntimeError):
    pass


class DockerWechatHook:
    def __init__(self):
        self.vnc = None
        self.wechat = None
        self.reg_hook = None
        self.bridge = None
        self.exiting = False
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
            ]
        )

    def run_wechat(self):
        self.wechat = subprocess.Popen(
            [
                "wine",
                "/home/user/.wine/drive_c/Program Files/Tencent/WeChat/WeChat.exe",
            ]
        )

    def run_hook(self):
        print("等待 5 秒再 hook", flush=True)
        time.sleep(5)
        self.reg_hook = subprocess.Popen(
            ["wine", "/comwechat/http/WeChatHook.exe"]
        )

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

    def start_bridge(self):
        config = BridgeConfig.from_env()
        if not config.enabled:
            print("Bridge API 未启用，继续使用原有 TCP 消息方式。", flush=True)
            return
        self.bridge = BridgeService(config)
        self.bridge.start()

    def monitor_children(self, poll_interval=1):
        while not self.exiting:
            for name, process in (
                ("WeChat", self.wechat),
                ("Hook", self.reg_hook),
            ):
                if process is None:
                    continue
                status = process.poll()
                if status is not None:
                    raise ChildProcessStopped(name, status)
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

    def wait_for_manual_restart(self):
        print(
            "微信栈已停止自动恢复，保留 VNC 与容器等待人工或定时重启。",
            flush=True,
        )
        while not self.exiting:
            time.sleep(60)

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
                process.terminate()
        except (OSError, ProcessLookupError):
            pass

    def exit_container(self, exit_code=0):
        if self.exiting:
            return
        self.exiting = True
        print(
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            + " 正在退出容器...",
            flush=True,
        )
        self.stop_wechat_stack()
        self._terminate("VNC", self.vnc)
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
            self.run_vnc()
            recovery_failures = 0
            last_failure = 0.0
            while not self.exiting:
                try:
                    self.run_wechat()
                    self.run_hook()
                    self.change_version()
                    self.start_bridge()
                    self.monitor_children()
                except (ChildProcessStopped, WechatStackStartupFailed) as error:
                    now = time.monotonic()
                    if now - last_failure >= CHILD_RECOVERY_RESET_SECONDS:
                        recovery_failures = 0
                    recovery_failures += 1
                    last_failure = now
                    print(f"微信栈需要恢复: {error}", flush=True)
                    self.stop_wechat_stack()
                    if recovery_failures > CHILD_RECOVERY_ATTEMPTS:
                        self.wait_for_manual_restart()
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
    print("---All in one 微信 ComRobot 容器---", flush=True)
    DockerWechatHook().run_all_in_one()
