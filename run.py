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

    def change_version(self):
        time.sleep(5)
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
        if result.returncode != 0:
            print(
                f"Curl command failed with error: {result.stderr.decode()}",
                flush=True,
            )
            raise RuntimeError("版本修改失败")
        print("版本已经修改", flush=True)

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
                    raise RuntimeError(f"{name} process stopped with code {status}")
            time.sleep(poll_interval)

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
        if self.bridge is not None:
            try:
                self.bridge.stop()
            except Exception as error:
                print(f"Bridge 停止失败: {error}", flush=True)
        self._terminate("Hook程序", self.reg_hook)
        self._terminate("微信", self.wechat)
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
            self.run_wechat()
            self.run_hook()
            self.change_version()
            self.start_bridge()
            self.monitor_children()
        except KeyboardInterrupt:
            self.exit_container()
        except Exception as error:
            print(f"容器主进程异常: {error}", flush=True)
            self.exit_container(exit_code=1)


if __name__ == "__main__":
    print("---All in one 微信 ComRobot 容器---", flush=True)
    DockerWechatHook().run_all_in_one()
