import json
import socket
import unittest
from urllib import request

from comwechat_bridge import (
    BridgeApiServer,
    BridgeConfig,
    MessageBuffer,
    is_fast_path,
    is_login_reorder_anchor,
)


def free_port():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def config(**overrides):
    values = {
        "enabled": True,
        "ingress_host": "127.0.0.1",
        "ingress_port": free_port(),
        "api_host": "127.0.0.1",
        "api_port": free_port(),
        "comwechat_api_port": 18888,
        "hook_save_path": "C:\\test",
        "boot_reorder_window_seconds": 30,
        "login_anchor_probe_window_seconds": 1.0,
        "login_state_probe_interval_seconds": 1.0,
        "consume_rate_per_sec": 5.0,
        "max_buffer": 100,
        "hook_retry_times": 1,
        "hook_retry_interval_seconds": 0.1,
        "metrics_interval_seconds": 10,
    }
    values.update(overrides)
    return BridgeConfig(**values)


class MessageBufferTests(unittest.TestCase):
    def test_message_classification(self):
        self.assertTrue(is_fast_path({"isSendMsg": 1, "isSendByPhone": 0}))
        self.assertTrue(
            is_login_reorder_anchor(
                {"message": '<sysmsg type="SafeModuleCfg"><x/></sysmsg>'}
            )
        )

    def test_fast_path_can_be_pulled_immediately(self):
        buffer = MessageBuffer(config())
        message = {"id": "fast", "isSendMsg": 1, "isSendByPhone": 0}
        buffer.ingest(message)
        result = buffer.pull(max_items=10, wait_ms=0)
        self.assertEqual(result["messages"], [message])

    def test_overflow_is_bounded(self):
        buffer = MessageBuffer(config(max_buffer=100))
        for index in range(120):
            buffer.ingest({"id": index})
        self.assertEqual(buffer.queue_size(), 100)
        self.assertEqual(buffer.snapshot()["overflow_drop_total"], 20)


class BridgeApiTests(unittest.TestCase):
    def setUp(self):
        self.config = config()
        self.buffer = MessageBuffer(self.config)
        self.server = BridgeApiServer(
            self.config,
            self.buffer,
            {"hooks_ready": True},
        )
        self.server.start()

    def tearDown(self):
        self.server.stop()

    def request_json(self, path, payload=None):
        url = f"http://127.0.0.1:{self.config.api_port}{path}"
        data = None if payload is None else json.dumps(payload).encode("utf-8")
        req = request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="GET" if data is None else "POST",
        )
        with request.urlopen(req, timeout=2) as response:
            return response.status, json.loads(response.read().decode("utf-8"))

    def test_health_endpoint(self):
        status, payload = self.request_json("/healthz")
        self.assertEqual(status, 200)
        self.assertTrue(payload["ok"])
        self.assertTrue(payload["hooks_ready"])

    def test_pull_endpoint(self):
        message = {"id": "one", "isSendMsg": 1, "isSendByPhone": 0}
        self.buffer.ingest(message)
        status, payload = self.request_json(
            "/v1/messages/pull",
            {"max_items": 10, "wait_ms": 0},
        )
        self.assertEqual(status, 200)
        self.assertEqual(payload["messages"], [message])


if __name__ == "__main__":
    unittest.main()
