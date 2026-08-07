import json
import socket
import unittest
from urllib import request
from urllib.error import HTTPError

from comwechat_bridge import BridgeApiServer, BridgeConfig, MessageBuffer


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
        "max_attempts": 1,
        "retry_delay_seconds": 0,
    }
    values.update(overrides)
    return BridgeConfig(**values)


class BridgeQueueApiTests(unittest.TestCase):
    def setUp(self):
        self.config = config()
        self.buffer = MessageBuffer(self.config)
        self.server = BridgeApiServer(self.config, self.buffer, {"hooks_ready": True})
        self.server.start()

    def tearDown(self):
        self.server.stop()
        self.buffer.close()

    def request_json(self, path, payload=None):
        url = f"http://127.0.0.1:{self.config.api_port}{path}"
        data = None if payload is None else json.dumps(payload).encode("utf-8")
        req = request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="GET" if data is None else "POST",
        )
        try:
            response = request.urlopen(req, timeout=2)
        except HTTPError as error:
            return error.code, json.loads(error.read().decode("utf-8"))
        with response:
            return response.status, json.loads(response.read().decode("utf-8"))

    def make_dead(self, message_id):
        self.buffer.ingest({"id": message_id, "isSendMsg": 1, "isSendByPhone": 0})
        _, pulled = self.request_json(
            "/v1/messages/pull",
            {"max_items": 1, "wait_ms": 0, "ack_mode": True, "consumer_id": "efb"},
        )
        self.request_json(
            "/v1/messages/nack",
            {
                "delivery_ids": [pulled["deliveries"][0]["delivery_id"]],
                "consumer_id": "efb",
                "reason": "test failure",
            },
        )
        _, dead = self.request_json("/v1/messages/dead?limit=10")
        return dead["messages"][0]["id"]

    def test_retry_and_discard_routes_update_queue_state(self):
        self.buffer.ingest({"id": "active-route"})
        _, active = self.request_json("/v1/messages/active?limit=10")
        message_id = active["messages"][0]["id"]

        status, retried = self.request_json(
            "/v1/messages/retry-active", {"message_id": message_id}
        )
        self.assertEqual(status, 200)
        self.assertEqual(retried, {"ok": True, "result": "retried"})

        status, discarded = self.request_json(
            "/v1/messages/discard",
            {"message_id": message_id, "reason": "admin test"},
        )
        self.assertEqual(status, 200)
        self.assertEqual(discarded, {"ok": True, "result": "discarded"})
        self.assertEqual(self.buffer.snapshot()["discarded_size"], 1)

    def test_inflight_messages_are_protected(self):
        self.buffer.ingest({"id": "inflight-route", "isSendMsg": 1, "isSendByPhone": 0})
        _, pulled = self.request_json(
            "/v1/messages/pull",
            {"max_items": 1, "wait_ms": 0, "ack_mode": True, "consumer_id": "efb"},
        )
        message_id = pulled["deliveries"][0]["message_id"]

        _, retry = self.request_json(
            "/v1/messages/retry-active", {"message_id": message_id}
        )
        _, discard = self.request_json(
            "/v1/messages/discard", {"message_id": message_id, "reason": "admin"}
        )
        self.assertEqual(retry["result"], "inflight")
        self.assertEqual(discard["result"], "inflight")

    def test_batch_routes_and_health_count(self):
        self.make_dead("dead-batch")

        status, result = self.request_json("/v1/messages/requeue-all-dead", {})
        self.assertEqual(status, 200)
        self.assertEqual(result, {"ok": True, "requeued": 1})

        _, pulled = self.request_json(
            "/v1/messages/pull",
            {"max_items": 1, "wait_ms": 0, "ack_mode": True, "consumer_id": "efb"},
        )
        self.request_json(
            "/v1/messages/nack",
            {
                "delivery_ids": [pulled["deliveries"][0]["delivery_id"]],
                "consumer_id": "efb",
                "reason": "test failure again",
            },
        )
        status, discarded = self.request_json(
            "/v1/messages/discard-all-dead", {"reason": "admin batch"}
        )
        self.assertEqual(status, 200)
        self.assertEqual(discarded, {"ok": True, "discarded": 1})

        status, health = self.request_json("/healthz")
        self.assertEqual(status, 200)
        self.assertEqual(health["discarded_size"], 1)

    def test_management_routes_validate_arguments(self):
        for path in (
            "/v1/messages/retry-active",
            "/v1/messages/discard",
        ):
            status, payload = self.request_json(path, {})
            self.assertEqual(status, 400)
            self.assertEqual(payload["error"], "invalid_arguments")


if __name__ == "__main__":
    unittest.main()
