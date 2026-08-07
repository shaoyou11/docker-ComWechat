import tempfile
import threading
import time
import unittest
from pathlib import Path

from reliable_queue import ReliableQueueConfig, SQLiteMessageQueue, build_dedup_key


class Clock:
    def __init__(self, value=1_000.0):
        self.value = float(value)

    def now(self):
        return self.value

    def advance(self, seconds):
        self.value += float(seconds)


class ReliableQueueTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.clock = Clock()
        self.path = Path(self.tempdir.name) / "queue.db"

    def tearDown(self):
        self.tempdir.cleanup()

    def queue(self, **overrides):
        values = {
            "db_path": str(self.path),
            "lease_seconds": 10,
            "max_attempts": 3,
            "message_ttl_seconds": 100,
            "ack_retention_seconds": 100,
            "dead_retention_seconds": 1_000,
            "retry_delay_seconds": 5,
        }
        values.update(overrides)
        return SQLiteMessageQueue(
            ReliableQueueConfig(**values),
            now_fn=self.clock.now,
        )

    @staticmethod
    def message(msgid="100", msg_type=1, sender="wxid_a"):
        return {
            "msgid": msgid,
            "type": msg_type,
            "sender": sender,
            "isSendMsg": 0,
            "message": "hello",
        }

    def test_staged_message_survives_reopen_and_recovers(self):
        queue = self.queue()
        message_id, _, inserted = queue.stage(self.message(), sort_at=900)
        self.assertTrue(inserted)
        self.assertEqual(queue.snapshot()["staged_size"], 1)
        queue.close()

        reopened = self.queue()
        self.assertEqual(reopened.recover_staged(), 1)
        result = reopened.pull(
            max_items=10,
            wait_ms=0,
            ack_mode=True,
            consumer_id="efb",
        )
        self.assertEqual(result["messages"], [self.message()])
        self.assertEqual(result["deliveries"][0]["message_id"], message_id)
        reopened.close()

    def test_ack_keeps_dedup_record_without_redelivery(self):
        queue = self.queue()
        _, dedup_key, _ = queue.stage(self.message())
        queue.release_all_staged()

        result = queue.pull(10, 0, True, "efb")
        delivery_id = result["deliveries"][0]["delivery_id"]
        self.assertEqual(queue.ack([delivery_id], "efb"), 1)
        self.assertEqual(queue.pull(10, 0, True, "efb")["messages"], [])

        _, repeated_key, inserted = queue.stage(self.message())
        self.assertEqual(repeated_key, dedup_key)
        self.assertFalse(inserted)
        snapshot = queue.snapshot()
        self.assertEqual(snapshot["acked_size"], 1)
        self.assertEqual(snapshot["deduplicated_total"], 1)
        queue.close()

    def test_expired_lease_is_redelivered_with_new_token(self):
        queue = self.queue(lease_seconds=2)
        queue.stage(self.message())
        queue.release_all_staged()

        first = queue.pull(1, 0, True, "efb")
        first_token = first["deliveries"][0]["delivery_id"]
        self.clock.advance(3)
        second = queue.pull(1, 0, True, "efb")

        self.assertEqual(second["messages"], [self.message()])
        self.assertNotEqual(second["deliveries"][0]["delivery_id"], first_token)
        self.assertEqual(second["deliveries"][0]["attempts"], 2)
        queue.close()

    def test_nack_waits_before_retry(self):
        queue = self.queue(retry_delay_seconds=5)
        queue.stage(self.message())
        queue.release_all_staged()

        result = queue.pull(1, 0, True, "efb")
        delivery_id = result["deliveries"][0]["delivery_id"]
        self.assertEqual(queue.nack([delivery_id], "efb", "dispatch failed"), 1)
        self.assertEqual(queue.pull(1, 0, True, "efb")["messages"], [])

        self.clock.advance(5)
        self.assertEqual(
            queue.pull(1, 0, True, "efb")["messages"],
            [self.message()],
        )
        queue.close()

    def test_expired_message_moves_to_dead_letter(self):
        queue = self.queue(message_ttl_seconds=3)
        queue.stage(self.message())
        queue.release_all_staged()
        self.clock.advance(4)

        self.assertEqual(queue.pull(1, 0, True, "efb")["messages"], [])
        snapshot = queue.snapshot()
        self.assertEqual(snapshot["dead_letter_size"], 1)
        self.assertEqual(snapshot["pending_size"], 0)
        queue.close()

    def test_max_attempts_moves_message_to_dead_letter(self):
        queue = self.queue(max_attempts=2, retry_delay_seconds=0)
        queue.stage(self.message())
        queue.release_all_staged()

        first = queue.pull(1, 0, True, "efb")
        queue.nack([first["deliveries"][0]["delivery_id"]], "efb", "first")
        second = queue.pull(1, 0, True, "efb")
        queue.nack([second["deliveries"][0]["delivery_id"]], "efb", "second")

        self.assertEqual(queue.snapshot()["dead_letter_size"], 1)
        self.assertEqual(queue.pull(1, 0, True, "efb")["messages"], [])
        queue.close()

    def test_dead_letter_can_be_listed_and_requeued(self):
        queue = self.queue(max_attempts=1, retry_delay_seconds=0)
        queue.stage(self.message("dead-retry"))
        queue.release_all_staged()
        delivery = queue.pull(1, 0, True, "efb")["deliveries"][0]
        queue.nack([delivery["delivery_id"]], "efb", "FileNotFoundError")

        dead = queue.list_dead()
        self.assertEqual(len(dead), 1)
        self.assertEqual(dead[0]["last_error"], "FileNotFoundError")
        self.assertTrue(queue.requeue_dead(dead[0]["id"]))
        retried = queue.pull(1, 0, True, "efb")
        self.assertEqual(retried["messages"][0]["msgid"], "dead-retry")
        self.assertEqual(retried["deliveries"][0]["attempts"], 1)
        queue.close()

    def test_retry_active_releases_pending_message_immediately(self):
        queue = self.queue()
        message_id, _, _ = queue.stage(self.message("active-retry"))

        self.assertEqual(queue.retry_active(message_id), "retried")
        result = queue.pull(1, 0, True, "efb")

        self.assertEqual(result["messages"][0]["msgid"], "active-retry")
        queue.close()

    def test_retry_active_rejects_inflight_message(self):
        queue = self.queue()
        message_id, _, _ = queue.stage(self.message("active-inflight"))
        queue.release([message_id])
        queue.pull(1, 0, True, "efb")

        self.assertEqual(queue.retry_active(message_id), "inflight")
        queue.close()

    def test_discard_dead_removes_dead_count_but_keeps_deduplication(self):
        queue = self.queue(max_attempts=1, retry_delay_seconds=0)
        message = self.message("dead-discard")
        message_id, dedup_key, _ = queue.stage(message)
        queue.release([message_id])
        delivery = queue.pull(1, 0, True, "efb")["deliveries"][0]
        queue.nack([delivery["delivery_id"]], "efb", "test failure")

        self.assertEqual(queue.discard_message(message_id, "admin"), "discarded")
        self.assertEqual(queue.snapshot()["dead_letter_size"], 0)
        self.assertEqual(queue.snapshot()["discarded_size"], 1)
        repeated_id, repeated_key, inserted = queue.stage(message)
        self.assertEqual(repeated_id, message_id)
        self.assertEqual(repeated_key, dedup_key)
        self.assertFalse(inserted)
        queue.close()

    def test_discard_batch_only_changes_dead_messages(self):
        queue = self.queue(max_attempts=1, retry_delay_seconds=0)
        for msgid in ("dead-a", "dead-b"):
            message_id, _, _ = queue.stage(self.message(msgid))
            queue.release([message_id])
            delivery = queue.pull(1, 0, True, "efb")["deliveries"][0]
            queue.nack([delivery["delivery_id"]], "efb", "test failure")
        active_id, _, _ = queue.stage(self.message("still-active"))

        self.assertEqual(queue.discard_all_dead("admin"), 2)
        self.assertEqual(queue.snapshot()["dead_letter_size"], 0)
        self.assertEqual(queue.snapshot()["pending_size"], 0)
        self.assertEqual(queue.discard_message(active_id, "admin"), "discarded")
        queue.close()

    def test_successful_ack_clears_previous_error(self):
        queue = self.queue(retry_delay_seconds=0)
        queue.stage(self.message("eventual-success"))
        queue.release_all_staged()
        first = queue.pull(1, 0, True, "efb")
        queue.nack([first["deliveries"][0]["delivery_id"]], "efb", "temporary")
        second = queue.pull(1, 0, True, "efb")
        queue.ack([second["deliveries"][0]["delivery_id"]], "efb")

        row = queue._db.execute(
            "SELECT state, last_error FROM messages WHERE dedup_key=?",
            (build_dedup_key(self.message("eventual-success")),),
        ).fetchone()
        self.assertEqual((row["state"], row["last_error"]), ("acked", None))
        queue.close()

    def test_legacy_pull_acks_immediately(self):
        queue = self.queue()
        queue.stage(self.message())
        queue.release_all_staged()

        first = queue.pull(1, 0, False, "legacy")
        second = queue.pull(1, 0, False, "legacy")

        self.assertEqual(first["messages"], [self.message()])
        self.assertEqual(first["deliveries"], [])
        self.assertEqual(second["messages"], [])
        self.assertEqual(queue.snapshot()["acked_size"], 1)
        queue.close()

    def test_release_order_wins_over_untrusted_message_timestamp(self):
        queue = self.queue()
        future_id, _, _ = queue.stage(self.message("future"), sort_at=9_999_999)
        fast_id, _, _ = queue.stage(self.message("fast"), sort_at=1)

        queue.release([future_id])
        queue.release([fast_id])
        result = queue.pull(2, 0, True, "efb")

        self.assertEqual(
            [message["msgid"] for message in result["messages"]],
            ["future", "fast"],
        )
        queue.close()

    def test_long_poll_wakes_when_nack_delay_expires(self):
        queue = SQLiteMessageQueue(
            ReliableQueueConfig(
                db_path=str(self.path),
                lease_seconds=10,
                max_attempts=3,
                message_ttl_seconds=100,
                ack_retention_seconds=100,
                dead_retention_seconds=1_000,
                retry_delay_seconds=0.1,
            )
        )
        queue.stage(self.message())
        queue.release_all_staged()
        first = queue.pull(1, 0, True, "efb")
        queue.nack(
            [first["deliveries"][0]["delivery_id"]],
            "efb",
            "retry",
        )

        result = {}

        def consume():
            result.update(queue.pull(1, 1_000, True, "efb"))

        started = time.monotonic()
        thread = threading.Thread(target=consume)
        thread.start()
        thread.join(timeout=0.5)

        self.assertFalse(thread.is_alive())
        self.assertEqual(result["messages"], [self.message()])
        self.assertLess(time.monotonic() - started, 0.5)
        queue.close()

    def test_fallback_dedup_key_is_stable_for_json_key_order(self):
        first = {"type": 1, "message": "same", "sender": "wxid"}
        second = {"sender": "wxid", "message": "same", "type": 1}
        self.assertEqual(build_dedup_key(first), build_dedup_key(second))


if __name__ == "__main__":
    unittest.main()
