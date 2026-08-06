import os
import tempfile
import time
import unittest
from pathlib import Path

from comwechat_bridge import is_stable_regular_file
from reliable_queue import message_priority, source_chat_key


class PriorityQueueTests(unittest.TestCase):
    def test_contact_has_priority_over_group(self):
        self.assertLess(
            message_priority({"chat_type": "private", "sender": "wxid_contact"}),
            message_priority({"chat_type": "group", "sender": "wxid_group"}),
        )

    def test_group_messages_share_a_stable_source_key(self):
        first = {"chat_id": "room-1", "sender": "wxid_a", "chat_type": "group"}
        second = {"chat_id": "room-1", "sender": "wxid_b", "chat_type": "group"}
        self.assertEqual(source_chat_key(first), source_chat_key(second))

    def test_missing_or_directory_attachment_is_not_stable(self):
        with tempfile.TemporaryDirectory() as directory:
            self.assertFalse(is_stable_regular_file(os.path.join(directory, "missing.bin")))
            self.assertFalse(is_stable_regular_file(directory))

    def test_regular_attachment_is_stable_after_settle_window(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "ready.bin"
            path.write_bytes(b"ready")
            self.assertTrue(is_stable_regular_file(str(path), settle_seconds=0.01))


if __name__ == "__main__":
    unittest.main()
