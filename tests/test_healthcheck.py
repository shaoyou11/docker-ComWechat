import os
import unittest
from unittest import mock

import healthcheck


class HealthcheckTests(unittest.TestCase):
    def test_bridge_mode_uses_bridge_api(self):
        env = {
            "COMWECHAT_BRIDGE_ENABLED": "true",
            "COMWECHAT_BRIDGE_API_PORT": "19088",
        }
        with mock.patch.dict(os.environ, env, clear=True):
            with mock.patch("healthcheck.core_processes_are_sane", return_value=True), \
                    mock.patch("healthcheck.check_url", return_value=True) as check:
                self.assertEqual(healthcheck.main(), 0)
        check.assert_called_once_with("http://127.0.0.1:19088/healthz")

    def test_tcp_mode_uses_comwechat_api(self):
        env = {
            "COMWECHAT_BRIDGE_ENABLED": "false",
            "COMWECHAT_API_PORT": "18888",
        }
        with mock.patch.dict(os.environ, env, clear=True):
            with mock.patch("healthcheck.core_processes_are_sane", return_value=True), \
                    mock.patch("healthcheck.check_url", return_value=True) as check:
                self.assertEqual(healthcheck.main(), 0)
        check.assert_called_once_with("http://127.0.0.1:18888/api/?type=0")

    def test_rejects_uninterruptible_core_process(self):
        with mock.patch("healthcheck.os.listdir", return_value=["42"]), \
                mock.patch("builtins.open", mock.mock_open(read_data="WeChat.exe\n")), \
                mock.patch("healthcheck.process_state", return_value="D"):
            self.assertFalse(healthcheck.core_processes_are_sane())

    def test_rejects_duplicate_desktop_generations(self):
        files = {
            "/proc/41/comm": "explorer.exe\n",
            "/proc/42/comm": "explorer.exe\n",
        }

        def open_file(path, *args, **kwargs):
            return mock.mock_open(read_data=files[path])()

        with mock.patch("healthcheck.os.listdir", return_value=["41", "42"]), \
                mock.patch("builtins.open", side_effect=open_file), \
                mock.patch("healthcheck.process_state", return_value="S"):
            self.assertFalse(healthcheck.core_processes_are_sane())


if __name__ == "__main__":
    unittest.main()
