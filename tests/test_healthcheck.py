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
            with mock.patch("healthcheck.check_url", return_value=True) as check:
                self.assertEqual(healthcheck.main(), 0)
        check.assert_called_once_with("http://127.0.0.1:19088/healthz")

    def test_tcp_mode_uses_comwechat_api(self):
        env = {
            "COMWECHAT_BRIDGE_ENABLED": "false",
            "COMWECHAT_API_PORT": "18888",
        }
        with mock.patch.dict(os.environ, env, clear=True):
            with mock.patch("healthcheck.check_url", return_value=True) as check:
                self.assertEqual(healthcheck.main(), 0)
        check.assert_called_once_with("http://127.0.0.1:18888/api/?type=0")


if __name__ == "__main__":
    unittest.main()
