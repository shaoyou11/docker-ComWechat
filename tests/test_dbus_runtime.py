import os
import subprocess
import unittest
from unittest import mock

import dbus_runtime


class DbusRuntimeTests(unittest.TestCase):
    def test_existing_buses_are_reused_and_exported_to_wine(self):
        with mock.patch.dict(os.environ, {}, clear=True), \
                mock.patch("dbus_runtime.bus_ready", return_value=True), \
                mock.patch("dbus_runtime.subprocess.run") as command:
            dbus_runtime.ensure_dbus()
            self.assertEqual(os.environ["DBUS_SESSION_BUS_ADDRESS"],
                             "unix:path=/run/comwechat/session_bus_socket")
            self.assertEqual(os.environ["DBUS_SYSTEM_BUS_ADDRESS"],
                             "unix:path=/run/dbus/system_bus_socket")
        command.assert_not_called()

    def test_failed_start_blocks_wine_startup(self):
        with mock.patch("dbus_runtime.bus_ready", return_value=False), \
                mock.patch("dbus_runtime.Path.mkdir"), \
                mock.patch("dbus_runtime.subprocess.run"):
            with self.assertRaisesRegex(RuntimeError, "system bus"):
                dbus_runtime.ensure_dbus()

    def test_timeout_is_unhealthy(self):
        with mock.patch("dbus_runtime.subprocess.run",
                        side_effect=subprocess.TimeoutExpired("dbus-send", 2)):
            self.assertFalse(dbus_runtime.bus_ready("session"))
