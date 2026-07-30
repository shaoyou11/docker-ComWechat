import unittest
from unittest import mock

import run


class FakeProcess:
    def __init__(self, status=None):
        self.status = status
        self.terminated = False

    def poll(self):
        return self.status

    def terminate(self):
        self.terminated = True
        self.status = 0


class DockerWechatHookTests(unittest.TestCase):
    @mock.patch("run.signal.signal")
    def test_bridge_disabled_keeps_original_mode(self, _signal):
        hook = run.DockerWechatHook()
        with mock.patch("run.BridgeConfig.from_env") as from_env:
            from_env.return_value.enabled = False
            hook.start_bridge()
        self.assertIsNone(hook.bridge)

    @mock.patch("run.signal.signal")
    def test_bridge_enabled_starts_service(self, _signal):
        hook = run.DockerWechatHook()
        config = mock.Mock(enabled=True)
        service = mock.Mock()
        with mock.patch("run.BridgeConfig.from_env", return_value=config):
            with mock.patch("run.BridgeService", return_value=service):
                hook.start_bridge()
        service.start.assert_called_once_with()
        self.assertIs(hook.bridge, service)

    @mock.patch("run.signal.signal")
    def test_shutdown_is_idempotent(self, _signal):
        hook = run.DockerWechatHook()
        hook.vnc = FakeProcess()
        hook.wechat = FakeProcess()
        hook.reg_hook = FakeProcess()
        hook.bridge = mock.Mock()
        hook.exit_container()
        hook.exit_container()
        hook.bridge.stop.assert_called_once_with()
        self.assertTrue(hook.vnc.terminated)
        self.assertTrue(hook.wechat.terminated)
        self.assertTrue(hook.reg_hook.terminated)

    @mock.patch("run.time.sleep", side_effect=RuntimeError("stop"))
    @mock.patch("run.signal.signal")
    def test_monitor_detects_dead_child(self, _signal, _sleep):
        hook = run.DockerWechatHook()
        hook.vnc = FakeProcess()
        hook.wechat = FakeProcess(status=7)
        hook.reg_hook = FakeProcess()
        with self.assertRaisesRegex(RuntimeError, "WeChat process stopped"):
            hook.monitor_children()


if __name__ == "__main__":
    unittest.main()
