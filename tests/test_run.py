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
    def test_prepare_copies_hook_and_keeps_source(self, _signal):
        hook = run.DockerWechatHook()
        with mock.patch("run.os.path.exists", return_value=True):
            with mock.patch("run.subprocess.run") as command:
                hook.prepare()
        self.assertEqual(command.call_args_list[1].args[0][0], "cp")
        self.assertEqual(command.call_args_list[1].args[0][1], "-p")

    @mock.patch("run.signal.signal")
    def test_prepare_accepts_existing_target_after_restart(self, _signal):
        hook = run.DockerWechatHook()
        exists = {
            "/WeChatHook.exe": False,
            "/comwechat/http/WeChatHook.exe": True,
        }
        with mock.patch("run.os.path.exists", side_effect=exists.__getitem__):
            with mock.patch("run.subprocess.run") as command:
                hook.prepare()
        self.assertEqual(command.call_count, 1)

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
        bridge = hook.bridge
        wechat = hook.wechat
        reg_hook = hook.reg_hook
        hook.exit_container()
        hook.exit_container()
        bridge.stop.assert_called_once_with()
        self.assertTrue(hook.vnc.terminated)
        self.assertTrue(wechat.terminated)
        self.assertTrue(reg_hook.terminated)

    @mock.patch("run.time.sleep", side_effect=RuntimeError("stop"))
    @mock.patch("run.signal.signal")
    def test_monitor_detects_dead_child(self, _signal, _sleep):
        hook = run.DockerWechatHook()
        hook.vnc = FakeProcess()
        hook.wechat = FakeProcess(status=7)
        hook.reg_hook = FakeProcess()
        with self.assertRaisesRegex(RuntimeError, "WeChat process stopped"):
            hook.monitor_children()

    @mock.patch("run.time.sleep", side_effect=RuntimeError("one loop"))
    @mock.patch("run.signal.signal")
    def test_monitor_ignores_vnc_daemon_parent_exit(self, _signal, _sleep):
        hook = run.DockerWechatHook()
        hook.vnc = FakeProcess(status=0)
        hook.wechat = FakeProcess()
        hook.reg_hook = FakeProcess()
        with self.assertRaisesRegex(RuntimeError, "one loop"):
            hook.monitor_children()

    @mock.patch("run.time.sleep")
    @mock.patch("run.signal.signal")
    def test_change_version_retries_transient_failure(self, _signal, sleep):
        hook = run.DockerWechatHook()
        failed = mock.Mock(returncode=7, stderr=b"not ready")
        succeeded = mock.Mock(returncode=0, stderr=b"")
        with mock.patch("run.subprocess.run", side_effect=[failed, succeeded]) as command:
            hook.change_version(attempts=3, retry_seconds=2)

        self.assertEqual(command.call_count, 2)
        self.assertEqual(sleep.call_args_list, [mock.call(5), mock.call(2)])

    @mock.patch("run.time.sleep")
    @mock.patch("run.signal.signal")
    def test_change_version_exhaustion_requests_internal_recovery(
        self, _signal, _sleep
    ):
        hook = run.DockerWechatHook()
        failed = mock.Mock(returncode=7, stderr=b"not ready")
        with mock.patch("run.subprocess.run", return_value=failed):
            with self.assertRaises(run.WechatStackStartupFailed):
                hook.change_version(attempts=2, retry_seconds=0)

    @mock.patch("run.time.sleep")
    @mock.patch("run.time.monotonic", return_value=1000)
    @mock.patch("run.signal.signal")
    def test_child_exit_recovers_inside_same_container(
        self, _signal, _monotonic, _sleep
    ):
        hook = run.DockerWechatHook()
        with mock.patch.object(hook, "prepare"), \
                mock.patch.object(hook, "run_vnc") as run_vnc, \
                mock.patch.object(hook, "run_wechat") as run_wechat, \
                mock.patch.object(hook, "run_hook"), \
                mock.patch.object(hook, "change_version"), \
                mock.patch.object(hook, "start_bridge"), \
                mock.patch.object(
                    hook,
                    "monitor_children",
                    side_effect=[
                        run.ChildProcessStopped("WeChat", 0),
                        KeyboardInterrupt(),
                    ],
                ), \
                mock.patch.object(hook, "stop_wechat_stack") as stop_stack, \
                mock.patch.object(hook, "exit_container"):
            hook.run_all_in_one()

        run_vnc.assert_called_once_with()
        self.assertEqual(run_wechat.call_count, 2)
        stop_stack.assert_called_once_with()

    @mock.patch("run.time.sleep")
    @mock.patch("run.time.monotonic", return_value=1000)
    @mock.patch("run.signal.signal")
    def test_recovery_limit_waits_without_restarting_container(
        self, _signal, _monotonic, _sleep
    ):
        hook = run.DockerWechatHook()
        with mock.patch.object(hook, "prepare"), \
                mock.patch.object(hook, "run_vnc"), \
                mock.patch.object(hook, "run_wechat"), \
                mock.patch.object(hook, "run_hook"), \
                mock.patch.object(hook, "change_version"), \
                mock.patch.object(hook, "start_bridge"), \
                mock.patch.object(
                    hook,
                    "monitor_children",
                    side_effect=run.ChildProcessStopped("WeChat", 0),
                ) as monitor, \
                mock.patch.object(hook, "stop_wechat_stack"), \
                mock.patch.object(hook, "wait_for_manual_restart") as wait:
            hook.run_all_in_one()

        self.assertEqual(
            monitor.call_count,
            run.CHILD_RECOVERY_ATTEMPTS + 1,
        )
        wait.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
