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
    @mock.patch("run.CHILD_RECOVERY_RESET_SECONDS", 0)
    def test_recovery_failure_budget_does_not_reset_by_default(self):
        self.assertFalse(run.recovery_failures_should_reset(1000, 1))

    @mock.patch("run.CHILD_RECOVERY_RESET_SECONDS", 300)
    def test_recovery_failure_budget_can_use_explicit_reset_window(self):
        self.assertTrue(run.recovery_failures_should_reset(1301, 1))

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
    def test_bridge_start_failure_uses_internal_recovery(self, _signal):
        hook = run.DockerWechatHook()
        config = mock.Mock(enabled=True)
        service = mock.Mock()
        service.start.side_effect = RuntimeError("API not ready")
        with mock.patch("run.BridgeConfig.from_env", return_value=config):
            with mock.patch("run.BridgeService", return_value=service):
                with self.assertRaisesRegex(
                    run.WechatStackStartupFailed, "Bridge 启动失败"
                ):
                    hook.start_bridge()

        service.stop.assert_called_once_with()
        self.assertIsNone(hook.bridge)

    @mock.patch("run.time.sleep")
    @mock.patch("run.subprocess.Popen")
    @mock.patch("run.subprocess.run")
    @mock.patch("run.signal.signal")
    def test_hook_starts_in_a_new_process_group(
        self, _signal, _command, popen, _sleep
    ):
        hook = run.DockerWechatHook()
        hook.run_hook()

        self.assertTrue(popen.call_args.kwargs["start_new_session"])

    @mock.patch("run.time.sleep")
    @mock.patch("run.subprocess.Popen")
    @mock.patch("run.subprocess.run")
    @mock.patch("run.signal.signal")
    def test_hook_clears_stale_api_listener_before_starting(
        self, _signal, command, popen, _sleep
    ):
        hook = run.DockerWechatHook()
        hook.run_hook()

        command.assert_any_call(
            ["fuser", "-k", "18888/tcp"],
            stdout=run.subprocess.DEVNULL,
            stderr=run.subprocess.DEVNULL,
            check=False,
        )

    @mock.patch("run.os.killpg")
    @mock.patch("run.signal.signal")
    def test_terminate_escalates_and_waits_for_process_group(self, _signal, killpg):
        hook = run.DockerWechatHook()
        process = mock.Mock(pid=4321)
        process.poll.return_value = None
        process.wait.side_effect = [
            run.subprocess.TimeoutExpired("Hook", 10),
            None,
        ]

        hook._terminate("Hook程序", process)

        killpg.assert_has_calls(
            [
                mock.call(4321, run.signal.SIGTERM),
                mock.call(4321, run.signal.SIGKILL),
            ]
        )
        self.assertEqual(process.wait.call_args_list, [mock.call(timeout=10), mock.call(timeout=5)])

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
    def test_change_version_allows_slow_hook_startup(self, _signal, _sleep):
        hook = run.DockerWechatHook()
        failed = mock.Mock(returncode=7, stderr=b"not ready")
        succeeded = mock.Mock(returncode=0, stderr=b"")
        with mock.patch(
            "run.subprocess.run",
            side_effect=[failed] * 7 + [succeeded],
        ) as command:
            hook.change_version(retry_seconds=0)

        self.assertEqual(command.call_count, 8)

    @mock.patch("run.time.sleep")
    @mock.patch("run.signal.signal")
    def test_change_version_bounds_curl_wait(self, _signal, _sleep):
        hook = run.DockerWechatHook()
        succeeded = mock.Mock(returncode=0, stderr=b"")
        with mock.patch("run.subprocess.run", return_value=succeeded) as command:
            hook.change_version(attempts=1, retry_seconds=0)

        args = command.call_args.args[0]
        self.assertIn("--fail", args)
        self.assertIn("--connect-timeout", args)
        self.assertIn("--max-time", args)

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

    @mock.patch("run.signal.signal")
    def test_version_change_is_skipped_when_switch_is_disabled(self, _signal):
        hook = run.DockerWechatHook()
        with mock.patch.object(run, "VERSION_CHANGE_ENABLED", False):
            with mock.patch.object(hook, "change_version") as change_version:
                hook.maybe_change_version()

        change_version.assert_not_called()

    @mock.patch("run.signal.signal")
    def test_version_change_runs_when_switch_is_enabled(self, _signal):
        hook = run.DockerWechatHook()
        with mock.patch.object(run, "VERSION_CHANGE_ENABLED", True):
            with mock.patch.object(hook, "change_version") as change_version:
                hook.maybe_change_version()

        change_version.assert_called_once_with()

    @mock.patch("run.signal.signal")
    def test_version_change_failure_does_not_abort_stack_when_enabled(self, _signal):
        hook = run.DockerWechatHook()
        with mock.patch.object(run, "VERSION_CHANGE_ENABLED", True):
            with mock.patch.object(
                hook,
                "change_version",
                side_effect=run.WechatStackStartupFailed("not ready"),
            ):
                hook.maybe_change_version()

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
