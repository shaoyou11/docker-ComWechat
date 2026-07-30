# shaoyou11/docker-ComWechat

为现有 EFB + ComWechat 部署维护的兼容镜像。镜像以当前生产环境使用的
`tomsnow1999/docker-com_wechat_robot` 固定摘要为底座，保留原有 Wine、
微信 3.9.12.16、VNC、Hook、版本修正和子进程监控，并加入可选的 Bridge API。

## 镜像

```text
ghcr.io/shaoyou11/docker-comwechat:latest
ghcr.io/shaoyou11/docker-comwechat:1.0.0-bridge.1
```

`latest` 用于跟随本仓库已验证版本；生产部署同时记录不可变镜像摘要和版本标签，
便于失败时回滚。当前只构建 `linux/amd64`。

## Bridge API

Bridge 默认关闭，关闭时行为与现有 TCP 消息接收方式一致。

```yaml
environment:
  COMWECHAT_VERSION: "3.9.12.16"
  COMWECHAT_BRIDGE_ENABLED: "true"
  COMWECHAT_BRIDGE_IN_PORT: "23456"
  COMWECHAT_BRIDGE_API_PORT: "19088"
  COMWECHAT_BRIDGE_MAX_BUFFER: "20000"
  COMWECHAT_CONSUME_RATE_PER_SEC: "5"
```

启用后提供：

- `GET /healthz`
- `POST /v1/messages/pull`
- 消息缓冲、登录阶段排序、速率控制和队列指标

Bridge 只负责消息 Hook 与拉取接口，不点击微信界面，也不自动重启微信。
登录恢复仍由现有独立 Watchdog 控制，避免两套逻辑相互冲突。

## 私有运行文件

`comwechat.zip`、VNC 密码、微信登录数据、EFB 配置和 Telegram 凭据不进入本仓库
或镜像。生产环境继续通过 Compose 挂载这些私有文件和持久化目录。

## 验证

```bash
python3 -m py_compile run.py comwechat_bridge.py healthcheck.py
python3 -m unittest discover -s tests -v
```

GitHub Actions 在测试通过后发布 GHCR 镜像。容器健康检查会根据
`COMWECHAT_BRIDGE_ENABLED` 自动检查 Bridge API 或原有 ComWechat API。

## 上线顺序

1. 备份 Compose、启动脚本、配置、会话目录和当前镜像。
2. 先以 `COMWECHAT_BRIDGE_ENABLED=false` 替换镜像，确认旧 TCP 模式正常。
3. EFB 更新到 Bridge 消费端后，再同时启用 Bridge。
4. 验证容器健康、`/healthz`、EFB 日志和真实微信消息。

## 回滚

生产部署保留原镜像摘要、本地回滚标签、原 Compose 和完整镜像归档。发生异常时：

1. 恢复备份 Compose。
2. 指向原镜像摘要或本地回滚标签。
3. 关闭 Bridge 环境变量。
4. 按 Comwechat、EFB、Watchdog 的既有顺序启动并核验。

本仓库基于 `tom-snow/docker-ComWechat` 的历史代码，并参考
`jiz4oh/docker-ComWechat` 的 Bridge 实现。
