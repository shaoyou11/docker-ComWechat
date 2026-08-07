# shaoyou11/docker-ComWechat

为现有 EFB + ComWechat 部署维护的兼容镜像。镜像以当前生产环境使用的
`tomsnow1999/docker-com_wechat_robot` 固定摘要为底座，保留原有 Wine、
微信 3.9.12.16、VNC、Hook、版本修正和子进程监控，并加入可选的 Bridge API。

## 镜像

```text
ghcr.io/shaoyou11/docker-comwechat:latest
ghcr.io/shaoyou11/docker-comwechat:1.1.0-bridge.1
```

`latest` 用于跟随本仓库已验证版本；生产部署同时记录不可变镜像摘要和版本标签，
便于失败时回滚。当前只构建 `linux/amd64`。

## Bridge API

Bridge 默认关闭，关闭时行为与现有 TCP 消息接收方式一致。

```yaml
environment:
  COMWECHAT_VERSION: "3.9.12.16"
  COMWECHAT_VERSION_CHANGE_ENABLED: "false"
  COMWECHAT_VERSION_CHANGE_ATTEMPTS: "20"
  COMWECHAT_VERSION_CHANGE_RETRY_SECONDS: "2"
  COMWECHAT_CHILD_RECOVERY_ATTEMPTS: "3"
  COMWECHAT_CHILD_RECOVERY_BACKOFF_SECONDS: "5"
  COMWECHAT_CHILD_RECOVERY_RESET_SECONDS: "300"
  COMWECHAT_BRIDGE_ENABLED: "true"
  COMWECHAT_BRIDGE_IN_PORT: "23456"
  COMWECHAT_BRIDGE_API_PORT: "19088"
  COMWECHAT_BRIDGE_DB_PATH: "/var/lib/comwechat-bridge/queue.db"
  COMWECHAT_BRIDGE_LEASE_SECONDS: "120"
  COMWECHAT_BRIDGE_MAX_ATTEMPTS: "10"
  COMWECHAT_BRIDGE_MESSAGE_TTL_SECONDS: "604800"
  COMWECHAT_BRIDGE_MAX_BUFFER: "20000"
  COMWECHAT_CONSUME_RATE_PER_SEC: "5"
volumes:
  - "./volume/Bridge:/var/lib/comwechat-bridge"
```

启用后提供：

- `GET /healthz`
- `POST /v1/messages/pull`
- `POST /v1/messages/ack`
- `POST /v1/messages/nack`
- `GET /v1/messages/active?limit=5&offset=0`
- `GET /v1/messages/dead?limit=5&offset=0`
- `POST /v1/messages/retry-active`
- `POST /v1/messages/retry-all-active`
- `POST /v1/messages/requeue`、`/v1/messages/requeue-all-dead`
- `POST /v1/messages/discard`、`/v1/messages/discard-all-active`、`/v1/messages/discard-all-dead`
- SQLite WAL 持久化、租约、去重、过期死信、登录阶段排序和队列指标

Bridge 管理 API 只绑定共享容器网络命名空间内的回环地址，不作为新的局域网或公网入口；
活动队列中的 `staged` 和 `inflight` 状态不会被批量操作强行改动。

新消费端在拉取请求中发送 `ack_mode: true`。消息在 ACK 前保持为租约状态；
消费失败可 NACK 并延迟重试。旧消费端不发送 `ack_mode` 时仍保持拉取即确认的
兼容行为。

Bridge 只负责消息 Hook 与拉取接口，不点击微信界面，也不自动重启微信。
微信或 Hook 子进程意外退出时，会优先在当前容器内有限恢复，避免共享网络命名空间被重建。连续失败超过上限后停止自动尝试并保留 VNC，等待人工或定时重启；登录界面恢复仍由独立 Watchdog 控制。

## 版本修改开关

`COMWECHAT_VERSION_CHANGE_ENABLED` 默认是 `false`。关闭时，容器启动不会调用版本修改接口；开启为 `true` 后才会执行版本修改。版本修改接口偶发不可用时只记录警告并继续启动微信栈，不再因为这一步直接触发容器内恢复。

## 私有运行文件

`comwechat.zip`、VNC 密码、微信登录数据、EFB 配置和 Telegram 凭据不进入本仓库
或镜像。生产环境继续通过 Compose 挂载这些私有文件和持久化目录。Bridge 数据库
必须挂载 `/var/lib/comwechat-bridge`；数据库只保存消息 JSON、附件路径和投递状态，
不复制附件文件本体。

## 验证

```bash
python3 -m py_compile run.py comwechat_bridge.py reliable_queue.py healthcheck.py
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
