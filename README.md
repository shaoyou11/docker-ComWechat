# shaoyou11/docker-ComWechat

这是面向现有 EFB + ComWechat 部署的兼容镜像。镜像以当前生产环境使用的
`tomsnow1999/docker-com_wechat_robot` 固定摘要为底座，保留原有 Wine、微信 `3.9.12.16`、VNC、Hook、
版本修正和子进程监控，并加入可选的 Bridge API。

Bridge 只负责消息 Hook、持久队列和拉取接口，不点击微信界面，也不自动重启微信。

## 镜像

```text
ghcr.io/shaoyou11/docker-comwechat:latest
ghcr.io/shaoyou11/docker-comwechat:1.1.0-bridge.1
```

`latest` 用于跟随本仓库已经验证的版本。生产部署应同时记录不可变镜像摘要和版本标签，
便于失败时回滚。当前只构建 `linux/amd64`。

## Bridge 开关

Bridge 默认关闭。关闭时，ComWechat 继续使用现有 TCP 消息接收方式。

启用示例：

```yaml
environment:
  COMWECHAT_VERSION: "3.9.12.16"
  COMWECHAT_VERSION_CHANGE_ENABLED: "false"
  COMWECHAT_VERSION_CHANGE_ATTEMPTS: "20"
  COMWECHAT_VERSION_CHANGE_RETRY_SECONDS: "2"
  COMWECHAT_CHILD_RECOVERY_ATTEMPTS: "3"
  COMWECHAT_CHILD_RECOVERY_BACKOFF_SECONDS: "5"
  COMWECHAT_CHILD_RECOVERY_RESET_SECONDS: "0"
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

生产环境必须挂载 `/var/lib/comwechat-bridge`，否则队列、租约和去重状态会在容器重建后丢失。

## API

| 方法 | 路径 | 作用 |
| --- | --- | --- |
| `GET` | `/healthz` | 检查 ComWechat 和 Bridge 是否可用。 |
| `POST` | `/v1/messages/pull` | 按长轮询方式获取消息。 |
| `POST` | `/v1/messages/ack` | 确认消息已被下游成功处理。 |
| `POST` | `/v1/messages/nack` | 记录处理失败并安排延迟重试。 |
| `GET` | `/v1/messages/active?limit=5&offset=0` | 分页查看活动队列。 |
| `GET` | `/v1/messages/dead?limit=5&offset=0` | 分页查看死信队列。 |
| `POST` | `/v1/messages/retry-active` | 重试指定活动消息。 |
| `POST` | `/v1/messages/retry-all-active` | 重试可处理的活动消息。 |
| `POST` | `/v1/messages/requeue`、`/v1/messages/requeue-all-dead` | 重新排队消息。 |
| `POST` | `/v1/messages/discard`、`/v1/messages/discard-all-active`、`/v1/messages/discard-all-dead` | 放弃消息并保留最小审计信息。 |

Bridge 使用 SQLite WAL 保存消息 JSON、附件路径和投递状态，不复制附件文件本体。队列支持：

- 租约和超时回收。
- ACK/NACK 确认回执。
- 消息去重。
- 过期消息进入死信。
- 登录阶段排序。
- 队列指标和有限的子进程恢复。

新消费端在拉取请求中发送 `ack_mode: true`。消息在 ACK 前保持租约状态；消费失败时发送 NACK 并延迟重试。
旧消费端不发送 `ack_mode` 时仍保持“拉取即确认”的兼容行为。

同一微信会话保持 FIFO，联系人优先于群聊；附件在文件稳定后才释放给 EFB。

Bridge 管理 API 只绑定共享容器网络命名空间内的回环地址，不作为新的局域网或公网入口。
活动队列中的 `staged` 和 `inflight` 状态不会被批量操作强行改动。

## 子进程恢复边界

微信或 Hook 子进程意外退出时，镜像优先在当前容器内有限恢复，避免共享网络命名空间被重建。
连续失败超过上限后停止自动尝试并保留 VNC；默认不会自动重置失败计数，只有显式设置正数
`COMWECHAT_CHILD_RECOVERY_RESET_SECONDS` 时才会按周期重新计数。

登录界面恢复由独立的 `efb-watchdog` 负责，Bridge 不会点击“确定”或“进入微信”，也不会绕过微信服务端验证。

## 版本修改开关

`COMWECHAT_VERSION_CHANGE_ENABLED` 默认是 `false`。关闭时，容器启动不会调用版本修改接口；开启为 `true` 后才会执行版本修改。
版本修改接口偶发不可用时只记录警告并继续启动微信栈，不会因为这一步直接触发容器内恢复。

## 私有运行文件

以下内容不进入本仓库，也不进入镜像：

- `comwechat.zip`。
- VNC 密码和微信登录数据。
- EFB 配置、Telegram 凭据和 Bot Token。
- 微信媒体文件及运行日志。

生产环境通过 Compose 挂载这些私有文件和持久化目录。

## 验证

```bash
python3 -m py_compile run.py comwechat_bridge.py reliable_queue.py healthcheck.py
python3 -m unittest discover -s tests -v
```

GitHub Actions 在测试通过后发布 GHCR 镜像。容器健康检查根据
`COMWECHAT_BRIDGE_ENABLED` 检查 Bridge API 或原有 ComWechat API。

## 上线顺序

1. 备份 Compose、启动脚本、配置、微信会话目录和当前镜像。
2. 先以 `COMWECHAT_BRIDGE_ENABLED=false` 替换镜像，确认 TCP 模式正常。
3. EFB 更新到 Bridge 消费端后，再启用 Bridge。
4. 验证容器健康、`/healthz`、EFB 日志和真实微信消息。

启用 Bridge 后，不要同时保留 TCP 和 Bridge 两条消息接收路径，避免重复收取同一消息。

## 回滚

生产部署保留原镜像摘要、本地回滚标签、原 Compose 和完整镜像归档。发生异常时：

1. 恢复备份 Compose。
2. 指向原镜像摘要或本地回滚标签。
3. 设置 `COMWECHAT_BRIDGE_ENABLED=false`。
4. 按 Comwechat、EFB、Watchdog 的既有顺序启动并核验。

本仓库基于 `tom-snow/docker-ComWechat` 的历史代码，并参考
`jiz4oh/docker-ComWechat` 的 Bridge 实现。
