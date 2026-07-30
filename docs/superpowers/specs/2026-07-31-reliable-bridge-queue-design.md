# ComWechat Bridge 可靠消息队列设计

## 目标

把现有内存缓冲 Bridge 升级为可在 ComWechat、EFB 或 NAS 重启后恢复的可靠消息队列，并保留旧版无 ACK 消费方式的兼容能力。

本次覆盖：

- SQLite WAL 持久化队列
- 租约式拉取
- ACK 与 NACK
- 入站消息持久化去重
- 消费端持久化回执去重
- 消息过期、重试上限和死信保留
- 附件进入 EFB 后的持久待发
- 队列状态与死信数量监控

不复制微信附件文件本体。Bridge 只保存消息 JSON、附件路径和投递状态，附件仍由现有 WeChat Files 持久化挂载保存。

## 仓库边界

### docker-ComWechat

负责消息入站、SQLite 队列、租约、ACK/NACK、过期和死信。

新增 `reliable_queue.py`，避免继续扩大 `comwechat_bridge.py`。Bridge 的登录探测和启动消息排序仍由 `MessageBuffer` 负责；每条消息到达后先写入 SQLite 的 `staged` 状态，再进入内存排序。消息可投递时改为 `pending`。若容器在排序期间退出，下次启动把遗留 `staged` 消息恢复为 `pending`，确保不丢失。

### python-comwechatrobot-http

负责使用可靠拉取协议、依次分发消息、保存消费回执，并在成功后 ACK、失败后 NACK。

消费回执数据库使用 SQLite WAL，默认路径为 `/data/operations/state/bridge-consumer.db`。相同去重键已经成功处理时，不再次分发，只补发 ACK。

### efb-wechat-comwechat-slave

负责确保文件、图片、视频和语音等异步附件在 Bridge ACK 前已经进入持久待发文件。Telegram 同步发送成功、消息被策略过滤，或者附件持久待发记录写入成功，都视为 EFB 已安全接收。

### ehforwarderbot

只更新上述两个 Python 依赖的固定提交，构建并发布新的 EFB 镜像。

## SQLite 队列

数据库路径由 `COMWECHAT_BRIDGE_DB_PATH` 控制，生产值为：

`/var/lib/comwechat-bridge/queue.db`

Compose 把该目录挂载到 NAS：

`${EFB_HOST_ROOT}/comwechat/Bridge:/var/lib/comwechat-bridge`

消息表核心字段：

- `id`：Bridge 内部稳定消息 ID
- `dedup_key`：入站去重键，唯一索引
- `payload`：原始消息 JSON
- `state`：`staged`、`pending`、`inflight`、`acked` 或 `dead`
- `received_at`、`sort_at`：接收和排序时间
- `available_at`：允许再次投递的时间
- `lease_token`、`lease_until`：本轮租约
- `attempts`：租约次数
- `expires_at`：待处理消息有效期
- `acked_at`、`dead_at`、`last_error`：最终状态和诊断

默认参数：

- 租约：120 秒
- 最大投递次数：10 次
- 待处理有效期：7 天
- ACK 去重记录：7 天
- 死信保留：30 天
- NACK 重试延迟：30 秒

数据库每次操作前执行轻量维护：

1. 过期租约重新变为 `pending`。
2. 达到最大投递次数或超过 7 天的消息转为 `dead`。
3. 超过保留期的 `acked` 与 `dead` 记录逐条由 SQL 清理。

## 去重键

优先使用以下字段生成规范化去重键：

`msgid/id + type + sender + isSendMsg`

同一微信消息可能产生不同消息类型回调，因此不能只使用 `msgid`。缺少有效消息 ID 时，对去除 Bridge 内部字段后的规范 JSON 做 SHA256。

Bridge 对保留期内已经存在的去重键不重复入队。消费端同时保存已处理去重键，防止 Telegram 已处理但 ACK 请求失败时再次转发。

## API

### 兼容拉取

旧请求不带 `ack_mode`：

```json
{"max_items": 50, "wait_ms": 15000}
```

保持旧行为：返回后立即标记为 `acked`。

### 可靠拉取

新消费端请求：

```json
{
  "max_items": 50,
  "wait_ms": 15000,
  "ack_mode": true,
  "consumer_id": "efb"
}
```

响应：

```json
{
  "messages": [],
  "deliveries": [
    {
      "delivery_id": "本轮租约令牌",
      "dedup_key": "稳定去重键"
    }
  ],
  "queue_size": 0,
  "dead_letter_size": 0
}
```

`messages` 与 `deliveries` 按下标一一对应。

### ACK

`POST /v1/messages/ack`

```json
{"delivery_ids": ["租约令牌"], "consumer_id": "efb"}
```

只有当前有效租约能转为 `acked`。重复 ACK 幂等返回。

### NACK

`POST /v1/messages/nack`

```json
{
  "delivery_ids": ["租约令牌"],
  "consumer_id": "efb",
  "reason": "已脱敏错误摘要"
}
```

消息在 30 秒后重试；达到上限或已经过期则转为死信。

### 状态

`GET /healthz` 增加：

- `pending_size`
- `inflight_size`
- `dead_letter_size`
- `acked_total`
- `deduplicated_total`

健康状态不因存在死信直接变为失败，避免健康检查造成无限重启。死信通过日志和 EFB 运维状态提醒人工处理。

## 失败语义

- EFB 暂停：消息保留在 SQLite，租约超时后继续投递。
- EFB 分发失败：消费端发送 NACK。
- ACK 网络失败：消费端已写入持久回执；下次收到相同去重键时跳过再次分发并补 ACK。
- ComWechat 重启：`pending` 和 `inflight` 数据保留，过期租约恢复；`staged` 数据恢复为 `pending`。
- NAS 异常断电：SQLite WAL 与同步事务保护队列元数据。
- 队列过期或反复失败：进入死信，不自动无限重启 ComWechat 或 EFB。

该方案提供“至少一次投递”。Telegram 成功返回后恰好在消费回执落盘前断电，仍可能产生一次重复，但不会为了追求理论上的恰好一次而提前确认并承担丢消息风险。

## 测试与发布

自动化测试必须覆盖：

- 入站先落盘、重启恢复
- ACK 前不删除
- 租约超时重投
- ACK 幂等
- NACK 延迟重试
- 入站去重
- 消费回执去重
- 过期与最大次数转死信
- 兼容旧拉取
- 所有附件类型进入持久待发

发布顺序：

1. 发布兼容新旧协议的 ComWechat 镜像。
2. 发布支持可靠消费的 Python 包和 EFB 镜像。
3. 备份 NAS Compose、数据库、会话和原镜像。
4. 先升级 ComWechat，再升级 EFB。
5. 注入测试消息，验证 ACK、重投、去重和容器重启恢复。
6. 生成最终加密备份并上传私有 Release。

