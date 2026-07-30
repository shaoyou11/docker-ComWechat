# ComWechat Bridge Reliable Queue Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a restart-safe ComWechat-to-EFB message transport with SQLite persistence, leases, ACK/NACK, durable deduplication, expiry, dead letters, and persistent attachment acceptance.

**Architecture:** ComWechat stages every inbound JSON message in SQLite before in-memory login ordering, then releases it for delivery. EFB leases messages through the existing pull endpoint, durably records successful processing, and ACKs; failed dispatches are NACKed for delayed retry. The EFB slave persists every asynchronous attachment before returning from its callback.

**Tech Stack:** Python 3, stdlib `sqlite3`, SQLite WAL, threaded HTTP server, `requests`, pytest/unittest, Docker, GitHub Actions, GHCR.

## Global Constraints

- Preserve the old pull request behavior when `ack_mode` is absent.
- Default lease is 120 seconds; retry delay is 30 seconds.
- Maximum delivery attempts are 10; pending TTL is 7 days; dead letters are retained for 30 days.
- Use `(msgid/id, type, sender, isSendMsg)` for normal deduplication and SHA256 for messages without a usable ID.
- Never persist attachment bytes inside the Bridge database.
- Do not expose Bridge ports outside the shared container network namespace.
- Do not automatically restart containers because dead letters exist.
- Back up every modified existing file before editing.

---

### Task 1: SQLite Reliable Queue

**Files:**
- Create: `reliable_queue.py`
- Modify: `tests/test_bridge.py`
- Test: `tests/test_reliable_queue.py`

**Interfaces:**
- Produces: `SQLiteMessageQueue(config)`, `stage(message, sort_at)`, `release(message_ids)`, `pull(max_items, ack_mode, consumer_id)`, `ack(delivery_ids, consumer_id)`, `nack(delivery_ids, consumer_id, reason)`, `snapshot()`.
- Produces: `build_dedup_key(message) -> str`.

- [ ] Write failing tests for durable stage/reopen recovery, ACK retention, lease expiry, NACK delay, deduplication, expiry/dead-letter transition, and legacy destructive pull.
- [ ] Run `python3 -m unittest tests.test_reliable_queue -v` and verify failures are caused by the missing module.
- [ ] Implement `reliable_queue.py` with SQLite WAL, explicit transactions, a process lock, retention maintenance, and condition-based long polling.
- [ ] Run `python3 -m unittest tests.test_reliable_queue -v` and verify all queue tests pass.
- [ ] Run the complete `python3 -m unittest discover -s tests -v` suite.
- [ ] Commit the queue implementation and tests.

### Task 2: Bridge API Integration

**Files:**
- Modify: `comwechat_bridge.py`
- Modify: `tests/test_bridge.py`
- Modify: `healthcheck.py`
- Modify: `Dockerfile`
- Modify: `docker-compose.yaml`

**Interfaces:**
- Consumes: `SQLiteMessageQueue`.
- Produces: reliable mode on `POST /v1/messages/pull`, `POST /v1/messages/ack`, `POST /v1/messages/nack`, and queue statistics on `GET /healthz`.

- [ ] Write failing API tests showing reliable pull returns aligned delivery metadata and that ACK/NACK call the queue correctly.
- [ ] Run the targeted tests and verify the new endpoint expectations fail.
- [ ] Wire `MessageBuffer` to stage before ordering and release when ready; recover staged rows during startup.
- [x] Extend the HTTP API; legacy responses are acknowledged only after the
  response body is written successfully.
- [ ] Add database configuration environment variables and create `/var/lib/comwechat-bridge`.
- [ ] Run all tests and verify restart recovery with a temporary database.
- [ ] Commit Bridge integration.

### Task 3: Reliable Python Consumer

**Files:**
- Create in `python-comwechatrobot-http`: `wechatrobot/BridgeReceiptStore.py`
- Modify in `python-comwechatrobot-http`: `wechatrobot/WeChatRobot.py`
- Modify in `python-comwechatrobot-http`: `tests/test_wechatrobot.py`
- Modify in `python-comwechatrobot-http`: `README.md`, `CHANGELOG.md`, version metadata

**Interfaces:**
- Produces: `BridgeReceiptStore.is_processed(dedup_key)`, `record_processed(dedup_key)`, `cleanup()`.
- Consumes: aligned `messages` and `deliveries`, ACK/NACK endpoints.

- [ ] Write failing tests for ACK after dispatch, NACK after dispatch failure, ACK replay for a processed dedup key, and persistence across consumer restart.
- [ ] Run targeted pytest tests and verify expected failures.
- [ ] Implement the receipt store and reliable pull flow with sanitized NACK reasons.
- [ ] Keep compatibility with Bridge responses that do not include delivery metadata.
- [ ] Run the full package test suite.
- [ ] Update documentation/version and commit.

### Task 4: Persist All Asynchronous Attachments

**Files:**
- Modify in `efb-wechat-comwechat-slave`: `efb_wechat_comwechat_slave/pending_files.py`
- Modify in `efb-wechat-comwechat-slave`: `efb_wechat_comwechat_slave/ComWechat.py`
- Modify or create tests in `efb-wechat-comwechat-slave/tests/`

**Interfaces:**
- Produces: an atomically saved pending record for share/file, image, video, voice, animation, and original-media download callbacks before the Bridge callback returns.

- [ ] Write failing tests showing non-share attachment records survive a new `PendingFileStore` instance.
- [ ] Run targeted tests and verify the existing share-only condition causes failure.
- [ ] Persist every asynchronous attachment record and strip only ephemeral observation fields before saving.
- [ ] Restore records without treating monotonic timestamps from an old process as current.
- [ ] Run all EFB slave tests.
- [ ] Commit attachment persistence.

### Task 5: Pin, Build, and Publish

**Files:**
- Modify in `ehforwarderbot`: `Dockerfile`
- Modify in `ehforwarderbot`: `README.md`

**Interfaces:**
- Consumes: immutable commits from Tasks 3 and 4.
- Produces: `ghcr.io/shaoyou11/efb:latest`.

- [ ] Back up the Dockerfile and update both immutable Git commit pins.
- [ ] Run repository tests and `git diff --check`.
- [ ] Commit and push all feature branches.
- [ ] Wait for GitHub Actions for ComWechat, Python consumer, EFB slave, and EFB image.
- [ ] Verify GHCR manifests and immutable rollback tags.

### Task 6: NAS Backup and Gray Deployment

**Files:**
- Modify on NAS: `/vol4/1000/docker/efb/docker-compose2.yaml`
- Modify in private config repo: `docker-compose.example.yaml`, `.env.example`, `README.md`

**Interfaces:**
- Adds persistent mount `${EFB_HOST_ROOT}/comwechat/Bridge:/var/lib/comwechat-bridge`.
- Adds `WECHATROBOT_RECEIPT_DB=/data/operations/state/bridge-consumer.db`.

- [ ] Capture container state, images, Compose, queue files, profiles, and checksums in a timestamped rollback directory.
- [ ] Create a stopped consistent snapshot of the ComWechat session and EFB mapping databases.
- [ ] Pull the new fixed images without changing running containers.
- [ ] Start an isolated canary with a temporary queue database and inject a synthetic message.
- [ ] Verify lease without ACK survives restart, ACK removes it, NACK retries it, duplicate ingress is suppressed, and expiry creates a dead letter.
- [ ] Update production Compose and start services in dependency order.
- [ ] Verify all four containers are healthy with restart count zero and a real message reaches EFB.
- [ ] Commit and push the sanitized private Compose.

### Task 7: Final Encrypted Backup and Recovery Record

**Files:**
- Create on NAS: timestamped encrypted backup under `encrypted-backups/`
- Upload to private GitHub Release: ciphertext and SHA256 only

**Interfaces:**
- Produces: a decryptable archive containing final Compose, profiles, queue configuration, mapping databases, and checksums.

- [ ] Run the existing encrypted backup script with the independent recovery key.
- [ ] Verify SHA256 on NAS and after download.
- [ ] Stream-decrypt without writing plaintext and confirm Compose plus mapping databases are present.
- [ ] Upload ciphertext and checksum to `shaoyou11/efb-config-private`.
- [ ] Record exact persistence, rollback tag, backup path, and remaining at-least-once duplicate boundary.
