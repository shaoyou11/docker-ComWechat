#!/usr/bin/python3
import hashlib
import json
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple


ACTIVE_STATES = ("staged", "pending", "inflight")
MAX_PULL_ITEMS = 500


def _as_text(value: Any) -> str:
    return str(value or "").strip()


def message_priority(message: Dict[str, Any]) -> int:
    """Return the delivery class used by the durable queue.

    Private conversations are deliberately preferred after login recovery, while
    group traffic remains ordered and is allowed to drain afterwards.
    """
    chat_type = _as_text(
        message.get("chat_type") or message.get("chatType") or message.get("conversation_type")
    ).lower()
    group_markers = {"group", "room", "chatroom", "群聊", "群组", "微信群"}
    if message.get("is_group") is True or message.get("isGroup") is True:
        return 10
    if chat_type in group_markers or any(marker in chat_type for marker in ("group", "room", "群")):
        return 10
    if any(message.get(key) for key in ("roomid", "room_id", "group_id", "chatroom_id")):
        return 10
    if chat_type in {"private", "contact", "friend", "direct", "私聊", "联系人"}:
        return 0
    if any(message.get(key) for key in ("sender", "from_user", "fromUser", "wxid")):
        return 0
    return 20


def source_chat_key(message: Dict[str, Any]) -> str:
    """Build a stable source conversation key for FIFO ordering."""
    for key in (
        "chat_id", "chatId", "conversation_id", "conversationId", "roomid",
        "room_id", "group_id", "chatroom_id", "from_user", "fromUser", "sender",
        "wxid", "chat",
    ):
        value = _as_text(message.get(key))
        if value:
            return value
    return "unknown"


@dataclass(frozen=True)
class ReliableQueueConfig:
    db_path: str
    lease_seconds: int = 120
    max_attempts: int = 10
    message_ttl_seconds: int = 7 * 24 * 60 * 60
    ack_retention_seconds: int = 7 * 24 * 60 * 60
    dead_retention_seconds: int = 30 * 24 * 60 * 60
    retry_delay_seconds: int = 30


def _canonical_json(message: Dict[str, Any]) -> str:
    return json.dumps(
        message,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def build_dedup_key(message: Dict[str, Any]) -> str:
    message_id = message.get("msgid")
    if message_id in (None, "", 0, "0"):
        message_id = message.get("id")
    if message_id not in (None, "", 0, "0"):
        parts = (
            str(message_id),
            str(message.get("type", "")),
            str(message.get("sender", "")),
            str(message.get("isSendMsg", "")),
        )
        return "msg:" + "|".join(parts)
    digest = hashlib.sha256(_canonical_json(message).encode("utf-8")).hexdigest()
    return "sha256:" + digest


class SQLiteMessageQueue:
    def __init__(
        self,
        config: ReliableQueueConfig,
        now_fn: Callable[[], float] = time.time,
    ):
        self.config = config
        self._now = now_fn
        self._condition = threading.Condition(threading.RLock())

        db_path = Path(config.db_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(
            str(db_path),
            timeout=30,
            isolation_level=None,
            check_same_thread=False,
        )
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=FULL")
        self._db.execute("PRAGMA busy_timeout=30000")
        self._create_schema()

    def _create_schema(self) -> None:
        with self._condition:
            self._db.executescript(
                """
                CREATE TABLE IF NOT EXISTS messages (
                    id TEXT PRIMARY KEY,
                    dedup_key TEXT NOT NULL UNIQUE,
                    payload TEXT NOT NULL,
                    state TEXT NOT NULL,
                    received_at REAL NOT NULL,
                    sort_at REAL NOT NULL,
                    priority INTEGER NOT NULL DEFAULT 20,
                    source_key TEXT NOT NULL DEFAULT 'unknown',
                    source_sequence INTEGER NOT NULL DEFAULT 0,
                    available_at REAL NOT NULL,
                    lease_token TEXT,
                    lease_owner TEXT,
                    lease_until REAL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    expires_at REAL NOT NULL,
                    acked_at REAL,
                    dead_at REAL,
                    last_error TEXT,
                    discarded_at REAL,
                    discard_reason TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_messages_delivery
                    ON messages(state, available_at, sort_at, received_at);
                CREATE INDEX IF NOT EXISTS idx_messages_lease
                    ON messages(state, lease_until);
                CREATE TABLE IF NOT EXISTS metrics (
                    key TEXT PRIMARY KEY,
                    value INTEGER NOT NULL DEFAULT 0
                );
                """
            )
            columns = {
                row[1]
                for row in self._db.execute("PRAGMA table_info(messages)").fetchall()
            }
            migrations = (
                ("priority", "INTEGER NOT NULL DEFAULT 20"),
                ("source_key", "TEXT NOT NULL DEFAULT 'unknown'"),
                ("source_sequence", "INTEGER NOT NULL DEFAULT 0"),
                ("discarded_at", "REAL"),
                ("discard_reason", "TEXT"),
            )
            for name, definition in migrations:
                if name not in columns:
                    self._db.execute(f"ALTER TABLE messages ADD COLUMN {name} {definition}")
            self._db.execute(
                "CREATE INDEX IF NOT EXISTS idx_messages_priority "
                "ON messages(state, priority, source_key, source_sequence, received_at)"
            )

    def _transaction(self):
        return _Transaction(self._db)

    def _increment_metric_locked(self, key: str, amount: int = 1) -> None:
        self._db.execute(
            """
            INSERT INTO metrics(key, value) VALUES(?, ?)
            ON CONFLICT(key) DO UPDATE SET value = value + excluded.value
            """,
            (key, amount),
        )

    def _next_release_sequence_locked(self) -> int:
        self._increment_metric_locked("release_sequence")
        row = self._db.execute(
            "SELECT value FROM metrics WHERE key='release_sequence'"
        ).fetchone()
        return int(row["value"])

    def _maintenance_locked(self, now: float) -> None:
        dead_cursor = self._db.execute(
            """
            UPDATE messages
               SET state='dead',
                   dead_at=?,
                   lease_token=NULL,
                   lease_owner=NULL,
                   lease_until=NULL,
                   last_error=CASE
                       WHEN expires_at <= ? THEN 'message expired'
                       ELSE 'maximum delivery attempts reached'
                   END
             WHERE state IN ('staged', 'pending', 'inflight')
               AND (
                    expires_at <= ?
                    OR (
                        state='inflight'
                        AND lease_until IS NOT NULL
                        AND lease_until <= ?
                        AND attempts >= ?
                    )
               )
            """,
            (now, now, now, now, self.config.max_attempts),
        )
        if dead_cursor.rowcount > 0:
            self._increment_metric_locked("dead_lettered_total", dead_cursor.rowcount)

        self._db.execute(
            """
            UPDATE messages
               SET state='pending',
                   available_at=?,
                   lease_token=NULL,
                   lease_owner=NULL,
                   lease_until=NULL
             WHERE state='inflight'
               AND lease_until IS NOT NULL
               AND lease_until <= ?
               AND attempts < ?
               AND expires_at > ?
            """,
            (now, now, self.config.max_attempts, now),
        )

        self._db.execute(
            "DELETE FROM messages WHERE state='acked' AND acked_at <= ?",
            (now - self.config.ack_retention_seconds,),
        )
        self._db.execute(
            "DELETE FROM messages WHERE state='dead' AND dead_at <= ?",
            (now - self.config.dead_retention_seconds,),
        )
        self._db.execute(
            "DELETE FROM messages WHERE state='discarded' AND discarded_at <= ?",
            (now - self.config.dead_retention_seconds,),
        )

    def stage(
        self,
        message: Dict[str, Any],
        sort_at: Optional[float] = None,
        priority: Optional[int] = None,
        source_key: Optional[str] = None,
    ) -> Tuple[str, str, bool]:
        payload = _canonical_json(message)
        dedup_key = build_dedup_key(message)
        now = self._now()
        message_id = uuid.uuid4().hex
        queue_priority = int(
            message_priority(message) if priority is None else priority
        )
        queue_priority = max(0, min(100, queue_priority))
        queue_source = str(source_key or message.get("_bridge_source_key") or source_chat_key(message))
        with self._condition:
            with self._transaction():
                self._maintenance_locked(now)
                sequence_row = self._db.execute(
                    "SELECT COALESCE(MAX(source_sequence), 0) AS sequence "
                    "FROM messages WHERE source_key=?",
                    (queue_source,),
                ).fetchone()
                source_sequence = int(sequence_row["sequence"] or 0) + 1
                cursor = self._db.execute(
                    """
                    INSERT OR IGNORE INTO messages(
                        id, dedup_key, payload, state, received_at, sort_at,
                        priority, source_key, source_sequence, available_at, expires_at
                    ) VALUES(?, ?, ?, 'staged', ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        message_id,
                        dedup_key,
                        payload,
                        now,
                        now if sort_at is None else float(sort_at),
                        queue_priority,
                        queue_source,
                        source_sequence,
                        now,
                        now + self.config.message_ttl_seconds,
                    ),
                )
                inserted = cursor.rowcount == 1
                if not inserted:
                    row = self._db.execute(
                        "SELECT id FROM messages WHERE dedup_key=?",
                        (dedup_key,),
                    ).fetchone()
                    message_id = row["id"]
                    self._increment_metric_locked("deduplicated_total")
            return message_id, dedup_key, inserted

    def release(self, message_ids: Iterable[str]) -> int:
        ids = list(dict.fromkeys(str(item) for item in message_ids))
        if not ids:
            return 0
        now = self._now()
        released = 0
        with self._condition:
            with self._transaction():
                self._maintenance_locked(now)
                for message_id in ids:
                    cursor = self._db.execute(
                        """
                        UPDATE messages
                           SET state='pending', available_at=?, sort_at=?
                         WHERE state='staged' AND id=?
                        """,
                        (
                            now,
                            self._next_release_sequence_locked(),
                            message_id,
                        ),
                    )
                    released += cursor.rowcount
            if released:
                self._condition.notify_all()
            return released

    def release_all_staged(self) -> int:
        now = self._now()
        released = 0
        with self._condition:
            with self._transaction():
                self._maintenance_locked(now)
                rows = self._db.execute(
                    """
                    SELECT id
                      FROM messages
                     WHERE state='staged'
                     ORDER BY received_at, id
                    """
                ).fetchall()
                for row in rows:
                    cursor = self._db.execute(
                        """
                        UPDATE messages
                           SET state='pending', available_at=?, sort_at=?
                         WHERE state='staged' AND id=?
                        """,
                        (
                            now,
                            self._next_release_sequence_locked(),
                            row["id"],
                        ),
                    )
                    released += cursor.rowcount
            if released:
                self._condition.notify_all()
            return released

    def recover_staged(self) -> int:
        return self.release_all_staged()

    def _claim_once_locked(
        self,
        max_items: int,
        ack_mode: bool,
        consumer_id: str,
    ) -> Dict[str, Any]:
        now = self._now()
        with self._transaction():
            self._maintenance_locked(now)
            rows = self._db.execute(
                """
                  SELECT id, dedup_key, payload, attempts
                    FROM messages
                   WHERE state='pending' AND available_at <= ?
                   ORDER BY priority, source_key, source_sequence, sort_at, received_at, id
                 LIMIT ?
                """,
                (now, min(MAX_PULL_ITEMS, max(1, int(max_items)))),
            ).fetchall()

            messages = []
            deliveries = []
            for row in rows:
                messages.append(json.loads(row["payload"]))
                attempts = int(row["attempts"]) + 1
                if ack_mode:
                    delivery_id = uuid.uuid4().hex
                    self._db.execute(
                        """
                        UPDATE messages
                           SET state='inflight',
                               lease_token=?,
                               lease_owner=?,
                               lease_until=?,
                               attempts=?
                         WHERE id=? AND state='pending'
                        """,
                        (
                            delivery_id,
                            consumer_id,
                            now + self.config.lease_seconds,
                            attempts,
                            row["id"],
                        ),
                    )
                    deliveries.append(
                        {
                            "message_id": row["id"],
                            "delivery_id": delivery_id,
                            "dedup_key": row["dedup_key"],
                            "attempts": attempts,
                        }
                    )
                else:
                    self._db.execute(
                        """
                        UPDATE messages
                           SET state='acked',
                               attempts=?,
                               acked_at=?,
                               lease_owner=?,
                               lease_until=NULL
                         WHERE id=? AND state='pending'
                        """,
                        (attempts, now, consumer_id, row["id"]),
                    )
                    self._increment_metric_locked("acked_total")

            counts = self._state_counts_locked()
            return {
                "messages": messages,
                "deliveries": deliveries,
                "queue_size": sum(counts.get(state, 0) for state in ACTIVE_STATES),
                "dead_letter_size": counts.get("dead", 0),
            }

    def _next_transition_delay_locked(self) -> Optional[float]:
        now = self._now()
        row = self._db.execute(
            """
            SELECT MIN(next_at) AS next_at
              FROM (
                    SELECT available_at AS next_at
                      FROM messages
                     WHERE state='pending' AND available_at > ?
                    UNION ALL
                    SELECT lease_until AS next_at
                      FROM messages
                     WHERE state='inflight' AND lease_until > ?
                    UNION ALL
                    SELECT expires_at AS next_at
                      FROM messages
                     WHERE state IN ('staged', 'pending', 'inflight')
                       AND expires_at > ?
                   )
            """,
            (now, now, now),
        ).fetchone()
        if row is None or row["next_at"] is None:
            return None
        return max(0.001, float(row["next_at"]) - now)

    def pull(
        self,
        max_items: int,
        wait_ms: int,
        ack_mode: bool = False,
        consumer_id: str = "legacy",
    ) -> Dict[str, Any]:
        timeout = max(0, int(wait_ms)) / 1000.0
        deadline = time.monotonic() + timeout
        with self._condition:
            while True:
                result = self._claim_once_locked(
                    max_items=max_items,
                    ack_mode=bool(ack_mode),
                    consumer_id=str(consumer_id or "legacy"),
                )
                if result["messages"] or timeout <= 0:
                    return result
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return result
                transition_delay = self._next_transition_delay_locked()
                if transition_delay is not None:
                    remaining = min(remaining, transition_delay)
                self._condition.wait(timeout=remaining)

    def ack(self, delivery_ids: Iterable[str], consumer_id: str) -> int:
        ids = list(dict.fromkeys(str(item) for item in delivery_ids))
        if not ids:
            return 0
        placeholders = ",".join("?" for _ in ids)
        now = self._now()
        with self._condition:
            with self._transaction():
                self._maintenance_locked(now)
                rows = self._db.execute(
                    f"""
                    SELECT id, state
                      FROM messages
                     WHERE lease_token IN ({placeholders})
                       AND lease_owner=?
                       AND state IN ('inflight', 'acked')
                    """,
                    ids + [str(consumer_id)],
                ).fetchall()
                inflight_ids = [row["id"] for row in rows if row["state"] == "inflight"]
                if inflight_ids:
                    update_placeholders = ",".join("?" for _ in inflight_ids)
                    self._db.execute(
                        f"""
                        UPDATE messages
                           SET state='acked',
                               acked_at=?,
                               lease_until=NULL,
                               last_error=NULL
                         WHERE id IN ({update_placeholders})
                        """,
                        [now] + inflight_ids,
                    )
                    self._increment_metric_locked("acked_total", len(inflight_ids))
                return len(rows)

    def nack(
        self,
        delivery_ids: Iterable[str],
        consumer_id: str,
        reason: str = "",
    ) -> int:
        ids = list(dict.fromkeys(str(item) for item in delivery_ids))
        if not ids:
            return 0
        placeholders = ",".join("?" for _ in ids)
        now = self._now()
        clean_reason = " ".join(str(reason).split())[:500]
        with self._condition:
            with self._transaction():
                self._maintenance_locked(now)
                rows = self._db.execute(
                    f"""
                    SELECT id, attempts, expires_at
                      FROM messages
                     WHERE lease_token IN ({placeholders})
                       AND lease_owner=?
                       AND state='inflight'
                    """,
                    ids + [str(consumer_id)],
                ).fetchall()
                for row in rows:
                    should_die = (
                        int(row["attempts"]) >= self.config.max_attempts
                        or float(row["expires_at"]) <= now
                    )
                    if should_die:
                        self._db.execute(
                            """
                            UPDATE messages
                               SET state='dead',
                                   dead_at=?,
                                   last_error=?,
                                   lease_token=NULL,
                                   lease_owner=NULL,
                                   lease_until=NULL
                             WHERE id=?
                            """,
                            (now, clean_reason, row["id"]),
                        )
                        self._increment_metric_locked("dead_lettered_total")
                    else:
                        self._db.execute(
                            """
                            UPDATE messages
                               SET state='pending',
                                   available_at=?,
                                   last_error=?,
                                   lease_token=NULL,
                                   lease_owner=NULL,
                                   lease_until=NULL
                             WHERE id=?
                            """,
                            (
                                now + self.config.retry_delay_seconds,
                                clean_reason,
                                row["id"],
                            ),
                        )
            if rows:
                self._condition.notify_all()
            return len(rows)

    def list_dead(self, limit: int = 20) -> List[Dict[str, Any]]:
        now = self._now()
        with self._condition:
            with self._transaction():
                self._maintenance_locked(now)
                rows = self._db.execute(
                    """
                    SELECT id, dedup_key, attempts, dead_at, last_error
                      FROM messages
                     WHERE state='dead'
                     ORDER BY dead_at DESC, received_at DESC
                     LIMIT ?
                    """,
                    (min(100, max(1, int(limit))),),
                ).fetchall()
        return [dict(row) for row in rows]

    def list_active(self, limit: int = 20) -> List[Dict[str, Any]]:
        now = self._now()
        with self._condition:
            with self._transaction():
                self._maintenance_locked(now)
                rows = self._db.execute(
                    """
                    SELECT id, state, received_at, sort_at, available_at,
                           attempts, lease_until, last_error, priority,
                           source_key, payload
                      FROM messages
                     WHERE state IN ('staged', 'pending', 'inflight')
                     ORDER BY priority ASC, sort_at ASC, received_at ASC
                     LIMIT ?
                    """,
                    (min(100, max(1, int(limit))),),
                ).fetchall()

        records = []
        for row in rows:
            try:
                payload = json.loads(row["payload"])
            except (TypeError, ValueError, json.JSONDecodeError):
                payload = {}
            if not isinstance(payload, dict):
                payload = {}
            records.append(
                {
                    "id": row["id"],
                    "state": row["state"],
                    "received_at": row["received_at"],
                    "available_at": row["available_at"],
                    "attempts": row["attempts"],
                    "last_error": row["last_error"],
                    "priority": row["priority"],
                    "source_key": row["source_key"],
                    "message": {
                        "type": payload.get("type"),
                        "msgid": payload.get("msgid") or payload.get("id"),
                        "sender": payload.get("sender"),
                        "wxid": payload.get("wxid"),
                        "filepath": payload.get("filepath") or payload.get("path"),
                        "thumb_path": payload.get("thumb_path"),
                        "timestamp": payload.get("timestamp") or payload.get("time"),
                        "content": str(
                            payload.get("message")
                            or payload.get("content")
                            or payload.get("text")
                            or ""
                        )[:200],
                    },
                }
            )
        return records

    def requeue_dead(self, message_id: str) -> bool:
        now = self._now()
        with self._condition:
            with self._transaction():
                cursor = self._db.execute(
                    """
                    UPDATE messages
                       SET state='pending',
                           available_at=?,
                           sort_at=?,
                           attempts=0,
                           expires_at=?,
                           lease_token=NULL,
                           lease_owner=NULL,
                           lease_until=NULL,
                           acked_at=NULL,
                           dead_at=NULL,
                           last_error=NULL
                     WHERE id=? AND state='dead'
                    """,
                    (
                        now,
                        self._next_release_sequence_locked(),
                        now + self.config.message_ttl_seconds,
                        str(message_id),
                    ),
                )
                changed = cursor.rowcount == 1
                if changed:
                    self._increment_metric_locked("requeued_total")
            if changed:
                self._condition.notify_all()
            return changed

    def requeue_all_dead(self) -> int:
        now = self._now()
        requeued = 0
        with self._condition:
            with self._transaction():
                self._maintenance_locked(now)
                rows = self._db.execute(
                    "SELECT id FROM messages WHERE state='dead' ORDER BY dead_at, received_at, id"
                ).fetchall()
                for row in rows:
                    cursor = self._db.execute(
                        """
                        UPDATE messages
                           SET state='pending',
                               available_at=?,
                               sort_at=?,
                               attempts=0,
                               expires_at=?,
                               lease_token=NULL,
                               lease_owner=NULL,
                               lease_until=NULL,
                               acked_at=NULL,
                               dead_at=NULL,
                               last_error=NULL
                         WHERE id=? AND state='dead'
                        """,
                        (
                            now,
                            self._next_release_sequence_locked(),
                            now + self.config.message_ttl_seconds,
                            row["id"],
                        ),
                    )
                    requeued += cursor.rowcount
                if requeued:
                    self._increment_metric_locked("requeued_total", requeued)
            if requeued:
                self._condition.notify_all()
            return requeued

    def retry_active(self, message_id: str) -> str:
        now = self._now()
        with self._condition:
            with self._transaction():
                self._maintenance_locked(now)
                row = self._db.execute(
                    "SELECT state FROM messages WHERE id=?",
                    (str(message_id),),
                ).fetchone()
                if row is None:
                    return "not_found"
                if row["state"] == "inflight":
                    return "inflight"
                if row["state"] not in ("staged", "pending"):
                    return "not_found"
                self._db.execute(
                    """
                    UPDATE messages
                       SET state='pending',
                           available_at=?,
                           sort_at=?,
                           lease_token=NULL,
                           lease_owner=NULL,
                           lease_until=NULL,
                           last_error=NULL
                     WHERE id=? AND state IN ('staged', 'pending')
                    """,
                    (now, self._next_release_sequence_locked(), str(message_id)),
                )
            self._condition.notify_all()
            return "retried"

    def discard_message(self, message_id: str, reason: str = "") -> str:
        now = self._now()
        clean_reason = " ".join(str(reason).split())[:500]
        with self._condition:
            with self._transaction():
                self._maintenance_locked(now)
                row = self._db.execute(
                    "SELECT state FROM messages WHERE id=?",
                    (str(message_id),),
                ).fetchone()
                if row is None:
                    return "not_found"
                if row["state"] == "inflight":
                    return "inflight"
                if row["state"] not in ("staged", "pending", "dead"):
                    return "not_found"
                cursor = self._db.execute(
                    """
                    UPDATE messages
                       SET state='discarded',
                           payload='{}',
                           discarded_at=?,
                           discard_reason=?,
                           lease_token=NULL,
                           lease_owner=NULL,
                           lease_until=NULL,
                           acked_at=NULL,
                           dead_at=NULL,
                           last_error=NULL
                     WHERE id=? AND state IN ('staged', 'pending', 'dead')
                    """,
                    (now, clean_reason, str(message_id)),
                )
                if cursor.rowcount != 1:
                    return "not_found"
                self._increment_metric_locked("discarded_total")
            self._condition.notify_all()
            return "discarded"

    def discard_all_dead(self, reason: str = "") -> int:
        now = self._now()
        clean_reason = " ".join(str(reason).split())[:500]
        with self._condition:
            with self._transaction():
                self._maintenance_locked(now)
                cursor = self._db.execute(
                    """
                    UPDATE messages
                       SET state='discarded',
                           payload='{}',
                           discarded_at=?,
                           discard_reason=?,
                           lease_token=NULL,
                           lease_owner=NULL,
                           lease_until=NULL,
                           acked_at=NULL,
                           dead_at=NULL,
                           last_error=NULL
                     WHERE state='dead'
                    """,
                    (now, clean_reason),
                )
                discarded = cursor.rowcount
                if discarded:
                    self._increment_metric_locked("discarded_total", discarded)
            if discarded:
                self._condition.notify_all()
            return discarded

    def _state_counts_locked(self) -> Dict[str, int]:
        return {
            row["state"]: int(row["count"])
            for row in self._db.execute(
                "SELECT state, COUNT(*) AS count FROM messages GROUP BY state"
            ).fetchall()
        }

    def snapshot(self) -> Dict[str, int]:
        now = self._now()
        with self._condition:
            with self._transaction():
                self._maintenance_locked(now)
                counts = self._state_counts_locked()
                priority_counts = {
                    str(row["priority"]): int(row["count"])
                    for row in self._db.execute(
                        "SELECT priority, COUNT(*) AS count "
                        "FROM messages WHERE state IN ('staged', 'pending', 'inflight') "
                        "GROUP BY priority ORDER BY priority"
                    ).fetchall()
                }
                metrics = {
                    row["key"]: int(row["value"])
                    for row in self._db.execute(
                        "SELECT key, value FROM metrics"
                    ).fetchall()
                }
            return {
                "staged_size": counts.get("staged", 0),
                "pending_size": counts.get("pending", 0),
                "inflight_size": counts.get("inflight", 0),
                "acked_size": counts.get("acked", 0),
                "dead_letter_size": counts.get("dead", 0),
                "discarded_size": counts.get("discarded", 0),
                "queue_size": sum(counts.get(state, 0) for state in ACTIVE_STATES),
                "acked_total": metrics.get("acked_total", 0),
                "deduplicated_total": metrics.get("deduplicated_total", 0),
                "dead_lettered_total": metrics.get("dead_lettered_total", 0),
                "priority_counts": priority_counts,
            }

    def close(self) -> None:
        with self._condition:
            self._db.close()


class _Transaction:
    def __init__(self, connection: sqlite3.Connection):
        self.connection = connection

    def __enter__(self):
        self.connection.execute("BEGIN IMMEDIATE")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            self.connection.execute("COMMIT")
        else:
            self.connection.execute("ROLLBACK")
        return False
