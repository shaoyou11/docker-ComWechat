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
                    available_at REAL NOT NULL,
                    lease_token TEXT,
                    lease_owner TEXT,
                    lease_until REAL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    expires_at REAL NOT NULL,
                    acked_at REAL,
                    dead_at REAL,
                    last_error TEXT
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

    def stage(
        self,
        message: Dict[str, Any],
        sort_at: Optional[float] = None,
    ) -> Tuple[str, str, bool]:
        payload = _canonical_json(message)
        dedup_key = build_dedup_key(message)
        now = self._now()
        message_id = uuid.uuid4().hex
        with self._condition:
            with self._transaction():
                self._maintenance_locked(now)
                cursor = self._db.execute(
                    """
                    INSERT OR IGNORE INTO messages(
                        id, dedup_key, payload, state, received_at, sort_at,
                        available_at, expires_at
                    ) VALUES(?, ?, ?, 'staged', ?, ?, ?, ?)
                    """,
                    (
                        message_id,
                        dedup_key,
                        payload,
                        now,
                        now if sort_at is None else float(sort_at),
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
        placeholders = ",".join("?" for _ in ids)
        with self._condition:
            with self._transaction():
                self._maintenance_locked(now)
                cursor = self._db.execute(
                    f"""
                    UPDATE messages
                       SET state='pending', available_at=?
                     WHERE state='staged' AND id IN ({placeholders})
                    """,
                    [now] + ids,
                )
            if cursor.rowcount:
                self._condition.notify_all()
            return cursor.rowcount

    def release_all_staged(self) -> int:
        now = self._now()
        with self._condition:
            with self._transaction():
                self._maintenance_locked(now)
                cursor = self._db.execute(
                    """
                    UPDATE messages
                       SET state='pending', available_at=?
                     WHERE state='staged'
                    """,
                    (now,),
                )
            if cursor.rowcount:
                self._condition.notify_all()
            return cursor.rowcount

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
                 ORDER BY sort_at, received_at, id
                 LIMIT ?
                """,
                (now, max(1, int(max_items))),
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
                           SET state='acked', acked_at=?, lease_until=NULL
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
                "queue_size": sum(counts.get(state, 0) for state in ACTIVE_STATES),
                "acked_total": metrics.get("acked_total", 0),
                "deduplicated_total": metrics.get("deduplicated_total", 0),
                "dead_lettered_total": metrics.get("dead_lettered_total", 0),
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
