from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from urllib.parse import unquote, urlparse

from .auth import hash_password
from .config import (
    BOOTSTRAP_ADMIN_EMAIL,
    DEMO_USER_PASSWORD,
    ENABLE_DEMO_ACCOUNTS,
    PROJECT_ROOT,
)

try:
    import psycopg2
except ModuleNotFoundError:  # pragma: no cover
    psycopg2 = None


_INITIALIZED_DATABASES: set[str] = set()
_INITIALIZED_DATABASES_LOCK = threading.Lock()


def _json_default(value):
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return str(value)


def json_dumps(payload: object) -> str:
    return json.dumps(payload, ensure_ascii=False, default=_json_default)


def json_loads(payload):
    if not payload:
        return None
    if isinstance(payload, (dict, list)):
        return payload
    return json.loads(payload)


@dataclass(frozen=True)
class DatabaseConfig:
    backend: str
    database: str
    host: str = ""
    port: int = 0
    user: str = ""
    password: str = ""


def parse_database_url(url: str) -> DatabaseConfig:
    parsed = urlparse(url)
    if parsed.scheme == "sqlite":
        raw_path = unquote(parsed.path or "")
        if not raw_path:
            raw_path = str(PROJECT_ROOT / "garmin_dashboard.db")
        return DatabaseConfig(backend="sqlite", database=raw_path)
    if parsed.scheme in {"postgres", "postgresql"}:
        return DatabaseConfig(
            backend="postgres",
            database=parsed.path.lstrip("/"),
            host=parsed.hostname or "127.0.0.1",
            port=parsed.port or 5432,
            user=unquote(parsed.username or ""),
            password=unquote(parsed.password or ""),
        )
    raise ValueError(f"Неподдерживаемый DATABASE_URL: {url}")


class Database:
    def __init__(self, url: str):
        self.url = url
        self.config = parse_database_url(url)

    def _rewrite_query(self, query: str) -> str:
        return query.replace("?", "%s") if self.config.backend == "postgres" else query

    def connect(self):
        if self.config.backend == "sqlite":
            db_path = Path(self.config.database)
            db_path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA foreign_keys = ON")
            return conn
        if psycopg2 is None:
            raise RuntimeError("Для PostgreSQL нужен psycopg2 или psycopg2-binary")
        return psycopg2.connect(
            dbname=self.config.database,
            user=self.config.user,
            password=self.config.password,
            host=self.config.host,
            port=self.config.port,
        )

    @contextmanager
    def transaction(self):
        conn = self.connect()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def execute(self, conn, query: str, params: tuple | list = ()):
        cursor = conn.cursor()
        cursor.execute(self._rewrite_query(query), params)
        return cursor

    def fetchall(self, conn, query: str, params: tuple | list = ()) -> list[dict]:
        cursor = self.execute(conn, query, params)
        rows = cursor.fetchall()
        columns = [item[0] for item in cursor.description] if cursor.description else []
        result = []
        for row in rows:
            result.append(dict(row) if isinstance(row, sqlite3.Row) else dict(zip(columns, row)))
        cursor.close()
        return result

    def fetchone(self, conn, query: str, params: tuple | list = ()) -> dict | None:
        cursor = self.execute(conn, query, params)
        row = cursor.fetchone()
        if row is None:
            cursor.close()
            return None
        result = dict(row) if isinstance(row, sqlite3.Row) else dict(zip([item[0] for item in cursor.description], row))
        cursor.close()
        return result

    def column_exists(self, conn, table_name: str, column_name: str) -> bool:
        if self.config.backend == "sqlite":
            rows = self.fetchall(conn, f"PRAGMA table_info({table_name})")
            return any(row.get("name") == column_name for row in rows)
        row = self.fetchone(
            conn,
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_name = ? AND column_name = ?
            """,
            (table_name, column_name),
        )
        return bool(row)

    def init_schema(self):
        if self.url in _INITIALIZED_DATABASES:
            return
        with _INITIALIZED_DATABASES_LOCK:
            if self.url in _INITIALIZED_DATABASES:
                return
            self._init_schema_impl()
            _INITIALIZED_DATABASES.add(self.url)

    def _init_schema_impl(self):
        with self.transaction() as conn:
            is_postgres = self.config.backend == "postgres"
            id_pk = "BIGSERIAL PRIMARY KEY" if is_postgres else "INTEGER PRIMARY KEY AUTOINCREMENT"
            bigint = "BIGINT" if is_postgres else "INTEGER"
            real = "DOUBLE PRECISION" if is_postgres else "REAL"
            json_type = "JSONB" if is_postgres else "TEXT"
            self.execute(
                conn,
                f"""
                CREATE TABLE IF NOT EXISTS accounts (
                    id {id_pk},
                    email TEXT NOT NULL UNIQUE,
                    password_hash TEXT NOT NULL,
                    first_name TEXT NOT NULL DEFAULT '',
                    last_name TEXT NOT NULL DEFAULT '',
                    role TEXT NOT NULL DEFAULT 'user',
                    is_active INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL DEFAULT '',
                    last_login_at TEXT NOT NULL DEFAULT ''
                )
                """,
            ).close()
            self.execute(
                conn,
                f"""
                CREATE TABLE IF NOT EXISTS user_sessions (
                    id {id_pk},
                    account_id {bigint} NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
                    session_token TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL DEFAULT '',
                    expires_at TEXT NOT NULL DEFAULT '',
                    last_seen_at TEXT NOT NULL DEFAULT '',
                    login_day TEXT NOT NULL DEFAULT ''
                )
                """,
            ).close()
            self.execute(
                conn,
                f"""
                CREATE TABLE IF NOT EXISTS user_preferences (
                    account_id {bigint} PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
                    swim_mode TEXT NOT NULL DEFAULT 'all',
                    period TEXT NOT NULL DEFAULT 'current_year',
                    days INTEGER,
                    target_distances TEXT NOT NULL DEFAULT '50,100,150,200,300,400,500,600,800,1000',
                    long_min_distance REAL NOT NULL DEFAULT 1000,
                    updated_at TEXT NOT NULL DEFAULT ''
                )
                """,
            ).close()
            self.execute(
                conn,
                f"""
                CREATE TABLE IF NOT EXISTS source_files (
                    id {id_pk},
                    owner_account_id {bigint} REFERENCES accounts(id) ON DELETE CASCADE,
                    file_path TEXT NOT NULL UNIQUE,
                    file_name TEXT NOT NULL,
                    original_file_name TEXT NOT NULL DEFAULT '',
                    file_hash TEXT NOT NULL DEFAULT '',
                    file_size {bigint} NOT NULL DEFAULT 0,
                    mtime_ns {bigint} NOT NULL DEFAULT 0,
                    parser_version INTEGER NOT NULL DEFAULT 0,
                    parse_status TEXT NOT NULL DEFAULT 'pending',
                    error_text TEXT NOT NULL DEFAULT '',
                    activity_key TEXT NOT NULL DEFAULT '',
                    uploaded_at TEXT NOT NULL DEFAULT '',
                    ingested_at TEXT NOT NULL DEFAULT ''
                )
                """,
            ).close()
            self.execute(
                conn,
                f"""
                CREATE TABLE IF NOT EXISTS activities (
                    id {id_pk},
                    owner_account_id {bigint} REFERENCES accounts(id) ON DELETE CASCADE,
                    source_file_id {bigint} NOT NULL UNIQUE REFERENCES source_files(id) ON DELETE CASCADE,
                    activity_key TEXT NOT NULL UNIQUE,
                    activity_date TEXT NOT NULL DEFAULT '',
                    garmin_user_id TEXT NOT NULL DEFAULT '',
                    garmin_user_name TEXT NOT NULL DEFAULT '',
                    sport TEXT NOT NULL DEFAULT '',
                    sub_sport TEXT NOT NULL DEFAULT '',
                    swim_type TEXT NOT NULL DEFAULT '',
                    total_distance_m {real} NOT NULL DEFAULT 0,
                    total_time_s {real} NOT NULL DEFAULT 0
                )
                """,
            ).close()
            self.execute(
                conn,
                f"""
                CREATE TABLE IF NOT EXISTS activity_payloads (
                    activity_id {bigint} PRIMARY KEY REFERENCES activities(id) ON DELETE CASCADE,
                    raw_json {json_type} NOT NULL
                )
                """,
            ).close()
            self.execute(
                conn,
                f"""
                CREATE TABLE IF NOT EXISTS intervals (
                    id {id_pk},
                    owner_account_id {bigint} REFERENCES accounts(id) ON DELETE CASCADE,
                    activity_id {bigint} NOT NULL REFERENCES activities(id) ON DELETE CASCADE,
                    interval_index INTEGER NOT NULL,
                    file_name TEXT NOT NULL DEFAULT '',
                    activity_key TEXT NOT NULL DEFAULT '',
                    activity_date TEXT NOT NULL DEFAULT '',
                    garmin_user_id TEXT NOT NULL DEFAULT '',
                    garmin_user_name TEXT NOT NULL DEFAULT '',
                    lap_start TEXT NOT NULL DEFAULT '',
                    lap_end TEXT NOT NULL DEFAULT '',
                    distance_m {real} NOT NULL DEFAULT 0,
                    raw_distance_m {real} NOT NULL DEFAULT 0,
                    time_s {real} NOT NULL DEFAULT 0,
                    time_text TEXT NOT NULL DEFAULT '',
                    workout_total_distance_m {real} NOT NULL DEFAULT 0,
                    workout_total_time_s {real} NOT NULL DEFAULT 0,
                    stroke TEXT NOT NULL DEFAULT '',
                    swim_type TEXT NOT NULL DEFAULT '',
                    pace_100m_s {real} NOT NULL DEFAULT 0,
                    pace_100m TEXT NOT NULL DEFAULT ''
                )
                """,
            ).close()
            self.execute(
                conn,
                f"""
                CREATE TABLE IF NOT EXISTS monthly_history (
                    id {id_pk},
                    owner_account_id {bigint} REFERENCES accounts(id) ON DELETE CASCADE,
                    year INTEGER NOT NULL,
                    month INTEGER NOT NULL,
                    distance_m INTEGER NOT NULL,
                    best_pace_s {real} NOT NULL DEFAULT 0,
                    best_pace_text TEXT NOT NULL DEFAULT '',
                    UNIQUE (owner_account_id, year, month, distance_m)
                )
                """,
            ).close()
            self.execute(
                conn,
                f"""
                CREATE TABLE IF NOT EXISTS background_jobs (
                    id {id_pk},
                    owner_account_id {bigint} REFERENCES accounts(id) ON DELETE CASCADE,
                    job_type TEXT NOT NULL DEFAULT 'ingest',
                    status TEXT NOT NULL DEFAULT 'queued',
                    stage TEXT NOT NULL DEFAULT 'queued',
                    total_files INTEGER NOT NULL DEFAULT 0,
                    processed_files INTEGER NOT NULL DEFAULT 0,
                    skipped_files INTEGER NOT NULL DEFAULT 0,
                    duplicate_files INTEGER NOT NULL DEFAULT 0,
                    error_files INTEGER NOT NULL DEFAULT 0,
                    parsed_rows INTEGER NOT NULL DEFAULT 0,
                    progress_percent INTEGER NOT NULL DEFAULT 0,
                    error_text TEXT NOT NULL DEFAULT '',
                    payload_json {json_type},
                    created_at TEXT NOT NULL DEFAULT '',
                    started_at TEXT NOT NULL DEFAULT '',
                    finished_at TEXT NOT NULL DEFAULT ''
                )
                """,
            ).close()
            self.execute(
                conn,
                f"""
                CREATE TABLE IF NOT EXISTS auth_codes (
                    id {id_pk},
                    email TEXT NOT NULL,
                    purpose TEXT NOT NULL,
                    code TEXT NOT NULL,
                    payload_json {json_type},
                    created_at TEXT NOT NULL DEFAULT '',
                    expires_at TEXT NOT NULL DEFAULT '',
                    consumed_at TEXT NOT NULL DEFAULT ''
                )
                """,
            ).close()
            self.execute(
                conn,
                f"""
                CREATE TABLE IF NOT EXISTS audit_log (
                    id {id_pk},
                    actor_account_id {bigint},
                    target_account_id {bigint},
                    event_type TEXT NOT NULL,
                    payload_json {json_type},
                    created_at TEXT NOT NULL DEFAULT ''
                )
                """,
            ).close()
            self.execute(
                conn,
                f"""
                CREATE TABLE IF NOT EXISTS login_attempts (
                    id {id_pk},
                    email TEXT NOT NULL DEFAULT '',
                    attempted_at TEXT NOT NULL DEFAULT '',
                    was_success INTEGER NOT NULL DEFAULT 0
                )
                """,
            ).close()

            legacy_columns = {
                "user_sessions": {
                    "last_seen_at": "TEXT NOT NULL DEFAULT ''",
                    "login_day": "TEXT NOT NULL DEFAULT ''",
                },
                "source_files": {
                    "owner_account_id": f"{bigint} REFERENCES accounts(id) ON DELETE CASCADE",
                    "original_file_name": "TEXT NOT NULL DEFAULT ''",
                    "uploaded_at": "TEXT NOT NULL DEFAULT ''",
                },
                "activities": {
                    "owner_account_id": f"{bigint} REFERENCES accounts(id) ON DELETE CASCADE",
                    "garmin_user_id": "TEXT NOT NULL DEFAULT ''",
                    "garmin_user_name": "TEXT NOT NULL DEFAULT ''",
                },
                "intervals": {
                    "owner_account_id": f"{bigint} REFERENCES accounts(id) ON DELETE CASCADE",
                    "garmin_user_id": "TEXT NOT NULL DEFAULT ''",
                    "garmin_user_name": "TEXT NOT NULL DEFAULT ''",
                },
            }
            for table_name, columns in legacy_columns.items():
                for column_name, column_sql in columns.items():
                    if not self.column_exists(conn, table_name, column_name):
                        self.execute(conn, f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}").close()

            index_statements = [
                "CREATE INDEX IF NOT EXISTS idx_accounts_email ON accounts(email)",
                "CREATE INDEX IF NOT EXISTS idx_sessions_token ON user_sessions(session_token)",
                "CREATE INDEX IF NOT EXISTS idx_jobs_owner_created ON background_jobs(owner_account_id, created_at)",
                "CREATE INDEX IF NOT EXISTS idx_jobs_status_created ON background_jobs(status, created_at)",
                "CREATE INDEX IF NOT EXISTS idx_auth_codes_email_purpose ON auth_codes(email, purpose, created_at)",
                "CREATE INDEX IF NOT EXISTS idx_audit_log_created ON audit_log(created_at)",
                "CREATE INDEX IF NOT EXISTS idx_login_attempts_email_time ON login_attempts(email, attempted_at)",
                "CREATE INDEX IF NOT EXISTS idx_source_files_owner_hash ON source_files(owner_account_id, file_hash)",
                "CREATE INDEX IF NOT EXISTS idx_source_files_owner_status ON source_files(owner_account_id, parse_status)",
                "CREATE INDEX IF NOT EXISTS idx_activities_owner_date ON activities(owner_account_id, activity_date)",
                "CREATE INDEX IF NOT EXISTS idx_intervals_owner_activity ON intervals(owner_account_id, activity_id)",
                "CREATE INDEX IF NOT EXISTS idx_monthly_owner_month ON monthly_history(owner_account_id, year, month)",
            ]
            for statement in index_statements:
                self.execute(conn, statement).close()
            self.ensure_bootstrap_accounts(conn)

    def ensure_bootstrap_accounts(self, conn) -> None:
        if BOOTSTRAP_ADMIN_EMAIL:
            existing = self.find_account_by_email(conn, BOOTSTRAP_ADMIN_EMAIL)
            if existing:
                self.execute(
                    conn,
                    """
                    UPDATE accounts
                    SET role = 'admin', is_active = 1
                    WHERE id = ?
                    """,
                    (int(existing["id"]),),
                ).close()

        demo_accounts = [
            ("demo.alpha@local", "Demo", "Alpha"),
            ("demo.beta@local", "Demo", "Beta"),
            ("demo.gamma@local", "Demo", "Gamma"),
            ("demo.delta@local", "Demo", "Delta"),
        ]
        if not ENABLE_DEMO_ACCOUNTS:
            for email, _, _ in demo_accounts:
                demo = self.find_account_by_email(conn, email)
                if demo:
                    self.delete_account(conn, int(demo["id"]))
            return
        created_at = datetime.now().isoformat(timespec="seconds")
        for email, first_name, last_name in demo_accounts:
            if self.find_account_by_email(conn, email):
                continue
            self.create_account(
                conn,
                email=email,
                password_hash=hash_password(DEMO_USER_PASSWORD),
                first_name=first_name,
                last_name=last_name,
                role="user",
                created_at=created_at,
            )

    def count_accounts(self, conn) -> int:
        row = self.fetchone(conn, "SELECT COUNT(*) AS count FROM accounts")
        return int(row["count"] if row else 0)

    def count_admin_accounts(self, conn) -> int:
        row = self.fetchone(conn, "SELECT COUNT(*) AS count FROM accounts WHERE role = 'admin'")
        return int(row["count"] if row else 0)

    def create_account(self, conn, *, email: str, password_hash: str, first_name: str, last_name: str, role: str, created_at: str) -> int:
        self.execute(
            conn,
            """
            INSERT INTO accounts (email, password_hash, first_name, last_name, role, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (email, password_hash, first_name, last_name, role, created_at),
        ).close()
        row = self.fetchone(conn, "SELECT id FROM accounts WHERE email = ?", (email,))
        return int(row["id"])

    def find_account_by_email(self, conn, email: str) -> dict | None:
        return self.fetchone(
            conn,
            """
            SELECT id, email, password_hash, first_name, last_name, role, is_active, created_at, last_login_at
            FROM accounts
            WHERE lower(email) = lower(?)
            """,
            (email,),
        )

    def find_account_by_id(self, conn, account_id: int) -> dict | None:
        return self.fetchone(
            conn,
            """
            SELECT id, email, password_hash, first_name, last_name, role, is_active, created_at, last_login_at
            FROM accounts
            WHERE id = ?
            """,
            (account_id,),
        )

    def update_last_login(self, conn, account_id: int, timestamp: str) -> None:
        self.execute(
            conn,
            "UPDATE accounts SET last_login_at = ? WHERE id = ?",
            (timestamp, account_id),
        ).close()

    def create_session(
        self,
        conn,
        account_id: int,
        token: str,
        created_at: str,
        expires_at: str,
        *,
        last_seen_at: str | None = None,
        login_day: str | None = None,
    ) -> None:
        self.execute(
            conn,
            """
            INSERT INTO user_sessions (account_id, session_token, created_at, expires_at, last_seen_at, login_day)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (account_id, token, created_at, expires_at, last_seen_at or created_at, login_day or created_at[:10]),
        ).close()

    def delete_session(self, conn, token: str) -> None:
        self.execute(conn, "DELETE FROM user_sessions WHERE session_token = ?", (token,)).close()

    def delete_expired_sessions(self, conn, now_text: str) -> None:
        self.execute(conn, "DELETE FROM user_sessions WHERE expires_at <> '' AND expires_at < ?", (now_text,)).close()

    def delete_idle_sessions(self, conn, idle_cutoff: str) -> None:
        self.execute(
            conn,
            """
            DELETE FROM user_sessions
            WHERE COALESCE(last_seen_at, '') <> '' AND last_seen_at < ?
            """,
            (idle_cutoff,),
        ).close()

    def touch_session(self, conn, token: str, last_seen_at: str) -> None:
        self.execute(
            conn,
            "UPDATE user_sessions SET last_seen_at = ? WHERE session_token = ?",
            (last_seen_at, token),
        ).close()

    def find_account_by_session(self, conn, token: str, now_text: str) -> dict | None:
        self.delete_expired_sessions(conn, now_text)
        return self.fetchone(
            conn,
            """
            SELECT
                accounts.id,
                accounts.email,
                accounts.first_name,
                accounts.last_name,
                accounts.role,
                accounts.is_active,
                accounts.created_at,
                accounts.last_login_at,
                user_sessions.created_at AS session_created_at,
                user_sessions.expires_at AS session_expires_at,
                user_sessions.last_seen_at AS session_last_seen_at,
                user_sessions.login_day AS session_login_day
            FROM user_sessions
            INNER JOIN accounts ON accounts.id = user_sessions.account_id
            WHERE user_sessions.session_token = ?
            """,
            (token,),
        )

    def list_accounts_with_stats(self, conn) -> list[dict]:
        return self.fetchall(
            conn,
            """
            SELECT
                accounts.id,
                accounts.email,
                accounts.first_name,
                accounts.last_name,
                accounts.role,
                accounts.is_active,
                accounts.created_at,
                accounts.last_login_at,
                COALESCE(files.files_count, 0) AS files_count,
                COALESCE(activities.activities_count, 0) AS activities_count,
                COALESCE(intervals.intervals_count, 0) AS intervals_count,
                COALESCE(activities.last_activity_date, '') AS last_activity_date,
                COALESCE(user_preferences.period, '') AS period,
                COALESCE(user_preferences.target_distances, '') AS target_distances
            FROM accounts
            LEFT JOIN (
                SELECT owner_account_id, COUNT(*) AS files_count
                FROM source_files
                GROUP BY owner_account_id
            ) AS files ON files.owner_account_id = accounts.id
            LEFT JOIN (
                SELECT owner_account_id, COUNT(*) AS activities_count, MAX(activity_date) AS last_activity_date
                FROM activities
                GROUP BY owner_account_id
            ) AS activities ON activities.owner_account_id = accounts.id
            LEFT JOIN (
                SELECT owner_account_id, COUNT(*) AS intervals_count
                FROM intervals
                GROUP BY owner_account_id
            ) AS intervals ON intervals.owner_account_id = accounts.id
            LEFT JOIN (
                SELECT account_id, period, target_distances
                FROM user_preferences
            ) AS user_preferences ON user_preferences.account_id = accounts.id
            ORDER BY accounts.created_at DESC, accounts.id DESC
            """
        )

    def admin_overview(self, conn) -> dict:
        users_row = self.fetchone(
            conn,
            """
            SELECT
                COUNT(*) AS total_users,
                SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) AS active_users
            FROM accounts
            """,
        ) or {}
        files_row = self.fetchone(conn, "SELECT COUNT(*) AS total_files FROM source_files") or {}
        activities_row = self.fetchone(conn, "SELECT COUNT(*) AS total_activities FROM activities") or {}
        intervals_row = self.fetchone(conn, "SELECT COUNT(*) AS total_intervals FROM intervals") or {}
        return {
            "total_users": int(users_row.get("total_users") or 0),
            "active_users": int(users_row.get("active_users") or 0),
            "total_files": int(files_row.get("total_files") or 0),
            "total_activities": int(activities_row.get("total_activities") or 0),
            "total_intervals": int(intervals_row.get("total_intervals") or 0),
        }

    def list_recent_logins(self, conn, limit: int = 10) -> list[dict]:
        return self.fetchall(
            conn,
            """
            SELECT
                id,
                email,
                first_name,
                last_name,
                role,
                last_login_at
            FROM accounts
            WHERE COALESCE(last_login_at, '') <> ''
            ORDER BY last_login_at DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        )

    def list_recent_uploads(self, conn, limit: int = 20) -> list[dict]:
        return self.fetchall(
            conn,
            """
            SELECT
                source_files.id,
                source_files.original_file_name,
                source_files.file_name,
                source_files.parse_status,
                source_files.uploaded_at,
                source_files.ingested_at,
                source_files.file_size,
                accounts.id AS account_id,
                accounts.email,
                accounts.first_name,
                accounts.last_name
            FROM source_files
            INNER JOIN accounts ON accounts.id = source_files.owner_account_id
            WHERE COALESCE(source_files.uploaded_at, '') <> ''
            ORDER BY source_files.uploaded_at DESC, source_files.id DESC
            LIMIT ?
            """,
            (limit,),
        )

    def update_account(self, conn, account_id: int, *, role: str, is_active: int) -> None:
        self.execute(
            conn,
            "UPDATE accounts SET role = ?, is_active = ? WHERE id = ?",
            (role, is_active, account_id),
        ).close()

    def update_account_password(self, conn, account_id: int, password_hash: str) -> None:
        self.execute(
            conn,
            "UPDATE accounts SET password_hash = ? WHERE id = ?",
            (password_hash, account_id),
        ).close()

    def delete_account(self, conn, account_id: int) -> None:
        self.execute(conn, "DELETE FROM accounts WHERE id = ?", (account_id,)).close()

    def get_user_preferences(self, conn, account_id: int) -> dict | None:
        return self.fetchone(
            conn,
            """
            SELECT swim_mode, period, days, target_distances, long_min_distance, updated_at
            FROM user_preferences
            WHERE account_id = ?
            """,
            (account_id,),
        )

    def save_user_preferences(
        self,
        conn,
        account_id: int,
        *,
        swim_mode: str,
        period: str,
        days: int | None,
        target_distances: str,
        long_min_distance: float,
        updated_at: str,
    ) -> None:
        self.execute(
            conn,
            """
            INSERT INTO user_preferences (
                account_id,
                swim_mode,
                period,
                days,
                target_distances,
                long_min_distance,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(account_id) DO UPDATE SET
                swim_mode = excluded.swim_mode,
                period = excluded.period,
                days = excluded.days,
                target_distances = excluded.target_distances,
                long_min_distance = excluded.long_min_distance,
                updated_at = excluded.updated_at
            """,
            (account_id, swim_mode, period, days, target_distances, long_min_distance, updated_at),
        ).close()

    def find_existing_file_by_hash(self, conn, owner_account_id: int, file_hash: str) -> dict | None:
        return self.fetchone(
            conn,
            """
            SELECT id, parse_status, activity_key, file_name, file_path
            FROM source_files
            WHERE owner_account_id = ? AND file_hash = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (owner_account_id, file_hash),
        )

    def create_background_job(self, conn, *, owner_account_id: int, job_type: str, total_files: int, payload_json: str, created_at: str) -> int:
        self.execute(
            conn,
            """
            INSERT INTO background_jobs (
                owner_account_id,
                job_type,
                status,
                stage,
                total_files,
                payload_json,
                created_at
            ) VALUES (?, ?, 'queued', 'queued', ?, ?, ?)
            """,
            (owner_account_id, job_type, total_files, payload_json, created_at),
        ).close()
        row = self.fetchone(conn, "SELECT id FROM background_jobs ORDER BY id DESC LIMIT 1")
        return int(row["id"])

    def update_background_job(
        self,
        conn,
        job_id: int,
        *,
        status: str | None = None,
        stage: str | None = None,
        processed_files: int | None = None,
        skipped_files: int | None = None,
        duplicate_files: int | None = None,
        error_files: int | None = None,
        parsed_rows: int | None = None,
        progress_percent: int | None = None,
        error_text: str | None = None,
        started_at: str | None = None,
        finished_at: str | None = None,
        payload_json: str | None = None,
    ) -> None:
        updates = []
        params: list = []
        mapping = {
            "status": status,
            "stage": stage,
            "processed_files": processed_files,
            "skipped_files": skipped_files,
            "duplicate_files": duplicate_files,
            "error_files": error_files,
            "parsed_rows": parsed_rows,
            "progress_percent": progress_percent,
            "error_text": error_text,
            "started_at": started_at,
            "finished_at": finished_at,
            "payload_json": payload_json,
        }
        for key, value in mapping.items():
            if value is not None:
                updates.append(f"{key} = ?")
                params.append(value)
        if not updates:
            return
        params.append(job_id)
        self.execute(conn, f"UPDATE background_jobs SET {', '.join(updates)} WHERE id = ?", tuple(params)).close()

    def get_background_job(self, conn, job_id: int) -> dict | None:
        return self.fetchone(
            conn,
            """
            SELECT id, owner_account_id, job_type, status, stage, total_files, processed_files, skipped_files,
                   duplicate_files, error_files, parsed_rows, progress_percent, error_text, payload_json,
                   created_at, started_at, finished_at
            FROM background_jobs
            WHERE id = ?
            """,
            (job_id,),
        )

    def list_background_jobs(self, conn, owner_account_id: int, limit: int = 20) -> list[dict]:
        return self.fetchall(
            conn,
            """
            SELECT id, owner_account_id, job_type, status, stage, total_files, processed_files, skipped_files,
                   duplicate_files, error_files, parsed_rows, progress_percent, error_text, payload_json,
                   created_at, started_at, finished_at
            FROM background_jobs
            WHERE owner_account_id = ?
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            (owner_account_id, limit),
        )

    def create_auth_code(self, conn, *, email: str, purpose: str, code: str, payload_json: str, created_at: str, expires_at: str) -> int:
        self.execute(
            conn,
            """
            INSERT INTO auth_codes (email, purpose, code, payload_json, created_at, expires_at, consumed_at)
            VALUES (?, ?, ?, ?, ?, ?, '')
            """,
            (email, purpose, code, payload_json, created_at, expires_at),
        ).close()
        row = self.fetchone(conn, "SELECT id FROM auth_codes ORDER BY id DESC LIMIT 1")
        return int(row["id"])

    def consume_auth_code(self, conn, *, email: str, purpose: str, code: str, now_text: str) -> dict | None:
        record = self.fetchone(
            conn,
            """
            SELECT id, email, purpose, code, payload_json, created_at, expires_at, consumed_at
            FROM auth_codes
            WHERE lower(email) = lower(?) AND purpose = ? AND code = ?
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            (email, purpose, code),
        )
        if not record:
            return None
        if str(record.get("consumed_at") or "").strip():
            return None
        if str(record.get("expires_at") or "") < now_text:
            return None
        self.execute(conn, "UPDATE auth_codes SET consumed_at = ? WHERE id = ?", (now_text, int(record["id"]))).close()
        return record

    def record_login_attempt(self, conn, *, email: str, attempted_at: str, was_success: bool) -> None:
        self.execute(
            conn,
            """
            INSERT INTO login_attempts (email, attempted_at, was_success)
            VALUES (?, ?, ?)
            """,
            (email, attempted_at, 1 if was_success else 0),
        ).close()

    def count_recent_failed_logins(self, conn, *, email: str, attempted_after: str) -> int:
        row = self.fetchone(
            conn,
            """
            SELECT COUNT(*) AS count
            FROM login_attempts
            WHERE lower(email) = lower(?) AND attempted_at >= ? AND was_success = 0
            """,
            (email, attempted_after),
        ) or {}
        return int(row.get("count") or 0)

    def append_audit_log(
        self,
        conn,
        *,
        actor_account_id: int | None,
        target_account_id: int | None,
        event_type: str,
        payload_json: str,
        created_at: str,
    ) -> None:
        self.execute(
            conn,
            """
            INSERT INTO audit_log (actor_account_id, target_account_id, event_type, payload_json, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (actor_account_id, target_account_id, event_type, payload_json, created_at),
        ).close()

    def list_audit_log(self, conn, limit: int = 50) -> list[dict]:
        return self.fetchall(
            conn,
            """
            SELECT
                audit_log.id,
                audit_log.actor_account_id,
                audit_log.target_account_id,
                audit_log.event_type,
                audit_log.payload_json,
                audit_log.created_at,
                actor.email AS actor_email,
                target.email AS target_email
            FROM audit_log
            LEFT JOIN accounts AS actor ON actor.id = audit_log.actor_account_id
            LEFT JOIN accounts AS target ON target.id = audit_log.target_account_id
            ORDER BY audit_log.created_at DESC, audit_log.id DESC
            LIMIT ?
            """,
            (limit,),
        )

    def db_activity_key(self, owner_account_id: int | None, activity_key: str) -> str:
        if owner_account_id is None:
            return activity_key
        return f"u{owner_account_id}:{activity_key}"

    def upsert_source_file(
        self,
        conn,
        *,
        owner_account_id: int | None,
        file_path: str,
        file_name: str,
        original_file_name: str,
        file_hash: str,
        file_size: int,
        mtime_ns: int,
        parser_version: int,
        parse_status: str,
        error_text: str,
        activity_key: str,
        uploaded_at: str,
        ingested_at: str,
    ) -> int:
        self.execute(
            conn,
            """
            INSERT INTO source_files (
                owner_account_id,
                file_path,
                file_name,
                original_file_name,
                file_hash,
                file_size,
                mtime_ns,
                parser_version,
                parse_status,
                error_text,
                activity_key,
                uploaded_at,
                ingested_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(file_path) DO UPDATE SET
                owner_account_id = excluded.owner_account_id,
                file_name = excluded.file_name,
                original_file_name = excluded.original_file_name,
                file_hash = excluded.file_hash,
                file_size = excluded.file_size,
                mtime_ns = excluded.mtime_ns,
                parser_version = excluded.parser_version,
                parse_status = excluded.parse_status,
                error_text = excluded.error_text,
                activity_key = excluded.activity_key,
                uploaded_at = excluded.uploaded_at,
                ingested_at = excluded.ingested_at
            """,
            (
                owner_account_id,
                file_path,
                file_name,
                original_file_name,
                file_hash,
                file_size,
                mtime_ns,
                parser_version,
                parse_status,
                error_text,
                activity_key,
                uploaded_at,
                ingested_at,
            ),
        ).close()
        row = self.fetchone(conn, "SELECT id FROM source_files WHERE file_path = ?", (file_path,))
        return int(row["id"])

    def fetch_activity_by_key(self, conn, owner_account_id: int | None, activity_key: str) -> dict | None:
        if owner_account_id is None:
            return self.fetchone(conn, "SELECT id, source_file_id, activity_key FROM activities WHERE activity_key = ?", (activity_key,))
        scoped_key = self.db_activity_key(owner_account_id, activity_key)
        return self.fetchone(
            conn,
            """
            SELECT id, source_file_id, activity_key
            FROM activities
            WHERE owner_account_id = ? AND activity_key IN (?, ?)
            """,
            (owner_account_id, scoped_key, activity_key),
        )

    def fetch_activity_payload_by_key(self, conn, owner_account_id: int | None, activity_key: str):
        if owner_account_id is None:
            row = self.fetchone(
                conn,
                """
                SELECT ap.raw_json
                FROM activity_payloads ap
                INNER JOIN activities a ON a.id = ap.activity_id
                WHERE a.activity_key = ?
                """,
                (activity_key,),
            )
            return json_loads(row.get("raw_json")) if row else None
        scoped_key = self.db_activity_key(owner_account_id, activity_key)
        row = self.fetchone(
            conn,
            """
            SELECT ap.raw_json
            FROM activity_payloads ap
            INNER JOIN activities a ON a.id = ap.activity_id
            WHERE a.owner_account_id = ? AND a.activity_key IN (?, ?)
            """,
            (owner_account_id, scoped_key, activity_key),
        )
        return json_loads(row.get("raw_json")) if row else None

    def replace_activity(
        self,
        conn,
        *,
        owner_account_id: int | None,
        source_file_id: int,
        activity_key: str,
        activity_date: str,
        garmin_user_id: str,
        garmin_user_name: str,
        sport: str,
        sub_sport: str,
        swim_type: str,
        total_distance_m: float,
        total_time_s: float,
        raw_payload: str,
        intervals: list[dict],
    ) -> int:
        db_activity_key = self.db_activity_key(owner_account_id, activity_key)
        existing = self.fetchone(conn, "SELECT id FROM activities WHERE source_file_id = ?", (source_file_id,))
        if existing:
            self.execute(conn, "DELETE FROM activities WHERE id = ?", (existing["id"],)).close()
        self.execute(
            conn,
            """
            INSERT INTO activities (
                owner_account_id,
                source_file_id,
                activity_key,
                activity_date,
                garmin_user_id,
                garmin_user_name,
                sport,
                sub_sport,
                swim_type,
                total_distance_m,
                total_time_s
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                owner_account_id,
                source_file_id,
                db_activity_key,
                activity_date,
                garmin_user_id,
                garmin_user_name,
                sport,
                sub_sport,
                swim_type,
                total_distance_m,
                total_time_s,
            ),
        ).close()
        activity_row = self.fetchone(conn, "SELECT id FROM activities WHERE source_file_id = ?", (source_file_id,))
        activity_id = int(activity_row["id"])
        self.execute(
            conn,
            """
            INSERT INTO activity_payloads (activity_id, raw_json)
            VALUES (?, ?)
            ON CONFLICT(activity_id) DO UPDATE SET raw_json = excluded.raw_json
            """,
            (activity_id, raw_payload),
        ).close()
        for index, row in enumerate(intervals, start=1):
            self.execute(
                conn,
                """
                INSERT INTO intervals (
                    owner_account_id,
                    activity_id,
                    interval_index,
                    file_name,
                    activity_key,
                    activity_date,
                    garmin_user_id,
                    garmin_user_name,
                    lap_start,
                    lap_end,
                    distance_m,
                    raw_distance_m,
                    time_s,
                    time_text,
                    workout_total_distance_m,
                    workout_total_time_s,
                    stroke,
                    swim_type,
                    pace_100m_s,
                    pace_100m
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    owner_account_id,
                    activity_id,
                    index,
                    row.get("file_name", ""),
                    row.get("activity_key", ""),
                    row.get("activity_date", ""),
                    row.get("user_id", ""),
                    row.get("user_name", ""),
                    row.get("lap_start", ""),
                    row.get("lap_end", ""),
                    row.get("distance_m", 0.0),
                    row.get("raw_distance_m", 0.0),
                    row.get("time_s", 0.0),
                    row.get("time_text", ""),
                    row.get("workout_total_distance_m", 0.0),
                    row.get("workout_total_time_s", 0.0),
                    row.get("stroke", ""),
                    row.get("swim_type", ""),
                    row.get("pace_100m_s", 0.0),
                    row.get("pace_100m", ""),
                ),
            ).close()
        return activity_id

    def fetch_interval_rows(self, conn, *, owner_account_id: int, swim_mode: str, start_date: str, end_date: str) -> list[dict]:
        clauses = ["intervals.owner_account_id = ?"]
        params: list = [owner_account_id]
        if swim_mode and swim_mode != "all":
            clauses.append("intervals.swim_type = ?")
            params.append(swim_mode)
        if start_date:
            clauses.append("substr(COALESCE(intervals.activity_date, intervals.lap_start, intervals.lap_end), 1, 10) >= ?")
            params.append(start_date)
        if end_date:
            clauses.append("substr(COALESCE(intervals.activity_date, intervals.lap_start, intervals.lap_end), 1, 10) <= ?")
            params.append(end_date)
        return self.fetchall(
            conn,
            f"""
            SELECT
                intervals.file_name,
                intervals.activity_key,
                intervals.activity_date,
                intervals.garmin_user_id AS user_id,
                intervals.garmin_user_name AS user_name,
                intervals.lap_start,
                intervals.lap_end,
                intervals.distance_m,
                intervals.raw_distance_m,
                intervals.time_s,
                intervals.time_text,
                intervals.workout_total_distance_m,
                intervals.workout_total_time_s,
                intervals.stroke,
                intervals.swim_type,
                intervals.pace_100m_s,
                intervals.pace_100m
            FROM intervals
            WHERE {" AND ".join(clauses)}
            ORDER BY intervals.activity_date, intervals.lap_start, intervals.file_name
            """,
            tuple(params),
        )

    def fetch_dataset_meta(self, conn, owner_account_id: int) -> dict:
        files = self.fetchone(
            conn,
            """
            SELECT
                COUNT(*) AS total_files,
                SUM(CASE WHEN parse_status = 'ready' THEN 1 ELSE 0 END) AS ready_files,
                SUM(CASE WHEN parse_status = 'ignored' THEN 1 ELSE 0 END) AS ignored_files,
                SUM(CASE WHEN parse_status = 'error' THEN 1 ELSE 0 END) AS error_files,
                SUM(CASE WHEN parse_status = 'duplicate' THEN 1 ELSE 0 END) AS duplicate_files
            FROM source_files
            WHERE owner_account_id = ?
            """,
            (owner_account_id,),
        ) or {}
        intervals = self.fetchone(
            conn,
            "SELECT COUNT(*) AS total_rows FROM intervals WHERE owner_account_id = ?",
            (owner_account_id,),
        ) or {}
        activities = self.fetchone(
            conn,
            """
            SELECT
                COUNT(*) AS total_activities,
                MIN(NULLIF(activity_date, '')) AS first_activity_date,
                MAX(NULLIF(activity_date, '')) AS last_activity_date
            FROM activities
            WHERE owner_account_id = ?
            """,
            (owner_account_id,),
        ) or {}
        return {
            "total_files": int(files.get("total_files") or 0),
            "ready_files": int(files.get("ready_files") or 0),
            "ignored_files": int(files.get("ignored_files") or 0),
            "error_files": int(files.get("error_files") or 0),
            "duplicate_files": int(files.get("duplicate_files") or 0),
            "total_rows": int(intervals.get("total_rows") or 0),
            "total_activities": int(activities.get("total_activities") or 0),
            "first_activity_date": str(activities.get("first_activity_date") or "")[:10],
            "last_activity_date": str(activities.get("last_activity_date") or "")[:10],
        }

    def upsert_monthly_best(self, conn, *, owner_account_id: int, year: int, month: int, distance_m: int, best_pace_s: float, best_pace_text: str) -> None:
        existing = self.fetchone(
            conn,
            """
            SELECT id, best_pace_s
            FROM monthly_history
            WHERE owner_account_id = ? AND year = ? AND month = ? AND distance_m = ?
            """,
            (owner_account_id, year, month, distance_m),
        )
        if existing is None:
            self.execute(
                conn,
                """
                INSERT INTO monthly_history (
                    owner_account_id,
                    year,
                    month,
                    distance_m,
                    best_pace_s,
                    best_pace_text
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (owner_account_id, year, month, distance_m, best_pace_s, best_pace_text),
            ).close()
            return
        if float(existing.get("best_pace_s") or 0) == 0 or best_pace_s < float(existing["best_pace_s"]):
            self.execute(
                conn,
                """
                UPDATE monthly_history
                SET best_pace_s = ?, best_pace_text = ?
                WHERE id = ?
                """,
                (best_pace_s, best_pace_text, existing["id"]),
            ).close()

    def fetch_monthly_history(self, conn, owner_account_id: int) -> list[dict]:
        return self.fetchall(
            conn,
            """
            SELECT year, month, distance_m, best_pace_s, best_pace_text
            FROM monthly_history
            WHERE owner_account_id = ?
            ORDER BY year DESC, month DESC, distance_m ASC
            """,
            (owner_account_id,),
        )
