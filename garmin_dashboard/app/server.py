from __future__ import annotations

import cgi
import json
import threading
import urllib.parse
from datetime import datetime, timedelta
from functools import partial
from http import HTTPStatus
from http.cookies import SimpleCookie
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

from garmin_dashboard.core.auth import hash_password, new_session_token, session_expiry, verify_password
from garmin_dashboard.core.config import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_MAX_WORKERS,
    IntervalConfig,
    LOGIN_RATE_LIMIT_ATTEMPTS,
    LOGIN_RATE_LIMIT_WINDOW_MINUTES,
    PROJECT_ROOT,
    ReportRequest,
    RuntimeConfig,
    SESSION_IDLE_TIMEOUT_MINUTES,
    SESSION_TTL_DAYS,
    UPLOAD_MAX_BATCH_BYTES,
    UPLOAD_MAX_FILE_BYTES,
    UPLOAD_MAX_FILES,
    parse_distances,
)
from garmin_dashboard.core.db import Database
from garmin_dashboard.core.db_ingest import load_monthly_history
from garmin_dashboard.core.jobs import enqueue_job, ensure_workers
from garmin_dashboard.core.monthly_history import (
    build_monthly_history_payload,
    build_monthly_history_workbook_bytes,
    build_yearly_records_payload,
)
from .reports import build_report

WEB_ROOT = PROJECT_ROOT / "web"


def iso_now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def serialize_job_for_api(job: dict | None) -> dict | None:
    if not job:
        return None
    serialized = dict(job)
    payload = serialized.get("payload_json")
    if isinstance(payload, dict):
        files = list(payload.get("files") or [])
        serialized["payload_json"] = {
            "files_count": len(files),
            "has_content": any("content_hex" in file_item for file_item in files),
            "timings": payload.get("timings") or {},
        }
    else:
        serialized["payload_json"] = {}
    return serialized


def build_job_status_message(job: dict | None) -> str:
    if not job:
        return ""
    stage = str(job.get("stage") or job.get("status") or "queued")
    status = str(job.get("status") or "queued")
    labels = {
        "queued": "ожидает старта",
        "ingest": "обрабатывает файлы",
        "monthly_history": "обновляет помесячную историю",
        "done": "завершён",
        "failed": "завершён с ошибкой",
    }
    processed = int(job.get("processed_files") or 0)
    skipped = int(job.get("skipped_files") or 0)
    duplicates = int(job.get("duplicate_files") or 0)
    errors = int(job.get("error_files") or 0)
    total_files = int(job.get("total_files") or 0)
    parts = [
        f"Job #{job['id']}: {labels.get(stage, stage)}",
        f"{int(job.get('progress_percent') or 0)}%",
    ]
    if status == "queued" and not job.get("started_at"):
        parts.append("worker ещё не стартовал")
    if total_files > 0:
        parts.append(f"в job файлов: {total_files}")
    parts.extend(
        [
            f"обработано {processed}",
            f"пропущено {skipped}",
            f"дубликаты {duplicates}",
            f"ошибки {errors}",
        ]
    )
    return " • ".join(parts)


def build_request_from_params(params: dict, owner_account_id: int) -> ReportRequest:
    swim_mode = params.get("swim_mode", ["all"])[0]
    period = params.get("period", ["current_year"])[0]
    days_raw = params.get("days", [""])[0]
    report_year_raw = params.get("report_year", [""])[0]
    distances_raw = params.get("distances", [""])[0]
    long_min_raw = params.get("long_min_distance", ["1000"])[0]
    if "distances" in params and not str(distances_raw).strip():
        raise ValueError("Выбери хотя бы одну дистанцию для отчёта")
    user_runtime_dir = RuntimeConfig().upload_dir / f"user_{owner_account_id}" / "exports"
    user_runtime_dir.mkdir(parents=True, exist_ok=True)
    return ReportRequest(
        swim_mode=swim_mode,
        period=period,
        days=int(days_raw) if days_raw else None,
        report_year=int(report_year_raw) if report_year_raw else None,
        persist_csv=False,
        owner_account_id=owner_account_id,
        interval_config=IntervalConfig(
            target_distances=parse_distances(distances_raw),
            long_freestyle_min_distance_m=float(long_min_raw),
        ),
        runtime_config=RuntimeConfig(
            detail_csv=user_runtime_dir / "garmin_swim_intervals_details.csv",
            summary_csv=user_runtime_dir / "garmin_swim_intervals_summary.csv",
            max_workers=DEFAULT_MAX_WORKERS,
            batch_size=DEFAULT_BATCH_SIZE,
        ),
    )


def resolve_registration_role(db: Database, conn) -> str:
    # Bootstrap mode: only the very first account in a clean database
    # becomes admin automatically.
    return "admin" if db.count_accounts(conn) == 0 else "user"


def validate_upload_request(files: list[dict], total_bytes: int) -> None:
    if not files:
        raise ValueError("Не выбраны файлы")
    if len(files) > UPLOAD_MAX_FILES:
        raise ValueError(f"Максимум можно обработать {UPLOAD_MAX_FILES} файлов")
    if total_bytes > UPLOAD_MAX_BATCH_BYTES:
        raise ValueError(f"Слишком большой batch. Лимит: {UPLOAD_MAX_BATCH_BYTES // (1024 * 1024)} MB")
    if any(len(file_item["content"]) > UPLOAD_MAX_FILE_BYTES for file_item in files):
        raise ValueError(f"Один из файлов слишком большой. Лимит: {UPLOAD_MAX_FILE_BYTES // (1024 * 1024)} MB")


class DashboardRequestHandler(SimpleHTTPRequestHandler):
    _schema_lock = threading.Lock()
    _initialized_databases: set[str] = set()

    def __init__(self, *args, directory=None, **kwargs):
        self.db = Database(RuntimeConfig().database_url)
        database_key = self.db.url
        if database_key not in self._initialized_databases:
            with self._schema_lock:
                if database_key not in self._initialized_databases:
                    self.db.init_schema()
                    self._initialized_databases.add(database_key)
        super().__init__(*args, directory=str(directory or WEB_ROOT), **kwargs)

    def end_headers(self):
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        super().end_headers()

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/api/healthz":
            self.send_json({"status": "ok"})
            return
        if parsed.path == "/api/runtime-status":
            self.handle_runtime_status()
            return
        if parsed.path == "/api/auth/session":
            self.handle_session()
            return
        if parsed.path == "/api/jobs":
            self.handle_jobs()
            return
        if parsed.path.startswith("/api/jobs/"):
            self.handle_job(parsed.path.rsplit("/", 1)[-1])
            return
        if parsed.path == "/api/report":
            self.handle_report(parsed.query)
            return
        if parsed.path == "/api/monthly-history":
            self.handle_monthly_history()
            return
        if parsed.path == "/api/yearly-records":
            self.handle_yearly_records()
            return
        if parsed.path == "/api/export/summary.xlsx":
            self.handle_excel_export(parsed.query, kind="summary")
            return
        if parsed.path == "/api/export/workouts.xlsx":
            self.handle_excel_export(parsed.query, kind="workouts")
            return
        if parsed.path == "/api/export/monthly-history.xlsx":
            self.handle_monthly_history_export()
            return
        if parsed.path == "/api/export/yearly-records.xlsx":
            self.handle_yearly_records_export()
            return
        if parsed.path == "/api/admin/users":
            self.handle_admin_users()
            return
        if parsed.path == "/api/admin/overview":
            self.handle_admin_overview()
            return
        if parsed.path == "/":
            self.path = "/index.html"
        if parsed.path in {"/admin", "/admin/"}:
            self.path = "/admin.html"
        super().do_GET()

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/api/auth/register":
            self.handle_register()
            return
        if parsed.path == "/api/auth/login":
            self.handle_login()
            return
        if parsed.path == "/api/auth/logout":
            self.handle_logout()
            return
        if parsed.path == "/api/upload":
            self.handle_upload()
            return
        if parsed.path == "/api/admin/users":
            self.handle_admin_update_user()
            return
        self.send_json({"error": "Маршрут не найден"}, status=HTTPStatus.NOT_FOUND)

    def current_account(self) -> dict | None:
        cookie = SimpleCookie(self.headers.get("Cookie", ""))
        token = cookie.get("garmin_session")
        if not token or not token.value:
            return None
        now_text = iso_now()
        idle_cutoff = (datetime.fromisoformat(now_text) - timedelta(minutes=SESSION_IDLE_TIMEOUT_MINUTES)).isoformat(timespec="seconds")
        with self.db.transaction() as conn:
            self.db.delete_idle_sessions(conn, idle_cutoff)
            account = self.db.find_account_by_session(conn, token.value, now_text)
            if account:
                self.db.touch_session(conn, token.value, now_text)
                account["session_last_seen_at"] = now_text
            return account

    def require_account(self) -> dict:
        account = self.current_account()
        if not account or not int(account.get("is_active") or 0):
            raise PermissionError("Требуется вход")
        return account

    def require_admin(self) -> dict:
        account = self.require_account()
        if account.get("role") != "admin":
            raise PermissionError("Нужны права администратора")
        return account

    def parse_json_body(self) -> dict:
        length = int(self.headers.get("Content-Length", "0") or 0)
        raw = self.rfile.read(length) if length > 0 else b"{}"
        if not raw:
            return {}
        return json.loads(raw.decode("utf-8"))

    def send_json(self, payload: dict, status: HTTPStatus = HTTPStatus.OK, headers: dict | None = None):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)

    def send_empty(self, status: HTTPStatus = HTTPStatus.NO_CONTENT, headers: dict | None = None):
        self.send_response(status)
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()

    def handle_session(self):
        account = self.current_account()
        if not account:
            self.send_json({"authenticated": False})
            return
        with self.db.transaction() as conn:
            prefs = self.db.get_user_preferences(conn, int(account["id"])) or {}
            meta = self.db.fetch_dataset_meta(conn, int(account["id"]))
        self.send_json({
            "authenticated": True,
            "account": account,
            "preferences": prefs,
            "dataset_meta": meta,
            "is_admin": account.get("role") == "admin",
            "session": {
                "created_at": account.get("session_created_at", ""),
                "expires_at": account.get("session_expires_at", ""),
                "last_seen_at": account.get("session_last_seen_at", ""),
                "login_day": account.get("session_login_day", ""),
                "idle_timeout_minutes": SESSION_IDLE_TIMEOUT_MINUTES,
            },
        })

    def handle_register(self):
        payload = self.parse_json_body()
        email = str(payload.get("email", "")).strip().lower()
        password = str(payload.get("password", ""))
        first_name = str(payload.get("first_name", "")).strip()
        last_name = str(payload.get("last_name", "")).strip()
        if not email or "@" not in email:
            self.send_json({"error": "Нужен корректный e-mail"}, status=HTTPStatus.BAD_REQUEST)
            return
        if len(password) < 6:
            self.send_json({"error": "Пароль должен быть не короче 6 символов"}, status=HTTPStatus.BAD_REQUEST)
            return
        if not first_name or not last_name:
            self.send_json({"error": "Нужны имя и фамилия"}, status=HTTPStatus.BAD_REQUEST)
            return
        with self.db.transaction() as conn:
            if self.db.find_account_by_email(conn, email):
                self.send_json({"error": "Такой e-mail уже зарегистрирован"}, status=HTTPStatus.BAD_REQUEST)
                return
            role = resolve_registration_role(self.db, conn)
            account_id = self.db.create_account(
                conn,
                email=email,
                password_hash=hash_password(password),
                first_name=first_name,
                last_name=last_name,
                role=role,
                created_at=iso_now(),
            )
            self.db.save_user_preferences(
                conn,
                account_id,
                swim_mode="all",
                period="current_year",
                days=None,
                report_year=None,
                target_distances="50,100,150,200,300,400,500,600,800,1000",
                long_min_distance=1000.0,
                updated_at=iso_now(),
            )
            self.db.append_audit_log(
                conn,
                actor_account_id=account_id,
                target_account_id=account_id,
                event_type="account_registered",
                payload_json=json.dumps({"email": email, "role": role}, ensure_ascii=False),
                created_at=iso_now(),
            )
        message = "Учётная запись создана"
        if role == "admin":
            message = "Учётная запись создана. Первый пользователь назначен администратором."
        self.send_json({"ok": True, "message": message, "role": role})

    def handle_login(self):
        payload = self.parse_json_body()
        email = str(payload.get("email", "")).strip().lower()
        password = str(payload.get("password", ""))
        window_start = (datetime.now() - timedelta(minutes=LOGIN_RATE_LIMIT_WINDOW_MINUTES)).isoformat(timespec="seconds")
        with self.db.transaction() as conn:
            failed_attempts = self.db.count_recent_failed_logins(conn, email=email, attempted_after=window_start)
            if failed_attempts >= LOGIN_RATE_LIMIT_ATTEMPTS:
                self.send_json({"error": "Слишком много неудачных попыток входа. Попробуй позже."}, status=HTTPStatus.TOO_MANY_REQUESTS)
                return
            account = self.db.find_account_by_email(conn, email)
            if not account or not verify_password(password, account.get("password_hash", "")):
                self.db.record_login_attempt(conn, email=email, attempted_at=iso_now(), was_success=False)
                self.send_json({"error": "Неверный e-mail или пароль"}, status=HTTPStatus.UNAUTHORIZED)
                return
            if not int(account.get("is_active") or 0):
                self.db.record_login_attempt(conn, email=email, attempted_at=iso_now(), was_success=False)
                self.send_json({"error": "Учётная запись отключена"}, status=HTTPStatus.FORBIDDEN)
                return
            token = new_session_token()
            now_text = iso_now()
            self.db.create_session(
                conn,
                int(account["id"]),
                token,
                now_text,
                session_expiry(),
                last_seen_at=now_text,
                login_day=now_text[:10],
            )
            self.db.update_last_login(conn, int(account["id"]), now_text)
            self.db.record_login_attempt(conn, email=email, attempted_at=now_text, was_success=True)
            self.db.append_audit_log(
                conn,
                actor_account_id=int(account["id"]),
                target_account_id=int(account["id"]),
                event_type="login_success",
                payload_json=json.dumps({"email": email}, ensure_ascii=False),
                created_at=now_text,
            )
        self.send_json(
            {"ok": True},
            headers={"Set-Cookie": f"garmin_session={token}; Path=/; Max-Age={SESSION_TTL_DAYS * 24 * 60 * 60}; HttpOnly; SameSite=Lax"},
        )

    def handle_logout(self):
        cookie = SimpleCookie(self.headers.get("Cookie", ""))
        token = cookie.get("garmin_session")
        if token and token.value:
            with self.db.transaction() as conn:
                self.db.delete_session(conn, token.value)
        self.send_empty(headers={"Set-Cookie": "garmin_session=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax"})

    def handle_report(self, query: str):
        try:
            account = self.require_account()
            params = urllib.parse.parse_qs(query)
            request = build_request_from_params(params, owner_account_id=int(account["id"]))
            report = build_report(request)
            with self.db.transaction() as conn:
                self.db.save_user_preferences(
                    conn,
                    int(account["id"]),
                    swim_mode=request.swim_mode,
                    period=request.period,
                    days=request.days,
                    report_year=request.report_year,
                    target_distances=",".join(str(x) for x in request.interval_config.target_distances),
                    long_min_distance=request.interval_config.long_freestyle_min_distance_m,
                    updated_at=iso_now(),
                )
            self.send_json(report)
        except PermissionError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.UNAUTHORIZED)
        except Exception as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)

    def handle_excel_export(self, query: str, kind: str):
        try:
            account = self.require_account()
            params = urllib.parse.parse_qs(query)
            report = build_report(build_request_from_params(params, owner_account_id=int(account["id"])))
            if kind == "summary":
                headers = ["Дистанция", "Отрезков", "Среднее время", "Лучшее время", "Лучший темп", "Дата лучшего", "Средний темп", "Middle темп", "Средний отдых"]
                rows = [[f"{row['distance_m']} м", row["count"], row["avg_time"], row["best_time"], row["best_pace_100m"], row["best_pace_date"], row["avg_pace_100m"], row["middle_pace_100m"], row.get("avg_rest") or "—"] for row in report["summary"]]
                sheet_name = "Сводка по дистанциям"
                filename = "summary.xlsx"
            else:
                headers = ["Дата", "Общее расстояние", "Время", "Рекорды", "Лучший темп", "Средний отдых", "Паузы > 2 мин"]
                rows = [[row["date"], f"{row['total_distance_m']} м", row["total_time"], row.get("record_distances_text") or "—", row["best_pace_100m"], row.get("avg_rest") or "—", row.get("long_rest_count") or 0] for row in report["workouts"]]
                sheet_name = "Тренировки"
                filename = "workouts.xlsx"
            from garmin_dashboard.core.xlsx_export import build_workbook_bytes
            payload = build_workbook_bytes(sheet_name=sheet_name, headers=headers, rows=rows)
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        except PermissionError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.UNAUTHORIZED)
        except Exception as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)

    def handle_upload(self):
        try:
            account = self.require_account()
        except PermissionError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.UNAUTHORIZED)
            return
        try:
            form = cgi.FieldStorage(
                fp=self.rfile,
                headers=self.headers,
                environ={
                    "REQUEST_METHOD": "POST",
                    "CONTENT_TYPE": self.headers.get("Content-Type", ""),
                    "CONTENT_LENGTH": self.headers.get("Content-Length", "0"),
                },
            )
            file_fields = form["files"] if "files" in form else []
            if not isinstance(file_fields, list):
                file_fields = [file_fields]
            files = []
            total_bytes = 0
            for item in file_fields:
                if getattr(item, "filename", ""):
                    content = item.file.read()
                    total_bytes += len(content)
                    files.append({
                        "name": item.filename,
                        "content": content,
                    })
            validate_upload_request(files, total_bytes)
            account_id = int(account["id"])
            payload_json = json.dumps(
                {
                    "files": [
                        {"name": file_item["name"], "content_hex": file_item["content"].hex()}
                        for file_item in files
                    ]
                },
                ensure_ascii=False,
            )
            with self.db.transaction() as conn:
                job_id = self.db.create_background_job(
                    conn,
                    owner_account_id=account_id,
                    job_type="ingest",
                    total_files=len(files),
                    payload_json=payload_json,
                    created_at=iso_now(),
                )
                self.db.append_audit_log(
                    conn,
                    actor_account_id=account_id,
                    target_account_id=account_id,
                    event_type="upload_job_created",
                    payload_json=json.dumps({"job_id": job_id, "files": len(files)}, ensure_ascii=False),
                    created_at=iso_now(),
                )
            ensure_workers(self.db.url)
            enqueue_job(self.db.url, job_id)
            self.send_json({"ok": True, "job_id": job_id})
        except Exception as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)

    def handle_jobs(self):
        try:
            account = self.require_account()
            with self.db.transaction() as conn:
                jobs = self.db.list_background_jobs(conn, int(account["id"]))
            self.send_json({"jobs": [serialize_job_for_api(job) for job in jobs]})
        except PermissionError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.UNAUTHORIZED)

    def handle_job(self, job_id_raw: str):
        try:
            account = self.require_account()
            job_id = int(job_id_raw)
            with self.db.transaction() as conn:
                job = self.db.get_background_job(conn, job_id)
            if not job or int(job.get("owner_account_id") or 0) != int(account["id"]):
                self.send_json({"error": "Job не найден"}, status=HTTPStatus.NOT_FOUND)
                return
            self.send_json({"job": serialize_job_for_api(job)})
        except PermissionError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.UNAUTHORIZED)
        except Exception as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)

    def handle_monthly_history(self):
        try:
            account = self.require_account()
            rows = load_monthly_history(RuntimeConfig(), int(account["id"]))
            self.send_json(build_monthly_history_payload(rows))
        except PermissionError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.UNAUTHORIZED)

    def handle_yearly_records(self):
        try:
            account = self.require_account()
            with self.db.transaction() as conn:
                rows = self.db.fetch_monthly_history(conn, int(account["id"]))
            self.send_json(build_yearly_records_payload(rows))
        except PermissionError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.UNAUTHORIZED)

    def handle_monthly_history_export(self):
        try:
            account = self.require_account()
            rows = load_monthly_history(RuntimeConfig(), int(account["id"]))
            payload = build_monthly_history_workbook_bytes(rows)
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            self.send_header("Content-Disposition", 'attachment; filename="monthly-history.xlsx"')
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        except PermissionError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.UNAUTHORIZED)

    def handle_yearly_records_export(self):
        try:
            account = self.require_account()
            with self.db.transaction() as conn:
                rows = self.db.fetch_monthly_history(conn, int(account["id"]))
            yearly_payload = build_yearly_records_payload(rows)
            headers = ["Год", *[f"{distance} м" for distance in yearly_payload["headers"]]]
            table_rows = [
                [str(row["year"]), *[(value.get("text") or "—") for value in row.get("values", [])]]
                for row in yearly_payload["rows"]
            ]
            from garmin_dashboard.core.xlsx_export import build_workbook_bytes
            payload = build_workbook_bytes(sheet_name="Годовые рекорды", headers=headers, rows=table_rows)
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            self.send_header("Content-Disposition", 'attachment; filename="yearly-records.xlsx"')
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        except PermissionError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.UNAUTHORIZED)

    def handle_runtime_status(self):
        account = self.current_account()
        if not account:
            self.send_json({"monthly_processing": False, "monthly_message": ""})
            return
        with self.db.transaction() as conn:
            jobs = self.db.list_background_jobs(conn, int(account["id"]), limit=5)
        active_job = next((job for job in jobs if job.get("status") in {"queued", "running"}), None)
        if not active_job:
            self.send_json({"monthly_processing": False, "monthly_message": ""})
            return
        self.send_json({
            "monthly_processing": True,
            "monthly_message": build_job_status_message(active_job),
        })

    def handle_admin_users(self):
        try:
            self.require_admin()
            with self.db.transaction() as conn:
                users = self.db.list_accounts_with_stats(conn)
            self.send_json({"users": users})
        except PermissionError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.UNAUTHORIZED)

    def handle_admin_overview(self):
        try:
            self.require_admin()
            with self.db.transaction() as conn:
                overview = self.db.admin_overview(conn)
                recent_logins = self.db.list_recent_logins(conn)
                recent_uploads = self.db.list_recent_uploads(conn)
                recent_audit = self.db.list_audit_log(conn, limit=20)
            self.send_json({
                "overview": overview,
                "recent_logins": recent_logins,
                "recent_uploads": recent_uploads,
                "recent_audit": recent_audit,
            })
        except PermissionError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.UNAUTHORIZED)

    def handle_admin_update_user(self):
        try:
            actor = self.require_admin()
            payload = self.parse_json_body()
            account_id = int(payload.get("account_id") or 0)
            action = str(payload.get("action") or "update")
            role = str(payload.get("role") or "user")
            is_active = 1 if payload.get("is_active", True) else 0
            with self.db.transaction() as conn:
                target = self.db.find_account_by_id(conn, account_id)
                primary_admin_id = self.db.get_primary_admin_account_id(conn)
                if not target:
                    raise ValueError("Пользователь не найден")
                if primary_admin_id is not None and account_id == primary_admin_id:
                    raise ValueError("Нельзя менять роль или доступ первому администратору")
                if action == "delete":
                    if int(actor["id"]) == account_id:
                        raise ValueError("Нельзя удалить текущего администратора")
                    self.db.append_audit_log(
                        conn,
                        actor_account_id=int(actor["id"]),
                        target_account_id=account_id,
                        event_type="admin_delete_user",
                        payload_json=json.dumps({"email": target.get("email", "")}, ensure_ascii=False),
                        created_at=iso_now(),
                    )
                    self.db.delete_account(conn, account_id)
                else:
                    if role not in {"user", "admin"}:
                        raise ValueError("Некорректная роль")
                    self.db.update_account(conn, account_id, role=role, is_active=is_active)
                    self.db.append_audit_log(
                        conn,
                        actor_account_id=int(actor["id"]),
                        target_account_id=account_id,
                        event_type="admin_update_user",
                        payload_json=json.dumps({"role": role, "is_active": is_active}, ensure_ascii=False),
                        created_at=iso_now(),
                    )
            self.send_json({"ok": True})
        except PermissionError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.UNAUTHORIZED)
        except Exception as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)

def run_server(host: str = "127.0.0.1", port: int = 8000):
    handler = partial(DashboardRequestHandler, directory=WEB_ROOT)
    httpd = ThreadingHTTPServer((host, port), handler)
    print(f"Garmin dashboard запущен: http://{host}:{port}")
    print(f"Статика: {WEB_ROOT}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()


def run_server_in_thread(host: str = "127.0.0.1", port: int = 8000):
    thread = threading.Thread(target=run_server, args=(host, port), daemon=True)
    thread.start()
    return thread
