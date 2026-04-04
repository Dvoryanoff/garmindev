from __future__ import annotations

import cgi
import json
import threading
import urllib.parse
from datetime import datetime
from functools import partial
from http import HTTPStatus
from http.cookies import SimpleCookie
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

from garmin_dashboard.core.auth import hash_password, new_session_token, session_expiry, verify_password
from garmin_dashboard.core.config import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_MAX_WORKERS,
    IntervalConfig,
    PROJECT_ROOT,
    ReportRequest,
    RuntimeConfig,
    SESSION_TTL_DAYS,
    parse_distances,
)
from garmin_dashboard.core.db import Database
from garmin_dashboard.core.db_ingest import ingest_uploaded_files, load_monthly_history
from garmin_dashboard.core.monthly_history import build_monthly_history_payload, build_monthly_history_workbook_bytes
from .reports import build_report


WEB_ROOT = PROJECT_ROOT / "web"
RUNTIME_STATUS = {
    "imports": {},
}


def iso_now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def build_request_from_params(params: dict, owner_account_id: int) -> ReportRequest:
    swim_mode = params.get("swim_mode", ["all"])[0]
    period = params.get("period", ["current_year"])[0]
    days_raw = params.get("days", [""])[0]
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


class DashboardRequestHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, directory=None, **kwargs):
        self.db = Database(RuntimeConfig().database_url)
        self.db.init_schema()
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
        if parsed.path == "/api/report":
            self.handle_report(parsed.query)
            return
        if parsed.path == "/api/monthly-history":
            self.handle_monthly_history()
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
        if parsed.path == "/api/admin/users":
            self.handle_admin_users()
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
        with self.db.transaction() as conn:
            return self.db.find_account_by_session(conn, token.value, iso_now())

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
            role = "user"
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
                target_distances="50,100,150,200,300,400,500,600,800,1000",
                long_min_distance=1000.0,
                updated_at=iso_now(),
            )
        self.send_json({"ok": True})

    def handle_login(self):
        payload = self.parse_json_body()
        email = str(payload.get("email", "")).strip().lower()
        password = str(payload.get("password", ""))
        with self.db.transaction() as conn:
            account = self.db.find_account_by_email(conn, email)
            if not account or not verify_password(password, account.get("password_hash", "")):
                self.send_json({"error": "Неверный e-mail или пароль"}, status=HTTPStatus.UNAUTHORIZED)
                return
            if not int(account.get("is_active") or 0):
                self.send_json({"error": "Учётная запись отключена"}, status=HTTPStatus.FORBIDDEN)
                return
            token = new_session_token()
            now_text = iso_now()
            self.db.create_session(conn, int(account["id"]), token, now_text, session_expiry())
            self.db.update_last_login(conn, int(account["id"]), now_text)
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
                headers = ["Дистанция", "Отрезков", "Среднее время", "Лучшее время", "Лучший темп", "Дата лучшего", "Средний темп", "Middle темп"]
                rows = [[f"{row['distance_m']} м", row["count"], row["avg_time"], row["best_time"], row["best_pace_100m"], row["best_pace_date"], row["avg_pace_100m"], row["middle_pace_100m"]] for row in report["summary"]]
                sheet_name = "Сводка по дистанциям"
                filename = "summary.xlsx"
            else:
                headers = ["Дата", "Общее расстояние", "Время", "Лучший темп"]
                rows = [[row["date"], f"{row['total_distance_m']} м", row["total_time"], row["best_pace_100m"]] for row in report["workouts"]]
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
            for item in file_fields:
                if getattr(item, "filename", ""):
                    files.append({
                        "name": item.filename,
                        "content": item.file.read(),
                    })
            if not files:
                self.send_json({"error": "Не выбраны файлы"}, status=HTTPStatus.BAD_REQUEST)
                return
            account_id = int(account["id"])
            RUNTIME_STATUS["imports"][account_id] = {"active": True, "message": f"Загрузка {len(files)} файлов"}
            meta = ingest_uploaded_files(RuntimeConfig(), account_id, files)
            RUNTIME_STATUS["imports"][account_id] = {
                "active": False,
                "message": f"Импорт завершён: обработано {meta['processed_files']}, пропущено {meta['skipped_files']}",
            }
            self.send_json({"ok": True, "meta": meta})
        except Exception as exc:
            RUNTIME_STATUS["imports"][account_id] = {"active": False, "message": f"Ошибка импорта: {exc}"}
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)

    def handle_monthly_history(self):
        try:
            account = self.require_account()
            rows = load_monthly_history(RuntimeConfig(), int(account["id"]))
            self.send_json(build_monthly_history_payload(rows))
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

    def handle_runtime_status(self):
        account = self.current_account()
        if not account:
            self.send_json({"monthly_processing": False, "monthly_message": ""})
            return
        status = RUNTIME_STATUS["imports"].get(int(account["id"]), {"active": False, "message": ""})
        self.send_json({
            "monthly_processing": bool(status.get("active")),
            "monthly_message": str(status.get("message") or ""),
        })

    def handle_admin_users(self):
        try:
            self.require_admin()
            with self.db.transaction() as conn:
                users = self.db.list_accounts_with_stats(conn)
            self.send_json({"users": users})
        except PermissionError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.UNAUTHORIZED)

    def handle_admin_update_user(self):
        try:
            self.require_admin()
            payload = self.parse_json_body()
            account_id = int(payload.get("account_id") or 0)
            role = str(payload.get("role") or "user")
            is_active = 1 if payload.get("is_active", True) else 0
            if role not in {"user", "admin"}:
                raise ValueError("Некорректная роль")
            with self.db.transaction() as conn:
                self.db.update_account(conn, account_id, role=role, is_active=is_active)
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
