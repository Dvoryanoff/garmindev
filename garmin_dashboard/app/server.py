import csv
import io
import json
import threading
import urllib.parse
from functools import partial
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

from garmin_dashboard.core.config import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_MAX_WORKERS,
    IntervalConfig,
    PROJECT_ROOT,
    RESOURCES_DIR,
    ReportRequest,
    RuntimeConfig,
    list_resource_dirs,
    parse_distances,
    resolve_resource_dir,
)
from garmin_dashboard.core.dataset import clear_cache_file
from garmin_dashboard.core.monthly_history import refresh_monthly_history
from .reports import build_report


WEB_ROOT = PROJECT_ROOT / "web"
RUNTIME_STATUS = {
    "monthly_processing": False,
    "monthly_message": "",
}


def build_request_from_params(params: dict) -> ReportRequest:
    swim_mode = params.get("swim_mode", ["all"])[0]
    period = params.get("period", ["year"])[0]
    days_raw = params.get("days", [""])[0]
    distances_raw = params.get("distances", [""])[0]
    long_min_raw = params.get("long_min_distance", ["1000"])[0]
    resource_dir_raw = params.get("resource_dir", [""])[0]
    persist_csv = params.get("persist_csv", ["0"])[0] == "1"

    interval_config = IntervalConfig(
        target_distances=parse_distances(distances_raw),
        long_freestyle_min_distance_m=float(long_min_raw),
    )
    runtime_config = RuntimeConfig(
        fit_dir=resolve_resource_dir(resource_dir_raw),
        max_workers=DEFAULT_MAX_WORKERS,
        batch_size=DEFAULT_BATCH_SIZE,
    )
    return ReportRequest(
        swim_mode=swim_mode,
        period=period,
        days=int(days_raw) if days_raw else None,
        persist_csv=persist_csv,
        interval_config=interval_config,
        runtime_config=runtime_config,
    )


class DashboardRequestHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, directory=None, **kwargs):
        super().__init__(*args, directory=str(directory or WEB_ROOT), **kwargs)

    def end_headers(self):
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        super().end_headers()

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/api/resources":
            self.handle_resources()
            return
        if parsed.path == "/api/runtime-status":
            self.handle_runtime_status()
            return
        if parsed.path == "/api/report":
            self.handle_report(parsed.query)
            return
        if parsed.path == "/api/export/details.csv":
            self.handle_export(parsed.query, kind="details")
            return
        if parsed.path == "/api/export/summary.csv":
            self.handle_export(parsed.query, kind="summary")
            return
        if parsed.path == "/":
            self.path = "/index.html"
        super().do_GET()

    def handle_report(self, query: str):
        params = urllib.parse.parse_qs(query)
        try:
            report = build_report(build_request_from_params(params))
            self.send_json(report)
        except Exception as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)

    def handle_export(self, query: str, kind: str):
        params = urllib.parse.parse_qs(query)
        try:
            report = build_report(build_request_from_params(params))
            rows = report[kind]
            buffer = io.StringIO()
            fieldnames = list(rows[0].keys()) if rows else []
            writer = csv.DictWriter(buffer, fieldnames=fieldnames)
            if fieldnames:
                writer.writeheader()
                writer.writerows(rows)
            filename = f"garmin_{kind}_{report['filters']['swim_mode']}_{report['filters']['period']}.csv"
            payload = buffer.getvalue().encode("utf-8-sig")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/csv; charset=utf-8")
            self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        except Exception as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)

    def handle_resources(self):
        resources = [
            {
                "name": str(path.relative_to(RESOURCES_DIR)),
                "path": str(path),
            }
            for path in list_resource_dirs(RESOURCES_DIR)
        ]
        self.send_json({
            "resources": resources,
            "default_resource": str(resolve_resource_dir(None).relative_to(RESOURCES_DIR)) if resources else "",
        })

    def handle_runtime_status(self):
        self.send_json(dict(RUNTIME_STATUS))

    def send_json(self, payload: dict, status: HTTPStatus = HTTPStatus.OK):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def run_server(host: str = "127.0.0.1", port: int = 8000):
    runtime_config = RuntimeConfig(
        max_workers=DEFAULT_MAX_WORKERS,
        batch_size=DEFAULT_BATCH_SIZE,
    )
    clear_cache_file(runtime_config.cache_file)

    handler = partial(DashboardRequestHandler, directory=WEB_ROOT)
    httpd = ThreadingHTTPServer((host, port), handler)
    print(f"Garmin dashboard запущен: http://{host}:{port}")
    print(f"Статика: {WEB_ROOT}")
    print(f"Дефолтные настройки: workers={DEFAULT_MAX_WORKERS}, batch_size={DEFAULT_BATCH_SIZE}")
    print(f"Кэш сброшен при старте: {runtime_config.cache_file}")

    def refresh_monthly_history_in_background():
        if not RESOURCES_DIR.exists():
            return
        RUNTIME_STATUS["monthly_processing"] = True
        RUNTIME_STATUS["monthly_message"] = "Обработка monthly history для resources"
        print("Обновление помесячной таблицы...")
        try:
            monthly_meta = refresh_monthly_history(
                RuntimeConfig(
                    fit_dir=RESOURCES_DIR,
                    max_workers=DEFAULT_MAX_WORKERS,
                    batch_size=DEFAULT_BATCH_SIZE,
                )
            )
            RUNTIME_STATUS["monthly_message"] = (
                f"Monthly history готова: users={monthly_meta['users']}, workouts={monthly_meta['workouts']}"
            )
            print(
                "Monthly history:",
                "resources",
                f"(users={monthly_meta['users']}, workouts={monthly_meta['workouts']}, processed={monthly_meta['processed_files']}, duplicates={monthly_meta['duplicate_files']}, cached={monthly_meta['cached_files']})",
            )
        except Exception as exc:
            RUNTIME_STATUS["monthly_message"] = f"Ошибка monthly history: {exc}"
            print(f"Monthly history error: {exc}")
        finally:
            RUNTIME_STATUS["monthly_processing"] = False

    threading.Thread(target=refresh_monthly_history_in_background, daemon=True).start()
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
