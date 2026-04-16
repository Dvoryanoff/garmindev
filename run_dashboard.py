import os
import socket
import threading
import webbrowser

from garmin_dashboard.app.server import run_server
from garmin_django.runner import ensure_django_admin_ready, run_django_admin_in_thread


HOST = os.getenv("GARMIN_HOST", "127.0.0.1")
PORT = int(os.getenv("GARMIN_PORT", "8000"))
DJANGO_ADMIN_PORT = int(os.getenv("GARMIN_DJANGO_ADMIN_PORT", "8010"))


def public_host() -> str:
    explicit = os.getenv("GARMIN_PUBLIC_HOST", "").strip()
    if explicit:
        return explicit
    if HOST in {"0.0.0.0", "::"}:
        return "127.0.0.1"
    return HOST


def should_open_browser() -> bool:
    value = os.getenv("GARMIN_OPEN_BROWSER", "1").strip().lower()
    return value not in {"0", "false", "no", "off"}


def find_available_port(host: str, start_port: int, attempts: int = 20) -> int:
    for port in range(start_port, start_port + attempts):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind((host, port))
                return port
            except OSError:
                continue
    raise OSError(f"Не удалось найти свободный порт в диапазоне {start_port}-{start_port + attempts - 1}")


def open_dashboard(port: int):
    url = f"http://{public_host()}:{port}"
    print(f"Открыть dashboard: {url}")
    if should_open_browser():
        threading.Timer(0.6, lambda: webbrowser.open(url)).start()


if __name__ == "__main__":
    port = find_available_port(HOST, PORT)
    admin_port = find_available_port(HOST, DJANGO_ADMIN_PORT)
    admin_username, admin_password = ensure_django_admin_ready()
    admin_url = f"http://{public_host()}:{admin_port}/admin/"
    os.environ["GARMIN_DJANGO_ADMIN_URL"] = admin_url
    run_django_admin_in_thread(host=HOST, port=admin_port)
    open_dashboard(port)
    print(f"Django admin: {admin_url}")
    print(f"Django admin login: {admin_username} / {admin_password}")
    run_server(host=HOST, port=port)
