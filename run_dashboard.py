import os
import socket
import threading
import webbrowser

from garmin_dashboard.app.server import run_server


HOST = os.getenv("GARMIN_HOST", "127.0.0.1")
PORT = int(os.getenv("GARMIN_PORT", "8000"))


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
    url = f"http://{HOST}:{port}"
    print(f"Открыть dashboard: {url}")
    if should_open_browser():
        threading.Timer(0.6, lambda: webbrowser.open(url)).start()


if __name__ == "__main__":
    port = find_available_port(HOST, PORT)
    open_dashboard(port)
    run_server(host=HOST, port=port)
