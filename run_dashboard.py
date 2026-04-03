import os
import threading
import webbrowser

from garmin_dashboard.app.server import run_server


HOST = "127.0.0.1"
PORT = 8000


def should_open_browser() -> bool:
    value = os.getenv("GARMIN_OPEN_BROWSER", "1").strip().lower()
    return value not in {"0", "false", "no", "off"}


def open_dashboard():
    url = f"http://{HOST}:{PORT}"
    print(f"Открыть dashboard: {url}")
    if should_open_browser():
        threading.Timer(0.6, lambda: webbrowser.open(url)).start()


if __name__ == "__main__":
    open_dashboard()
    run_server(host=HOST, port=PORT)
