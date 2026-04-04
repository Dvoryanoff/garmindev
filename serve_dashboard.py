import os

from garmin_dashboard.app.server import run_server


if __name__ == "__main__":
    host = os.getenv("GARMIN_HOST", "0.0.0.0")
    port = int(os.getenv("GARMIN_PORT", "8000"))
    run_server(host=host, port=port)
