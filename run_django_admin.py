import os

from garmin_django.runner import ensure_django_admin_ready, run_django_admin_server


HOST = os.getenv("GARMIN_HOST", "127.0.0.1")
PORT = int(os.getenv("GARMIN_DJANGO_ADMIN_PORT", "8010"))


if __name__ == "__main__":
    username, password = ensure_django_admin_ready()
    print(f"Django admin: http://{HOST}:{PORT}/admin/")
    print(f"Django admin login: {username} / {password}")
    run_django_admin_server(host=HOST, port=PORT)
