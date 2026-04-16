from __future__ import annotations

import os
import threading
from wsgiref.simple_server import WSGIServer, make_server

from socketserver import ThreadingMixIn


class ThreadingWSGIServer(ThreadingMixIn, WSGIServer):
    daemon_threads = True


def setup_django() -> None:
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "garmin_django.settings")
    import django

    django.setup()


def ensure_django_admin_ready() -> tuple[str, str]:
    setup_django()
    from garmin_dashboard.core.config import RuntimeConfig
    from garmin_dashboard.core.db import Database
    from django.contrib.auth import get_user_model
    from django.core.management import call_command

    Database(RuntimeConfig().database_url).init_schema()
    call_command("migrate", interactive=False, verbosity=0)

    username = os.getenv("GARMIN_DJANGO_ADMIN_USER", "admin")
    email = os.getenv("GARMIN_DJANGO_ADMIN_EMAIL", "admin@local")
    password = os.getenv("GARMIN_DJANGO_ADMIN_PASSWORD", "admin12345")
    User = get_user_model()
    user = User.objects.filter(username=username).first()
    if user is None:
        User.objects.create_superuser(username=username, email=email, password=password)
    return username, password


def run_django_admin_server(host: str = "127.0.0.1", port: int = 8010):
    setup_django()
    from django.contrib.staticfiles.handlers import StaticFilesHandler
    from django.core.wsgi import get_wsgi_application

    application = StaticFilesHandler(get_wsgi_application())
    httpd = make_server(host, port, application, server_class=ThreadingWSGIServer)
    try:
        httpd.serve_forever()
    finally:
        httpd.server_close()


def run_django_admin_in_thread(host: str = "127.0.0.1", port: int = 8010):
    thread = threading.Thread(target=run_django_admin_server, args=(host, port), daemon=True)
    thread.start()
    return thread
