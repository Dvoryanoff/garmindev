from __future__ import annotations

import os
from pathlib import Path

from garmin_dashboard.core.config import PROJECT_ROOT
from garmin_dashboard.core.db import parse_database_url


BASE_DIR = Path(__file__).resolve().parent.parent
SECRET_KEY = os.getenv("GARMIN_DJANGO_SECRET_KEY", "garmin-local-django-admin")
DEBUG = os.getenv("GARMIN_DJANGO_DEBUG", "1").strip().lower() not in {"0", "false", "no", "off"}
ALLOWED_HOSTS = ["127.0.0.1", "localhost"]

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "garmin_admin",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "garmin_django.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "garmin_django.wsgi.application"
ASGI_APPLICATION = "garmin_django.asgi.application"

database_url = os.getenv("DATABASE_URL", f"sqlite:///{PROJECT_ROOT / 'garmin_dashboard.db'}")
db_config = parse_database_url(database_url)
if db_config.backend == "sqlite":
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.sqlite3",
            "NAME": db_config.database,
        }
    }
else:
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.postgresql",
            "NAME": db_config.database,
            "USER": db_config.user,
            "PASSWORD": db_config.password,
            "HOST": db_config.host,
            "PORT": db_config.port,
        }
    }

LANGUAGE_CODE = "ru-ru"
TIME_ZONE = "Europe/Moscow"
USE_I18N = True
USE_TZ = False

STATIC_URL = "/static/"
STATIC_ROOT = BASE_DIR / "runtime" / "django_static"

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
