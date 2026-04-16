FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    GARMIN_HOST=0.0.0.0 \
    GARMIN_PUBLIC_HOST=127.0.0.1 \
    GARMIN_PORT=8000 \
    GARMIN_DJANGO_ADMIN_PORT=8010 \
    GARMIN_OPEN_BROWSER=0 \
    GARMIN_RESOURCES_DIR=/app/resources \
    GARMIN_CACHE_FILE=/app/runtime/garmin_swim_fit_cache.pkl \
    GARMIN_DETAIL_CSV=/app/runtime/garmin_swim_intervals_details.csv \
    GARMIN_SUMMARY_CSV=/app/runtime/garmin_swim_intervals_summary.csv \
    GARMIN_MONTHLY_HISTORY_DIR=/app/monthly_history \
    GARMIN_UPLOAD_DIR=/app/uploads \
    DATABASE_URL=postgresql://garmin:garmin@db:5432/garmin_dashboard

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY garmin_dashboard ./garmin_dashboard
COPY garmin_django ./garmin_django
COPY garmin_admin ./garmin_admin
COPY run_dashboard.py run_django_admin.py serve_dashboard.py garmin_local.py manage.py ./
COPY web ./web
COPY tools ./tools

RUN mkdir -p /app/resources /app/runtime /app/monthly_history /app/uploads

EXPOSE 8000 8010

CMD ["python", "run_dashboard.py"]
