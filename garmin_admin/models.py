from __future__ import annotations

import json

from django.db import models


class Account(models.Model):
    email = models.EmailField(unique=True)
    password_hash = models.TextField()
    first_name = models.TextField(blank=True, default="")
    last_name = models.TextField(blank=True, default="")
    role = models.TextField(default="user")
    is_active = models.IntegerField(default=1)
    created_at = models.TextField(blank=True, default="")
    last_login_at = models.TextField(blank=True, default="")

    class Meta:
        db_table = "accounts"
        managed = False
        verbose_name = "Пользователь Garmin"
        verbose_name_plural = "Пользователи Garmin"

    def __str__(self) -> str:
        return f"{self.full_name} <{self.email}>"

    @property
    def full_name(self) -> str:
        return f"{self.first_name} {self.last_name}".strip() or self.email


class SourceFile(models.Model):
    owner_account = models.ForeignKey(Account, models.DO_NOTHING, db_column="owner_account_id", related_name="source_files")
    file_path = models.TextField()
    file_name = models.TextField()
    original_file_name = models.TextField(blank=True, default="")
    file_hash = models.TextField(blank=True, default="")
    file_size = models.BigIntegerField(default=0)
    mtime_ns = models.BigIntegerField(default=0)
    parser_version = models.IntegerField(default=0)
    parse_status = models.TextField(default="pending")
    error_text = models.TextField(blank=True, default="")
    activity_key = models.TextField(blank=True, default="")
    uploaded_at = models.TextField(blank=True, default="")
    ingested_at = models.TextField(blank=True, default="")

    class Meta:
        db_table = "source_files"
        managed = False
        verbose_name = "Загруженный FIT-файл"
        verbose_name_plural = "Загруженные FIT-файлы"

    def __str__(self) -> str:
        return self.original_file_name or self.file_name


class BackgroundJob(models.Model):
    owner_account = models.ForeignKey(Account, models.DO_NOTHING, db_column="owner_account_id", related_name="background_jobs")
    job_type = models.TextField(default="ingest")
    status = models.TextField(default="queued")
    stage = models.TextField(default="queued")
    total_files = models.IntegerField(default=0)
    processed_files = models.IntegerField(default=0)
    skipped_files = models.IntegerField(default=0)
    duplicate_files = models.IntegerField(default=0)
    error_files = models.IntegerField(default=0)
    parsed_rows = models.IntegerField(default=0)
    progress_percent = models.IntegerField(default=0)
    error_text = models.TextField(blank=True, default="")
    payload_json = models.TextField(blank=True, default="")
    created_at = models.TextField(blank=True, default="")
    started_at = models.TextField(blank=True, default="")
    finished_at = models.TextField(blank=True, default="")

    class Meta:
        db_table = "background_jobs"
        managed = False
        verbose_name = "Фоновая задача"
        verbose_name_plural = "Фоновые задачи"


class MonthlyHistory(models.Model):
    owner_account = models.ForeignKey(Account, models.DO_NOTHING, db_column="owner_account_id", related_name="monthly_rows")
    year = models.IntegerField()
    month = models.IntegerField()
    distance_m = models.IntegerField()
    best_pace_s = models.FloatField(default=0)
    best_pace_text = models.TextField(blank=True, default="")

    class Meta:
        db_table = "monthly_history"
        managed = False
        verbose_name = "Помесячная запись"
        verbose_name_plural = "Помесячная история"


class ReportRun(models.Model):
    owner_account = models.ForeignKey(Account, models.DO_NOTHING, db_column="owner_account_id", related_name="report_runs")
    created_at = models.TextField(blank=True, default="")
    period_label = models.TextField(blank=True, default="")
    filters_json = models.TextField(blank=True, default="")
    overview_json = models.TextField(blank=True, default="")
    summary_json = models.TextField(blank=True, default="")
    workouts_json = models.TextField(blank=True, default="")

    class Meta:
        db_table = "report_runs"
        managed = False
        verbose_name = "Снимок отчёта"
        verbose_name_plural = "Последние отчёты"

    def __str__(self) -> str:
        return f"{self.owner_account.full_name} • {self.period_label} • {self.created_at}"

    def parse_json(self, field_name: str):
        try:
            return json.loads(getattr(self, field_name) or "")
        except Exception:
            return None
