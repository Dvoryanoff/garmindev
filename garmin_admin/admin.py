from __future__ import annotations

import json

from django.contrib import admin, messages
from django.utils.html import format_html

from .models import Account, BackgroundJob, MonthlyHistory, ReportRun, SourceFile


def _pretty_json(value) -> str:
    try:
        return json.dumps(json.loads(value or ""), ensure_ascii=False, indent=2)
    except Exception:
        return str(value or "")


@admin.register(Account)
class AccountAdmin(admin.ModelAdmin):
    list_display = ("id", "email", "full_name", "role", "is_active_flag", "created_at", "last_login_at", "recent_reports_count")
    list_filter = ("role",)
    search_fields = ("email", "first_name", "last_name")
    readonly_fields = ("created_at", "last_login_at", "recent_reports_preview")
    actions = ("make_admin", "make_user", "activate_accounts", "deactivate_accounts")

    fieldsets = (
        ("Профиль", {"fields": ("email", "first_name", "last_name", "role", "is_active")}),
        ("История", {"fields": ("created_at", "last_login_at", "recent_reports_preview")}),
    )

    def is_active_flag(self, obj: Account) -> bool:
        return bool(obj.is_active)

    is_active_flag.boolean = True
    is_active_flag.short_description = "Активен"

    def get_primary_admin_id(self) -> int | None:
        row = Account.objects.order_by("id").values_list("id", flat=True).first()
        return int(row) if row else None

    def recent_reports_count(self, obj: Account) -> int:
        return obj.report_runs.count()

    recent_reports_count.short_description = "Отчётов"

    def recent_reports_preview(self, obj: Account):
        runs = obj.report_runs.order_by("-created_at", "-id")[:10]
        if not runs:
            return "Нет сохранённых отчётов."
        lines = [
            f"{run.created_at} • {run.period_label} • summary={len(run.parse_json('summary_json') or [])}, workouts={len(run.parse_json('workouts_json') or [])}"
            for run in runs
        ]
        return format_html("<pre style='white-space:pre-wrap'>{}</pre>", "\n".join(lines))

    recent_reports_preview.short_description = "Последние 10 отчётов"

    def get_readonly_fields(self, request, obj=None):
        fields = list(super().get_readonly_fields(request, obj))
        primary_id = self.get_primary_admin_id()
        if obj and primary_id is not None and obj.id == primary_id:
            fields.extend(["role", "is_active"])
        return fields

    def save_model(self, request, obj, form, change):
        primary_id = self.get_primary_admin_id()
        if change and primary_id is not None and obj.id == primary_id:
            original = Account.objects.get(pk=obj.pk)
            obj.role = original.role
            obj.is_active = original.is_active
        super().save_model(request, obj, form, change)

    @admin.action(description="Сделать admin")
    def make_admin(self, request, queryset):
        updated = queryset.update(role="admin")
        self.message_user(request, f"Обновлено админов: {updated}", level=messages.SUCCESS)

    @admin.action(description="Сделать user")
    def make_user(self, request, queryset):
        primary_id = self.get_primary_admin_id()
        safe_queryset = queryset.exclude(id=primary_id) if primary_id is not None else queryset
        updated = safe_queryset.update(role="user")
        self.message_user(request, f"Переведено в user: {updated}", level=messages.SUCCESS)

    @admin.action(description="Включить аккаунты")
    def activate_accounts(self, request, queryset):
        updated = queryset.update(is_active=1)
        self.message_user(request, f"Включено аккаунтов: {updated}", level=messages.SUCCESS)

    @admin.action(description="Отключить аккаунты")
    def deactivate_accounts(self, request, queryset):
        primary_id = self.get_primary_admin_id()
        safe_queryset = queryset.exclude(id=primary_id) if primary_id is not None else queryset
        updated = safe_queryset.update(is_active=0)
        self.message_user(request, f"Отключено аккаунтов: {updated}", level=messages.SUCCESS)


@admin.register(SourceFile)
class SourceFileAdmin(admin.ModelAdmin):
    list_display = ("id", "owner_account", "original_file_name", "parse_status", "file_size", "uploaded_at", "activity_key")
    list_filter = ("parse_status",)
    search_fields = ("original_file_name", "file_name", "activity_key", "owner_account__email")
    readonly_fields = ("owner_account", "file_path", "file_name", "original_file_name", "file_hash", "file_size", "mtime_ns", "parser_version", "parse_status", "error_text", "activity_key", "uploaded_at", "ingested_at")

    def has_add_permission(self, request):
        return False


@admin.register(BackgroundJob)
class BackgroundJobAdmin(admin.ModelAdmin):
    list_display = ("id", "owner_account", "job_type", "status", "stage", "progress_percent", "processed_files", "duplicate_files", "error_files", "created_at")
    list_filter = ("job_type", "status", "stage")
    search_fields = ("owner_account__email", "payload_json")
    readonly_fields = ("owner_account", "job_type", "status", "stage", "total_files", "processed_files", "skipped_files", "duplicate_files", "error_files", "parsed_rows", "progress_percent", "error_text", "payload_pretty", "created_at", "started_at", "finished_at")

    def payload_pretty(self, obj: BackgroundJob):
        return format_html("<pre style='white-space:pre-wrap'>{}</pre>", _pretty_json(obj.payload_json))

    payload_pretty.short_description = "Payload"

    def has_add_permission(self, request):
        return False


@admin.register(MonthlyHistory)
class MonthlyHistoryAdmin(admin.ModelAdmin):
    list_display = ("owner_account", "year", "month", "distance_m", "best_pace_text")
    list_filter = ("year", "distance_m")
    search_fields = ("owner_account__email", "best_pace_text")
    readonly_fields = ("owner_account", "year", "month", "distance_m", "best_pace_s", "best_pace_text")

    def has_add_permission(self, request):
        return False


@admin.register(ReportRun)
class ReportRunAdmin(admin.ModelAdmin):
    list_display = ("id", "owner_account", "created_at", "period_label", "summary_rows_count", "workouts_rows_count")
    list_filter = ("period_label",)
    search_fields = ("owner_account__email", "period_label", "created_at")
    readonly_fields = ("owner_account", "created_at", "period_label", "filters_pretty", "overview_pretty", "summary_pretty", "workouts_pretty")

    def summary_rows_count(self, obj: ReportRun) -> int:
        return len(obj.parse_json("summary_json") or [])

    summary_rows_count.short_description = "Строк summary"

    def workouts_rows_count(self, obj: ReportRun) -> int:
        return len(obj.parse_json("workouts_json") or [])

    workouts_rows_count.short_description = "Строк workouts"

    def filters_pretty(self, obj: ReportRun):
        return format_html("<pre style='white-space:pre-wrap'>{}</pre>", _pretty_json(obj.filters_json))

    filters_pretty.short_description = "Фильтры"

    def overview_pretty(self, obj: ReportRun):
        return format_html("<pre style='white-space:pre-wrap'>{}</pre>", _pretty_json(obj.overview_json))

    overview_pretty.short_description = "Overview"

    def summary_pretty(self, obj: ReportRun):
        return format_html("<pre style='white-space:pre-wrap'>{}</pre>", _pretty_json(obj.summary_json))

    summary_pretty.short_description = "Summary"

    def workouts_pretty(self, obj: ReportRun):
        return format_html("<pre style='white-space:pre-wrap'>{}</pre>", _pretty_json(obj.workouts_json))

    workouts_pretty.short_description = "Workouts"

    def has_add_permission(self, request):
        return False
