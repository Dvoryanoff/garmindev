PYTHON ?= python3

.PHONY: help run run-no-browser run-django-admin django-migrate restart test test-reports test-auth check benchmark organize-fits db-reset cache-clear runtime-clear reset-local git-status git-push git-wip

help:
	@echo "Доступные команды:"
	@echo "  make run            - запустить дашборд"
	@echo "  make run-no-browser - запустить без автооткрытия браузера"
	@echo "  make run-django-admin - запустить только Django admin"
	@echo "  make django-migrate - применить миграции Django admin"
	@echo "  make test           - все тесты"
	@echo "  make test-reports   - тесты отчётов"
	@echo "  make test-auth      - тесты auth/jobs"
	@echo "  make check          - полный локальный check"
	@echo "  make benchmark      - benchmark runtime"
	@echo "  make organize-fits  - разложить resources/fits по годам"
	@echo "  make db-reset       - удалить локальную sqlite базу"
	@echo "  make cache-clear    - удалить локальные кэши и временные экспортные артефакты"
	@echo "  make runtime-clear  - очистить uploads/runtime/monthly_history"
	@echo "  make reset-local    - полностью сбросить локальные данные проекта"
	@echo "  make git-status     - git status --short"
	@echo "  make git-wip        - git add . && git commit -m \"WIP\""
	@echo "  make git-push       - git push"

run:
	$(PYTHON) run_dashboard.py

run-no-browser:
	GARMIN_OPEN_BROWSER=0 $(PYTHON) run_dashboard.py

run-django-admin:
	$(PYTHON) run_django_admin.py

django-migrate:
	$(PYTHON) manage.py migrate

restart:
	$(MAKE) run-no-browser

test:
	$(PYTHON) -m pytest -q tests

test-reports:
	$(PYTHON) -m pytest -q tests/test_reports.py tests/test_monthly_history.py

test-auth:
	$(PYTHON) -m pytest -q tests/test_jobs_and_auth.py tests/test_db_ingest.py

check:
	$(PYTHON) tools/check.py

benchmark:
	$(PYTHON) tools/benchmark_runtime.py

organize-fits:
	$(PYTHON) tools/organize_fits_by_year.py

db-reset:
	rm -f garmin_dashboard.db

cache-clear:
	rm -f garmin_swim_fit_cache.pkl
	rm -f garmin_swim_intervals_details.csv
	rm -f garmin_swim_intervals_summary.csv
	rm -f monthly_history/*.pkl

runtime-clear:
	rm -rf uploads
	rm -rf runtime
	rm -f monthly_history/*.xlsx
	rm -f monthly_history/*.pkl

reset-local: db-reset cache-clear runtime-clear

git-status:
	git status --short

git-wip:
	git add .
	git commit -m "WIP"

git-push:
	git push
