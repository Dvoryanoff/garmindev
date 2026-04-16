# Garmin Dev Dashboard

Multi-user веб-сервис для загрузки Garmin FIT-файлов, инкрементального ingest в базу данных и построения персональных отчётов по плаванию.

Сервис уже работает как личный кабинет:

- пользователь регистрируется и входит по `e-mail / password`
- загружает FIT-файлы через веб-интерфейс
- данные сохраняются в БД и больше не читаются напрямую из исходных файлов для каждого отчёта
- отчёты, monthly history и экспорт строятся только по данным текущего пользователя
- админка вынесена в отдельный маршрут `/admin`

## Возможности

- регистрация и вход пользователей
- cookie-сессии и изоляция данных по `user_id`
- загрузка FIT-файлов отдельными файлами или целой папкой
- пакетная загрузка больших наборов файлов
- дедупликация по `sha256` файла и `activity_key`
- хранение реестра обработанных файлов
- хранение нормализованных `activities`, `intervals` и raw payload
- фильтрация отчётов по периоду, типу плавания и выбранным дистанциям
- сохранение последних пользовательских фильтров
- помесячная история с выбором года
- экспорт в Excel для summary, workouts и monthly history
- отдельная админка для управления пользователями, ролями и статусами

## Архитектура

Проект работает по модели `ingest once -> query many`:

1. Пользователь загружает FIT-файлы через UI.
2. Сервис сохраняет файл, считает hash и проверяет дубликаты.
3. FIT разбирается один раз.
4. Данные пишутся в БД:
   - `accounts`
   - `user_sessions`
   - `user_preferences`
   - `source_files`
   - `activities`
   - `activity_payloads`
   - `intervals`
   - `monthly_history`
5. Дашборд и monthly history строятся уже из БД.

Это позволяет:

- быстро пересчитывать отчёты при смене фильтров
- не перечитывать одни и те же FIT-файлы при каждом запросе
- безопасно разделять данные нескольких пользователей

## Quick Start

### Локальный запуск

Установить зависимости:

```bash
python3 -m pip install -r requirements.txt
```

Запустить приложение:

```bash
python3 run_dashboard.py
```

По умолчанию сервис будет доступен на [http://127.0.0.1:8000](http://127.0.0.1:8000).

Если не нужно автоматически открывать браузер:

```bash
GARMIN_OPEN_BROWSER=0 python3 run_dashboard.py
```

### Короткие команды через Make

Если удобнее короткими командами:

```bash
make run
make run-no-browser
make test
make check
```

### Docker

Подготовить env:

```bash
cp .env.example .env
```

Запустить сервисы:

```bash
docker compose up --build
```

По умолчанию frontend будет доступен на [http://127.0.0.1:8080](http://127.0.0.1:8080).

При первом запуске на пустой базе данных первый зарегистрированный через UI пользователь автоматически получает роль `admin`.

## Пользовательский сценарий

1. Открыть главную страницу `/`.
2. Зарегистрироваться или войти.
3. Если это новый аккаунт, сначала появится экран первой загрузки FIT-файлов.
4. После успешного ingest откроется персональный дашборд.
5. При следующих входах пользователь возвращается к последнему рабочему состоянию с сохранёнными фильтрами.

Что есть в дашборде:

- загрузка новых FIT-файлов
- summary по дистанциям
- workouts
- monthly history
- export в Excel
- фильтры:
  - режим плавания
  - период
  - свои `N` дней
  - порог длинных дистанций
  - выбранные дистанции отчёта

## Админка

Админка открывается по отдельному маршруту:

- локально: [http://127.0.0.1:8000/admin](http://127.0.0.1:8000/admin)
- Docker: [http://127.0.0.1:8080/admin](http://127.0.0.1:8080/admin)

При чистой базе админка инициализируется через bootstrap-режим:

- первый зарегистрированный пользователь становится `admin`
- все последующие регистрации получают роль `user`

Сейчас админка умеет:

- смотреть список пользователей
- видеть базовую статистику по каждому аккаунту
- менять роль `user/admin`
- включать и отключать учётные записи

## Конфигурация

Основные переменные окружения:

- `GARMIN_HOST`
- `GARMIN_PORT`
- `GARMIN_OPEN_BROWSER`
- `GARMIN_MAX_WORKERS`
- `GARMIN_BATCH_SIZE`
- `DATABASE_URL`
- `GARMIN_UPLOAD_DIR`
- `GARMIN_SESSION_TTL_DAYS`
- `GARMIN_BOOTSTRAP_ADMIN_EMAIL`

Для Docker Compose:

- `GARMIN_FRONTEND_PORT`
- `GARMIN_POSTGRES_DB`
- `GARMIN_POSTGRES_USER`
- `GARMIN_POSTGRES_PASSWORD`

Пример находится в [`.env.example`](/Users/dvoryanoff/DEV/garmindev/.env.example).

## Хранение данных

В локальном режиме по умолчанию используется `sqlite`.

В Docker Compose:

- `frontend` — `nginx`
- `app` — Python backend
- `db` — `PostgreSQL`

Docker volumes:

- `garmin_postgres` — база данных
- `garmin_runtime` — runtime-артефакты
- `garmin_monthly_history` — monthly history exports/state
- `garmin_uploads` — загруженные FIT-файлы

## Проверки

Полный локальный check:

```bash
python3 tools/check.py
```

Сейчас он включает:

- `py_compile`
- unit/integration tests из `tests/`

Ключевой автоматический multi-user тест уже проверяет:

- двух пользователей
- сессии
- отдельный ingest для каждого
- отдельные отчёты
- monthly history
- смену фильтров и пересборку отчёта
- admin statistics

## Шпаргалка Команд

### Запуск проекта

Обычный запуск:

```bash
python3 run_dashboard.py
```

Запуск без автооткрытия браузера:

```bash
GARMIN_OPEN_BROWSER=0 python3 run_dashboard.py
```

Коротко через `make`:

```bash
make run
make run-no-browser
```

### Тесты и проверки

Все тесты:

```bash
python3 -m pytest -q tests
```

Только отчёты и история:

```bash
python3 -m pytest -q tests/test_reports.py tests/test_monthly_history.py
```

Только ingest, jobs и auth:

```bash
python3 -m pytest -q tests/test_jobs_and_auth.py tests/test_db_ingest.py
```

Полный локальный check:

```bash
python3 tools/check.py
```

Коротко через `make`:

```bash
make test
make test-reports
make test-auth
make check
```

### Производительность и сервисные скрипты

Локальный benchmark:

```bash
python3 tools/benchmark_runtime.py
```

Разложить `resources/fits` по годам:

```bash
python3 tools/organize_fits_by_year.py
```

Коротко через `make`:

```bash
make benchmark
make organize-fits
```

### Очистка локальных данных

Удалить только локальную sqlite-базу:

```bash
rm -f garmin_dashboard.db
```

Удалить кэши и временные CSV:

```bash
rm -f garmin_swim_fit_cache.pkl
rm -f garmin_swim_intervals_details.csv
rm -f garmin_swim_intervals_summary.csv
rm -f monthly_history/*.pkl
```

Очистить uploads/runtime и файлы monthly history:

```bash
rm -rf uploads
rm -rf runtime
rm -f monthly_history/*.xlsx
rm -f monthly_history/*.pkl
```

Полный локальный сброс проекта:

```bash
rm -f garmin_dashboard.db
rm -f garmin_swim_fit_cache.pkl
rm -f garmin_swim_intervals_details.csv
rm -f garmin_swim_intervals_summary.csv
rm -rf uploads
rm -rf runtime
rm -f monthly_history/*.xlsx
rm -f monthly_history/*.pkl
```

Коротко через `make`:

```bash
make db-reset
make cache-clear
make runtime-clear
make reset-local
```

### Git

Посмотреть изменения:

```bash
git status
git status --short
```

Закоммитить и отправить:

```bash
git add .
git commit -m "your message"
git push
```

Коротко:

```bash
make git-status
make git-wip
make git-push
```

Если хочешь всегда одинаковый промежуточный коммит без размышлений над текстом:

```bash
make git-wip
make git-push
```

## Структура проекта

- [garmin_dashboard/core](/Users/dvoryanoff/DEV/garmindev/garmin_dashboard/core) — конфиг, БД, auth, ingest, FIT-парсинг
- [garmin_dashboard/app](/Users/dvoryanoff/DEV/garmindev/garmin_dashboard/app) — HTTP API и сборка отчётов
- [web](/Users/dvoryanoff/DEV/garmindev/web) — frontend для дашборда и админки
- [tests](/Users/dvoryanoff/DEV/garmindev/tests) — тесты
- [docker](/Users/dvoryanoff/DEV/garmindev/docker) — frontend image и nginx-конфигурация
- [docker-compose.yml](/Users/dvoryanoff/DEV/garmindev/docker-compose.yml) — локальная оркестрация

## Текущее состояние

Проект уже переведён с модели “каждый раз читать FIT-файлы и кэши” на модель “загружать в БД и строить отчёты из БД”.

Следующие естественные шаги развития:

- расширение админки
- управление паролями через UI
- фоновые import jobs с прогрессом
- дальнейшая оптимизация хранения raw payload и исходных FIT-файлов
