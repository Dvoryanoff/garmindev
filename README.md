# Garmin Dev Dashboard

Локальный проект для анализа плавательных FIT-файлов Garmin и просмотра отчётов в браузере.

## Что уже умеет

- читает локальные FIT-файлы из папки `fits`
- сохраняет и переиспользует кэш разбора
- строит dashboard на `localhost`
- фильтрует по типу плавания и периоду
- позволяет задавать список дистанций и порог длинных отрезков
- показывает сводку по дистанциям и тренировкам
- позволяет скачать `summary` и `details` в CSV, совместимом с Excel

## Запуск dashboard

```bash
python3 run_dashboard.py
```

Открыть в браузере:

```text
http://127.0.0.1:8000
```

## Старый CLI-режим

```bash
python3 garmin_local.py
```

Дополнительно можно задать:

- `SWIM_MODE=all|pool|open_water`
- `SWIM_PERIOD=all|year|quarter|month|current_month|last_month`
- `SWIM_DAYS=180`
- `SWIM_MAX_WORKERS=8`
- `SWIM_BATCH_SIZE=400`

## Структура

- `garmin_dashboard/core/` — доменная логика, FIT-парсинг, кэш, конфиг, обработка
- `garmin_dashboard/app/` — отчёты и HTTP-сервер
- `garmin_dashboard/cli.py` — CLI-оболочка
- `garmin_dashboard/analyzer.py` — совместимый facade-слой
- `web/` — dashboard
- `run_dashboard.py` — точка входа для localhost
- `garmin_local.py` — CLI-оболочка над новой логикой

## Дефолты производительности

На этом устройстве протестирована выборка из 300 FIT-файлов. Лучший результат дал конфиг:

- `DEFAULT_MAX_WORKERS=4`
- `DEFAULT_BATCH_SIZE=100`

Повторный локальный benchmark:

```bash
python3 tools/benchmark_runtime.py
```
# Garmin Dev Dashboard

Локальный dashboard для анализа плавательных тренировок Garmin из FIT-файлов.

## Запуск

```bash
python3 run_dashboard.py
```

При старте dashboard печатает адрес `http://127.0.0.1:8000` и по умолчанию открывает браузер автоматически.

Если автооткрытие не нужно:

```bash
GARMIN_OPEN_BROWSER=0 python3 run_dashboard.py
```

## Проверки перед push

Локальный `pre-push` хук запускает:

- компиляцию Python-файлов
- базовые `unittest`-тесты

Ручной запуск:

```bash
python3 tools/check.py
```

## CI на GitHub

В репозитории добавлен GitHub Actions workflow `.github/workflows/ci.yml`, который запускает те же проверки на `push` и `pull_request`.
