import pickle
import re
import tempfile
import zipfile
from collections import defaultdict
from datetime import date
from pathlib import Path
from xml.sax.saxutils import escape

from .config import CACHE_VERSION, MONTHLY_FIXED_DISTANCES, MONTHLY_HISTORY_DIR, RESOURCES_DIR, IntervalConfig, RuntimeConfig
from .db_ingest import load_report_rows
from .dataset import find_fit_files, process_batches
from .rest_metrics import format_rest
from .utils import pace_str_precise, to_datetime


MONTHLY_HISTORY_VERSION = CACHE_VERSION + 101
MONTHLY_INTERVAL_CONFIG = IntervalConfig(target_distances=MONTHLY_FIXED_DISTANCES)
MONTHLY_STATE = MONTHLY_HISTORY_DIR / "garmin_monthly_pace_state.pkl"
MONTH_NAMES_RU = [
    "",
    "Январь",
    "Февраль",
    "Март",
    "Апрель",
    "Май",
    "Июнь",
    "Июль",
    "Август",
    "Сентябрь",
    "Октябрь",
    "Ноябрь",
    "Декабрь",
]

STYLE_DEFAULT = 0
STYLE_HEADER = 1
STYLE_BEST_PACE = 2
MONTHLY_REST_HEADER = "Средний отдых"


def sanitize_user_slug(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9._-]+", "_", str(value or "").strip())
    slug = slug.strip("._-").lower()
    return slug or "unknown_user"


def workbook_path_for_user(user_name: str, user_id: str) -> Path:
    slug = sanitize_user_slug(user_name)
    suffix = sanitize_user_slug(user_id)[-8:] if user_id else "unknown"
    return MONTHLY_HISTORY_DIR / f"{slug}_{suffix}.xlsx"


def workbook_suffix_for_user(user_id: str) -> str:
    return sanitize_user_slug(user_id)[-8:] if user_id else "unknown"


def choose_user_name(current_name: str | None, candidate_name: str | None, user_id: str) -> str:
    current = str(current_name or "").strip()
    candidate = str(candidate_name or "").strip()
    fallback = f"user_{user_id}" if user_id else "unknown_user"

    def score(value: str) -> tuple[int, int]:
        normalized = value.lower()
        placeholder = normalized in {"", "unknown_user", fallback.lower()}
        return (1 if placeholder else 0, len(value))

    best = min([name for name in [current, candidate, fallback] if name], key=score)
    return best


def state_matches_root(files_state: dict, expected_root: Path | None = None) -> bool:
    if not isinstance(files_state, dict):
        return False
    if not files_state:
        return True

    expected_root = (expected_root or RESOURCES_DIR).resolve()
    for key in files_state.keys():
        try:
            path = Path(str(key)).resolve()
        except Exception:
            return False
        if expected_root not in path.parents and path != expected_root:
            return False
    return True


def load_monthly_state(state_file: Path, expected_root: Path | None = None) -> dict:
    if not state_file.exists():
        return {"files": {}, "workouts": {}}

    try:
        with state_file.open("rb") as fh:
            payload = pickle.load(fh)
    except Exception:
        return {"files": {}, "workouts": {}}

    if not isinstance(payload, dict):
        return {"files": {}, "workouts": {}}
    if payload.get("cache_version") != MONTHLY_HISTORY_VERSION:
        return {"files": {}, "workouts": {}}

    files = payload.get("files", {})
    workouts = payload.get("workouts", {})
    if not state_matches_root(files, expected_root=expected_root):
        return {"files": {}, "workouts": {}}
    return {
        "files": files if isinstance(files, dict) else {},
        "workouts": workouts if isinstance(workouts, dict) else {},
    }


def save_pickle_state(state_file: Path, payload: dict) -> None:
    state_file.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="wb",
        dir=state_file.parent,
        prefix=f"{state_file.name}.",
        suffix=".tmp",
        delete=False,
    ) as fh:
        pickle.dump(payload, fh, protocol=pickle.HIGHEST_PROTOCOL)
        tmp_path = Path(fh.name)
    tmp_path.replace(state_file)


def save_monthly_state(state_file: Path, files_state: dict, workouts_state: dict) -> None:
    save_pickle_state(state_file, {
        "cache_version": MONTHLY_HISTORY_VERSION,
        "files": files_state,
        "workouts": workouts_state,
    })


def row_month_start(row: dict) -> date | None:
    for key in ("activity_date", "lap_start", "lap_end"):
        dt = to_datetime(row.get(key))
        if dt:
            return dt.date().replace(day=1)
    return None


def format_monthly_pace(seconds_per_100m: float) -> str:
    return pace_str_precise(seconds_per_100m).replace("/100m", "")


def iter_month_starts(start: date, end: date):
    current = start.replace(day=1)
    end = end.replace(day=1)
    while current <= end:
        yield current
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)


def dedupe_workouts(workouts_state: dict) -> dict:
    unique = {}
    for activity_key, workout in workouts_state.items():
        if not workout or not workout.get("rows"):
            continue
        unique[activity_key] = workout
    return unique


def build_monthly_entries(rows: list[dict], month_rest_by_key: dict[tuple[int, int], float | None] | None = None) -> list[dict]:
    grouped = defaultdict(lambda: defaultdict(list))
    month_dates = []
    month_rest_by_key = month_rest_by_key or {}

    for row in rows:
        month_start = row_month_start(row)
        if not month_start:
            continue
        month_dates.append(month_start)
        distance = int(row["distance_m"])
        if distance in MONTHLY_FIXED_DISTANCES:
            grouped[month_start][distance].append(float(row["time_s"]))

    if not month_dates:
        return []

    entries = []
    for month_start in iter_month_starts(min(month_dates), max(month_dates)):
        row = {
            "year": month_start.year,
            "date": f"{MONTH_NAMES_RU[month_start.month]} {month_start.year}",
        }
        month_group = grouped.get(month_start, {})
        avg_rest_s = month_rest_by_key.get((month_start.year, month_start.month))
        for distance in MONTHLY_FIXED_DISTANCES:
            times = month_group.get(distance, [])
            if times:
                best_time_s = min(times)
                best_pace_s = (best_time_s / distance) * 100.0
                row[distance] = format_monthly_pace(best_pace_s)
                row[f"{distance}_s"] = round(best_pace_s, 4)
            else:
                row[distance] = ""
                row[f"{distance}_s"] = None
        row["avg_rest"] = format_rest(avg_rest_s)
        row["avg_rest_s"] = avg_rest_s
        entries.append(row)
    return entries


def build_entries_by_user(workouts_state: dict) -> dict:
    rows_by_user = defaultdict(list)
    names_by_user = {}
    for workout in workouts_state.values():
        rows = workout.get("rows", [])
        if not rows:
            continue
        user_id = workout.get("user_id") or rows[0].get("user_id") or "unknown"
        user_name = workout.get("user_name") or rows[0].get("user_name") or f"user_{user_id}"
        names_by_user[user_id] = choose_user_name(names_by_user.get(user_id), user_name, user_id)
        rows_by_user[user_id].extend(rows)

    result = {}
    for user_id, rows in rows_by_user.items():
        user_name = names_by_user.get(user_id) or f"user_{user_id}"
        result[(user_id, user_name)] = build_monthly_entries(rows)
    return result


def column_name(index: int) -> str:
    result = []
    while index > 0:
        index, remainder = divmod(index - 1, 26)
        result.append(chr(65 + remainder))
    return "".join(reversed(result))


def build_sheet_xml(rows: list[list[dict]]) -> str:
    sheet_rows = []
    for row_index, cells_data in enumerate(rows, start=1):
        cells = []
        for col_index, cell_data in enumerate(cells_data, start=1):
            cell_ref = f"{column_name(col_index)}{row_index}"
            value = cell_data.get("value", "")
            style = cell_data.get("style", STYLE_DEFAULT)
            escaped = escape(str(value))
            cells.append(f'<c r="{cell_ref}" s="{style}" t="inlineStr"><is><t>{escaped}</t></is></c>')
        sheet_rows.append(f'<row r="{row_index}">{"".join(cells)}</row>')

    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        '<sheetViews><sheetView workbookViewId="0" tabSelected="1"/></sheetViews>'
        '<sheetFormatPr defaultRowHeight="18"/>'
        f'<sheetData>{"".join(sheet_rows)}</sheetData>'
        "</worksheet>"
    )


def write_workbook(entries: list[dict], target_path: Path) -> None:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    rows_by_year = defaultdict(list)
    header = [{"value": "Дата", "style": STYLE_HEADER}, *[
        {"value": str(distance), "style": STYLE_HEADER} for distance in MONTHLY_FIXED_DISTANCES
    ], {"value": MONTHLY_REST_HEADER, "style": STYLE_HEADER}]

    for entry in entries:
        rows_by_year[entry["year"]].append(entry)

    years = sorted(rows_by_year, reverse=True)
    if not years:
        years = [date.today().year]
        rows_by_year[years[0]] = []

    content_types = [
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">',
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>',
        '<Default Extension="xml" ContentType="application/xml"/>',
        '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>',
        '<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>',
    ]
    for sheet_index in range(1, len(years) + 1):
        content_types.append(
            f'<Override PartName="/xl/worksheets/sheet{sheet_index}.xml" '
            'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        )
    content_types.append("</Types>")

    workbook_xml = [
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">',
        '<bookViews><workbookView activeTab="0"/></bookViews>',
        "<sheets>",
    ]
    for sheet_index, year in enumerate(years, start=1):
        workbook_xml.append(f'<sheet name="{year}" sheetId="{sheet_index}" r:id="rId{sheet_index}"/>')
    workbook_xml.append("</sheets></workbook>")

    workbook_rels = [
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">',
    ]
    for sheet_index in range(1, len(years) + 1):
        workbook_rels.append(
            f'<Relationship Id="rId{sheet_index}" '
            'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
            f'Target="worksheets/sheet{sheet_index}.xml"/>'
        )
    workbook_rels.append(
        f'<Relationship Id="rId{len(years) + 1}" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" '
        'Target="styles.xml"/>'
    )
    workbook_rels.append("</Relationships>")

    root_rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
        'Target="xl/workbook.xml"/>'
        "</Relationships>"
    )

    styles_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        '<fonts count="3">'
        '<font><sz val="11"/><name val="Calibri"/></font>'
        '<font><b/><sz val="13"/><name val="Calibri"/></font>'
        '<font><sz val="11"/><color rgb="FFFFFFFF"/><name val="Calibri"/></font>'
        '</fonts>'
        '<fills count="4">'
        '<fill><patternFill patternType="none"/></fill>'
        '<fill><patternFill patternType="gray125"/></fill>'
        '<fill><patternFill patternType="solid"><fgColor rgb="FFF4E7D1"/><bgColor indexed="64"/></patternFill></fill>'
        '<fill><patternFill patternType="solid"><fgColor rgb="FFCD3F3E"/><bgColor indexed="64"/></patternFill></fill>'
        '</fills>'
        '<borders count="1"><border/></borders>'
        '<cellStyleXfs count="1"><xf/></cellStyleXfs>'
        '<cellXfs count="3">'
        '<xf xfId="0" fontId="0" fillId="0" borderId="0" applyFont="0" applyFill="0"/>'
        '<xf xfId="0" fontId="1" fillId="2" borderId="0" applyFont="1" applyFill="1"/>'
        '<xf xfId="0" fontId="2" fillId="3" borderId="0" applyFont="1" applyFill="1"/>'
        '</cellXfs>'
        '<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>'
        "</styleSheet>"
    )

    with tempfile.NamedTemporaryFile(
        mode="wb",
        dir=target_path.parent,
        prefix=f"{target_path.name}.",
        suffix=".tmp",
        delete=False,
    ) as fh:
        with zipfile.ZipFile(fh, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("[Content_Types].xml", "".join(content_types))
            zf.writestr("_rels/.rels", root_rels)
            zf.writestr("xl/workbook.xml", "".join(workbook_xml))
            zf.writestr("xl/_rels/workbook.xml.rels", "".join(workbook_rels))
            zf.writestr("xl/styles.xml", styles_xml)
            for sheet_index, year in enumerate(years, start=1):
                year_entries = rows_by_year[year]
                best_by_distance = {}
                for distance in MONTHLY_FIXED_DISTANCES:
                    values = [entry.get(f"{distance}_s") for entry in year_entries if entry.get(f"{distance}_s") is not None]
                    best_by_distance[distance] = min(values) if values else None

                sheet_rows = [header]
                for entry in year_entries:
                    row = [{"value": entry["date"], "style": STYLE_DEFAULT}]
                    for distance in MONTHLY_FIXED_DISTANCES:
                        is_best = (
                            entry.get(f"{distance}_s") is not None
                            and best_by_distance[distance] is not None
                            and entry.get(f"{distance}_s") == best_by_distance[distance]
                        )
                        row.append({
                            "value": entry.get(distance, ""),
                            "style": STYLE_BEST_PACE if is_best else STYLE_DEFAULT,
                        })
                    row.append({"value": entry.get("avg_rest", ""), "style": STYLE_DEFAULT})
                    sheet_rows.append(row)
                zf.writestr(f"xl/worksheets/sheet{sheet_index}.xml", build_sheet_xml(sheet_rows))
        tmp_path = Path(fh.name)
    tmp_path.replace(target_path)


def refresh_monthly_history(runtime_config: RuntimeConfig) -> dict:
    resource_dir = runtime_config.fit_dir
    state = load_monthly_state(MONTHLY_STATE, expected_root=resource_dir)
    old_files = state["files"]
    workouts_state = dedupe_workouts(state["workouts"])
    fit_files = find_fit_files(resource_dir)

    file_state = {}
    for fit_file in fit_files:
        try:
            stat = fit_file.stat()
        except FileNotFoundError:
            continue
        path_key = str(fit_file.resolve())
        file_state[path_key] = {
            "size": stat.st_size,
            "mtime_ns": stat.st_mtime_ns,
            "name": fit_file.name,
        }

    next_files = {}
    paths_to_process = []
    cached_files = 0
    duplicate_files = 0

    for path_key, meta in file_state.items():
        cached = old_files.get(path_key)
        if (
            cached
            and cached.get("size") == meta["size"]
            and cached.get("mtime_ns") == meta["mtime_ns"]
            and cached.get("interval_config_key") == repr(MONTHLY_INTERVAL_CONFIG)
        ):
            next_files[path_key] = cached
            cached_files += 1
        else:
            paths_to_process.append(path_key)

    if paths_to_process:
        for batch_results in process_batches(
            paths_to_process,
            batch_size=runtime_config.batch_size,
            max_workers=runtime_config.max_workers,
            interval_config=MONTHLY_INTERVAL_CONFIG,
        ):
            for path_key, file_rows in batch_results:
                meta = file_state[path_key]
                activity_key = file_rows[0].get("activity_key") if file_rows else f"empty|{path_key}"
                user_id = file_rows[0].get("user_id", "unknown") if file_rows else "unknown"
                user_name = file_rows[0].get("user_name", f"user_{user_id}") if file_rows else f"user_{user_id}"

                if file_rows and activity_key in workouts_state:
                    duplicate_files += 1
                elif file_rows:
                    workouts_state[activity_key] = {
                        "activity_key": activity_key,
                        "user_id": user_id,
                        "user_name": user_name,
                        "rows": file_rows,
                    }

                next_files[path_key] = {
                    "size": meta["size"],
                    "mtime_ns": meta["mtime_ns"],
                    "interval_config_key": repr(MONTHLY_INTERVAL_CONFIG),
                    "activity_key": activity_key,
                    "user_id": user_id,
                    "user_name": user_name,
                    "accepted": bool(file_rows) and activity_key in workouts_state and workouts_state[activity_key].get("rows") == file_rows,
                }

    entries_by_user = build_entries_by_user(workouts_state)
    workbook_files = []
    suffix_to_target = {}
    for (user_id, user_name), entries in entries_by_user.items():
        workbook_path = workbook_path_for_user(user_name, user_id)
        write_workbook(entries, workbook_path)
        workbook_files.append(str(workbook_path))
        suffix_to_target[workbook_suffix_for_user(user_id)] = workbook_path.resolve()

    current_workbooks = {Path(path).resolve() for path in workbook_files}
    for stale_workbook in MONTHLY_HISTORY_DIR.glob("*.xlsx"):
        resolved = stale_workbook.resolve()
        if resolved not in current_workbooks:
            try:
                suffix = stale_workbook.stem.split("_")[-1]
                target = suffix_to_target.get(suffix)
                if target is None or resolved != target:
                    stale_workbook.unlink()
            except Exception:
                pass

    for temp_workbook in MONTHLY_HISTORY_DIR.glob("~$*.xlsx"):
        try:
            temp_workbook.unlink()
        except Exception:
            pass

    save_monthly_state(MONTHLY_STATE, next_files, workouts_state)

    return {
        "resource_dir": str(resource_dir),
        "workbook_files": workbook_files,
        "users": len(entries_by_user),
        "workouts": len(workouts_state),
        "total_files": len(file_state),
        "cached_files": cached_files,
        "processed_files": len(paths_to_process),
        "duplicate_files": duplicate_files,
    }


def refresh_monthly_history_from_database(runtime_config: RuntimeConfig) -> dict:
    rows, _ = load_report_rows(
        runtime_config=RuntimeConfig(
            fit_dir=runtime_config.fit_dir,
            detail_csv=runtime_config.detail_csv,
            summary_csv=runtime_config.summary_csv,
            cache_file=runtime_config.cache_file,
            database_url=runtime_config.database_url,
            db_auto_ingest=False,
            max_workers=runtime_config.max_workers,
            batch_size=runtime_config.batch_size,
        ),
        swim_mode="all",
        start_date="",
        end_date="",
    )

    grouped = defaultdict(list)
    for row in rows:
        activity_key = row.get("activity_key") or "unknown"
        grouped[activity_key].append(row)

    workouts_state = {}
    for activity_key, activity_rows in grouped.items():
        sample = activity_rows[0]
        user_id = sample.get("user_id") or "unknown"
        user_name = sample.get("user_name") or f"user_{user_id}"
        workouts_state[activity_key] = {
            "activity_key": activity_key,
            "user_id": user_id,
            "user_name": user_name,
            "rows": activity_rows,
        }

    entries_by_user = build_entries_by_user(workouts_state)
    workbook_files = []
    suffix_to_target = {}
    for (user_id, user_name), entries in entries_by_user.items():
        workbook_path = workbook_path_for_user(user_name, user_id)
        write_workbook(entries, workbook_path)
        workbook_files.append(str(workbook_path))
        suffix_to_target[workbook_suffix_for_user(user_id)] = workbook_path.resolve()

    current_workbooks = {Path(path).resolve() for path in workbook_files}
    for stale_workbook in MONTHLY_HISTORY_DIR.glob("*.xlsx"):
        resolved = stale_workbook.resolve()
        if resolved not in current_workbooks:
            try:
                suffix = stale_workbook.stem.split("_")[-1]
                target = suffix_to_target.get(suffix)
                if target is None or resolved != target:
                    stale_workbook.unlink()
            except Exception:
                pass

    for temp_workbook in MONTHLY_HISTORY_DIR.glob("~$*.xlsx"):
        try:
            temp_workbook.unlink()
        except Exception:
            pass

    return {
        "resource_dir": str(runtime_config.fit_dir),
        "workbook_files": workbook_files,
        "users": len(entries_by_user),
        "workouts": len(workouts_state),
        "total_files": 0,
        "cached_files": 0,
        "processed_files": 0,
        "duplicate_files": 0,
    }


def monthly_rows_to_entries(rows: list[dict]) -> list[dict]:
    if rows and "date" in rows[0]:
        return rows
    grouped: dict[tuple[int, int], dict] = {}
    for row in rows:
        year = int(row.get("year") or 0)
        month = int(row.get("month") or 0)
        if not year or not month:
            continue
        key = (year, month)
        entry = grouped.setdefault(
            key,
            {
                "year": year,
                "date": f"{MONTH_NAMES_RU[month]} {year}",
            },
        )
        distance = int(row.get("distance_m") or 0)
        entry[distance] = row.get("best_pace_text", "")
        entry[f"{distance}_s"] = row.get("best_pace_s")

    if not grouped:
        return []
    return [grouped[key] for key in sorted(grouped.keys(), reverse=True)]


def build_monthly_history_payload(rows: list[dict]) -> dict:
    entries = monthly_rows_to_entries(rows)
    headers = list(MONTHLY_FIXED_DISTANCES)
    years = sorted({entry["year"] for entry in entries}, reverse=True)
    best_by_year_distance = {}
    for year in years:
        for distance in headers:
            values = [entry.get(f"{distance}_s") for entry in entries if entry.get("year") == year and entry.get(f"{distance}_s") is not None]
            best_by_year_distance[(year, distance)] = min(values) if values else None
    return {
        "headers": headers,
        "years": years,
        "rows": [
            {
                "year": entry["year"],
                "month": entry["date"].split(" ")[0],
                "date": entry["date"],
                "avg_rest": entry.get("avg_rest", ""),
                "values": [
                    {
                        "text": entry.get(distance, ""),
                        "best": (
                            entry.get(f"{distance}_s") is not None
                            and best_by_year_distance.get((entry["year"], distance)) is not None
                            and entry.get(f"{distance}_s") == best_by_year_distance.get((entry["year"], distance))
                        ),
                    }
                    for distance in headers
                ],
            }
            for entry in entries
        ],
    }


def build_yearly_records_payload(rows: list[dict]) -> dict:
    headers = list(MONTHLY_FIXED_DISTANCES)
    best_by_year_distance: dict[tuple[int, int], tuple[float, str]] = {}
    for row in rows:
        year = int(row.get("year") or 0)
        distance = int(row.get("distance_m") or 0)
        pace_s = row.get("best_pace_s")
        pace_text = str(row.get("best_pace_text") or "")
        if not year or distance not in headers or pace_s in (None, "", 0):
            continue
        try:
            pace_value = float(pace_s)
        except Exception:
            continue
        key = (year, distance)
        current = best_by_year_distance.get(key)
        if current is None or pace_value < current[0]:
            best_by_year_distance[key] = (pace_value, pace_text)

    years = sorted({year for year, _ in best_by_year_distance.keys()}, reverse=True)
    overall_best_by_distance: dict[int, float | None] = {}
    for distance in headers:
        values = [best_by_year_distance[(year, distance)][0] for year in years if (year, distance) in best_by_year_distance]
        overall_best_by_distance[distance] = min(values) if values else None

    return {
        "headers": headers,
        "rows": [
            {
                "year": year,
                "values": [
                    {
                        "text": best_by_year_distance.get((year, distance), ("", ""))[1] or "",
                        "best": (
                            (year, distance) in best_by_year_distance
                            and overall_best_by_distance.get(distance) is not None
                            and best_by_year_distance[(year, distance)][0] == overall_best_by_distance.get(distance)
                        ),
                    }
                    for distance in headers
                ],
            }
            for year in years
        ],
    }


def build_monthly_history_workbook_bytes(rows: list[dict]) -> bytes:
    entries = monthly_rows_to_entries(rows)
    with tempfile.NamedTemporaryFile(mode="wb", suffix=".xlsx", delete=False) as fh:
        tmp_path = Path(fh.name)
    try:
        write_workbook(entries, tmp_path)
        return tmp_path.read_bytes()
    finally:
        try:
            tmp_path.unlink()
        except Exception:
            pass
