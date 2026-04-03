const resourceDirEl = document.getElementById("resourceDir");
const swimModeEl = document.getElementById("swimMode");
const periodModePresetEl = document.getElementById("periodModePreset");
const periodModeCustomEl = document.getElementById("periodModeCustom");
const presetPeriodWrapEl = document.getElementById("presetPeriodWrap");
const periodEl = document.getElementById("period");
const daysWrapEl = document.getElementById("daysWrap");
const daysEl = document.getElementById("days");
const longMinDistanceEl = document.getElementById("longMinDistance");
const distancePickerEl = document.getElementById("distancePicker");
const selectedDistancesTextEl = document.getElementById("selectedDistancesText");
const loadButtonEl = document.getElementById("loadButton");
const saveCsvButtonEl = document.getElementById("saveCsvButton");
const metricsEl = document.getElementById("metrics");
const runtimeBannerEl = document.getElementById("runtimeBanner");
const summaryTableEl = document.querySelector("#summaryTable tbody");
const workoutsTableEl = document.querySelector("#workoutsTable tbody");
const metaBoxEl = document.getElementById("metaBox");
const workoutsHintEl = document.getElementById("workoutsHint");
const summaryExportEl = document.getElementById("summaryExport");
const detailsExportEl = document.getElementById("detailsExport");
const heroSourceEl = document.getElementById("heroSource");
const defaultSelectedDistances = new Set([50, 100, 150, 200, 300, 400, 500, 600, 800, 1000]);
let runtimeStatusTimer = null;

function buildDistanceOptions(maxDistance) {
  const options = [];
  for (let distance = 50; distance <= maxDistance; distance += 50) {
    options.push(distance);
  }
  return options;
}

function getSelectedDistances() {
  return Array.from(distancePickerEl.querySelectorAll('input[type="checkbox"]:checked'))
    .map((input) => Number(input.value))
    .filter((value) => Number.isFinite(value))
    .sort((a, b) => a - b);
}

function syncSelectedDistancesText() {
  const selected = getSelectedDistances();
  selectedDistancesTextEl.textContent = selected.length ? selected.join(", ") : "ничего не выбрано";
}

function renderDistancePicker() {
  const previousSelected = new Set(getSelectedDistances());
  const maxDistance = Math.max(50, Number(longMinDistanceEl.value || 1000));
  const options = buildDistanceOptions(maxDistance);

  distancePickerEl.innerHTML = options.map((distance) => {
    const checked = previousSelected.size
      ? previousSelected.has(distance)
      : defaultSelectedDistances.has(distance);
    return `
      <label class="distance-item">
        <input type="checkbox" value="${distance}" ${checked ? "checked" : ""}>
        <span>${distance} м</span>
      </label>
    `;
  }).join("");

  distancePickerEl.querySelectorAll('input[type="checkbox"]').forEach((checkbox) => {
    checkbox.addEventListener("change", syncSelectedDistancesText);
  });

  syncSelectedDistancesText();
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function buildQuery({ persistCsv = false } = {}) {
  const useCustomDays = periodModeCustomEl.checked;
  const params = new URLSearchParams({
    resource_dir: resourceDirEl.value,
    swim_mode: swimModeEl.value,
    period: useCustomDays ? "all" : periodEl.value,
  });

  if (useCustomDays) {
    params.set("days", daysEl.value || "180");
  }

  params.set("distances", getSelectedDistances().join(","));
  params.set("long_min_distance", longMinDistanceEl.value || "1000");

  if (persistCsv) {
    params.set("persist_csv", "1");
  }

  return params;
}

function syncPeriodMode() {
  const useCustomDays = periodModeCustomEl.checked;

  presetPeriodWrapEl.classList.toggle("is-disabled", useCustomDays);
  daysWrapEl.classList.toggle("is-disabled", !useCustomDays);

  periodEl.disabled = useCustomDays;
  daysEl.disabled = !useCustomDays;
}

function renderMetrics(overview, filters) {
  const items = [
    ["Период", filters.period_label, `${filters.date_start || "начало"} - ${filters.date_end || "сегодня"}`],
    ["Отрезков", overview.intervals, `${overview.workouts} тренировок`],
    ["Объём", `${overview.total_distance_m} м`, overview.swim_types.join(", ") || "без типа"],
    ["Время", overview.total_time, overview.strokes.join(", ") || "без стиля"],
    ["Средний темп", overview.avg_pace_100m || "—", `Лучший: ${overview.best_pace_100m || "—"}`],
  ];

  metricsEl.innerHTML = items.map(([label, value, note]) => `
    <article class="metric">
      <div class="metric-label">${escapeHtml(label)}</div>
      <div class="metric-value">${escapeHtml(value)}</div>
      <div class="metric-note">${escapeHtml(note)}</div>
    </article>
  `).join("");
}

function renderSummary(rows) {
  if (!rows.length) {
    summaryTableEl.innerHTML = `<tr><td colspan="8">Нет данных для выбранных фильтров.</td></tr>`;
    return;
  }

  summaryTableEl.innerHTML = rows.map((row) => `
    <tr>
      <td>${escapeHtml(row.distance_m)} м</td>
      <td>${escapeHtml(row.count)}</td>
      <td>${escapeHtml(row.avg_time)}</td>
      <td>${escapeHtml(row.best_time)}</td>
      <td>${escapeHtml(row.best_pace_100m)}</td>
      <td>${escapeHtml(row.best_pace_date)}</td>
      <td>${escapeHtml(row.avg_pace_100m)}</td>
      <td>${escapeHtml(row.middle_pace_100m)} <span class="muted">(${escapeHtml(row.middle_count)})</span></td>
    </tr>
  `).join("");
}

function renderWorkouts(rows) {
  workoutsHintEl.textContent = `${rows.length} тренировок в выборке`;
  if (!rows.length) {
    workoutsTableEl.innerHTML = `<tr><td colspan="4">Тренировки не найдены.</td></tr>`;
    return;
  }

  workoutsTableEl.innerHTML = rows.map((row) => `
    <tr>
      <td>${escapeHtml(row.date)}</td>
      <td>${escapeHtml(row.total_distance_m)} м</td>
      <td>${escapeHtml(row.total_time)}</td>
      <td>${escapeHtml(row.best_pace_100m)}</td>
    </tr>
  `).join("");
}

function renderMeta(meta) {
  metaBoxEl.innerHTML = `
    <div><strong>Папка FIT:</strong> ${escapeHtml(meta.fit_dir)}</div>
    <div><strong>Всего файлов:</strong> ${escapeHtml(meta.total_files)}</div>
    <div><strong>Из кэша:</strong> ${escapeHtml(meta.cached_files)}</div>
    <div><strong>Переработано:</strong> ${escapeHtml(meta.processed_files)}</div>
    <div><strong>Сгенерировано:</strong> ${escapeHtml(meta.generated_at)}</div>
    <div><strong>Worker'ы:</strong> ${escapeHtml(meta.max_workers)}</div>
    <div><strong>Batch size:</strong> ${escapeHtml(meta.batch_size)}</div>
    <hr>
    <div><strong>Поиск файлов:</strong> ${escapeHtml(meta.timings.find_files)}</div>
    <div><strong>Загрузка кэша:</strong> ${escapeHtml(meta.timings.load_cache)}</div>
    <div><strong>Сверка кэша:</strong> ${escapeHtml(meta.timings.cache_compare)}</div>
    <div><strong>Декод FIT:</strong> ${escapeHtml(meta.timings.decode_files)}</div>
    <div><strong>Сохранение кэша:</strong> ${escapeHtml(meta.timings.save_cache)}</div>
    <div><strong>Итого:</strong> ${escapeHtml(meta.timings.total)}</div>
  `;
}

function setRuntimeBanner(message = "", visible = false) {
  runtimeBannerEl.textContent = message;
  runtimeBannerEl.classList.toggle("is-hidden", !visible || !message);
}

async function refreshRuntimeStatus() {
  try {
    const response = await fetch(`/api/runtime-status?_ts=${Date.now()}`, {
      cache: "no-store",
    });
    const payload = await response.json();
    if (payload.monthly_processing) {
      setRuntimeBanner(payload.monthly_message || "Обработка данных...", true);
    } else {
      setRuntimeBanner("", false);
    }
    return payload;
  } catch (error) {
    // Ignore runtime status errors to avoid blocking report rendering.
    return null;
  }
}

async function loadReport({ persistCsv = false } = {}) {
  loadButtonEl.disabled = true;
  saveCsvButtonEl.disabled = true;
  loadButtonEl.textContent = persistCsv ? "Пересборка..." : "Загрузка...";
  setRuntimeBanner("Обработка данных, пожалуйста подожди...", true);

  const query = buildQuery({ persistCsv });
  query.set("_ts", String(Date.now()));
  summaryExportEl.href = `/api/export/summary.csv?${query.toString()}`;
  detailsExportEl.href = `/api/export/details.csv?${query.toString()}`;

  try {
    const response = await fetch(`/api/report?${query.toString()}`, {
      cache: "no-store",
    });
    const payload = await response.json();

    if (!response.ok) {
      throw new Error(payload.error || "Не удалось построить отчёт");
    }

    renderMetrics(payload.overview, payload.filters);
    renderSummary(payload.summary);
    renderWorkouts(payload.workouts);
    renderMeta(payload.dataset_meta);
    await refreshRuntimeStatus();
  } catch (error) {
    alert(error.message);
  } finally {
    loadButtonEl.disabled = false;
    saveCsvButtonEl.disabled = false;
    loadButtonEl.textContent = "Обновить отчёт";
    if (runtimeBannerEl.textContent === "Обработка данных, пожалуйста подожди...") {
      setRuntimeBanner("", false);
    }
  }
}

async function loadResources() {
  const response = await fetch(`/api/resources?_ts=${Date.now()}`, {
    cache: "no-store",
  });
  const payload = await response.json();

  if (!response.ok) {
    throw new Error(payload.error || "Не удалось получить список папок с FIT-файлами");
  }

  const resources = Array.isArray(payload.resources) ? payload.resources : [];
  resourceDirEl.innerHTML = resources.map((resource) => `
    <option value="${escapeHtml(resource.name)}">${escapeHtml(resource.name)}</option>
  `).join("");

  if (payload.default_resource) {
    resourceDirEl.value = payload.default_resource;
    heroSourceEl.textContent = payload.default_resource;
  }
}

periodModePresetEl.addEventListener("change", syncPeriodMode);
periodModeCustomEl.addEventListener("change", syncPeriodMode);

longMinDistanceEl.addEventListener("change", renderDistancePicker);
resourceDirEl.addEventListener("change", () => {
  heroSourceEl.textContent = resourceDirEl.value || "локальная папка Garmin Dev";
});

loadButtonEl.addEventListener("click", () => loadReport());
saveCsvButtonEl.addEventListener("click", () => loadReport({ persistCsv: true }));

syncPeriodMode();
renderDistancePicker();

runtimeStatusTimer = window.setInterval(refreshRuntimeStatus, 3000);

(async () => {
  try {
    await loadResources();
    await refreshRuntimeStatus();
    await loadReport();
  } catch (error) {
    alert(error.message);
  }
})();
