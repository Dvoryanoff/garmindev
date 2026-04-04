const authViewEl = document.getElementById("authView");
const appViewEl = document.getElementById("appView");
const authChooserEl = document.getElementById("authChooser");
const authFormsEl = document.getElementById("authForms");
const showLoginButtonEl = document.getElementById("showLoginButton");
const showRegisterButtonEl = document.getElementById("showRegisterButton");
const loginCardEl = document.getElementById("loginCard");
const registerCardEl = document.getElementById("registerCard");
const backFromLoginButtonEl = document.getElementById("backFromLoginButton");
const backFromRegisterButtonEl = document.getElementById("backFromRegisterButton");
const loginEmailEl = document.getElementById("loginEmail");
const loginPasswordEl = document.getElementById("loginPassword");
const loginButtonEl = document.getElementById("loginButton");
const loginStatusEl = document.getElementById("loginStatus");
const registerFirstNameEl = document.getElementById("registerFirstName");
const registerLastNameEl = document.getElementById("registerLastName");
const registerEmailEl = document.getElementById("registerEmail");
const registerPasswordEl = document.getElementById("registerPassword");
const registerButtonEl = document.getElementById("registerButton");
const registerStatusEl = document.getElementById("registerStatus");
const accountNameEl = document.getElementById("accountName");
const accountEmailEl = document.getElementById("accountEmail");
const accountRoleEl = document.getElementById("accountRole");
const adminLinkEl = document.getElementById("adminLink");
const logoutButtonEl = document.getElementById("logoutButton");
const uploadOnlyStateEl = document.getElementById("uploadOnlyState");
const dashboardContentEl = document.getElementById("dashboardContent");
const uploadInputEl = document.getElementById("uploadInput");
const uploadFolderInputEl = document.getElementById("uploadFolderInput");
const uploadButtonEl = document.getElementById("uploadButton");
const pickFilesButtonEl = document.getElementById("pickFilesButton");
const pickFolderButtonEl = document.getElementById("pickFolderButton");
const uploadSelectionEl = document.getElementById("uploadSelection");
const uploadInputEmptyEl = document.getElementById("uploadInputEmpty");
const uploadFolderInputEmptyEl = document.getElementById("uploadFolderInputEmpty");
const uploadButtonEmptyEl = document.getElementById("uploadButtonEmpty");
const pickFilesButtonEmptyEl = document.getElementById("pickFilesButtonEmpty");
const pickFolderButtonEmptyEl = document.getElementById("pickFolderButtonEmpty");
const uploadSelectionEmptyEl = document.getElementById("uploadSelectionEmpty");
const uploadHintEl = document.getElementById("uploadHint");
const uploadHintEmptyEl = document.getElementById("uploadHintEmpty");
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
const metricsEl = document.getElementById("metrics");
const runtimeBannerEl = document.getElementById("runtimeBanner");
const summaryTableEl = document.querySelector("#summaryTable tbody");
const workoutsTableEl = document.querySelector("#workoutsTable tbody");
const metaBoxEl = document.getElementById("metaBox");
const workoutsHintEl = document.getElementById("workoutsHint");
const summaryExportEl = document.getElementById("summaryExport");
const workoutsExportEl = document.getElementById("workoutsExport");
const monthlyHeaderRowEl = document.getElementById("monthlyHeaderRow");
const monthlyTableBodyEl = document.querySelector("#monthlyTable tbody");
const monthlyExportEl = document.getElementById("monthlyExport");
const monthlyYearSelectEl = document.getElementById("monthlyYearSelect");

const defaultSelectedDistances = new Set([50, 100, 150, 200, 300, 400, 500, 600, 800, 1000]);
let runtimeStatusTimer = null;
let sessionState = null;
let monthlyHistoryState = { headers: [], years: [], rows: [] };
let authRequestState = { login: false, register: false };
const MAX_FILES_PER_BATCH = 50;
const MAX_BATCH_BYTES = 16 * 1024 * 1024;

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function showAuth() {
  authViewEl.hidden = false;
  appViewEl.hidden = true;
  authChooserEl.hidden = false;
  authFormsEl.hidden = true;
  loginCardEl.hidden = true;
  registerCardEl.hidden = true;
  setFormStatus(loginStatusEl);
  setFormStatus(registerStatusEl);
}

function showApp() {
  authViewEl.hidden = true;
  appViewEl.hidden = false;
}

function showAuthForm(mode) {
  authChooserEl.hidden = true;
  authFormsEl.hidden = false;
  loginCardEl.hidden = mode !== "login";
  registerCardEl.hidden = mode !== "register";
  if (mode === "login") {
    setFormStatus(registerStatusEl);
  } else {
    setFormStatus(loginStatusEl);
  }
}

function setFormStatus(element, message = "", kind = "") {
  element.textContent = message;
  element.hidden = !message;
  element.classList.remove("is-error", "is-success");
  if (kind) {
    element.classList.add(kind === "error" ? "is-error" : "is-success");
  }
}

function showDashboard(hasData) {
  uploadOnlyStateEl.hidden = hasData;
  dashboardContentEl.hidden = !hasData;
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    credentials: "same-origin",
    ...options,
  });
  const contentType = response.headers.get("Content-Type") || "";
  const payload = contentType.includes("application/json") ? await response.json() : null;
  const fallbackText = payload ? "" : await response.text();
  if (!response.ok) {
    throw new Error((payload && payload.error) || fallbackText || `Ошибка запроса (${response.status})`);
  }
  return payload;
}

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

function renderDistancePicker(preselected = null) {
  const currentSelected = preselected ? new Set(preselected) : new Set(getSelectedDistances());
  const maxDistance = Math.max(50, Number(longMinDistanceEl.value || 1000));
  const options = buildDistanceOptions(maxDistance);
  distancePickerEl.innerHTML = options.map((distance) => {
    const checked = currentSelected.size ? currentSelected.has(distance) : defaultSelectedDistances.has(distance);
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

function syncPeriodMode() {
  const useCustomDays = periodModeCustomEl.checked;
  presetPeriodWrapEl.classList.toggle("is-disabled", useCustomDays);
  daysWrapEl.classList.toggle("is-disabled", !useCustomDays);
  periodEl.disabled = useCustomDays;
  daysEl.disabled = !useCustomDays;
}

function applyPreferences(preferences = {}) {
  swimModeEl.value = preferences.swim_mode || "all";
  periodEl.value = preferences.period || "current_year";
  daysEl.value = preferences.days || 180;
  longMinDistanceEl.value = preferences.long_min_distance || 1000;

  if (preferences.days) {
    periodModeCustomEl.checked = true;
    periodModePresetEl.checked = false;
  } else {
    periodModePresetEl.checked = true;
    periodModeCustomEl.checked = false;
  }
  syncPeriodMode();

  const selected = String(preferences.target_distances || "")
    .split(",")
    .map((value) => Number(value.trim()))
    .filter((value) => Number.isFinite(value) && value > 0);
  renderDistancePicker(selected.length ? selected : null);
}

function syncControlsFromReportFilters(filters = {}) {
  applyPreferences({
    swim_mode: filters.swim_mode || swimModeEl.value,
    period: filters.period || periodEl.value,
    days: filters.days ?? (periodModeCustomEl.checked ? Number(daysEl.value || 180) : null),
    target_distances: Array.isArray(filters.target_distances) ? filters.target_distances.join(",") : getSelectedDistances().join(","),
    long_min_distance: filters.long_freestyle_min_distance_m ?? longMinDistanceEl.value,
  });
}

function validateReportFilters() {
  const selectedDistances = getSelectedDistances();
  if (!selectedDistances.length) {
    throw new Error("Выбери хотя бы одну дистанцию для отчёта.");
  }
  const longMinDistance = Number(longMinDistanceEl.value || "0");
  if (!Number.isFinite(longMinDistance) || longMinDistance < 0) {
    throw new Error("Порог длинных дистанций должен быть числом 0 или больше.");
  }
  return { selectedDistances, longMinDistance };
}

function buildQuery() {
  const { selectedDistances, longMinDistance } = validateReportFilters();
  const params = new URLSearchParams({
    swim_mode: swimModeEl.value,
    period: periodModeCustomEl.checked ? "all" : periodEl.value,
    distances: selectedDistances.join(","),
    long_min_distance: String(longMinDistance),
  });
  if (periodModeCustomEl.checked) {
    params.set("days", daysEl.value || "180");
  }
  return params;
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
  workoutsTableEl.innerHTML = rows.length
    ? rows.map((row) => `
      <tr>
        <td>${escapeHtml(row.date)}</td>
        <td>${escapeHtml(row.total_distance_m)} м</td>
        <td>${escapeHtml(row.total_time)}</td>
        <td>${escapeHtml(row.best_pace_100m)}</td>
      </tr>
    `).join("")
    : `<tr><td colspan="4">Тренировки не найдены.</td></tr>`;
}

function renderMeta(meta) {
  const timings = meta.timings || {};
  metaBoxEl.innerHTML = `
    <div><strong>Файлов в базе:</strong> ${escapeHtml(meta.db_total_files ?? meta.total_files ?? 0)}</div>
    <div><strong>Готовых:</strong> ${escapeHtml(meta.db_ready_files ?? meta.ready_files ?? 0)}</div>
    <div><strong>Дубликатов:</strong> ${escapeHtml(meta.db_duplicate_files ?? meta.duplicate_files ?? 0)}</div>
    <div><strong>Ошибок:</strong> ${escapeHtml(meta.db_error_files ?? meta.error_files ?? 0)}</div>
    <div><strong>Интервалов:</strong> ${escapeHtml(meta.db_total_rows ?? 0)}</div>
    <div><strong>Сгенерировано:</strong> ${escapeHtml(meta.generated_at || "—")}</div>
    <hr>
    <div><strong>Время операции:</strong> ${escapeHtml(timings.total || "—")}</div>
  `;
}

function renderMonthlyHistory(payload) {
  monthlyHistoryState = {
    headers: Array.isArray(payload.headers) ? payload.headers : [],
    years: Array.isArray(payload.years) ? payload.years : [],
    rows: Array.isArray(payload.rows) ? payload.rows : [],
  };
  const years = monthlyHistoryState.years;
  const selectedYear = years.includes(Number(monthlyYearSelectEl.value))
    ? Number(monthlyYearSelectEl.value)
    : (years[0] || new Date().getFullYear());
  monthlyYearSelectEl.innerHTML = years.map((year) => `
    <option value="${escapeHtml(year)}">${escapeHtml(year)}</option>
  `).join("");
  monthlyYearSelectEl.value = String(selectedYear);
  renderMonthlyHistoryYear(selectedYear);
}

function renderMonthlyHistoryYear(year) {
  const headers = monthlyHistoryState.headers;
  const rows = monthlyHistoryState.rows.filter((row) => Number(row.year) === Number(year));
  monthlyHeaderRowEl.innerHTML = `<th>Месяц</th>${headers.map((distance) => `<th>${escapeHtml(distance)} м</th>`).join("")}`;
  monthlyTableBodyEl.innerHTML = rows.length
    ? rows.map((row) => `
      <tr>
        <td>${escapeHtml(row.month)}</td>
        ${row.values.map((value) => `
          <td>
            <span class="pace-pill ${value.best ? "is-best" : ""}">
              ${escapeHtml(value.text || "—")}
            </span>
          </td>
        `).join("")}
      </tr>
    `).join("")
    : `<tr><td colspan="${headers.length + 1}">Пока нет monthly history.</td></tr>`;
}

function setRuntimeBanner(message = "", visible = false) {
  runtimeBannerEl.textContent = message;
  runtimeBannerEl.classList.toggle("is-hidden", !visible || !message);
}

async function refreshRuntimeStatus() {
  if (!sessionState?.authenticated) {
    return;
  }
  try {
    const payload = await api(`/api/runtime-status?_ts=${Date.now()}`);
    if (payload.monthly_processing) {
      setRuntimeBanner(payload.monthly_message || "Обработка данных...", true);
    } else {
      setRuntimeBanner(payload.monthly_message || "", Boolean(payload.monthly_message));
    }
  } catch (error) {
    // keep quiet
  }
}

async function loadReport() {
  loadButtonEl.disabled = true;
  try {
    const query = buildQuery();
    query.set("_ts", String(Date.now()));
    summaryExportEl.href = `/api/export/summary.xlsx?${query.toString()}`;
    workoutsExportEl.href = `/api/export/workouts.xlsx?${query.toString()}`;
    const payload = await api(`/api/report?${query.toString()}`);
    syncControlsFromReportFilters(payload.filters || {});
    sessionState = {
      ...(sessionState || {}),
      preferences: {
        swim_mode: payload.filters?.swim_mode || swimModeEl.value,
        period: payload.filters?.period || periodEl.value,
        days: payload.filters?.days ?? null,
        target_distances: Array.isArray(payload.filters?.target_distances) ? payload.filters.target_distances.join(",") : getSelectedDistances().join(","),
        long_min_distance: payload.filters?.long_freestyle_min_distance_m ?? longMinDistanceEl.value,
      },
    };
    setRuntimeBanner("", false);
    renderMetrics(payload.overview, payload.filters);
    renderSummary(payload.summary);
    renderWorkouts(payload.workouts);
    renderMeta(payload.dataset_meta);
  } catch (error) {
    setRuntimeBanner("Показан предыдущий успешный отчёт. Последнее обновление завершилось ошибкой.", true);
    alert(error.message);
  } finally {
    loadButtonEl.disabled = false;
  }
}

async function loadMonthlyHistory() {
  try {
    const payload = await api(`/api/monthly-history?_ts=${Date.now()}`);
    renderMonthlyHistory(payload);
  } catch (error) {
    monthlyTableBodyEl.innerHTML = `<tr><td colspan="10">${escapeHtml(error.message)}</td></tr>`;
  }
}

async function refreshSession() {
  sessionState = await api(`/api/auth/session?_ts=${Date.now()}`);
  if (!sessionState.authenticated) {
    showAuth();
    return false;
  }
  showApp();
  const account = sessionState.account;
  accountNameEl.textContent = `${account.first_name} ${account.last_name}`;
  accountEmailEl.textContent = account.email;
  accountRoleEl.textContent = account.role;
  adminLinkEl.hidden = !sessionState.is_admin;
  applyPreferences(sessionState.preferences || {});
  const hasData = Number(sessionState.dataset_meta?.total_files || 0) > 0;
  showDashboard(hasData);
  if (hasData) {
    await loadReport();
    await loadMonthlyHistory();
  } else {
    metricsEl.innerHTML = "";
    summaryTableEl.innerHTML = "";
    workoutsTableEl.innerHTML = "";
    monthlyHeaderRowEl.innerHTML = "";
    monthlyTableBodyEl.innerHTML = "";
    metaBoxEl.innerHTML = `
      <div><strong>Файлов в базе:</strong> 0</div>
      <div><strong>Статус:</strong> ждём первую загрузку FIT-файлов</div>
    `;
  }
  await refreshRuntimeStatus();
  return true;
}

async function login() {
  if (authRequestState.login) {
    return;
  }
  authRequestState.login = true;
  loginButtonEl.disabled = true;
  setFormStatus(loginStatusEl, "Выполняем вход...");
  try {
    await api("/api/auth/login", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        email: loginEmailEl.value.trim(),
        password: loginPasswordEl.value,
      }),
    });
    setFormStatus(loginStatusEl);
    await refreshSession();
  } catch (error) {
    setFormStatus(loginStatusEl, error.message, "error");
    throw error;
  } finally {
    authRequestState.login = false;
    loginButtonEl.disabled = false;
  }
}

async function register() {
  if (authRequestState.register) {
    return;
  }
  authRequestState.register = true;
  registerButtonEl.disabled = true;
  const email = registerEmailEl.value.trim();
  const password = registerPasswordEl.value;
  setFormStatus(registerStatusEl, "Создаём учётную запись...");
  try {
    await api("/api/auth/register", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        first_name: registerFirstNameEl.value.trim(),
        last_name: registerLastNameEl.value.trim(),
        email,
        password,
      }),
    });
    setFormStatus(registerStatusEl, "Учётная запись создана. Выполняем вход...", "success");
    try {
      await api("/api/auth/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          email,
          password,
        }),
      });
      registerFirstNameEl.value = "";
      registerLastNameEl.value = "";
      registerEmailEl.value = "";
      registerPasswordEl.value = "";
      setFormStatus(registerStatusEl);
      await refreshSession();
    } catch (loginError) {
      loginEmailEl.value = email;
      loginPasswordEl.value = "";
      showAuthForm("login");
      setFormStatus(loginStatusEl, "Учётная запись создана. Теперь войди с этим e-mail и паролем.", "success");
      throw loginError;
    }
  } catch (error) {
    setFormStatus(registerStatusEl, error.message, "error");
    throw error;
  } finally {
    authRequestState.register = false;
    registerButtonEl.disabled = false;
  }
}

async function logout() {
  await fetch("/api/auth/logout", { method: "POST", credentials: "same-origin" });
  sessionState = null;
  showAuth();
}

async function uploadFiles() {
  const files = getPendingUploadFiles();
  if (!files.length) {
    alert("Выбери хотя бы один FIT-файл или папку с FIT-файлами.");
    return;
  }
  uploadButtonEl.disabled = true;
  uploadButtonEmptyEl.disabled = true;
  try {
    const batches = buildUploadBatches(files);
    let processed = 0;
    let skipped = 0;
    let duplicates = 0;
    let errors = 0;
    for (let index = 0; index < batches.length; index += 1) {
      setUploadHint(`Загрузка пакета ${index + 1} из ${batches.length} (${batches[index].length} файлов)...`);
      const formData = new FormData();
      batches[index].forEach((file) => formData.append("files", file, file.webkitRelativePath || file.name));
      const payload = await api("/api/upload", {
        method: "POST",
        body: formData,
      });
      processed += Number(payload.meta.processed_files || 0);
      skipped += Number(payload.meta.skipped_files || 0);
      duplicates += Number(payload.meta.duplicate_files || 0);
      errors += Number(payload.meta.error_files || 0);
    }
    alert(`Импорт завершён. Обработано: ${processed}, пропущено: ${skipped}, дубликаты: ${duplicates}, ошибки: ${errors}`);
    clearUploadInputs();
    setUploadHint("Старые файлы будут проигнорированы по hash, новые тренировки попадут в базу пользователя. Можно загружать и папкой.");
    showDashboard(true);
    await loadReport();
    await loadMonthlyHistory();
  } catch (error) {
    alert(error.message);
    setUploadHint(error.message);
  } finally {
    uploadButtonEl.disabled = false;
    uploadButtonEmptyEl.disabled = false;
  }
}

function getPendingUploadFiles() {
  const sources = [
    ...Array.from(uploadInputEl.files || []),
    ...Array.from(uploadFolderInputEl.files || []),
    ...Array.from(uploadInputEmptyEl.files || []),
    ...Array.from(uploadFolderInputEmptyEl.files || []),
  ];
  return sources.filter((file) => String(file.name || "").toLowerCase().endsWith(".fit"));
}

function buildUploadBatches(files) {
  const batches = [];
  let current = [];
  let currentBytes = 0;
  for (const file of files) {
    const fileSize = Number(file.size || 0);
    const shouldSplit = current.length >= MAX_FILES_PER_BATCH || (current.length > 0 && currentBytes + fileSize > MAX_BATCH_BYTES);
    if (shouldSplit) {
      batches.push(current);
      current = [];
      currentBytes = 0;
    }
    current.push(file);
    currentBytes += fileSize;
  }
  if (current.length) {
    batches.push(current);
  }
  return batches;
}

function clearUploadInputs() {
  uploadInputEl.value = "";
  uploadFolderInputEl.value = "";
  uploadInputEmptyEl.value = "";
  uploadFolderInputEmptyEl.value = "";
  updateUploadSelection();
}

function setUploadHint(text) {
  uploadHintEl.textContent = text;
  uploadHintEmptyEl.textContent = text;
}

function updateUploadSelection() {
  const files = getPendingUploadFiles();
  const fitCount = files.length;
  const totalBytes = files.reduce((sum, file) => sum + Number(file.size || 0), 0);
  const summary = fitCount
    ? `Выбрано FIT-файлов: ${fitCount}, объём: ${(totalBytes / (1024 * 1024)).toFixed(1)} MB`
    : "Файлы не выбраны";
  uploadSelectionEl.textContent = summary;
  uploadSelectionEmptyEl.textContent = summary;
}

function resetOtherInputs(activeInput) {
  const allInputs = [uploadInputEl, uploadFolderInputEl, uploadInputEmptyEl, uploadFolderInputEmptyEl];
  allInputs.forEach((input) => {
    if (input !== activeInput) {
      input.value = "";
    }
  });
}

periodModePresetEl.addEventListener("change", syncPeriodMode);
periodModeCustomEl.addEventListener("change", syncPeriodMode);
longMinDistanceEl.addEventListener("change", () => renderDistancePicker());
loadButtonEl.addEventListener("click", () => loadReport());
loginButtonEl.addEventListener("click", () => login().catch(() => {}));
registerButtonEl.addEventListener("click", () => register().catch(() => {}));
logoutButtonEl.addEventListener("click", () => logout().catch((error) => alert(error.message)));
uploadButtonEl.addEventListener("click", () => uploadFiles());
uploadButtonEmptyEl.addEventListener("click", () => uploadFiles());
pickFilesButtonEl.addEventListener("click", () => uploadInputEl.click());
pickFolderButtonEl.addEventListener("click", () => uploadFolderInputEl.click());
pickFilesButtonEmptyEl.addEventListener("click", () => uploadInputEmptyEl.click());
pickFolderButtonEmptyEl.addEventListener("click", () => uploadFolderInputEmptyEl.click());
uploadInputEl.addEventListener("change", () => { resetOtherInputs(uploadInputEl); updateUploadSelection(); });
uploadFolderInputEl.addEventListener("change", () => { resetOtherInputs(uploadFolderInputEl); updateUploadSelection(); });
uploadInputEmptyEl.addEventListener("change", () => { resetOtherInputs(uploadInputEmptyEl); updateUploadSelection(); });
uploadFolderInputEmptyEl.addEventListener("change", () => { resetOtherInputs(uploadFolderInputEmptyEl); updateUploadSelection(); });
showLoginButtonEl.addEventListener("click", () => showAuthForm("login"));
showRegisterButtonEl.addEventListener("click", () => showAuthForm("register"));
backFromLoginButtonEl.addEventListener("click", () => showAuth());
backFromRegisterButtonEl.addEventListener("click", () => showAuth());

syncPeriodMode();
renderDistancePicker();
updateUploadSelection();
monthlyExportEl.href = "/api/export/monthly-history.xlsx";
monthlyYearSelectEl.addEventListener("change", () => renderMonthlyHistoryYear(Number(monthlyYearSelectEl.value)));
runtimeStatusTimer = window.setInterval(refreshRuntimeStatus, 3000);

(async () => {
  try {
    const restored = await refreshSession();
    if (!restored) {
      showAuth();
    }
  } catch (error) {
    showAuth();
  }
})();
