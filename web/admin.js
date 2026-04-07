const adminAuthViewEl = document.getElementById("adminAuthView");
const adminAppViewEl = document.getElementById("adminAppView");
const adminLoginStatusEl = document.getElementById("adminLoginStatus");
const adminLoginEmailEl = document.getElementById("adminLoginEmail");
const adminLoginPasswordEl = document.getElementById("adminLoginPassword");
const adminLoginButtonEl = document.getElementById("adminLoginButton");
const adminLogoutButtonEl = document.getElementById("adminLogoutButton");
const adminAccountNameEl = document.getElementById("adminAccountName");
const adminAccountEmailEl = document.getElementById("adminAccountEmail");
const adminOnlyMessageEl = document.getElementById("adminOnlyMessage");
const adminMetricsEl = document.getElementById("adminMetrics");
const adminRolesTableEl = document.querySelector("#adminRolesTable tbody");
const adminUsersTableEl = document.querySelector("#adminUsersTable tbody");
const adminLoginsTableEl = document.querySelector("#adminLoginsTable tbody");
const adminUploadsTableEl = document.querySelector("#adminUploadsTable tbody");
const adminAuditTableEl = document.querySelector("#adminAuditTable tbody");

let adminLoginInFlight = false;
const ADMIN_SESSION_HEARTBEAT_MS = 60 * 1000;
const BROWSER_SESSION_DAY_KEY = "garmin_browser_session_day";
let adminSessionState = null;

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function setStatus(message = "", kind = "") {
  adminLoginStatusEl.textContent = message;
  adminLoginStatusEl.hidden = !message;
  adminLoginStatusEl.classList.remove("is-error", "is-success");
  if (kind) {
    adminLoginStatusEl.classList.add(kind === "error" ? "is-error" : "is-success");
  }
}

function showAuth() {
  adminAuthViewEl.hidden = false;
  adminAppViewEl.hidden = true;
}

function showAdminApp(account) {
  adminAuthViewEl.hidden = true;
  adminAppViewEl.hidden = false;
  adminAccountNameEl.textContent = `${account.first_name} ${account.last_name}`;
  adminAccountEmailEl.textContent = account.email;
}

function currentCalendarDay() {
  return new Date().toLocaleDateString("sv-SE");
}

function getBrowserSessionDay() {
  return window.sessionStorage.getItem(BROWSER_SESSION_DAY_KEY) || "";
}

function markBrowserSessionActive() {
  window.sessionStorage.setItem(BROWSER_SESSION_DAY_KEY, currentCalendarDay());
}

function clearBrowserSessionMarker() {
  window.sessionStorage.removeItem(BROWSER_SESSION_DAY_KEY);
}

function shouldForceReloginOnRestore(session) {
  if (getBrowserSessionDay()) {
    return false;
  }
  const loginDay = String(session?.session?.login_day || "");
  return Boolean(loginDay) && loginDay !== currentCalendarDay();
}

function setAdminOnlyMessage(message = "", visible = false) {
  adminOnlyMessageEl.textContent = message;
  adminOnlyMessageEl.classList.toggle("is-hidden", !visible || !message);
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
    const error = new Error((payload && payload.error) || fallbackText || `Ошибка запроса (${response.status})`);
    error.status = response.status;
    throw error;
  }
  return payload;
}

function renderAdminRoles(users) {
  if (!users.length) {
    adminRolesTableEl.innerHTML = `<tr><td colspan="6">Пользователи не найдены.</td></tr>`;
    return;
  }
  adminRolesTableEl.innerHTML = users.map((user) => `
    <tr>
      <td>${escapeHtml(user.id)}</td>
      <td>${escapeHtml(`${user.first_name} ${user.last_name}`)}<br><span class="muted">${escapeHtml(user.email)}</span></td>
      <td>${escapeHtml(user.role)}</td>
      <td>${Number(user.is_active) ? "active" : "disabled"}</td>
      <td>${escapeHtml(user.last_login_at || "—")}</td>
      <td>
        <div class="admin-actions">
          <button class="small-button admin-action-button" data-action="toggle-role" data-id="${escapeHtml(user.id)}" data-role="${escapeHtml(user.role)}">
            ${user.role === "admin" ? "Сделать user" : "Сделать admin"}
          </button>
          <button class="small-button secondary-button admin-action-button" data-action="toggle-active" data-id="${escapeHtml(user.id)}" data-active="${Number(user.is_active) ? "1" : "0"}">
            ${Number(user.is_active) ? "Отключить" : "Включить"}
          </button>
          ${Number(user.id) === Number(adminSessionState?.account?.id)
            ? ""
            : `<button class="small-button secondary-button admin-action-button" data-action="delete-user" data-id="${escapeHtml(user.id)}" data-email="${escapeHtml(user.email)}">Удалить</button>`}
        </div>
      </td>
    </tr>
  `).join("");
}

function renderAdminUsers(users) {
  if (!users.length) {
    adminUsersTableEl.innerHTML = `<tr><td colspan="7">Пользовательские данные пока недоступны.</td></tr>`;
    return;
  }
  adminUsersTableEl.innerHTML = users.map((user) => `
    <tr>
      <td>${escapeHtml(user.id)}</td>
      <td>${escapeHtml(`${user.first_name} ${user.last_name}`)}<br><span class="muted">${escapeHtml(user.email)}</span></td>
      <td>${escapeHtml(user.files_count)}</td>
      <td>${escapeHtml(user.activities_count)}</td>
      <td>${escapeHtml(user.intervals_count)}</td>
      <td>${escapeHtml(user.last_activity_date || "—")}</td>
      <td>${escapeHtml(user.period || "—")}<br><span class="muted">${escapeHtml(user.target_distances || "—")}</span></td>
    </tr>
  `).join("");
}

function renderAdminOverview(payload) {
  const overview = payload.overview || {};
  const metrics = [
    ["Пользователи", overview.total_users || 0, `${overview.active_users || 0} активных`],
    ["Файлы", overview.total_files || 0, "всего загружено"],
    ["Тренировки", overview.total_activities || 0, "activities в БД"],
    ["Интервалы", overview.total_intervals || 0, "intervals в БД"],
  ];
  adminMetricsEl.innerHTML = metrics.map(([label, value, note]) => `
    <article class="metric">
      <div class="metric-label">${escapeHtml(label)}</div>
      <div class="metric-value">${escapeHtml(value)}</div>
      <div class="metric-note">${escapeHtml(note)}</div>
    </article>
  `).join("");

  const recentLogins = Array.isArray(payload.recent_logins) ? payload.recent_logins : [];
  adminLoginsTableEl.innerHTML = recentLogins.length
    ? recentLogins.map((entry) => `
      <tr>
        <td>${escapeHtml(`${entry.first_name} ${entry.last_name}`)}<br><span class="muted">${escapeHtml(entry.email)}</span></td>
        <td>${escapeHtml(entry.role)}</td>
        <td>${escapeHtml(entry.last_login_at || "—")}</td>
      </tr>
    `).join("")
    : `<tr><td colspan="3">Пока нет входов.</td></tr>`;

  const recentUploads = Array.isArray(payload.recent_uploads) ? payload.recent_uploads : [];
  adminUploadsTableEl.innerHTML = recentUploads.length
    ? recentUploads.map((entry) => `
      <tr>
        <td>${escapeHtml(`${entry.first_name} ${entry.last_name}`)}<br><span class="muted">${escapeHtml(entry.email)}</span></td>
        <td>${escapeHtml(entry.original_file_name || entry.file_name || "—")}</td>
        <td>${escapeHtml(entry.parse_status || "—")}</td>
        <td>${escapeHtml(entry.uploaded_at || "—")}</td>
      </tr>
    `).join("")
    : `<tr><td colspan="4">Пока нет загрузок.</td></tr>`;

  const recentAudit = Array.isArray(payload.recent_audit) ? payload.recent_audit : [];
  adminAuditTableEl.innerHTML = recentAudit.length
    ? recentAudit.map((entry) => `
      <tr>
        <td>${escapeHtml(entry.created_at || "—")}</td>
        <td>${escapeHtml(entry.event_type || "—")}</td>
        <td>${escapeHtml(entry.actor_email || "system")}</td>
        <td>${escapeHtml(entry.target_email || "—")}</td>
      </tr>
    `).join("")
    : `<tr><td colspan="4">Журнал пока пуст.</td></tr>`;
}

async function loadAdminUsers() {
  const payload = await api(`/api/admin/users?_ts=${Date.now()}`);
  const users = payload.users || [];
  renderAdminRoles(users);
  renderAdminUsers(users);
}

async function loadAdminOverview() {
  const payload = await api(`/api/admin/overview?_ts=${Date.now()}`);
  renderAdminOverview(payload);
}

async function refreshAdminSession() {
  const session = await api(`/api/auth/session?_ts=${Date.now()}`);
  adminSessionState = session;
  if (!session.authenticated) {
    clearBrowserSessionMarker();
    showAuth();
    return false;
  }
  if (shouldForceReloginOnRestore(session)) {
    await logoutAdmin({
      reason: "С прошлого входа сменился календарный день. Войди снова.",
      suppressNetworkErrors: true,
    });
    return false;
  }
  markBrowserSessionActive();
  if (!session.is_admin) {
    showAuth();
    setAdminOnlyMessage("Для этого раздела нужны права администратора.", true);
    return false;
  }
  setAdminOnlyMessage("", false);
  showAdminApp(session.account);
  adminRolesTableEl.innerHTML = `<tr><td colspan="6">Загрузка пользователей...</td></tr>`;
  adminUsersTableEl.innerHTML = `<tr><td colspan="7">Загрузка пользовательских данных...</td></tr>`;
  adminMetricsEl.innerHTML = "";
  adminLoginsTableEl.innerHTML = `<tr><td colspan="3">Загрузка...</td></tr>`;
  adminUploadsTableEl.innerHTML = `<tr><td colspan="4">Загрузка...</td></tr>`;
  adminAuditTableEl.innerHTML = `<tr><td colspan="4">Загрузка...</td></tr>`;
  loadAdminUsers().catch((error) => {
    setAdminOnlyMessage(`Не удалось загрузить список пользователей: ${error.message}`, true);
  });
  loadAdminOverview().catch((error) => {
    setAdminOnlyMessage(`Не удалось загрузить обзор админки: ${error.message}`, true);
  });
  return true;
}

async function heartbeatAdminSession() {
  if (adminAppViewEl.hidden) {
    return;
  }
  try {
    const session = await api(`/api/auth/session?_ts=${Date.now()}`);
    if (!session.authenticated) {
      throw Object.assign(new Error("Требуется вход"), { status: 401 });
    }
    adminSessionState = session;
    markBrowserSessionActive();
  } catch (error) {
    if (error?.status === 401) {
      clearBrowserSessionMarker();
      adminSessionState = null;
      showAuth();
      setStatus("Сессия завершилась. Войди снова.", "error");
    }
  }
}

async function loginAdmin() {
  if (adminLoginInFlight) {
    return;
  }
  adminLoginInFlight = true;
  adminLoginButtonEl.disabled = true;
  setStatus("Выполняем вход...");
  try {
    await api("/api/auth/login", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        email: adminLoginEmailEl.value.trim(),
        password: adminLoginPasswordEl.value,
      }),
    });
    setStatus("", "");
    await refreshAdminSession();
  } catch (error) {
    setStatus(error.message, "error");
  } finally {
    adminLoginInFlight = false;
    adminLoginButtonEl.disabled = false;
  }
}

async function logoutAdmin(options = {}) {
  const { reason = "", suppressNetworkErrors = false } = options;
  try {
    await fetch("/api/auth/logout", { method: "POST", credentials: "same-origin" });
  } catch (error) {
    if (!suppressNetworkErrors) {
      throw error;
    }
  }
  clearBrowserSessionMarker();
  adminSessionState = null;
  showAuth();
  if (reason) {
    setStatus(reason, "error");
  }
}

async function handleAdminAction(event) {
  const button = event.target.closest("button[data-action]");
  if (!button) {
    return;
  }
  const accountId = Number(button.dataset.id);
  const action = button.dataset.action;
  let role = button.dataset.role || "user";
  let isActive = button.dataset.active === "1";
  if (action === "toggle-role") {
    role = role === "admin" ? "user" : "admin";
  }
  if (action === "toggle-active") {
    isActive = !isActive;
    const row = button.closest("tr");
    const roleButton = row.querySelector('[data-action="toggle-role"]');
    role = roleButton ? roleButton.dataset.role : "user";
  }
  if (action === "delete-user") {
    const email = button.dataset.email || `ID ${accountId}`;
    const confirmed = window.confirm(`Точно удалить пользователя ${email} и все его данные из базы?`);
    if (!confirmed) {
      return;
    }
    await api("/api/admin/users", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        account_id: accountId,
        action: "delete",
      }),
    });
    await Promise.all([loadAdminUsers(), loadAdminOverview()]);
    return;
  }
  await api("/api/admin/users", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      account_id: accountId,
      role,
      is_active: isActive,
    }),
  });
  await Promise.all([loadAdminUsers(), loadAdminOverview()]);
}

adminLoginButtonEl.addEventListener("click", () => loginAdmin());
adminLogoutButtonEl.addEventListener("click", () => logoutAdmin().catch((error) => setStatus(error.message, "error")));
adminRolesTableEl.addEventListener("click", (event) => {
  handleAdminAction(event).catch((error) => setStatus(error.message, "error"));
});
window.setInterval(heartbeatAdminSession, ADMIN_SESSION_HEARTBEAT_MS);

(async () => {
  try {
    const restored = await refreshAdminSession();
    if (!restored) {
      showAuth();
    }
  } catch (error) {
    showAuth();
  }
})();
