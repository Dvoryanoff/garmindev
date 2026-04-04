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
const adminUsersTableEl = document.querySelector("#adminUsersTable tbody");

let adminLoginInFlight = false;

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
    throw new Error((payload && payload.error) || fallbackText || `Ошибка запроса (${response.status})`);
  }
  return payload;
}

function renderAdminUsers(users) {
  adminUsersTableEl.innerHTML = users.map((user) => `
    <tr>
      <td>${escapeHtml(user.id)}</td>
      <td>${escapeHtml(`${user.first_name} ${user.last_name}`)}<br><span class="muted">${escapeHtml(user.email)}</span></td>
      <td>${escapeHtml(user.role)}</td>
      <td>${Number(user.is_active) ? "active" : "disabled"}</td>
      <td>${escapeHtml(user.files_count)}</td>
      <td>${escapeHtml(user.activities_count)}</td>
      <td>${escapeHtml(user.last_activity_date || "—")}</td>
      <td>
        <button class="small-button" data-action="toggle-role" data-id="${escapeHtml(user.id)}" data-role="${escapeHtml(user.role)}">
          ${user.role === "admin" ? "Сделать user" : "Сделать admin"}
        </button>
        <button class="small-button secondary-button" data-action="toggle-active" data-id="${escapeHtml(user.id)}" data-active="${Number(user.is_active) ? "1" : "0"}">
          ${Number(user.is_active) ? "Отключить" : "Включить"}
        </button>
      </td>
    </tr>
  `).join("");
}

async function loadAdminUsers() {
  const payload = await api(`/api/admin/users?_ts=${Date.now()}`);
  renderAdminUsers(payload.users || []);
}

async function refreshAdminSession() {
  const session = await api(`/api/auth/session?_ts=${Date.now()}`);
  if (!session.authenticated) {
    showAuth();
    return false;
  }
  if (!session.is_admin) {
    showAuth();
    setAdminOnlyMessage("Для этого раздела нужны права администратора.", true);
    return false;
  }
  setAdminOnlyMessage("", false);
  showAdminApp(session.account);
  await loadAdminUsers();
  return true;
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

async function logoutAdmin() {
  await fetch("/api/auth/logout", { method: "POST", credentials: "same-origin" });
  showAuth();
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
  await api("/api/admin/users", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      account_id: accountId,
      role,
      is_active: isActive,
    }),
  });
  await loadAdminUsers();
}

adminLoginButtonEl.addEventListener("click", () => loginAdmin());
adminLogoutButtonEl.addEventListener("click", () => logoutAdmin().catch((error) => setStatus(error.message, "error")));
adminUsersTableEl.addEventListener("click", (event) => {
  handleAdminAction(event).catch((error) => setStatus(error.message, "error"));
});

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
