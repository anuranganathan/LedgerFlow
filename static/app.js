"use strict";
// LedgerFlow dashboard. The access token lives only in memory; the refresh token is an
// HttpOnly cookie the browser sends to /auth/refresh, so a reload keeps you logged in.

const $ = (id) => document.getElementById(id);
const DEMO_KEY = "ledgerflow-demo";
let accessToken = null;
let me = null;
let merchants = [];
let openPaymentId = null;
let paymentKey = crypto.randomUUID(); // Idempotency-Key for the payment being entered
let pollTimer = null;

function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, (c) => ({"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"}[c]));
}

function say(text, isError = false) {
  $("message").textContent = text;
  $("message").classList.toggle("error", isError);
}

function errorText(body, fallback) {
  if (Array.isArray(body.detail)) return body.detail.map((d) => d.msg).join("; ");
  return body.detail || fallback;
}

async function refreshSession() {
  const response = await fetch("/auth/refresh", {method: "POST"});
  if (!response.ok) { accessToken = null; return false; }
  accessToken = (await response.json()).access_token;
  return true;
}

// Calls the API with the access token, refreshing it once if it has expired.
async function api(path, {method = "GET", body, headers = {}, retry = true} = {}) {
  const response = await fetch(path, {
    method,
    headers: {"Content-Type": "application/json", ...(accessToken ? {Authorization: `Bearer ${accessToken}`} : {}), ...headers},
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (response.status === 401 && retry && accessToken && await refreshSession()) {
    return api(path, {method, body, headers, retry: false});
  }
  if (response.status === 401 && accessToken) { signedOut(); throw new Error("Your session expired. Please log in again."); }
  if (response.status === 204) return null;
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(errorText(data, response.statusText));
  return data;
}

async function login(email, password) {
  const response = await fetch("/auth/login", {
    method: "POST",
    headers: {"Content-Type": "application/x-www-form-urlencoded"},
    body: new URLSearchParams({username: email, password}),
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(errorText(data, "Login failed"));
  accessToken = data.access_token;
  await signedIn();
}

async function register(name, email, password, role) {
  await api("/auth/register", {method: "POST", body: {name, email, password, role}});
}

function randomPassword() {
  const bytes = crypto.getRandomValues(new Uint8Array(18));
  return btoa(String.fromCharCode(...bytes)).replace(/[+/=]/g, "x");
}

function demoLogins() {
  try { return JSON.parse(sessionStorage.getItem(DEMO_KEY)); } catch { return null; }
}

// ---------- Views ----------

function signedOut() {
  accessToken = null; me = null;
  clearTimeout(pollTimer);
  $("signed-in").hidden = true; $("signed-out").hidden = false;
  $("logout").hidden = true; $("switch-demo").hidden = true;
  $("whoami").textContent = "";
}

async function signedIn() {
  me = await api("/auth/me");
  $("signed-out").hidden = true; $("signed-in").hidden = false; $("logout").hidden = false;
  $("whoami").textContent = `${me.name} · ${me.role.toLowerCase()}`;
  const demo = demoLogins();
  const other = demo && (me.role === "CUSTOMER" ? demo.merchant : demo.customer);
  $("switch-demo").hidden = !(demo && [demo.customer.email, demo.merchant.email].includes(me.email));
  if (other) $("switch-demo").textContent = `Switch to ${me.role === "CUSTOMER" ? "merchant" : "customer"} view`;
  $("pay-card").hidden = me.role !== "CUSTOMER";
  $("top-up-form").hidden = me.role !== "CUSTOMER";
  $("reconciliation-card").hidden = me.role !== "ADMIN";
  $("payments-title").firstChild.textContent = me.role === "MERCHANT" ? "Payments received " : "Payments ";
  $("details").innerHTML = ""; openPaymentId = null;
  if (me.role === "CUSTOMER") {
    merchants = await api("/merchants?limit=100");
    $("merchant").innerHTML = merchants.map((m) => `<option value="${m.id}">${escapeHtml(m.name)}</option>`).join("")
      || `<option value="" disabled selected>No merchants yet</option>`;
  }
  if (me.role === "ADMIN") showReconciliation();
  await refresh();
}

function renderAccount() {
  const account = me.accounts[0];
  $("account").innerHTML = account
    ? `<div class="muted">${escapeHtml(account.name)} · ${account.account_type.toLowerCase()} · ${account.currency}</div>
       <div class="balance">₹${account.balance}</div>`
    : `<div class="muted">${me.role === "ADMIN" ? "Admins see every account's payments below." : "No account."}</div>`;
}

async function refresh() {
  if (!me) return;
  clearTimeout(pollTimer);
  try {
    me = await api("/auth/me");
    renderAccount();
    const payments = await api("/payments?limit=20");
    $("payments").innerHTML = payments.length ? payments.map((p) => `
      <tr class="clickable" data-id="${p.id}">
        <td class="mono">${p.id.slice(0, 8)}</td>
        <td>${escapeHtml(p.description)}</td>
        <td class="num">₹${p.amount}</td>
        <td class="num">${Number(p.refunded_amount) ? "₹" + p.refunded_amount : ""}</td>
        <td><span class="badge ${p.status}">${p.status}</span></td>
        <td>${escapeHtml(p.failure_reason)}</td>
        <td class="muted">${new Date(p.created_at).toLocaleTimeString()}</td>
      </tr>`).join("") : `<tr><td colspan="7" class="empty">No payments yet.</td></tr>`;
    if (openPaymentId) await showDetails(openPaymentId);
    // Poll quickly while something is still being processed, slowly otherwise.
    const busy = payments.some((p) => p.status === "PENDING") || $("details").querySelector(".badge.PENDING");
    pollTimer = setTimeout(refresh, busy ? 1500 : 15000);
  } catch (error) {
    if (me) { say(error.message, true); pollTimer = setTimeout(refresh, 15000); }
  }
}

async function showDetails(paymentId) {
  openPaymentId = paymentId;
  const [payment, ledger, refunds] = await Promise.all([
    api(`/payments/${paymentId}`), api(`/payments/${paymentId}/ledger`), api(`/payments/${paymentId}/refunds`),
  ]);
  const receipt = payment.receipt_s3_key ? await api(`/payments/${paymentId}/receipt`).catch(() => null) : null;
  const mine = new Set(me.accounts.map((a) => a.id));
  const accountLabel = (id) => mine.has(id) ? "You" : id === payment.merchant_account_id ? "Merchant" : "Customer";
  const refundable = Number(payment.amount) - refunds.filter((r) => r.status !== "FAILED").reduce((sum, r) => sum + Number(r.amount), 0);
  const canRefund = me.role !== "CUSTOMER" && payment.status === "SUCCESS" && refundable > 0;
  $("details").innerHTML = `
    <h2 style="margin-top:20px">Payment <span class="mono">${paymentId}</span> <span class="badge ${payment.status}">${payment.status}</span></h2>
    ${ledger.length ? `<table>
      <thead><tr><th>Ledger entry</th><th>Account</th><th>For</th><th class="num">Amount</th></tr></thead>
      <tbody>${ledger.map((e) => `<tr><td>${e.entry_type}</td><td>${accountLabel(e.account_id)}</td><td>${e.refund_id ? "Refund" : "Payment"}</td><td class="num">₹${e.amount}</td></tr>`).join("")}</tbody>
    </table>` : `<div class="empty">No ledger entries: ${payment.status === "PENDING" ? "the payment is still being processed" : "the payment failed, so no money moved"}.</div>`}
    ${refunds.length ? `<div class="muted" style="margin-top:12px">Refunds</div><table><tbody>${refunds.map((r) => `<tr><td class="mono">${r.id.slice(0, 8)}</td><td class="num">₹${r.amount}</td><td><span class="badge ${r.status}">${r.status}</span></td><td>${escapeHtml(r.failure_reason || r.reason)}</td></tr>`).join("")}</tbody></table>` : ""}
    ${canRefund ? `<form id="refund-form" class="row" style="margin-top:12px">
      <input id="refund-amount" type="number" min="0.01" max="${refundable.toFixed(2)}" step="0.01" value="${refundable.toFixed(2)}" aria-label="Refund amount">
      <button type="submit" class="secondary">Refund</button></form>` : ""}
    ${receipt ? `<div class="muted" style="margin-top:12px">Receipt stored in S3:</div><pre>${escapeHtml(JSON.stringify(receipt, null, 2))}</pre>` : ""}`;
  if (canRefund) $("refund-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    try {
      await api(`/payments/${paymentId}/refunds`, {
        method: "POST", body: {amount: $("refund-amount").value, reason: "Refund from dashboard"},
        headers: {"Idempotency-Key": crypto.randomUUID()},
      });
      say("Refund accepted. The worker is processing it...");
      refresh();
    } catch (error) { say(`Refund rejected: ${error.message}`, true); }
  });
}

async function showReconciliation() {
  try {
    const report = await api("/reconciliation");
    $("reconciliation").innerHTML = `<p class="${report.ok ? "ok-text" : "bad-text"}">${report.ok ? "✓ Books balance, nothing stuck" : "✗ Problems found"}</p>
      <pre>${escapeHtml(JSON.stringify(report, null, 2))}</pre>`;
  } catch (error) { $("reconciliation").textContent = error.message; }
}

// ---------- Events ----------

$("login-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  try { await login($("login-email").value, $("login-password").value); say(""); }
  catch (error) { say(error.message, true); }
});

$("register-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const email = $("register-email").value, password = $("register-password").value;
  try {
    await register($("register-name").value, email, password, $("register-role").value);
    await login(email, password);
    say("Account created.");
  } catch (error) { say(error.message, true); }
});

$("demo").addEventListener("click", async () => {
  say("Creating demo users...");
  try {
    const id = crypto.randomUUID().slice(0, 8);
    const demo = {
      customer: {email: `demo-customer-${id}@example.com`, password: randomPassword()},
      merchant: {email: `demo-merchant-${id}@example.com`, password: randomPassword()},
    };
    await register(`Demo Store ${id}`, demo.merchant.email, demo.merchant.password, "MERCHANT");
    await register(`Demo Customer ${id}`, demo.customer.email, demo.customer.password, "CUSTOMER");
    // Throwaway demo logins, kept for this tab only so you can switch between the two views.
    sessionStorage.setItem(DEMO_KEY, JSON.stringify(demo));
    await login(demo.customer.email, demo.customer.password);
    await api(`/accounts/${me.accounts[0].id}/fund`, {method: "POST", body: {amount: "5000.00", description: "Demo funds"}});
    $("merchant").value = merchants.find((m) => m.name === `Demo Store ${id}`)?.id ?? $("merchant").value;
    say("You're a demo customer with ₹5000. Pay the demo store, then switch to the merchant view to refund.");
    refresh();
  } catch (error) { say(error.message, true); }
});

$("switch-demo").addEventListener("click", async () => {
  const demo = demoLogins();
  const target = me.role === "CUSTOMER" ? demo.merchant : demo.customer;
  await api("/auth/logout", {method: "POST"});
  try { await login(target.email, target.password); say(""); } catch (error) { say(error.message, true); }
});

$("logout").addEventListener("click", async () => {
  await api("/auth/logout", {method: "POST"}).catch(() => null);
  sessionStorage.removeItem(DEMO_KEY);
  signedOut();
  say("Logged out.");
});

$("top-up-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  try {
    await api(`/accounts/${me.accounts[0].id}/fund`, {method: "POST", body: {amount: $("top-up-amount").value}});
    say("Money added.");
    refresh();
  } catch (error) { say(`Top-up rejected: ${error.message}`, true); }
});

$("payment-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const button = event.submitter; button.disabled = true;
  try {
    // The same key is reused if this exact payment is retried, so it can't be charged twice.
    const result = await api("/payments", {
      method: "POST", headers: {"Idempotency-Key": paymentKey},
      body: {customer_account_id: me.accounts[0].id, merchant_account_id: $("merchant").value,
             amount: $("amount").value, currency: "INR", description: $("description").value},
    });
    paymentKey = crypto.randomUUID();
    say(`Payment accepted (${result.status}). Processing through Kafka...`);
    openPaymentId = result.payment_id;
    refresh();
  } catch (error) {
    say(`Payment rejected: ${error.message}`, true);
  } finally { button.disabled = false; }
});
["merchant", "amount", "description"].forEach((id) => $(id).addEventListener("input", () => { paymentKey = crypto.randomUUID(); }));

$("payments").addEventListener("click", (event) => {
  const row = event.target.closest("tr[data-id]");
  if (row) showDetails(row.dataset.id).catch((error) => say(error.message, true));
});

$("run-reconciliation").addEventListener("click", showReconciliation);

// On load: resume the session from the refresh cookie, if there is one.
refreshSession().then((ok) => ok ? signedIn() : signedOut()).catch(() => signedOut());
