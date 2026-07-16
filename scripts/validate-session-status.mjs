/**
 * Valida estados live de whatsmeow-api sin enviar mensajes reales.
 * Clasifica sesiones: usable (connected+has_session+phone) vs fantasma (connected sin device).
 */
const API = process.env.WHATSMEOW_API_URL || "https://whatsmeow-api-production.up.railway.app";
const KEY = process.env.WHATSMEOW_API_KEY || "remaxnoa2024secret";

async function api(path, options = {}) {
  const res = await fetch(`${API}${path}`, {
    ...options,
    headers: {
      "X-API-Key": KEY,
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
  });
  const data = await res.json().catch(() => ({}));
  return { ok: res.ok, status: res.status, data };
}

function classify(s) {
  const status = String(s.status || "").toLowerCase();
  const hasSession = Boolean(s.has_session);
  const phone = String(s.phone || "");
  if (status === "connected" && hasSession && phone) return "usable";
  if (status === "connected" && !hasSession) return "ghost";
  if (status === "logged_out" || status === "need_scan") return "unusable";
  if (hasSession) return "has_device_offline";
  return "other";
}

async function main() {
  console.log("→ GET /api/status");
  const all = await api("/api/status");
  if (!all.ok || !all.data?.success) {
    console.error("FAIL status", all.status, all.data);
    process.exit(1);
  }

  const sessions = all.data?.data?.sessions || [];
  const buckets = { usable: [], ghost: [], unusable: [], has_device_offline: [], other: [] };
  for (const s of sessions) {
    buckets[classify(s)].push(s.agent_code);
  }

  console.log(`total=${sessions.length}`);
  for (const [k, list] of Object.entries(buckets)) {
    console.log(`${k}: ${list.length}${list.length && list.length <= 8 ? ` → ${list.join(", ")}` : ""}`);
  }

  const sampleUsable = buckets.usable[0];
  const sampleGhost = buckets.ghost[0];
  const sampleUnusable = buckets.unusable[0];

  if (sampleUsable) {
    const one = await api(`/api/status?agent_code=${encodeURIComponent(sampleUsable)}`);
    const d = one.data?.data || {};
    const ok = d.status === "connected" && d.has_session && d.phone;
    console.log(`usable check ${sampleUsable}:`, ok ? "OK" : "FAIL", d);
    if (!ok) process.exit(1);
  }

  // Envío a sesión fantasma/unusable debe fallar limpio (sin crash)
  const probe = sampleGhost || sampleUnusable;
  if (probe) {
    console.log(`→ POST /api/messages/send probe agent=${probe}`);
    const send = await api("/api/messages/send", {
      method: "POST",
      body: JSON.stringify({
        agent_code: probe,
        phone: "5491111111111",
        message: "probe-no-enviar-validacion",
      }),
    });
    const msg = send.data?.message || send.data?.error || "";
    const blocked =
      !send.data?.success &&
      (String(msg).toLowerCase().includes("not connected") ||
        String(msg).toLowerCase().includes("device jid") ||
        String(msg).toLowerCase().includes("not found") ||
        String(msg).toLowerCase().includes("logged"));
    console.log(`probe send blocked=${blocked} status=${send.status} msg=${msg}`);
    if (send.data?.success) {
      console.error("FAIL: probe no debería enviar en sesión no usable");
      process.exit(1);
    }
  }

  // Endpoint send-media debe existir (404 ok si aún no deploy; tras push = 400/401/etc)
  console.log("→ OPTIONS-like probe send-media route");
  const media = await api("/api/messages/send-media", {
    method: "POST",
    body: JSON.stringify({ agent_code: sampleUsable || "x" }),
  });
  console.log(`send-media HTTP ${media.status} success=${media.data?.success} msg=${media.data?.message || ""}`);

  console.log("DONE");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
