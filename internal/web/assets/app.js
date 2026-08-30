// The scaffold's whole client: one snapshot shape, two transports.
// Server-sent events first; when the stream fails, degrade to polling
// and keep trying to climb back — the content never changes, only the
// cadence.
"use strict";

const POLL_EVERY = 5000; // ms between fetches while degraded
const SSE_RETRY = 30000; // ms between attempts to climb back

const $ = (id) => document.getElementById(id);

function uptimeWord(secs) {
  if (secs < 3600) return Math.floor(secs / 60) + "m";
  if (secs < 172800) {
    return Math.floor(secs / 3600) + "h " +
      String(Math.floor(secs / 60) % 60).padStart(2, "0") + "m";
  }
  return Math.floor(secs / 86400) + "d " + (Math.floor(secs / 3600) % 24) + "h";
}

function fillRows(tbody, rows) {
  tbody.replaceChildren(...rows.map((cells) => {
    const tr = document.createElement("tr");
    for (const c of cells) {
      const td = document.createElement("td");
      if (typeof c === "string") {
        td.textContent = c;
      } else {
        td.textContent = c.text;
        if (c.className) td.className = c.className;
      }
      tr.appendChild(td);
    }
    return tr;
  }));
}

function stateCell(state, cause) {
  const bad = state === "error" || state === "down" || state === "stillborn";
  return {
    text: cause ? state + " — " + cause : state,
    className: bad ? "bad" : (state === "disabled" ? "muted" : "good"),
  };
}

function render(s) {
  document.title = s.system + " · " + s.product;
  $("system").textContent = s.system;
  $("build").textContent =
    s.product + " " + s.version + (s.revision ? " (" + s.revision + ")" : "");
  $("uptime").textContent = uptimeWord(s.uptimeSecs);
  $("heard").textContent = s.framesHeard;
  $("sent").textContent = s.framesSent;
  fillRows($("relays").querySelector("tbody"), s.relays.map((r) => [
    r.name, stateCell(r.state, r.cause), r.radio, r.waveform || "",
  ]));
  fillRows($("observers").querySelector("tbody"), s.observers.map((o) => [
    o.name, stateCell(o.state, o.cause), o.url,
  ]));
  $("journal").textContent = !s.journal ? "" :
    s.journal.healthy
      ? "journal: healthy, " + s.journal.writes + " writes"
      : "journal: DEGRADED — " + (s.journal.lastError || "");
}

// --- the two transports ---------------------------------------------

let es = null;
let pollTimer = null;
let retryTimer = null;

function setMode(word) { $("mode").textContent = word; }

async function pollOnce() {
  try {
    const resp = await fetch("/api/status", { cache: "no-store" });
    if (resp.ok) render(await resp.json());
  } catch { /* the next tick retries; the page keeps its last truth */ }
}

function startPolling() {
  if (pollTimer) return;
  setMode("polling");
  pollOnce();
  pollTimer = setInterval(pollOnce, POLL_EVERY);
  // Keep trying to climb back to the stream.
  retryTimer = setInterval(startStream, SSE_RETRY);
}

function stopPolling() {
  clearInterval(pollTimer); pollTimer = null;
  clearInterval(retryTimer); retryTimer = null;
}

function startStream() {
  if (es) es.close();
  es = new EventSource("/events");
  es.addEventListener("status", (ev) => {
    stopPolling();
    setMode("live");
    render(JSON.parse(ev.data));
  });
  es.onerror = () => {
    // EventSource retries on its own for transient breaks; a page
    // that never got an event degrades so it shows SOMETHING.
    if (!pollTimer) startPolling();
  };
}

startStream();
