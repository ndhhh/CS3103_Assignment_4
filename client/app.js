/*
 * LiveRecall browser client.
 *
 * The client stores the newest accepted caption state by segment. Provisional
 * revisions update the same row until a final revision locks it in place.
 */

const WS_URL = "ws://127.0.0.1:8765";
const SESSION_ID = "L1";
const FINAL_RETENTION_SECONDS = 90;
const SERVER_HISTORY_SECONDS = 60;

const statusEl = document.getElementById("status");
const liveEl = document.getElementById("live");
const recallEl = document.getElementById("recall");

// Each value stores the latest caption fields plus the row used to display it.
const CAPTIONS_STATE = new Map();
let lastSegment = 0;
let lastRevision = 0;
let recallWindowSeconds = 30;

let reconnectTimer = null;
let reconnectAttempt = 0;
let ws;

function numericClientId() {
  const key = "liverecall-client-id";
  let value = sessionStorage.getItem(key);
  if (!value) {
    value = String(Math.floor(100000 + Math.random() * 900000));
    sessionStorage.setItem(key, value);
  }
  return Number(value);
}

function createCaptionRow(segment) {
  const row = document.createElement("div");
  row.className = "caption-line provisional";
  row.dataset.segment = String(segment);

  // Insert by segment number. Existing rows keep their relative order, even if
  // a previously unseen segment arrives after a later one.
  const nextRow = [...liveEl.children].find(
    (child) => Number(child.dataset.segment) > segment,
  );
  liveEl.insertBefore(row, nextRow ?? null);
  return row;
}

function renderCaption(caption) {
  const label = caption.final ? "FINAL" : "PROVISIONAL";
  caption.row.className = `caption-line ${caption.final ? "final" : "provisional"}`;
  caption.row.textContent =
    `[${label}] segment ${caption.segment} rev ${caption.revision}: ${caption.text}`;
  liveEl.scrollTop = liveEl.scrollHeight;
}

function upsertCaption(msg) {
  if (
    !msg ||
    !Number.isInteger(msg.segment) ||
    msg.segment < 1 ||
    !Number.isInteger(msg.revision) ||
    msg.revision < 1 ||
    typeof msg.text !== "string" ||
    typeof msg.final !== "boolean" ||
    !Number.isFinite(msg.source_ts)
  ) {
    return false;
  }

  const existing = CAPTIONS_STATE.get(msg.segment);
  if (existing?.final || (existing && msg.revision <= existing.revision)) {
    return false;
  }

  // The cursor lets us ignore old segments even after their rows expire.
  if (!existing && msg.segment <= lastSegment) {
    return false;
  }

  const caption = {
    ...msg,
    row: existing?.row ?? createCaptionRow(msg.segment),
  };
  CAPTIONS_STATE.set(msg.segment, caption);
  renderCaption(caption);

  // Resume state describes finalized captions only, never provisional ones.
  if (
    caption.final &&
    (caption.segment > lastSegment ||
      (caption.segment === lastSegment && caption.revision > lastRevision))
  ) {
    lastSegment = caption.segment;
    lastRevision = caption.revision;
  }

  return true;
}

function pruneExpiredFinals(nowSeconds = Date.now() / 1000) {
  const cutoff = nowSeconds - FINAL_RETENTION_SECONDS;
  for (const [segment, caption] of CAPTIONS_STATE) {
    if (caption.final && caption.source_ts < cutoff) {
      caption.row.remove();
      CAPTIONS_STATE.delete(segment);
    }
  }
}

function renderRecall(seconds = recallWindowSeconds, nowSeconds = Date.now() / 1000) {
  const recentFinals = [...CAPTIONS_STATE.values()]
    .filter((caption) => {
      const age = nowSeconds - caption.source_ts;
      return caption.final && age >= 0 && age <= seconds;
    })
    .sort((a, b) => a.segment - b.segment);

  recallEl.textContent = recentFinals.length
    ? recentFinals.map((caption) => `[${caption.segment}] ${caption.text}`).join("\n")
    : `No finalized captions in the last ${seconds} seconds.`;
  recallEl.scrollTop = recallEl.scrollHeight;
}

function handleServerMessage(event) {
  let msg;
  try {
    msg = JSON.parse(event.data);
  } catch (error) {
    console.error("Ignoring invalid server JSON", error);
    return;
  }

  if (msg.type === "CAPTION_UPDATE") {
    upsertCaption(msg);
  } else if (msg.type === "HISTORY") {
    if (!Array.isArray(msg.items)) {
      console.error("Ignoring invalid HISTORY message: items must be an array", msg);
      return;
    }
    for (const caption of msg.items) {
      // HISTORY is reserved for finalized captions.
      if (caption?.final === true) upsertCaption(caption);
    }
  }

  pruneExpiredFinals();
  renderRecall();
}

function connect() {
  statusEl.textContent = "connecting";
  ws = new WebSocket(WS_URL);

  ws.addEventListener("open", () => {
    statusEl.textContent = "connected";
    reconnectAttempt = 0;
    ws.send(JSON.stringify({
      type: "CLIENT_HELLO",
      client_id: numericClientId(),
      session: SESSION_ID,
      last_segment: lastSegment,
      last_revision: lastRevision,
      history_seconds: SERVER_HISTORY_SECONDS,
    }));
  });

  ws.addEventListener("message", handleServerMessage);

  ws.addEventListener("close", () => {
    statusEl.textContent = "disconnected";
    if (reconnectTimer !== null) return;

    const delay = Math.min(1000 * 2 ** reconnectAttempt, 30000);
    reconnectAttempt += 1;
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      connect();
    }, delay);
  });

  ws.addEventListener("error", () => {
    statusEl.textContent = "connection error";
  });
}

for (const button of document.querySelectorAll("button[data-seconds]")) {
  button.addEventListener("click", () => {
    recallWindowSeconds = Number(button.dataset.seconds);
    renderRecall();
  });
}

// Expire old finalized rows even when no new captions are arriving.
setInterval(() => {
  pruneExpiredFinals();
  renderRecall();
}, 1000);

renderRecall();
connect();
