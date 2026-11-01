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
const disconnectButton = document.getElementById("disconnect");
const resetButton = document.getElementById("reset");
const backToLiveButton = document.getElementById("back-to-live");
const emptyStateEl = document.getElementById("empty-state");

// Each value stores the latest caption fields plus the row used to display it.
const CAPTIONS_STATE = new Map();
let lastSegment = 0;
let lastRevision = 0;
let recallWindowSeconds = 30;

let reconnectTimer = null;
let reconnectAttempt = 0;
let ws;

disconnectButton.disabled = true;

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

  const state = document.createElement("span");
  state.className = "caption-state";
  const text = document.createElement("span");
  text.className = "caption-text";
  row.append(state, text);

  // Newest segments lead; revisions keep their existing row.
  const nextRow = [...liveEl.children].find(
    (child) => Number(child.dataset.segment) < segment,
  );
  liveEl.insertBefore(row, nextRow ?? null);
  return row;
}

function renderCaption(caption) {
  caption.row.className = `caption-line ${caption.final ? "final" : "provisional"}`;
  caption.row.querySelector(".caption-state").textContent = caption.final ? "Final caption" : "Updating live";
  caption.row.querySelector(".caption-text").textContent = caption.text;
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

function captureReadingPosition() {
  const anchor = [...liveEl.children].find(
    (row) => !row.hidden && row.getBoundingClientRect().bottom > 0,
  );
  return { anchor, top: anchor?.getBoundingClientRect().top, scrollTop: window.scrollY };
}

function renderRecall(seconds = recallWindowSeconds, nowSeconds = Date.now() / 1000,
                      view = captureReadingPosition()) {
  let visibleCount = 0;
  for (const row of liveEl.children) {
    const caption = CAPTIONS_STATE.get(Number(row.dataset.segment));
    const age = nowSeconds - caption.source_ts;
    row.hidden = caption.final && (age < 0 || age > seconds);
    if (row.hidden) continue;
    row.classList.toggle("latest", visibleCount === 0);
    visibleCount += 1;
  }

  if (view.scrollTop <= 24) {
    window.scrollTo({ top: 0, behavior: "instant" });
  } else if (view.anchor?.isConnected && !view.anchor.hidden) {
    window.scrollBy({ top: view.anchor.getBoundingClientRect().top - view.top, behavior: "instant" });
  }

  backToLiveButton.hidden = window.scrollY <= 24;
  emptyStateEl.hidden = visibleCount > 0;
  recallEl.textContent = `Showing the last ${seconds} seconds`;
  for (const button of document.querySelectorAll("button[data-seconds]")) {
    button.setAttribute("aria-pressed", String(Number(button.dataset.seconds) === seconds));
  }
}

function handleServerMessage(event) {
  let msg;
  try {
    msg = JSON.parse(event.data);
  } catch (error) {
    console.error("Ignoring invalid server JSON", error);
    return;
  }

  const view = captureReadingPosition();
  if (msg.type === "CAPTION_UPDATE") {
    upsertCaption(msg);
  } else if (msg.type === "HISTORY") {
    if (!Array.isArray(msg.items)) {
      console.error("Ignoring invalid HISTORY message: items must be an array", msg);
      return;
    }
    if (msg.items.length > 0) {
        console.log(`Last segment of client is ${lastSegment} with final = ${CAPTIONS_STATE.get(lastSegment)?.final}.`);
        console.log(`oldest segment received is ${msg.items[0].segment}.`);
        console.log(`Oldest caption age in History is ${Number((Date.now() / 1000) - msg.items[0]?.source_ts)} seconds`);
        console.log(`History size is ${msg.items.length} with segments ${msg.items[0].segment} to ${msg.items[msg.items.length - 1].segment}.`);
    }

    for (const caption of msg.items) {
      // HISTORY is reserved for finalized captions.
      if (caption?.final === true) upsertCaption(caption);
    }
  }

  pruneExpiredFinals();
  renderRecall(recallWindowSeconds, Date.now() / 1000, view);
}

function connect() {
  statusEl.textContent = "connecting";
  statusEl.dataset.state = "connecting";
  ws = new WebSocket(WS_URL);

  ws.addEventListener("open", () => {
    statusEl.textContent = "connected";
    statusEl.dataset.state = "connected";
    console.log("Client connected.")
    disconnectButton.disabled = false;
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
    statusEl.dataset.state = "disconnected";
    disconnectButton.disabled = true;
    if (reconnectTimer !== null) return;

    const delay = Math.min(3000 * 2 ** reconnectAttempt, 30000);
    reconnectAttempt += 1;
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      connect();
    }, delay);
  });

  ws.addEventListener("error", () => {
    statusEl.textContent = "connection error";
    statusEl.dataset.state = "error";
  });
}


for (const button of document.querySelectorAll("button[data-seconds]")) {
  button.addEventListener("click", () => {
    recallWindowSeconds = Number(button.dataset.seconds);
    renderRecall();
  });
}

window.addEventListener("scroll", () => {
  backToLiveButton.hidden = window.scrollY <= 24;
}, { passive: true });

backToLiveButton.addEventListener("click", () => {
  window.scrollTo({ top: 0, behavior: "instant" });
  backToLiveButton.hidden = true;
});

disconnectButton.addEventListener("click", () => {
  if (ws?.readyState === WebSocket.OPEN) {
    disconnectButton.disabled = true;
    statusEl.textContent = "Simulating disconnect";
    console.log("Client disconnect simulated.");
    ws.close(4000, "Disconnect test requested");
  }
});

resetButton.addEventListener("click", () => {
  CAPTIONS_STATE.clear();
  liveEl.replaceChildren();
  lastSegment = 0;
  lastRevision = 0;
  renderRecall();

  if (ws?.readyState === WebSocket.OPEN) {
    statusEl.textContent = "resetting";
    ws.close(1000, "Client state reset");
  }

  console.log("Client state cleared; reconnecting for recent history.");
});

// Expire old finalized rows even when no new captions are arriving.
setInterval(() => {
  const view = captureReadingPosition();
  pruneExpiredFinals();
  renderRecall(recallWindowSeconds, Date.now() / 1000, view);
}, 1000);

renderRecall();
connect();
