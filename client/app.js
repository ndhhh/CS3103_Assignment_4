/*
 * Working but deliberately naive LiveRecall browser starter.
 *
 * Provided mechanical behavior:
 *   - opens a WebSocket
 *   - sends CLIENT_HELLO
 *   - visibly appends every received CAPTION_UPDATE
 *   - displays whether an update is provisional/final
 *
 * Graded TODOs:
 *   - replace revisions rather than appending duplicates
 *   - maintain >=60 s of finalized local state
 *   - implement 30 s default recall + 15/30/60 s views
 *   - automatic reconnect + correct resume state
 *   - HISTORY merge/deduplication
 *   - usable final UI
 */

const WS_URL = "ws://127.0.0.1:8765";

// Elements below are to be updated by the client's state
// These include things like connection status, current live caption, and past recalled captions
const statusEl = document.getElementById("status");
const liveEl = document.getElementById("live");
const recallEl = document.getElementById("recall");


// This is to store caption state of the client
// Any new captions are added/updated here 
// Recalled captions are retrieved from here too.
const CAPTIONS_STATE = new Map()
let lastSegment = 0
let lastRevision = 0

let ws;

// Return a stable numeric client ID, creating and storing one for this tab if needed.
function numericClientId() {
  const key = "liverecall-client-id";
  let value = sessionStorage.getItem(key);
  if (!value) {
    value = String(Math.floor(100000 + Math.random() * 900000));
    sessionStorage.setItem(key, value);
  }
  return Number(value);
}

// Append one caption revision as a separate row; this baseline does not replace older revisions.
// TODO: Replace captions with newer one each time it is updated. DONE DONE
function appendNaively(msg) {
  // Check if the Live div has a child element for displaying text.
  // The Live div should only have 1 child at all times.
  if (liveEl.childNodes.length > 0 && liveEl.childNodes.length <= 1) {
    // If yes, then update its text. 
    let row = liveEl.firstChild;
    const state = msg.final ? "FINAL" : "PROVISIONAL";
    row.textContent = `[${state}] segment ${msg.segment} rev ${msg.revision}: ${msg.text}`;
  } else {
    // If no child, i.e. this is the first caption displayed, then create one
    const row = document.createElement("div");
    row.className = "caption-line";
    const state = msg.final ? "FINAL" : "PROVISIONAL";
    row.textContent = `[${state}] segment ${msg.segment} rev ${msg.revision}: ${msg.text}`;
    liveEl.appendChild(row);
  }

  liveEl.scrollTop = liveEl.scrollHeight;
}

// Open the WebSocket, send the client hello, and register handlers for server messages and connection events.
function connect() {
  statusEl.textContent = "connecting";
  ws = new WebSocket(WS_URL);

  // Identify this client to the server after the WebSocket connection opens.
  ws.addEventListener("open", () => {
    statusEl.textContent = "connected";
    ws.send(JSON.stringify({
      type: "CLIENT_HELLO",
      client_id: numericClientId(),
      session: "L1",
      last_segment: lastSegment,       // TODO: resume latest finalized state. DONE
      last_revision: lastRevision,
      history_seconds: 30
    }));
  });

  // Display incoming live captions and log the HISTORY response; merging history is not implemented yet.
  ws.addEventListener("message", (event) => {
    const msg = JSON.parse(event.data);
    if (msg.type === "CAPTION_UPDATE") {
      // For updates, verify that it is a new revision or segment,
      // then perform the appending/displaying logic
      const existing = CAPTIONS_STATE.get(msg.segment);
      
      if (!existing || (caption.revision > existing.revision && caption.final != true)) {
        // Update state with latest revision OR add if this is a new caption
        CAPTIONS_STATE.set(caption.segment, caption);

        // Update client lastSegment and lastRevision in case of disconnect.
        if (
          msg.segment > lastSegment ||
          (msg.segment === lastSegment && msg.revision > lastRevision)
        ) {
          lastSegment = caption.segment;
          lastRevision = caption.revision;
        }

        // Add to state + display on div
        appendNaively(msg);

      }
      
    

    } else if (msg.type === "HISTORY") {
      console.log(`HISTORY with ${msg.items?.length ?? 0} item(s)`, msg);
      if (!Array.isArray(msg.items)) {
        // Ensure that items is an array of captions
        console.error("Invalid HISTORY message: items must be an array", msg);
        return;
      }

      for (const caption of msg.items) {
        // Error handling
        if (
          !caption ||
          !caption.final ||
          !Number.isInteger(caption.segment) ||
          !Number.isInteger(caption.revision)
        ) {
          continue;
        }

        // Update state with latest revision OR add if this is a new caption
        const existing = CAPTIONS_STATE.get(caption.segment);
        if (!existing || (caption.revision > existing.revision && caption.final != true)) {
          CAPTIONS_STATE.set(caption.segment, caption);
        }
        
        
        if (
          caption.segment > lastSegment ||
          (caption.segment === lastSegment && caption.revision > lastRevision)
        ) {
          lastSegment = caption.segment;
          lastRevision = caption.revision;
        }
      }

      // Update the recent finalised captions in the Recall div element
      const recentFinals = [...CAPTIONS_STATE.values()]
        .filter((caption) => caption.final)
        .sort((a, b) => a.segment - b.segment);
      recallEl.textContent = recentFinals
        .map((caption) => caption.text)
        .join("\n");
    }
  });

  // Update the UI when the WebSocket closes; automatic reconnect is not implemented yet.
  ws.addEventListener("close", () => {
    statusEl.textContent = "disconnected";
    // TODO: automatic reconnect after a short delay, using finalized resume state.
  });

  // Reflect WebSocket errors in the connection status display.
  ws.addEventListener("error", () => {
    statusEl.textContent = "connection error";
  });
}

// Show a placeholder recall message for the time window selected by each button.
for (const button of document.querySelectorAll("button[data-seconds]")) {
  button.addEventListener("click", () => {
    const seconds = Number(button.dataset.seconds);
    recallEl.textContent = `TODO: show finalized captions from the previous ${seconds} seconds using local state.`;
  });
}

connect();
