#!/usr/bin/env python3
"""Build a LiveRecall report preview or the validated final report.

The preview visibly marks missing public results. For the final report, supply
docs/report_data.json with T1-T5 objects containing the fields in PUBLIC_KEYS.
Do not copy preview output to report.pdf.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import (
    Flowable,
    KeepTogether,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)


ROOT = Path(__file__).resolve().parent.parent
PUBLIC_KEYS = ("condition", "p50", "p95", "p99", "fresh", "fresh_finals", "complete", "coverage")
TESTS = ("T1", "T2", "T3", "T4", "T5")
PENDING = {key: "pending" for key in PUBLIC_KEYS}

INK = colors.HexColor("#162127")
MUTED = colors.HexColor("#53636a")
TEAL = colors.HexColor("#007c78")
PALE = colors.HexColor("#eaf5f3")
LINE = colors.HexColor("#d8e1e3")


class Architecture(Flowable):
    """Small system flow diagram that stays sharp in the PDF."""

    def __init__(self, width: float):
        super().__init__()
        self.width = width
        self.height = 92

    def draw(self):
        canvas = self.canv
        boxes = [
            (0, "Caption source", "deterministic revisions"),
            (160, "LiveRecall server", "state + bounded outboxes"),
            (320, "Browser clients", "live + local recall"),
        ]
        for x, title, subtitle in boxes:
            canvas.setFillColor(PALE)
            canvas.setStrokeColor(LINE)
            canvas.roundRect(x, 18, 145, 62, 8, fill=1, stroke=1)
            canvas.setFillColor(INK)
            canvas.setFont("Helvetica-Bold", 10)
            canvas.drawCentredString(x + 72.5, 53, title)
            canvas.setFont("Helvetica", 7.5)
            canvas.setFillColor(MUTED)
            canvas.drawCentredString(x + 72.5, 38, subtitle)
        canvas.setStrokeColor(TEAL)
        canvas.setLineWidth(1.8)
        for start in (146, 306):
            canvas.line(start, 49, start + 12, 49)
            p = canvas.beginPath()
            p.moveTo(start + 12, 49)
            p.lineTo(start + 7, 52)
            p.moveTo(start + 12, 49)
            p.lineTo(start + 7, 46)
            canvas.drawPath(p)


def styles():
    base = getSampleStyleSheet()
    base.add(ParagraphStyle(name="CoverTitle", parent=base["Title"], fontName="Helvetica-Bold", fontSize=22, leading=26, textColor=INK, alignment=TA_LEFT, spaceAfter=8))
    base.add(ParagraphStyle(name="Deck", parent=base["Normal"], fontSize=10, leading=14, textColor=MUTED, spaceAfter=11))
    base.add(ParagraphStyle(name="Section", parent=base["Heading1"], fontName="Helvetica-Bold", fontSize=14, leading=17, textColor=INK, spaceBefore=8, spaceAfter=7))
    base.add(ParagraphStyle(name="Subsection", parent=base["Heading2"], fontName="Helvetica-Bold", fontSize=10.5, leading=13, textColor=TEAL, spaceBefore=9, spaceAfter=4))
    base.add(ParagraphStyle(name="Body", parent=base["Normal"], fontSize=9.1, leading=13.2, textColor=INK, spaceAfter=7))
    base.add(ParagraphStyle(name="Note", parent=base["Normal"], fontSize=8, leading=11.2, textColor=MUTED, spaceAfter=6))
    base.add(ParagraphStyle(name="TableHead", parent=base["Normal"], fontName="Helvetica-Bold", fontSize=7, leading=9, textColor=colors.white, alignment=TA_CENTER))
    base.add(ParagraphStyle(name="TableCell", parent=base["Normal"], fontSize=7, leading=9, textColor=INK, alignment=TA_CENTER))
    base.add(ParagraphStyle(name="TableLeft", parent=base["TableCell"], alignment=TA_LEFT))
    return base


def para(text: str, style: str = "Body"):
    return Paragraph(text, STYLES[style])


def bullet(text: str):
    return para("<font color='#007c78'><b>•</b></font>  " + text)


def table(rows, widths, first_header=True):
    prepared = []
    for i, row in enumerate(rows):
        style_name = "TableHead" if first_header and i == 0 else "TableCell"
        prepared.append([para(str(value), "TableLeft" if j == 0 and i else style_name) for j, value in enumerate(row)])
    obj = Table(prepared, colWidths=widths, hAlign="LEFT", repeatRows=1 if first_header else 0)
    obj.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), TEAL),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f5f8f8")]),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ("TOPPADDING", (0, 0), (-1, -1), 7),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
        ("LINEBELOW", (0, -1), (-1, -1), 0.5, LINE),
    ]))
    return obj


def load_public(preview: bool) -> dict:
    path = ROOT / "docs" / "report_data.json"
    if not path.exists():
        if preview:
            return {test: dict(PENDING) for test in TESTS}
        raise SystemExit("Missing docs/report_data.json. Run T1-T5 and fill all report data before building report.pdf.")
    data = json.loads(path.read_text(encoding="utf-8"))
    for test in TESTS:
        if test not in data or any(not str(data[test].get(key, "")).strip() or str(data[test][key]).lower() == "pending" for key in PUBLIC_KEYS):
            if not preview:
                raise SystemExit(f"Incomplete {test} in docs/report_data.json")
            data[test] = {**PENDING, **data.get(test, {})}
    return data


def footer(canvas, doc, preview: bool):
    canvas.saveState()
    canvas.setStrokeColor(LINE)
    canvas.line(48, 39, A4[0] - 48, 39)
    canvas.setFont("Helvetica", 7.5)
    canvas.setFillColor(MUTED)
    canvas.drawString(48, 26, "CS3103 Assignment 4  |  LiveRecall")
    if preview:
        canvas.setFillColor(colors.HexColor("#ae4b2f"))
        canvas.drawCentredString(A4[0] / 2, 26, "DRAFT - T1-T5 DATA PENDING")
    canvas.drawRightString(A4[0] - 48, 26, f"{doc.page}")
    canvas.restoreState()


def build(preview: bool):
    public = load_public(preview)
    output = ROOT / "docs" / "report_preview.pdf" if preview else ROOT / "report.pdf"
    doc = SimpleDocTemplate(str(output), pagesize=A4, rightMargin=48, leftMargin=48, topMargin=50, bottomMargin=53,
                            title="LiveRecall: revision-aware delivery", author="CS3103 LiveRecall team")
    width = A4[0] - 96
    story = []

    # Page 1: system and user behavior.
    story += [para("LiveRecall", "CoverTitle"),
              para("Revision-aware live captions with bounded recovery and per-client delivery", "Deck"),
              para("Ng De Heng  •  Foo Juinn Kai  •  Jay Kim  •  Ong Cheng Leong", "Note"),
              para("1  System architecture", "Section"),
              para("The supplied deterministic source emits provisional revisions and a final caption for each segment. A WebSocket server validates and distributes that stream to many browser clients. The browser replaces revisions in place, retains recent finals, and provides a local 15/30/60-second recall view.")]
    story += [Architecture(width), Spacer(1, 5), para("Figure 1. Required data path. The server keeps bounded session history and separate per-client outboxes.", "Note")]
    story += [para("Protocol", "Subsection"),
              bullet("The source sends <b>SOURCE_HELLO</b>, then <b>CAPTION_UPDATE</b> objects with session, segment, revision, source timestamp, final flag, and text."),
              bullet("A student sends <b>CLIENT_HELLO</b> with the latest finalized segment/revision and a requested history window. The server replies with oldest-first <b>HISTORY</b>, then live updates."),
              bullet("The browser stores one current caption per segment, distinguishes provisional and final text, reconnects automatically, and requests only recent missed finals after a gap."),
              para("The same submitted server and browser are used for the deterministic application demo and the measured tests; speech recognition is outside the mandatory path.", "Note"),
              PageBreak()]

    # Page 2: correctness and overload design.
    story += [para("2  Correctness and recovery", "Section"),
              para("The server accepts a revision only when it is newer than the stored revision and the segment has not already become final. This prevents stale updates from overwriting completed text. Final captions enter a bounded recent-history deque. On reconnect, the client sends its latest finalized cursor; the server returns only newer finals within the 60-second recovery limit. The browser deduplicates history against local segment state."),
              para("A late joiner starts with cursor zero and receives recent context before live updates. Snapshotting history and subscribing a client occur without an intervening await, avoiding a gap or duplicate around the handoff. The browser holds at least 60 seconds of recent finalized captions locally for ordinary recall."),
              para("3  Semantic backpressure", "Section"),
              para("Each client has a separate bounded outbox and sender task, so one slow receiver cannot block the source handler or normal readers. Under normal conditions the full revision stream is forwarded. When the network path falls behind, TCP acknowledgement feedback holds provisional revisions while finals proceed. Application-level ping/pong feedback detects a reader that has stopped consuming even when TCP has accepted bytes; that reader is held and then paced near its measured reading rate."),
              bullet("Unsent obsolete provisional revisions can be replaced with the latest state of their segment; finalized captions remain durable."),
              bullet("Several pending finals may be bundled in one catch-up HISTORY frame, reducing the number of reads a slow client must perform."),
              bullet("Per-client pending work is capped; a client that falls too far behind is closed so it can resume through bounded history rather than growing an unlimited queue."),
              para("Trade-off", "Subsection"),
              para("Coalescing lowers provisional update coverage during overload but protects final-caption delivery and freshness. The control loops should return to full-update delivery once lag clears. The browser demo separately verifies revision replacement, local recall, reconnect, and late join."),
              para("The six server correctness checks (<b>bash tests/run_checks.sh</b>) pass on the machine used for every reported run.", "Note"),
              PageBreak()]

    # Page 3: public test methodology and tables.
    story += [para("4  Public experimental method", "Section"),
              para("The supplied harness, unmodified, starts a fresh server for each run and logs caption receipt by simulated clients. All runs used native Ubuntu 26.04 (Linux 7.0, 16 cores) with server, source and clients on one machine; T5 applies Linux tc/netem to loopback. Each scenario has three independent runs and median metrics. Caption Age is client receive time minus source timestamp. Freshness uses the public 500 ms threshold."),
              para("Required public tests", "Subsection")]
    rows = [["Test", "Condition", "P50", "P95", "P99", "Fresh", "Finals", "Compl.", "Cover."]]
    for test in TESTS:
        item = public[test]
        rows.append([test, item["condition"], item["p50"], item["p95"], item["p99"], item["fresh"], item["fresh_finals"], item["complete"], item["coverage"]])
    story += [table(rows, [31, 119, 48, 48, 48, 50, 50, 54, 51]), Spacer(1, 9),
              para("Latencies in ms. Every run reached 100% final completeness, with no connection-error, duplicate or stale events. T3: normal clients stayed 100% fresh; slow readers had P50 181 ms and P95 7.5 s, which raises the overall P99. T4: recovery P95 was 3.4 ms.", "Note"),
              para("T5 network profiles", "Subsection")]
    profiles = public.get("T5_profiles", {})
    if not profiles and not preview:
        raise SystemExit("Missing T5_profiles in docs/report_data.json")
    profile_rows = [["Profile", "P50", "P95", "P99", "Fresh", "Finals", "Compl.", "Cover."]]
    for key in ("delay_100ms", "delay_loss", "loss_3pct"):
        item = profiles.get(key, PENDING)
        profile_rows.append([item.get("profile", "pending"), item["p50"], item["p95"], item["p99"],
                             item["fresh"], item["fresh_finals"], item["complete"], item["coverage"]])
    story += [table(profile_rows, [151, 49, 49, 49, 50, 50, 51, 50]), Spacer(1, 8),
              para("Public network profiles on loopback, 100 clients, 40-second window, three runs each; every run reached 100% completeness. Delay shifts the whole distribution by about one round trip. Loss only hurts the tail: a lost segment is resent after TCP's 200 ms minimum retransmission timeout and every caption behind it waits (head-of-line blocking), so P95/P99 sit near that timeout.", "Note"),
              PageBreak()]

    # Page 4: before/after comparison, failures and trade-offs.
    story += [para("5  Before/after comparison", "Section"),
              para("Before: the team's earlier queue-based server (PR 8), which reacts only when its own outbox backs up. After: the final design. Same machine and harness, three runs each."),
              table([
                  ["Scenario", "Compl. before", "Compl. after", "Metric", "Before", "After"],
                  ["T3, 10% slow readers", "95.13%", "100.00%", "Slow-reader P50", "14.7 s", "181 ms"],
                  ["30% slow readers", "85.39%", "100.00%", "Slow-reader P50", "14.7 s", "204 ms"],
                  ["Slow readers, 300 clients, 2 Mbit/s", "95.00%", "100.00%", "Slow-reader P50", "14.6 s", "482 ms"],
                  ["700 clients, 2 Mbit/s", "89.51%", "99.99%", "P95", "7.0 s", "2.0 s"],
                  ["300 clients, 2 Mbit/s", "100%", "100%", "P50", "71 ms", "370 ms"],
              ], [150, 62, 62, 85, 70, 70]),
              Spacer(1, 9),
              para("A slow reader's websockets library and kernel buffers absorb thousands of ~42-byte messages before the earlier server's outbox backs up, so its slow readers ended about 15 s behind and missed finals. The final design detects the pause at the library through ping/pong and paces the reader. The last row is a regression, explained below."),
              para("6  Failures and trade-offs", "Section"),
              para("What did not work (each measured, then dropped)", "Subsection"),
              bullet("<b>Receive-window signal.</b> A slow reader's advertised TCP window stayed at 65,536 B with about 70 unread messages (128 KB buffers, ~42-byte messages), so TCP cannot reveal a slow reader. Hence the ping/pong loop."),
              bullet("<b>TCP_NOTSENT_LOWAT.</b> Keeping unsent data in the outbox, where it can be replaced, gave nothing on a 2 Mbit/s bottleneck: the backlog was already in flight, not unsent."),
              bullet("<b>One ping per 8 messages.</b> The pause point was known only to within 8 messages, so the server overestimated what was still queued after a stall (12 vs about 8). Readers taking 2 s per message idled about 10 s and missed the last final (99.87%). A ping on every 4th message, with the stall timed from the middle of the uncertainty interval, fixed this (100%; T3 slow-reader P50 from 222 to 181 ms)."),
              bullet("<b>Thresholds from TCP's retransmission timeout and the caption stream's rate.</b> On a congested link a late pong includes network delay, so the reading rate implied by a stall is wrong: 909 fast clients were paced at 500 clients, and worst-run completeness fell to 94.3% at 700 clients. Reverted."),
              para("Trade-offs we kept", "Subsection"),
              bullet("<b>Slow readers</b> get fewer provisionals but current text: T3 coverage 94.4% (queue-only server 95.2%), slow-reader P50 181 ms (14.7 s), completeness 100% (95.13%)."),
              bullet("<b>Under overload</b>, provisionals wait in the outbox and are replaced while finals go out: coverage 61% vs 99% at 500 clients on 2 Mbit/s, but completeness 99.99% vs 89.5% at 700 clients."),
              bullet("<b>Probe precision costs bandwidth.</b> Each ping adds a pong and its ACK; at 300 clients on 2 Mbit/s P50 is 370 ms vs 71 ms."),
              bullet("<b>A slow reader's first 15-20 s are stale</b> (8% fresh, then about 100%), because its library buffers 16 messages before any signal reaches the server. Shortening this would mean throttling every new client, which the coverage check penalizes."),
              bullet("<b>Lost pongs under heavy overload</b> still cause false holds (518 fast-client pacing decisions across our network runs)."),
              para("7  Limitations and future work", "Section"),
              para("Results come from one machine with three runs per condition and a deterministic source; the hidden workload may combine conditions we did not test. Both loops read Linux TCP_INFO: on other platforms the network gate is off and a ping still in flight counts as a stall. Future work: a pause signal that survives congestion (for example, reading-progress reports from the client), pinging less often once a client has proven fast, and a real lecture speech recognizer behind the same protocol."),
              Spacer(1, 8),
              para("Sources: CS3103 Assignment 4 LiveRecall brief (17 Oct 2026); supplied harness and code; experiments/ and tests/ in this repository; earlier controlled slow-client study in docs/slow_client_evaluation.md.", "Note")]

    doc.build(story, onFirstPage=lambda c, d: footer(c, d, preview), onLaterPages=lambda c, d: footer(c, d, preview))
    print(output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--preview", action="store_true", help="allow missing T1-T5 data and watermark the PDF")
    args = parser.parse_args()
    STYLES = styles()
    build(args.preview)
