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
              para("The six deterministic server correctness checks passed on the Ubuntu VM used for the controlled slow-client study; the public and hidden performance workloads probe additional conditions.", "Note"),
              PageBreak()]

    # Page 3: public test methodology and tables.
    story += [para("4  Public experimental method", "Section"),
              para("The supplied harness starts a fresh server for each run and logs caption receipt by simulated clients. Tests run on Ubuntu with the source and clients in one VM; T5 applies Linux tc/netem to loopback. Each reported scenario has three independent runs and median metrics. Caption Age is client receive time minus source timestamp. Freshness uses the public 500 ms threshold."),
              para("Required public tests", "Subsection")]
    rows = [["Test", "Condition", "P50", "P95", "P99", "Fresh", "Finals", "Compl.", "Cover."]]
    for test in TESTS:
        item = public[test]
        rows.append([test, item["condition"], item["p50"], item["p95"], item["p99"], item["fresh"], item["fresh_finals"], item["complete"], item["coverage"]])
    story += [table(rows, [31, 119, 48, 48, 48, 50, 50, 54, 51]), Spacer(1, 9),
              para("T1-T4 had 100% median final completeness, but minimum individual-run completeness was 95.45%, 97.96%, 100%, and 97.37%, respectively. T3's slow readers raised P99 to 3.57 s; T4 recovery P95 was 15.1 ms. All reported T1-T5 runs had zero connection errors, duplicate captions, and stale captions.", "Note"),
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
              para("Profiles used Linux tc/netem on loopback: 100 ± 20 ms delay, 120 ± 30 ms delay with 3% IP packet loss, and 3% loss alone. Each used 100 clients, a 40-second window, and three completed runs. Minimum per-run completeness was 100%, 97.82%, and 83.67%. The combined profile's P95 rose to 818 ms; the loss-only profile's 85.71% median completeness is a material shortfall despite its 64 ms P95 among observed captions. TCP retransmissions can increase tail latency; these summaries alone cannot establish why expected finals were absent before the cutoff.", "Note"),
              PageBreak()]

    # Page 4: controlled comparison and interpretation.
    story += [para("5  Controlled slow-reader comparison", "Section"),
              para("A separate A/B study used 100 clients, 30% slow readers sleeping 500 ms after each message, exactly 76 source segments, and a 100-second client window. Three independent runs were made with the final lag-probe design and with <b>LIVERECALL_BACKPRESSURE=0</b>. The fixed segment count and longer window followed a 60-second sweep whose role breakdown exposed the cutoff confound."),
              table([
                  ["Lag probes", "Overall P95", "Slow P95", "Fresh finals", "Final completeness", "Connection errors"],
                  ["Enabled", "1,908 ms", "8,321 ms", "88.95%", "100 / 100 / 100%", "0 / 0 / 0"],
                  ["Disabled", "35,563 ms", "42,356 ms", "70.39%", "92.5 / 100 / 100%", "7 / 30 / 30"],
              ], [76, 75, 75, 79, 104, 90]),
              Spacer(1, 11),
              para("With probes enabled, median slow-client P95 was about 34 seconds lower and the fresh-final ratio was 18.56 percentage points higher. Normal-client P95 stayed below 30 ms in both configurations. Normal update coverage was 83.68% with probes and 92.73% without, consistent with replacing obsolete provisional revisions during overload. No duplicate or stale captions were recorded in these runs."),
              para("6  Limitations and future work", "Section"),
              para("The A/B result is one controlled workload on one VM with three repetitions; hidden tests may combine larger populations, burstier revisions, constrained bandwidth, and impairment removal. The initial 60-second sweep shows why observation-window timing must be inspected before interpreting missing finals. T5 packet loss exposed a completeness shortfall; per-run source and delivery timestamps are needed to separate source timing, retransmission delay, and server behavior."),
              para("Future work could adapt read-rate estimation to other WebSocket client libraries and network paths, measure convergence after a bottleneck clears, and integrate a real lecture speech recognizer while preserving the same protocol and mandatory lightweight server path."),
              Spacer(1, 8),
              para("Sources: CS3103 Assignment 4 LiveRecall brief (17 Oct 2026); supplied harness and code; controlled study in docs/slow_client_evaluation.md.", "Note")]

    doc.build(story, onFirstPage=lambda c, d: footer(c, d, preview), onLaterPages=lambda c, d: footer(c, d, preview))
    print(output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--preview", action="store_true", help="allow missing T1-T5 data and watermark the PDF")
    args = parser.parse_args()
    STYLES = styles()
    build(args.preview)
