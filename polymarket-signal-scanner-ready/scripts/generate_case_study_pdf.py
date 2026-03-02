"""
Generate a professional multi-page case-study PDF report.

Output:
  docs/BIT_Capital_Case_Study_Final_Report_v2_2026-02-27.pdf
"""

from datetime import date
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import (
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)


OUT_PATH = Path("docs/BIT_Capital_Case_Study_Final_Report_v2_2026-02-27.pdf")


def build_styles():
    styles = getSampleStyleSheet()
    styles.add(
        ParagraphStyle(
            name="TitleMain",
            parent=styles["Title"],
            fontName="Helvetica-Bold",
            fontSize=22,
            leading=28,
            spaceAfter=14,
            textColor=colors.HexColor("#0f172a"),
        )
    )
    styles.add(
        ParagraphStyle(
            name="SubTitle",
            parent=styles["Normal"],
            fontName="Helvetica",
            fontSize=12,
            leading=16,
            textColor=colors.HexColor("#334155"),
            spaceAfter=8,
        )
    )
    styles.add(
        ParagraphStyle(
            name="H1",
            parent=styles["Heading1"],
            fontName="Helvetica-Bold",
            fontSize=16,
            leading=20,
            textColor=colors.HexColor("#111827"),
            spaceAfter=8,
            spaceBefore=2,
        )
    )
    styles.add(
        ParagraphStyle(
            name="H2",
            parent=styles["Heading2"],
            fontName="Helvetica-Bold",
            fontSize=13,
            leading=17,
            textColor=colors.HexColor("#111827"),
            spaceAfter=6,
            spaceBefore=6,
        )
    )
    styles.add(
        ParagraphStyle(
            name="Body",
            parent=styles["Normal"],
            fontName="Helvetica",
            fontSize=10.5,
            leading=15,
            textColor=colors.HexColor("#1f2937"),
            spaceAfter=6,
        )
    )
    styles.add(
        ParagraphStyle(
            name="Muted",
            parent=styles["Normal"],
            fontName="Helvetica-Oblique",
            fontSize=9,
            leading=12,
            textColor=colors.HexColor("#64748b"),
            spaceAfter=6,
        )
    )
    styles.add(
        ParagraphStyle(
            name="BulletBody",
            parent=styles["Normal"],
            fontName="Helvetica",
            fontSize=10.5,
            leading=14,
            leftIndent=12,
            bulletIndent=0,
            textColor=colors.HexColor("#1f2937"),
            spaceAfter=3,
        )
    )
    return styles


def footer(canvas, doc):
    canvas.saveState()
    canvas.setFont("Helvetica", 8.5)
    canvas.setFillColor(colors.HexColor("#6b7280"))
    canvas.drawString(20 * mm, 10 * mm, "Confidential - Case Study Submission")
    canvas.drawRightString(190 * mm, 10 * mm, f"Page {doc.page}")
    canvas.restoreState()


def bullets(story, styles, items):
    for item in items:
        story.append(Paragraph(item, styles["BulletBody"], bulletText="•"))


def add_cover(story, styles):
    story.append(Spacer(1, 36))
    story.append(Paragraph("BIT Capital - Case Study", styles["SubTitle"]))
    story.append(Paragraph("Final Report - Project Learnings", styles["SubTitle"]))
    story.append(Spacer(1, 10))
    story.append(Paragraph("Polymarket Signal Scanner for BIT Capital", styles["TitleMain"]))
    story.append(
        Paragraph(
            "AI Engineering Intern Case Study",
            styles["SubTitle"],
        )
    )
    story.append(Spacer(1, 26))
    story.append(
        Paragraph(
            "<b>Purpose</b>  Transform raw Polymarket prediction contracts into strict, explainable, and "
            "portfolio-linked signals for BIT analysts and PMs. The system prioritizes precision, stores explainability "
            "artifacts, and remains reliable under LLM rate limits via deterministic fallback.",
            styles["Body"],
        )
    )
    story.append(
        Paragraph(
            "<b>What makes this BIT-native</b>  Signals are routed to BIT fund sections and holdings via a portfolio "
            "trigger ontology (drivers, channels, macro regimes, and holding-level catalysts).",
            styles["Body"],
        )
    )
    story.append(
        Paragraph(
            "<b>How to review (2 minutes)</b>  Run SQLite mode from ZIP, click Ingest -> Analyze -> Generate Report, "
            "and verify strict IGNORE ratio plus explainable ACTIONABLE/MONITOR chains.",
            styles["Body"],
        )
    )
    story.append(Spacer(1, 24))
    story.append(Paragraph(f"Date: {date(2026, 2, 27).strftime('%B %d, %Y')}", styles["Body"]))
    story.append(Paragraph("Confidential: Case study submission", styles["Muted"]))
    story.append(PageBreak())


def add_toc(story, styles):
    story.append(Paragraph("Table of Contents", styles["H1"]))
    toc_rows = [
        ["1.", "Executive summary", "3"],
        ["2.", "Problem framing: why Polymarket and why it is hard", "4"],
        ["3.", "System architecture and operations", "5"],
        ["4.", "Ingestion and signal strength", "6"],
        ["5.", "Normalization and taxonomy", "7"],
        ["6.", "BIT ontology and routing", "8"],
        ["7.", "LLM layer and explainability", "9"],
        ["8.", "Database design", "10"],
        ["9.", "Analyst dashboard learnings", "11"],
        ["10.", "Evaluation approach", "11"],
        ["11.", "Constraints and trade-offs", "12"],
        ["12.", "Roadmap (next best steps)", "12"],
        ["Appendix", "Quickstart, API endpoints, glossary", "13"],
    ]
    t = Table(toc_rows, colWidths=[18 * mm, 145 * mm, 12 * mm])
    t.setStyle(
        TableStyle(
            [
                ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
                ("FONTSIZE", (0, 0), (-1, -1), 10),
                ("TEXTCOLOR", (0, 0), (-1, -1), colors.HexColor("#1f2937")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LINEBELOW", (0, 0), (-1, -1), 0.25, colors.HexColor("#e5e7eb")),
                ("ALIGN", (2, 0), (2, -1), "RIGHT"),
                ("LEFTPADDING", (0, 0), (-1, -1), 3),
                ("RIGHTPADDING", (0, 0), (-1, -1), 3),
                ("TOPPADDING", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ]
        )
    )
    story.append(t)
    story.append(PageBreak())


def add_section(story, styles, title, paragraphs, bullet_items=None):
    story.append(Paragraph(title, styles["H1"]))
    for p in paragraphs:
        story.append(Paragraph(p, styles["Body"]))
    if bullet_items:
        bullets(story, styles, bullet_items)


def build_story():
    styles = build_styles()
    s = []

    add_cover(s, styles)
    add_toc(s, styles)

    add_section(
        s,
        styles,
        "1. Executive summary",
        [
            "This project converts Polymarket's noisy prediction markets into a reliable, analyst-friendly signal pipeline tailored to BIT Capital exposures.",
            "The central learning is precision-over-recall: most markets must be ignored unless a defensible event -> channel -> exposure path can be stated explicitly.",
            "The resulting workflow is operational end-to-end: ingestion -> normalization/analysis -> report generation -> dashboard review.",
        ],
        [
            "End-to-end workflow delivered and ZIP-runnable.",
            "Strict explainability artifacts persisted for analyst trust and auditability.",
            "BIT-native routing to fund sections and holdings through trigger dictionaries.",
            "LLM layer optional; deterministic fallback guarantees continuity.",
            "Dual DB support: SQLite reviewer mode + Postgres/Supabase-ready mode.",
        ],
    )
    s.append(Paragraph("<b>North star:</b> Every signal must answer what happened, why it matters, which channel transmits it, which BIT exposures are affected, and what to watch next.", styles["Body"]))
    s.append(PageBreak())

    add_section(
        s,
        styles,
        "2. Problem framing: why Polymarket and why it is hard",
        [
            "Polymarket is an always-on market for real-world outcomes. For equity investors, implied probabilities can contain early macro and policy information.",
            "However, most contracts are not economically relevant for public equities. A robust system must aggressively suppress noise and preserve only transmission-valid events.",
            "Resolution-rule ambiguity and thin-liquidity contracts can also introduce false confidence if not handled with penalties and confidence controls.",
        ],
        [
            "High precision to protect analyst attention and decision quality.",
            "Explainability with persisted causal chain and matched signals.",
            "Reliability with graceful degradation under LLM limits.",
            "Portfolio alignment as a first-class routing objective.",
            "Persistence for future monitoring and backtesting.",
        ],
    )
    s.append(PageBreak())

    add_section(
        s,
        styles,
        "3. System architecture and operations",
        [
            "Pipeline design: ingestion stores markets and snapshots; analysis normalizes events, scores relevance, and routes to BIT funds/holdings; reporting compiles analyst summaries; UI/API expose all layers.",
            "The architecture is intentionally modular so each stage can be validated independently: fetch health, score quality, routing quality, report quality, and dashboard usability.",
        ],
        [
            "Provider-aware LLM paths with structured outputs and guardrails.",
            "Cooldown and quota-aware logic to avoid batch collapse.",
            "Deterministic fallback to preserve output continuity.",
            "Scheduled jobs plus manual controls for reviewer demonstrations.",
        ],
    )
    s.append(PageBreak())

    add_section(
        s,
        styles,
        "4. Ingestion and signal strength",
        [
            "Ingestion stores both contract metadata and time-series snapshots. Signal ranking is movement-aware, not static.",
        ],
    )
    signal_tbl = Table(
        [
            ["Signal strength feature", "Purpose"],
            ["Delta odds (24h / 7d)", "Detects rapid repricing and attention shifts."],
            ["Liquidity threshold", "Reduces fragile signals from thin contracts."],
            ["Volume threshold", "Prioritizes markets with meaningful participation."],
            ["Time-to-resolution bucket", "Improves actionability ranking for near-term events."],
        ],
        colWidths=[68 * mm, 112 * mm],
    )
    signal_tbl.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#eef2ff")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#111827")),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
                ("FONTSIZE", (0, 0), (-1, -1), 10),
                ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#d1d5db")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ]
        )
    )
    s.append(signal_tbl)
    s.append(PageBreak())

    add_section(
        s,
        styles,
        "5. Normalization and taxonomy",
        [
            "Raw market text is converted to canonical event objects: driver category, channels, macro regime hints, entities, polarity, and horizon.",
            "This normalization layer reduces prompt variance and makes downstream routing and scoring deterministic and auditable.",
        ],
        [
            "disinflation_soft_landing",
            "growth_scare_recession",
            "stagflation_oil_shock",
            "policy_surprise_hawkish / policy_surprise_dovish",
            "liquidity_crunch_funding_stress",
            "geopolitical_risk_premium",
        ],
    )
    s.append(PageBreak())

    add_section(
        s,
        styles,
        "6. BIT ontology and routing",
        [
            "BIT-native routing maps events to fund sections and holdings with a trigger ontology spanning macro, sector, policy, and company-specific catalysts.",
            "Routing is intentionally capped to keep analyst outputs focused and avoid over-broad exposure assignment.",
        ],
        [
            "AI/semi catalysts: hyperscaler capex, export controls, HBM, advanced packaging, utilization/capex cycles.",
            "Crypto infrastructure catalysts: BTC/ETH regime, hashrate/difficulty, power-cost shocks, regulatory actions.",
            "Fintech/insurtech catalysts: PFOF, KYC/AML, underwriting cycle, reinsurance repricing.",
            "Cyber catalysts: breach waves, mandates, procurement cycles.",
        ],
    )
    s.append(PageBreak())

    add_section(
        s,
        styles,
        "7. LLM layer and explainability",
        [
            "LLM reasoning is used for richer causal narratives and clearer analyst language, while outputs remain schema-constrained.",
            "When LLM is unavailable or rate-limited, deterministic templates maintain consistency and prevent operational stoppage.",
            "Each signal stores explainability artifacts for trust and traceability.",
        ],
        [
            "Matched keywords and normalized tags",
            "Causal chain steps and channel map",
            "Impacted holdings with direction and confidence",
            "What-to-watch-next metrics",
            "Red flags and unknowns",
        ],
    )
    s.append(PageBreak())

    add_section(
        s,
        styles,
        "8. Database design",
        [
            "The schema is normalized for analyst querying across time, funds, channels, holdings, and labels.",
            "This enables future quality monitoring and backtesting against realized equity moves.",
        ],
    )
    db_tbl = Table(
        [
            ["Entity Group", "Purpose"],
            ["markets + market_snapshots", "Raw contract universe and time-series state."],
            ["market_signals + relation tables", "Normalized relevance, channels, keywords, themes, holdings."],
            ["reports + report_items", "Analyst report archive and top-signal linkage."],
            ["job_runs", "Operational visibility for ingestion/analysis/report jobs."],
        ],
        colWidths=[74 * mm, 106 * mm],
    )
    db_tbl.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#ecfeff")),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
                ("FONTSIZE", (0, 0), (-1, -1), 10),
                ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#cbd5e1")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ]
        )
    )
    s.append(db_tbl)

    add_section(
        s,
        styles,
        "9. Analyst dashboard learnings",
        [
            "The dashboard must prioritize decisions, not raw data density. The core pattern is a triage center with top signals and one-click explainability.",
            "Useful analyst structure was: event -> channel -> fund -> holding, with explicit confidence and watch metrics.",
            "UI improvements were driven by practical PM workflow feedback: visibility, ordering clarity, and actionable language.",
        ],
    )
    s.append(Paragraph("10. Evaluation approach", styles["H1"]))
    bullets(
        s,
        styles,
        [
            "Operational smoke tests in SQLite mode (no external keys).",
            "Graceful degradation tests under provider quota exhaustion.",
            "Precision-first verification: most markets should be IGNORE.",
            "Manual plausibility review of MONITOR/ACTIONABLE samples.",
        ],
    )
    s.append(PageBreak())

    add_section(
        s,
        styles,
        "11. Constraints and trade-offs",
        [
            "The system intentionally biases toward precision to protect analyst trust.",
        ],
        [
            "Precision vs recall: conservative labeling by design.",
            "Low-liquidity noise: reduced with thresholds, not eliminated.",
            "Resolution ambiguity: reflected as confidence penalty.",
            "Holdings drift risk: ontology should be refreshed periodically.",
        ],
    )

    s.append(Paragraph("12. Roadmap (next best steps)", styles["H1"]))
    bullets(
        s,
        styles,
        [
            "Evidence retriever with source ranking and contradiction checks.",
            "Backtesting module for signal quality vs realized returns.",
            "Provider health/quota telemetry in dashboard.",
            "Enterprise hardening: auth, roles, audit trail, alert routing.",
            "Expanded CI and regression tests for API/UI pipeline.",
        ],
    )
    s.append(
        Paragraph(
            "<b>Final reflection:</b> A prediction-market scanner is only valuable if it behaves like a disciplined analyst: strict, explainable, and portfolio-aligned. "
            "This system is operational end-to-end and remains reliable even when the LLM layer is constrained.",
            styles["Body"],
        )
    )
    s.append(PageBreak())

    add_section(
        s,
        styles,
        "Appendix A. Reviewer quickstart (copy/paste)",
        [],
    )
    quick = Table(
        [
            ["Step", "Command"],
            ["1", "cd /path/to/polymarket-signal-scanner-v2"],
            ["2", "python3 -m venv .venv"],
            ["3", "source .venv/bin/activate"],
            ["4", "pip install -r requirements.txt"],
            ["5", "cp .env.example .env"],
            ["6", "python3 app.py --port 5001"],
        ],
        colWidths=[16 * mm, 164 * mm],
    )
    quick.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#f8fafc")),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
                ("FONTSIZE", (0, 0), (-1, -1), 9.5),
                ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#d1d5db")),
                ("LEFTPADDING", (0, 0), (-1, -1), 5),
                ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ]
        )
    )
    s.append(quick)
    s.append(Spacer(1, 10))

    s.append(Paragraph("Appendix B. Core API endpoints", styles["H2"]))
    bullets(
        s,
        styles,
        [
            "GET /api/stats",
            "GET /api/markets",
            "GET /api/markets/live",
            "GET /api/signals",
            "GET /api/reports",
            "POST /api/actions/ingest",
            "POST /api/actions/analyze",
            "POST /api/actions/report",
            "GET /api/job/status",
        ],
    )
    s.append(Spacer(1, 6))
    s.append(Paragraph("Appendix C. Glossary", styles["H2"]))
    bullets(
        s,
        styles,
        [
            "Driver category: the economic reason a market can matter.",
            "Market channel: transmission path into prices (rates, liquidity, regulation, etc.).",
            "Macro regime: scenario context (soft landing, stagflation, liquidity stress).",
            "Routing: assigning signals to BIT funds/holdings based on exposure logic.",
            "Explainability artifacts: persisted chain/keywords/holding impacts for analyst trust.",
        ],
    )

    return s


def main():
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    doc = SimpleDocTemplate(
        str(OUT_PATH),
        pagesize=A4,
        leftMargin=20 * mm,
        rightMargin=20 * mm,
        topMargin=18 * mm,
        bottomMargin=16 * mm,
        title="BIT Capital Polymarket Signal Scanner - Final Report (Extended v2)",
        author="Case Study Submission",
        subject="AI Engineering Intern Case Study",
    )
    story = build_story()
    doc.build(story, onFirstPage=footer, onLaterPages=footer)
    print(f"Generated: {OUT_PATH}")


if __name__ == "__main__":
    main()
