"""
lens_deep_report.py
Project Lens — 10-Page Deep Intelligence Report

Runs once per day after morning cron (05:30 UTC).
Fetches S3-A, S3-B, S3-D, S4 predictions, MA report.
Calls Claude Sonnet 4.6 for deep 10-page analysis.
Generates PDF and sends to Telegram.

Cost: ~$0.25/day (15K input + 8K output tokens)
"""

import os, json, logging, time
from datetime import datetime, timezone, timedelta
from typing import Optional

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [DEEP-REPORT] %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("DEEP_REPORT")

MODEL = "claude-sonnet-4-6"
MAX_TOKENS = 16000  # ~6000 words = ~10 A4 pages


# ── Clients ───────────────────────────────────────────────────────────────────
def get_supabase():
    from supabase import create_client
    return create_client(
        os.environ["SUPABASE_URL"],
        os.environ["SUPABASE_SERVICE_KEY"]
    )

def get_anthropic():
    import anthropic
    return anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])


# ── Data fetch ────────────────────────────────────────────────────────────────
def fetch_s3_reports(sb) -> dict:
    """Fetch latest S3-A, S3-B, S3-D reports."""
    ctx = {}
    for position in ["S3-A", "S3-B", "S3-D", "S3-C"]:
        try:
            r = sb.table("lens_system3_reports") \
                .select("position,summary,first_domino,patterns_found,quality_score,generated_at") \
                .eq("position", position) \
                .order("generated_at", desc=True) \
                .limit(1).execute()
            if r.data:
                ctx[position] = r.data[0]
                log.info(f"{position} loaded: {r.data[0].get('generated_at','?')[:16]}")
        except Exception as e:
            log.warning(f"{position} fetch failed: {e}")
    return ctx


def fetch_s4_predictions(sb) -> list:
    """Fetch last 5 S4 predictions."""
    try:
        r = sb.table("lens_predictions") \
            .select("prediction,confidence,verification_date,created_at") \
            .order("created_at", desc=True) \
            .limit(5).execute()
        return r.data or []
    except Exception as e:
        log.warning(f"S4 predictions fetch failed: {e}")
        return []


def fetch_ma_report(sb) -> dict:
    """Fetch latest Mission Analyst macro report."""
    try:
        r = sb.table("lens_macro_reports") \
            .select("threat_level,executive_summary,key_findings,manufactured_narratives,adversary_narrative_summary,actors_of_concern,gcsp_implications,intelligence_gaps,cui_bono_synthesis,quality_score,created_at") \
            .order("created_at", desc=True) \
            .limit(1).execute()
        return r.data[0] if r.data else {}
    except Exception as e:
        log.warning(f"MA report fetch failed: {e}")
        return {}


def fetch_s2_summary(sb) -> list:
    """Fetch top S2 injection findings from last 24h."""
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        r = sb.table("injection_reports") \
            .select("analyst,injection_type,confidence_score,flagged_phrases,evidence") \
            .gte("created_at", cutoff) \
            .order("confidence_score", desc=True) \
            .limit(10).execute()
        return r.data or []
    except Exception as e:
        log.warning(f"S2 fetch failed: {e}")
        return []


# ── Prompt builder ────────────────────────────────────────────────────────────
def build_deep_prompt(s3: dict, s4: list, ma: dict, s2: list) -> str:
    today = datetime.now(timezone.utc).strftime("%B %d, %Y")

    # Format S3-A
    s3a = s3.get("S3-A", {})
    s3a_text = f"""
S3-A: PATTERN INTELLIGENCE (7-day window)
Generated: {s3a.get('generated_at','unknown')[:16]}
Quality: {s3a.get('quality_score', 0):.2f}

Summary:
{s3a.get('summary', 'No data available')}

First Domino (what becomes inevitable if patterns continue):
{s3a.get('first_domino', 'Not yet identified')}

Patterns found:
{s3a.get('patterns_found', '[]')}
""" if s3a else "S3-A: No pattern intelligence available yet."

    # Format S3-B
    s3b = s3.get("S3-B", {})
    s3b_text = f"""
S3-B: TRUE HISTORY / HISTORICAL ANALOG
Generated: {s3b.get('generated_at','unknown')[:16]}

{s3b.get('summary', 'No historical analog available yet.')}

Historical First Domino:
{s3b.get('first_domino', 'Not identified')}
""" if s3b else "S3-B: No historical analog available yet."

    # Format S3-D
    s3d = s3.get("S3-D", {})
    s3d_text = f"""
S3-D: STRUCTURAL ACCUMULATION (30-day window)
Generated: {s3d.get('generated_at','unknown')[:16]}
Quality: {s3d.get('quality_score', 0):.2f}

{s3d.get('summary', 'No structural analysis available yet.')}

Structural First Domino:
{s3d.get('first_domino', 'Not identified')}
""" if s3d else "S3-D: No structural analysis available yet."

    # Format S3-C
    s3c = s3.get("S3-C", {})
    s3c_text = f"""
S3-C: BIAS DRIFT MONITOR (weekly)
{s3c.get('summary', 'No drift analysis available yet — runs weekly.')}
""" if s3c else "S3-C: Weekly drift monitor — not yet run."

    # Format S4
    if s4:
        s4_text = "S4: ACTIVE PREDICTIONS (awaiting verification)\n"
        for p in s4:
            conf = p.get('confidence', 0) or 0
            s4_text += f"\n- Prediction: {p.get('prediction','?')}\n"
            s4_text += f"  Confidence: {conf:.0%} | Verify by: {p.get('verification_date','?')}\n"
    else:
        s4_text = "S4: No predictions recorded yet — System 4 begins collecting from next run."

    # Format MA
    if ma:
        findings = ma.get('key_findings', [])
        if isinstance(findings, str):
            try: findings = json.loads(findings)
            except: findings = []
        findings_text = ""
        if findings and isinstance(findings, list):
            for f in findings[:5]:
                if isinstance(f, dict):
                    findings_text += f"\n- {f.get('finding','')}: {f.get('significance','')}"

        cui_bono = ma.get('cui_bono_synthesis', {})
        if isinstance(cui_bono, str):
            try: cui_bono = json.loads(cui_bono)
            except: cui_bono = {}

        ma_text = f"""
MISSION ANALYST MACRO REPORT
Generated: {ma.get('created_at','?')[:16]}
Threat Level: {ma.get('threat_level', 'UNKNOWN')}
Quality: {ma.get('quality_score', 0):.2f}

Executive Summary:
{ma.get('executive_summary', 'Not available')}

Key Findings:{findings_text}

Adversary Narrative:
{ma.get('adversary_narrative_summary', 'Not available')}

Cui Bono (who benefits):
Primary beneficiary: {cui_bono.get('primary_beneficiary','?') if isinstance(cui_bono, dict) else 'Unknown'}
Convergence: {cui_bono.get('convergence','?') if isinstance(cui_bono, dict) else 'Unknown'}

Intelligence Gaps:
{ma.get('intelligence_gaps', 'None identified')}

GCSP Implications:
{json.dumps(ma.get('gcsp_implications', []), indent=2)}
"""
    else:
        ma_text = "MISSION ANALYST: No macro report available yet."

    # Format S2
    s2_text = "S2 INJECTION SUMMARY (last 24h):\n"
    for inj in s2[:5]:
        analyst = inj.get('analyst','?')
        itype = inj.get('injection_type','?')
        conf = inj.get('confidence_score', 0) or 0
        ev = inj.get('evidence', {})
        if isinstance(ev, str):
            try: ev = json.loads(ev)
            except: ev = {}
        desc = ""
        if isinstance(ev, dict):
            desc = ev.get('description','') or ev.get('primary_narrative','') or ev.get('q1','')
        s2_text += f"\n[{analyst}] {itype} ({conf:.0%})"
        if desc:
            s2_text += f"\n  {str(desc)[:200]}"

    prompt = f"""You are the Senior Intelligence Analyst for Project Lens — a deep geopolitical intelligence platform serving GCSP (Geneva Centre for Security Policy) and emerging global leaders.

Today's date: {today}

You have received intelligence from all four analytical systems. Your task is to synthesize this into a comprehensive 10-page intelligence assessment.

INTELLIGENCE INPUT:
═══════════════════════════════════════════════════════════

{s2_text}

{ma_text}

{s3a_text}

{s3b_text}

{s3d_text}

{s3c_text}

{s4_text}

═══════════════════════════════════════════════════════════

REPORT REQUIREMENTS:
- Length: 10 A4 pages (5,000-6,000 words minimum)
- Audience: Senior analysts, GCSP educators, emerging global leaders
- Tone: Analytically precise. Written for intelligent adults.
- NO speculation without evidence from the intelligence above
- Every claim must be grounded in the data provided
- Connect patterns across systems — this is the synthesis layer

MANDATORY STRUCTURE (write each section heading clearly):

═══ PAGE 1: EXECUTIVE SUMMARY & THREAT ASSESSMENT ═══
- Overall threat assessment with justification
- The 3 most critical findings from all systems
- What decision-makers need to know in 60 seconds
- Today's information environment in one paragraph

═══ PAGES 2-3: CURRENT INFORMATION ENVIRONMENT ═══
- What System 1 observed across all analytical lenses
- What System 2 found: injection methods, emotional framing, adversary narrative
- The Broken Window: what was NOT being reported
- Who shaped today's information environment and why
- Cui Bono analysis: who benefits from today's narrative pattern

═══ PAGES 4-6: PATTERN INTELLIGENCE (7-DAY DEEP ANALYSIS) ═══
- The pattern that has been forming over the last 7 days
- Event sequence analysis: what order did things happen in?
- What loud event is consuming analytical bandwidth?
- What quiet structural event is happening behind the noise?
- The First Domino: what becomes inevitable if this pattern continues?
- Acceleration analysis: what is speeding up? What is quietly ending?
- Hidden builder analysis: who is gaining structural advantage quietly?
- ACH (Analysis of Competing Hypotheses): what is the strongest evidence that contradicts this pattern?
- Sectarian Trap analysis: is ethnic/religious/political tension being manufactured?

═══ PAGES 7-8: STRUCTURAL ACCUMULATION (30-DAY ANALYSIS) ═══
- What has changed structurally over the past 30 days
- Closing windows: what opportunities are quietly disappearing?
- Silent builders: which actors have been gaining position steadily?
- Injection drift: how has the manipulation pattern evolved over 30 days?
- Manufactured causality: is a false causal chain being repeated as fact?
- Structural First Domino: what structural collision is becoming inevitable?

═══ PAGE 9: HISTORICAL ANALOG — WE HAVE SEEN THIS BEFORE ═══
- The closest historical parallel to current patterns
- What happened in that historical case
- The mechanism: how did it unfold step by step
- What happened to actors who did not recognize the pattern in time
- What the analog predicts for the next 30-90 days
- The critical difference: what is different this time that could change the outcome

═══ PAGE 10: PREDICTIONS, STRATEGIC IMPLICATIONS & GCSP RECOMMENDATIONS ═══
- Active System 4 predictions and their confidence levels
- What Project Lens predicts for the next 30 days
- What Project Lens predicts for the next 90 days
- Strategic implications for democratic governance
- Strategic implications for global institutional actors
- Three specific recommendations for GCSP educators and emerging leaders
- What to watch: the 3 signals that would confirm or deny the current pattern analysis
- Analytical limitations: what we could not determine with current intelligence

Write the full report now. Do not abbreviate. Do not summarize. This is a full analytical document."""

    log.info(f"Prompt built: {len(prompt)} chars")
    return prompt


# ── Sonnet 4.6 call ───────────────────────────────────────────────────────────
def call_sonnet(client, prompt: str) -> Optional[str]:
    for attempt in range(1, 4):
        try:
            log.info(f"Calling {MODEL} (attempt {attempt}/3) — generating 10-page report...")
            msg = client.messages.create(
                model=MODEL,
                max_tokens=MAX_TOKENS,
                messages=[{"role": "user", "content": prompt}]
            )
            report = msg.content[0].text
            log.info(f"Report generated: {len(report)} chars, ~{len(report)//5} words")
            return report
        except Exception as e:
            log.error(f"Sonnet call failed attempt {attempt}: {e}")
            if attempt < 3:
                time.sleep(30)
    return None


# ── PDF generation ────────────────────────────────────────────────────────────
def generate_pdf(report_text: str, output_path: str, date_str: str) -> bool:
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import cm
        from reportlab.lib import colors
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, HRFlowable
        from reportlab.lib.enums import TA_JUSTIFY, TA_CENTER, TA_LEFT

        doc = SimpleDocTemplate(
            output_path,
            pagesize=A4,
            rightMargin=2.5*cm,
            leftMargin=2.5*cm,
            topMargin=2.5*cm,
            bottomMargin=2.5*cm
        )

        styles = getSampleStyleSheet()

        # Custom styles
        title_style = ParagraphStyle(
            'ReportTitle',
            parent=styles['Heading1'],
            fontSize=18,
            fontName='Helvetica-Bold',
            textColor=colors.HexColor('#1a1a2e'),
            spaceAfter=6,
            alignment=TA_CENTER
        )
        subtitle_style = ParagraphStyle(
            'Subtitle',
            parent=styles['Normal'],
            fontSize=11,
            fontName='Helvetica',
            textColor=colors.HexColor('#555555'),
            spaceAfter=20,
            alignment=TA_CENTER
        )
        section_style = ParagraphStyle(
            'Section',
            parent=styles['Heading2'],
            fontSize=13,
            fontName='Helvetica-Bold',
            textColor=colors.HexColor('#1a1a2e'),
            spaceBefore=16,
            spaceAfter=8,
            borderPad=4,
        )
        body_style = ParagraphStyle(
            'Body',
            parent=styles['Normal'],
            fontSize=10,
            fontName='Helvetica',
            leading=16,
            spaceAfter=8,
            alignment=TA_JUSTIFY
        )
        meta_style = ParagraphStyle(
            'Meta',
            parent=styles['Normal'],
            fontSize=8,
            fontName='Helvetica',
            textColor=colors.HexColor('#888888'),
            spaceAfter=4
        )

        story = []

        # Cover
        story.append(Spacer(1, 1*cm))
        story.append(Paragraph("PROJECT LENS", title_style))
        story.append(Paragraph("Deep Intelligence Assessment", subtitle_style))
        story.append(Paragraph(f"Classification: ANALYTICAL | Date: {date_str}", meta_style))
        story.append(Paragraph("Prepared for: GCSP Educators and Emerging Global Leaders", meta_style))
        story.append(Spacer(1, 0.3*cm))
        story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor('#1a1a2e')))
        story.append(Spacer(1, 0.5*cm))

        # Parse and format report
        lines = report_text.split('\n')
        for line in lines:
            line = line.strip()
            if not line:
                story.append(Spacer(1, 0.2*cm))
                continue

            # Section headers (═══)
            if line.startswith('═══') or line.startswith('==='):
                clean = line.replace('═', '').replace('=', '').strip()
                story.append(Spacer(1, 0.3*cm))
                story.append(HRFlowable(width="100%", thickness=0.5,
                                        color=colors.HexColor('#cccccc')))
                story.append(Paragraph(clean, section_style))
                continue

            # Sub-bullets
            if line.startswith('- ') or line.startswith('• '):
                clean = line[2:].strip()
                # Escape HTML chars
                clean = clean.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
                bullet_style = ParagraphStyle(
                    'Bullet',
                    parent=body_style,
                    leftIndent=15,
                    firstLineIndent=-10,
                    spaceBefore=2
                )
                story.append(Paragraph(f"• {clean}", bullet_style))
                continue

            # Bold headers (ALL CAPS lines)
            if line.isupper() and len(line) > 10 and ':' not in line:
                story.append(Paragraph(f"<b>{line}</b>", body_style))
                continue

            # Regular body text
            safe = line.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
            story.append(Paragraph(safe, body_style))

        # Footer
        story.append(Spacer(1, 1*cm))
        story.append(HRFlowable(width="100%", thickness=0.5, color=colors.HexColor('#cccccc')))
        story.append(Paragraph(
            f"Generated by Project Lens · {date_str} · Claude Sonnet 4.6 · GCSP Intelligence Platform",
            meta_style
        ))

        doc.build(story)
        log.info(f"PDF generated: {output_path}")
        return True

    except Exception as e:
        log.error(f"PDF generation failed: {e}")
        return False


# ── Telegram send ─────────────────────────────────────────────────────────────
def send_pdf_telegram(pdf_path: str, date_str: str) -> bool:
    import requests
    token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        log.warning("Telegram credentials missing")
        return False
    try:
        caption = (
            f"📄 <b>PROJECT LENS — Deep Intelligence Report</b>\n"
            f"<i>{date_str}</i>\n\n"
            f"10-page analysis powered by Claude Sonnet 4.6\n"
            f"Covering: Pattern Intelligence · Structural Analysis · "
            f"Historical Analog · S4 Predictions\n\n"
            f"<i>Rate MA report: python code/lens_rate.py 4</i>"
        )
        url = f"https://api.telegram.org/bot{token}/sendDocument"
        with open(pdf_path, 'rb') as f:
            resp = requests.post(url, data={
                "chat_id": chat_id,
                "caption": caption,
                "parse_mode": "HTML"
            }, files={"document": (f"lens_deep_report_{date_str.replace(' ','_')}.pdf", f, "application/pdf")},
            timeout=30)
        if resp.status_code == 200:
            log.info("PDF sent via Telegram ✅")
            return True
        else:
            log.error(f"Telegram send failed: {resp.status_code} {resp.text[:200]}")
            return False
    except Exception as e:
        log.error(f"Telegram PDF send failed: {e}")
        return False


# ── Main ──────────────────────────────────────────────────────────────────────
def run_deep_report() -> dict:
    start = time.time()
    date_str = datetime.now(timezone.utc).strftime("%B %d, %Y %H:%M UTC")
    log.info(f"=== DEEP REPORT START | {date_str} ===")

    try:
        sb     = get_supabase()
        client = get_anthropic()
    except Exception as e:
        log.error(f"Client init failed: {e}")
        return {"status": "ERROR", "error": str(e)}

    # Fetch all intelligence
    log.info("Fetching intelligence from all systems...")
    s3   = fetch_s3_reports(sb)
    s4   = fetch_s4_predictions(sb)
    ma   = fetch_ma_report(sb)
    s2   = fetch_s2_summary(sb)

    log.info(f"Data loaded: S3 positions={list(s3.keys())}, S4 predictions={len(s4)}, MA={'yes' if ma else 'no'}, S2={len(s2)}")

    # Build prompt and call Sonnet 4.6
    prompt = build_deep_prompt(s3, s4, ma, s2)
    report = call_sonnet(client, prompt)

    if not report:
        return {"status": "GENERATION_FAILED"}

    # Generate PDF
    import tempfile
    pdf_path = os.path.join(tempfile.gettempdir(), f"lens_deep_report_{datetime.now(timezone.utc).strftime('%Y%m%d')}.pdf")

    try:
        import reportlab
        pdf_ok = generate_pdf(report, pdf_path, date_str)
    except ImportError:
        log.warning("reportlab not installed — sending text report via Telegram instead")
        pdf_ok = False

    if pdf_ok:
        send_pdf_telegram(pdf_path, date_str)
    else:
        # Fallback: send as text chunks
        import requests
        token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
        if token and chat_id:
            # Send in 4000-char chunks
            chunks = [report[i:i+4000] for i in range(0, len(report), 4000)]
            log.info(f"Sending report as {len(chunks)} text messages")
            for i, chunk in enumerate(chunks[:8]):  # max 8 messages
                requests.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    json={"chat_id": chat_id, "text": chunk},
                    timeout=10
                )
                time.sleep(1)

    elapsed = round(time.time() - start, 1)
    words = len(report.split())
    log.info(f"=== DEEP REPORT COMPLETE | {words} words | {elapsed}s ===")

    return {
        "status":  "OK",
        "words":   words,
        "elapsed": elapsed,
        "pdf":     pdf_ok,
    }


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    result = run_deep_report()
    print(json.dumps(result, indent=2))
