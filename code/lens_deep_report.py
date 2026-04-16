"""
lens_deep_report.py v2
Project Lens — 10-Page Deep Intelligence Report

Changes from v1:
  - DOCX format (via Node.js + docx-js) — cleaner than PDF
  - All 4 systems: S1 + S2 + S3 + S4
  - No markdown artifacts (#, ##, ---, *) in output
  - No page numbers
  - Clean professional Word document

Runs once per day at 05:30 UTC (after 04:28 morning cron completes).
"""

import os, json, logging, time, tempfile, subprocess
from datetime import datetime, timezone, timedelta
from typing import Optional

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [DEEP-REPORT] %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("DEEP_REPORT")

MODEL      = "claude-sonnet-4-6"
MAX_TOKENS = 16000


def get_supabase():
    from supabase import create_client
    return create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])

def get_anthropic():
    import anthropic
    return anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])


# ── Data fetchers ─────────────────────────────────────────────────────────────

def fetch_s1_reports(sb) -> list:
    try:
        r = sb.table("lens_reports") \
            .select("domain_focus,summary,quality_score,cycle,generated_at") \
            .order("generated_at", desc=True).limit(4).execute()
        reports = r.data or []
        log.info(f"S1: {len(reports)} lens reports loaded")
        return reports
    except Exception as e:
        log.warning(f"S1 fetch failed: {e}")
        return []

def fetch_s2_summary(sb) -> list:
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        r = sb.table("injection_reports") \
            .select("analyst,injection_type,confidence_score,flagged_phrases,evidence") \
            .gte("created_at", cutoff) \
            .order("confidence_score", desc=True).limit(10).execute()
        return r.data or []
    except Exception as e:
        log.warning(f"S2 fetch failed: {e}")
        return []

def fetch_s3_reports(sb) -> dict:
    ctx = {}
    for pos in ["S3-A", "S3-B", "S3-D", "S3-C"]:
        try:
            r = sb.table("lens_system3_reports") \
                .select("position,summary,first_domino,patterns_found,quality_score,generated_at") \
                .eq("position", pos).order("generated_at", desc=True).limit(1).execute()
            if r.data:
                ctx[pos] = r.data[0]
                log.info(f"{pos} loaded: {r.data[0].get('generated_at','?')[:16]}")
        except Exception as e:
            log.warning(f"{pos} fetch failed: {e}")
    return ctx

def fetch_s4_predictions(sb) -> list:
    try:
        r = sb.table("lens_predictions") \
            .select("prediction,confidence,verification_date,created_at") \
            .order("created_at", desc=True).limit(5).execute()
        return r.data or []
    except Exception as e:
        log.warning(f"S4 fetch failed: {e}")
        return []

def fetch_ma_report(sb) -> dict:
    try:
        r = sb.table("lens_macro_reports") \
            .select("threat_level,executive_summary,key_findings,adversary_narrative_summary,actors_of_concern,gcsp_implications,intelligence_gaps,cui_bono_synthesis,quality_score,created_at") \
            .order("created_at", desc=True).limit(1).execute()
        return r.data[0] if r.data else {}
    except Exception as e:
        log.warning(f"MA fetch failed: {e}")
        return {}


# ── Prompt builder ────────────────────────────────────────────────────────────

def build_deep_prompt(s1, s2, s3, s4, ma) -> str:
    today = datetime.now(timezone.utc).strftime("%B %d, %Y")

    # S1
    s1_text = "SYSTEM 1 LENS REPORTS (current cycle):\n"
    if s1:
        for r in s1:
            focus = (r.get("domain_focus","") or "")
            summary = (r.get("summary","") or "")[:600]
            q = r.get("quality_score",0) or 0
            s1_text += f"\nLens: {focus} (quality {q:.1f}/10)\n{summary}\n"
    else:
        s1_text += "No S1 data available yet.\n"

    # S2
    s2_text = "SYSTEM 2 INJECTION FINDINGS (last 24h):\n"
    if s2:
        for inj in s2[:6]:
            analyst = inj.get("analyst","?")
            itype   = inj.get("injection_type","?")
            conf    = inj.get("confidence_score",0) or 0
            ev      = inj.get("evidence",{})
            if isinstance(ev, str):
                try: ev = json.loads(ev)
                except: ev = {}
            desc = ""
            if isinstance(ev, dict):
                desc = ev.get("description","") or ev.get("primary_narrative","") or ev.get("q1","")
            s2_text += f"\n[{analyst}] {itype} — confidence {conf:.0%}"
            if desc: s2_text += f"\n  {str(desc)[:300]}"
    else:
        s2_text += "No S2 data in last 24h.\n"

    # S3
    s3a = s3.get("S3-A",{})
    s3b = s3.get("S3-B",{})
    s3d = s3.get("S3-D",{})
    s3c = s3.get("S3-C",{})

    s3_text = "SYSTEM 3 INTELLIGENCE:\n"
    if s3a:
        s3_text += f"\nS3-A PATTERN (7-day, quality {s3a.get('quality_score',0):.2f}):\n"
        s3_text += (s3a.get("summary","") or "")[:800] + "\n"
        dom = s3a.get("first_domino","")
        if dom: s3_text += f"First Domino: {dom}\n"
    if s3d:
        s3_text += f"\nS3-D STRUCTURAL (30-day, quality {s3d.get('quality_score',0):.2f}):\n"
        s3_text += (s3d.get("summary","") or "")[:800] + "\n"
        dom = s3d.get("first_domino","")
        if dom: s3_text += f"Structural First Domino: {dom}\n"
    if s3b:
        s3_text += f"\nS3-B HISTORICAL ANALOG:\n"
        s3_text += (s3b.get("summary","") or "")[:600] + "\n"
    if s3c:
        s3_text += f"\nS3-C BIAS DRIFT (weekly):\n"
        s3_text += (s3c.get("summary","") or "")[:400] + "\n"

    # S4
    s4_text = "SYSTEM 4 PREDICTIONS:\n"
    if s4:
        for p in s4:
            conf = p.get("confidence",0) or 0
            s4_text += f"\nPrediction: {p.get('prediction','?')}\n"
            s4_text += f"Confidence: {conf:.0%} | Verify by: {p.get('verification_date','?')}\n"
    else:
        s4_text += "No predictions recorded yet.\n"

    # MA
    if ma:
        findings = ma.get("key_findings",[]) or []
        if isinstance(findings, str):
            try: findings = json.loads(findings)
            except: findings = []
        f_text = ""
        for f in (findings[:4] if isinstance(findings,list) else []):
            if isinstance(f, dict):
                f_text += f"\n- {f.get('finding','')}: {f.get('significance','')}"
        cui = ma.get("cui_bono_synthesis",{}) or {}
        if isinstance(cui, str):
            try: cui = json.loads(cui)
            except: cui = {}
        ma_text = f"""MISSION ANALYST REPORT:
Threat Level: {ma.get('threat_level','UNKNOWN')}
Quality: {ma.get('quality_score',0):.2f}

Executive Summary:
{ma.get('executive_summary','Not available')}

Key Findings:{f_text}

Adversary Narrative:
{ma.get('adversary_narrative_summary','Not available')}

Cui Bono:
Primary: {cui.get('primary_beneficiary','?') if isinstance(cui,dict) else 'Unknown'}
Convergence: {cui.get('convergence','?') if isinstance(cui,dict) else 'Unknown'}

Intelligence Gaps:
{ma.get('intelligence_gaps','None identified')}
"""
    else:
        ma_text = "MISSION ANALYST: No report available yet.\n"

    prompt = f"""You are the Senior Intelligence Analyst for Project Lens — a deep geopolitical intelligence platform serving GCSP (Geneva Centre for Security Policy) and emerging global leaders.

Today: {today}

CRITICAL FORMATTING RULES — FOLLOW EXACTLY:
1. Do NOT use any markdown syntax. No hashtags (#), no asterisks (*), no dashes (---), no underscores.
2. Write section headers in ALL CAPS followed by a colon and new line.
3. Sub-headers in Title Case followed by a colon and new line.
4. Bullet points: start each item with a dash and space (- item)
5. Write in clear, plain paragraphs. No markdown. No symbols.
6. This will be formatted as a Word document — write clean prose only.

INTELLIGENCE INPUT:
{s1_text}

{s2_text}

{s3_text}

{s4_text}

{ma_text}

WRITE A COMPREHENSIVE 10-PAGE INTELLIGENCE ASSESSMENT with this exact structure:

EXECUTIVE SUMMARY AND THREAT ASSESSMENT:
Write the overall threat level and its justification. Cover the 3 most critical findings. Tell decision-makers what they need to know in 60 seconds. Describe today's information environment in one paragraph. (Target: 600 words)

SYSTEM 1 — WHAT THE CANARY SEES:
Analyze what each of the 4 analytical lenses observed. What is the information environment presenting right now? What signals are strong and which are weak? What is the quality of intelligence this cycle and why? (Target: 500 words)

SYSTEM 2 — HOW THE INFORMATION IS BEING SHAPED:
Analyze each injection finding in depth. What methods are being used? What emotional states are being engineered? What adversary wants you to believe? What coordination was detected? What did System 1 miss — the Broken Window? Who benefits from today's information environment — the full Cui Bono analysis across all S2 positions? (Target: 700 words)

SYSTEM 3 — WHAT IS ACTUALLY BEING BUILT:
The 7-day pattern forming (S3-A deep analysis). The event sequence — what order did things happen? The loud event consuming analytical bandwidth. The quiet structural event behind the noise. The first domino — what becomes inevitable. What is speeding up and what is quietly ending. Who is gaining structural advantage quietly. The ACH check — strongest contradicting evidence. Sectarian trap analysis. Then the 30-day structural accumulation (S3-D). Closing windows. Silent builders. Manufactured causality. The structural first domino. The historical analog from S3-B if available. (Target: 1800 words)

SYSTEM 4 — THE CONSCIENCE:
List all active predictions with confidence levels and verification dates. What does System 4 expect to verify in 30 days? In 90 days? What would confirm the current pattern analysis? What would falsify it? What is the calibration quality of predictions so far? (Target: 400 words)

MISSION ANALYST SYNTHESIS:
The final verdict. Synthesize all four systems into one coherent picture. What is the deeper truth beneath all the noise? What structural transformation is underway? How do the systems agree and where do they diverge? The Cui Bono synthesis from the Mission Analyst — who is the primary beneficiary across all intelligence streams? (Target: 500 words)

STRATEGIC IMPLICATIONS FOR GCSP:
What does this mean for global governance? For emerging leaders? For democratic resilience? What institutional responses are needed? What windows are closing? Three specific recommendations for GCSP educators. Three signals to watch in the next 30 days. (Target: 400 words)

ANALYTICAL LIMITATIONS AND CONFIDENCE ASSESSMENT:
What could not be determined. Where data was incomplete. Where reasoning required judgment beyond evidence. Overall confidence rating for this assessment. What additional intelligence would improve the next assessment. (Target: 300 words)

Write the full report now. Minimum 5000 words. No markdown. No symbols. Plain analytical prose."""

    log.info(f"Prompt built: {len(prompt)} chars")
    return prompt


# ── Sonnet 4.6 call ───────────────────────────────────────────────────────────

def call_sonnet(client, prompt: str) -> Optional[str]:
    for attempt in range(1, 4):
        try:
            log.info(f"Calling {MODEL} attempt {attempt}/3 — generating 10-page report...")
            msg = client.messages.create(
                model=MODEL,
                max_tokens=MAX_TOKENS,
                messages=[{"role": "user", "content": prompt}]
            )
            report = msg.content[0].text
            log.info(f"Report generated: {len(report)} chars, ~{len(report.split())} words")
            return report
        except Exception as e:
            log.error(f"Sonnet call failed attempt {attempt}: {e}")
            if attempt < 3:
                time.sleep(30)
    return None


# ── DOCX generation via Node.js ───────────────────────────────────────────────

def generate_docx(report_text: str, output_path: str, date_str: str) -> bool:
    try:
        # Check node available
        result = subprocess.run(["node", "--version"], capture_output=True, text=True)
        if result.returncode != 0:
            log.warning("Node.js not available — falling back to text")
            return False

        # Install docx if needed
        subprocess.run(["npm", "install", "-g", "docx"], capture_output=True)

        # Parse report into sections
        sections = []
        current_section = None
        current_content = []

        for line in report_text.split('\n'):
            stripped = line.strip()
            if not stripped:
                if current_content:
                    current_content.append("")
                continue

            # Detect ALL CAPS section headers
            words = stripped.rstrip(':').split()
            is_header = (
                len(words) >= 2 and
                stripped.endswith(':') and
                stripped.rstrip(':').replace(' ','').replace('-','').replace('—','').isupper() and
                len(stripped) < 100
            )

            if is_header:
                if current_section:
                    sections.append({"header": current_section, "content": '\n'.join(current_content)})
                current_section = stripped.rstrip(':')
                current_content = []
            else:
                current_content.append(stripped)

        if current_section:
            sections.append({"header": current_section, "content": '\n'.join(current_content)})

        # Write JS generator
        sections_json = json.dumps(sections, ensure_ascii=False)
        date_safe = date_str.replace("'", "\\'")

        js = f"""
const {{ Document, Packer, Paragraph, TextRun, HeadingLevel,
         AlignmentType, LevelFormat, BorderStyle }} = require('docx');
const fs = require('fs');

const sections_data = {sections_json};

const children = [];

// Cover
children.push(new Paragraph({{
  alignment: AlignmentType.CENTER,
  spacing: {{ before: 2880, after: 240 }},
  children: [new TextRun({{ text: 'PROJECT LENS', bold: true, size: 56, font: 'Arial' }})]
}}));
children.push(new Paragraph({{
  alignment: AlignmentType.CENTER,
  spacing: {{ before: 0, after: 480 }},
  children: [new TextRun({{ text: 'Deep Intelligence Assessment', size: 28, font: 'Arial', color: '555555' }})]
}}));
children.push(new Paragraph({{
  alignment: AlignmentType.CENTER,
  spacing: {{ after: 120 }},
  children: [new TextRun({{ text: '{date_safe}', size: 22, font: 'Arial', color: '888888' }})]
}}));
children.push(new Paragraph({{
  alignment: AlignmentType.CENTER,
  spacing: {{ after: 480 }},
  children: [new TextRun({{ text: 'Prepared for: GCSP Educators and Emerging Global Leaders', size: 22, font: 'Arial', color: '888888' }})]
}}));

// Divider line
children.push(new Paragraph({{
  spacing: {{ after: 480 }},
  border: {{ bottom: {{ style: BorderStyle.SINGLE, size: 6, color: '1a1a2e', space: 1 }} }}
}}));

// Sections
for (const sec of sections_data) {{
  // Section header
  children.push(new Paragraph({{
    spacing: {{ before: 480, after: 240 }},
    border: {{ bottom: {{ style: BorderStyle.SINGLE, size: 2, color: 'cccccc', space: 4 }} }},
    children: [new TextRun({{ text: sec.header, bold: true, size: 28, font: 'Arial', color: '1a1a2e' }})]
  }}));

  // Section content
  const lines = sec.content.split('\\n');
  for (const line of lines) {{
    const trimmed = line.trim();
    if (!trimmed) {{
      children.push(new Paragraph({{ spacing: {{ after: 80 }} }}));
      continue;
    }}

    // Bullet items
    if (trimmed.startsWith('- ')) {{
      children.push(new Paragraph({{
        numbering: {{ reference: 'bullets', level: 0 }},
        spacing: {{ after: 80 }},
        children: [new TextRun({{ text: trimmed.slice(2), size: 20, font: 'Arial' }})]
      }}));
      continue;
    }}

    // Sub-headers (Title Case ending with colon, under 80 chars)
    const isSubHeader = trimmed.endsWith(':') && trimmed.length < 80 &&
      !trimmed.includes('  ') && trimmed[0] === trimmed[0].toUpperCase();

    if (isSubHeader) {{
      children.push(new Paragraph({{
        spacing: {{ before: 240, after: 120 }},
        children: [new TextRun({{ text: trimmed, bold: true, size: 22, font: 'Arial', color: '333333' }})]
      }}));
      continue;
    }}

    // Regular body paragraph
    children.push(new Paragraph({{
      spacing: {{ after: 120 }},
      alignment: AlignmentType.JUSTIFIED,
      children: [new TextRun({{ text: trimmed, size: 20, font: 'Arial' }})]
    }}));
  }}
}}

const doc = new Document({{
  numbering: {{
    config: [{{
      reference: 'bullets',
      levels: [{{
        level: 0,
        format: LevelFormat.BULLET,
        text: '•',
        alignment: AlignmentType.LEFT,
        style: {{ paragraph: {{ indent: {{ left: 720, hanging: 360 }} }} }}
      }}]
    }}]
  }},
  sections: [{{
    properties: {{
      page: {{
        size: {{ width: 11906, height: 16838 }},
        margin: {{ top: 1440, right: 1440, bottom: 1440, left: 1440 }}
      }}
    }},
    children: children
  }}]
}});

Packer.toBuffer(doc).then(buffer => {{
  fs.writeFileSync('{output_path}', buffer);
  console.log('DOCX generated: {output_path}');
}}).catch(e => {{
  console.error('DOCX error:', e.message);
  process.exit(1);
}});
"""

        # Write JS file
        js_path = output_path.replace('.docx', '_gen.js')
        with open(js_path, 'w', encoding='utf-8') as f:
            f.write(js)

        # Run Node.js
        result = subprocess.run(
            ["node", js_path],
            capture_output=True, text=True, timeout=60
        )

        if result.returncode == 0 and os.path.exists(output_path):
            log.info(f"DOCX generated: {output_path}")
            os.remove(js_path)
            return True
        else:
            log.error(f"Node.js failed: {result.stderr[:300]}")
            return False

    except Exception as e:
        log.error(f"DOCX generation failed: {e}")
        return False


# ── Telegram send ─────────────────────────────────────────────────────────────

def send_docx_telegram(docx_path: str, date_str: str, word_count: int) -> bool:
    import requests
    token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        log.warning("Telegram credentials missing")
        return False
    try:
        caption = (
            f"📄 PROJECT LENS — Deep Intelligence Report\n"
            f"{date_str}\n\n"
            f"Systems: S1 Canary + S2 Immune + S3 Memory + S4 Conscience\n"
            f"Model: Claude Sonnet 4.6 | Words: ~{word_count:,}\n"
            f"Format: Word document (.docx)\n\n"
            f"Rate: python code/lens_rate.py 4"
        )
        fname = f"ProjectLens_Intel_{date_str.replace(' ','_').replace(':','').replace(',','')}.docx"
        url = f"https://api.telegram.org/bot{token}/sendDocument"
        with open(docx_path, 'rb') as f:
            resp = requests.post(url,
                data={"chat_id": chat_id, "caption": caption},
                files={"document": (fname, f, "application/vnd.openxmlformats-officedocument.wordprocessingml.document")},
                timeout=60)
        if resp.status_code == 200:
            log.info("DOCX sent via Telegram ✅")
            return True
        else:
            log.error(f"Telegram failed: {resp.status_code} {resp.text[:200]}")
            return False
    except Exception as e:
        log.error(f"Telegram DOCX send failed: {e}")
        return False


# ── Main ──────────────────────────────────────────────────────────────────────

def run_deep_report() -> dict:
    start    = time.time()
    date_str = datetime.now(timezone.utc).strftime("%B %d, %Y %H:%M UTC")
    log.info(f"=== DEEP REPORT v2 START | {date_str} ===")

    try:
        sb     = get_supabase()
        client = get_anthropic()
    except Exception as e:
        return {"status": "ERROR", "error": str(e)}

    log.info("Fetching all 4 systems...")
    s1 = fetch_s1_reports(sb)
    s2 = fetch_s2_summary(sb)
    s3 = fetch_s3_reports(sb)
    s4 = fetch_s4_predictions(sb)
    ma = fetch_ma_report(sb)

    log.info(f"S1={len(s1)} lenses | S2={len(s2)} injections | S3={list(s3.keys())} | S4={len(s4)} predictions | MA={'yes' if ma else 'no'}")

    prompt = build_deep_prompt(s1, s2, s3, s4, ma)
    report = call_sonnet(client, prompt)
    if not report:
        return {"status": "GENERATION_FAILED"}

    word_count = len(report.split())

    # Generate DOCX
    docx_path = os.path.join(tempfile.gettempdir(),
        f"lens_deep_{datetime.now(timezone.utc).strftime('%Y%m%d')}.docx")

    docx_ok = generate_docx(report, docx_path, date_str)

    if docx_ok:
        send_docx_telegram(docx_path, date_str, word_count)
    else:
        # Fallback: plain text chunks
        import requests
        token   = os.environ.get("TELEGRAM_BOT_TOKEN","")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID","")
        if token and chat_id:
            log.info("DOCX failed — sending as text chunks")
            chunks = [report[i:i+4000] for i in range(0, min(len(report), 32000), 4000)]
            for chunk in chunks:
                requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                    json={"chat_id": chat_id, "text": chunk}, timeout=10)
                time.sleep(1)

    elapsed = round(time.time() - start, 1)
    log.info(f"=== DEEP REPORT COMPLETE | {word_count} words | {elapsed}s ===")
    return {"status": "OK", "words": word_count, "elapsed": elapsed, "docx": docx_ok}


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    result = run_deep_report()
    print(json.dumps(result, indent=2))
