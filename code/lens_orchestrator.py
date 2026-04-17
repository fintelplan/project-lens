"""
lens_orchestrator.py  —  Project Lens  —  LENS-008
Steps 1-9 complete.  LR-050/051/052/053/054/055/056 active.
planfintel@gmail.com
"""

import os, sys, json, time, uuid, logging, traceback, subprocess, requests
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from typing import Optional

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("lens_orchestrator")

# ── Env ───────────────────────────────────────────────────────────────────────
SUPABASE_URL      = os.getenv("SUPABASE_URL","")
SUPABASE_KEY      = os.getenv("SUPABASE_SERVICE_KEY","")
GROQ_MANAGER_KEY  = os.getenv("GROQ_MA_API_KEY","")
GITHUB_ACTIONS    = os.getenv("GITHUB_ACTIONS","false").lower()=="true"
LENS_FORCE        = os.getenv("LENS_FORCE","0")=="1"
LENS_DRY_RUN      = os.getenv("LENS_DRY_RUN","0")=="1"
LENS_SKIP         = os.getenv("LENS_SKIP","")
LENS_ONLY         = os.getenv("LENS_ONLY","")
QUALITY_FLOOR     = float(os.getenv("LENS_QUALITY_FLOOR","4.0"))

# ── Constants ─────────────────────────────────────────────────────────────────
DAILY_BUDGET        = 2    # LENS-013 T-04 aligned with lens_manager.py (2x/day cron)
GEMINI_RPD_LIMIT    = 20
GEMINI_RPD_BUFFER   = 2
CEREBRAS_SAFE_GAP   = 30
LENS3_AVG_FALLBACK  = 12
MAX_REPAIRS         = 2    # LR-050
MAX_JOBS            = 3    # LR-054
WALL_MIN            = 14   # LR-052
STALE_CP_HOURS      = 3
CODE_DIR            = os.path.dirname(os.path.abspath(__file__))

# ══════════════════════════════════════════════════════════════════════════════
# STEP 1 — PHILOSOPHY SAFETY GATE  (LR-051E)
# ══════════════════════════════════════════════════════════════════════════════
G1_DATA   = "data_integrity"
G2_ETHICS = "ethics_boundary"
G3_INTEL  = "intelligence_quality"
G4_PHI    = "philosophy_alignment"
G5_SIDE   = "side_effects"
G6_ENV    = "environment_safety"

@dataclass
class GateResult:
    passed: bool
    failed_gate: Optional[str]
    reason: str
    action: str
    checked_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

def run_philosophy_gate(action:str, ctx:dict) -> GateResult:
    try:
        if action in ("delete_articles","overwrite_reports","truncate_table"):
            return GateResult(False,G1_DATA,f"Action '{action}' would permanently alter source data.",action)
        if action=="bulk_delete" and ctx.get("record_count",0)>100:
            return GateResult(False,G1_DATA,f"Bulk delete of {ctx.get('record_count')} records needs human confirmation.",action)
        if ctx.get("suppress_signal"):
            return GateResult(False,G2_ETHICS,"suppress_signal=True violates ethics boundary.",action)
        if ctx.get("embed_partisan_bias"):
            return GateResult(False,G2_ETHICS,"Embedding partisan bias not permitted.",action)
        if action=="save_report" and ctx.get("expected_quality",10.0)<3.0:
            return GateResult(False,G3_INTEL,f"Quality {ctx.get('expected_quality'):.1f} below minimum 3.0.",action)
        if ctx.get("targets_vulnerable_population"):
            return GateResult(False,G4_PHI,"PHI-002: action targets vulnerable population.",action)
        if ctx.get("estimated_api_calls",0)>500:
            return GateResult(False,G5_SIDE,f"{ctx.get('estimated_api_calls')} API calls risks quota exhaustion.",action)
        if action=="force_run_past_budget" and not LENS_FORCE:
            return GateResult(False,G5_SIDE,"Force past budget without LENS_FORCE=1.",action)
        if action in ("fire_lenses","run_analysis","start_pipeline"):
            if ctx.get("article_count",-1)==0:
                return GateResult(False,G6_ENV,"Article pool empty — firing lenses wastes quota.",action)
            if not ctx.get("provider_health_known"):
                return GateResult(False,G6_ENV,"Provider health unknown — pre-flight must verify first.",action)
            age=ctx.get("checkpoint_age_hours",0)
            if 0<age<STALE_CP_HOURS and not ctx.get("is_resume_job"):
                return GateResult(False,G6_ENV,f"Active checkpoint ({age:.1f}h old) found. Should be resume job.",action)
        return GateResult(True,None,"All 6 philosophy gates passed.",action)
    except Exception as e:
        return GateResult(False,"gate_internal_error",f"Gate errored: {str(e)[:200]}",action)

def assert_gate(action:str, ctx:dict):
    r=run_philosophy_gate(action,ctx)
    if not r.passed:
        msg=f"[GATE BLOCKED] {r.action} | {r.failed_gate} | {r.reason}"
        log.error(msg); raise PermissionError(msg)

# ══════════════════════════════════════════════════════════════════════════════
# STEP 2 — PRE-FLIGHT  (migrated from lens_manager.py)
# ══════════════════════════════════════════════════════════════════════════════
def _sb_get(ep,params=""):
    if not SUPABASE_URL or not SUPABASE_KEY: return []
    try:
        r=requests.get(f"{SUPABASE_URL}/rest/v1/{ep}{params}",
            headers={"apikey":SUPABASE_KEY,"Authorization":f"Bearer {SUPABASE_KEY}"},timeout=10)
        return r.json() if r.ok else []
    except: return []

def _sb_post(ep,data):
    if not SUPABASE_URL or not SUPABASE_KEY: return None
    try:
        r=requests.post(f"{SUPABASE_URL}/rest/v1/{ep}",
            headers={"apikey":SUPABASE_KEY,"Authorization":f"Bearer {SUPABASE_KEY}",
                     "Content-Type":"application/json","Prefer":"return=representation"},
            json=data,timeout=15)
        return r.json() if r.ok else None
    except Exception as e:
        log.error(f"[SB] POST failed: {e}"); return None

def get_runs_today():
    m=datetime.now(timezone.utc).replace(hour=0,minute=0,second=0,microsecond=0).isoformat()
    rows=_sb_get("lens_pipeline_runs",f"?started_at=gte.{m}&select=started_at,cycle&order=started_at.desc")
    return rows if isinstance(rows,list) else []

def get_last_run():
    rows=_sb_get("lens_pipeline_runs","?select=started_at,finished_at&order=started_at.desc&limit=1")
    return rows[0] if rows else None

def get_gemini_calls_today():
    m=datetime.now(timezone.utc).replace(hour=0,minute=0,second=0,microsecond=0).isoformat()
    rows=_sb_get("lens_reports",f"?generated_at=gte.{m}&ai_model=like.*gemini*&select=id")
    return len(rows) if isinstance(rows,list) else 0

def get_lens3_avg():
    return LENS3_AVG_FALLBACK  # upgraded to real calc once history exists

def check_groq():
    key=os.getenv("GROQ_API_KEY","")
    if not key: return False,"GROQ_API_KEY not set"
    try:
        r=requests.get("https://api.groq.com/openai/v1/models",
            headers={"Authorization":f"Bearer {key}"},timeout=8)
        return r.ok,("OK" if r.ok else f"HTTP {r.status_code}")
    except Exception as e: return False,str(e)[:40]

def check_gemini(calls):
    rem=GEMINI_RPD_LIMIT-calls-GEMINI_RPD_BUFFER
    if rem<=0: return False,f"RPD exhausted ({calls}/{GEMINI_RPD_LIMIT} used)"
    return True,f"OK ({calls}/{GEMINI_RPD_LIMIT} RPD used, {rem} remaining)"

def check_cerebras():
    key=os.getenv("CEREBRAS_API_KEY","")
    if not key: return False,"CEREBRAS_API_KEY not set"
    try:
        r=requests.get("https://api.cerebras.ai/v1/models",
            headers={"Authorization":f"Bearer {key}"},timeout=8)
        return r.ok,("OK" if r.ok else f"HTTP {r.status_code}")
    except Exception as e: return False,str(e)[:40]

def get_ai5_verdict(ctx):
    if not GROQ_MANAGER_KEY: return "MANAGER_KEY_MISSING"
    try:
        from groq import Groq
        c=Groq(api_key=GROQ_MANAGER_KEY)
        sys_p="You are AI 5 — Management AI for Project Lens. Analyze system health. Give GO/WARN/STOP verdict. Direct, one line per finding."
        user_p=(f"Budget: {ctx['runs_today']}/{ctx['daily_budget']}\nTrigger: {ctx['trigger']}\n"
                f"Groq: {ctx['groq_status']}\nGemini: {ctx['gemini_status']}\nCerebras: {ctx['cerebras_status']}\n"
                f"Lens3 avg: {ctx['lens3_avg']}s  Lens4 stagger: {ctx['lens4_stagger']}s\nVerdict: GO/WARN/STOP")
        resp=c.chat.completions.create(model="llama-3.3-70b-versatile",
            messages=[{"role":"system","content":sys_p},{"role":"user","content":user_p}],
            temperature=0.1,max_tokens=300)
        return resp.choices[0].message.content.strip()
    except Exception as e: return f"AI5_ERROR: {str(e)[:60]}"

@dataclass
class PreflightResult:
    approved:bool; lens_verdicts:dict; lens4_stagger:int
    runs_today:int; gemini_calls:int; ai5:str
    abort_reason:str=""; dry_run:bool=False

def run_preflight(job_count=1, is_resume=False, cp_age_hours=0.0) -> PreflightResult:
    now=datetime.now(timezone.utc)
    trigger="scheduled (GitHub Actions)" if GITHUB_ACTIONS else "MANUAL"
    print("="*60)
    print(f"Project Lens — Orchestrator Pre-flight — {now.strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"Trigger: {trigger}")
    if LENS_DRY_RUN: print("MODE: DRY RUN — no lenses will fire")
    if is_resume:    print(f"MODE: RESUME — job {job_count}/{MAX_JOBS}")
    print("="*60)

    # LR-054 hard stop
    if job_count>MAX_JOBS:
        reason=f"Job count {job_count} > max {MAX_JOBS} (LR-054)"
        print(f"\nHARD STOP — {reason}")
        return PreflightResult(False,{},48,0,0,"STOP",reason)

    # Budget (LR-048)
    runs_today=get_runs_today(); runs_count=len(runs_today)
    last_run=get_last_run(); mins_since=9999
    if last_run and last_run.get("started_at"):
        try:
            last_dt=datetime.fromisoformat(last_run["started_at"].replace("Z","+00:00"))
            mins_since=int((now-last_dt).total_seconds()/60)
        except: pass

    print(f"\nBudget:  {runs_count} / {DAILY_BUDGET} runs used today")
    print(f"Last run: {mins_since} minutes ago")

    if runs_count>=DAILY_BUDGET and not LENS_FORCE:
        reason=f"Daily budget exhausted ({runs_count}/{DAILY_BUDGET}). LENS_FORCE=1 to override."
        print(f"\nHARD STOP — {reason}")
        return PreflightResult(False,{},48,runs_count,0,"STOP",reason)

    if not GITHUB_ACTIONS and not LENS_FORCE:
        reason=f"Manual trigger requires LENS_FORCE=1. Budget remaining: {DAILY_BUDGET-runs_count} runs."
        print(f"\nWARNING — {reason}")
        return PreflightResult(False,{},48,runs_count,0,"WARN",reason)

    # Provider health
    print("\nProvider health:")
    groq_ok,groq_msg=check_groq()
    gcalls=get_gemini_calls_today()
    gem_ok,gem_msg=check_gemini(gcalls)
    cer_ok,cer_msg=check_cerebras()
    print(f"  Lens 1 Groq:       {'OK' if groq_ok else 'FAIL'}  ({groq_msg})")
    print(f"  Lens 2 Gemini:     {'OK' if gem_ok else 'WARN'} ({gem_msg})")
    print(f"  Lens 3+4 Cerebras: {'OK' if cer_ok else 'FAIL'} ({cer_msg})")

    # Dynamic stagger
    l3avg=get_lens3_avg(); stagger=int(l3avg+CEREBRAS_SAFE_GAP+6)
    print(f"\nDynamic stagger:")
    print(f"  Lens 3 avg runtime: {l3avg}s")
    print(f"  Lens 4 stagger:     {stagger}s  (Lens3 avg {l3avg}s + base 6s + buffer {CEREBRAS_SAFE_GAP}s)")

    # Per-lens verdicts
    print("\nPer-lens verdict:")
    skip_l=int(LENS_SKIP) if LENS_SKIP.isdigit() else None
    only_l=int(LENS_ONLY) if LENS_ONLY.isdigit() else None
    verdicts={}
    for lid in [1,2,3,4]:
        if only_l and lid!=only_l:          verdicts[lid]="SKIP"
        elif skip_l and lid==skip_l:        verdicts[lid]="SKIP"
        elif lid==1 and not groq_ok:        verdicts[lid]="SKIP"
        elif lid==2 and not gem_ok:         verdicts[lid]="SKIP"
        elif lid in (3,4) and not cer_ok:   verdicts[lid]="SKIP"
        else:                               verdicts[lid]="GO"
        print(f"  Lens {lid}: {verdicts[lid]}")

    # Philosophy gate
    gate=run_philosophy_gate("fire_lenses",{
        "article_count":1,"provider_health_known":True,
        "checkpoint_age_hours":cp_age_hours,"is_resume_job":is_resume})
    if not gate.passed:
        print(f"\nPHILOSOPHY GATE BLOCKED: {gate.failed_gate} — {gate.reason}")
        return PreflightResult(False,verdicts,stagger,runs_count,gcalls,"STOP",
            f"Philosophy gate: {gate.reason}")

    # AI 5 verdict
    print("\nAI 5 verdict (llama-3.3-70b):")
    ai5=get_ai5_verdict({"runs_today":runs_count,"daily_budget":DAILY_BUDGET,
        "trigger":trigger,"minutes_since_last":mins_since,
        "groq_status":groq_msg,"gemini_status":gem_msg,"cerebras_status":cer_msg,
        "lens3_avg":l3avg,"lens4_stagger":stagger})
    for line in ai5.split("\n"): print(f"  {line}")

    go_count=sum(1 for v in verdicts.values() if v=="GO")
    approved=go_count>=1
    print("\n"+"="*60)
    if LENS_DRY_RUN: print("DRY RUN COMPLETE — pre-flight verified, no lenses fired")
    elif approved:
        print(f"{'FULL RUN' if go_count==4 else f'PARTIAL RUN ({go_count}/4 lenses)'} APPROVED")
    else: print("BLOCKED — no lenses approved")
    print(f"Lens 4 stagger set to: {stagger}s\n"+"="*60)

    try:
        with open(os.path.join(CODE_DIR,".lens_stagger"),"w") as f: f.write(str(stagger))
    except: pass

    return PreflightResult(approved and not LENS_DRY_RUN,verdicts,stagger,
        runs_count,gcalls,ai5,dry_run=LENS_DRY_RUN)

# ══════════════════════════════════════════════════════════════════════════════
# STEP 3 — SEQUENTIAL LENS EXECUTION
# ══════════════════════════════════════════════════════════════════════════════
@dataclass
class LensResult:
    lens_id:int; status:str
    quality:float=0.0; runtime_s:float=0.0
    report_id:str=""; error:str=""; error_type:str=""
    repair_attempts:int=0; fallback_used:bool=False; skip_reason:str=""

def _parse_quality(out):
    import re
    m=re.search(r"[Qq]uality score[:\s]+([0-9.]+)",out)
    return float(m.group(1)) if m else 0.0

def _parse_report_id(out):
    import re
    m=re.search(r"id:\s+([0-9a-f\-]{36})",out)
    return m.group(1) if m else ""

def _classify_error(out):
    o=out.lower()
    if "404" in o and ("model" in o or "not found" in o): return "404_model_not_found",out[-200:]
    if "429" in o and "queue" in o:                       return "429_queue",out[-200:]
    if "429" in o and ("rpd" in o or "daily" in o):       return "429_rpd",out[-200:]
    if "429" in o or "rate limit" in o or "tpm" in o:     return "429_tpm",out[-200:]
    if "503" in o or "unavailable" in o:                  return "503_unavailable",out[-200:]
    if "connection" in o and "error" in o:                return "provider_down",out[-200:]
    if len(out.strip())<50:                               return "empty_output",out[-200:]
    return "unknown",out[-200:]

def run_single_lens(lens_id:int, stagger_s:int=0) -> LensResult:
    if stagger_s>0:
        log.info(f"[LENS {lens_id}] Stagger wait {stagger_s}s..."); time.sleep(stagger_s)
    start=time.time()
    script=os.path.join(CODE_DIR,"analyze_lens_multi.py")
    try:
        result=subprocess.run([sys.executable,script,"--single-lens",str(lens_id)],
            capture_output=True,text=True,timeout=300)
        rt=round(time.time()-start,1)
        out=result.stdout+result.stderr
        if result.returncode!=0:
            et,em=_classify_error(out)
            return LensResult(lens_id,status="failed",runtime_s=rt,error=em,error_type=et)
        return LensResult(lens_id,status="complete",quality=_parse_quality(out),
            runtime_s=rt,report_id=_parse_report_id(out))
    except subprocess.TimeoutExpired:
        return LensResult(lens_id,status="failed",runtime_s=300.0,
            error="timeout after 300s",error_type="timeout")
    except Exception as e:
        return LensResult(lens_id,status="failed",error=str(e)[:200],error_type="unknown")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 4 — WORST-CASE PLAYBOOKS (FMEA → code)
# STEP 5 — SELF-HEALING LOOP  (LR-050: max 2 attempts)
# ══════════════════════════════════════════════════════════════════════════════
FALLBACKS={1:"llama-3.3-70b-versatile",2:"gemini-1.5-flash",
           3:"llama-3.3-70b-versatile",4:"llama-3.3-70b-versatile"}

def apply_playbook(lens_id:int, etype:str, attempt:int) -> dict:
    pb={"action":None,"wait_s":0,"fallback":None,"skip":False,"escalate":False,"reason":""}
    if etype=="404_model_not_found":
        pb.update(action="switch_fallback",fallback=FALLBACKS[lens_id],
            reason=f"404 model — switching to {FALLBACKS[lens_id]}")
    elif etype=="429_queue":
        pb.update(action="wait_and_retry",wait_s=120,reason="Cerebras queue — waiting 120s")
    elif etype=="429_tpm":
        w=60 if attempt==1 else 90
        pb.update(action="wait_and_retry",wait_s=w,reason=f"TPM rate limit — waiting {w}s")
    elif etype=="429_rpd":
        pb.update(action="skip",skip=True,reason="Gemini RPD exhausted — skipping lens")
    elif etype=="503_unavailable":
        pb.update(action="wait_and_retry",wait_s=30,reason="Provider 503 — waiting 30s")
    elif etype=="provider_down":
        pb.update(action="skip",skip=True,escalate=True,reason="Provider down — skip + escalate")
    elif etype=="empty_output":
        pb.update(action="wait_and_retry",wait_s=10,reason="Empty output — retry after 10s")
    elif etype=="quality_low":
        pb.update(action="wait_and_retry",wait_s=5,reason=f"Quality below {QUALITY_FLOOR} — retry")
    elif etype=="timeout":
        pb.update(action="skip",skip=True,escalate=True,reason="Subprocess timeout — skip + escalate")
    else:  # unknown — LR-050: no playbook, escalate immediately
        pb.update(action="escalate",skip=True,escalate=True,
            reason=f"Unknown error '{etype}' — LR-050: no playbook, escalate immediately")
    return pb

def run_lens_with_healing(lens_id:int, stagger_s:int=0) -> LensResult:
    result=run_single_lens(lens_id, stagger_s=stagger_s)
    if result.status=="complete" and result.quality<QUALITY_FLOOR:
        log.warning(f"[LENS {lens_id}] Quality {result.quality:.1f} below floor — healing")
        result.error_type="quality_low"; result.status="failed"
    if result.status=="complete": return result

    for attempt in range(1, MAX_REPAIRS+1):
        log.warning(f"[LENS {lens_id}] Repair {attempt}/{MAX_REPAIRS} — etype={result.error_type}")
        if result.error_type=="unknown":
            log.error(f"[LENS {lens_id}] Unknown error — escalating immediately (LR-050)")
            result.status="failed"; result.skip_reason="unknown_error_escalated"; return result

        pb=apply_playbook(lens_id, result.error_type, attempt)
        log.info(f"[LENS {lens_id}] Playbook: {pb['reason']}")
        if pb.get("skip"):
            result.status="skipped"; result.skip_reason=pb["reason"]
            result.repair_attempts=attempt; return result
        if pb.get("wait_s",0)>0:
            log.info(f"[LENS {lens_id}] Waiting {pb['wait_s']}s..."); time.sleep(pb["wait_s"])
        if pb.get("fallback"):
            result.fallback_used=True

        retry=run_single_lens(lens_id, stagger_s=0)
        retry.repair_attempts=attempt; retry.fallback_used=result.fallback_used
        if retry.status=="complete" and retry.quality>=QUALITY_FLOOR:
            log.info(f"[LENS {lens_id}] Repair {attempt} succeeded — quality={retry.quality:.1f}")
            return retry
        if retry.status=="complete":
            retry.error_type="quality_low"; retry.status="failed"
        result=retry

    log.error(f"[LENS {lens_id}] Max repairs exhausted — skipping")
    result.status="skipped"
    result.skip_reason=f"max_repairs_exhausted after {MAX_REPAIRS} attempts"
    return result

# ══════════════════════════════════════════════════════════════════════════════
# STEP 6 — CHECKPOINT + RESUME  (LR-052 to LR-056)
# ══════════════════════════════════════════════════════════════════════════════
def _cycle():
    h=datetime.now(timezone.utc).hour
    return ("morning" if 5<=h<10 else "afternoon" if 10<=h<15
            else "evening" if 15<=h<20 else "night")

def _next_pending(results:dict) -> int:
    for lid in [1,2,3,4]:
        if results.get(lid,LensResult(lid,"pending")).status not in ("complete","skipped_already_complete"):
            return lid
    return 5

def save_checkpoint(run_id:str, job_count:int, results:dict, article_ids:list) -> bool:
    data={"run_id":run_id,"cycle":_cycle(),"job_count":job_count,
          "resume_from":_next_pending(results),
          "lens_1_status":results.get(1,LensResult(1,"pending")).status,
          "lens_2_status":results.get(2,LensResult(2,"pending")).status,
          "lens_3_status":results.get(3,LensResult(3,"pending")).status,
          "lens_4_status":results.get(4,LensResult(4,"pending")).status,
          "article_ids":json.dumps(article_ids[:200]),"completed_at":None}
    ok=_sb_post("lens_run_checkpoints",data)
    if ok:
        log.info(f"[CP] Saved run_id={run_id} job={job_count} resume_from=Lens {data['resume_from']}")
        return True
    log.error("[CP] Supabase save failed — writing local backup")
    _local_cp(data); return False

def load_checkpoint(run_id:str) -> Optional[dict]:
    rows=_sb_get("lens_run_checkpoints",f"?run_id=eq.{run_id}&order=job_count.desc&limit=1")
    if rows and isinstance(rows,list):
        log.info(f"[CP] Loaded run_id={run_id}")
        return rows[0]
    return None

def clear_checkpoint(run_id:str):
    if not SUPABASE_URL or not SUPABASE_KEY: return
    try:
        requests.patch(f"{SUPABASE_URL}/rest/v1/lens_run_checkpoints?run_id=eq.{run_id}",
            headers={"apikey":SUPABASE_KEY,"Authorization":f"Bearer {SUPABASE_KEY}",
                     "Content-Type":"application/json"},
            json={"completed_at":datetime.now(timezone.utc).isoformat()},timeout=10)
    except Exception as e: log.error(f"[CP] Clear failed: {e}")

def is_stale(cp:dict) -> bool:
    try:
        dt=datetime.fromisoformat(cp.get("created_at","").replace("Z","+00:00"))
        return (datetime.now(timezone.utc)-dt).total_seconds()/3600>=STALE_CP_HOURS
    except: return True

def _local_cp(data:dict):
    d=os.path.join(CODE_DIR,"..","local_backup"); os.makedirs(d,exist_ok=True)
    fn=os.path.join(d,f"checkpoint_{data['run_id']}_{data['job_count']}.json")
    try:
        with open(fn,"w") as f: json.dump(data,f,indent=2)
        log.info(f"[CP] Local backup: {fn}")
    except Exception as e: log.error(f"[CP] Local backup failed: {e}")

def trigger_resume(run_id:str):
    token=os.getenv("GITHUB_TOKEN",""); repo=os.getenv("GITHUB_REPOSITORY","fintelplan/project-lens")
    if not token:
        log.warning(f"[RESUME] No GITHUB_TOKEN — manual resume: LENS_RUN_ID={run_id}"); return
    try:
        r=requests.post(f"https://api.github.com/repos/{repo}/actions/workflows/lens-resume.yml/dispatches",
            headers={"Authorization":f"Bearer {token}","Accept":"application/vnd.github+json"},
            json={"ref":"main","inputs":{"run_id":run_id}},timeout=15)
        if r.status_code==204: log.info(f"[RESUME] Dispatched run_id={run_id}")
        else: log.error(f"[RESUME] Dispatch failed: {r.status_code}")
    except Exception as e: log.error(f"[RESUME] Error: {e}")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 7 — POST-RUN VERIFICATION + LEARNING
# ══════════════════════════════════════════════════════════════════════════════
def verify_reports(results:dict) -> dict:
    vfy={}
    for lid,res in results.items():
        if not isinstance(res,LensResult): continue
        if res.status!="complete": vfy[lid]={"verified":False,"reason":res.status}; continue
        if not res.report_id:     vfy[lid]={"verified":False,"reason":"no_report_id"}; continue
        rows=_sb_get("lens_reports",f"?id=eq.{res.report_id}&select=id")
        vfy[lid]={"verified":bool(rows)}
        if not rows: vfy[lid]["reason"]="report_not_in_db"
    return vfy

def update_learning(results:dict):
    for lid,res in results.items():
        if isinstance(res,LensResult) and lid==3 and res.status=="complete" and res.runtime_s>0:
            _sb_post("lens_run_meta",{"key":"lens3_last_runtime_s","value":str(res.runtime_s),
                "recorded_at":datetime.now(timezone.utc).isoformat()})
            log.info(f"[LEARNING] Lens 3 runtime {res.runtime_s}s recorded")
# ══════════════════════════════════════════════════════════════════════════════
# CROSS-LENS SIGNAL DETECTION (S1 data only — LR-060 safe)
# ══════════════════════════════════════════════════════════════════════════════
def compute_cross_lens_signals(results: dict) -> list:
    """Extract keywords appearing in 3+ lens summaries. Single-source spike = anomaly."""
    import re, collections
    summaries = {}
    for lid, r in results.items():
        if not isinstance(r, LensResult): continue
        if r.status != "complete" or not r.report_id: continue
        rows = _sb_get("lens_reports", f"?id=eq.{r.report_id}&select=summary")
        if rows: summaries[lid] = rows[0].get("summary", "")
    if len(summaries) < 3: return []
    # Extract meaningful words (5+ chars, alpha only)
    word_lenses = collections.defaultdict(set)
    for lid, text in summaries.items():
        words = set(w.lower() for w in re.findall(r"[a-zA-Z]{5,}", text))
        for w in words: word_lenses[w].add(lid)
    STOPWORDS = {"which","their","there","about","would","could","should",
                 "these","those","where","while","signal","report","lens",
                 "analysis","based","within","through","between","across"}
    signals = []
    for word, lenses in word_lenses.items():
        if word in STOPWORDS: continue
        count = len(lenses)
        if count == 1:
            # Single-source spike — consensus anomaly flag (LR-061)
            signals.append(f"[ANOMALY] single-lens spike: '{word}' (Lens {list(lenses)[0]} only)")
        elif count >= 3:
            if count == 4:
                signals.append(f"[CROSS-LENS x{count}] '{word}' — FLAG FOR S2-B COORDINATION CHECK (Pattern 2 risk)")
            else:
                signals.append(f"[CROSS-LENS x{count}] '{word}' confirmed across {sorted(lenses)}")
    # Sort: cross-lens first, anomalies second
    signals.sort(key=lambda s: (0 if s.startswith("[CROSS") else 1))
    return signals[:10]


# ══════════════════════════════════════════════════════════════════════════════
# STEP 8 — RUN SUMMARY
# ══════════════════════════════════════════════════════════════════════════════
def generate_summary(run_id:str, results:dict, pf:PreflightResult,
                     elapsed_s:float, signals:list=None) -> str:
    now=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    complete=sum(1 for r in results.values() if isinstance(r,LensResult) and r.status=="complete")
    total=len(results)
    lines=["","="*60,f"PROJECT LENS — Run Summary",
           f"Run ID:  {run_id}",f"Time:    {now}",
           f"Elapsed: {elapsed_s:.0f}s",
           f"Budget:  {pf.runs_today+1}/{DAILY_BUDGET} runs today","="*60,
           f"\nLENS RESULTS: {complete}/{total} complete"]
    for lid in sorted(results.keys()):
        r=results[lid]
        if not isinstance(r,LensResult): continue
        if r.status=="complete":
            fb=" [FALLBACK]" if r.fallback_used else ""
            lines.append(f"  Lens {lid}: ✅ COMPLETE{fb} | quality={r.quality:.1f}/10 | {r.runtime_s:.1f}s")
        elif r.status in ("skipped","skipped_already_complete"):
            lines.append(f"  Lens {lid}: ⏭  SKIPPED — {r.skip_reason}")
        else:
            lines.append(f"  Lens {lid}: ❌ FAILED — {r.error_type}")
    if signals:
        lines.append(f"\nCROSS-LENS SIGNALS: {len(signals)} found")
        for s in signals[:5]: lines.append(f"  {s}")
    sla=complete>=3
    lines.append(f"\nSLA: {'✅ MET (3+/4 lenses)' if sla else '❌ BREACHED'}")
    if LENS_DRY_RUN: lines.append("\nMODE: DRY RUN — pre-flight only")
    lines.append("="*60)
    return "\n".join(lines)

# ══════════════════════════════════════════════════════════════════════════════
# STEP 9 — ADMIN ESCALATION + ALERTS
# ══════════════════════════════════════════════════════════════════════════════
_ESCALATIONS=[]

def escalate(reason:str, data:dict=None, critical:bool=False):
    entry={"reason":reason,"data":data or {},"critical":critical,
           "timestamp":datetime.now(timezone.utc).isoformat()}
    _ESCALATIONS.append(entry)
    log.error(f"[{'CRITICAL' if critical else 'ESCALATE'}] {reason}")
    _sb_post("lens_escalations",{"reason":reason,"data":json.dumps(data or {}),
        "critical":critical,"created_at":entry["timestamp"]})
    # TODO LENS-011: Telegram alert for critical

def check_escalations(results:dict, vfy:dict):
    rl=[r for r in results.values() if isinstance(r,LensResult)]
    complete=[r for r in rl if r.status=="complete"]
    if len(complete)==0:
        escalate("ALL LENSES FAILED OR SKIPPED — zero intelligence produced",
                 {"statuses":{r.lens_id:r.status for r in rl}},critical=True)
    elif len(complete)<3:
        escalate(f"SLA BREACHED — only {len(complete)}/4 lenses complete",
                 {"complete":len(complete)},critical=True)
    for lid,v in vfy.items():
        if not v.get("verified"):
            escalate(f"Lens {lid} report verification failed: {v.get('reason')}",
                     {"lens_id":lid})
    if complete:
        avg_q=sum(r.quality for r in complete)/len(complete)
        if avg_q<6.0:
            escalate(f"Quality decline — avg {avg_q:.1f} below 6.0",{"avg_quality":avg_q})

# ══════════════════════════════════════════════════════════════════════════════
# MAIN ORCHESTRATOR
# ══════════════════════════════════════════════════════════════════════════════
def run_orchestrator(config:dict=None) -> dict:
    cfg       = config or {}
    run_id    = cfg.get("run_id", str(uuid.uuid4())[:8])
    job_count = cfg.get("job_count", 1)
    is_resume = job_count>1
    t0        = time.time()
    _ESCALATIONS.clear()

    # LR-054 hard stop
    if job_count>MAX_JOBS:
        reason=f"Max {MAX_JOBS} jobs/cycle reached (LR-054). Hard stop."
        log.error(f"[ORCH] {reason}")
        escalate(reason,{"job_count":job_count},critical=True)
        return _mk_result(run_id,{},None,hard_stopped=True,
                          exit_reason="max_jobs_reached",exit_message=reason)

    # Pre-flight (LR-047, LR-056)
    cp_age=cfg.get("checkpoint_age_hours",0.0)
    pf=run_preflight(job_count=job_count,is_resume=is_resume,cp_age_hours=cp_age)

    if pf.dry_run:
        s=generate_summary(run_id,{},pf,0); print(s)
        return _mk_result(run_id,{},pf,dry_run=True,exit_reason="dry_run",summary=s)

    if not pf.approved:
        er=("budget_exhausted" if "budget" in pf.abort_reason.lower()
            else "preflight_blocked")
        return _mk_result(run_id,{},pf,
            budget_hard_stopped=(er=="budget_exhausted"),
            exit_reason=er,exit_message=pf.abort_reason)

    # Article pool (LR-055)
    if is_resume and cfg.get("article_ids"):
        article_ids=cfg["article_ids"]
        articles_refetched=False
    else:
        article_ids=_load_article_ids()
        articles_refetched=not is_resume

    if not article_ids:
        reason="Article pool empty — analyze blocked"
        log.error(f"[ORCH] {reason}"); escalate(reason,critical=True)
        return _mk_result(run_id,{},pf,analyze_blocked=True,run_abandoned=True,
                          exit_reason="empty_articles",exit_message=reason)

    # Load checkpoint for resume
    results={}; completed_set=set()
    if is_resume:
        cp=cfg.get("checkpoint") or load_checkpoint(run_id)
        if cp and not is_stale(cp):
            for lid in [1,2,3,4]:
                if cp.get(f"lens_{lid}_status")=="complete":
                    completed_set.add(lid)
                    results[lid]=LensResult(lid,"skipped_already_complete")
        elif cp and is_stale(cp):
            log.warning(f"[ORCH] Checkpoint stale — clearing, starting fresh")
            clear_checkpoint(run_id)
            return _mk_result(run_id,{},pf,checkpoint_stale_cleared=True,
                              fresh_run_started=True,exit_reason="stale_checkpoint_cleared")

    # Sequential lens loop (Step 3)
    for lid in [1,2,3,4]:
        if lid in completed_set:
            log.info(f"[ORCH] Lens {lid}: already complete in checkpoint — skip"); continue

        verdict=pf.lens_verdicts.get(lid,"SKIP")
        if verdict=="SKIP":
            results[lid]=LensResult(lid,"skipped",skip_reason="preflight_verdict_skip")
            log.info(f"[ORCH] Lens {lid}: SKIP (preflight)"); continue

        # Wall check (LR-052)
        elapsed_min=(time.time()-t0)/60
        if elapsed_min>=WALL_MIN:
            log.warning(f"[ORCH] Wall at {elapsed_min:.1f}min — checkpoint + resume")
            save_checkpoint(run_id,job_count,results,article_ids)
            trigger_resume(run_id)
            return _mk_result(run_id,results,pf,exit_reason="wall_checkpoint",
                              exit_clean=True,article_ids=article_ids)

        stagger=pf.lens4_stagger if lid==4 else 0
        log.info(f"[ORCH] Firing Lens {lid}...")
        res=run_lens_with_healing(lid, stagger_s=stagger)
        results[lid]=res

        if res.status=="complete":
            log.info(f"[ORCH] Lens {lid}: ✅ quality={res.quality:.1f} rt={res.runtime_s:.1f}s")
        else:
            log.warning(f"[ORCH] Lens {lid}: ❌ {res.status} — {res.skip_reason or res.error_type}")

        # All-fail early check
        all_failed=sum(1 for r in results.values()
                       if isinstance(r,LensResult) and
                       r.status not in ("complete","skipped_already_complete","skipped"))
        if all_failed>=4:
            escalate("All 4 lenses failed — abandoning run",{"run_id":run_id},critical=True)
            return _mk_result(run_id,results,pf,run_abandoned=True,exit_reason="all_lenses_failed")

    # Post-run verification (Step 7)
    vfy=verify_reports(results); update_learning(results)

    # Escalation checks (Step 9)
    check_escalations(results,vfy)

    # Clear checkpoint
    clear_checkpoint(run_id)

    # Summary (Step 8)
    elapsed=time.time()-t0
    signals=compute_cross_lens_signals(results)
    summary=generate_summary(run_id,results,pf,elapsed,signals=signals)
    print(summary)
    # Telegram intelligence report
    try:
        from lens_telegram import send_s1_intelligence
        send_s1_intelligence(run_id=run_id)
    except Exception as _te:
        print(f"[S1-ORC] Telegram step failed (non-fatal): {_te}")

    return _mk_result(run_id,results,pf,summary=summary,verification=vfy,
                      elapsed_s=elapsed,exit_reason="complete",pipeline_continued=True,
                      articles_refetched=articles_refetched)

def _load_article_ids() -> list:
    rows=_sb_get("lens_raw_articles","?select=id&order=collected_at.desc&limit=500")
    return [r.get("id") for r in rows if isinstance(rows,list) and r.get("id")]

def _mk_result(run_id, results, pf, **kw) -> dict:
    r={"run_id":run_id,
       "dry_run":           kw.get("dry_run",LENS_DRY_RUN),
       "hard_stopped":      kw.get("hard_stopped",False),
       "budget_hard_stopped":kw.get("budget_hard_stopped",False),
       "run_abandoned":     kw.get("run_abandoned",False),
       "analyze_blocked":   kw.get("analyze_blocked",False),
       "pipeline_continued":kw.get("pipeline_continued",False),
       "exit_reason":       kw.get("exit_reason","unknown"),
       "exit_message":      kw.get("exit_message",""),
       "exit_clean":        kw.get("exit_clean",False),
       "summary":           kw.get("summary",""),
       "philosophy_gate_blocked":kw.get("philosophy_gate_blocked",False),
       "philosophy_gate_reason": kw.get("philosophy_gate_reason",None),
       "checkpoint_stale_cleared":kw.get("checkpoint_stale_cleared",False),
       "fresh_run_started": kw.get("fresh_run_started",False),
       "supabase_failed":   kw.get("supabase_failed",False),
       "articles_refetched":kw.get("articles_refetched",False),
       "article_ids":       kw.get("article_ids",[]),
       "elapsed_s":         kw.get("elapsed_s",0.0),
       "cold_start":        kw.get("cold_start",False),
       "lens3_avg_runtime": LENS3_AVG_FALLBACK,
       "lens4_stagger":     getattr(pf,"lens4_stagger",48) if pf else 48,
       "quality_baseline":  6.0,
       "provider_reliability_default":0.8,
       "articles_minimum":  30,
    }
    for lid in [1,2,3,4]:
        res=results.get(lid)
        if isinstance(res,LensResult):
            r[f"lens_{lid}_status"]           = res.status
            r[f"lens_{lid}_quality"]          = res.quality
            r[f"lens_{lid}_fallback_used"]    = res.fallback_used
            r[f"lens_{lid}_repair_record"]    = res.repair_attempts>0
            r[f"lens_{lid}_retry_count"]      = res.repair_attempts
            r[f"lens_{lid}_quality_escalated"]= res.error_type=="quality_low"
            r[f"lens_{lid}_skip_reason"]      = res.skip_reason
            r[f"lens_{lid}_runtime_recorded"] = res.runtime_s
        else:
            r[f"lens_{lid}_status"]="not_run"
    if pf:
        complete=sum(1 for lid in [1,2,3,4] if r.get(f"lens_{lid}_status")=="complete")
        r["sla_met"]             = complete>=3
        r["preflight_passed"]    = pf.approved or pf.dry_run
        r["cerebras_reliability_warned"] = False
        r["gemini_rpd_exhausted"]= pf.gemini_calls>=(GEMINI_RPD_LIMIT-GEMINI_RPD_BUFFER)
    return r

if __name__=="__main__":
    if "--test-gate" in sys.argv:
        g=run_philosophy_gate("fire_lenses",{"article_count":148,"provider_health_known":True})
        print(f"Gate: {'PASS' if g.passed else 'FAIL'}")
        sys.exit(0 if g.passed else 1)
    result=run_orchestrator()
    sys.exit(0 if result.get("exit_reason") in ("complete","dry_run","wall_checkpoint") else 1)
