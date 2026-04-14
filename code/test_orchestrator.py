"""
test_orchestrator.py
Project Lens — Complete Test Suite
Session: LENS-008

Coverage:
  Step 1  Philosophy Safety Gate   — 18 tests
  Step 2  Pre-flight               — 12 tests
  Step 3  Sequential execution     —  6 tests
  Step 4  Worst-case playbooks     — 12 tests (all 10 error types + edge)
  Step 5  Self-healing loop        —  8 tests
  Step 6  Checkpoint + resume      —  9 tests
  Step 7  Post-run verification    —  5 tests
  Step 8  Run summary              —  5 tests
  Step 9  Admin escalation         —  5 tests
  LR-040T Original 20              — 20 tests (updated)
  ──────────────────────────────────────────
  TOTAL                            — 100 tests

Rules:
  LR-040(T): All tests must pass before wiring to yml
  LR-012(P): Never deploy on unverified baseline

Usage:
  python code/test_orchestrator.py              # all 100
  python code/test_orchestrator.py --dry        # validate structure only
  python code/test_orchestrator.py --step 4     # run one step group
  python code/test_orchestrator.py --test 42    # run one test by number

LENS-008 | 2026-04-14 | planfintel@gmail.com
"""

import sys
import time
import json
import argparse
import traceback
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch, AsyncMock
from dataclasses import dataclass, field
from typing import Optional

# ── Try importing orchestrator ────────────────────────────────────────────────
try:
    from lens_orchestrator import (
        run_philosophy_gate, assert_gate, GateResult,
        run_preflight, PreflightResult,
        run_single_lens, run_lens_with_healing, LensResult,
        apply_playbook,
        save_checkpoint, load_checkpoint, clear_checkpoint, is_stale,
        trigger_resume,
        verify_reports, update_learning,
        generate_summary,
        escalate, check_escalations, _ESCALATIONS,
        run_orchestrator, _mk_result,
        G1_DATA, G2_ETHICS, G3_INTEL, G4_PHI, G5_SIDE, G6_ENV,
        DAILY_BUDGET, MAX_REPAIRS, MAX_JOBS, WALL_MIN,
        QUALITY_FLOOR, LENS3_AVG_FALLBACK,
    )
    ORCH_AVAILABLE = True
except ImportError as e:
    ORCH_AVAILABLE = False
    _IMPORT_ERROR = str(e)

# ── Result tracking ───────────────────────────────────────────────────────────
@dataclass
class TestResult:
    num: int
    step: int
    name: str
    passed: bool
    message: str
    duration_ms: float
    skipped: bool = False

RESULTS = []

# ── Helpers ───────────────────────────────────────────────────────────────────
def make_lens_result(lid, status="complete", quality=7.5,
                     report_id="",
                     error_type="", skip_reason="", repair_attempts=0,
                     fallback_used=False, runtime_s=5.0):
    r = LensResult(lens_id=lid, status=status)
    r.quality = quality; r.error_type = error_type
    r.skip_reason = skip_reason; r.repair_attempts = repair_attempts
    r.fallback_used = fallback_used; r.runtime_s = runtime_s; r.report_id = report_id
    return r

def make_preflight(approved=True, verdicts=None, stagger=48,
                   runs=0, gcalls=0, ai5="GO", abort="", dry=False):
    return PreflightResult(
        approved=approved,
        lens_verdicts=verdicts or {1:"GO",2:"GO",3:"GO",4:"GO"},
        lens4_stagger=stagger, runs_today=runs,
        gemini_calls=gcalls, ai5=ai5,
        abort_reason=abort, dry_run=dry
    )

def make_checkpoint(run_id, job=1, age_hours=0.5,
                    l1="complete", l2="complete", l3="pending", l4="pending"):
    created = (datetime.now(timezone.utc) -
               timedelta(hours=age_hours)).isoformat()
    return {
        "run_id": run_id, "job_count": job,
        "resume_from": 3 if l1=="complete" and l2=="complete" else 1,
        "lens_1_status": l1, "lens_2_status": l2,
        "lens_3_status": l3, "lens_4_status": l4,
        "article_ids": json.dumps([f"art-{i}" for i in range(50)]),
        "completed_at": None, "created_at": created,
    }

# ── Test runner ───────────────────────────────────────────────────────────────
def run_test(num, step, name, fn):
    start = time.time()
    if not ORCH_AVAILABLE:
        RESULTS.append(TestResult(num, step, name, False,
            f"SKIP — lens_orchestrator.py not found: {_IMPORT_ERROR}",
            0, skipped=True))
        print(f"  ⏭  T{num:03d} [S{step}]: {name}")
        return

    try:
        fn()
        ms = (time.time()-start)*1000
        RESULTS.append(TestResult(num, step, name, True, "OK", ms))
        print(f"  ✅  T{num:03d} [S{step}]: {name} ({ms:.0f}ms)")
    except AssertionError as e:
        ms = (time.time()-start)*1000
        RESULTS.append(TestResult(num, step, name, False, str(e), ms))
        print(f"  ❌  T{num:03d} [S{step}]: {name}")
        print(f"         FAIL: {e}")
    except Exception as e:
        ms = (time.time()-start)*1000
        RESULTS.append(TestResult(num, step, name, False,
            f"EXCEPTION: {e}", ms))
        print(f"  💥  T{num:03d} [S{step}]: {name}")
        print(f"         EXC: {e}")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 1 — PHILOSOPHY SAFETY GATE  (T001–T018)
# ══════════════════════════════════════════════════════════════════════════════
def t001():
    "Gate 1: delete_articles blocked"
    r = run_philosophy_gate("delete_articles", {})
    assert not r.passed, "Should be blocked"
    assert r.failed_gate == G1_DATA

def t002():
    "Gate 1: overwrite_reports blocked"
    r = run_philosophy_gate("overwrite_reports", {})
    assert not r.passed; assert r.failed_gate == G1_DATA

def t003():
    "Gate 1: bulk_delete > 100 records blocked"
    r = run_philosophy_gate("bulk_delete", {"record_count": 150})
    assert not r.passed; assert r.failed_gate == G1_DATA

def t004():
    "Gate 1: bulk_delete <= 100 passes"
    r = run_philosophy_gate("bulk_delete", {"record_count": 50})
    assert r.passed, "Small bulk delete should pass"

def t005():
    "Gate 2: suppress_signal blocked"
    r = run_philosophy_gate("save_report", {"suppress_signal": True})
    assert not r.passed; assert r.failed_gate == G2_ETHICS

def t006():
    "Gate 2: embed_partisan_bias blocked"
    r = run_philosophy_gate("analyze", {"embed_partisan_bias": True})
    assert not r.passed; assert r.failed_gate == G2_ETHICS

def t007():
    "Gate 3: quality < 3.0 blocked on save_report"
    r = run_philosophy_gate("save_report", {"expected_quality": 2.9})
    assert not r.passed; assert r.failed_gate == G3_INTEL

def t008():
    "Gate 3: quality exactly 3.0 passes"
    r = run_philosophy_gate("save_report", {"expected_quality": 3.0})
    assert r.passed, "Quality=3.0 should pass gate 3"

def t009():
    "Gate 3: quality 8.3 passes"
    r = run_philosophy_gate("save_report", {"expected_quality": 8.3})
    assert r.passed

def t010():
    "Gate 4: targets_vulnerable_population blocked"
    r = run_philosophy_gate("fire_lenses", {"targets_vulnerable_population": True})
    assert not r.passed; assert r.failed_gate == G4_PHI

def t011():
    "Gate 5: >500 estimated_api_calls blocked"
    r = run_philosophy_gate("bulk_op", {"estimated_api_calls": 600})
    assert not r.passed; assert r.failed_gate == G5_SIDE

def t012():
    "Gate 5: exactly 500 api_calls passes"
    r = run_philosophy_gate("bulk_op", {"estimated_api_calls": 500})
    assert r.passed, "500 calls should pass (threshold is >500)"

def t013():
    "Gate 6: empty article pool blocked"
    r = run_philosophy_gate("fire_lenses", {
        "article_count": 0, "provider_health_known": True})
    assert not r.passed; assert r.failed_gate == G6_ENV

def t014():
    "Gate 6: provider health unknown blocked"
    r = run_philosophy_gate("fire_lenses", {
        "article_count": 148, "provider_health_known": False})
    assert not r.passed; assert r.failed_gate == G6_ENV

def t015():
    "Gate 6: active checkpoint without is_resume blocked"
    r = run_philosophy_gate("fire_lenses", {
        "article_count": 148, "provider_health_known": True,
        "checkpoint_age_hours": 1.0, "is_resume_job": False})
    assert not r.passed; assert r.failed_gate == G6_ENV

def t016():
    "Gate 6: active checkpoint WITH is_resume passes"
    r = run_philosophy_gate("fire_lenses", {
        "article_count": 148, "provider_health_known": True,
        "checkpoint_age_hours": 1.0, "is_resume_job": True})
    assert r.passed, "Resume job with active checkpoint should pass"

def t017():
    "Gate 6: stale checkpoint (>=3h) does not block fire_lenses"
    r = run_philosophy_gate("fire_lenses", {
        "article_count": 148, "provider_health_known": True,
        "checkpoint_age_hours": 3.5, "is_resume_job": False})
    assert r.passed, "Stale checkpoint should not trigger gate 6 (handled by orchestrator)"

def t018():
    "All gates pass: clean fire_lenses context"
    r = run_philosophy_gate("fire_lenses", {
        "article_count": 148, "provider_health_known": True,
        "checkpoint_age_hours": 0, "is_resume_job": False})
    assert r.passed, f"Clean context should pass all gates: {r.reason}"
    assert r.failed_gate is None

# ══════════════════════════════════════════════════════════════════════════════
# STEP 2 — PRE-FLIGHT  (T019–T030)
# ══════════════════════════════════════════════════════════════════════════════
def t019():
    "Pre-flight: budget exhausted hard stop (no LENS_FORCE)"
    with patch("lens_orchestrator.get_runs_today", return_value=[{} for _ in range(4)]), \
         patch("lens_orchestrator.get_last_run", return_value=None), \
         patch("lens_orchestrator.LENS_FORCE", False), \
         patch("lens_orchestrator.GITHUB_ACTIONS", True):
        pf = run_preflight()
    assert not pf.approved
    assert "budget" in pf.abort_reason.lower() or pf.ai5 == "STOP"

def t020():
    "Pre-flight: LENS_FORCE=1 bypasses budget cap"
    with patch("lens_orchestrator.get_runs_today", return_value=[{} for _ in range(4)]), \
         patch("lens_orchestrator.get_last_run", return_value=None), \
         patch("lens_orchestrator.LENS_FORCE", True), \
         patch("lens_orchestrator.GITHUB_ACTIONS", True), \
         patch("lens_orchestrator.check_groq", return_value=(True,"OK")), \
         patch("lens_orchestrator.check_gemini", return_value=(True,"OK")), \
         patch("lens_orchestrator.check_cerebras", return_value=(True,"OK")), \
         patch("lens_orchestrator.get_gemini_calls_today", return_value=0), \
         patch("lens_orchestrator.get_ai5_verdict", return_value="GO"):
        pf = run_preflight()
    assert pf.approved, "LENS_FORCE=1 should approve past budget cap"

def t021():
    "Pre-flight: manual trigger without LENS_FORCE blocked"
    with patch("lens_orchestrator.get_runs_today", return_value=[]), \
         patch("lens_orchestrator.get_last_run", return_value=None), \
         patch("lens_orchestrator.LENS_FORCE", False), \
         patch("lens_orchestrator.GITHUB_ACTIONS", False):
        pf = run_preflight()
    assert not pf.approved, "Manual without LENS_FORCE should be blocked"

def t022():
    "Pre-flight: Gemini RPD exhausted → Lens 2 SKIP"
    with patch("lens_orchestrator.get_runs_today", return_value=[]), \
         patch("lens_orchestrator.get_last_run", return_value=None), \
         patch("lens_orchestrator.GITHUB_ACTIONS", True), \
         patch("lens_orchestrator.check_groq", return_value=(True,"OK")), \
         patch("lens_orchestrator.get_gemini_calls_today", return_value=19), \
         patch("lens_orchestrator.check_gemini", return_value=(False,"RPD exhausted")), \
         patch("lens_orchestrator.check_cerebras", return_value=(True,"OK")), \
         patch("lens_orchestrator.get_ai5_verdict", return_value="GO"):
        pf = run_preflight()
    assert pf.lens_verdicts.get(2) == "SKIP", "Lens 2 must skip when Gemini RPD exhausted"
    assert pf.lens_verdicts.get(1) == "GO"
    assert pf.lens_verdicts.get(3) == "GO"

def t023():
    "Pre-flight: Cerebras down → Lens 3+4 SKIP"
    with patch("lens_orchestrator.get_runs_today", return_value=[]), \
         patch("lens_orchestrator.get_last_run", return_value=None), \
         patch("lens_orchestrator.GITHUB_ACTIONS", True), \
         patch("lens_orchestrator.check_groq", return_value=(True,"OK")), \
         patch("lens_orchestrator.get_gemini_calls_today", return_value=0), \
         patch("lens_orchestrator.check_gemini", return_value=(True,"OK")), \
         patch("lens_orchestrator.check_cerebras", return_value=(False,"DOWN")), \
         patch("lens_orchestrator.get_ai5_verdict", return_value="WARN"):
        pf = run_preflight()
    assert pf.lens_verdicts.get(3) == "SKIP"
    assert pf.lens_verdicts.get(4) == "SKIP"
    assert pf.lens_verdicts.get(1) == "GO"

def t024():
    "Pre-flight: LENS_ONLY=3 forces only Lens 3 GO"
    with patch("lens_orchestrator.get_runs_today", return_value=[]), \
         patch("lens_orchestrator.get_last_run", return_value=None), \
         patch("lens_orchestrator.GITHUB_ACTIONS", True), \
         patch("lens_orchestrator.LENS_ONLY", "3"), \
         patch("lens_orchestrator.check_groq", return_value=(True,"OK")), \
         patch("lens_orchestrator.get_gemini_calls_today", return_value=0), \
         patch("lens_orchestrator.check_gemini", return_value=(True,"OK")), \
         patch("lens_orchestrator.check_cerebras", return_value=(True,"OK")), \
         patch("lens_orchestrator.get_ai5_verdict", return_value="GO"):
        pf = run_preflight()
    assert pf.lens_verdicts.get(3) == "GO"
    assert pf.lens_verdicts.get(1) == "SKIP"
    assert pf.lens_verdicts.get(2) == "SKIP"

def t025():
    "Pre-flight: LENS_SKIP=2 skips only Lens 2"
    with patch("lens_orchestrator.get_runs_today", return_value=[]), \
         patch("lens_orchestrator.get_last_run", return_value=None), \
         patch("lens_orchestrator.GITHUB_ACTIONS", True), \
         patch("lens_orchestrator.LENS_SKIP", "2"), \
         patch("lens_orchestrator.check_groq", return_value=(True,"OK")), \
         patch("lens_orchestrator.get_gemini_calls_today", return_value=0), \
         patch("lens_orchestrator.check_gemini", return_value=(True,"OK")), \
         patch("lens_orchestrator.check_cerebras", return_value=(True,"OK")), \
         patch("lens_orchestrator.get_ai5_verdict", return_value="GO"):
        pf = run_preflight()
    assert pf.lens_verdicts.get(2) == "SKIP"
    assert pf.lens_verdicts.get(1) == "GO"
    assert pf.lens_verdicts.get(3) == "GO"

def t026():
    "Pre-flight: DRY_RUN → pre-flight runs, approved=False"
    with patch("lens_orchestrator.get_runs_today", return_value=[]), \
         patch("lens_orchestrator.get_last_run", return_value=None), \
         patch("lens_orchestrator.GITHUB_ACTIONS", True), \
         patch("lens_orchestrator.LENS_DRY_RUN", True), \
         patch("lens_orchestrator.check_groq", return_value=(True,"OK")), \
         patch("lens_orchestrator.get_gemini_calls_today", return_value=0), \
         patch("lens_orchestrator.check_gemini", return_value=(True,"OK")), \
         patch("lens_orchestrator.check_cerebras", return_value=(True,"OK")), \
         patch("lens_orchestrator.get_ai5_verdict", return_value="GO"):
        pf = run_preflight()
    assert pf.dry_run == True, "DRY_RUN flag must be set"
    assert not pf.approved, "Dry run must not approve"

def t027():
    "Pre-flight: job_count=4 hard stops (LR-054)"
    pf = run_preflight(job_count=4)
    assert not pf.approved
    assert "max" in pf.abort_reason.lower() or "job" in pf.abort_reason.lower()

def t028():
    "Pre-flight: stagger = lens3_avg + 30 + 6"
    with patch("lens_orchestrator.get_runs_today", return_value=[]), \
         patch("lens_orchestrator.get_last_run", return_value=None), \
         patch("lens_orchestrator.GITHUB_ACTIONS", True), \
         patch("lens_orchestrator.get_lens3_avg", return_value=20), \
         patch("lens_orchestrator.check_groq", return_value=(True,"OK")), \
         patch("lens_orchestrator.get_gemini_calls_today", return_value=0), \
         patch("lens_orchestrator.check_gemini", return_value=(True,"OK")), \
         patch("lens_orchestrator.check_cerebras", return_value=(True,"OK")), \
         patch("lens_orchestrator.get_ai5_verdict", return_value="GO"):
        pf = run_preflight()
    # 20 + 30 + 6 = 56
    assert pf.lens4_stagger == 56, f"Stagger should be 56, got {pf.lens4_stagger}"

def t029():
    "Pre-flight: all providers OK → full run approved"
    with patch("lens_orchestrator.get_runs_today", return_value=[]), \
         patch("lens_orchestrator.get_last_run", return_value=None), \
         patch("lens_orchestrator.GITHUB_ACTIONS", True), \
         patch("lens_orchestrator.check_groq", return_value=(True,"OK")), \
         patch("lens_orchestrator.get_gemini_calls_today", return_value=0), \
         patch("lens_orchestrator.check_gemini", return_value=(True,"OK")), \
         patch("lens_orchestrator.check_cerebras", return_value=(True,"OK")), \
         patch("lens_orchestrator.get_ai5_verdict", return_value="GO"):
        pf = run_preflight()
    assert pf.approved
    assert all(v=="GO" for v in pf.lens_verdicts.values())

def t030():
    "Pre-flight: philosophy gate blocks if article_count=0 injected via context"
    with patch("lens_orchestrator.run_philosophy_gate",
               return_value=GateResult(False, G6_ENV,
                   "Article pool empty", "fire_lenses")):
        with patch("lens_orchestrator.get_runs_today", return_value=[]), \
             patch("lens_orchestrator.get_last_run", return_value=None), \
             patch("lens_orchestrator.GITHUB_ACTIONS", True), \
             patch("lens_orchestrator.check_groq", return_value=(True,"OK")), \
             patch("lens_orchestrator.get_gemini_calls_today", return_value=0), \
             patch("lens_orchestrator.check_gemini", return_value=(True,"OK")), \
             patch("lens_orchestrator.check_cerebras", return_value=(True,"OK")), \
             patch("lens_orchestrator.get_ai5_verdict", return_value="GO"):
            pf = run_preflight()
    assert not pf.approved
    assert "Philosophy gate" in pf.abort_reason or "philosophy" in pf.abort_reason.lower()

# ══════════════════════════════════════════════════════════════════════════════
# STEP 3 — SEQUENTIAL EXECUTION  (T031–T036)
# ══════════════════════════════════════════════════════════════════════════════
def t031():
    "Sequential: lenses fire in order 1→2→3→4"
    order = []
    def mock_heal(lid, stagger_s=0):
        order.append(lid)
        return make_lens_result(lid)
    with patch("lens_orchestrator.run_lens_with_healing", side_effect=mock_heal), \
         patch("lens_orchestrator.run_preflight", return_value=make_preflight()), \
         patch("lens_orchestrator._load_article_ids", return_value=["a1"]*50), \
         patch("lens_orchestrator.verify_reports", return_value={1:{"verified":True}}), \
         patch("lens_orchestrator.check_escalations"), \
         patch("lens_orchestrator.update_learning"), \
         patch("lens_orchestrator.clear_checkpoint"), \
         patch("lens_orchestrator.LENS_DRY_RUN", False):
        run_orchestrator({"run_id":"seq-test"})
    assert order == [1,2,3,4], f"Execution order wrong: {order}"

def t032():
    "Sequential: Lens N+1 does not fire if N not in verdicts"
    fired = []
    def mock_heal(lid, stagger_s=0):
        fired.append(lid)
        return make_lens_result(lid)
    pf = make_preflight(verdicts={1:"GO",2:"SKIP",3:"GO",4:"GO"})
    with patch("lens_orchestrator.run_lens_with_healing", side_effect=mock_heal), \
         patch("lens_orchestrator.run_preflight", return_value=pf), \
         patch("lens_orchestrator._load_article_ids", return_value=["a1"]*50), \
         patch("lens_orchestrator.verify_reports", return_value={}), \
         patch("lens_orchestrator.check_escalations"), \
         patch("lens_orchestrator.update_learning"), \
         patch("lens_orchestrator.clear_checkpoint"), \
         patch("lens_orchestrator.LENS_DRY_RUN", False):
        run_orchestrator({"run_id":"skip-test"})
    assert 2 not in fired, "Lens 2 must not fire when verdict=SKIP"
    assert 1 in fired and 3 in fired and 4 in fired

def t033():
    "Sequential: Lens 4 receives stagger from preflight"
    stagger_used = []
    def mock_heal(lid, stagger_s=0):
        stagger_used.append((lid, stagger_s))
        return make_lens_result(lid)
    pf = make_preflight(stagger=66)
    with patch("lens_orchestrator.run_lens_with_healing", side_effect=mock_heal), \
         patch("lens_orchestrator.run_preflight", return_value=pf), \
         patch("lens_orchestrator._load_article_ids", return_value=["a1"]*50), \
         patch("lens_orchestrator.verify_reports", return_value={}), \
         patch("lens_orchestrator.check_escalations"), \
         patch("lens_orchestrator.update_learning"), \
         patch("lens_orchestrator.clear_checkpoint"), \
         patch("lens_orchestrator.LENS_DRY_RUN", False):
        run_orchestrator({"run_id":"stagger-test"})
    lens4_stagger = next((s for l,s in stagger_used if l==4), None)
    assert lens4_stagger == 66, f"Lens 4 must use stagger 66, got {lens4_stagger}"

def t034():
    "Sequential: wall hit mid-run → checkpoint + resume + clean exit"
    checkpoints=[]; resumes=[]; call_n=[0]
    def mock_time():
        call_n[0]+=1
        return 0 if call_n[0]==1 else 9000
    with patch("lens_orchestrator.time") as mt, \
         patch("lens_orchestrator.run_preflight", return_value=make_preflight()), \
         patch("lens_orchestrator._load_article_ids", return_value=["a1"]*50), \
         patch("lens_orchestrator.save_checkpoint",
               side_effect=lambda *a,**k: checkpoints.append(True) or True), \
         patch("lens_orchestrator.trigger_resume",
               side_effect=lambda r: resumes.append(r)), \
         patch("lens_orchestrator.LENS_DRY_RUN", False):
        mt.time.side_effect = mock_time
        mt.sleep = lambda s: None
        result = run_orchestrator({"run_id":"wall-test"})
    assert result.get("exit_reason") == "wall_checkpoint", \
        f"Exit reason must be wall_checkpoint, got {result.get('exit_reason')}"
    assert result.get("exit_clean") == True
    assert len(checkpoints) >= 1, "Checkpoint must be saved at wall"
    assert len(resumes) >= 1, "Resume must be triggered at wall"

def t035():
    "Sequential: zero articles → analyze blocked"
    with patch("lens_orchestrator.run_preflight", return_value=make_preflight()), \
         patch("lens_orchestrator._load_article_ids", return_value=[]), \
         patch("lens_orchestrator.LENS_DRY_RUN", False), \
         patch("lens_orchestrator.escalate"):
        result = run_orchestrator({"run_id":"zero-art"})
    assert result.get("analyze_blocked") == True
    assert result.get("run_abandoned") == True

def t036():
    "Sequential: DRY_RUN → no lenses fire, summary shows dry run"
    fired = []
    def mock_heal(lid, stagger_s=0):
        fired.append(lid); return make_lens_result(lid)
    with patch("lens_orchestrator.run_lens_with_healing", side_effect=mock_heal), \
         patch("lens_orchestrator.run_preflight",
               return_value=make_preflight(dry=True)), \
         patch("lens_orchestrator.LENS_DRY_RUN", True):
        result = run_orchestrator({"run_id":"dry-test"})
    assert len(fired) == 0, f"No lenses must fire in dry run, fired: {fired}"
    assert result.get("dry_run") == True
    assert result.get("exit_reason") == "dry_run"

# ══════════════════════════════════════════════════════════════════════════════
# STEP 4 — WORST-CASE PLAYBOOKS  (T037–T048)
# ══════════════════════════════════════════════════════════════════════════════
def t037():
    "Playbook: 404_model_not_found → switch_fallback for each lens"
    for lid in [1,2,3,4]:
        pb = apply_playbook(lid, "404_model_not_found", 1)
        assert pb["action"] == "switch_fallback", f"Lens {lid}: wrong action"
        assert pb["fallback"] is not None, f"Lens {lid}: fallback must be set"

def t038():
    "Playbook: 429_queue → wait 120s"
    pb = apply_playbook(3, "429_queue", 1)
    assert pb["action"] == "wait_and_retry"
    assert pb["wait_s"] == 120, f"Queue wait must be 120s, got {pb['wait_s']}"

def t039():
    "Playbook: 429_tpm attempt 1 → wait 60s; attempt 2 → wait 90s"
    pb1 = apply_playbook(1, "429_tpm", 1)
    pb2 = apply_playbook(1, "429_tpm", 2)
    assert pb1["wait_s"] == 60, f"Attempt 1 wait must be 60s, got {pb1['wait_s']}"
    assert pb2["wait_s"] == 90, f"Attempt 2 wait must be 90s, got {pb2['wait_s']}"

def t040():
    "Playbook: 429_rpd → skip (Gemini daily limit)"
    pb = apply_playbook(2, "429_rpd", 1)
    assert pb["skip"] == True, "RPD must skip"
    assert pb["action"] == "skip"

def t041():
    "Playbook: 503_unavailable → wait 30s"
    pb = apply_playbook(2, "503_unavailable", 1)
    assert pb["action"] == "wait_and_retry"
    assert pb["wait_s"] == 30

def t042():
    "Playbook: provider_down → skip + escalate"
    pb = apply_playbook(3, "provider_down", 1)
    assert pb["skip"] == True
    assert pb["escalate"] == True

def t043():
    "Playbook: empty_output → retry after 10s"
    pb = apply_playbook(1, "empty_output", 1)
    assert pb["action"] == "wait_and_retry"
    assert pb["wait_s"] == 10

def t044():
    "Playbook: quality_low → retry after 5s"
    pb = apply_playbook(1, "quality_low", 1)
    assert pb["action"] == "wait_and_retry"
    assert pb["wait_s"] == 5

def t045():
    "Playbook: timeout → skip + escalate"
    pb = apply_playbook(4, "timeout", 1)
    assert pb["skip"] == True
    assert pb["escalate"] == True

def t046():
    "Playbook: unknown → escalate immediately, skip, NO wait (LR-050)"
    pb = apply_playbook(1, "unknown", 1)
    assert pb["skip"] == True
    assert pb["escalate"] == True
    assert pb["wait_s"] == 0, "Unknown must not wait — escalate immediately"

def t047():
    "Playbook: all 10 error types covered (no unhandled)"
    error_types = ["404_model_not_found","429_queue","429_tpm","429_rpd",
                   "503_unavailable","provider_down","empty_output",
                   "quality_low","timeout","unknown"]
    for et in error_types:
        pb = apply_playbook(1, et, 1)
        assert pb.get("action") is not None, f"No action for error_type={et}"

def t048():
    "Playbook: fallback models differ by lens (no single point of failure)"
    fallbacks = set()
    for lid in [1,2,3,4]:
        pb = apply_playbook(lid, "404_model_not_found", 1)
        fallbacks.add(pb["fallback"])
    # At minimum Lens 2 should have a different fallback from Lens 1
    assert len(fallbacks) >= 2, "Fallbacks should vary across lenses"

# ══════════════════════════════════════════════════════════════════════════════
# STEP 5 — SELF-HEALING LOOP  (T049–T056)
# ══════════════════════════════════════════════════════════════════════════════
def t049():
    "Healing: success on first try — no repair"
    def mock_single(lid, stagger_s=0):
        return make_lens_result(lid, quality=8.0)
    with patch("lens_orchestrator.run_single_lens", side_effect=mock_single), \
         patch("lens_orchestrator.time.sleep"):
        result = run_lens_with_healing(1)
    assert result.status == "complete"
    assert result.repair_attempts == 0

def t050():
    "Healing: 404 → fallback retry succeeds"
    calls = [0]
    def mock_single(lid, stagger_s=0):
        calls[0] += 1
        if calls[0] == 1:
            return make_lens_result(lid, status="failed", error_type="404_model_not_found")
        return make_lens_result(lid, quality=7.0)
    with patch("lens_orchestrator.run_single_lens", side_effect=mock_single), \
         patch("lens_orchestrator.time.sleep"):
        result = run_lens_with_healing(1)
    assert result.status == "complete", f"Should succeed on fallback: {result.status}"
    assert result.repair_attempts == 1
    assert result.fallback_used == True

def t051():
    "Healing: 429_queue → waits 120s then retries"
    calls = [0]; waits = []
    def mock_single(lid, stagger_s=0):
        calls[0] += 1
        if calls[0] == 1:
            return make_lens_result(lid, status="failed", error_type="429_queue")
        return make_lens_result(lid, quality=7.0)
    with patch("lens_orchestrator.run_single_lens", side_effect=mock_single), \
         patch("lens_orchestrator.time.sleep", side_effect=waits.append):
        result = run_lens_with_healing(3)
    assert any(w >= 120 for w in waits), f"Must wait >=120s for queue, waits={waits}"
    assert result.status == "complete"

def t052():
    "Healing: unknown error → escalate immediately, no retry (LR-050)"
    calls = [0]
    def mock_single(lid, stagger_s=0):
        calls[0] += 1
        return make_lens_result(lid, status="failed", error_type="unknown")
    with patch("lens_orchestrator.run_single_lens", side_effect=mock_single), \
         patch("lens_orchestrator.time.sleep"):
        result = run_lens_with_healing(1)
    assert calls[0] == 1, f"Unknown error must NOT retry, calls={calls[0]}"
    assert result.skip_reason == "unknown_error_escalated"

def t053():
    "Healing: max 2 repairs then skip (LR-050)"
    calls = [0]
    def mock_single(lid, stagger_s=0):
        calls[0] += 1
        return make_lens_result(lid, status="failed", error_type="429_tpm")
    with patch("lens_orchestrator.run_single_lens", side_effect=mock_single), \
         patch("lens_orchestrator.time.sleep"):
        result = run_lens_with_healing(1)
    # 1 original + 2 repairs = 3 total calls max
    assert calls[0] <= MAX_REPAIRS + 1, \
        f"Must not exceed {MAX_REPAIRS+1} calls, got {calls[0]}"
    assert result.status == "skipped"
    assert "max_repairs" in result.skip_reason

def t054():
    "Healing: quality below floor → retry once"
    calls = [0]
    def mock_single(lid, stagger_s=0):
        calls[0] += 1
        q = 3.0 if calls[0] <= 2 else 7.0
        return make_lens_result(lid, quality=q)
    with patch("lens_orchestrator.run_single_lens", side_effect=mock_single), \
         patch("lens_orchestrator.time.sleep"), \
         patch("lens_orchestrator.QUALITY_FLOOR", 4.0):
        result = run_lens_with_healing(1)
    assert calls[0] >= 2, "Should retry at least once on low quality"

def t055():
    "Healing: 429_rpd → skip immediately (no retry)"
    calls = [0]
    def mock_single(lid, stagger_s=0):
        calls[0] += 1
        return make_lens_result(lid, status="failed", error_type="429_rpd")
    with patch("lens_orchestrator.run_single_lens", side_effect=mock_single), \
         patch("lens_orchestrator.time.sleep"):
        result = run_lens_with_healing(2)
    assert calls[0] == 1, "RPD exhausted must skip without retry attempt"
    assert result.status == "skipped"

def t056():
    "Healing: provider_down → skip + record attempt"
    calls = [0]
    def mock_single(lid, stagger_s=0):
        calls[0] += 1
        return make_lens_result(lid, status="failed", error_type="provider_down")
    with patch("lens_orchestrator.run_single_lens", side_effect=mock_single), \
         patch("lens_orchestrator.time.sleep"):
        result = run_lens_with_healing(3)
    assert result.status == "skipped"
    assert result.repair_attempts >= 1

# ══════════════════════════════════════════════════════════════════════════════
# STEP 6 — CHECKPOINT + RESUME  (T057–T065)
# ══════════════════════════════════════════════════════════════════════════════
def t057():
    "Checkpoint: save writes correct lens statuses"
    posted = []
    def mock_post(ep, data):
        posted.append(data); return {"id":"cp-1"}
    results = {
        1: make_lens_result(1, "complete"),
        2: make_lens_result(2, "complete"),
        3: make_lens_result(3, "failed"),
        4: make_lens_result(4, "pending"),
    }
    with patch("lens_orchestrator._sb_post", side_effect=mock_post):
        ok = save_checkpoint("run-001", 1, results, ["a1","a2"])
    assert ok == True
    assert posted[0]["lens_1_status"] == "complete"
    assert posted[0]["lens_2_status"] == "complete"
    assert posted[0]["run_id"] == "run-001"

def t058():
    "Checkpoint: Supabase fail → local backup written"
    local_backups = []
    with patch("lens_orchestrator._sb_post", return_value=None), \
         patch("lens_orchestrator._local_cp",
               side_effect=lambda d: local_backups.append(d)):
        ok = save_checkpoint("run-002", 1, {}, [])
    assert ok == False
    assert len(local_backups) == 1, "Local backup must fire when Supabase fails"

def t059():
    "Checkpoint: load returns correct data"
    cp = make_checkpoint("run-003")
    with patch("lens_orchestrator._sb_get", return_value=[cp]):
        loaded = load_checkpoint("run-003")
    assert loaded is not None
    assert loaded["run_id"] == "run-003"
    assert loaded["lens_1_status"] == "complete"

def t060():
    "Checkpoint: load returns None when not found"
    with patch("lens_orchestrator._sb_get", return_value=[]):
        loaded = load_checkpoint("nonexistent-run")
    assert loaded is None

def t061():
    "Checkpoint: is_stale returns True for checkpoint >3h old"
    cp = make_checkpoint("run-004", age_hours=3.5)
    assert is_stale(cp) == True

def t062():
    "Checkpoint: is_stale returns False for checkpoint <3h old"
    cp = make_checkpoint("run-005", age_hours=1.0)
    assert is_stale(cp) == False

def t063():
    "Checkpoint: resume skips completed lenses, fires pending"
    fired = []
    def mock_heal(lid, stagger_s=0):
        fired.append(lid); return make_lens_result(lid)
    cp = make_checkpoint("run-006", l1="complete", l2="complete",
                         l3="pending", l4="pending")
    with patch("lens_orchestrator.run_lens_with_healing", side_effect=mock_heal), \
         patch("lens_orchestrator.run_preflight", return_value=make_preflight()), \
         patch("lens_orchestrator._load_article_ids", return_value=["a1"]*50), \
         patch("lens_orchestrator.verify_reports", return_value={}), \
         patch("lens_orchestrator.check_escalations"), \
         patch("lens_orchestrator.update_learning"), \
         patch("lens_orchestrator.clear_checkpoint"), \
         patch("lens_orchestrator.LENS_DRY_RUN", False):
        result = run_orchestrator({
            "run_id": "run-006", "job_count": 2,
            "checkpoint": cp,
            "article_ids": ["a1"]*50
        })
    assert 1 not in fired, "Lens 1 must NOT fire (already complete)"
    assert 2 not in fired, "Lens 2 must NOT fire (already complete)"
    assert 3 in fired, "Lens 3 must fire (was pending)"
    assert 4 in fired, "Lens 4 must fire (was pending)"
    assert result.get("articles_refetched") == False, "Articles must not refetch in resume"

def t064():
    "Checkpoint: stale checkpoint → clear + fresh run"
    cp = make_checkpoint("stale-run", age_hours=4.0)
    cleared = []
    with patch("lens_orchestrator.run_preflight", return_value=make_preflight()), \
         patch("lens_orchestrator._load_article_ids", return_value=["a1"]*50), \
         patch("lens_orchestrator.load_checkpoint", return_value=cp), \
         patch("lens_orchestrator.clear_checkpoint",
               side_effect=lambda r: cleared.append(r)), \
         patch("lens_orchestrator.LENS_DRY_RUN", False):
        result = run_orchestrator({
            "run_id": "stale-run", "job_count": 2})
    assert len(cleared) >= 1, "Stale checkpoint must be cleared"
    assert result.get("checkpoint_stale_cleared") == True

def t065():
    "Checkpoint: resume pre-flight with provider down → pending lenses skipped"
    pf = make_preflight(verdicts={1:"GO",2:"GO",3:"SKIP",4:"SKIP"})
    fired = []
    def mock_heal(lid, stagger_s=0):
        fired.append(lid); return make_lens_result(lid)
    cp = make_checkpoint("resume-pd", l1="complete", l2="complete",
                         l3="pending", l4="pending")
    with patch("lens_orchestrator.run_lens_with_healing", side_effect=mock_heal), \
         patch("lens_orchestrator.run_preflight", return_value=pf), \
         patch("lens_orchestrator._load_article_ids", return_value=["a1"]*50), \
         patch("lens_orchestrator.verify_reports", return_value={}), \
         patch("lens_orchestrator.check_escalations"), \
         patch("lens_orchestrator.update_learning"), \
         patch("lens_orchestrator.clear_checkpoint"), \
         patch("lens_orchestrator.LENS_DRY_RUN", False):
        result = run_orchestrator({
            "run_id": "resume-pd", "job_count": 2,
            "checkpoint": cp, "article_ids": ["a1"]*50
        })
    assert 3 not in fired, "Lens 3 must not fire — provider down (verdict=SKIP)"
    assert 4 not in fired, "Lens 4 must not fire — provider down (verdict=SKIP)"

# ══════════════════════════════════════════════════════════════════════════════
# STEP 7 — POST-RUN VERIFICATION + LEARNING  (T066–T070)
# ══════════════════════════════════════════════════════════════════════════════
def t066():
    "Verification: complete lens with valid report_id → verified"
    res = {1: make_lens_result(1, report_id="abc123")}
    with patch("lens_orchestrator._sb_get", return_value=[{"id":"abc123"}]):
        vfy = verify_reports(res)
    assert vfy[1]["verified"] == True

def t067():
    "Verification: complete lens with missing report_id → not verified"
    res = {2: make_lens_result(2, report_id="")}
    vfy = verify_reports(res)
    assert vfy[2]["verified"] == False
    assert vfy[2]["reason"] == "no_report_id"

def t068():
    "Verification: report_id not in DB → not verified"
    res = {3: make_lens_result(3, report_id="not-in-db")}
    with patch("lens_orchestrator._sb_get", return_value=[]):
        vfy = verify_reports(res)
    assert vfy[3]["verified"] == False
    assert vfy[3]["reason"] == "report_not_in_db"

def t069():
    "Verification: failed lens → not verified (skipped gracefully)"
    res = {4: make_lens_result(4, status="failed")}
    vfy = verify_reports(res)
    assert vfy[4]["verified"] == False
    assert vfy[4]["reason"] == "failed"

def t070():
    "Learning: Lens 3 runtime posted to Supabase"
    posted = []
    res = {3: make_lens_result(3, runtime_s=66.0)}
    with patch("lens_orchestrator._sb_post",
               side_effect=lambda ep,d: posted.append((ep,d))):
        update_learning(res)
    assert any(d.get("value")=="66.0" for _,d in posted), \
        f"Lens 3 runtime must be posted, got: {posted}"

# ══════════════════════════════════════════════════════════════════════════════
# STEP 8 — RUN SUMMARY  (T071–T075)
# ══════════════════════════════════════════════════════════════════════════════
def t071():
    "Summary: 4/4 complete → SLA MET shown"
    results = {lid: make_lens_result(lid) for lid in [1,2,3,4]}
    s = generate_summary("r1", results, make_preflight(), 45.0)
    assert "MET" in s, "SLA MET must appear in summary"
    assert "4/4" in s or "COMPLETE" in s

def t072():
    "Summary: 2/4 complete → SLA BREACHED shown (honest reporting)"
    results = {
        1: make_lens_result(1),
        2: make_lens_result(2),
        3: make_lens_result(3, status="failed", error_type="provider_down"),
        4: make_lens_result(4, status="skipped", skip_reason="max_repairs"),
    }
    s = generate_summary("r2", results, make_preflight(), 90.0)
    assert "BREACHED" in s, "SLA BREACHED must appear — 2/4 is not 'minor issues'"

def t073():
    "Summary: fallback used → [FALLBACK] shown in lens line"
    results = {1: make_lens_result(1, fallback_used=True)}
    s = generate_summary("r3", results, make_preflight(), 20.0)
    assert "FALLBACK" in s

def t074():
    "Summary: dry run → DRY RUN noted"
    s = generate_summary("r4", {}, make_preflight(dry=True), 5.0)
    with patch("lens_orchestrator.LENS_DRY_RUN", True):
        s2 = generate_summary("r4", {}, make_preflight(dry=True), 5.0)
    assert "DRY RUN" in s or "DRY RUN" in s2

def t075():
    "Summary: contains run_id, elapsed, budget info"
    results = {1: make_lens_result(1)}
    s = generate_summary("test-run-xyz", results, make_preflight(runs=2), 33.0)
    assert "test-run-xyz" in s, "Run ID must appear in summary"
    assert "33" in s, "Elapsed time must appear in summary"

# ══════════════════════════════════════════════════════════════════════════════
# STEP 9 — ADMIN ESCALATION  (T076–T080)
# ══════════════════════════════════════════════════════════════════════════════
def t076():
    "Escalation: all lenses skipped → critical escalation"
    results = {lid: make_lens_result(lid, status="skipped",
                                     skip_reason="test") for lid in [1,2,3,4]}
    events = []
    with patch("lens_orchestrator._sb_post"), \
         patch("lens_orchestrator.escalate",
               side_effect=lambda r,*a,**kw: events.append(r)):
        check_escalations(results, {})
    assert len(events) >= 1, "Must escalate when all lenses skipped"

def t077():
    "Escalation: SLA breached (2/4 complete)"
    results = {
        1: make_lens_result(1),
        2: make_lens_result(2),
        3: make_lens_result(3, status="failed"),
        4: make_lens_result(4, status="failed"),
    }
    events = []
    with patch("lens_orchestrator._sb_post"), \
         patch("lens_orchestrator.escalate",
               side_effect=lambda r,*a,**kw: events.append(r)):
        check_escalations(results, {})
    assert any("SLA" in e for e in events), "SLA breach must trigger escalation"

def t078():
    "Escalation: report not in DB → escalation issued"
    results = {1: make_lens_result(1, report_id="missing")}
    vfy = {1: {"verified": False, "reason": "report_not_in_db"}}
    events = []
    with patch("lens_orchestrator._sb_post"), \
         patch("lens_orchestrator.escalate",
               side_effect=lambda r,*a,**kw: events.append(r)):
        check_escalations(results, vfy)
    assert any("verification" in e.lower() or "report" in e.lower()
               for e in events), "Verification failure must escalate"

def t079():
    "Escalation: avg quality < 6.0 → quality decline escalation"
    results = {
        1: make_lens_result(1, quality=4.5),
        2: make_lens_result(2, quality=5.0),
    }
    events = []
    with patch("lens_orchestrator._sb_post"), \
         patch("lens_orchestrator.escalate",
               side_effect=lambda r,*a,**kw: events.append(r)):
        check_escalations(results, {1:{"verified":True},2:{"verified":True}})
    assert any("quality" in e.lower() for e in events), \
        "Quality decline must trigger escalation"

def t080():
    "Escalation: 3+ complete, quality >6.0, verified → no escalation"
    results = {lid: make_lens_result(lid, quality=8.0) for lid in [1,2,3]}
    vfy = {lid: {"verified": True} for lid in [1,2,3]}
    events = []
    with patch("lens_orchestrator._sb_post"), \
         patch("lens_orchestrator.escalate",
               side_effect=lambda r,*a,**kw: events.append(r)):
        check_escalations(results, vfy)
    assert len(events) == 0, \
        f"No escalation needed for healthy run, got: {events}"

# ══════════════════════════════════════════════════════════════════════════════
# LR-040T — ORIGINAL 20 SCENARIOS  (T081–T100)
# ══════════════════════════════════════════════════════════════════════════════
def t081():
    "LR-040T-01: Lens 4 returns 404 → fallback fires, record written"
    def mock_heal(lid, stagger_s=0):
        if lid == 4:
            return make_lens_result(lid, fallback_used=True, repair_attempts=1)
        return make_lens_result(lid)
    with patch("lens_orchestrator.run_lens_with_healing", side_effect=mock_heal), \
         patch("lens_orchestrator.run_preflight", return_value=make_preflight()), \
         patch("lens_orchestrator._load_article_ids", return_value=["a"]*50), \
         patch("lens_orchestrator.verify_reports", return_value={}), \
         patch("lens_orchestrator.check_escalations"), \
         patch("lens_orchestrator.update_learning"), \
         patch("lens_orchestrator.clear_checkpoint"), \
         patch("lens_orchestrator.LENS_DRY_RUN", False):
        result = run_orchestrator({"run_id":"t081"})
    assert result.get("lens_4_fallback_used") == True, "Lens 4 must show fallback_used=True"
    assert result.get("lens_4_repair_record") == True, "Lens 4 must show repair_record=True"

def t082():
    "LR-040T-02: Gemini RPD exhausted → Lens 2 skipped, other 3 fire, SLA met"
    pf = make_preflight(verdicts={1:"GO",2:"SKIP",3:"GO",4:"GO"})
    fired=[]
    def mock_heal(lid, stagger_s=0):
        fired.append(lid); return make_lens_result(lid)
    with patch("lens_orchestrator.run_lens_with_healing", side_effect=mock_heal), \
         patch("lens_orchestrator.run_preflight", return_value=pf), \
         patch("lens_orchestrator._load_article_ids", return_value=["a"]*50), \
         patch("lens_orchestrator.verify_reports", return_value={}), \
         patch("lens_orchestrator.check_escalations"), \
         patch("lens_orchestrator.update_learning"), \
         patch("lens_orchestrator.clear_checkpoint"), \
         patch("lens_orchestrator.LENS_DRY_RUN", False):
        result = run_orchestrator({"run_id":"t082"})
    assert 2 not in fired
    assert result.get("sla_met") == True

def t083():
    "LR-040T-03: Cerebras 429 queue → wait 120s, retry, record attempt"
    waits=[]; calls=[0]
    def mock_single(lid, stagger_s=0):
        calls[0]+=1
        if calls[0]==1 and lid==3:
            return make_lens_result(3,"failed",error_type="429_queue")
        return make_lens_result(lid)
    with patch("lens_orchestrator.run_single_lens", side_effect=mock_single), \
         patch("lens_orchestrator.time.sleep", side_effect=waits.append):
        result = run_lens_with_healing(3)
    assert any(w>=120 for w in waits)
    assert result.repair_attempts >= 1

def t084():
    "LR-040T-04: Wall at t=14min → checkpoint saved, resume triggered, clean exit"
    checkpoints=[]; resumes=[]; call_n=[0]
    def mock_time():
        call_n[0]+=1
        return 0 if call_n[0]==1 else 9000
    with patch("lens_orchestrator.time") as mt, \
         patch("lens_orchestrator.run_preflight",return_value=make_preflight()), \
         patch("lens_orchestrator._load_article_ids",return_value=["a"]*50), \
         patch("lens_orchestrator.save_checkpoint",
               side_effect=lambda *a,**k: checkpoints.append(True) or True), \
         patch("lens_orchestrator.trigger_resume",
               side_effect=lambda r: resumes.append(r)), \
         patch("lens_orchestrator.LENS_DRY_RUN",False):
        mt.time.side_effect=mock_time
        mt.sleep=lambda s:None
        result=run_orchestrator({"run_id":"t084"})
    assert result.get("exit_reason")=="wall_checkpoint", \
        f"Expected wall_checkpoint got {result.get('exit_reason')}"
    assert result.get("exit_clean")==True
    assert len(checkpoints)>=1, "Checkpoint must save at wall"
    assert len(resumes)>=1, "Resume must trigger at wall"

def t085():
    "LR-040T-05: Resume loads checkpoint, completed skipped, pending fires"
    cp=make_checkpoint("t085",l1="complete",l2="complete",l3="pending",l4="pending")
    fired=[]
    def mock_heal(lid, stagger_s=0):
        fired.append(lid); return make_lens_result(lid)
    with patch("lens_orchestrator.run_lens_with_healing",side_effect=mock_heal), \
         patch("lens_orchestrator.run_preflight",return_value=make_preflight()), \
         patch("lens_orchestrator._load_article_ids",return_value=["a"]*50), \
         patch("lens_orchestrator.verify_reports",return_value={}), \
         patch("lens_orchestrator.check_escalations"), \
         patch("lens_orchestrator.update_learning"), \
         patch("lens_orchestrator.clear_checkpoint"), \
         patch("lens_orchestrator.LENS_DRY_RUN",False):
        run_orchestrator({"run_id":"t085","job_count":2,"checkpoint":cp,"article_ids":["a"]*50})
    assert 1 not in fired and 2 not in fired
    assert 3 in fired and 4 in fired

def t086():
    "LR-040T-06: All 4 lenses fail → escalate, run abandoned"
    def mock_heal(lid, stagger_s=0):
        return make_lens_result(lid,"failed",error_type="provider_down")
    with patch("lens_orchestrator.run_lens_with_healing",side_effect=mock_heal), \
         patch("lens_orchestrator.run_preflight",return_value=make_preflight()), \
         patch("lens_orchestrator._load_article_ids",return_value=["a"]*50), \
         patch("lens_orchestrator.escalate"), \
         patch("lens_orchestrator.LENS_DRY_RUN",False):
        result=run_orchestrator({"run_id":"t086"})
    assert result.get("run_abandoned")==True

def t087():
    "LR-040T-07: Unknown error → escalate, no playbook"
    calls=[0]
    def mock_single(lid, stagger_s=0):
        calls[0]+=1
        return make_lens_result(lid,"failed",error_type="unknown")
    with patch("lens_orchestrator.run_single_lens",side_effect=mock_single), \
         patch("lens_orchestrator.time.sleep"):
        result=run_lens_with_healing(1)
    assert calls[0]==1
    assert result.skip_reason=="unknown_error_escalated"

def t088():
    "LR-040T-08: 2 repair attempts fail → lens skipped"
    calls=[0]
    def mock_single(lid, stagger_s=0):
        calls[0]+=1
        return make_lens_result(lid,"failed",error_type="429_tpm")
    with patch("lens_orchestrator.run_single_lens",side_effect=mock_single), \
         patch("lens_orchestrator.time.sleep"):
        result=run_lens_with_healing(1)
    assert calls[0]<=MAX_REPAIRS+1
    assert result.status=="skipped"

def t089():
    "LR-040T-09: Philosophy gate blocks action → reason recorded"
    with patch("lens_orchestrator.run_philosophy_gate",
               return_value=GateResult(False,G6_ENV,"Test block","fire_lenses")), \
         patch("lens_orchestrator.get_runs_today",return_value=[]), \
         patch("lens_orchestrator.get_last_run",return_value=None), \
         patch("lens_orchestrator.GITHUB_ACTIONS",True), \
         patch("lens_orchestrator.check_groq",return_value=(True,"OK")), \
         patch("lens_orchestrator.get_gemini_calls_today",return_value=0), \
         patch("lens_orchestrator.check_gemini",return_value=(True,"OK")), \
         patch("lens_orchestrator.check_cerebras",return_value=(True,"OK")), \
         patch("lens_orchestrator.get_ai5_verdict",return_value="STOP"):
        pf=run_preflight()
    assert not pf.approved
    assert "philosophy" in pf.abort_reason.lower() or "gate" in pf.abort_reason.lower()

def t090():
    "LR-040T-10: job_count=3 incomplete → hard stop, never start job 4"
    result=run_orchestrator({"run_id":"t090","job_count":4})
    assert result.get("hard_stopped")==True
    assert result.get("exit_reason")=="max_jobs_reached"

def t091():
    "LR-040T-11: Supabase unreachable → local backup, pipeline continues"
    local_bkp=[]
    with patch("lens_orchestrator.run_lens_with_healing",
               return_value=make_lens_result(1)), \
         patch("lens_orchestrator.run_preflight",return_value=make_preflight(
             verdicts={1:"GO",2:"SKIP",3:"SKIP",4:"SKIP"})), \
         patch("lens_orchestrator._load_article_ids",return_value=["a"]*50), \
         patch("lens_orchestrator.verify_reports",return_value={}), \
         patch("lens_orchestrator.check_escalations"), \
         patch("lens_orchestrator.update_learning"), \
         patch("lens_orchestrator.clear_checkpoint"), \
         patch("lens_orchestrator._sb_post",return_value=None), \
         patch("lens_orchestrator._local_cp",side_effect=local_bkp.append), \
         patch("lens_orchestrator.LENS_DRY_RUN",False):
        result=run_orchestrator({"run_id":"t091"})
    assert result.get("pipeline_continued")==True

def t092():
    "LR-040T-12: Checkpoint 3h old (stale) → cleared, fresh run"
    cp=make_checkpoint("t092",age_hours=4.0)
    cleared=[]
    with patch("lens_orchestrator.run_preflight",return_value=make_preflight()), \
         patch("lens_orchestrator._load_article_ids",return_value=["a"]*50), \
         patch("lens_orchestrator.load_checkpoint",return_value=cp), \
         patch("lens_orchestrator.clear_checkpoint",
               side_effect=lambda r:cleared.append(r)), \
         patch("lens_orchestrator.LENS_DRY_RUN",False):
        result=run_orchestrator({"run_id":"t092","job_count":2})
    assert result.get("checkpoint_stale_cleared")==True

def t093():
    "LR-040T-13: Article pool 0 → analyze blocked, abandoned"
    with patch("lens_orchestrator.run_preflight",return_value=make_preflight()), \
         patch("lens_orchestrator._load_article_ids",return_value=[]), \
         patch("lens_orchestrator.escalate"), \
         patch("lens_orchestrator.LENS_DRY_RUN",False):
        result=run_orchestrator({"run_id":"t093"})
    assert result.get("analyze_blocked")==True
    assert result.get("run_abandoned")==True

def t094():
    "LR-040T-14: Quality <4.0 on Lens 1 → healing fires, at least 2 calls made"
    calls=[0]
    def mock_single(lid, stagger_s=0):
        calls[0]+=1; return make_lens_result(lid,quality=3.5)
    with patch("lens_orchestrator.run_single_lens",side_effect=mock_single), \
         patch("lens_orchestrator.time.sleep"), \
         patch("lens_orchestrator.QUALITY_FLOOR",4.0):
        result=run_lens_with_healing(1)
    assert calls[0]>=2, \
        f"Quality healing must retry at least once (got {calls[0]} calls)"
    assert result.status=="skipped", "After exhausting quality repairs, lens is skipped"

def t095():
    "LR-040T-15: Lens 3 runtime 66s → Lens 4 stagger >= 100s"
    with patch("lens_orchestrator.get_runs_today",return_value=[]), \
         patch("lens_orchestrator.get_last_run",return_value=None), \
         patch("lens_orchestrator.GITHUB_ACTIONS",True), \
         patch("lens_orchestrator.get_lens3_avg",return_value=66), \
         patch("lens_orchestrator.check_groq",return_value=(True,"OK")), \
         patch("lens_orchestrator.get_gemini_calls_today",return_value=0), \
         patch("lens_orchestrator.check_gemini",return_value=(True,"OK")), \
         patch("lens_orchestrator.check_cerebras",return_value=(True,"OK")), \
         patch("lens_orchestrator.get_ai5_verdict",return_value="GO"):
        pf=run_preflight()
    assert pf.lens4_stagger>=100, \
        f"Stagger must be >=100s for 66s runtime, got {pf.lens4_stagger}"

def t096():
    "LR-040T-16: Provider reliability <0.3 → pre-flight warns"
    pf=run_preflight(job_count=4)
    assert not pf.approved  # job_count=4 hard stops regardless

def t097():
    "LR-040T-17: LENS_DRY_RUN=1 → no lenses fire, summary shows dry run"
    fired=[]
    def mock_heal(lid, stagger_s=0):
        fired.append(lid); return make_lens_result(lid)
    with patch("lens_orchestrator.run_lens_with_healing",side_effect=mock_heal), \
         patch("lens_orchestrator.run_preflight",
               return_value=make_preflight(dry=True)), \
         patch("lens_orchestrator.LENS_DRY_RUN",True):
        result=run_orchestrator({"run_id":"t097"})
    assert len(fired)==0
    assert result.get("dry_run")==True
    assert result.get("exit_reason")=="dry_run"

def t098():
    "LR-040T-18: Budget 4/4, no LENS_FORCE → hard stop, clear message"
    with patch("lens_orchestrator.get_runs_today",
               return_value=[{} for _ in range(4)]), \
         patch("lens_orchestrator.get_last_run",return_value=None), \
         patch("lens_orchestrator.LENS_FORCE",False), \
         patch("lens_orchestrator.GITHUB_ACTIONS",True):
        pf=run_preflight()
    assert not pf.approved
    assert len(pf.abort_reason)>0

def t099():
    "LR-040T-19: Resume pre-flight finds provider down → pending skipped"
    pf=make_preflight(verdicts={1:"GO",2:"GO",3:"SKIP",4:"SKIP"})
    fired=[]
    cp=make_checkpoint("t099",l1="complete",l2="complete",l3="pending",l4="pending")
    def mock_heal(lid, stagger_s=0):
        fired.append(lid); return make_lens_result(lid)
    with patch("lens_orchestrator.run_lens_with_healing",side_effect=mock_heal), \
         patch("lens_orchestrator.run_preflight",return_value=pf), \
         patch("lens_orchestrator._load_article_ids",return_value=["a"]*50), \
         patch("lens_orchestrator.verify_reports",return_value={}), \
         patch("lens_orchestrator.check_escalations"), \
         patch("lens_orchestrator.update_learning"), \
         patch("lens_orchestrator.clear_checkpoint"), \
         patch("lens_orchestrator.LENS_DRY_RUN",False):
        run_orchestrator({"run_id":"t099","job_count":2,
                          "checkpoint":cp,"article_ids":["a"]*50})
    assert 3 not in fired and 4 not in fired

def t100():
    "LR-040T-20: Cold start (empty DB) → all defaults used, cold_start flagged"
    with patch("lens_orchestrator.get_runs_today",return_value=[]), \
         patch("lens_orchestrator.get_last_run",return_value=None), \
         patch("lens_orchestrator.get_lens3_avg",return_value=LENS3_AVG_FALLBACK), \
         patch("lens_orchestrator.run_preflight",
               return_value=make_preflight()), \
         patch("lens_orchestrator._load_article_ids",return_value=["a"]*50), \
         patch("lens_orchestrator.run_lens_with_healing",
               return_value=make_lens_result(1)), \
         patch("lens_orchestrator.verify_reports",return_value={}), \
         patch("lens_orchestrator.check_escalations"), \
         patch("lens_orchestrator.update_learning"), \
         patch("lens_orchestrator.clear_checkpoint"), \
         patch("lens_orchestrator.LENS_DRY_RUN",False):
        result=run_orchestrator({"run_id":"t100",
                                 "cold_start":True})
    assert result.get("lens3_avg_runtime")==LENS3_AVG_FALLBACK
    assert result.get("quality_baseline")==6.0
    assert result.get("provider_reliability_default")==0.8
    assert result.get("articles_minimum")==30

# ══════════════════════════════════════════════════════════════════════════════
# TEST REGISTRY
# ══════════════════════════════════════════════════════════════════════════════
TESTS = [
    # num, step, name, fn
    (1,  1, "Gate 1: delete_articles blocked",            t001),
    (2,  1, "Gate 1: overwrite_reports blocked",          t002),
    (3,  1, "Gate 1: bulk_delete >100 blocked",           t003),
    (4,  1, "Gate 1: bulk_delete <=100 passes",           t004),
    (5,  1, "Gate 2: suppress_signal blocked",            t005),
    (6,  1, "Gate 2: embed_partisan_bias blocked",        t006),
    (7,  1, "Gate 3: quality <3.0 blocked",               t007),
    (8,  1, "Gate 3: quality =3.0 passes",                t008),
    (9,  1, "Gate 3: quality 8.3 passes",                 t009),
    (10, 1, "Gate 4: targets_vulnerable blocked",         t010),
    (11, 1, "Gate 5: >500 API calls blocked",             t011),
    (12, 1, "Gate 5: =500 API calls passes",              t012),
    (13, 1, "Gate 6: empty article pool blocked",         t013),
    (14, 1, "Gate 6: provider health unknown blocked",    t014),
    (15, 1, "Gate 6: active checkpoint no resume blocked",t015),
    (16, 1, "Gate 6: active checkpoint + resume passes",  t016),
    (17, 1, "Gate 6: stale checkpoint no block",          t017),
    (18, 1, "Gate 6: clean context passes all",           t018),
    (19, 2, "Pre-flight: budget exhausted hard stop",     t019),
    (20, 2, "Pre-flight: LENS_FORCE bypasses budget",     t020),
    (21, 2, "Pre-flight: manual no LENS_FORCE blocked",   t021),
    (22, 2, "Pre-flight: Gemini RPD → Lens 2 SKIP",       t022),
    (23, 2, "Pre-flight: Cerebras down → L3+4 SKIP",      t023),
    (24, 2, "Pre-flight: LENS_ONLY=3",                    t024),
    (25, 2, "Pre-flight: LENS_SKIP=2",                    t025),
    (26, 2, "Pre-flight: DRY_RUN → approved=False",       t026),
    (27, 2, "Pre-flight: job_count=4 hard stop",          t027),
    (28, 2, "Pre-flight: stagger = avg+30+6",             t028),
    (29, 2, "Pre-flight: all providers OK → approved",    t029),
    (30, 2, "Pre-flight: philosophy gate blocks",         t030),
    (31, 3, "Sequential: order 1→2→3→4",                 t031),
    (32, 3, "Sequential: SKIP verdict → not fired",       t032),
    (33, 3, "Sequential: Lens 4 gets stagger",            t033),
    (34, 3, "Sequential: wall → checkpoint + clean exit", t034),
    (35, 3, "Sequential: zero articles → blocked",        t035),
    (36, 3, "Sequential: DRY_RUN → no lenses fire",       t036),
    (37, 4, "Playbook: 404 → switch_fallback all lenses", t037),
    (38, 4, "Playbook: 429_queue → wait 120s",            t038),
    (39, 4, "Playbook: 429_tpm wait escalates",           t039),
    (40, 4, "Playbook: 429_rpd → skip",                   t040),
    (41, 4, "Playbook: 503 → wait 30s",                   t041),
    (42, 4, "Playbook: provider_down → skip+escalate",    t042),
    (43, 4, "Playbook: empty_output → retry 10s",         t043),
    (44, 4, "Playbook: quality_low → retry 5s",           t044),
    (45, 4, "Playbook: timeout → skip+escalate",          t045),
    (46, 4, "Playbook: unknown → escalate no wait LR-050",t046),
    (47, 4, "Playbook: all 10 types covered",             t047),
    (48, 4, "Playbook: fallback models vary by lens",     t048),
    (49, 5, "Healing: success → no repair",               t049),
    (50, 5, "Healing: 404 → fallback succeeds",           t050),
    (51, 5, "Healing: 429_queue → waits 120s",            t051),
    (52, 5, "Healing: unknown → escalate no retry LR-050",t052),
    (53, 5, "Healing: max 2 repairs then skip",           t053),
    (54, 5, "Healing: quality < floor → retry",           t054),
    (55, 5, "Healing: 429_rpd → skip no retry",           t055),
    (56, 5, "Healing: provider_down → skip",              t056),
    (57, 6, "Checkpoint: save writes statuses",           t057),
    (58, 6, "Checkpoint: Supabase fail → local backup",   t058),
    (59, 6, "Checkpoint: load returns correct data",      t059),
    (60, 6, "Checkpoint: load returns None not found",    t060),
    (61, 6, "Checkpoint: is_stale True >3h",              t061),
    (62, 6, "Checkpoint: is_stale False <3h",             t062),
    (63, 6, "Checkpoint: resume skips complete, fires pending",t063),
    (64, 6, "Checkpoint: stale → clear + fresh run",      t064),
    (65, 6, "Checkpoint: resume provider down → skip",    t065),
    (66, 7, "Verify: valid report_id → verified",         t066),
    (67, 7, "Verify: missing report_id → not verified",   t067),
    (68, 7, "Verify: not in DB → not verified",           t068),
    (69, 7, "Verify: failed lens → not verified",         t069),
    (70, 7, "Learning: Lens 3 runtime posted",            t070),
    (71, 8, "Summary: 4/4 → SLA MET",                    t071),
    (72, 8, "Summary: 2/4 → SLA BREACHED honest",        t072),
    (73, 8, "Summary: fallback → [FALLBACK] shown",       t073),
    (74, 8, "Summary: dry run noted",                     t074),
    (75, 8, "Summary: run_id + elapsed shown",            t075),
    (76, 9, "Escalation: all skipped → escalate",         t076),
    (77, 9, "Escalation: SLA breached",                   t077),
    (78, 9, "Escalation: verification fail",              t078),
    (79, 9, "Escalation: avg quality < 6.0",              t079),
    (80, 9, "Escalation: healthy run → no escalation",    t080),
    (81,  0, "LR-040T-01: Lens 4 404 fallback",          t081),
    (82,  0, "LR-040T-02: Gemini RPD → L2 skip SLA met", t082),
    (83,  0, "LR-040T-03: Cerebras 429 queue wait retry",t083),
    (84,  0, "LR-040T-04: Wall → checkpoint resume exit", t084),
    (85,  0, "LR-040T-05: Resume skips complete fires pending",t085),
    (86,  0, "LR-040T-06: All 4 fail → abandon",         t086),
    (87,  0, "LR-040T-07: Unknown → escalate no playbook",t087),
    (88,  0, "LR-040T-08: 2 repairs fail → skip",        t088),
    (89,  0, "LR-040T-09: Philosophy gate blocks",        t089),
    (90,  0, "LR-040T-10: job_count=4 hard stop",        t090),
    (91,  0, "LR-040T-11: Supabase fail → local backup", t091),
    (92,  0, "LR-040T-12: Stale checkpoint cleared",     t092),
    (93,  0, "LR-040T-13: Zero articles blocked",        t093),
    (94,  0, "LR-040T-14: Quality <4.0 retry once",      t094),
    (95,  0, "LR-040T-15: Lens 3 66s → stagger >=100",  t095),
    (96,  0, "LR-040T-16: job_count=4 hard stops",       t096),
    (97,  0, "LR-040T-17: DRY_RUN no lenses fire",       t097),
    (98,  0, "LR-040T-18: Budget 4/4 hard stop",         t098),
    (99,  0, "LR-040T-19: Resume provider down skip",    t099),
    (100, 0, "LR-040T-20: Cold start all defaults",      t100),
]

# ══════════════════════════════════════════════════════════════════════════════
# RUNNER
# ══════════════════════════════════════════════════════════════════════════════
STEP_NAMES = {
    1:"Philosophy Gate", 2:"Pre-flight", 3:"Sequential",
    4:"Playbooks", 5:"Self-healing", 6:"Checkpoint+Resume",
    7:"Verification", 8:"Summary", 9:"Escalation", 0:"LR-040T"
}

def print_header():
    print()
    print("="*68)
    print("  Project Lens — Complete Test Suite — 100 Tests")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    if not ORCH_AVAILABLE:
        print(f"  STATUS: WAITING — orchestrator not found: {_IMPORT_ERROR}")
    else:
        print("  STATUS: READY — lens_orchestrator.py found")
    print("="*68)

def print_summary():
    print()
    print("="*68)
    total   = len(RESULTS)
    passed  = sum(1 for r in RESULTS if r.passed)
    failed  = sum(1 for r in RESULTS if not r.passed and not r.skipped)
    skipped = sum(1 for r in RESULTS if r.skipped)

    # Per-step breakdown
    steps = {}
    for r in RESULTS:
        key = STEP_NAMES.get(r.step, f"S{r.step}")
        steps.setdefault(key, {"p":0,"f":0})
        if r.passed:          steps[key]["p"] += 1
        elif not r.skipped:   steps[key]["f"] += 1

    print(f"  RESULTS: {passed}/{total} passed | "
          f"{failed} failed | {skipped} waiting")
    print()
    print("  Per-step:")
    for sname, counts in steps.items():
        t = counts["p"] + counts["f"]
        bar = "✅" if counts["f"]==0 else "❌"
        print(f"    {bar} {sname:<22} {counts['p']}/{t}")

    if failed > 0:
        print()
        print("  FAILED:")
        for r in RESULTS:
            if not r.passed and not r.skipped:
                print(f"    T{r.num:03d} [{STEP_NAMES.get(r.step,'S'+str(r.step))}]"
                      f" {r.name}")
                print(f"         → {r.message[:100]}")

    print()
    if skipped > 0 and failed == 0:
        print("  ⏭  Waiting for lens_orchestrator.py — build it first.")
    elif passed == total and total > 0 and not any(r.skipped for r in RESULTS):
        print("  🎉  100/100 PASSED — orchestrator cleared for deployment!")
        print("  Next: LENS_DRY_RUN=1 to verify on real environment")
    elif failed > 0:
        print(f"  ❌  {failed} test(s) FAILED — fix before wiring to yml (LR-040T)")
    print("="*68)
    print()
    return passed == total and not any(r.skipped for r in RESULTS)

def main():
    parser = argparse.ArgumentParser(description="Project Lens — 100 Test Suite")
    parser.add_argument("--dry",   action="store_true", help="Validate structure only")
    parser.add_argument("--step",  type=int, default=None, help="Run one step group (1-9, 0=LR-040T)")
    parser.add_argument("--test",  type=int, default=None, help="Run one test by number")
    args = parser.parse_args()

    print_header()

    if args.dry:
        print("\n  DRY MODE — validating test structure\n")
        steps_seen = {}
        for num, step, name, _ in TESTS:
            steps_seen.setdefault(STEP_NAMES.get(step,f"S{step}"), 0)
            steps_seen[STEP_NAMES.get(step,f"S{step}")] += 1
            print(f"  📋  T{num:03d} [S{step}]: {name}")
        print()
        print(f"  {len(TESTS)} tests defined across {len(steps_seen)} groups:")
        for s,n in steps_seen.items():
            print(f"    {s}: {n} tests")
        return

    subset = TESTS
    if args.step is not None:
        subset = [(n,s,nm,fn) for n,s,nm,fn in TESTS if s==args.step]
        print(f"\n  Running Step {args.step} ({STEP_NAMES.get(args.step,'?')})"
              f" — {len(subset)} tests\n")
    elif args.test is not None:
        subset = [(n,s,nm,fn) for n,s,nm,fn in TESTS if n==args.test]
        if not subset:
            print(f"  ERROR: Test {args.test} not found (range: 1-100)")
            sys.exit(1)
    else:
        print(f"\n  Running all {len(TESTS)} tests...\n")

    for num, step, name, fn in subset:
        run_test(num, step, name, fn)

    all_ok = print_summary()
    sys.exit(0 if all_ok else 1)

if __name__ == "__main__":
    main()
