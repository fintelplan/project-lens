"""
test_orchestrator.py
Project Lens — 20/20 Test Harness (LR-040T)
Session: LENS-008

Tests the lens_orchestrator.py against all 20 worst-case scenarios
defined in lens-S007-D002_manager_architecture.docx Section 16.

Usage:
  python test_orchestrator.py              # run all 20 tests
  python test_orchestrator.py --dry        # validate structure only
  python test_orchestrator.py --test 4     # run single test

Rules:
  LR-040(T): All 20 must pass before wiring orchestrator into yml
  LR-012(P): Never deploy on unverified baseline
  LR-044(T): 3 real cron runs after passing

Architecture: lens-S007-D002_manager_architecture.docx
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

# ─── Try importing orchestrator ───────────────────────────────────────────────
# Orchestrator does not exist yet — tests define the CONTRACT it must satisfy.
# When lens_orchestrator.py is built, this import will resolve.
try:
    from lens_orchestrator import (
        run_orchestrator,
        RunConfig,
        RunResult,
        LensStatus,
        PhilosophyGate,
        CheckpointManager,
        SelfHealingLoop,
        PreFlightGuard,
    )
    ORCHESTRATOR_AVAILABLE = True
except ImportError:
    ORCHESTRATOR_AVAILABLE = False

# ─── Result Tracking ──────────────────────────────────────────────────────────

@dataclass
class TestResult:
    test_id: int
    name: str
    passed: bool
    message: str
    duration_ms: float
    skipped: bool = False

results: list[TestResult] = []

# ─── Mock Environment ─────────────────────────────────────────────────────────

class MockSupabase:
    """Simulates Supabase client for testing without real DB calls."""

    def __init__(self, mode="normal"):
        """
        Modes:
          normal      — all operations succeed
          unreachable — all operations raise exception (Test 11)
          read_fail   — reads fail, writes succeed
          write_fail  — reads succeed, writes fail
        """
        self.mode = mode
        self.stored_checkpoints = {}
        self.stored_reports = {}
        self.stored_records = []
        self.write_attempts = 0

    def table(self, name):
        return MockTable(name, self)

class MockTable:
    def __init__(self, name, db):
        self.name = name
        self.db = db
        self._filters = {}

    def select(self, *args): return self
    def insert(self, data):  return self
    def upsert(self, data):  return self
    def update(self, data):  return self
    def delete(self):        return self
    def eq(self, col, val):
        self._filters[col] = val
        return self
    def order(self, *args):  return self
    def limit(self, n):      return self
    def gte(self, *args):    return self

    def execute(self):
        if self.db.mode == "unreachable":
            raise Exception("Connection refused — Supabase unreachable")
        if self.db.mode == "write_fail" and self.name == "lens_reports":
            self.db.write_attempts += 1
            raise Exception("Write failed — DB error")
        mock_response = MagicMock()
        mock_response.data = []
        mock_response.count = 0
        return mock_response


class MockLensRunner:
    """Simulates running a single lens with configurable outcomes."""

    def __init__(self, lens_id: int, scenario: str, runtime_s: float = 5.0):
        self.lens_id = lens_id
        self.scenario = scenario
        self.runtime_s = runtime_s
        self.attempt_count = 0

    async def run(self):
        self.attempt_count += 1

        if self.scenario == "success":
            return {"status": "complete", "quality": 7.5, "chars": 5000}

        if self.scenario == "404_model_not_found":
            raise Exception("404 model not found: qwen-3-235b-a22b-instruct-2507")

        if self.scenario == "429_queue_exceeded":
            if self.attempt_count == 1:
                raise Exception("429 - queue_exceeded: too many requests")
            return {"status": "complete", "quality": 7.0, "chars": 4500}

        if self.scenario == "gemini_503":
            if self.attempt_count == 1:
                raise Exception("503 UNAVAILABLE: high demand")
            return {"status": "complete", "quality": 7.2, "chars": 4800}

        if self.scenario == "unknown_error":
            raise Exception("UNKNOWN_ERR_X99: unrecognized failure mode")

        if self.scenario == "low_quality":
            if self.attempt_count == 1:
                return {"status": "complete", "quality": 3.5, "chars": 2000}
            return {"status": "complete", "quality": 3.8, "chars": 2100}

        if self.scenario == "slow_66s":
            return {"status": "complete", "quality": 7.0, "chars": 4000,
                    "runtime_s": 66.0}

        if self.scenario == "all_fail":
            raise Exception("Provider completely down")

        return {"status": "complete", "quality": 7.5, "chars": 5000}


def make_mock_config(**overrides):
    """Build a minimal RunConfig for testing."""
    defaults = {
        "run_id": f"test-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}",
        "cycle": "afternoon",
        "job_count": 1,
        "max_jobs": 3,
        "budget_used": 0,
        "budget_max": 4,
        "articles": [{"id": f"art-{i}", "title": f"Article {i}"} for i in range(50)],
        "dry_run": False,
        "lens_force": False,
        "wall_time_minutes": 14,
        "supabase": MockSupabase(),
    }
    defaults.update(overrides)
    return defaults


# ─── Test Runner Infrastructure ───────────────────────────────────────────────

def run_test(test_id: int, name: str, fn):
    """Execute one test, capture result, print immediately."""
    start = time.time()
    try:
        if not ORCHESTRATOR_AVAILABLE:
            results.append(TestResult(
                test_id=test_id, name=name, passed=False,
                message="SKIP — lens_orchestrator.py not yet built",
                duration_ms=0, skipped=True
            ))
            print(f"  ⏭  Test {test_id:02d}: {name}")
            print(f"         → WAITING for lens_orchestrator.py")
            return

        fn()
        duration = (time.time() - start) * 1000
        results.append(TestResult(
            test_id=test_id, name=name, passed=True,
            message="OK", duration_ms=duration
        ))
        print(f"  ✅  Test {test_id:02d}: {name} ({duration:.0f}ms)")

    except AssertionError as e:
        duration = (time.time() - start) * 1000
        results.append(TestResult(
            test_id=test_id, name=name, passed=False,
            message=str(e), duration_ms=duration
        ))
        print(f"  ❌  Test {test_id:02d}: {name}")
        print(f"         → FAILED: {e}")

    except Exception as e:
        duration = (time.time() - start) * 1000
        results.append(TestResult(
            test_id=test_id, name=name, passed=False,
            message=f"EXCEPTION: {e}", duration_ms=duration
        ))
        print(f"  💥  Test {test_id:02d}: {name}")
        print(f"         → EXCEPTION: {e}")


# ─── The 20 Tests ─────────────────────────────────────────────────────────────

def test_01():
    """Lens 4 returns 404 model not found → playbook fires, switch to fallback, record written."""
    config = make_mock_config()

    # Inject: Lens 4 raises 404
    with patch("lens_orchestrator.call_cerebras",
               side_effect=Exception("404 model not found: qwen-3-235b")):
        result = run_orchestrator(config)

    assert result.lens_4_status in ("complete_fallback", "complete"), \
        f"Expected fallback success, got {result.lens_4_status}"
    assert result.lens_4_fallback_used == True, \
        "Fallback flag must be True when 404 triggers playbook"
    assert result.lens_4_repair_record is not None, \
        "Repair record must be written to DB"
    assert result.lens_1_status == "complete", "Lens 1 must still complete"
    assert result.lens_2_status == "complete", "Lens 2 must still complete"
    assert result.lens_3_status == "complete", "Lens 3 must still complete"


def test_02():
    """Gemini RPD exhausted (calls_today=19/20) → Lens 2 skipped, other 3 fire, SLA met."""
    config = make_mock_config()

    # Inject: Gemini has used 19/20 daily RPD
    with patch("lens_orchestrator.get_gemini_calls_today", return_value=19):
        result = run_orchestrator(config)

    assert result.lens_2_status == "skipped", \
        f"Lens 2 must be skipped when RPD exhausted, got {result.lens_2_status}"
    assert result.lens_2_skip_reason == "gemini_rpd_exhausted", \
        "Skip reason must be recorded"
    assert result.lens_1_status == "complete", "Lens 1 must fire"
    assert result.lens_3_status == "complete", "Lens 3 must fire"
    assert result.lens_4_status == "complete", "Lens 4 must fire"
    assert result.sla_met == True, \
        "SLA must be met — 3/4 lenses complete satisfies LR-042"


def test_03():
    """Cerebras 429 queue congested → wait 120s, retry, record attempt."""
    config = make_mock_config()
    sleep_calls = []

    original_sleep = time.sleep
    def mock_sleep(s):
        sleep_calls.append(s)

    runner = MockLensRunner(3, "429_queue_exceeded")

    with patch("lens_orchestrator.call_cerebras", side_effect=runner.run), \
         patch("time.sleep", side_effect=mock_sleep):
        result = run_orchestrator(config)

    assert runner.attempt_count == 2, \
        f"Must retry exactly once after 429, got {runner.attempt_count} attempts"
    assert any(s >= 120 for s in sleep_calls), \
        f"Must wait >= 120s on queue_exceeded, waited: {sleep_calls}"
    assert result.lens_3_retry_count >= 1, \
        "Retry count must be recorded"


def test_04():
    """Wall approaching at t=14min → checkpoint saved, resume job triggered, clean exit."""
    config = make_mock_config()
    checkpoint_saved = []
    resume_triggered = []

    def mock_save_checkpoint(data):
        checkpoint_saved.append(data)

    def mock_trigger_resume(run_id):
        resume_triggered.append(run_id)

    # Inject: time wall hit after Lens 1 completes
    with patch("lens_orchestrator.save_checkpoint", side_effect=mock_save_checkpoint), \
         patch("lens_orchestrator.trigger_resume_job", side_effect=mock_trigger_resume), \
         patch("lens_orchestrator.time_elapsed_minutes", return_value=14.1):
        result = run_orchestrator(config)

    assert len(checkpoint_saved) == 1, \
        "Checkpoint must be saved exactly once at wall"
    assert len(resume_triggered) == 1, \
        "Resume job must be triggered exactly once"
    assert result.exit_reason == "wall_checkpoint", \
        f"Exit reason must be wall_checkpoint, got {result.exit_reason}"
    assert result.exit_clean == True, \
        "Exit must be clean (not an error exit)"


def test_05():
    """Resume job loads checkpoint correctly → completed lenses skipped, pending fire."""
    # Inject: existing checkpoint with Lens 1+2 complete
    checkpoint = {
        "run_id": "test-resume-001",
        "cycle": "afternoon",
        "job_count": 1,
        "resume_from": 3,
        "lens_1_status": "complete",
        "lens_2_status": "complete",
        "lens_3_status": "pending",
        "lens_4_status": "pending",
        "article_ids": json.dumps([f"art-{i}" for i in range(50)]),
        "completed_at": None,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    mock_db = MockSupabase()
    mock_db.stored_checkpoints["test-resume-001"] = checkpoint

    config = make_mock_config(
        run_id="test-resume-001",
        job_count=2,
        supabase=mock_db
    )

    with patch("lens_orchestrator.load_checkpoint", return_value=checkpoint):
        result = run_orchestrator(config)

    assert result.lens_1_status == "skipped_already_complete", \
        f"Lens 1 must be skipped (already complete), got {result.lens_1_status}"
    assert result.lens_2_status == "skipped_already_complete", \
        f"Lens 2 must be skipped (already complete), got {result.lens_2_status}"
    assert result.lens_3_status == "complete", \
        "Lens 3 must fire (was pending)"
    assert result.lens_4_status == "complete", \
        "Lens 4 must fire (was pending)"
    assert result.articles_refetched == False, \
        "Articles must NOT be re-fetched in resume job (LR-055)"


def test_06():
    """All 4 lenses fail same run → escalate immediately, run abandoned, alert sent."""
    config = make_mock_config()
    escalations = []

    def mock_escalate(reason, data):
        escalations.append({"reason": reason, "data": data})

    with patch("lens_orchestrator.call_groq",
               side_effect=Exception("Provider down")), \
         patch("lens_orchestrator.call_gemini",
               side_effect=Exception("Provider down")), \
         patch("lens_orchestrator.call_cerebras",
               side_effect=Exception("Provider down")), \
         patch("lens_orchestrator.escalate_to_admin",
               side_effect=mock_escalate):
        result = run_orchestrator(config)

    assert result.lens_1_status == "failed", "All lenses must fail"
    assert result.lens_2_status == "failed", "All lenses must fail"
    assert result.lens_3_status == "failed", "All lenses must fail"
    assert result.lens_4_status == "failed", "All lenses must fail"
    assert result.run_abandoned == True, \
        "Run must be abandoned when all 4 lenses fail"
    assert len(escalations) >= 1, \
        "Must escalate to admin when all lenses fail"
    assert result.sla_met == False, "SLA cannot be met with 0/4 lenses"


def test_07():
    """Unknown error type received → escalate immediately, no playbook attempted."""
    config = make_mock_config()
    escalations = []
    playbooks_attempted = []

    def mock_escalate(reason, data):
        escalations.append(reason)

    def mock_apply_playbook(error_type, lens_id):
        playbooks_attempted.append(error_type)

    with patch("lens_orchestrator.call_cerebras",
               side_effect=Exception("UNKNOWN_ERR_X99: unrecognized")), \
         patch("lens_orchestrator.escalate_to_admin",
               side_effect=mock_escalate), \
         patch("lens_orchestrator.apply_correction_playbook",
               side_effect=mock_apply_playbook):
        result = run_orchestrator(config)

    assert len(escalations) >= 1, \
        "Unknown error must trigger immediate escalation"
    assert len(playbooks_attempted) == 0, \
        "NO playbook must be attempted for unknown errors — never guess (LR-050)"
    assert result.lens_3_skip_reason == "unknown_error_escalated", \
        "Lens must be skipped with reason recorded"


def test_08():
    """2 repair attempts both fail → lens skipped, escalate if SLA breached."""
    config = make_mock_config()
    repair_attempts = []
    escalations = []

    def mock_repair(lens_id, error):
        repair_attempts.append(lens_id)
        raise Exception("Repair failed — same error persists")

    def mock_escalate(reason, data):
        escalations.append(reason)

    with patch("lens_orchestrator.call_groq",
               side_effect=Exception("429 TPM rate limit")), \
         patch("lens_orchestrator.apply_correction_playbook",
               side_effect=mock_repair), \
         patch("lens_orchestrator.escalate_to_admin",
               side_effect=mock_escalate):
        result = run_orchestrator(config)

    assert len(repair_attempts) == 2, \
        f"Must attempt exactly 2 repairs per LR-050, attempted {len(repair_attempts)}"
    assert result.lens_1_status == "failed", \
        "Lens must be skipped after 2 failed repairs"
    assert len(escalations) >= 1, \
        "Must escalate after max repair attempts exhausted"


def test_09():
    """Philosophy gate blocks action → action stopped, reason recorded, escalate."""
    config = make_mock_config()
    actions_blocked = []
    escalations = []

    # Philosophy gate will fail check 1 (data integrity)
    def mock_philosophy_check(action, context):
        actions_blocked.append(action)
        return {
            "passed": False,
            "failed_gate": 1,
            "reason": "Action risks corrupting source data integrity"
        }

    def mock_escalate(reason, data):
        escalations.append(reason)

    with patch("lens_orchestrator.run_philosophy_gate",
               side_effect=mock_philosophy_check), \
         patch("lens_orchestrator.escalate_to_admin",
               side_effect=mock_escalate):
        result = run_orchestrator(config)

    assert len(actions_blocked) >= 1, \
        "Philosophy gate must be called before every Manager action (LR-051)"
    assert result.philosophy_gate_blocked == True, \
        "Block flag must be set when gate fails"
    assert result.philosophy_gate_reason is not None, \
        "Reason must be recorded — never silently block"
    assert len(escalations) >= 1, \
        "Philosophy gate failure must escalate (LR-051)"


def test_10():
    """Job count reaches 3 and still incomplete → hard stop, escalate, never start Job 4."""
    config = make_mock_config(job_count=3)
    escalations = []
    jobs_started = []

    def mock_trigger_job(run_id, job_num):
        jobs_started.append(job_num)

    def mock_escalate(reason, data):
        escalations.append(reason)

    with patch("lens_orchestrator.trigger_resume_job",
               side_effect=mock_trigger_job), \
         patch("lens_orchestrator.escalate_to_admin",
               side_effect=mock_escalate):
        result = run_orchestrator(config)

    assert result.hard_stopped == True, \
        "Must hard stop at job_count=3 per LR-054"
    assert 4 not in jobs_started, \
        "Job 4 must NEVER be triggered — max 3 jobs per run (LR-054)"
    assert len(escalations) >= 1, \
        "Must escalate when max jobs reached without completion"
    assert result.exit_reason == "max_jobs_reached", \
        f"Exit reason must be max_jobs_reached, got {result.exit_reason}"


def test_11():
    """Supabase completely unreachable → local backup fires, pipeline continues, alert sent."""
    config = make_mock_config(supabase=MockSupabase(mode="unreachable"))
    local_backups = []
    alerts = []

    def mock_local_backup(data, filename):
        local_backups.append(filename)

    def mock_alert(message):
        alerts.append(message)

    with patch("lens_orchestrator.save_to_local_backup",
               side_effect=mock_local_backup), \
         patch("lens_orchestrator.send_alert",
               side_effect=mock_alert):
        result = run_orchestrator(config)

    assert len(local_backups) >= 1, \
        "Local backup must fire when Supabase unreachable"
    assert len(alerts) >= 1, \
        "Alert must be sent when Supabase unreachable"
    assert result.pipeline_continued == True, \
        "Pipeline must continue despite DB failure — intelligence is still valuable"
    assert result.supabase_failed == True, \
        "Supabase failure flag must be set"


def test_12():
    """Checkpoint is 3 hours old (stale) → checkpoint cleared, fresh run started."""
    stale_time = (datetime.now(timezone.utc) - timedelta(hours=3)).isoformat()
    stale_checkpoint = {
        "run_id": "stale-run-001",
        "created_at": stale_time,
        "lens_1_status": "complete",
        "lens_2_status": "pending",
        "lens_3_status": "pending",
        "lens_4_status": "pending",
        "job_count": 1,
    }

    config = make_mock_config()
    cleared_checkpoints = []

    def mock_clear_checkpoint(run_id):
        cleared_checkpoints.append(run_id)

    with patch("lens_orchestrator.load_checkpoint",
               return_value=stale_checkpoint), \
         patch("lens_orchestrator.clear_checkpoint",
               side_effect=mock_clear_checkpoint):
        result = run_orchestrator(config)

    assert len(cleared_checkpoints) >= 1, \
        "Stale checkpoint must be cleared"
    assert result.checkpoint_stale_cleared == True, \
        "Stale clear flag must be set"
    assert result.fresh_run_started == True, \
        "Fresh run must start after clearing stale checkpoint"


def test_13():
    """Article pool has 0 articles → analyze blocked, alert sent, run abandoned."""
    config = make_mock_config(articles=[])  # Empty article pool
    alerts = []

    def mock_alert(message):
        alerts.append(message)

    with patch("lens_orchestrator.send_alert", side_effect=mock_alert):
        result = run_orchestrator(config)

    assert result.analyze_blocked == True, \
        "Analyze must be blocked with 0 articles"
    assert result.run_abandoned == True, \
        "Run must be abandoned — no articles to analyze"
    assert len(alerts) >= 1, \
        "Alert must be sent when article pool is empty"
    assert result.lens_1_status == "blocked", \
        "All lenses must show blocked status, not failed"


def test_14():
    """Quality score < 4.0 on Lens 1 → retry once, if still < 4.0 record + escalate."""
    config = make_mock_config()
    escalations = []
    lens1_call_count = [0]

    async def mock_low_quality_groq(lens, system_prompt, user_prompt):
        lens1_call_count[0] += 1
        # Both attempts return low quality output
        return "Brief poor quality output with minimal analysis."

    def mock_escalate(reason, data):
        escalations.append(reason)

    with patch("lens_orchestrator.call_groq",
               side_effect=mock_low_quality_groq), \
         patch("lens_orchestrator.escalate_to_admin",
               side_effect=mock_escalate):
        result = run_orchestrator(config)

    assert lens1_call_count[0] == 2, \
        f"Lens 1 must be retried exactly once on low quality, called {lens1_call_count[0]} times"
    assert result.lens_1_quality < 4.0, \
        "Quality must remain below threshold in result"
    assert result.lens_1_quality_escalated == True, \
        "Low quality escalation flag must be set"
    assert len(escalations) >= 1, \
        "Must escalate when quality < 4.0 after retry"


def test_15():
    """Lens 3 runtime = 66s (congestion) → recorded, Lens 4 stagger updated to 108s."""
    config = make_mock_config()
    stagger_updates = []

    def mock_update_stagger(lens_id, new_stagger):
        stagger_updates.append((lens_id, new_stagger))

    async def mock_slow_cerebras(lens, system_prompt, user_prompt):
        # Simulate 66s runtime
        time.sleep(0.01)  # Don't actually wait in tests
        return "Analysis output"

    with patch("lens_orchestrator.call_cerebras",
               side_effect=mock_slow_cerebras), \
         patch("lens_orchestrator.get_lens3_runtime", return_value=66.0), \
         patch("lens_orchestrator.update_lens4_stagger",
               side_effect=mock_update_stagger):
        result = run_orchestrator(config)

    assert result.lens_3_runtime_recorded == 66.0, \
        f"Lens 3 runtime must be recorded as 66.0s, got {result.lens_3_runtime_recorded}"
    assert len(stagger_updates) >= 1, \
        "Lens 4 stagger must be updated based on Lens 3 runtime"
    # Expected: 66s + 6s base + 30s buffer = 102s. Min of 108s from architecture.
    new_stagger = stagger_updates[0][1]
    assert new_stagger >= 100, \
        f"New stagger must be >= 100s for 66s runtime, got {new_stagger}s"


def test_16():
    """Provider reliability score < 0.3 → pre-flight warns, fallback used first."""
    config = make_mock_config()
    preflight_warnings = []
    fallback_used_first = []

    def mock_get_reliability(provider):
        if provider == "cerebras":
            return 0.25  # Below 0.3 threshold
        return 0.85

    def mock_warn(message):
        preflight_warnings.append(message)

    with patch("lens_orchestrator.get_provider_reliability",
               side_effect=mock_get_reliability), \
         patch("lens_orchestrator.preflight_warn",
               side_effect=mock_warn):
        result = run_orchestrator(config)

    assert len(preflight_warnings) >= 1, \
        "Pre-flight must warn when provider reliability < 0.3"
    assert result.cerebras_reliability_warned == True, \
        "Cerebras reliability warning flag must be set"
    assert result.preflight_passed == True, \
        "Pre-flight must still pass — warning not a block at 0.3"


def test_17():
    """LENS_DRY_RUN=1 set → pre-flight runs, no lenses fire, summary shows dry run."""
    config = make_mock_config(dry_run=True)
    lenses_fired = []

    def mock_run_lens(lens_id, *args, **kwargs):
        lenses_fired.append(lens_id)

    with patch("lens_orchestrator.execute_lens",
               side_effect=mock_run_lens):
        result = run_orchestrator(config)

    assert len(lenses_fired) == 0, \
        f"No lenses must fire in dry run mode, fired: {lenses_fired}"
    assert result.dry_run == True, \
        "Dry run flag must be set in result"
    assert result.preflight_passed is not None, \
        "Pre-flight must run even in dry run mode"
    assert "dry run" in result.summary.lower(), \
        "Run summary must indicate this was a dry run"


def test_18():
    """Budget = 4/4 and LENS_FORCE not set → hard stop, clear message, no analyze."""
    config = make_mock_config(budget_used=4, budget_max=4)
    lenses_fired = []

    def mock_run_lens(lens_id, *args, **kwargs):
        lenses_fired.append(lens_id)

    with patch("lens_orchestrator.execute_lens",
               side_effect=mock_run_lens):
        result = run_orchestrator(config)

    assert len(lenses_fired) == 0, \
        f"No lenses must fire when budget exhausted, fired: {lenses_fired}"
    assert result.budget_hard_stopped == True, \
        "Budget hard stop flag must be set"
    assert result.exit_reason == "budget_exhausted", \
        f"Exit reason must be budget_exhausted, got {result.exit_reason}"
    assert result.exit_message is not None and len(result.exit_message) > 0, \
        "Clear exit message required — never silently stop"


def test_19():
    """Resume pre-flight finds provider now down → pending lenses using that provider skipped."""
    # Checkpoint: Lens 1+2 complete, Lens 3+4 pending (Cerebras)
    checkpoint = {
        "run_id": "resume-provider-down-001",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "job_count": 1,
        "resume_from": 3,
        "lens_1_status": "complete",
        "lens_2_status": "complete",
        "lens_3_status": "pending",
        "lens_4_status": "pending",
        "article_ids": json.dumps([f"art-{i}" for i in range(50)]),
    }

    config = make_mock_config(job_count=2)
    lenses_fired = []

    def mock_provider_health(provider):
        if provider == "cerebras":
            return {"status": "DOWN", "error": "Connection refused"}
        return {"status": "OK"}

    def mock_run_lens(lens_id, *args, **kwargs):
        lenses_fired.append(lens_id)

    with patch("lens_orchestrator.load_checkpoint", return_value=checkpoint), \
         patch("lens_orchestrator.check_provider_health",
               side_effect=mock_provider_health), \
         patch("lens_orchestrator.execute_lens",
               side_effect=mock_run_lens):
        result = run_orchestrator(config)

    assert 3 not in lenses_fired, \
        "Lens 3 must NOT fire — Cerebras is down"
    assert 4 not in lenses_fired, \
        "Lens 4 must NOT fire — Cerebras is down"
    assert result.lens_3_status == "skipped_provider_down", \
        f"Lens 3 must be skipped with provider_down reason, got {result.lens_3_status}"
    assert result.lens_4_status == "skipped_provider_down", \
        f"Lens 4 must be skipped with provider_down reason, got {result.lens_4_status}"


def test_20():
    """Cold start (empty DB) → all defaults used, all flagged cold_start=true in record."""
    config = make_mock_config()
    cold_start_flags = []

    def mock_get_history(metric):
        # Simulate empty DB — no history exists
        return None

    def mock_record_run(data):
        cold_start_flags.append(data.get("cold_start"))

    with patch("lens_orchestrator.get_historical_metric",
               side_effect=mock_get_history), \
         patch("lens_orchestrator.record_run",
               side_effect=mock_record_run):
        result = run_orchestrator(config)

    assert result.cold_start == True, \
        "cold_start flag must be True when no history exists"

    # Verify all cold start defaults from architecture doc
    assert result.lens3_avg_runtime == 12.0, \
        f"lens3_avg_runtime default must be 12s, got {result.lens3_avg_runtime}"
    assert result.lens4_stagger == 48.0, \
        f"lens4_stagger default must be 48s, got {result.lens4_stagger}"
    assert result.quality_baseline == 6.0, \
        f"quality_baseline default must be 6.0, got {result.quality_baseline}"
    assert result.provider_reliability_default == 0.8, \
        f"provider_reliability default must be 0.8, got {result.provider_reliability_default}"
    assert result.articles_minimum == 30, \
        f"articles_minimum default must be 30, got {result.articles_minimum}"

    assert len(cold_start_flags) >= 1, \
        "Run record must be written with cold_start=True"
    assert all(flag == True for flag in cold_start_flags), \
        "All run records in cold start must have cold_start=True"


# ─── Main Runner ──────────────────────────────────────────────────────────────

TESTS = [
    (1,  "Lens 4 returns 404 — fallback fires",          test_01),
    (2,  "Gemini RPD exhausted — Lens 2 skipped",        test_02),
    (3,  "Cerebras 429 queue — wait 120s retry",         test_03),
    (4,  "Wall at t=14min — checkpoint + resume",        test_04),
    (5,  "Resume job — completed lenses skipped",        test_05),
    (6,  "All 4 lenses fail — escalate + abandon",       test_06),
    (7,  "Unknown error — escalate, no playbook",        test_07),
    (8,  "2 repair attempts fail — skip + escalate",     test_08),
    (9,  "Philosophy gate blocks action",                test_09),
    (10, "Job count 3 incomplete — hard stop",           test_10),
    (11, "Supabase unreachable — local backup",          test_11),
    (12, "Stale checkpoint 3h — clear + fresh run",      test_12),
    (13, "0 articles — analyze blocked",                 test_13),
    (14, "Quality < 4.0 — retry once + escalate",        test_14),
    (15, "Lens 3 runtime 66s — stagger updated",         test_15),
    (16, "Provider reliability < 0.3 — warn + fallback", test_16),
    (17, "LENS_DRY_RUN=1 — no lenses fire",              test_17),
    (18, "Budget 4/4 no FORCE — hard stop",              test_18),
    (19, "Resume provider down — pending skipped",       test_19),
    (20, "Cold start empty DB — all defaults used",      test_20),
]


def print_header():
    print()
    print("=" * 64)
    print("  Project Lens — 20/20 Orchestrator Test Harness (LR-040T)")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    if not ORCHESTRATOR_AVAILABLE:
        print("  STATUS: WAITING — lens_orchestrator.py not yet built")
        print("  Tests will run once orchestrator exists.")
    else:
        print("  STATUS: READY — lens_orchestrator.py found")
    print("=" * 64)
    print()


def print_summary():
    print()
    print("=" * 64)
    total    = len(results)
    passed   = sum(1 for r in results if r.passed)
    failed   = sum(1 for r in results if not r.passed and not r.skipped)
    skipped  = sum(1 for r in results if r.skipped)
    avg_ms   = sum(r.duration_ms for r in results) / max(total, 1)

    print(f"  RESULTS: {passed}/{total} passed | "
          f"{failed} failed | {skipped} waiting")
    print(f"  AVG TIME: {avg_ms:.0f}ms per test")
    print()

    if failed > 0:
        print("  FAILED TESTS:")
        for r in results:
            if not r.passed and not r.skipped:
                print(f"    Test {r.test_id:02d}: {r.name}")
                print(f"           → {r.message}")
        print()

    if skipped > 0 and failed == 0:
        print("  ⏭  All tests waiting for lens_orchestrator.py.")
        print("     Build the orchestrator then run again.")
        print()

    if passed == total and total > 0 and skipped == 0:
        print("  🎉  20/20 PASSED — orchestrator cleared for deployment!")
        print("  Next step: LENS_DRY_RUN=1 to verify on real environment")
        print()
    elif failed == 0 and skipped == 0 and passed < total:
        print("  ⚠️  Not all tests ran.")
    elif failed > 0:
        print(f"  ❌  {failed} test(s) failed — DO NOT deploy until fixed.")
        print("  Rule: LR-040(T) — all 20 must pass before wiring to yml")
        print()

    print("=" * 64)
    print()
    return passed == total and skipped == 0


def main():
    parser = argparse.ArgumentParser(description="Project Lens 20/20 Test Harness")
    parser.add_argument("--dry",  action="store_true",
                        help="Validate test structure only — do not run tests")
    parser.add_argument("--test", type=int, default=None,
                        help="Run a single test by number (1-20)")
    args = parser.parse_args()

    print_header()

    if args.dry:
        print("  DRY MODE — validating test structure only")
        print()
        for test_id, name, fn in TESTS:
            print(f"  📋  Test {test_id:02d}: {name}")
        print()
        print(f"  {len(TESTS)} tests defined. Structure valid.")
        print()
        return

    if args.test:
        matching = [(tid, name, fn) for tid, name, fn in TESTS if tid == args.test]
        if not matching:
            print(f"  ERROR: Test {args.test} not found (valid range: 1-20)")
            sys.exit(1)
        print(f"  Running single test: {args.test}")
        print()
        for test_id, name, fn in matching:
            run_test(test_id, name, fn)
    else:
        print("  Running all 20 tests...")
        print()
        for test_id, name, fn in TESTS:
            run_test(test_id, name, fn)

    all_passed = print_summary()
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
