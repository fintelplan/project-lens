# lens_quota_guard — Pre-flight Quota Guard

Project Lens | LENS-013 | Error-0 Design | Observer Mode MVP

## Purpose

Prevent the Run #29 class of failure: positions burning retry storms on an
already-exhausted provider quota, leaving downstream positions (MA, S3-A)
with no headroom. Before expensive calls fire, check the ledger, decide
PROCEED / DEGRADE / SKIP.

Imported from GNI Autonomous `ai_engine/quota_guard.py` pattern (GNI-R-112)
and adapted for Project Lens multi-position architecture.

## Philosophy (from DOC-006)

1. **Fail-safe, not fail-secure** — on guard error, PROCEED. Blocking a
   legitimate cron because the guard itself had a bug is worse than
   degraded telemetry.
2. **Fire-and-forget ledger** — if Supabase write fails, cron still
   proceeds. Telemetry is secondary.
3. **Conservative estimation** — when quota data is ambiguous, assume worse
   consumption.
4. **Dedup alerts** — Telegram deserves signal, not noise (future phase).
5. **Observer first, active later** — this MVP only logs. Full blocking
   comes in LENS-014 after 2-3 cycles of observed data.

## Files

```
code/lens_quota_guard.py                — Main module (~500 lines)
tests/test_lens_quota_guard.py          — 33-scenario test suite (~400 lines)
sql/create_lens_quota_ledger.sql        — Supabase DDL
```

## Deployment Steps

### Step 1: Supabase table

Open Supabase SQL Editor → paste and run `create_lens_quota_ledger.sql`.
Verify with:

```sql
SELECT column_name, data_type FROM information_schema.columns
WHERE table_name = 'lens_quota_ledger' ORDER BY ordinal_position;
```

Expected: 16 columns including `positions text[]`.

### Step 2: Move module

```bash
mv "/c/Users/James Maverick/Downloads/lens_quota_guard.py" /c/school/lens/code/
mv "/c/Users/James Maverick/Downloads/test_lens_quota_guard.py" /c/school/lens/tests/
```

(Create `tests/` folder if it doesn't exist.)

### Step 3: Syntax check + test suite

```bash
cd /c/school/lens
python -m py_compile code/lens_quota_guard.py

python tests/test_lens_quota_guard.py
# Expected: 27 passed, 0 failed
```

### Step 4: CLI smoke test (no real calls)

```bash
python code/lens_quota_guard.py --test-mode --no-write
```

Expected: all 5 positions → PROCEED decision with TEST reason.

### Step 5: Real dry-run (writes ledger, no blocking)

```bash
python code/lens_quota_guard.py
```

Expected: real Supabase read + write, returns PROCEED with 62-90% headroom
(fresh day, low usage). One row inserted into `lens_quota_ledger`.

### Step 6: Wire into orchestrator (LATER — not this session)

**Do NOT wire this session.** Observer mode MVP is meant to run standalone
to collect ledger data. In LENS-014, after 2-3 cron cycles of observed data,
we'll add this call to `lens_orchestrator.py`:

```python
# At top of lens_orchestrator.py, after imports
from lens_quota_guard import guard_check_with_fallback, filter_positions_by_guard

# Before firing S2/S3/MA (in main run function)
results = guard_check_with_fallback(
    positions=["S2-A", "S2-B", "S2-C", "S2-D", "S2-E", "S2-GAP",
               "MA", "S3-A", "S3-B", "S3-C", "S3-D", "S3-E"],
    run_id=run_id,
)
per_position = filter_positions_by_guard(positions_to_fire, results)
# Then skip positions where per_position[pos] == "SKIP"
```

## Configuration

### Environment variables

| Variable | Effect |
|---|---|
| `SUPABASE_URL` | Required for ledger I/O |
| `SUPABASE_SERVICE_KEY` | Required for ledger I/O (service role bypasses RLS) |
| `LENS_FORCE=1` | Emergency override — always PROCEED regardless of quota |
| `LENS_GUARD_TEST=1` | Test mode — no real calls, always returns TEST decision |

### Thresholds (in lens_quota_guard.py)

```python
THRESHOLD_TIGHT    = 40.0   # < 40% headroom → PROCEED_TIGHT (warn)
THRESHOLD_DEGRADE  = 20.0   # < 20% → DEGRADE (skip heaviest positions)
THRESHOLD_SKIP     =  0.0   # <= 0% → SKIP (skip all positions using this provider)
```

Adjust after observing real data — current values are imported from GNI
experience.

### Known provider limits (update when tiers change)

```python
PROVIDER_LIMITS = {
    ("groq",      "llama-3.3-70b-versatile"): {"TPD": 100_000},
    ("groq",      "qwen3-32b"):                {"TPD": 100_000},
    ("gemini",    "gemini-2.0-flash"):         {"RPD": 1_500},
    ("cerebras",  "qwen-3-235b"):              {"TPD": 1_000_000},
    ("mistral",   "mistral-small"):            {"RPD": 2_000},
    ("sambanova", "llama-3.3-70b"):            {"TPD": 500_000},
    ("cohere",    "command-r-plus"):           {"RPD": 1_000},
}
```

## Test Coverage

33-test matrix covering 6 failure layers:

| Layer | Tests | What is verified |
|---|---|---|
| Connectivity | T01-T03 | Provider unreachable, timeout, 500 errors |
| Authentication | T04-T06 | Missing key, 401, 403 |
| Response integrity | T07-T11 | Non-JSON, schema drift, negative values |
| Quota data truth | T12-T15 | Clock skew, over-limit reports, zero limit |
| Ledger persistence | T16-T19 | Supabase unreachable, RLS, schema stale |
| Decision correctness | T20-T30 | All threshold branches + FORCE + TEST |
| Plus | T31-T33 | Partial response, nested errors, multi-provider |

Run: `python tests/test_lens_quota_guard.py`

All tests use mock providers and mock Supabase — no real calls, no network.

## Queries for Monitoring

### How much have we used today?

```sql
SELECT provider, model, SUM(estimated_use) as used_today
FROM lens_quota_ledger
WHERE cron_time_utc >= date_trunc('day', NOW() AT TIME ZONE 'UTC')
  AND decision != 'SKIP'  -- skipped runs didn't actually consume
GROUP BY provider, model
ORDER BY used_today DESC;
```

### Decision distribution over last 7 days

```sql
SELECT decision, COUNT(*) as n
FROM lens_quota_ledger
WHERE cron_time_utc >= NOW() - INTERVAL '7 days'
GROUP BY decision
ORDER BY n DESC;
```

### Recent SKIPs (if any) — investigate root cause

```sql
SELECT cron_time_utc, provider, model, used_value, limit_value,
       headroom_pct, reason, positions
FROM lens_quota_ledger
WHERE decision = 'SKIP'
ORDER BY cron_time_utc DESC
LIMIT 10;
```

## Rollback Plan

If the guard misbehaves in production:

1. **Soft disable**: `export LENS_FORCE=1` in GitHub Actions secrets. All
   decisions become FORCE → PROCEED regardless.
2. **Full disable**: remove the import + call from lens_orchestrator.py
   (single revert commit).
3. **Remove ledger table**: `DROP TABLE lens_quota_ledger;` (not
   recommended — historical data is valuable).

## Author

Team Geeks (Bro Alpha + Claude Opus 4.7), April 17, 2026, LENS-013 session.

Imported and adapted from GNI Autonomous `ai_engine/quota_guard.py`
(GNI-R-112 Pre-flight quota reservation).
