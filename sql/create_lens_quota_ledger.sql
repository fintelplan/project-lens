-- ===========================================================================
-- LENS-013 T-05: Supabase schema for lens_quota_ledger
-- ---------------------------------------------------------------------------
-- Purpose: Ledger-first quota tracking for Project Lens pre-flight guard.
-- Each position's consumption is logged here; guard reads this to decide
-- PROCEED / DEGRADE / SKIP before firing expensive calls.
--
-- Philosophy: Zero-cost quota awareness. We track our own usage rather than
-- polling provider APIs, saving both tokens and time.
--
-- Run in Supabase SQL Editor (https://supabase.com/dashboard → SQL Editor).
-- Safe to run multiple times (all statements are idempotent).
-- ===========================================================================

-- ── Create table (idempotent) ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS lens_quota_ledger (
  id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id          TEXT        NOT NULL,
  cron_time_utc   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  provider        TEXT        NOT NULL,
  model           TEXT        NOT NULL,
  quota_type      TEXT        NOT NULL,              -- 'TPD' | 'RPD' | 'UNKNOWN'
  limit_value     INTEGER,                            -- NULL if unknown
  used_value      INTEGER,                            -- NULL if ledger read failed
  remaining       INTEGER,                            -- computed: limit - used
  estimated_use   INTEGER     NOT NULL,               -- this cron's expected usage
  headroom_pct    NUMERIC(6,2),                       -- (remaining - estimated) / limit * 100
  decision        TEXT        NOT NULL,               -- 'PROCEED' | 'PROCEED_TIGHT' | 'DEGRADE' | 'SKIP' | 'FORCE' | 'TEST'
  reason          TEXT,                               -- human-readable
  error_class     TEXT,                               -- NULL if clean
  positions       TEXT[],                             -- ['S2-A', 'S2-E', 'MA']
  created_at      TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE lens_quota_ledger IS
  'Pre-flight quota guard ledger. Populated by lens_quota_guard.py every cron run. '
  'Retention: keep all rows — small size, valuable historical data for tuning thresholds.';

-- ── Indexes ────────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_lens_quota_ledger_cron_time
  ON lens_quota_ledger (cron_time_utc DESC);

CREATE INDEX IF NOT EXISTS idx_lens_quota_ledger_provider_time
  ON lens_quota_ledger (provider, model, cron_time_utc DESC);

CREATE INDEX IF NOT EXISTS idx_lens_quota_ledger_run_id
  ON lens_quota_ledger (run_id);

-- ── Row Level Security (read-only for anon, full for service role) ─────────
ALTER TABLE lens_quota_ledger ENABLE ROW LEVEL SECURITY;

-- Public read policy (for dashboards/monitoring)
DROP POLICY IF EXISTS "quota_ledger_public_read" ON lens_quota_ledger;
CREATE POLICY "quota_ledger_public_read"
  ON lens_quota_ledger
  FOR SELECT
  USING (true);

-- Service role can do everything (for the cron to write)
-- (service role bypasses RLS by default, no policy needed)

-- ── Verification query ─────────────────────────────────────────────────────
-- Run this to confirm the table was created correctly:
--
-- SELECT column_name, data_type
--   FROM information_schema.columns
--   WHERE table_name = 'lens_quota_ledger'
--   ORDER BY ordinal_position;
--
-- Expected: 14 columns including id, run_id, cron_time_utc, provider, model,
-- quota_type, limit_value, used_value, remaining, estimated_use, headroom_pct,
-- decision, reason, error_class, positions, created_at.
