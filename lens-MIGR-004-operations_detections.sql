-- ═══════════════════════════════════════════════════════════════════════════
-- lens-MIGR-004-operations_detections.sql
-- LENS-020 — S2-F v2 operations-based detection persistence
-- ═══════════════════════════════════════════════════════════════════════════
--
-- Creates lens_operation_detections table for v2 rubric (29-operation catalog).
-- This replaces lens_framing_scores (v1 5-axis) as the primary S2-F output table.
-- lens_framing_scores is kept for backward compatibility but no longer written to.
--
-- Schema design principles:
--   - One row per (article, voice, state_actor_lens, stage_filter, rubric_version)
--   - operations_detected stored as JSONB array (flexible, queryable)
--   - ensemble_mode flag distinguishes single-model vs dual-provider runs
--   - provider column records which provider(s) were used
--   - reviewed_by_operator for Direction B gate (operator must review before public)
--
-- PHI-003 alignment:
--   state_actor_lens uses Office-names (xi_office, trump_office, etc)
--
-- Deployment per LR-084:
--   Confirm Supabase URL: app.supabase.com/project/imfjhwqivwwreehvtyac
--   Then paste entire file into SQL editor → Run
-- ═══════════════════════════════════════════════════════════════════════════


-- ─── Table: lens_operation_detections ───────────────────────────────────────
-- Core S2-F v2 output. One row per scoring run.
CREATE TABLE IF NOT EXISTS public.lens_operation_detections (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Article reference
    raw_article_id          UUID NOT NULL REFERENCES public.lens_raw_articles(id) ON DELETE CASCADE,

    -- Voice being analyzed
    voice_name              TEXT NOT NULL,
    voice_type              TEXT NOT NULL
                            CHECK (voice_type IN ('author','expert','official','think_tank','unknown')),

    -- Lens and stage
    state_actor_lens        TEXT NOT NULL,   -- xi_office, trump_office, etc (PHI-003)
    stage_filter            TEXT NOT NULL
                            CHECK (stage_filter IN ('early_warning','all')),

    -- Detection results
    operations_detected     JSONB NOT NULL DEFAULT '[]'::jsonb,
    operations_not_present  JSONB NOT NULL DEFAULT '[]'::jsonb,
    operation_count         INTEGER NOT NULL DEFAULT 0,
    early_warning_count     INTEGER NOT NULL DEFAULT 0,
    post_suspect_count      INTEGER NOT NULL DEFAULT 0,

    -- Scoring metadata
    confidence              DOUBLE PRECISION NOT NULL DEFAULT 0.0
                            CHECK (confidence BETWEEN 0.0 AND 1.0),
    not_applicable          BOOLEAN NOT NULL DEFAULT FALSE,
    food_for_thought        TEXT,

    -- Rubric versioning
    rubric_version          TEXT NOT NULL DEFAULT 'v2-operations',
    catalog_version         TEXT NOT NULL DEFAULT 'v3.1',

    -- Provider info
    provider                TEXT NOT NULL,   -- cerebras | cloudflare | groq | ensemble
    ensemble_mode           BOOLEAN NOT NULL DEFAULT FALSE,

    -- Lifecycle
    scored_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    status                  TEXT NOT NULL DEFAULT 'pending'
                            CHECK (status IN ('pending','reviewed','archived')),
    reviewed_by_operator    BOOLEAN NOT NULL DEFAULT FALSE,
    reviewed_at             TIMESTAMPTZ,
    operator_notes          TEXT,

    -- Dedup: one scoring run per (article, voice, lens, stage, rubric_version)
    UNIQUE (raw_article_id, voice_name, state_actor_lens, stage_filter, rubric_version)
);

-- Indexes for aggregator queries
CREATE INDEX IF NOT EXISTS idx_lod_article
    ON public.lens_operation_detections (raw_article_id);

CREATE INDEX IF NOT EXISTS idx_lod_voice
    ON public.lens_operation_detections (voice_name, state_actor_lens);

CREATE INDEX IF NOT EXISTS idx_lod_lens_stage
    ON public.lens_operation_detections (state_actor_lens, stage_filter);

CREATE INDEX IF NOT EXISTS idx_lod_scored_at
    ON public.lens_operation_detections (scored_at DESC);

CREATE INDEX IF NOT EXISTS idx_lod_unreviewed
    ON public.lens_operation_detections (scored_at DESC)
    WHERE reviewed_by_operator = FALSE;

CREATE INDEX IF NOT EXISTS idx_lod_not_applicable
    ON public.lens_operation_detections (state_actor_lens, not_applicable)
    WHERE not_applicable = FALSE;

-- GIN index for querying inside operations_detected JSONB
CREATE INDEX IF NOT EXISTS idx_lod_operations_gin
    ON public.lens_operation_detections USING GIN (operations_detected);


-- ─── PostgREST reload ───────────────────────────────────────────────────────
NOTIFY pgrst, 'reload schema';


-- ─── VERIFICATION SELECTS (LR-080 write-then-verify) ────────────────────────

-- 1/4: table exists
SELECT 'Table present' AS check_name,
       COUNT(*) AS count
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_name = 'lens_operation_detections';
-- Expected: count=1

-- 2/4: column count
SELECT 'Column count' AS check_name, COUNT(*) AS column_count
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'lens_operation_detections';
-- Expected: column_count = 22

-- 3/4: indexes
SELECT 'Index count' AS check_name, COUNT(*) AS idx_count
FROM pg_indexes
WHERE schemaname = 'public'
  AND tablename = 'lens_operation_detections'
  AND indexname LIKE 'idx_lod_%';
-- Expected: idx_count = 7

-- 4/4: smoke test INSERT
WITH smoke AS (
    INSERT INTO public.lens_operation_detections (
        raw_article_id, voice_name, voice_type,
        state_actor_lens, stage_filter,
        operations_detected, operation_count,
        confidence, provider, rubric_version, catalog_version
    )
    SELECT
        id,
        'LENS-020 Migration Smoke Test',
        'author',
        'xi_office',
        'early_warning',
        '[]'::jsonb,
        0,
        0.0,
        'smoke_test',
        'v2-operations',
        'v3.1'
    FROM public.lens_raw_articles
    LIMIT 1
    RETURNING id, voice_name, operation_count
)
SELECT 'Smoke INSERT' AS check_name, voice_name, operation_count FROM smoke;
-- Expected: 1 row, operation_count=0

-- Clean up smoke row
DELETE FROM public.lens_operation_detections
WHERE voice_name = 'LENS-020 Migration Smoke Test';

-- ─── END OF MIGRATION ───────────────────────────────────────────────────────
