-- ═══════════════════════════════════════════════════════════════════════════
-- lens-MIGR-003-s2f_schema.sql
-- LENS-018 Task 2 — S2-F Longitudinal Framing Drift Analyst foundation
-- ═══════════════════════════════════════════════════════════════════════════
--
-- Creates 4 new tables + extends lens_raw_articles with 2 optional columns.
-- Forward-compatible: all new columns nullable, no breaking changes to
-- existing ingestion or orchestrator code.
--
-- PHI-003 alignment:
--   - state_actor_lens column uses Office-names (xi_office, putin_office, etc)
--     NOT country codes. See canonical vocabulary in PHI-003 Appendix A.
--   - Framing rubrics built symmetrically for all state apparatus of
--     consequence; legitimacy weighting applied at finding level per Section 3.
--
-- Deployment per LR-084 (verification context = production context):
--   1. CONFIRM the Supabase URL in browser address bar reads:
--      app.supabase.com/project/imfjhwqivwwreehvtyac
--   2. If URL confirms production project → execute this file
--   3. If URL differs → STOP, switch project, re-check
--
-- Per LR-080 (write-then-verify):
--   Verification SELECTs at the end confirm all 4 tables + 2 columns exist.
--
-- Run: paste entire file into Supabase SQL editor → Run
-- Expected: all CREATE/ALTER/NOTIFY statements succeed + 6 verification rows
-- ═══════════════════════════════════════════════════════════════════════════


-- ─── Table 1: lens_entities ─────────────────────────────────────────────────
-- Canonical registry of humans and organizations named in articles.
-- Deduplicated by (entity_type, canonical_name).
-- Populated opportunistically during fetch by entity extraction (Task 3).
CREATE TABLE IF NOT EXISTS public.lens_entities (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_type       TEXT NOT NULL
                      CHECK (entity_type IN ('author','expert','think_tank','official','unknown')),
    name              TEXT NOT NULL,
    canonical_name    TEXT NOT NULL,
    primary_outlet    TEXT,
    affiliations      JSONB DEFAULT '[]'::jsonb,
    first_seen        TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen         TIMESTAMPTZ NOT NULL DEFAULT now(),
    total_mentions    INTEGER NOT NULL DEFAULT 0,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (entity_type, canonical_name)
);

CREATE INDEX IF NOT EXISTS idx_lens_entities_canonical
    ON public.lens_entities (canonical_name);

CREATE INDEX IF NOT EXISTS idx_lens_entities_last_seen
    ON public.lens_entities (last_seen DESC);

CREATE INDEX IF NOT EXISTS idx_lens_entities_total_mentions
    ON public.lens_entities (total_mentions DESC);


-- ─── Table 2: lens_entity_mentions ──────────────────────────────────────────
-- Per-article, per-entity link. One row per (article, entity, mention_type).
-- An article can have multiple entities; an entity can appear in many articles.
CREATE TABLE IF NOT EXISTS public.lens_entity_mentions (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_id         UUID NOT NULL REFERENCES public.lens_entities(id) ON DELETE CASCADE,
    raw_article_id    UUID NOT NULL,
    mention_type      TEXT NOT NULL
                      CHECK (mention_type IN ('byline','quoted_expert','affiliated_source','mentioned')),
    context_snippet   TEXT,
    collected_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (entity_id, raw_article_id, mention_type)
);

CREATE INDEX IF NOT EXISTS idx_lens_entity_mentions_entity
    ON public.lens_entity_mentions (entity_id);

CREATE INDEX IF NOT EXISTS idx_lens_entity_mentions_article
    ON public.lens_entity_mentions (raw_article_id);

CREATE INDEX IF NOT EXISTS idx_lens_entity_mentions_collected
    ON public.lens_entity_mentions (collected_at DESC);


-- ─── Table 3: lens_framing_scores ───────────────────────────────────────────
-- Per (article, entity, state_actor_lens, topic) LLM-scored framing vector.
-- Async-populated by LENS-020 nightly scoring cron.
-- state_actor_lens uses Office-names per PHI-003 (xi_office, putin_office, etc).
CREATE TABLE IF NOT EXISTS public.lens_framing_scores (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_id         UUID NOT NULL REFERENCES public.lens_entities(id) ON DELETE CASCADE,
    raw_article_id    UUID NOT NULL,
    topic             TEXT NOT NULL,
    state_actor_lens  TEXT NOT NULL,
    -- 5-axis framing vector per PHI-003 Section 3 rubric design
    axis_sympathy_democratic_movements   DOUBLE PRECISION CHECK (axis_sympathy_democratic_movements BETWEEN 0.0 AND 1.0),
    axis_state_actor_legitimacy          DOUBLE PRECISION CHECK (axis_state_actor_legitimacy BETWEEN 0.0 AND 1.0),
    axis_blame_attribution               DOUBLE PRECISION CHECK (axis_blame_attribution BETWEEN 0.0 AND 1.0),
    axis_historical_context_completeness DOUBLE PRECISION CHECK (axis_historical_context_completeness BETWEEN 0.0 AND 1.0),
    axis_sources_quoted_diversity        DOUBLE PRECISION CHECK (axis_sources_quoted_diversity BETWEEN 0.0 AND 1.0),
    confidence        DOUBLE PRECISION CHECK (confidence BETWEEN 0.0 AND 1.0),
    rubric_version    TEXT NOT NULL DEFAULT 'v1',
    scored_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (entity_id, raw_article_id, topic, state_actor_lens, rubric_version)
);

CREATE INDEX IF NOT EXISTS idx_lens_framing_scores_entity_topic
    ON public.lens_framing_scores (entity_id, topic);

CREATE INDEX IF NOT EXISTS idx_lens_framing_scores_actor_lens
    ON public.lens_framing_scores (state_actor_lens);

CREATE INDEX IF NOT EXISTS idx_lens_framing_scores_scored_at
    ON public.lens_framing_scores (scored_at DESC);


-- ─── Table 4: lens_drift_findings ───────────────────────────────────────────
-- Weekly S2-F drift-detection output. One row per (entity, state_actor_lens,
-- window) where statistical deviance crosses threshold + alternative-hypothesis
-- plausibility supports "coordinated influence" as non-least-plausible.
--
-- Every finding includes alternative_hypotheses with plausibility scores so
-- GCSP readers can see the full reasoning, never just the conclusion.
-- Every finding must name Office (Xi Office, Putin Office, Trump Office etc)
-- per PHI-003 vocabulary discipline.
CREATE TABLE IF NOT EXISTS public.lens_drift_findings (
    id                         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_id                  UUID NOT NULL REFERENCES public.lens_entities(id) ON DELETE CASCADE,
    state_actor_lens           TEXT NOT NULL,
    window_start               DATE NOT NULL,
    window_end                 DATE NOT NULL,
    sample_size                INTEGER NOT NULL CHECK (sample_size >= 15),
    framing_mean               JSONB NOT NULL,
    outlet_baseline            JSONB NOT NULL,
    deviance_sigma             DOUBLE PRECISION NOT NULL,
    cross_topic_coherence      DOUBLE PRECISION CHECK (cross_topic_coherence BETWEEN 0.0 AND 1.0),
    alternative_hypotheses     JSONB NOT NULL DEFAULT '[]'::jsonb,
    finding_confidence         TEXT NOT NULL
                               CHECK (finding_confidence IN ('LOW','MEDIUM','HIGH')),
    legitimacy_category        TEXT
                               CHECK (legitimacy_category IN ('elected_bounded','hybrid_contested','unelected_indefinite','unknown')),
    evidence_article_ids       JSONB NOT NULL DEFAULT '[]'::jsonb,
    finding_phrasing           TEXT NOT NULL,
    rubric_version             TEXT NOT NULL DEFAULT 'v1',
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
    reviewed_by_operator       BOOLEAN NOT NULL DEFAULT FALSE,
    reviewed_at                TIMESTAMPTZ,
    operator_notes             TEXT
);

CREATE INDEX IF NOT EXISTS idx_lens_drift_findings_entity
    ON public.lens_drift_findings (entity_id);

CREATE INDEX IF NOT EXISTS idx_lens_drift_findings_actor_lens
    ON public.lens_drift_findings (state_actor_lens);

CREATE INDEX IF NOT EXISTS idx_lens_drift_findings_window
    ON public.lens_drift_findings (window_end DESC);

CREATE INDEX IF NOT EXISTS idx_lens_drift_findings_unreviewed
    ON public.lens_drift_findings (created_at DESC)
    WHERE reviewed_by_operator = FALSE;


-- ─── Extend lens_raw_articles (optional columns) ────────────────────────────
-- author: byline extracted from RSS/HTML (nullable — some sources omit)
-- extracted_quoted_entities: jsonb array of entity_ids populated by Task 3
-- Both optional; existing ingestion continues unchanged.
ALTER TABLE public.lens_raw_articles
    ADD COLUMN IF NOT EXISTS author TEXT;

ALTER TABLE public.lens_raw_articles
    ADD COLUMN IF NOT EXISTS extracted_quoted_entities JSONB DEFAULT '[]'::jsonb;


-- ─── PostgREST reload ───────────────────────────────────────────────────────
-- LR-084: PostgREST caches schema on startup. Without this NOTIFY, the REST
-- API will 404 on the new tables even though they exist in the DB. This is
-- the exact failure mode that broke I7 migration verification at the start
-- of LENS-017.
NOTIFY pgrst, 'reload schema';


-- ─── VERIFICATION SELECTS — LR-080 write-then-verify ────────────────────────
-- If any of these return zero rows, the migration did not land in THIS
-- project. If that happens, check browser URL per LR-084 and re-run.

-- 1/6: confirm all 4 new tables exist
SELECT 'Tables present' AS check_name,
       COUNT(*) AS count,
       COUNT(*) FILTER (WHERE table_name IN
           ('lens_entities','lens_entity_mentions','lens_framing_scores','lens_drift_findings')) AS s2f_tables
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_name IN ('lens_entities','lens_entity_mentions','lens_framing_scores','lens_drift_findings');
-- Expected: count=4, s2f_tables=4

-- 2/6: confirm lens_entities column shape
SELECT 'lens_entities columns' AS check_name, COUNT(*) AS column_count
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'lens_entities';
-- Expected: column_count = 11

-- 3/6: confirm lens_drift_findings column shape (largest table)
SELECT 'lens_drift_findings columns' AS check_name, COUNT(*) AS column_count
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'lens_drift_findings';
-- Expected: column_count = 16

-- 4/6: confirm the 2 new columns landed on lens_raw_articles
SELECT 'lens_raw_articles new columns' AS check_name,
       COUNT(*) AS new_column_count
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'lens_raw_articles'
  AND column_name IN ('author','extracted_quoted_entities');
-- Expected: new_column_count = 2

-- 5/6: confirm indexes landed
SELECT 'S2-F indexes' AS check_name, COUNT(*) AS idx_count
FROM pg_indexes
WHERE schemaname = 'public'
  AND tablename IN ('lens_entities','lens_entity_mentions','lens_framing_scores','lens_drift_findings')
  AND indexname LIKE 'idx_lens_%';
-- Expected: idx_count = 10

-- 6/6: smoke-test INSERT on lens_entities to prove write path + UNIQUE works
-- This deletes itself via ON CONFLICT/DELETE cycle so the table stays empty.
WITH smoke AS (
    INSERT INTO public.lens_entities (entity_type, name, canonical_name)
    VALUES ('author', 'LENS-018 Migration Smoke Test', 'lens018_migration_smoke')
    ON CONFLICT (entity_type, canonical_name) DO UPDATE SET last_seen = now()
    RETURNING id, canonical_name, total_mentions
)
SELECT 'Smoke test INSERT' AS check_name, canonical_name, total_mentions FROM smoke;
-- Expected: 1 row with canonical_name='lens018_migration_smoke', total_mentions=0

-- Clean up the smoke test row
DELETE FROM public.lens_entities WHERE canonical_name = 'lens018_migration_smoke';

-- ─── END OF MIGRATION ──────────────────────────────────────────────────────
-- If verification 1-6 all returned the expected values, migration succeeded.
-- Next step (LENS-018 Task 3): build entity extraction module.
