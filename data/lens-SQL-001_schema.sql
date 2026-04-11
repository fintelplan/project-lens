-- ============================================================
-- Project Lens — Supabase Schema
-- File: lens-SQL-001_schema.sql
-- Created: LENS-002
-- ML-ready from day 1
-- ============================================================

-- ── 1. RAW ARTICLES ─────────────────────────────────────────
-- Every collected article stored here first
CREATE TABLE lens_raw_articles (
  id              uuid DEFAULT gen_random_uuid() PRIMARY KEY,
  url             text NOT NULL,
  url_hash        text UNIQUE NOT NULL,  -- SHA256 for dedup
  title           text,
  content         text,
  source_id       text NOT NULL,         -- SRC-XXX
  source_name     text,
  published_at    timestamptz,
  collected_at    timestamptz DEFAULT now(),
  modality        text DEFAULT 'text',   -- text/image/audio/video
  language        text DEFAULT 'en',
  domain          text,                  -- POWER/TECH/FINANCE/etc
  is_verified     boolean DEFAULT false,
  raw_metadata    jsonb DEFAULT '{}'
);

-- ── 2. INDICATOR MATCHES ────────────────────────────────────
-- Which indicators were detected in each article
CREATE TABLE lens_indicator_matches (
  id              uuid DEFAULT gen_random_uuid() PRIMARY KEY,
  article_id      uuid REFERENCES lens_raw_articles(id),
  indicator_id    text NOT NULL,          -- POWER-001 etc
  domain          text NOT NULL,
  tier            integer NOT NULL,       -- 1/2/3
  confidence      float DEFAULT 0.0,     -- 0.0-1.0
  detected_by     text DEFAULT 'rule',   -- rule/ml/human
  human_verified  boolean DEFAULT false,
  created_at      timestamptz DEFAULT now()
);

-- ── 3. INTELLIGENCE REPORTS ────────────────────────────────
-- AI-generated summaries and Food for Thought
CREATE TABLE lens_reports (
  id              uuid DEFAULT gen_random_uuid() PRIMARY KEY,
  generated_at    timestamptz DEFAULT now(),
  cycle           text,                  -- morning/midday/afternoon/evening
  domain_focus    text,                  -- which domain this covers
  summary         text,
  food_for_thought text,
  signals_used    jsonb DEFAULT '[]',    -- list of indicator IDs used
  articles_used   jsonb DEFAULT '[]',    -- list of article IDs
  ai_model        text,
  prompt_version  text,
  quality_score   float,                 -- set after human feedback
  status          text DEFAULT 'pending' -- pending/reviewed/archived
);

-- ── 4. HUMAN FEEDBACK (RL Training Data) ───────────────────
-- Every human rating = reward signal for RL
CREATE TABLE lens_feedback (
  id              uuid DEFAULT gen_random_uuid() PRIMARY KEY,
  report_id       uuid REFERENCES lens_reports(id),
  article_id      uuid REFERENCES lens_raw_articles(id),
  rating          integer,               -- 1-5
  feedback_type   text,                  -- report/article/indicator
  correction      text,                  -- what was wrong
  missed_signals  jsonb DEFAULT '[]',    -- indicators analyst found that system missed
  wrong_signals   jsonb DEFAULT '[]',    -- indicators system got wrong
  notes           text,
  created_at      timestamptz DEFAULT now()
);

-- ── 5. ML WEIGHTS (Learned State) ──────────────────────────
-- System learns which indicators matter most
CREATE TABLE lens_ml_weights (
  indicator_id    text PRIMARY KEY,
  domain          text NOT NULL,
  weight          float DEFAULT 1.0,
  feedback_count  integer DEFAULT 0,
  accuracy_score  float DEFAULT 0.0,
  last_updated    timestamptz DEFAULT now()
);

-- ── 6. SOURCES REGISTRY ─────────────────────────────────────
-- All monitored sources
CREATE TABLE lens_sources (
  id              text PRIMARY KEY,      -- SRC-001 etc
  name            text NOT NULL,
  url             text NOT NULL,
  rss_url         text,
  domain          text,                  -- which domain this source covers
  actor           text,                  -- which actor (US/China/etc)
  credibility     float DEFAULT 1.0,     -- 0.0-1.0 learned over time
  is_active       boolean DEFAULT true,
  ethics_verified boolean DEFAULT false, -- three tests passed
  added_at        timestamptz DEFAULT now(),
  notes           text
);

-- ── 7. PIPELINE RUNS ───────────────────────────────────────
-- Track every collection cycle
CREATE TABLE lens_pipeline_runs (
  id              uuid DEFAULT gen_random_uuid() PRIMARY KEY,
  cycle           text,                  -- morning/midday/afternoon/evening
  started_at      timestamptz DEFAULT now(),
  finished_at     timestamptz,
  articles_collected integer DEFAULT 0,
  indicators_matched integer DEFAULT 0,
  reports_generated  integer DEFAULT 0,
  status          text DEFAULT 'running',
  errors          jsonb DEFAULT '[]',
  tokens_used     integer DEFAULT 0
);

-- ── INDEXES ─────────────────────────────────────────────────
CREATE INDEX idx_raw_articles_collected ON lens_raw_articles(collected_at DESC);
CREATE INDEX idx_raw_articles_domain ON lens_raw_articles(domain);
CREATE INDEX idx_raw_articles_source ON lens_raw_articles(source_id);
CREATE INDEX idx_indicator_matches_article ON lens_indicator_matches(article_id);
CREATE INDEX idx_indicator_matches_indicator ON lens_indicator_matches(indicator_id);
CREATE INDEX idx_indicator_matches_tier ON lens_indicator_matches(tier);
CREATE INDEX idx_reports_generated ON lens_reports(generated_at DESC);
CREATE INDEX idx_reports_domain ON lens_reports(domain_focus);
CREATE INDEX idx_feedback_report ON lens_feedback(report_id);
CREATE INDEX idx_pipeline_runs_started ON lens_pipeline_runs(started_at DESC);

-- ── NOTES ───────────────────────────────────────────────────
-- lens_feedback     → RL training data (reward signals)
-- lens_indicator_matches → ML classifier training data
-- lens_ml_weights   → learned state (improves over time)
-- lens_sources.credibility → learned from feedback loop
-- All tables designed for ML from day 1 (LENS-001 LR-020(D))
