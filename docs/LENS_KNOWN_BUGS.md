# LENS KNOWN BUGS

Open defects found by evidence, deliberately NOT fixed yet, each with the reason
the fix is deferred. A bug lives here only with the receipt that found it.

---

## BUG-001 — S2-D drops ~1/3 of each batch's articles and states a count it did not send

**Status:** OPEN — deferred by ruling (LENS-028, 2026-07-28). Post-cert change, own probe, own cert.
**File:** `code/lens_s2d_adversary.py` — `build_articles_prompt()` truncation site.

**Defect.** Two independent caps disagree, and neither knows about the other:

| Stage | Cap | Result on the 2026-07-28 production data |
| --- | --- | --- |
| `_split_batches` (nested in `run_s2d`) | 4,500 tokens | batch 1 = **42 articles** |
| `build_articles_prompt` | `MAX_TOTAL_CHARS = 9000` | silently truncates to **23 articles** |

So 19 of batch 1's 42 articles never reach the model. Worse, the user message is
built from `len(articles)` — the *pre-truncation* count — so the prompt tells the
model it is reading 42 articles from 8 sources when 23 articles from 4 sources are
actually present. Of 60 articles fetched per run, roughly 19 are silently discarded.

**Evidence (CC-5 probe, `probe_results.jsonl`, 2026-07-28).** Not inferred — observed
in the models' own output on the identical real prompt:

- baseline `llama-3.3-70b-versatile` returned `"articles_analyzed": 42` with 8 source IDs
  — it trusted the false header and reported articles it was never shown.
- primary `openai/gpt-oss-120b` returned `"articles_analyzed": 23` with 4 source IDs
  — it counted what was actually in the prompt.

The header does not merely mislead a reader; it corrupts the analytical record that
gets written to `injection_reports`. The 70b's count and source list were both wrong.

**Why it is not fixed yet (ruling, LENS-028):**
1. A model migration and a prompt-semantics change in the same wave makes any quality
   difference un-attributable. The L-CLIFF comparison must stay clean.
2. The corrected prompt is ~50% larger and moves toward the 8K Groq TPM ceiling, so it
   needs its own probe before it ships.

Explicitly **not** folded into CC-3. Today's probe measured production as-is, which is
the right call: both candidates faced the identical prompt, bug included.

**Fix sketch (for whoever picks it up):** make one cap authoritative. Either size batches
in characters against `MAX_TOTAL_CHARS`, or drop the char cap and let the token budget
own it — then build the user message from the articles actually included, never from the
input list length.

**Sequencing (ruled 2026-07-28):** the fix lands AFTER CC-1c, with its own probe on the
larger prompt. Projected shape of the corrected call: **~5,300 prompt + ~1,900 completion
≈ 7,200 tokens**. Note the ceiling it is measured against is **8,000, not 8,192** — the
8,192 figure was always TPM-derived, and Groq gpt-oss-120b's verified TPM is 8,000
(context is 131,072 and never the binding constraint). That leaves ~800 tokens of margin,
which is thin enough that the probe is mandatory rather than a formality.

---

## BUG-002 — S2-E sends `Lens: unknown` on every call

**Status:** OPEN — logged 2026-07-28, not fixed (freeze).
**File:** `code/lens_s2e_legitimacy.py:204`.

`call_legitimacy_filter()` reads `report.get("lens_name", "unknown")`, but
`fetch_latest_reports()` selects `id, domain_focus, summary, cycle, generated_at` — there
is no `lens_name` key, so the default always wins. Every S2-E prompt has therefore been
telling the model `Lens: unknown` instead of naming the lens under assessment.

**Evidence (CC-5 fixture, 2026-07-28):** `report_domain_focus: ALL`, `lens_name_sent: unknown`.

The probe fixture reproduces this faithfully rather than correcting it — a probe must
measure what production actually sends. Low severity (the report text carries the real
content), but it silently removes context the prompt was designed to supply.

---

## BUG-003 (HYPOTHESIS — unconfirmed) — S2-F's Cloudflare 429s may be neuron exhaustion, not pacing

**Status:** HYPOTHESIS. Do not act until measured.

**Observation (run 81938481905, 2026-07-27):** S2-F's Cloudflare secondary logged
10x HTTP 200 against 42x HTTP 429, retries ~1s apart with no backoff; the 429 stalls
account for most of the 27-minute runtime.

**What is ruled out.** Cloudflare Workers AI rate-limits Text Generation at **300 requests
per minute** ([limits page](https://developers.cloudflare.com/workers-ai/platform/limits/)).
S2-F is nowhere near 300 RPM at ~1s retries, so a per-minute request ceiling does not
explain a 4:1 429 ratio. Pacing is not the cause, and **backoff is therefore not the fix** —
it would only spread the same failures over a longer window and lengthen the run.

**Daily neuron exhaustion — WEAKENED, 2026-07-28.** The free tier allocates **10,000
Neurons/day**, a GPU-compute unit rather than a token count
([pricing page](https://developers.cloudflare.com/workers-ai/platform/pricing/)), and
exhaustion initially fit the shape. It no longer does: **the FIRST Cloudflare call of run
81938481905 429'd at 05:08:22**, and that run was the first S2-F of the UTC day. A
midnight-reset daily allocation should have been full. An allocation cannot be exhausted
before it is used.

**Live alternatives to test (no favourite):**
1. The **REST API free tier differs from the Workers-binding tier** — S2-F calls REST, and
   the published 300 RPM / 10,000 Neurons may describe the binding path only.
2. **Workers AI is not provisioned on the account at all**, in which case every call 429s
   from the first request and the secondary has never worked.

Alternative 2 fits "429 on the very first call" most economically and should be checked
first — it is a console question, not a code question.

**Still worth computing:** the per-call Neuron cost of `@cf/openai/gpt-oss-120b`. Now
derivable from the pricing page ($0.35/M input + $0.75/M output). Record it here when
someone works it out — it bounds calls/day whichever alternative proves true.

---

*LENS_KNOWN_BUGS.md v2 | opened 2026-07-28 | LENS-028 CC-5 | BUG-002/003 added 2026-07-28*
