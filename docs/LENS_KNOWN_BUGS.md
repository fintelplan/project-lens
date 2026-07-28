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

---

*LENS_KNOWN_BUGS.md v1 | opened 2026-07-28 | LENS-028 CC-5*
