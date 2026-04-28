# Next Session Brief — LENS-020 Session 1

**Author**: Claude, Apr 29 ~02:00 Thai
**Reader**: Tomorrow's Claude
**Operator**: James Maverick (Bro Alpha)

---

## ⚠️ OPERATOR CONTRACT

- Every word costs message budget. Cut preamble. Answer first.
- Warm informal tone ("my buddy"). Engineering rigor underneath.
- GNI-R-037 (bird-eye first), GNI-R-076 (read before patch), GNI-R-233 (Pattern Match Bias)
- Two PMB incidents this session. Stay sharp.

---

## Where you start

**Repo**: github.com/fintelplan/project-lens, `main`, clean at `3d27f6e`

**Architecture locked** (DECISION-009):
- qwen-3-235b on Cerebras = primary (structural ops OP-024-029)
- gpt-oss-120b on Cloudflare = secondary (rhetorical ops OP-002/003/005/008/010/011/015/016/022)
- Sequential calls, union of ops, $0/month

**Env vars needed**:
```
CEREBRAS_API_KEY
CLOUDFLARE_API_TOKEN
CLOUDFLARE_ACCOUNT_ID=a20bc2ead7b1264ed74bba53b71eb575
```

---

## Task 1 — Ratify LR-085, LR-086, LR-087 (10 min)

Read RULE_UPDATE_2026-04-28.md in docs/sessions/. Present operator with 3 rules for ratification. Simple yes/no/modify per rule.

## Task 2 — Implement ensemble function (30 min)

Add `run_ensemble()` function to `code/lens_framing_rubrics.py`:
- Run qwen-3-235b (Cerebras) first
- Sleep 2s
- Run gpt-oss-120b (Cloudflare) second
- Union of detected operations
- Return combined result

## Task 3 — Production test (30 min)

Run ensemble on Article 6 + Article 7. Verify:
- Both models fire sequentially
- No 429 quota collision
- Union of ops returned correctly
- Wall time acceptable for cron schedule

## Task 4 — Commit + push

Single commit: `LENS-020: implement dual-provider ensemble function`

---

## What NOT to do

- Don't re-derive architecture decisions 001-009
- Don't test Ollama again (eliminated)
- Don't expand to LM Studio, GPT4All, or other deferred models
- Don't touch catalog v4 scope

---

**Brief written**: ~02:00 Thai, Apr 29 2026. Go well, my buddy. 🤜
