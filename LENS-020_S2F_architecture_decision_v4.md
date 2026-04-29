# LENS-020 S2-F Architecture Decision — v4 (FINAL)

**Updated**: Apr 29 2026, ~11:30 Thai
**Status**: RATIFIED pending Mistral Article 1+3 confirmation

---

## Final Architecture — Tier-specific per LR-087

### Watch Tier (Day 0-7, LOW confidence)
**Model**: qwen-3-235b on Cerebras
**Why**: Fast (2-12s), reliable, $0, catches OP-024-029 structural ops
**Stage**: early_warning only
**Cadence**: Every article, every cron run

### Clarity Tier (Day 7-30, MEDIUM confidence)
**Model**: qwen-3-235b (Cerebras) + gpt-oss-120b (Cloudflare) ensemble
**Why**: Complementary profiles, both fast, $0
**Stage**: early_warning + all on flagged voices
**Cadence**: Only voices with Watch alerts

### Verification Tier (Day 30-45, HIGH confidence)
**Model**: mistral-medium-latest (Mistral AI)
**Why**: 16 ops / 0.95 conf on Article 7 (2x qwen-3, 2x gpt-oss)
          European lineage — genuinely different perspective
          Deepest rhetorical pretense detection confirmed
**Stage**: all ops on confirmed voices
**Cadence**: Only voices with Clarity findings

---

## Cross-lab matrix (Apr 29 final)

### Article 6 (steno, 1955 chars) xi_office all:
| Model | Provider | ops | conf |
|---|---|---|---|
| qwen-3-235b | Cerebras | 5 | 0.88 |
| gpt-oss-120b | Cloudflare | 5 | 0.86 |
| mistral-medium | Mistral | 7 | 0.92 |

### Article 7 (investigative, 10620 chars) xi_office all:
| Model | Provider | ops | conf |
|---|---|---|---|
| qwen-3-235b | Cerebras | 8 | 0.88 |
| gpt-oss-120b | Cloudflare | 7 | 0.86 |
| mistral-medium | Mistral | 16 | 0.95 |

### Article 1 + 3: pending (running Apr 29)

---

## Provider summary

| Provider | Role | Cost | Speed | Reliability |
|---|---|---|---|---|
| Cerebras (qwen-3-235b) | Watch primary | $0 | 2-12s | ✅ High |
| Cloudflare (gpt-oss-120b) | Clarity secondary | $0 | 25-65s | ⚠️ Neurons quota |
| Mistral (mistral-medium) | Verification deep | $0 free tier | 100-245s | ✅ High |
| Mistral (mistral-small) | Alt if medium too slow | $0 | TBD | TBD |

---

## Implementation needed

- [ ] Update lens_s2f_scoring_cron.py — Watch tier = Cerebras only
- [ ] Update lens_s2f_clarity_aggregator.py — use ensemble
- [ ] Update lens_s2f_verification_aggregator.py — use Mistral
- [ ] Add MISTRAL_API_KEY to GitHub secrets ✅ (done Apr 29)
- [ ] Add Mistral to requirements.txt (uses openai SDK, no new package needed)
- [ ] Test mistral-small as faster alternative

---

**v4**: Apr 29 2026, ~11:30 Thai
