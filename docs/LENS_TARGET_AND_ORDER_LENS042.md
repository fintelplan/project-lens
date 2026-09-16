# LENS TARGET AND ORDER — LENS-042
Regenerated 2026-09-16 at the LENS-041 close. Supersedes LENS041 entirely.
Item numbers are NEW. Nothing carries its old number.

## THE TARGET (unchanged since LENS-036)
A daily intelligence pipeline that runs at $0/month, tells the truth about
its own failures, and delivers a complete Regular Report to Telegram.
Loud failure always beats silent success.

## WHAT LENS-041 SETTLED — DO NOT RE-OPEN
- **Item 1 (repoint) and item 3 (response_format): CERTIFIED** on wave
  `34936864688`. S2-E `COMPLETE`, `reports_saved: 4`, zero JSON parse errors.
- **Item 2 (Regular Report): DELIVERED.** Run `35069334184`, 2026-09-16
  07:37Z: `finish_reason=stop`, `budget_used=69%`, `PART 4 built in code:
  38 of 38`, `sent=True`, 1,806 words. First delivery since 2026-09-03.
- **`All positions complete`** on wave `35063596665`, 2026-09-16 06:47Z.
  First time in five weeks. Seven of seven S2 positions.
- **R11 RULED IN** (James, 2026-09-15): "we never ask providers for the
  guarantees they offer" is a ROOT, not an item.
- **Item 2.2 ruled D**; item 21.3 ruled C and discharged; repoint scope
  ruled A then A1.
- **D-027 is discharged for the Regular Report** and was then applied by
  analogy to four other prose positions. See item 7.

---

# THE MISSION — ITEM 1

## 1. CEREBRAS IS DEAD AS A FREE PROVIDER
Verified by cold call and by console, 2026-09-16, same account confirmed by
key fingerprint:
- `POST /v1/chat/completions` on **both** models -> **402
  `payment_required` `param: quota`**. `gpt-oss-120b` AND `qwen-3.8-27b`.
  Account-wide, not model-scoped — this is NOT LR-167's shape.
- `GET /v1/models` -> 200. `GET /v1/models/gpt-oss-120b` -> 200. Reads work,
  inference does not.
- Console: **`Current balance $0.00`**, `Tokens used (last month) 0.0`,
  `API calls (last month) 0.0`. The only buttons are ADD CREDITS and
  EXPLORE PLANS.
- `x-should-retry: false` on the 402.

**Four positions have Cerebras as PRIMARY: `s2d_adversary`,
`s2e_legitimacy`, `mission_analyst`, `s3d_longterm`.** All four now run
entirely on their Mistral fallback leg. Wave `35063596665` proves it — S2-D
x2, S2-E x4, MA x1, every one a `FALLBACK:` line, zero Cerebras successes.

**This is SambaNova again.** 2026-07-28: `HTTP 402, balance_units 0`,
provider gone. Fifty days later, same code, same `$0.00`. Two of six
providers have now died the same death and nothing watched for either.

### 1.1 The registry still says Cerebras is alive
`("cerebras", CEREBRAS_GPT_OSS_120B): {"METER":"tokens","TPD":1_000_000,...}`
tagged VERIFIED-console **2026-07-28** — fifty days stale, and now false.
Four ROLES rows name it as primary. Decide: tombstone it the way
GEMINI_25_FLASH was tombstoned (CC-31), or leave it and record why.

### 1.2 Cloudflare is in the registry and NO role uses it
`("cloudflare", CLOUDFLARE_GPT_OSS_120B): {"METER":"requests","RPM":300,
"CTX":128_000}` — VERIFIED-docs 2026-07-28, 10,000 Neurons/day free, same
`gpt-oss-120b` family the four orphaned positions already use. **Never
probed, never wired.** This is the closest thing to a ready replacement in
the repo and nobody has called it.

### 1.3 What the fork looks like (do NOT rule this before probing 1.2)
- **A** — probe Cloudflare on the four orphaned positions' real fixtures,
  wire it as the new primary.
- **B** — accept ministral-8b as primary for all four and give them a
  DIFFERENT fallback (Groq for the two that fit under 8K TPM).
- **C** — pay for Cerebras. Breaks $0/month.
- **D** — find a sixth provider.
Claude's lean at close: **A, then B for whatever A cannot carry.** A uses
something already measured and already in the registry. But 1.2 is unprobed
and a lean without a probe is what LR-106 exists to prevent.

---

# ROOT 12 — ONE PROVIDER IS HOLDING THE WHOLE SYSTEM UP

| Provider | State 2026-09-16 |
| --- | --- |
| Cerebras | **402 account-wide.** Primary for 4 positions. |
| Gemini `2.0-flash` | **404 since 2026-06-01.** Primary for S2-B and S3-B. |
| Mistral `small*` | **429 class-wide since 2026-09-04.** Reason UNKNOWN. |
| SambaNova | Dead 2026-07-28. Tombstoned. |
| Groq | Alive. S2-A, lens1 only. |
| **Mistral `ministral-8b-2512`** | **Alive. Carries everything else.** |

Every green position on 2026-09-16 except S2-A and lens1 ran on
`ministral-8b-2512`. If Mistral 429s that id the way it 429'd
`mistral-small`, the pipeline stops in one wave.

**R12 is not "add redundancy" as a slogan. It is: no single (provider,
model) pair may be the only live leg for more than N positions, and nothing
currently measures N.**

---

# ITEMS

## 2. S3-A fails silently and it is not the 402
Wave `35063596665`, 06:47:45Z and 06:48:06Z: two attempts,
`prompt 19256 chars, max_tokens 5000`, then
`ERROR S3-A failed — no analysis produced`. **No exception text. No status
code. Twenty seconds.** The 17:43 X wave shows S3-A failing with a visible
402; this one does not. Different failure, unnamed. Probe pack has an `s3a`
fixture.

## 3. gemini-2.0-flash has been 404 for three and a half months
Google's own 404 body: *"This model models/gemini-2.0-flash is no longer
available. Please update your code to use models/gemini-3.6-flash."*
- `lens_s2b_coordination.py:32` `MODEL = "gemini-2.0-flash"` — hardcoded.
- Registry `s2b_coordination` says `gemini-2.5-flash-lite`, **which dies
  2026-10-16** (thirty days).
- `gemini-3.6-flash` is in NO registry row and has never been probed.
CC-70 made S2-B reach its fallback, so the position works. The primary is
still a corpse and its replacement is a corpse-in-waiting.

## 4. Three prose positions still call the dead mistral-small class
`lens_s1_report.py:19`, `lens_s2_step_report.py:19`,
`lens_s3_step_report.py:223`. All three ask for PART-structured prose, none
logs `finish_reason`, and `[S1-RPT] Mistral 429` appears on every wave.
**Deliberately not swapped** — CC-71's reasoning: swapping converts a loud
429 into a silent truncation, which is D-027's exact hazard.
**Order of work: instrument with the CC-65 pattern, THEN swap.** Not the
reverse.

## 5. `x-should-retry: false` is never read — R11 instance 3
Cerebras returns it on the 402. Call sites retry 2-3 times with 10-20s
sleeps regardless. Wave `35063596665`: S2-E burned eight pre-failed retries.
R11's other two instances: `response_format` (fixed, CC-63) and
`/v1/models` `deprecation` fields (item 12).

## 6. The Regular Report can still truncate 1 in 5
CC-67 set `MAX_TOKENS = 8192` on measurement: at 4096, 5/5 `length`; at
8192, 4/5 `stop` with completions spread **3,686 to 8,192 on
byte-identical input** — a 2.2x spread. The 16384 re-test lost 3/5 trials
to ReadTimeout and its two survivors were the low end, **so the top of the
distribution has never been observed.** 2026-09-16's live run used 69%.
One good day is not the distribution.

## 7. The reference pool discards 60% and the log lies about it
`fetch_references()`: `timedelta(days=3)` in a variable named `yesterday`,
logged as `References: 1000 articles in 36h window`. It is **three days,
not 36 hours**. 1,000 returned, `MAX_REFS = 400` keeps the first 400 by
`collected_date desc, ref_id asc`. Six hundred articles are dropped by sort
order and nothing says so.

## 8. `PROVIDER_LIMITS` cannot see ministral-8b
`_GOVERNING_AXIS["mistral"] = "RPD"`; the ministral LIMITS row has TPM and
RPS and **no RPD**, because Mistral has no RPD axis. The filter
`if _axis and _axis in _lim` therefore drops it. Confirmed:
`("mistral","ministral-8b-2512") in PROVIDER_LIMITS -> False`.
The guard fails safe and says so loudly (`UNKNOWN ... proceeding
conservatively`), so nothing breaks — **but the pair every position now
depends on is unmetered.** `_GOVERNING_AXIS` calls itself a "Back-compat
single-axis view"; Mistral needs two axes. Ruled A at LENS-041 (leave it,
record it) precisely so this item exists.

## 9. Registry `max_out` is decorative for six positions
`s2c_emotion` row says 1600, call site uses 1500. `s3f_countercheck` row
says 1600, call site uses **3000**. `s1_report`/`s2_report` say 2400, call
sites use 4000/4500. `regular_report` says `mistral-small-latest` while the
wire is `ministral-8b-2512`. None of these files import `lens_models`.

## 10. S3-B truncates its JSON even with response_format
Wave `35063596665` 06:50:14Z: `S3-B Mistral fallback attempt 1:
Unterminated string starting at: line 113 column 32 (char 10537)`. Attempt
2 succeeded at 10,135 chars. `lens_s3b_truehistory.py` sends
`max_tokens: 2500` hardcoded. **`json_object` guarantees VALID JSON, not
COMPLETE JSON** — a cap still cuts it. No `finish_reason` logged here, so
truncation is inferred from the parse error, not measured.

## 11. The probe pack calls a timeout a refusal
Three `ReadTimeout` trials in the 16384 run were banked with
`refusal_flag=True`. A transport timeout is not a model refusal. Those
three jsonl lines are wrong in the record.

## 12. `/v1/models` deprecation fields are never read
Mistral's model list carries `deprecation` and
`deprecation_replacement_model`. R7 ("nothing watches provider lifecycle")
has had its answer sitting in the provider's own API the whole time. This
is R11 instance 2 and the natural home of a lifecycle check.

## 13. CHARS_PER_TOKEN = 3 is not conservative for prose
Measured on the Regular Report across five trials: **2.64, 2.91, 2.97,
3.07, 3.30 — mean 2.98.** The constant's own comment calls 3 "deliberately
conservative" against "a measured mean of 4.42". Heavy markdown bold is the
likely cause. On this position the estimate is LOW, not high.

## 14. Two SDK-client legs still have no response_format
`lens_framing_rubrics.py` and `lens_s2a_injection.py` share one
`client.chat.completions.create()` across several providers, so adding the
constraint would send it to Cerebras, Cloudflare and Groq, which are
unprobed for it. CC-63 left both out on purpose. Ten of twelve legs carry
it; these are the two.

## 15. `_FORCE_PROVIDER` is written and never read
`lens_regular_report.py:310` sets it, nothing reads it, so the Cerebras leg
of that chain has never been reachable. Found 2026-08-05, still open.
**Lower priority now** — CC-68 made the primary live, and the Cerebras leg
it would reach is 402 anyway. Fix it when a real second leg exists.

---

# OPEN RULINGS
- **R10** — Claude's lean is RULE OUT, recorded at LENS-039 and unchanged
  through LENS-041. Never taken.
- **Item 1.3 fork (A/B/C/D)** — do not rule before 1.2 is probed.
- **Item 1.1** — tombstone Cerebras or leave it.

# DEFINITION OF DONE FOR ITEM 1
A wave in which at least one of the four orphaned positions completes on a
NON-ministral leg, with the model named in the log and `finish_reason`
readable. Falling back to ministral and succeeding is the current state and
does not count.
