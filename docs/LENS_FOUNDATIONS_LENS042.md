# Project Lens — Foundations (consolidated canon)

`docs/LENS_FOUNDATIONS_LENS042.md` · assembled at LENS-042 (2026-09-17) · Bro Alpha (no team name)

> **Read this before changing any position, workflow, model, key, prompt or test.**
> Implementation drift is recoverable. Philosophy drift is not. (DOC-006, LENS-013)

This file gathers what already exists in the repo and in the session records into one
text file an agent can grep. It does not replace the originals. Every section names its
source. Where the source is lost or unclear it says **UNKNOWN — James to confirm**.
Where the canon disagrees with itself, it says **TENSION — James to rule** instead of
choosing.

| Source | Where | Date |
|---|---|---|
| PHI-001 Origin Story | `philosophies/lens-PHI-001_origin_story.docx` | LENS-001, Apr 11 |
| PHI-002 Global Class Sustainable Personality | **no file in the repo** — James's words in the LENS-004 chat (Apr 12), wired into every lens prompt | Apr 12 |
| PHI-003 Popular Sovereignty | `lens-PHI-003_popular_sovereignty.docx` | LENS-017, Apr 20–21 |
| PHI-004 Cognitive Sovereignty Cadence | `lens-PHI-004_cognitive_sovereignty_cadence.docx` | LENS-019 |
| DOC-006 Foundations | `lens-DOC-006_foundations_LENS013.docx` | LENS-013, Apr 17 |
| DOC-007 Guard System Spec | `data/lens-DOC-007_guard_system_spec.docx` | LENS-014, Apr 18 |
| S2-F architecture v4 | `LENS-020_S2F_architecture_decision_v4.md` | Apr 29 |
| Canary gas-mask test | `CLAUDE.md` §0 | LENS-042 (CC-81) |
| AI-origin guideline | James's ruling | LENS-042, Sep 17 |

---

## Part A — The soul

### A1. PHI-001 — Let's dig behind the screen (verbatim)

> You see eggs next to bread and milk at the marketplace. Is that a coincidence? We started
> asking that same question about everything around us. We are living in the age of AI and
> Quantum now. Things are happening fast. We want to dig behind the screen and find out what
> is really going on. We will share everything we find. You can dig too. Lets dig together.
>
> This is the soul of Project Lens. Every decision connects back to this.

### A2. PHI-002 — Global Class Sustainable Personality (James's words, LENS-004)

> **On Sustainability and Human Rights.** A safe environment, a law-governed society, the
> right to grow up with an education suited to one's development, accumulated experience,
> insight, the ability to think and envision broadly, prosperity, and the freedom to shape
> one's own future without harming others' — sustainability is the fundamental right of
> every person.
>
> "Global Class Sustainable Personality" means having an education, experiences, insights,
> thinking, prosperity, and the freedom to create one's own future — all suited to one's
> growth — without causing harm to others' future.
>
> **On Human Dignity and Freedom.** Project Lens supports the growth of people's ability to
> "Global Class Sustainable Personality". It stands against the oppression and suppression
> of people. It stands against ethnic and religious extremism. At the beginning of humanity,
> there was no concept of ethnicity or religion. While ethnic and religious traditions
> should be valued and recorded as part of human history, we must remain conscious of the
> bloodshed throughout history caused by ethnic and religious fanaticism. The core point is
> that equal human worth and fundamental rights must be recognized above ethnic and
> religious identity.
>
> A person's coming of age means developing the ability to "Global Class Sustainable
> Personality". Any organization or government that blocks people's right to reach that
> maturity is opposed. The original and permanent owner of sovereignty is always the
> people. Unilateral seizure of power that suppresses the people's right to elect and
> extend rule is opposed. The use of pseudo-democracy to unilaterally oppress and surveil
> the people is opposed. The use of feudal history as justification to block and suppress
> the people's fundamental rights is opposed.

Distilled in the same sessions: **pro-people and anti-pretense** · "Project Lens is a
pretense detector" · "real for people's right, not pretended" · Cui Bono is the first
question before any conclusion · watch for the manufactured Sectarian Trap.

Standing prompt rules that came with it (LENS-004 SHARED_RULES): never make predictions,
never say "will happen" · Food for Thought = open questions that make people think deeper ·
stick to facts from the articles, no invention · be direct, no fluff.

**GAP:** PHI-002 exists only in chat and in prompts. Recommended: commit it as
`philosophies/lens-PHI-002_global_class_sustainable_personality.md` with the text above.

### A3. PHI-003 — Popular sovereignty and the apparatus–people separation (summary)

- The people are the permanent, original owner of sovereignty. Legitimacy comes from
  consent expressed through free elections, free press, independent courts, protected
  dissent.
- **Lens analyses apparatus, never peoples.** Say "Xi Office / CCP Politburo", "Putin
  Office", "Trump Office", "Khamenei Office", "Min Aung Hlaing junta" — never "China",
  "Russia", "US", "Iran", "Myanmar" as shorthand. The peoples are the beneficiaries, not the
  targets.
- **Asymmetric legitimacy accounting.** Rubrics are structurally symmetric; the weight of a
  finding is not. Elected, term-bounded offices ≠ hybrid offices ≠ unelected / fake-elected /
  indefinite offices. "Neutrality is not symmetry between rigged and genuine. Neutrality is
  fidelity to evidence."
- **Findings discipline.** Every output says "pattern warrants review" with alternative
  hypotheses. Never "entity X is a state asset of Y." Output that conflates a people with an
  apparatus is a rubric bug — fix the rubric, do not ship the finding.
- Protects against: Sectarian Trap in our own voice, false equivalence, reflexive pro-US
  bias, "Asian values" exceptionalism, single-villain reduction.
- Reviewed annually; the baseline is stable, its application follows evidence.

### A4. PHI-004 — Cognitive sovereignty cadence (summary)

- "The alert is the medicine. The timing is the dose."
- Short-form media erodes sustained attention; psychological warfare exploits that. The
  delivery cadence is itself the intervention.
- **Three alerts:** Watch (day 7–10, ≥3 articles, LOW confidence, "pattern may be
  forming") → Clarity (day 14–21, ≥8, MEDIUM, shows which suspects dropped out) →
  Verification (day 30–45, ≥15, HIGH, includes the whole arc).
- Deployment is phased: **Direction B** (operator-gated test) before **Direction A**
  (public, with uncertainty framing).
- Protects against false-confidence acceleration and detection that arrives too late.

### A5. Audience

Global Game Changers — GCSP educators and emerging global leaders who need to see through
information warfare. "A report that looks authoritative but was built on injection is worse
than no report at all, because it teaches the wrong lesson to the people the project is
meant to serve." (DOC-006 closing)

---

## Part B — The architecture logic (DOC-006)

### B1. The origin question
*When the information environment is engineered, how do you still see what is actually
happening?* Not better sources (they get captured), not better models (they absorb injection
more fluently), not harder filtering (it destroys the evidence). Instead: let the attack in
deliberately, study it with a second system, place it in deep-time memory with a third,
judge it with a fourth. Intelligence emerges from the **difference** between what each
system sees.

### B2. The Four-Step Pattern of History
Technology changes who can accumulate power → power redistribution creates new biases →
new biases create new threats → threats produce new technology responses. Historical
structure predates the news cycle, so current information warfare cannot corrupt it. This is
why S3-B and S3-D matter more than they appear to.

### B3. The Earthquake (LENS-008) and the canary
The first clean 4/4 cross-lens run agreed on "weaponized / crackdown / suppression". All four
lenses had read the same 39 coordinated sources inside three hours. The cross-lens check had
confirmed a coordinated injection. The instinct was to protect S1. Bro Alpha refused.

> A canary in a coal mine that wears a gas mask stops being a canary. System 1 breathes the
> poisoned air so the poison can be measured.

Immune-system logic, not security logic: let the pathogen in, study it, build memory.

### B4. One-way flow is absolute
S3 never writes to S2 or S1. S2 never writes to S1 and never reads S3. S1 reads only external
sources. If S3 could write back, a compromised S3 corrupts the canary; if S2 could read S3, a
compromised S3 pre-filters what S2 looks for; if S1 could read S2/S3, the canary is protected
and stops being a canary.

### B5. The five injection patterns (checked by every S2 and S3 analysis)
1. **Attention architecture** — loud accurate stories crowd out quiet strategic ones.
   Defence: permanence multiplier.
2. **Manufactured consensus** — many outlets, same framing, same hours. Defence: perfect
   consensus is a RED FLAG, not confidence.
3. **Emotional language injection** — loaded words bypass evaluation.
4. **Broken window + butterfly** — the visible outrage hides the structural change.
   Defence: permanence weighting.
5. **Recursive self-injection** — stored output re-read as own reasoning; confidence rises
   without new evidence. Defence: recursive audit; rising confidence without evidence = STOP.

**TENSION — James to rule:** DOC-006 lists Pattern 3's defence as "a sanitization layer
strips emotional loading before lenses read." Non-negotiable #2 says S1 is never protected.
The two cannot both hold for S1's input. Record which one governs.

### B6. Canary gas-mask test (CLAUDE.md §0, LENS-042)
Before any change **or any command**, ask:
1. **Filtering** — does this narrow, clean or pre-screen what a lens reads?
2. **Air supply** — could this stop a lens or Collection, or spend the quota they breathe?
3. **Manufacturing** — does this push an instrument to produce output (retry until a score
   clears, fill an empty reading, fall back onto another lens's model)?
4. **The tool itself** — does the test/probe/script use a canary key
   (`lens_models.canary_air_keys()`) or write with the canary's voice (Telegram "What the
   canary sees", `lens_reports`)?

A "yes" is a stop: say it plainly, then ask James.

---

## Part C — Non-negotiables

From DOC-006 Part 11, with later rulings marked.

1. **One-way flow is absolute.**
2. **S1 is never protected.** Every proposal to sanitize S1 input is refused.
3. **$0 is a design constraint, not a limitation.** Free providers are chosen for epistemic
   diversity — different institutional bias is the feature.
   *Amended LENS-042 by C8:* China-related providers and models are no longer part of that
   diversity.
4. **Source verifiability is mandatory.** Every finding traceable to `REF-YYYYMMDD-NNNN`
   (LR-070). No citations = no evidence.
5. **Free first, evidence before upgrade** (LR-068). Never upgrade on theory.
6. **Read before edit, always** (LR-006). Check → re-check → counter-check (LR-233).
7. **No DeepSeek anywhere** (founding). Extended by D-004 (Jul 27) and by C8.
8. **AI engines and AI brains — origin guideline (James, LENS-042, 2026-09-17).**
   Use only engines (providers, hosts) and brains (models) from a *Freedom from Fear*
   environment. **China-related: never, no exception** — including models fine-tuned on a
   China-origin base. **Any other origin that is not clearly Freedom-from-Fear: the agent
   stops and asks James** (case-by-case; no external index decides). Example needing a
   ruling before use: `allam-2-7b` on Groq.
9. **Loud failure always beats silent success** (target since LENS-036). A position that
   fails must say so in the log, the status and the run summary.
10. **Canary lenses take no cross-lens fallback** (LENS-042, CC-76). A fallback onto another
    lens's model collapses epistemic diversity; a dead provider must show FAILED.

---

## Part D — Engineering principles (DOC-007 and the session record)

- **"The main duty of every guard system is not to fail main system."** (James, LENS-014)
- **History before assumption.** Read why a mechanism exists before redesigning it; bytes say
  what IS, history says which side is canonical.
- **Pre-flight connects to main flight by logic, not timing.** A cron that runs broken code
  on time is worse than one that waits for verification.
- **Safety is the gate, not effort.** Ask "will main system fail?", not "how much work?".
- Five properties of a guard: complete coverage · early detection · fail-safe defaults ·
  independent of main system · self-evident.
- Five guard layers: preflight · quota · response · write · audit.
- A test harness must never reach production (LENS-042, CC-78).
- Probe before wiring (LR-106/107); a censored measurement cannot size a cap (LR-177); moving
  input invalidates a cross-run comparison (LR-178); derive fixtures from the position, not
  from a constant (LR-179); run every CI gate locally before a push (LR-180).

---

## Part E — Position charters

Format: **why it exists · what it reads · what it must produce · cadence · model rationale ·
state at LENS-042 · drift from charter.** "Why" text comes from DOC-006 unless marked.

### System 1 — the unprotected canary
*Why:* observe the world as reported, unprotected, so S2 can detect injection from outside.
*Reads:* external sources only. *Produces:* four independent lens reports in `lens_reports`
tagged `system=S1, protected=false, injection_assumed=true`, plus cross-lens coordination
signals (3+/4 lenses). *Why four:* pretense hides from one angle; four perspectives on the
**same** articles make sustained pretense hard.

| Lens | Question | Model (LENS-042) | Drift |
|---|---|---|---|
| 1 Foundation | GCSP human rights — who is blocked from growing freely? | Groq gpt-oss-120b | **Reads STATE-tier articles only** (trim takes state first) — violates "same articles"; key shared with Collection's saturated Groq bucket |
| 2 Physical Reality | What is materially true on the ground? | Google gemini-2.5-flash | Shutdown date uncertain (Oct 16/20) |
| 3 Causal Chain | First Domino — what causes what? | Cohere command-r-plus | Restored CC-76 (dead Aug 17–Sep 17) |
| 4 Sovereignty Check | From/of/for the people — who is pretending? | Mistral ministral-8b | Restored CC-76; lenses 3 and 4 had shared one model since May (S1-001 shape) |

Also recorded: each lens was written four times per wave from at least May until CC-75.

### System 2 — the immune watcher
*Why:* analyse how S1 was manipulated this run and find what S1's sources collectively
cannot see. Never analyses the world directly; never reads S3.

| Position | Question (DOC-006) | Drift at LENS-042 |
|---|---|---|
| S2-A Injection Tracer | Q1 How was S1 manipulated? | — |
| S2-B Coordination Analyzer | Q2 Which sources coordinated? Reads **raw** articles | Primary `gemini-2.0-flash` dead since Jun; runs on fallback |
| S2-C Emotion Decoder | Q3 PRIME→TRIGGER→FRAME→DELIVER→ANCHOR | Single leg (ministral) |
| S2-D Adversary Narrative | Q4 What is the injection trying to make S1 believe? Reads adversary state media | Cerebras primary dead; ministral fallback |
| S2-E Legitimacy Filter | Q5 How deep did injection penetrate? | Cerebras primary dead; ministral fallback; every call logged "for unknown" |
| S2-GAP | What did **none** of the sources show? (black swan / ostrich) | — |
| Mission Analyst | Synthesis; corrections enforced in **Python**, not prompt; Cui Bono 3-tier | Cerebras primary dead; LENS-034 found S1 almost never reaches its prompt (status of item 1.2: check the order) |
| **S2-F** (LENS-020) | Operation detection against the OPS-001 catalog, per apparatus office (PHI-003) | See below |

**S2-F tier design (LENS-020 v4, ratified Apr 29):** Watch = Cerebras qwen (structural ops
OP-024–029, Sectarian Trap) · Clarity = qwen + Cloudflare gpt-oss **ensemble, chosen for
complementary detection profiles** · Verification = mistral-medium (European lineage, deepest
rhetorical detection). *State at LENS-042:* Cerebras is dead, so every row labelled
`ensemble` came from Cloudflare alone; the **structural-ops detector is gone**; Cloudflare's
free pool serves ~60% of calls; mistral-medium 429s. The qwen legs are now also barred by C8.
**Charter gap: S2-F needs a new structural-ops detector from an allowed origin.**

### System 3 — deep-time memory
*Why:* stand outside the current information frame that S1 and S2 share. Reads everything;
writes back to nothing.

| Position | Capability / purpose | Cadence | Drift at LENS-042 |
|---|---|---|---|
| S3-A Pattern Intelligence | Cap. 1 — what sequence is forming behind the noise? 7-day window; ACH + Sectarian Trap checks | daily | Restored CC-74 (Cohere). Fetch takes the **oldest** 20 rows of the window (`asc limit`) |
| S3-B True History | Cap. 4 — match events to structural historical analogs (Four-Step Pattern) | per wave | Primary `gemini-2.0-flash` dead; ministral fallback truncates; `summary` stores raw JSON |
| S3-C Bias Drift Monitor | Cap. 3 — vocabulary drift, framing lock, confidence without evidence, analytical convergence, across 30 days, from outside | weekly (DOC-006 says Sundays; code runs Mon/Thu with a weekly guard) | — |
| S3-D Long-term Researcher | Cap. 2 — **manufactured causality** (injection day 1–7 → solidification day 8–30+; needs claim **and** outcome) + Cap. 4 structural. "Hold ALL reports simultaneously … across the full window." Model rationale: "largest free model, maximum reasoning depth." | Mon 30-day · Thu 90-day (180-day deferred until True History has 90+ days) | **Failing** (Cerebras dead; ministral cap 2500 truncates). Fetch takes the **oldest** 30/90 rows, so the "90-day" run reads ~3 days of June. Record says `provider=cerebras` whatever answered |
| S3-E Self-Check | S3 reading S3 — did we converge or drift? Model-family independence | local only | Skipped in CI by design (PHI-002 local-only); DOC-006 names SambaNova, which is dead — **UNKNOWN which local model** |
| S3-F Counter-Check | Challenges S3-A/S3-D for over-claim | Mon/Thu | Restored CC-71; its S3-D input is two weeks old |
| S3 Strategic Report | Six-part long-horizon briefing to Telegram | per wave | Restored CC-77 |

### System 4 — the conscience
*Why:* judge the other systems over time without authority over them; S3 is food for
thought, never command. S4 predictions seeded from S3-A's first domino; S4-B outcome
verifier (month 3–6); S4-C calibration (month 6–12); S4-E upgrade-trigger monitor.
*State:* **UNKNOWN at LENS-042** — not read this session.

### Reports and supporting workflows

| Workflow (`.github/workflows`) | UTC cron | Charter | Notes |
|---|---|---|---|
| `lens-collect.yml` | 13:00, 01:00 | The canary's **air**: external sources (Tier A–D) + entity extraction | Shares `GROQ_API_KEY` with Lens 1 |
| `lens-gdelt.yml` | 13:45, 01:45 | Enrich collection with GDELT | GDELT was blocked on GitHub IPs (DOC-006) — current state UNKNOWN |
| `lens-manage-analyze.yml` | 13:28, 01:28 | S1 → S1 report → S2 → S3 | S2/S3 run `if: !cancelled()` (CC-75) |
| `lens-s2f-scoring.yml` | 13:30, 01:30 | S2-F scoring + Watch/Clarity/Verification aggregators (PHI-004) + Direction B | See S2-F |
| `lens-regular-report.yml` | 02:10 | Free daily report to Telegram; PART 4 references built in code (REF IDs, LR-070) | Successor to the paid Sonnet layer? **UNKNOWN — James to confirm** |
| `lens-compendium.yml` | 02:30 | Intelligence compendium | Charter text UNKNOWN |
| `lens-ref-export.yml` | 02:30, 14:30 | Reference export (1of2 / 2of2 pools) for source verifiability | — |
| `lens-forensic-report.yml` | manual | Forensic report incl. Architect-Hypothesis (PHI-003, LR-083) | — |
| `lens-resume.yml` | manual | Wall-checkpoint resume (LR-052..056) | — |
| `lens-ci.yml` | push | Compile · registry self-test · quota tests · response-schema tests | — |

**Paid layer (DOC-006 Part 8):** a Sonnet 4.6 S2+S3 report, 2×/day, earned per LR-068.
Current status **UNKNOWN — James to confirm**.

---

## Part F — Rulings and tensions awaiting James

1. **Pattern 3 sanitization vs "S1 is never protected"** (Part B5).
2. **S2-F structural-ops detector** — Cerebras qwen is dead and barred; what replaces it?
3. **S3-D input** — the charter needs the whole window (early claims and later outcomes);
   the code reads the oldest rows. Sample evenly across the window? (Newest-first would also
   break Capability 2.)
4. **S3-D model** — "maximum reasoning depth" vs the free models left that are not canary
   air (ministral-8b is the smallest in the stack).
5. **Lens 1 input** — state-only reading vs "four perspectives on the same articles", weighed
   against its saturated Groq air.
6. **S3-E** — which local model, from an allowed origin.
7. **Attribution** — DOC-007 and PHI-003 carry "Team Geeks"; Lens is Bro Alpha's solo project
   with no team name. Correct in the originals?
8. **PHI-002 file** — commit the text in A2 as the canonical file?

---

## Part G — How to use this file

1. Before touching a position or workflow, read its charter row and Part C.
2. If the change moves the position away from its charter, **stop and ask James**.
3. If the charter is wrong or out of date, propose the edit here in the same commit as the
   code, and say why.
4. Never cite this file as the source of a philosophy — cite the original named in the table
   at the top.

*Let's dig behind the screen.*
