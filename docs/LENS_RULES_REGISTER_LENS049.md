# LENS RULES REGISTER (LENS-049)

Project Lens -- Bro Alpha. **The one register.** New rules are appended here with the next free number;
a hole is indistinguishable from a lost rule, so numbers are never skipped from here on.

## Census at the LENS-047 close (bytes, `lens047_close_read.sh`)

- Until now LR rules were defined inside briefs, orders, CLAUDE.md and the foundations -- about 35 files.
- The highest id found anywhere in the tree is **LR-233** (`LENS_FOUNDATIONS_LENS042.md:194`); the briefs reach
  LR-183. Ids between them may be defined in formats the scan did not recognise.
- Four orders held earned rules with no number (written at the LENS-043, 044, 045 and 046 closes);
  LENS-043 had already written "register not read". The debt had been deferred four times.
- **Numbering decision (LENS-047, delegated):** new numbers start at **LR-234**, above every id seen, so
  no collision is possible whatever the older formats hold. Older rules keep their numbers where they
  were written; an older rule moves into this file when it is next cited, one at a time -- a bulk
  copy now would create a second source of truth for 233 rules at once.

## Entries

| LR | Rule | Earned at | First written in |
| --- | --- | --- | --- |
| LR-234 | A verification step that cannot fail is not a gate: before citing a check as evidence, confirm it would have gone red had the change been wrong (CC-87 shipped on a dry run that queried nothing). | LENS-043 | order LENS044 |
| LR-235 | A grep pattern built from memory indicts itself first: build it from a known-good line already read in that file. (Broken twice more at LENS-044 and a sixth time at LENS-045 -- same rule.) | LENS-043 | order LENS044 |
| LR-236 | Count before alarming: the head of a grep is not the population ("0 ops on every row"; the tail held three rows at 0.86). | LENS-043 | order LENS044 |
| LR-237 | Scan every file against the .env VALUES, not against patterns, before it leaves the machine. | LENS-044 | order LENS045 |
| LR-238 | A test double must enforce production's limits (CC-93's fake ignored PostgREST's 1000-row cap). | LENS-044 | order LENS045 |
| LR-239 | A check that a projection can satisfy is not an evidence check: count dates on or before today. | LENS-044 | order LENS045 |
| LR-240 | Do not raise a budget to revive a position whose input is stale -- that is pretend-right bias. | LENS-044 | order LENS045 |
| LR-241 | One prompt at temperature 0.3 varied by more than 1,800 output tokens: size on the long run. | LENS-044 | order LENS045 |
| LR-242 | A guard fed only by successes is inert exactly when everything fails. | LENS-044 | order LENS045 |
| LR-243 | Even by row count is not even in time: sample the dimension the charter names (days, not rows). | LENS-044 | order LENS045 |
| LR-244 | An instrument must not be stricter than what it measures (probe timeout 180 s vs production 240 s). | LENS-044 | order LENS045 |
| LR-245 | Set a live threshold on the real data's shape, not the synthetic one (gaps exist). | LENS-044 | order LENS045 |
| LR-246 | A substring check is only as good as everything else that contains the substring; check the call-site shape, and a fix's own comment must not satisfy or break its check (six instances). | LENS-045 | order LENS046 |
| LR-247 | A cert read must first prove the run carries the commit. | LENS-045 | order LENS046 |
| LR-248 | A documented reset time is a claim, not a measurement. | LENS-045 | order LENS046 |
| LR-249 | `// true` on a step that delivers something is a silent-failure switch. | LENS-045 | order LENS046 |
| LR-250 | A library's own log escapes your redaction: scan every scratch file, probe logs included. | LENS-045 | order LENS046 |
| LR-251 | Measure the bucket before widening what breathes from it. | LENS-045 | order LENS046 |
| LR-252 | A defect found in one position is a question for all its siblings. | LENS-045 | order LENS046 |
| LR-253 | A voice must count the wave, not the newest N rows. | LENS-045 | order LENS046 |
| LR-254 | A gate can find debt before it prevents it: write it as a ratchet that stops new harm and keeps the debt in sight. | LENS-045 | order LENS046 |
| LR-255 | Prove an org boundary without touching the thing you protect (headroom on the idle key). | LENS-045 | order LENS046 |
| LR-256 | Never hand a commit block in the same message as the script it depends on. | LENS-045 | order LENS046 |
| LR-257 | Run the guard before you rely on it: a proof that has not been executed is a promise. | LENS-045 | order LENS046 |
| LR-258 | A design that names a place must first find the place. | LENS-045 | order LENS046 |
| LR-259 | "In flight" in a record is not "shipped". | LENS-045 | order LENS046 |
| LR-260 | Count line endings by bytes, never by grep; an instrument that returns the same answer for every input is not measuring. | LENS-046 | order LENS047 |
| LR-261 | A commit message is shell input: -F with a quoted heredoc, never `!` in double quotes, written outside any conditional. | LENS-046 | order LENS047 |
| LR-262 | A synthetic rehearsal cannot reveal bytes you did not see: build it from bytes and rehearse every branch you rely on. | LENS-046 | order LENS047 |
| LR-263 | Before trusting a zero, run a positive control. | LENS-046 | order LENS047 |
| LR-264 | A deferral must carry its reopening trigger; when the trigger ships, reopen every sibling. | LENS-046 | order LENS047 |
| LR-265 | "Done" needs a closure test: a stored prediction is not a checked one. | LENS-046 | order LENS047 |
| LR-266 | A prompt's "never" line is dead text unless a gate or an output check enforces it. | LENS-046 | order LENS047 |
| LR-267 | De-identification: scan names and metadata, joined text, case-insensitively; write an all-or-nothing change only after every step passed. | LENS-046 | order LENS047 |
| LR-268 | A model change is an adaptive-maintenance event: re-validate the outputs against the purpose. | LENS-046 | order LENS047 |
| LR-269 | Filter first, cap last: a limit applied before a filter can empty the result (S2-F: newest 8, then the length check, 0 scored). | LENS-047 | this register |
| LR-270 | Choose a send rule's constants by replaying it on the real history, not by estimate. | LENS-047 | this register |
| LR-271 | An off-delay counts only the days the pipeline ran: a dark pipeline is not a finding that ended. | LENS-047 | this register |
| LR-272 | A delivery stamp in the operator's own field is two faults (it overwrites him and cannot say what was told); keep a journal. | LENS-047 | this register |
| LR-273 | Strip an Actions log's job/step columns before grepping it: the job name matches words you search for. | LENS-047 | this register |
| LR-274 | Run bite mutants with bytecode caching off, in a fresh directory: a stale .pyc serves the old mutant. | LENS-047 | this register |
| LR-275 | A step with no logging configuration drops its INFO lines: check a module's log reaches the run before relying on it. | LENS-047 | this register |
| LR-276 | A checkout under core.autocrlf rewrites the working copy's endings: detect the EOL at patch time, never from an earlier read. | LENS-047 | this register |
| LR-277 | A source-bytes test must be line-ending blind, or it fails on a Windows checkout and passes in CI. | LENS-047 | this register |
| LR-278 | Before patching a file, list every test that reads it -- a byte-window test broke when four lines went inside a block it reads. | LENS-047 | this register |
| LR-279 | A replay must carry state across runs, or it cannot show a rotation. | LENS-047 | this register |
| LR-280 | Order a rotation by when, not by how many: ordering by past volume starves the heaviest source. | LENS-047 | this register |
| LR-281 | Before designing a bound on evidence, check that the evidence varies: a saturated confidence cannot move a level. | LENS-047 | this register |
| LR-282 | A probe that passes on mechanics can fail on truth: read the model's reasons, not only its label (CC-123 invented a previous basis). | LENS-047 | this register |
| LR-283 | A label must count what it says: 23 rows were 8 articles x 3 lenses. | LENS-047 | this register |
| LR-284 | A finding rests on its sample: before calling a voice's pattern confirmed, check how that voice came to be sampled (RT was 89% of S2-F's detections because it was the collector's last thread). | LENS-047 | this register |
| LR-285 | Model text entering an HTML message is escaped at the one sender, always: Telegram refuses the whole message on one unknown tag (Sep 25: `<sign>` lost the S3 message). | LENS-047 (post-close) | this register |
| LR-286 | A fix for CI is proved in CI's shape before it ships: run the gate with the packages CI lacks hidden, not only in a venv that has them all (CC-127's gate went red on `No module named 'cerebras'`). | LENS-047 (post-close) | this register |
| LR-287 | A script named in a reply must exist before the reply asks for it to be run: create the file and dry-run it, then hand it over (LENS-048 asked for `lens048_certs.sh` that had not been written). | LENS-048 | this register |
| LR-288 | A census by one instrument sees only what that instrument logs: Mistral's SDK writes no `HTTP Request:` line, so the host census read zero Mistral calls; every host claimed as zero needs its own positive control. | LENS-048 | this register |
| LR-289 | Read the component's record before offering a hypothesis about it: "Cloudflare starves only in the evening" had been disproven in the LENS-045 record (first call refused after the 00:00Z reset). | LENS-048 | this register |
| LR-290 | A sender's result is read, never dropped: an orchestrator that ignores `False` from its message sender is silent by construction (S3, CC-128; S2 had the same rule since CC-117). | LENS-048 | this register |
| LR-291 | A mutant whose anchor does not match exactly once did not run -- that is a missing proof, not a pass; a rehearsal on rebuilt files cannot show duplicated lines (CC-130's anchor matched 9 times in the real file). | LENS-048 | this register |
| LR-292 | Output held until the end looks like a hang: a long step prints progress as it goes, and secret scans run on the saved file afterwards. | LENS-048 | this register |
| LR-293 | Measure demand before adding supply: 92% of S2-F's calls came back not_applicable and fed no reader; a second provider would have doubled the waste (ITIL demand management). | LENS-048 | this register |
| LR-294 | A relevance gate belongs before the selection, not inside the loop: items it skips write no row, so a gate in the loop leaves them unscored and their sources "oldest" in the rotation for ever (LR-269, LR-280). | LENS-048 | this register |
| LR-295 | A key the caller owns is never taken from the model's answer: `parsed.get("state_actor_lens", ...)` let the model rename a lens ("Xi Office", 64 rows). | LENS-048 | this register |
| LR-296 | One column, one quantity: a field named confidence that holds five different measures makes every max over it a false number (injection_reports.confidence_score). | LENS-048 | this register |
| LR-297 | An instrument checks everything the claim covers: an evidence-verbatim check must search all the model was shown (title + body); body-only read 61% for what was 89%. | LENS-048 | this register |
| LR-298 | A cert covers what its bytes covered: "L2.5 certified" rested on the canary and S2 messages; the Regular Report caption, another sender, still carried markdown. | LENS-048 | this register |
| LR-299 | A rate limit is not the only budget: a monthly allowance shared by one organization is the axis a new consumer can exhaust for every position on it, the canary included. | LENS-048 | this register |
| LR-300 | A standing alarm that asks for no response is removed at its cause, not hidden: a dead primary called every wave kept a DOWN line in PROVIDER HEALTH for four months (ISA-18.2; CC-131). | LENS-048 | this register |
| LR-301 | A position whose prompt is built inside its entry point is probed through that entry point with database writes suppressed and captured -- never through a copied prompt (C probe, LENS-048). | LENS-048 | this register |
| LR-302 | A dated death on a provider model is a claim with a source date: re-read the provider's page before acting on it ("gemini-2.5-flash dies 2026-10-16" had been withdrawn). | LENS-048 | this register |
| LR-303 | A provider's displayed meter and its enforcement can differ: when they disagree, bank a prediction that separates the two readings and let the next run decide (Cloudflare showed 0/10k used while refusing the first call). | LENS-048 | this register |
| LR-304 | A discriminating test is sized on the limit the bytes show, not the documented one: every Cloudflare cut came at ~11-12k, so a test built on "10k" could not separate a calendar day from a rolling window. | LENS-048 | this register |
| LR-305 | An order on a mixed column ahead of a limit is a selection, not a sort: it decides whose rows reach the reader (the Compendium's order-then-limit-30 kept 0 of S2-E's 8 rows, CC-135). | LENS-049 | this register |
| LR-306 | A dict built from ordered rows keeps the last row: the order decides which row survives (`lens_ref_system` kept each analyst's LOWEST-scored row, CC-135). | LENS-049 | this register |
| LR-307 | Before fixing a position, read its run record: a position that never runs where the change ships cannot be field-certified (S3-E is `SKIPPED_CI` by PHI-002; the Forensic Report has been manual-only since May). | LENS-049 | this register |
| LR-308 | Mutants count only against a gate that is green without them: a gate already red makes every mutant "bite" (CC-136's first run). | LENS-049 | this register |
| LR-309 | A product named in a commit message is a claim: read which workflow runs the file before naming it (CC-135/136 called `lens_forensic_report.py` "the Regular Report"; it is the dormant Forensic Report). | LENS-049 | this register |
| LR-310 | A claim a commit message makes about bytes is checked by the commit script before it commits, and the script aborts if the bytes do not back it (CC-138's known-limit claim). | LENS-049 | this register |
| LR-311 | "Relation does not exist" is first a question about the instrument: list the tables the editor can see before trusting it (the SQL Editor was on another project, LENS-049). | LENS-049 | this register |
| LR-312 | A retry spends the same budget as the call it repeats: count it inside the cap, never retry a quota refusal, keep retries near 10% of calls (Google SRE retry budget; CC-137). | LENS-049 | this register |
| LR-313 | Two copies of a safety function drift apart: keep one implementation and make the other delegate (`_redact`: one copy lacked "URL", CC-140). | LENS-049 | this register |
| LR-314 | Masking a secret URL is not masking its host: exceptions name `host='...'` without the scheme (CC-140). | LENS-049 | this register |
| LR-315 | A section built from the newest row says when that row was written: newest is not current (the Brief's SYSTEM 3 lagged a wave, CC-141). | LENS-049 | this register |
| LR-316 | `tee` overwrites: a script re-run after success loses the first log. Guard every commit script on the expected HEAD so a second run aborts (CC-139/140 ran twice; the guard held). | LENS-049 | this register |
| LR-317 | A check that needs a package CI lacks must say it was skipped, and the step's count shows it (CC-139/140: 16 of 17 in CI, python-docx absent; LR-286 kin). | LENS-049 | this register |

Ranges: LENS-043 LR-234..LR-236; LENS-044 LR-237..LR-245; LENS-045 LR-246..LR-259; LENS-046 LR-260..LR-268; LENS-047 LR-269..LR-284; LENS-047 post-close LR-285..LR-286; LENS-048 LR-287..LR-304; LENS-049 LR-305..LR-317.
Broken again at LENS-049 and not re-numbered: LR-246 (a file-wide `conf=` check, CC-136; a CI grep on a count
three tests share), LR-236 (`head -1` picked the wrong PROVIDER HEALTH file), LR-258 (the forensic label; S3-E),
LR-289 (S3-E's record not read), LR-260 (a dry-run counted CRs with grep).
Next free number: **LR-318**.
