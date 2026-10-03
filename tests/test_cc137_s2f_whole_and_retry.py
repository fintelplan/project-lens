"""CC-137 -- order item 2: S2-F scores an article whole or not at all, asks once more on invalid
JSON inside its call cap, and its coverage line no longer says the exit stays green.
Bytes (LENS-049): the cap was checked inside the lens loop (lens_s2f_scoring_cron.py:189), so an
article could be cut between two relevant lenses -- and the selection excludes any article with a
row, so its other lenses were never scored. One PARSE_FAILED in 17 calls had no second ask (CC-127
gave Mission Analyst one; LR-252). Guideline: Google SRE handling overload -- a retry budget near
10% of calls, never a retry on out of quota.
    python tests/test_cc137_s2f_whole_and_retry.py
"""
import os
import sys

ROOT = os.environ.get("LENS_ROOT_DIR") or os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
CODE = os.path.join(ROOT, "code")
sys.path.insert(0, CODE)
PASS = 0
FAIL = 0


def check(name, got, want):
    global PASS, FAIL
    if got == want:
        PASS += 1
        print(f"  PASS  {name}: {got!r}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}: got {got!r} want {want!r}")


src = open(os.path.join(CODE, "lens_s2f_scoring_cron.py"), "rb").read().decode("utf-8").replace("\r\n", "\n")
import lens_s2f_scoring_cron as C

check("an article that fits the calls left is started", C.article_fits(7, 3, 10), True)
check("an article that does not fit is not started", C.article_fits(8, 3, 10), False)
check("the retry budget is one in ten calls", (C.retry_budget(10), C.retry_budget(24), C.retry_budget(5)), (1, 2, 1))
check("invalid JSON is asked once more when the article still fits",
      C.may_retry("PARSE_FAILED", 0, 1, 5, 3, 10), True)
check("no second retry past the budget", C.may_retry("PARSE_FAILED", 1, 1, 5, 3, 10), False)
check("a quota refusal is never retried", C.may_retry("SKIP_QUOTA", 0, 1, 1, 1, 10), False)
check("an LLM failure is not retried here", C.may_retry("LLM_FAILED", 0, 1, 1, 1, 10), False)
check("no retry that would cut the article's next lens", C.may_retry("PARSE_FAILED", 0, 1, 8, 3, 10), False)
check("main starts an article only when it fits", "        if not article_fits(calls, len(need), max_calls):" in src, True)
check("main retries through may_retry", "                if may_retry(result.status, retries, retries_allowed, calls," in src, True)
check("the run logs what it deferred and retried", 'log.info(f"S2F_BUDGET deferred={deferred}' in src, True)
check("the coverage line no longer says the exit stays green", "the exit stays green" in src.replace('was "the exit stays green"', ""), False)
print()
print(f"RESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
