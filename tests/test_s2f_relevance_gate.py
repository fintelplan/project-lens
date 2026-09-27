"""CC-129 + CC-130 -- S2-F scores only lenses an article is about; rows carry the caller's lens.

LENS-048: 92% of 737 scorings in 30 days were not_applicable, and 64 rows carried the model's
own spelling ("Xi Office") instead of the lens S2-F asked about ("xi_office").
    python tests/test_s2f_relevance_gate.py
"""
import os
import sys
import json
import types

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.join(HERE, "..")
CODE = os.environ.get("LENS_CODE_DIR") or os.path.join(ROOT, "code")
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


def blind(b):
    return b.replace(b"\r\n", b"\n")   # LR-277


import lens_s2f_relevance as G
L3 = ["xi_office", "trump_office", "khamenei_office"]

print("== the gate ==")
check("China story -> xi", G.relevant_lenses("Beijing sets new tariffs", "Chinese officials said...", L3), ["xi_office"])
check("Iran story -> khamenei", G.relevant_lenses("Tehran talks stall", "The IRGC said...", L3), ["khamenei_office"])
check("central banks -> none", G.relevant_lenses("Central banks hold rates", "Inflation eased in the euro area and in Japan.", L3), [])
check("Trump story -> trump", G.relevant_lenses("Trump signs order", "The president said...", L3), ["trump_office"])
check("one article, two lenses", G.relevant_lenses("Trump meets Xi", "", L3), ["xi_office", "trump_office"])
check("unknown lens fails open", G.relevant_lenses("anything", "", ["putin_office"]), ["putin_office"])
check("Title-case lens name is the same lens", G.relevant_lenses("Iran", "", ["Khamenei Office"]), ["Khamenei Office"])
check("only the text the model sees", G.relevant_lenses("x", "a" * 5000 + " Iran", L3), [])
check("word boundary: 'Xiamen' is not xi", G.relevant_lenses("Xiamen port", "", ["xi_office"]), [])

print("== the cron: filter first, cap last (bytes) ==")
cron = blind(open(os.path.join(CODE, "lens_s2f_scoring_cron.py"), "rb").read())
i_f = cron.find(b"rows = [r for r in rows if relevant_lenses(")
i_s = cron.find(b"picked, report = select_articles(rows, last, max_articles, exclude_ids=scored_ids)")
check("selection reads only articles about a lens", i_f != -1, True)
check("the filter runs before the cap (LR-269)", i_f != -1 and i_s != -1 and i_f < i_s, True)
check("each lens is gated before its call", b"if lens not in rel:" in cron, True)
check("calls are capped by S2F_MAX_CALLS", b'os.environ.get("S2F_MAX_CALLS", "24")' in cron, True)
check("the run says what the gate did", b"S2F_RELEVANCE" in cron, True)
wf = blind(open(os.path.join(ROOT, ".github", "workflows", "lens-s2f-scoring.yml"), "rb").read())
check('workflow: S2F_MAX_CALLS "24"', b'S2F_MAX_CALLS:' in wf and b'"24"' in wf, True)

print("== CC-130: the row carries the caller's keys, not the model's echo ==")
import lens_framing_rubrics as R


class _NoGuard:
    def wait_if_needed(self, *a, **k): pass
    def log_usage(self, *a, **k): pass


answer = {"state_actor_lens": "Xi Office", "stage_filter": "all", "catalog_version": "v0.0-echo",
          "operations_detected": [], "operations_not_present": [], "not_applicable": True, "confidence": 0.1}
reply = types.SimpleNamespace(choices=[types.SimpleNamespace(message=types.SimpleNamespace(content=json.dumps(answer)))])
client = types.SimpleNamespace(chat=types.SimpleNamespace(completions=types.SimpleNamespace(create=lambda **k: reply)))
R._get_llm_client = lambda: (client, "fake-model", "fake")
R._guard_for = lambda p: _NoGuard()
r = R.detect_operations_in_article(article_title="t", article_body="word " * 200, article_source="s",
                                   voice_name="v", voice_type="author", state_actor_lens="xi_office",
                                   stage_filter="early_warning")
check("status", r.status, "OK")
check("lens is the caller's", r.state_actor_lens, "xi_office")
check("stage is the caller's", r.stage_filter, "early_warning")
check("catalog version is the catalog's", r.catalog_version != "v0.0-echo", True)

print()
print(f"RESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
