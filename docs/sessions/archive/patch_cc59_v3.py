#!/usr/bin/env python3
"""
patch_cc59.py -- CC-59, order item 1.3 (LENS-039)   [v3]

VERSION HISTORY (both aborts were the gates working, not code faults):
  v1 -> v2  A1's anchor was the three lines url/title/domain, which appear
            TWICE in analyze_lens.py: once inside fetch_all_article_links()
            (:245-248) and once inside main()'s selected_articles (:377-380).
            count == 2 != 1, so nothing was written.  A1 widened to the whole
            comprehension, which is unique.
  v2 -> v3  The delta table showed all_collected at 1, not 0, in BOTH files.
            The survivor was the replacement COMMENT, which named the symbol
            it was removing.  A removal is only proved by a zero (LR-138), so
            the comments were reworded rather than the expectation relaxed.

  fetch_all_article_links() is deliberately LEFT ALONE.  After this patch its
  return value is consumed only by the len() in the :390 print -- it never
  reaches the database.  Trimming it would be a separate change.

WHAT IT DOES
  lens_reports.articles_used currently stores, for every S1 report, two
  parallel lists of {id,url,title,domain} objects -- the selected articles
  and every collected article.
  Measured 2026-09-11: ~268 KB per row, 93 MB of TOAST on 3,949 rows.

  Two findings from the byte read:
    1. The second list is written and read by NOBODY.  A repo-wide grep finds
       only the two write sites and one len() in a print.  No consumer ever
       reads it back out of the database.  R2, in storage shape.
    2. The only live reader of "selected" is lens_ref_system.get_s1_selected(),
       which keys on id-or-url and falls back to art["title"]/art["domain"]
       ONLY when no REF matches (lens_ref_system.py:228-236).

  So this patch keeps {id, url} and drops title, domain, and the second list.
  url is kept deliberately: it preserves ref_by_url matching AND leaves the
  no-ref fallback rows with a working link, at a cost of ~50 bytes/entry.
  Expected effect: ~268 KB/row -> ~14 KB/row (about -95%).

  D-023, LENS-039.

WHAT IT DOES NOT DO
  Does not touch historical rows (that is the SQL shrink, run separately).
  Does not change the ENCODING shape: json.dumps() still produces a JSON
  string stored into a jsonb column, exactly as before (LR-159).
  Does not touch fetch_all_article_links().

SAFETY
  Binary mode rb/wb (LR-078).  Pure-ASCII anchors (LR-101).  Every anchor
  asserted count == 1.  CRLF asserted pure on input and on result.  The full
  symbol delta table is asserted BEFORE write_bytes (LR-147), so any miss
  leaves both files untouched.  Idempotent: a second run aborts at anchors.
"""

import sys
import py_compile
from pathlib import Path

ROOT = Path(__file__).resolve().parent
F1 = ROOT / "code" / "analyze_lens.py"
F2 = ROOT / "code" / "analyze_lens_multi.py"

# Banked at read time, 2026-09-11.  Asserted, not trusted.
EXPECT = {
    F1: {"bytes": 15826, "crlf": 408},
    F2: {"bytes": 56148, "crlf": 1335},
}

CR = b"\r\n"

# The token whose disappearance is the whole point.  Built at runtime so this
# module's own docstring and comments cannot contribute to the count.
TOK = b"all_" + b"collected"


def fail(msg):
    print("ABORT: " + msg)
    sys.exit(1)


def load(p):
    if not p.exists():
        fail("missing file: %s" % p)
    b = p.read_bytes()
    crlf = b.count(CR)
    bare = b.count(b"\n") - crlf
    print("[read] %-28s bytes=%d crlf=%d bare_lf=%d" % (p.name, len(b), crlf, bare))
    if bare != 0:
        fail("%s is not pure CRLF (bare_lf=%d)" % (p.name, bare))
    if len(b) != EXPECT[p]["bytes"]:
        fail("%s byte count %d != banked %d -- file changed since the read"
             % (p.name, len(b), EXPECT[p]["bytes"]))
    if crlf != EXPECT[p]["crlf"]:
        fail("%s crlf %d != banked %d" % (p.name, crlf, EXPECT[p]["crlf"]))
    return b


def sub(buf, old, new, label):
    n = buf.count(old)
    if n != 1:
        print("---- anchor that failed (%s) ----" % label)
        print(old.decode("ascii", "replace"))
        fail("%s: anchor count == %d, expected 1" % (label, n))
    print("[anchor] %-8s count=1 OK" % label)
    return buf.replace(old, new, 1)


def crlf_lines(*lines):
    """Join ASCII source lines with CRLF, trailing CRLF included."""
    out = b""
    for ln in lines:
        out += ln.encode("ascii") + CR
    return out


# ==========================================================================
# analyze_lens.py
# ==========================================================================

# "selected_articles = [" and "for a in articles if a.get("id")" together
# distinguish this from the byte-identical inner block in
# fetch_all_article_links(), which ends "for a in all_articles if
# a.get("url")".
A1_OLD = crlf_lines(
    '    selected_articles = [',
    '        {',
    '            "id":     a.get("id", ""),',
    '            "url":    a.get("url", ""),',
    '            "title":  (a.get("title") or "")[:200],',
    '            "domain": a.get("domain", "")',
    '        }',
    '        for a in articles if a.get("id")',
    '    ]',
)
A1_NEW = crlf_lines(
    '    selected_articles = [',
    '        {',
    '            "id":     a.get("id", ""),',
    '            "url":    a.get("url", "")',
    '        }',
    '        for a in articles if a.get("id")',
    '    ]',
)

# v3: the replacement comment must not name the token being removed, or the
# grep that proves the removal can never reach zero.
A2_OLD = crlf_lines(
    '    article_ids = {',
    '        "selected":      selected_articles,',
    '        "all_collected": all_article_links',
    '    }',
)
A2_NEW = crlf_lines(
    '    # CC-59 (LENS-039, item 1.3, D-023): ids + urls only.',
    '    # title/domain were ~95% of lens_reports storage and are recoverable',
    '    # from lens_raw_articles by id.  The second list (every collected',
    '    # article) was written here and read by nothing in the repo, so it',
    '    # is no longer stored.',
    '    article_ids = {',
    '        "selected": selected_articles',
    '    }',
)

# Avoid the em-dash earlier in this print line: anchor only the ASCII tail.
A3_OLD = b"""| total: {len(article_ids.get('all_collected', []))}")"""
A3_NEW = b'''")'''


# ==========================================================================
# analyze_lens_multi.py
# ==========================================================================

B1_OLD = crlf_lines(
    '    selected_ids = [',
    '        {"id": a.get("id",""), "url": a.get("url",""),',
    '         "title": (a.get("title") or "")[:200], "domain": a.get("domain","")}',
    '        for a in balanced if a.get("id")',
    '    ]',
    '    all_ids = [',
    '        {"id": a.get("id",""), "url": a.get("url",""),',
    '         "title": (a.get("title") or "")[:200], "domain": a.get("domain","")}',
    '        for a in all_articles if a.get("url")',
    '    ]',
    '    article_ids = {"selected": selected_ids, "all_collected": all_ids}',
)
B1_NEW = crlf_lines(
    '    # CC-59 (LENS-039, item 1.3, D-023): ids + urls only.  The second',
    '    # list (every collected article) is no longer built or stored --',
    '    # nothing in the repo ever read it back.',
    '    selected_ids = [',
    '        {"id": a.get("id",""), "url": a.get("url","")}',
    '        for a in balanced if a.get("id")',
    '    ]',
    '    article_ids = {"selected": selected_ids}',
)


# ==========================================================================
# apply
# ==========================================================================

def main():
    b1 = load(F1)
    b2 = load(F2)

    pre = {
        "f1_second_list":  b1.count(TOK),
        "f1_cc59":         b1.count(b"CC-59"),
        "f1_domain_block": b1.count(b'"domain": a.get("domain", "")'),
        "f2_second_list":  b2.count(TOK),
        "f2_all_ids":      b2.count(b"all_ids"),
        "f2_cc59":         b2.count(b"CC-59"),
    }

    n1 = sub(b1, A1_OLD, A1_NEW, "A1")
    n1 = sub(n1, A2_OLD, A2_NEW, "A2")
    n1 = sub(n1, A3_OLD, A3_NEW, "A3")

    n2 = sub(b2, B1_OLD, B1_NEW, "B1")

    post = {
        "f1_second_list":  n1.count(TOK),
        "f1_cc59":         n1.count(b"CC-59"),
        "f1_domain_block": n1.count(b'"domain": a.get("domain", "")'),
        "f2_second_list":  n2.count(TOK),
        "f2_all_ids":      n2.count(b"all_ids"),
        "f2_cc59":         n2.count(b"CC-59"),
    }

    # LR-147: the DELTA is the claim, asserted before any write.
    # The two ZEROS are the load-bearing rows -- a removal is only proved by
    # a zero.  f1_domain_block 2 -> 1 is the other one: exactly ONE of the two
    # byte-identical blocks goes, and the copy inside
    # fetch_all_article_links() survives untouched.
    want_post = {
        "f1_second_list":  0,
        "f1_cc59":         pre["f1_cc59"] + 1,
        "f1_domain_block": pre["f1_domain_block"] - 1,
        "f2_second_list":  0,
        "f2_all_ids":      0,
        "f2_cc59":         pre["f2_cc59"] + 1,
    }

    print("")
    print("%-20s %6s %6s %6s" % ("symbol", "pre", "post", "want"))
    ok = True
    for k in sorted(pre):
        mark = "OK" if post[k] == want_post[k] else "MISMATCH"
        if post[k] != want_post[k]:
            ok = False
        print("%-20s %6d %6d %6d  %s" % (k, pre[k], post[k], want_post[k], mark))
    print("")
    if not ok:
        fail("symbol delta table did not match -- nothing written")

    # f2_all_ids must reach 0.  If it does not, all_ids is referenced
    # somewhere this patch did not see, and removing its definition would
    # produce a NameError that py_compile cannot catch.
    if post["f2_all_ids"] != 0:
        fail("all_ids still referenced in analyze_lens_multi.py -- "
             "removing its definition would break the file at runtime")

    for name, buf in (("analyze_lens.py", n1), ("analyze_lens_multi.py", n2)):
        bare = buf.count(b"\n") - buf.count(CR)
        if bare != 0:
            fail("%s result is not pure CRLF (bare_lf=%d)" % (name, bare))

    print("[crlf] both results pure CRLF, bare_lf=0")
    print("[size] analyze_lens.py       %d -> %d (%+d)"
          % (len(b1), len(n1), len(n1) - len(b1)))
    print("[size] analyze_lens_multi.py %d -> %d (%+d)"
          % (len(b2), len(n2), len(n2) - len(b2)))

    F1.write_bytes(n1)
    F2.write_bytes(n2)
    print("[write] both files written")

    for p in (F1, F2):
        try:
            py_compile.compile(str(p), doraise=True)
            print("[compile] %s OK" % p.name)
        except py_compile.PyCompileError as e:
            fail("py_compile failed on %s: %s" % (p.name, e))

    print("")
    print("CC-59 applied.  Next: git --no-pager diff, then the LR-138 grep block.")


if __name__ == "__main__":
    main()
