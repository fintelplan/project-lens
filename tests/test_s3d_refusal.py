"""CC-110 -- S3-D records its refusals, and nothing that shapes its answer moved.

Checked by bytes: the record captures both the HTTP refusal and the transport
error, and writes once, only when no analysis came back. The sampling, prompt,
budget and timeout are the CC-93/94/96 ones (gate 9 guards them by behaviour).
    python tests/test_s3d_refusal.py
"""
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CODE = os.path.join(HERE, "..", "code")
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


b = open(os.path.join(CODE, "lens_s3d_longterm.py"), "rb").read()
i_http = b.find(b"_last = (r.status_code, r.text)")
i_warn = b.find(b'f"retryable={retryable} {r.text[:200]}")')
i_cont = b.find(b"continue", i_warn)
check("the HTTP refusal is kept after its warning and before continue", 0 < i_warn < i_http < i_cont, True)
i_tr = b.find(b"_last = (None, str(e))")
i_trw = b.find(b"transport {repr(e)[:160]}")
check("the transport error is kept after its warning", 0 < i_trw < i_tr, True)
check("recorded once, with the registry's provider and model",
      b.count(b"record_refusal(PROVIDER, MODEL, status=_last[0], text=_last[1])"), 1)
check("only when no analysis came back", b"if not analysis and _last is not None:" in b, True)
check("lazy: no module-scope import", re.search(rb"(?m)^(from|import) lens_provider_refusal", b) is None, True)

import lens_s3d_longterm as S3D
from lens_models import wire
p, m, k, mo = wire("s3d_longterm")
check("budget untouched: MAX_TOKENS is the registry's 16000", (S3D.MAX_TOKENS, mo), (16000, 16000))
check("timeout untouched: 240", S3D.REQUEST_TIMEOUT_S, 240)

print()
print(f"RESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
