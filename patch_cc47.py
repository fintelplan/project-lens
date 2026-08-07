from pathlib import Path

p = Path("code/lens_mission_analyst.py")
b = p.read_bytes()
crlf = b.count(b"\r\n"); lf = b.count(b"\n") - crlf
print("BEFORE  CRLF", crlf, "bare_LF", lf)
assert not crlf, "expected pure LF -- STOP"
assert b"S3_RESERVE_CHARS" not in b, "ALREADY PATCHED -- STOP"

# edit 1 -- raise the cap by exactly the reserve, and record why
old = b"MAX_TOTAL_CHARS  = 28000"
new = (b"# CC-47: S3 context is appended LAST and was being dropped whenever S2\n"
       b"# filled the cap -- reopening the S1->S2->S3->MA circuit that closes at\n"
       b"# fetch_s3_context(). Measured 2026-08-07 (run 31071967358 / MA #257):\n"
       b"# the S3 section is 949 chars and MA ran at prompt=7741 completion=2606\n"
       b"# total=10347, i.e. ~34% of the Cerebras 30,000-token ceiling. So the\n"
       b"# circuit was being cut over ~3% of the budget while two thirds of the\n"
       b"# envelope sat unused. S3 now gets a RESERVED slot, and the cap rises by\n"
       b"# exactly that reserve so S2 keeps the same 28,000 it has today.\n"
       b"S3_RESERVE_CHARS = 4000\n"
       b"MAX_TOTAL_CHARS  = 32000")
n = b.count(old); assert n == 1, "cap anchor %d != 1 -- STOP" % n
b = b.replace(old, new, 1)

# edit 2 -- S2 loop stops at the reserve boundary, not the full cap
old2 = (b"        if total_chars + len(entry) > MAX_TOTAL_CHARS:\n"
        b"            log.info(f\"Prompt cap reached at S2 entry for {analyst}\")\n")
new2 = (b"        if total_chars + len(entry) > MAX_TOTAL_CHARS - S3_RESERVE_CHARS:\n"
        b"            log.info(f\"Prompt cap reached at S2 entry for {analyst} \"\n"
        b"                     f\"(total_chars={total_chars} s2_budget=\"\n"
        b"                     f\"{MAX_TOTAL_CHARS - S3_RESERVE_CHARS})\")\n")
n = b.count(old2); assert n == 1, "S2 break anchor %d != 1 -- STOP" % n
b = b.replace(old2, new2, 1)

p.write_bytes(b)
d = p.read_bytes()
print("AFTER   CRLF", d.count(b"\r\n"), "bare_LF", d.count(b"\n") - d.count(b"\r\n"))
print("DONE")
