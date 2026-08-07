from pathlib import Path
p = Path("docs/LENS_TARGET_AND_ORDER.md")
b = p.read_bytes()
print("BEFORE  CRLF", b.count(b"\r\n"), "bare_LF", b.count(b"\n") - b.count(b"\r\n"))
assert b.count(b"\r\n") == 0, "expected pure LF -- STOP"
assert b"WAVE SEQUENCING" not in b, "ALREADY AMENDED -- STOP"

old = b"- CC-47 -- S3 context now has a reserved 4,000-char slot. Awaiting cert.\n"
new = (b"- CC-47 -- S3 context reserved slot. CERTIFIED on MA 660da99 (14:31 UTC):\n"
       b"  total_chars=27664 at the S2 break plus an 876-char S3 section = 28540,\n"
       b"  which EXCEEDS the old 28000 cap -- without the reserve S3 would have\n"
       b"  been dropped on that wave. S2's budget stayed at 28000.\n")
n = b.count(old); assert n == 1, "cert anchor %d != 1 -- STOP" % n
b = b.replace(old, new, 1)

old2 = b"### OTHERS\n"
new2 = b"""6c WAVE SEQUENCING -- NO ORDERING GUARANTEE  [R3]
   Collect and Manage+Analyze are independently scheduled AND independently
   lagged. MA is nominally 28 min behind Collect; Collect has measured
   16m08s, 23m35s and 27m15s. Lag order is NOT guaranteed -- on Aug 5, S2-F
   (cron 30) started BEFORE MA (cron 28). If MA starts before Collect
   finishes it analyses the PREVIOUS wave's articles and reports SUCCESS,
   which is a silent failure under the declared target. Also unexamined:
   GDELT fires ~17 min into MA's ~27 min run, so enrichment writes land
   mid-analysis; Compendium and Ref Export both fire at 30 2.

6d S1 BREAK LOG -- the missing half of the CC-47 side-effect check  [R3]
   The S1 loop uses MAX_TOTAL_CHARS * 0.6 and has NO break log, so S1's
   share of total_chars is invisible. CC-47 raised the cap, moving S1's
   allotment 16800 -> 19200; whether S1 consumes it cannot be answered
   without this log. Tonight's total_chars=27664 is the FIRST measurement
   of the S2 break, so there is no before-figure to compare against.
   Promoted from OTHERS: it is the only way to settle whether CC-47 cost
   S2 content.

### OTHERS
"""
n = b.count(old2); assert n == 1, "OTHERS anchor %d != 1 -- STOP" % n
p.write_bytes(b.replace(old2, new2, 1))
d = p.read_bytes()
print("AFTER   CRLF", d.count(b"\r\n"), "bare_LF", d.count(b"\n") - d.count(b"\r\n"))
print("DONE")
