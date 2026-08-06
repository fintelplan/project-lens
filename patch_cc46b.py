from pathlib import Path
for path, old, new in [
    (".github/workflows/lens-manage-analyze.yml",
     b"openai tzdata openpyxl cohere python-docx",
     b"openai tzdata openpyxl cohere python-docx lxml"),
    ("requirements.txt",
     b"python-docx==1.1.2",
     b"python-docx==1.1.2\nlxml"),
]:
    p = Path(path); b = p.read_bytes()
    crlf = b.count(b"\r\n"); lf = b.count(b"\n") - crlf
    E = b"\r\n" if crlf and not lf else b"\n"
    print(path, "crlf=%d bare_lf=%d" % (crlf, lf))
    assert b"lxml" not in b, "%s ALREADY HAS lxml -- STOP" % path
    n = b.count(old); assert n == 1, "%s anchor count %d != 1 -- STOP" % (path, n)
    p.write_bytes(b.replace(old, new.replace(b"\n", E), 1))
    print("  patched")
