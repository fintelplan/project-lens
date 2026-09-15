import os, collections
try:
    from dotenv import load_dotenv; load_dotenv()
except Exception:
    pass
from supabase import create_client
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])
rows, start, step = [], 0, 500
while True:
    r = sb.table("injection_reports").select("created_at,evidence") \
        .eq("analyst", "S2-E").order("created_at", desc=False) \
        .range(start, start + step - 1).execute()
    d = r.data or []
    rows += d
    if len(d) < step:
        break
    start += step
print("S2-E rows:", len(rows))
if not rows:
    raise SystemExit("no rows")
print("range:", rows[0]["created_at"], "->", rows[-1]["created_at"])
tiers, depth, months = collections.Counter(), collections.Counter(), collections.defaultdict(list)
actors, lows, mand = [], [], 0
for x in rows:
    ev = x.get("evidence") or {}
    aa = ev.get("actors_assessed") or []
    lo = ev.get("low_legitimacy_actors") or []
    cm = ev.get("correction_to_ma") or {}
    actors.append(len(aa)); lows.append(len(lo))
    months[x["created_at"][:7]].append(len(aa))
    for a in aa:
        tiers[a.get("legitimacy_tier", "?")] += 1
    if cm.get("mandatory"):
        mand += 1
    depth[cm.get("contamination_depth", "NONE")] += 1
n = len(rows)
print("actors/row: mean %.2f  min %d  max %d" % (sum(actors)/n, min(actors), max(actors)))
print("low/row:    mean %.2f  rows with >=2 low: %d" % (sum(lows)/n, sum(1 for v in lows if v >= 2)))
print("tier counts:", dict(tiers))
print("MANDATORY corrections: %d of %d (%.1f%%)" % (mand, n, 100.0*mand/n))
print("depth:", dict(depth))
for m in sorted(months):
    v = months[m]
    print("  %s  rows=%3d  actors/row=%.2f" % (m, len(v), sum(v)/len(v)))
