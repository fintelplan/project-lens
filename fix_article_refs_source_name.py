"""
fix_article_refs_source_name.py
Backfills source_name in lens_article_refs from lens_raw_articles.
Fixes "Unknown" source_name appearing in Forensic Report references.

Run once: python fix_article_refs_source_name.py
"""
import os
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])

# Fetch refs with Unknown source_name
print("Fetching refs with Unknown source_name...")
refs = sb.table("lens_article_refs") \
    .select("ref_id, raw_article_id, source_name") \
    .eq("source_name", "Unknown") \
    .limit(500).execute().data or []

print(f"Found {len(refs)} refs with Unknown source_name")
if not refs:
    print("Nothing to fix!")
    exit(0)

# Fetch corresponding raw articles
article_ids = [r["raw_article_id"] for r in refs if r.get("raw_article_id")]
print(f"Fetching {len(article_ids)} raw articles...")

# Batch fetch in chunks of 50
fixed = 0
for i in range(0, len(article_ids), 50):
    chunk = article_ids[i:i+50]
    articles = sb.table("lens_raw_articles") \
        .select("id, source_name") \
        .in_("id", chunk).execute().data or []
    
    art_map = {a["id"]: a["source_name"] for a in articles if a.get("source_name")}
    
    for ref in refs:
        if ref.get("raw_article_id") in art_map:
            new_name = art_map[ref["raw_article_id"]]
            if new_name and new_name != "Unknown":
                try:
                    sb.table("lens_article_refs") \
                        .update({"source_name": new_name}) \
                        .eq("ref_id", ref["ref_id"]).execute()
                    fixed += 1
                except Exception as e:
                    print(f"Error updating {ref['ref_id']}: {e}")

print(f"Fixed {fixed}/{len(refs)} refs")
