import os, datetime
from dotenv import load_dotenv
from supabase import create_client
load_dotenv()
url = os.environ.get("SUPABASE_URL")
name = next((n for n in ("SUPABASE_SERVICE_KEY","SUPABASE_SERVICE_ROLE_KEY","SUPABASE_KEY")
             if os.environ.get(n)), None)
print("utc_now:", datetime.datetime.now(datetime.timezone.utc).isoformat())
print("url_set:", bool(url), "key_env:", name)
sb = create_client(url, os.environ[name])
for t in ("lens_reports","lens_raw_articles","injection_reports","lens_macro_reports"):
    try:
        r = sb.table(t).select("id").limit(1).execute()
        print(t, "OK rows=", len(r.data))
    except Exception as e:
        print(t, "ERR", repr(e)[:220])
