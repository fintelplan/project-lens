"""
patch_add_cloudflare_provider.py
Inserts Cloudflare Workers AI provider branch into lens_framing_rubrics.py
Insert point: before '    # Default: groq' (line ~363)
"""
import re

RUBRICS_PATH = "code/lens_framing_rubrics.py"

CLOUDFLARE_BLOCK = '''    if provider == "cloudflare":
        try:
            from openai import OpenAI
        except ImportError:
            log.error("openai SDK not installed — pip install openai")
            return None, None, None
        token = os.environ.get("CLOUDFLARE_API_TOKEN", "")
        account_id = os.environ.get("CLOUDFLARE_ACCOUNT_ID", "")
        if not token or not account_id:
            log.error("S2F_PROVIDER=cloudflare but CLOUDFLARE_API_TOKEN or CLOUDFLARE_ACCOUNT_ID not set")
            return None, None, None
        cf_model = os.environ.get("CLOUDFLARE_MODEL", "@cf/openai/gpt-oss-120b")
        client = OpenAI(
            api_key=token,
            base_url=f"https://api.cloudflare.com/client/v4/accounts/{account_id}/ai/v1",
        )
        log.info(f"Using Cloudflare Workers AI provider (model: {cf_model})")
        return client, cf_model, "cloudflare"
'''

with open(RUBRICS_PATH, "r", encoding="utf-8") as f:
    content = f.read()

TARGET = "    # Default: groq"
if TARGET not in content:
    print("ERROR: insertion point not found")
    exit(1)

if 'provider == "cloudflare"' in content:
    print("Already patched — cloudflare branch exists")
    exit(0)

content = content.replace(TARGET, CLOUDFLARE_BLOCK + TARGET, 1)

with open(RUBRICS_PATH, "w", encoding="utf-8") as f:
    f.write(content)

print("PATCHED: Cloudflare Workers AI branch inserted")
print("Verify with: grep -n 'cloudflare' code/lens_framing_rubrics.py")
