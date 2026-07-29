"""
smoke_cerebras_sdk.py -- LENS-028 STAGE 6 pre-cert SDK smoke test.

Why this exists: the CC-5 probe proved gpt-oss-120b over Cerebras' OpenAI-
compatible HTTP endpoint. It did NOT prove the cerebras-cloud-sdk response
OBJECT that CC-1c's usage logging reaches into. Those are different surfaces.
If `getattr(usage, "completion_tokens_details", None)` returns None because the
SDK hands back a dict rather than a model, the log line degrades to 'n/a'
silently and the first sign of trouble would be a cert that looks fine.

READ-ONLY. Writes nothing to Supabase and nothing to disk. Three trivial calls
(~1,200 tokens each) on CEREBRAS_API_KEY, spaced to respect the console's
short-interval enforcement warning.

Run:  python scripts/smoke_cerebras_sdk.py
"""
import os
import sys
import time

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "code"))

from dotenv import load_dotenv
load_dotenv(os.path.join(REPO, ".env"))

from lens_models import assert_model_known, fit_max_tokens, LensModelRegistryError

# role -> (module name, real prompt size measured in CC-5)
TARGETS = [
    ("s2d_adversary",   "lens_s2d_adversary",   11_589),
    ("s2e_legitimacy",  "lens_s2e_legitimacy",   9_300),
    ("mission_analyst", "lens_mission_analyst", 30_072),
]

SMOKE_PROMPT = "Reply with exactly one short sentence confirming you are online."


def describe(obj, indent="      "):
    """Every readable field on an SDK object, without assuming its type."""
    if obj is None:
        return f"{indent}(None)"
    if isinstance(obj, dict):
        return "\n".join(f"{indent}{k} = {v!r}" for k, v in obj.items())
    fields = [a for a in dir(obj) if not a.startswith("_")
              and not callable(getattr(obj, a, None))]
    if not fields:
        return f"{indent}(no readable fields; repr={obj!r})"
    return "\n".join(f"{indent}{f} = {getattr(obj, f)!r}" for f in fields)


def main():
    print("=" * 74)
    print("STAGE 6 PRE-CERT -- Cerebras SDK response-object smoke test")
    print("=" * 74)
    failures = []

    for i, (role, modname, real_prompt_chars) in enumerate(TARGETS):
        if i:
            time.sleep(5)  # respect short-interval enforcement (RPM 5)
        print(f"\n---- {role}  ({modname}) " + "-" * (46 - len(role) - len(modname)))
        mod = __import__(modname)

        provider, model = mod.PROVIDER, mod.MODEL
        print(f"  wire        : {provider}/{model}")
        print(f"  key_env     : {mod.KEY_ENV} (value never printed)")
        print(f"  max_out     : {mod.MAX_OUT}")

        try:
            assert_model_known(provider, model)
            print("  registry    : assert_model_known OK")
        except LensModelRegistryError as e:
            print(f"  registry    : FAILED -- {e}")
            failures.append(f"{role}: unregistered pair")
            continue

        fitted = fit_max_tokens(real_prompt_chars, mod.MAX_OUT, provider, model)
        print(f"  fit_max_tokens({real_prompt_chars}, {mod.MAX_OUT}) = {fitted}")
        if fitted < mod.MAX_OUT:
            print(f"  NOTE        : fitted BELOW max_out -- ceiling is binding")

        try:
            client = mod.get_client()
        except Exception as e:
            print(f"  client      : FAILED -- {type(e).__name__}: {e}")
            failures.append(f"{role}: get_client failed")
            continue
        print(f"  client      : {type(client).__module__}.{type(client).__name__}")

        try:
            resp = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": SMOKE_PROMPT}],
                max_tokens=1000,
                temperature=0.2,
            )
        except Exception as e:
            print(f"  CALL FAILED : {type(e).__name__}: {str(e)[:200]}")
            failures.append(f"{role}: SDK call failed")
            continue

        content = (resp.choices[0].message.content or "").strip()
        print(f"  content     : {content[:70]!r}")
        print(f"  finish      : {resp.choices[0].finish_reason}")

        usage = getattr(resp, "usage", None)
        print(f"  usage type  : {type(usage).__module__}.{type(usage).__name__}")
        print("  usage fields:")
        print(describe(usage))

        # --- the exact getattr chain CC-1c relies on -----------------------
        print("  -- CC-1c getattr chain --")
        total = getattr(usage, "total_tokens", None)
        print(f"    usage.total_tokens                     = {total!r}"
              f"   {'OK' if total else 'BROKEN -> log_usage would be skipped'}")
        if not total:
            failures.append(f"{role}: total_tokens unresolvable")

        details = getattr(usage, "completion_tokens_details", None)
        print(f"    usage.completion_tokens_details        = "
              f"{type(details).__name__ if details is not None else None}")
        if details is not None:
            print(describe(details, indent="        "))
        reasoning = getattr(details, "reasoning_tokens", None) if details else None
        verdict = ("OK" if reasoning is not None
                   else "resolves to None -> logs 'n/a' (acceptable only if the "
                        "provider truly omits it)")
        print(f"    ...reasoning_tokens                    = {reasoning!r}   {verdict}")
        if details is not None and reasoning is None:
            failures.append(f"{role}: details present but reasoning_tokens unreachable")

        comp = getattr(usage, "completion_tokens", None)
        if comp and total:
            print(f"    budget_used% would print               = "
                  f"{comp / 1000:.0%}  (vs max_tokens 1000)")

    print("\n" + "=" * 74)
    if failures:
        print("SMOKE TEST FAILURES:")
        for f in failures:
            print(f"  - {f}")
    else:
        print("ALL THREE OK -- SDK usage object resolves exactly as CC-1c expects")
    print("=" * 74)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
