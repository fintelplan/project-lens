"""
patch_add_ensemble_function.py
Inserts detect_operations_ensemble() into lens_framing_rubrics.py
Insert point: before 'if __name__ == "__main__":'
"""

RUBRICS_PATH = "code/lens_framing_rubrics.py"

ENSEMBLE_BLOCK = '''
# ══════════════════════════════════════════════════════════════════════════════
# Ensemble API — dual-provider detection (LENS-020 production architecture)
# ══════════════════════════════════════════════════════════════════════════════
# Architecture decision DECISION-009 (Apr 29 2026):
#   Primary:   qwen-3-235b on Cerebras  (OP-024-029 structural/apparatus)
#   Secondary: gpt-oss-120b on Cloudflare (OP-002/003/005/008/010/011/015/016/022)
#   Sequential calls with 2s sleep to avoid TPM quota collision.
#   Union of detected operations from both models.

import copy

def detect_operations_ensemble(
    article_title: str,
    article_body: str,
    article_source: str,
    voice_name: str,
    voice_type: str,
    state_actor_lens: str,
    stage_filter: str = "early_warning",
    inter_model_sleep: float = 2.0,
) -> DetectionResult:
    """Run dual-provider ensemble detection and return union of operations.

    Calls qwen-3-235b (Cerebras) then gpt-oss-120b (Cloudflare) sequentially.
    Returns a merged DetectionResult with union of detected operations.
    If one provider fails, returns the other's result (graceful degradation).
    If both fail, returns the first failure result.

    Args:
        inter_model_sleep: seconds to sleep between model calls (default 2s)
                           prevents TPM quota collision on shared Cerebras guard.
    """
    args = dict(
        article_title=article_title,
        article_body=article_body,
        article_source=article_source,
        voice_name=voice_name,
        voice_type=voice_type,
        state_actor_lens=state_actor_lens,
        stage_filter=stage_filter,
    )

    # ── Primary: qwen-3-235b on Cerebras ──
    original_provider = os.environ.get("S2F_PROVIDER", "groq")
    original_model = os.environ.get("CEREBRAS_MODEL", "")

    os.environ["S2F_PROVIDER"] = "cerebras"
    os.environ["CEREBRAS_MODEL"] = "qwen-3-235b-a22b-instruct-2507"
    log.info("[ENSEMBLE] Running primary: qwen-3-235b on Cerebras")
    result_primary = detect_operations_in_article(**args)
    log.info(f"[ENSEMBLE] Primary result: {result_primary.status} "
             f"({result_primary.operation_count()} ops)")

    # ── Sleep between models ──
    log.info(f"[ENSEMBLE] Sleeping {inter_model_sleep}s between models")
    time.sleep(inter_model_sleep)

    # ── Secondary: gpt-oss-120b on Cloudflare ──
    os.environ["S2F_PROVIDER"] = "cloudflare"
    os.environ["CLOUDFLARE_MODEL"] = "@cf/openai/gpt-oss-120b"
    log.info("[ENSEMBLE] Running secondary: gpt-oss-120b on Cloudflare")
    result_secondary = detect_operations_in_article(**args)
    log.info(f"[ENSEMBLE] Secondary result: {result_secondary.status} "
             f"({result_secondary.operation_count()} ops)")

    # ── Restore original env ──
    os.environ["S2F_PROVIDER"] = original_provider
    if original_model:
        os.environ["CEREBRAS_MODEL"] = original_model
    else:
        os.environ.pop("CEREBRAS_MODEL", None)

    # ── Graceful degradation ──
    primary_ok = result_primary.status == "OK"
    secondary_ok = result_secondary.status == "OK"

    if not primary_ok and not secondary_ok:
        log.warning("[ENSEMBLE] Both providers failed — returning primary failure")
        return result_primary

    if not primary_ok:
        log.warning("[ENSEMBLE] Primary failed — returning secondary only")
        return result_secondary

    if not secondary_ok:
        log.warning("[ENSEMBLE] Secondary failed — returning primary only")
        return result_primary

    # ── Merge: union of operations ──
    # Use primary as base. Add secondary ops not already detected by primary.
    primary_op_ids = {op["id"] for op in (result_primary.operations_detected or [])}
    secondary_unique = [
        op for op in (result_secondary.operations_detected or [])
        if op["id"] not in primary_op_ids
    ]

    merged_ops = list(result_primary.operations_detected or []) + secondary_unique
    merged_confidence = max(result_primary.confidence, result_secondary.confidence)

    log.info(
        f"[ENSEMBLE] Merged: {len(result_primary.operations_detected or [])} primary "
        f"+ {len(secondary_unique)} secondary-unique = {len(merged_ops)} total ops"
    )

    merged = copy.copy(result_primary)
    merged.operations_detected = merged_ops
    merged.confidence = merged_confidence
    merged.rubric_version = "v2-operations-ensemble"
    # food_for_thought: prefer primary's question (qwen-3 tends to be sharper here)
    if not merged.food_for_thought and result_secondary.food_for_thought:
        merged.food_for_thought = result_secondary.food_for_thought

    return merged

'''

TARGET = 'if __name__ == "__main__":'

with open(RUBRICS_PATH, "r", encoding="utf-8") as f:
    content = f.read()

if TARGET not in content:
    print("ERROR: insertion point not found")
    exit(1)

if "detect_operations_ensemble" in content:
    print("Already patched — ensemble function exists")
    exit(0)

content = content.replace(TARGET, ENSEMBLE_BLOCK + TARGET, 1)

with open(RUBRICS_PATH, "w", encoding="utf-8") as f:
    f.write(content)

print("PATCHED: ensemble function inserted")
print("Verify: grep -n 'detect_operations_ensemble' code/lens_framing_rubrics.py")
