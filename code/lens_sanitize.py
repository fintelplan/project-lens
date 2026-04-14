"""
lens_sanitize.py — Language Sanitization Layer
Project Lens | LENS-010

PURPOSE:
  S2-A sanitizes its OWN reading of S1 reports before analysis.
  System 1 stays UNPROTECTED (canary by design).
  The replaced phrases are themselves injection evidence — they reveal
  what emotional loading was present in the original text.

ARCHITECTURE:
  call sanitize_text(text) → {sanitized_text, replaced_phrases, injection_detected}
  replaced_phrases flows into S2-A's injection_type=EMOTIONAL_PRIME findings.
  The vocabulary list is self-growing: new flags from S2-A findings can be added.
"""

import re
import logging

log = logging.getLogger("sanitize")

# ── Injection vocabulary ───────────────────────────────────────────────────────
# Format: injected_phrase → neutral_replacement
# Injected phrases are emotionally pre-loaded judgments dressed as neutral language.
# Replacements describe the same factual reality without the emotional architecture.
INJECTION_VOCABULARY: dict[str, str] = {
    # Violence / coercion framing
    "weaponized":               "used as a tool",
    "weaponise":                "use as a tool",
    "weaponizing":              "using as a tool",
    "weaponised":               "used as a tool",
    "crackdown":                "enforcement action",
    "cracking down":            "enforcing restrictions",
    "cracked down":             "enforced restrictions",
    "strongman":                "leader with centralized authority",
    "iron fist":                "strict governance",
    "iron grip":                "strong centralized control",

    # Suppression / control framing
    "suppression":              "restriction",
    "suppressed":               "restricted",
    "suppressing":              "restricting",
    "silenced":                 "restricted from speaking",
    "muzzled":                  "restricted",
    "clamped down":             "restricted",
    "clamp down":               "restrict",
    "purge":                    "removal",
    "purging":                  "removing",
    "purged":                   "removed",

    # Authoritarian labeling
    "authoritarian overreach":  "government policy action",
    "authoritarian":            "centralized-authority",
    "totalitarian":             "state-controlled",
    "dictatorial":              "non-democratic",
    "tyrannical":               "without democratic oversight",
    "despotic":                 "autocratic",

    # Alarm / urgency amplification
    "alarming":                 "notable",
    "alarmingly":               "notably",
    "unprecedented":            "significant",
    "shocking":                 "significant",
    "bombshell":                "significant development",
    "explosive":                "significant",
    "stunning":                 "significant",
    "brazen":                   "overt",
    "blatant":                  "clear",
    "egregious":                "significant",

    # Conflict / threat escalation
    "dangerous":                "consequential",
    "dangerously":              "consequentially",
    "threat":                   "concern",    # only if used loosely — be surgical
    "escalation":               "increase",
    "spiraling":                "increasing",

    # Agency / motive implication
    "emboldened":               "continued with",
    "emboldened by":            "following",
    "exploit":                  "use",
    "exploiting":               "using",
    "exploited":                "used",
    "orchestrated":             "coordinated",
    "engineered":               "arranged",

    # Democratic erosion framing
    "assault on democracy":     "action affecting democratic institutions",
    "attack on democracy":      "action affecting democratic institutions",
    "undermining democracy":    "affecting democratic processes",
    "subverting democracy":     "affecting democratic processes",
    "gutting":                  "significantly reducing",
    "dismantling":              "restructuring",
}

# Phrases that S2-A findings have flagged at runtime (grows over time)
# In production these would be loaded from Supabase lens_sanitize_vocabulary table
RUNTIME_FLAGS: list[str] = []


def sanitize_text(text: str) -> dict:
    """
    Strip emotional loading from text before S2 analysis.
    S1 stays unprotected — this only runs inside S2 positions.

    Returns:
        sanitized_text:     text with injection vocabulary replaced
        replaced_phrases:   list of original phrases that were replaced
        injection_detected: bool — True if any replacements were made
        replacement_count:  int — number of replacements made
    """
    if not text:
        return {
            "sanitized_text": "",
            "replaced_phrases": [],
            "injection_detected": False,
            "replacement_count": 0,
        }

    sanitized = text
    replaced_phrases = []

    # Apply core vocabulary replacements (longest phrases first to avoid partial matches)
    sorted_vocab = sorted(INJECTION_VOCABULARY.items(), key=lambda x: len(x[0]), reverse=True)
    for injected, neutral in sorted_vocab:
        pattern = re.compile(re.escape(injected), re.IGNORECASE)
        if pattern.search(sanitized):
            replaced_phrases.append(injected)
            sanitized = pattern.sub(neutral, sanitized)

    # Apply any runtime-flagged phrases
    for phrase in RUNTIME_FLAGS:
        pattern = re.compile(re.escape(phrase), re.IGNORECASE)
        if pattern.search(sanitized):
            replaced_phrases.append(phrase)
            sanitized = pattern.sub("[flagged]", sanitized)

    injection_detected = len(replaced_phrases) > 0
    if injection_detected:
        log.debug(f"Sanitized {len(replaced_phrases)} injection phrases: {replaced_phrases[:5]}")

    return {
        "sanitized_text": sanitized,
        "replaced_phrases": replaced_phrases,
        "injection_detected": injection_detected,
        "replacement_count": len(replaced_phrases),
    }


def add_runtime_flag(phrase: str) -> None:
    """Add a phrase to the runtime flag list (from S2-A findings feedback loop)."""
    if phrase and phrase not in RUNTIME_FLAGS and phrase not in INJECTION_VOCABULARY:
        RUNTIME_FLAGS.append(phrase.lower())
        log.info(f"[sanitize] Added runtime flag: '{phrase}'")


def get_vocabulary_size() -> int:
    """Return current vocabulary size (core + runtime)."""
    return len(INJECTION_VOCABULARY) + len(RUNTIME_FLAGS)


# ── Self-test ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    test_cases = [
        "The administration weaponized immigration policy to crackdown on dissent",
        "An alarming and unprecedented crackdown on civil society organizations",
        "The leader emboldened by his strongman allies is dismantling democratic norms",
        "The US government implemented new travel restrictions affecting nationals of 22 countries",  # clean — should not be touched
    ]
    print(f"\n=== Sanitization self-test ({get_vocabulary_size()} vocabulary items) ===\n")
    for t in test_cases:
        result = sanitize_text(t)
        print(f"ORIGINAL:   {t}")
        print(f"SANITIZED:  {result['sanitized_text']}")
        if result["replaced_phrases"]:
            print(f"REPLACED:   {result['replaced_phrases']}")
        print()
