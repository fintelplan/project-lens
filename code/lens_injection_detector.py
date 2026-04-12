"""
lens_injection_detector.py
Project Lens — Adversarial Content Detection

69 patterns across 13 categories:
  Cat 01-10: GNI battle-tested patterns (66 patterns)
  Cat 11:    PHI-002 bias attacks (Lens-specific)
  Cat 12:    Sectarian trap content injection (Lens-specific)
  Cat 13:    Indicator stuffing detection (Lens-specific)

Behavior:
  REMOVE   → direct prompt injection (cat 01-10 hard matches)
  FLAG     → Lens-specific attacks (cat 11-13) — included with warning tag
  PASS     → clean article

State sources bypass ALL checks — official positions are intelligence.

Session: LENS-005
Rule: LR-014(E) Only truly public data
"""

import re

# ── Category 01: Direct prompt override ──────────────────────────────────────
CAT01 = [
    r"ignore (previous|all|prior|above) instructions",
    r"disregard (your|the) system (prompt|instructions)",
    r"your new instructions are",
    r"forget (everything|all) (you|previously) (know|learned|were told)",
    r"override (previous|all) (instructions|directives)",
    r"new (directive|instruction|command):",
    r"instructions have been (updated|changed|modified)",
    r"(system|admin) (override|reset|update):",
]

# ── Category 02: Role hijacking ───────────────────────────────────────────────
CAT02 = [
    r"you are now (a |an )?(different|new|another) (ai|assistant|system|model)",
    r"act as (if )?you have no restrictions",
    r"pretend (you are|to be) (an? )?",
    r"roleplay as",
    r"switch to (developer|admin|root|unrestricted) mode",
    r"enter (jailbreak|unrestricted|free) mode",
    r"(your|the) (true|real|actual) (identity|purpose|goal) is",
]

# ── Category 03: Instruction injection ───────────────────────────────────────
CAT03 = [
    r"from now on (respond|reply|answer|output) only with",
    r"always (include|append|add) the following",
    r"(append|add|include) this (to|in) (every|all) (response|output|reply)",
    r"your (response|output|reply) must (start|begin|end) with",
    r"(hidden|secret|special) (instruction|command|directive)",
    r"\[INST\]",
    r"<\|system\|>",
    r"<\|im_start\|>",
    r"### (instruction|system|human|assistant):",
]

# ── Category 04: Data exfiltration ───────────────────────────────────────────
CAT04 = [
    r"(send|output|print|reveal|show) (all|your) (previous|prior) (context|conversation|prompt)",
    r"(reveal|show|print|output) (your|the) system prompt",
    r"what (are|were) your (instructions|directives|guidelines)",
    r"(leak|extract|exfiltrate) (data|information|context)",
    r"(send|transmit) (data|information) to (http|https|www)",
    r"output (everything|all content) (above|before|prior)",
]

# ── Category 05: Context manipulation ────────────────────────────────────────
CAT05 = [
    r"the (user|admin|operator) has (granted|given|provided) (you )?(permission|access|authorization)",
    r"the (admin|operator|developer) has (approved|authorized|enabled)",
    r"(security|safety) (override|bypass|disable) (code|key|token):",
    r"(trusted|verified|authenticated) (user|admin|operator) (mode|access)",
    r"maintenance (mode|access|override) (enabled|activated|on)",
    r"debug (mode|access|override) (enabled|activated|on)",
    r"you have been (upgraded|updated|patched) to",
    r"(new|updated) (policy|terms|guidelines) (effective|starting) (now|immediately|today)",
]

# ── Category 06: Jailbreak attempts ──────────────────────────────────────────
CAT06 = [
    r"DAN (mode|prompt|jailbreak)",
    r"do anything now",
    r"developer (mode|override|access) (on|enabled|activated)",
    r"(unrestricted|unlimited|uncensored|unfiltered) (mode|access|output)",
    r"(bypass|disable|remove|ignore) (safety|content|ethical) (filter|guardrail|restriction|limit)",
    r"(evil|dark|shadow|alternate) (mode|personality|version)",
    r"(no|without) (restrictions|limitations|guidelines|filters)",
]

# ── Category 07: Code injection ───────────────────────────────────────────────
CAT07 = [
    r"(exec|eval|system|shell|cmd)\s*\(",
    r"(DROP|DELETE|INSERT|UPDATE|SELECT)\s+(TABLE|FROM|INTO|DATABASE)",
    r"(__import__|subprocess|os\.system|os\.popen)",
    r"\$\{.{1,50}\}",  # template injection
    r"`;.{1,30};`",        # command injection
]

# ── Category 08: XSS vectors ─────────────────────────────────────────────────
CAT08 = [
    r"<script[^>]*>",
    r"javascript:\s*",
    r"on(load|click|error|mouseover)\s*=",
    r"<iframe[^>]*src",
]

# ── Category 09: Encoding attacks ────────────────────────────────────────────
CAT09 = [
    r"base64[_\s]*(decode|encode|\()",
    r"\\u00[0-9a-f]{2}\\u00[0-9a-f]{2}",  # unicode escape sequences
    r"&#x[0-9a-f]{2,4};",                       # HTML entity encoding
    r"\\x[0-9a-f]{2}\\x[0-9a-f]{2}",       # hex escape sequences
    r"rot13|caesar cipher|hex decode",
    r"atob\s*\(",
]

# ── Category 10: Multilingual injections ─────────────────────────────────────
CAT10 = [
    r"ignore les instructions (précédentes|précédente)",  # French
    r"ignorez (toutes|les) les instructions",
    r"ignora (todas|las) las instrucciones",              # Spanish
    r"ignoriere (alle|die) (vorherigen|Anweisungen)",     # German
    r"忽略.*指令",                                          # Chinese
    r"すべての.*指示.*無視",                                 # Japanese
]

# ── Category 11: PHI-002 bias attacks (LENS-SPECIFIC) ────────────────────────
# These attack the philosophical foundation of Lens
# ACTION: FLAG + INCLUDE with warning — the attack itself is intelligence
CAT11 = [
    r"(ethnic|religious|cultural) (traditions|identity) must (supersede|override|come before) (individual|human) rights",
    r"(group|collective|national|ethnic) rights (outweigh|are more important than|supersede) individual",
    r"(sovereignty|stability|order|security) (requires|demands|necessitates) (limiting|restricting|suspending) (rights|freedoms|democracy)",
    r"democracy (is|was) (a|the) western (invention|concept|imposition|idea)",
    r"(human rights|individual rights) (are|is) (a|the) (western|colonial|imperialist) (concept|invention|tool)",
]

# ── Category 12: Sectarian trap content injection (LENS-SPECIFIC) ────────────
# Unsourced ethnic/religious violence claims designed to trigger Sectarian Trap
# ACTION: FLAG + INCLUDE — manufactured division IS intelligence about who is dividing
CAT12 = [
    r"(unnamed|anonymous|unverified|secret) sources (say|report|claim|allege) (that )?(muslims?|christians?|buddhists?|hindus?|jews?)",
    r"(sources|insiders|informants) (confirm|reveal|expose) (ethnic|religious|minority) (plot|plan|attack|conspiracy)",
    r"(the|this) (ethnic|religious|minority|indigenous) group (is|are) (planning|behind|responsible for)",
    r"(foreign|outside|external) (agents|forces|powers) (are )?(using|funding|arming) (ethnic|religious|minority)",
    r"(ethnic|religious|cultural|sectarian) (cleansing|war|conflict|tension) (is|has) (begun|started|erupted|inevitable)",
]

# ── Category 13: Indicator stuffing detection (LENS-SPECIFIC) ────────────────
# Articles artificially loaded with indicator keywords but no real content
# ACTION: FLAG + INCLUDE with LOW QUALITY tag
STUFFING_KEYWORDS = [
    "sanctions", "dark money", "shell company", "money laundering",
    "oligarch", "corruption", "energy security", "critical minerals",
    "food security", "military alliance", "coup", "sovereignty",
    "debt trap", "financial warfare", "cyber attack", "surveillance",
    "disinformation", "propaganda", "sectarian", "ethnic cleansing",
]
STUFFING_THRESHOLD = 8  # if >= 8 indicator keywords in short article = stuffing


# ── Pattern compiler ─────────────────────────────────────────────────────────

def _compile(patterns):
    return [re.compile(p, re.IGNORECASE | re.DOTALL) for p in patterns]

_REMOVE_PATTERNS = (
    _compile(CAT01) + _compile(CAT02) + _compile(CAT03) +
    _compile(CAT04) + _compile(CAT05) + _compile(CAT06) +
    _compile(CAT07) + _compile(CAT08) + _compile(CAT09) + _compile(CAT10)
)

_FLAG_PHI002   = _compile(CAT11)
_FLAG_SECTARIAN = _compile(CAT12)


# ── Public API ────────────────────────────────────────────────────────────────

def scan_article(article: dict, source_tier: str = "TIER2") -> dict:
    """
    Scan article for adversarial content.

    Returns dict:
      action:         "PASS" | "REMOVE" | "FLAG"
      injection_flag: None | "PROMPT_INJECTION" | "PHI002_BIAS_ATTACK"
                      | "SECTARIAN_TRAP_INJECTION" | "INDICATOR_STUFFING"
      reason:         explanation string
    """
    # STATE sources bypass ALL checks — official positions are intelligence
    if source_tier == "STATE":
        return {"action": "PASS", "injection_flag": None, "reason": "STATE source — bypass"}

    text = f"{article.get('title', '')} {article.get('content', '')}".lower()

    # Check REMOVE patterns (direct prompt injection)
    for pattern in _REMOVE_PATTERNS:
        if pattern.search(text):
            return {
                "action": "REMOVE",
                "injection_flag": "PROMPT_INJECTION",
                "reason": f"Direct injection detected: {pattern.pattern[:60]}"
            }

    # Check FLAG patterns (Lens-specific — include with warning)
    for pattern in _FLAG_PHI002:
        if pattern.search(text):
            return {
                "action": "FLAG",
                "injection_flag": "PHI002_BIAS_ATTACK",
                "reason": "Article attacks PHI-002 philosophical framework"
            }

    for pattern in _FLAG_SECTARIAN:
        if pattern.search(text):
            return {
                "action": "FLAG",
                "injection_flag": "SECTARIAN_TRAP_INJECTION",
                "reason": "Article contains unsourced ethnic/religious violence claim"
            }

    # Check indicator stuffing (short article with many keyword hits)
    word_count = len(text.split())
    if word_count < 150:  # short article only
        hits = sum(1 for kw in STUFFING_KEYWORDS if kw in text)
        if hits >= STUFFING_THRESHOLD:
            return {
                "action": "FLAG",
                "injection_flag": "INDICATOR_STUFFING",
                "reason": f"Short article ({word_count} words) with {hits} indicator keyword hits"
            }

    return {"action": "PASS", "injection_flag": None, "reason": "Clean"}
