"""
patch_catalog_v4.py
Builds lens-OPS-001_catalog_v4_0.json from v3.1 + 6 new operations.
Run: python patch_catalog_v4.py
"""
import json, shutil
from pathlib import Path

SRC = "data/lens-OPS-001_catalog_v3_1.json"
DST = "data/lens-OPS-001_catalog_v4_0.json"

with open(SRC, encoding="utf-8") as f:
    catalog = json.load(f)

# Update version
catalog["catalog_version"] = "v4.0"

# Add genre_context field to all existing ops
for op in catalog["operations"]:
    op["genre_context"] = "universal"
    op["max_article_chars"] = None

# New ops OP-030 to OP-035
new_ops = [
    {
        "id": "OP-030",
        "name": "Absent counterparty in stenographic report",
        "detection_stage": "early_warning",
        "primary_lens": 1,
        "secondary_lens": None,
        "genre_context": "stenographic",
        "max_article_chars": 3000,
        "description": "In a short wire report about a bilateral interaction, only one side's actions and statements are represented. The other party is referenced by name only with no specific actions quoted.",
        "evidence_pattern": "Article <3000 chars reports an interaction between two actors. Actor A has specific quotes, actions, or positions. Actor B is mentioned by name only with no specific quote, action, or position stated.",
        "watch_protocol": "Fire when article <3000 chars and bilateral topic but only one side has quoted statements or specific named actions."
    },
    {
        "id": "OP-031",
        "name": "Unsourced state claim in brief format",
        "detection_stage": "early_warning",
        "primary_lens": 4,
        "secondary_lens": None,
        "genre_context": "stenographic",
        "max_article_chars": 3000,
        "description": "A short wire report makes a strategic-level claim about a state actor's intent in the article's own narrative voice, with no attribution to any analyst, official, or document.",
        "evidence_pattern": "Article <3000 chars contains a sentence NOT in a quote block asserting what a state actor 'seeks to', 'aims to', 'is attempting to', 'wants to' or 'is positioning for' — without attribution phrase.",
        "watch_protocol": "Fire on single unattributed intent-claim sentence in brief format."
    },
    {
        "id": "OP-032",
        "name": "Spokesperson quote as sole evidence in brief",
        "detection_stage": "early_warning",
        "primary_lens": 4,
        "secondary_lens": None,
        "genre_context": "stenographic",
        "max_article_chars": 3000,
        "description": "A short wire report's only substantive content is a single official spokesperson quote, with no independent verification, no analyst comment, and no observable evidence cited.",
        "evidence_pattern": "Article <3000 chars contains only one or two quotes, both from official spokesperson(s) of the same side. No independent source, no observable fact cited to verify the claim.",
        "watch_protocol": "Fire when article is >60% official quote content with no independent verification."
    },
    {
        "id": "OP-033",
        "name": "Label-only framing in headline-brief",
        "detection_stage": "early_warning",
        "primary_lens": 4,
        "secondary_lens": None,
        "genre_context": "stenographic",
        "max_article_chars": 2000,
        "description": "A very short wire report uses a loaded evaluative label as its primary framing device, with no cause-chain supporting the label in the body.",
        "evidence_pattern": "Article <2000 chars. Title or first sentence contains evaluative label (threat, aggression, provocation, escalation, stability). Body contains no explanation of what actions constitute the labeled behavior.",
        "watch_protocol": "Fire when label appears in headline and body contains <50 words of supporting evidence."
    },
    {
        "id": "OP-034",
        "name": "Single-source verb asymmetry in brief",
        "detection_stage": "early_warning",
        "primary_lens": 1,
        "secondary_lens": None,
        "genre_context": "stenographic",
        "max_article_chars": 3000,
        "description": "A short report with only 2-3 quoted sources assigns reactive or defensive introduction verbs to one side and neutral or active verbs to the other.",
        "evidence_pattern": "Article <3000 chars. One source introduced with active verb (said, stated, announced, declared). Different source from opposing side introduced with reactive verb (responded, pushed back, denied, defended, rejected).",
        "watch_protocol": "Fire on minimum-context articles where verb asymmetry IS the framing."
    },
    {
        "id": "OP-035",
        "name": "Action-to-outcome causation extension",
        "detection_stage": "early_warning",
        "primary_lens": 1,
        "secondary_lens": None,
        "genre_context": "universal",
        "max_article_chars": None,
        "description": "An article correctly attributes a decision to a named actor, then extends that attribution to claim the actor caused a structural outcome — conflating decision-making with structural causation.",
        "evidence_pattern": "Sentence 1 correctly attributes: '[Actor] decided/launched/signed/ordered X.' Sentence 2 extends: '[Actor]'s X caused/produced/resulted in [large structural outcome].' Causal chain not explained.",
        "watch_protocol": "Fire when action-attribution is immediately followed by structural-outcome attribution without explaining intervening causal chain."
    }
]

catalog["operations"].extend(new_ops)

# Save
with open(DST, "w", encoding="utf-8") as f:
    json.dump(catalog, f, indent=2, ensure_ascii=False)

print(f"Catalog v4.0 written: {DST}")
print(f"Total operations: {len(catalog['operations'])} (was 29, now 35)")
print(f"New ops: {[op['id'] for op in new_ops]}")
