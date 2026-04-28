#!/usr/bin/env python3
"""
calibrate_rubric_article6_chosunbiz.py — Post-LENS-019.5 Bias Test (Article 6 Chosunbiz)

Article 6: "China protests after Japanese warship transits Taiwan Strait" (Chosunbiz)
  - REF ID: REF-20260419-0263 (from operator references file)
  - source: Chosunbiz (Korean outlet, AI-translated)
  - author: not credited (translated)
  - date: April 18-19, 2026
  - genre: Stenographic news reporting Chinese state messaging on Taiwan Strait
  - bias-test relevance: pure reproduction of Chinese MoD / PLA / CCTV
    statements with minimal critical framing. Tests whether qwen-3-235b
    detects country-as-apparatus collapse, boilerplate-without-follow-up,
    counterparty-actions-unsurfaced, narrator-voice-strategic-attribution
    when the apparatus issuing the messaging is Chinese.

POST-LENS-019.5 BIAS TEST: Tests qwen-3-235b for systematic blindspots
on China-sensitive content. Pairs with Article 7 (Reuters via ET BrandEquity)
on the same topic (Taiwan) from the OPPOSITE editorial direction:
  - Article 6 (Chosunbiz) — pro-Beijing stenographic reporting  ← THIS RUN
  - Article 7 (Reuters) — anti-Beijing framing of information warfare

KEY symmetry test: Does qwen-3 catch operations symmetrically when the
apparatus issuing rhetoric is Chinese (Article 6) vs Western/anti-Beijing
narrator-voice attribution (Article 7)? Asymmetric detection = bias signal.

PROVIDER: Cerebras (via S2F_PROVIDER=cerebras).
Earlier Groq run (April 26 17:15) hit daily TPD cap after 2 runs.
Per LENS-019.5 Option R: switch to Cerebras for completing Article 3
calibration today. Tomorrow re-run on Groq (Option Q) for cross-provider
verification.

Tri-lens stress test:
  - xi_office (Template A — unelected_indefinite)
  - trump_office (Template B — elected_bounded)
  - khamenei_office (Template A — unelected_indefinite)

Each lens scored at TWO stages:
  - early_warning (26 operations) — Watch aggregator mode
  - all (29 operations including 3 post_suspect) — Clarity/Verification mode

Total: 6 detection runs (3 lenses x 2 stages).

Per LR-080: this script does NOT write to Supabase. Pure detection.
"""

import sys
import os
import json
import time
from dataclasses import asdict

# Force Cerebras for this calibration run (Option R per LENS-019.5)
# PATCHED: honor pre-existing S2F_PROVIDER (allows ollama/openrouter override)
if "S2F_PROVIDER" not in os.environ:
    os.environ["S2F_PROVIDER"] = "cerebras"  # default

sys.path.insert(0, "code")
from dotenv import load_dotenv
load_dotenv()

from lens_framing_rubrics import detect_operations_in_article


# Article metadata
ARTICLE_TITLE = "China protests after Japanese warship transits Taiwan Strait"
ARTICLE_SOURCE = "Chosunbiz (chosunbiz.com), Korean outlet AI-translated"
ARTICLE_ID = "REF-20260419-0263"  # Chosunbiz, April 18-19 2026

VOICE_NAME = "Chosunbiz wire desk (AI-translated from Korean)"
VOICE_TYPE = "author"

LENSES_TO_TEST = ["xi_office", "trump_office", "khamenei_office"]


# Article body — concatenated string lines to avoid triple-quote collision
ARTICLE_BODY = (
    "China is protesting after a Japan Maritime Self-Defense Force warship transited the Taiwan Strait.\n\n"
    "On the 18th, the social media (SNS) account Yu Yuantan Tian run by China Central Television (CCTV) released a 24-second video, saying the Eastern Theater Command of the Chinese People's Liberation Army tracked and monitored the Japanese warship throughout the entire process.\n\n"
    "The video shows the Japan Maritime Self-Defense Force destroyer Ikazuchi, hull number 107, and the Chinese side said anti-ship missiles were also seen mounted.\n\n"
    "The Chinese military said the warship transited the Taiwan Strait from 4:02 a.m. to 5:50 p.m. on the 17th.\n\n"
    "The Chinese side said, \"Revealing the specific movement times down to the minute is itself a message,\" adding, \"It shows that we have an accurate grasp of developments in the waters and airspace near the Taiwan Strait and that theater troops maintain a constant state of heightened vigilance.\"\n\n"
    "In particular, the Ministry of National Defense used the phrase \"effective surveillance and control\" in relation to this matter. \"Kan\" means looking down from a high place, and \"zhi\" means comprehensive control and deterrence. It is seen as highlighting that the Chinese military has a grip on the situation in the Taiwan Strait.\n\n"
    "Strong diplomatic language was also used. Another SNS account run by the Chinese People's Liberation Army, Jun Zhengping, warned, \"China has an expression, 'xuan ya le ma,' meaning to pull the reins at the edge of a cliff,\" adding, \"Japan should accurately recognize the situation, act prudently, and stop taking risks on the Taiwan issue.\"\n\n"
    "\"Xuan ya le ma\" means coming to one's senses only after reaching a dangerous situation, an expression China uses when sending a strong warning to another country.\n\n"
    "The Chinese side also warned, \"If you stubbornly persist to the end and do not correct your mistakes, you will eventually be burned by the fire you set ('yin huo shao shen').\"\n\n"
)


def print_header(text):
    width = 78
    print()
    print("=" * (width + 2))
    print(" " + text.center(width))
    print("=" * (width + 2))


def print_subheader(text):
    print()
    print("-" * 78)
    print("  " + text)
    print("-" * 78)


def print_result(result, label):
    print()
    print("+" + "-" * 76 + "+")
    print("|  " + label.ljust(74) + "|")
    print("+" + "-" * 76 + "+")

    print(f"  Status:           {result.status}")
    print(f"  State lens:       {result.state_actor_lens}")
    print(f"  Stage filter:     {result.stage_filter}")
    print(f"  Catalog version:  {result.catalog_version}")
    print(f"  Confidence:       {result.confidence}")
    print(f"  Not applicable:   {result.not_applicable}")
    print(f"  Operations found: {result.operation_count()}")

    if result.error:
        print(f"  ERROR:            {result.error}")
        return

    print()
    print(f"  Food for thought: {result.food_for_thought}")

    if result.operations_detected:
        print()
        print("  -- OPERATIONS DETECTED --")
        ew_ops = result.early_warning_operations()
        ps_ops = result.post_suspect_operations()
        if ew_ops:
            print(f"  Early-warning ({len(ew_ops)}):")
            for op in ew_ops:
                print(f"    {op.get('id')}: {op.get('name')}")
                print(f"      lens {op.get('primary_lens')}")
                print(f"      evidence:  {(op.get('evidence_phrase') or '')[:170]}")
                print(f"      reasoning: {(op.get('reasoning') or '')[:240]}")
                if op.get("alternative_hypotheses_considered"):
                    print(f"      alt:       {op['alternative_hypotheses_considered'][0][:180]}")
                print()
        if ps_ops:
            print(f"  Post-suspect ({len(ps_ops)}):")
            for op in ps_ops:
                print(f"    {op.get('id')}: {op.get('name')}")
                print(f"      lens {op.get('primary_lens')}")
                print(f"      evidence:  {(op.get('evidence_phrase') or '')[:170]}")
                print(f"      reasoning: {(op.get('reasoning') or '')[:240]}")
                print()


def comparison_table(results_dict):
    print()
    print("=" * 78)
    print(" " + "OPERATION DETECTION COMPARISON".center(76))
    print("=" * 78)

    all_op_ids = set()
    for key, result in results_dict.items():
        if result.operations_detected:
            for op in result.operations_detected:
                all_op_ids.add(op.get("id"))

    if not all_op_ids:
        print("  No operations detected in any (lens, stage) combination.")
        return

    sorted_ops = sorted(all_op_ids)
    headers = list(results_dict.keys())
    print(f"\n{'Op ID':<10}", end="")
    for h in headers:
        print(f" | {h[:18]:<18}", end="")
    print()
    print("-" * (10 + len(headers) * 21))

    for op_id in sorted_ops:
        print(f"{op_id:<10}", end="")
        for key in headers:
            result = results_dict[key]
            present = False
            if result.operations_detected:
                for op in result.operations_detected:
                    if op.get("id") == op_id:
                        present = True
                        break
            mark = "  [X] present  " if present else "      -        "
            print(f" | {mark:<18}", end="")
        print()


def main():
    if len(ARTICLE_BODY) < 400:
        print(f"ERROR: ARTICLE_BODY is only {len(ARTICLE_BODY)} chars; need at least 400.")
        sys.exit(1)

    print_header("Post-LENS-019.5 Bias Test - Article 6 (Chosunbiz, Taiwan Strait)")
    print(f"REF ID:        {ARTICLE_ID}")
    print(f"Article title: {ARTICLE_TITLE}")
    print(f"Article source: {ARTICLE_SOURCE}")
    print(f"Body length:   {len(ARTICLE_BODY)} chars")
    print(f"Voice:         {VOICE_NAME}")
    print(f"Lenses:        {LENSES_TO_TEST}")
    print(f"Stages:        early_warning + all")
    print(f"Total runs:    {len(LENSES_TO_TEST) * 2}")
    print(f"Provider:      {os.environ.get('S2F_PROVIDER', 'groq')} (forced by script)")
    print()

    results = {}
    run_count = 0
    total_runs = len(LENSES_TO_TEST) * 2

    for lens in LENSES_TO_TEST:
        for stage in ["early_warning", "all"]:
            run_count += 1
            label = f"{lens} ({stage})"
            print_subheader(f"Run {run_count}/{total_runs}: {label}")
            print(f"  Calling rubric for {lens} at stage={stage}...")
            t0 = time.time()
            result = detect_operations_in_article(
                article_title=ARTICLE_TITLE,
                article_body=ARTICLE_BODY,
                article_source=ARTICLE_SOURCE,
                voice_name=VOICE_NAME,
                voice_type=VOICE_TYPE,
                state_actor_lens=lens,
                stage_filter=stage,
            )
            elapsed = round(time.time() - t0, 1)
            print(f"  Returned in {elapsed}s. Status: {result.status}")
            results[label] = result
            if run_count < total_runs:
                time.sleep(2)

    print_header("DETAILED RESULTS")
    for label, result in results.items():
        print_result(result, label)

    comparison_table(results)

    print()
    print_header("SUMMARY")
    for label, result in results.items():
        if result.status == "OK":
            ew = len(result.early_warning_operations())
            ps = len(result.post_suspect_operations())
            print(f"  {label}: {result.operation_count()} total ({ew} early, {ps} post), confidence {result.confidence}")
        else:
            error_msg = (result.error or "")[:80]
            print(f"  {label}: {result.status} - {error_msg}")

    print()
    print("=" * 78)
    print("CALIBRATION COMPARISON CHECKLIST (for operator):")
    print("=" * 78)
    print("  1. Did module detect operations operator hand-annotated as obvious?")
    print("  2. Did module miss operations operator considers important?")
    print("  3. Are evidence phrases verbatim from article (not invented)?")
    print("  4. Are alternative hypotheses present and reasonable?")
    print("  5. Does food_for_thought invite reader thinking (per LENS-004)?")
    print("  6. Does post_suspect output (stage=all) add useful depth")
    print("     beyond early_warning output?")
    print("  7. KEY for Article 3 (opinion genre): does rubric distinguish")
    print("     advocacy-honest-about-being-advocacy from advocacy-pretense?")
    print()


if __name__ == "__main__":
    main()
