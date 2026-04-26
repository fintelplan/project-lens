#!/usr/bin/env python3
"""
calibrate_rubric_article4_scmp.py — LENS-019.5 Calibration Round 5 (Article 4 SCMP)

Article 4: "China steps up aid to Africa but huge funding gap left by Trump\'s cuts remains"
  - REF ID: REF-20260419-0299 (from operator references file)
  - source: South China Morning Post (scmp.com)
  - author: Jevans Nyabiage (SCMP first Africa correspondent, Nairobi)
  - date: April 19, 2026
  - genre: Structural-economic news with soft Xi-favorable framing

Article 4 in 5-article calibration set. Tests catalog v3.1 against
SCMP soft-Xi-favorable structural-economic genre:
  - Article 1 (Reuters) — wire-service with subtle pretense
  - Article 2 (AP) — wire-service relatively clean
  - Article 3 (Asia Times) — opinion column anti-Trump
  - Article 4 (SCMP) — structural-economic news soft-Xi-favorable  ← THIS RUN
  - Article 5 (Middle East Eye) — opinion column anti-US/Israel

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
os.environ["S2F_PROVIDER"] = "cerebras"

sys.path.insert(0, "code")
from dotenv import load_dotenv
load_dotenv()

from lens_framing_rubrics import detect_operations_in_article


# Article metadata
ARTICLE_TITLE = "China steps up aid to Africa but huge funding gap left by Trump\'s cuts remains"
ARTICLE_SOURCE = "South China Morning Post (scmp.com)"
ARTICLE_ID = "REF-20260419-0299"  # SCMP, Jevans Nyabiage, April 19 2026

VOICE_NAME = "Jevans Nyabiage (SCMP Africa correspondent)"
VOICE_TYPE = "author"

LENSES_TO_TEST = ["xi_office", "trump_office", "khamenei_office"]


# Article body — concatenated string lines to avoid triple-quote collision
ARTICLE_BODY = (
    "During Vice-President Han Zheng's visit to Nairobi in March, China signed a cash grant for drought relief and recently delivered food aid to Somalia, Togo, Zimbabwe and Zambia – helping 217,057 people in Zambia and providing Zimbabwe with 5,000 tonnes of rice.\n\n"
    "Du Xiaohui, director general of the foreign ministry's African affairs department, later affirmed that Beijing would help nations in Africa boost food security and agricultural resilience through emergency aid and long-term support for self-reliant development.\n\n"
    "Beijing has also stepped up its health diplomacy across the continent, recently giving South Africa a US$3.49 million grant, facilitated by UNAids, to fund HIV prevention for 54,000 students and drug users.\n\n"
    "The grant to South Africa, home to an estimated 8 million people living with HIV, marked China's first major entry into a sector that had long been dominated by the US President's Emergency Plan for Aids Relief (PEPFAR) initiative.\n\n"
    "Before Washington sharply cut foreign help in early 2025, South Africa relied on the US for nearly 17 per cent of its HIV budget, or more than US$400 million annually, according to the South African National Aids Council.\n\n"
    "When the Donald Trump administration dismantled the United States Agency for International Development (USAID), cancelling most grants and leading to a sharp drop in health aid to Africa, some nations lost more than half their funding.\n\n"
    "According to observers, Beijing's actions could partly fill the void left by Washington, whose influence on the continent has been in sharp decline following the decision to cut foreign aid.\n\n"
    "They noted that Beijing prioritised economic development over the West's donor-heavy aid model, while its recent food and health relief efforts presented China as a reliable alternative to the US without long-term financial commitment.\n\n"
    "However, while Beijing was increasingly providing food and medical relief – an area that Washington dominated for decades – its focus on infrastructure and trade meant it remained a supplement to the traditional US aid model, rather than a full replacement.\n\n"
    "David Shinn, a professor at George Washington University's Elliott School of International Affairs, drew a sharp distinction between the impact of China's medical teams and the recent increase in Beijing's food donations and support for HIV/Aids programmes.\n\n"
    "Since the first Chinese medical team arrived in Algeria in 1963, China's flagship programme has sent more than 30,000 healthcare workers to over 75 countries, with a main focus on Africa.\n\n"
    "Shinn said the medical teams were one of China's oldest and most effective programmes but he added that more recent donations had likely been intended to 'underscore the American reductions in these programmes'.\n\n"
    "China's contributions were very modest in both dollar value and impact, he noted. 'China does an excellent job at getting maximum positive publicity for these small programmes but they do not come close to filling the financial gap left by the Trump administration's reductions.'\n\n"
    "Cameron Hudson, a senior fellow at the Centre for Strategic and International Studies, echoed this view. China's help was 'frankly not filling the gap that Washington has left in the overall aid portfolio', he said, adding that it was 'merely performative on China's part'.\n\n"
    "Comparing the current relief to the 'splashy' but short-lived vaccine contributions made during the Covid-19 pandemic, Hudson said that 'China does not shift strategies overnight'.\n\n"
    "As part of its 'health Silk Road' medical diplomacy, China also financed and constructed the US$80 million headquarters of the Africa Centres for Disease Control and Prevention in Addis Ababa.\n\n"
    "According to Yun Sun, director of the China programme at the Stimson Centre think tank in Washington, China's food and medical help to Africa had a long history that had been rather consistent since it started in the 1950s.\n\n"
    "'And as a great power, it is part of China's natural responsibility to aid less-developed countries, with or without the US doing the same,' she said.\n\n"
    "Zhou Yuyuan, deputy director of the Centre for West Asian and African Studies at the Shanghai Institutes for International Studies, rejected the suggestion that China's food aid was related to the US action. Such help had been going on for years, he said.\n\n"
    "The China International Development Cooperation Agency, established in 2018, and the Global Development and South-South Cooperation Fund set up in 2015, have carried out food and medical projects in more than 30 African countries through the UN World Food Programme and UNAids, in what Beijing has called a keystone of deepening China-Africa cooperation.\n\n"
    "Zhou said the interruption of US aid had prompted African countries to 'urgently seek new financial support', which had accelerated the implementation of existing Chinese projects.\n\n"
    "He also dismissed the view that this acceleration indicated a change in foreign policy, pointing out that aid remained small compared with trade and investment.\n\n"
    "'The recent increase in Chinese aid is more of an emergency response, both to the food shortages caused by droughts in Southern African countries in recent years and to the public health challenges resulting from the US withdrawal,' Zhou said.\n\n"
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

    print_header("LENS-019.5 Calibration Round 5 - Article 4 (SCMP)")
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
