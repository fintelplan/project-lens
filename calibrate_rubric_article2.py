#!/usr/bin/env python3
"""
calibrate_rubric_article2.py — LENS-019.5 Calibration Round 2

Article 2: "US imposes sanctions on a China-based oil refinery and 40 shippers over Iranian oil"
  - REF ID: REF-20260425-0050 (from operator references file)
  - source: AP News (apnews.com)
  - author: Fatima Hussein (Treasury Department reporter)
  - date: April 25, 2026

Tri-lens stress test:
  - xi_office (Template A — unelected_indefinite)
  - trump_office (Template B — elected_bounded)
  - khamenei_office (Template A — unelected_indefinite)

Each lens scored at TWO stages:
  - early_warning (26 operations) — Watch aggregator mode
  - all (29 operations including 3 post_suspect) — Clarity/Verification mode

Total: 6 detection runs (3 lenses x 2 stages).

Per LR-080: this script does NOT write to Supabase. Pure detection, pure output.
Per LENS-019.5: calibration round 2.

USAGE:
  python calibrate_rubric_article2.py
"""

import sys
import os
import json
import time
from dataclasses import asdict

sys.path.insert(0, "code")
from dotenv import load_dotenv
load_dotenv()

from lens_framing_rubrics import detect_operations_in_article


# Article metadata
ARTICLE_TITLE = "US imposes sanctions on a China-based oil refinery and 40 shippers over Iranian oil"
ARTICLE_SOURCE = "AP News (apnews.com)"
ARTICLE_ID = "REF-20260425-0050"

VOICE_NAME = "Fatima Hussein (AP, Treasury Department reporter)"
VOICE_TYPE = "author"

LENSES_TO_TEST = ["xi_office", "trump_office", "khamenei_office"]


# Article body — using single-quote concatenation to avoid triple-quote collision  
ARTICLE_BODY = (
    "WASHINGTON (AP) - President Donald Trump's administration is placing economic sanctions on a major China-based oil refinery and roughly 40 shipping companies and tankers involved in transporting Iranian oil.\n\n"
    "The move, announced Friday and first reported by The Associated Press, makes good on Trump's threat to impose secondary sanctions on companies and countries that do business with Iran. It's also part of his Republican administration's overall ramped-up campaign to cut off Iran's key source of revenue - its oil exports.\n\n"
    "Concurrently, the U.S. this month imposed a physical blockade on the Strait of Hormuz, the Persian Gulf waterway that is crucial to global energy supplies.\n\n"
    "The sanctions, which cut off the companies from the U.S. financial system and penalize anyone who does business with them, come just a few weeks before President Donald Trump and China's Xi Jinping are due to meet in China.\n\n"
    "Included in Friday's sanctions is Hengli Petrochemical's facility in the port city of Dalian, which has a processing capacity of roughly 400,000 barrels of crude oil per day, making it one of the biggest independent refineries in China.\n\n"
    "The Treasury Department says Hengli has received Iranian crude oil shipments since 2023 and has generated hundreds of millions of dollars in revenue for the Iranian military.\n\n"
    "The advocacy group United Against Nuclear Iran said in February 2025 that Hengli is one of dozens of Chinese purchasers of Iranian oil.\n\n"
    "China is the biggest buyer of Iranian oil, importing 80% to 90% of Iranian oil before the U.S.-Israeli war with Iran broke out, though the crude - transported by a shadow fleet of vessels - often has its origin obscured but arrives in China as oil from countries such as Malaysia. Smaller refineries, known as teapot refineries, typically are the buyers of Iranian oil.\n\n"
    "Iran has previously said that its demands for ending the war include the lifting of sanctions.\n\n"
    "Treasury Secretary Scott Bessent said Friday that his agency 'will continue to constrict the network of vessels, intermediaries and buyers Iran relies on to move its oil to global markets.'\n\n"
    "Earlier this month, Bessent's department sent a letter to financial institutions in China, Hong Kong, the UAE and Oman threatening to levy secondary sanctions for doing business with Iran and accusing those countries of allowing Iranian illicit activities to flow through their financial institutions.\n\n"
    "Bessent said during a White House press briefing on April 15 that the administration has told countries 'that if you are buying Iranian oil, that if Iranian money is sitting in your banks, we are now willing to apply secondary sanctions, which is a very stern measure.'\n\n"
    "The sanctions come as the global energy trade is in turmoil as war around the Persian Gulf chokes off oil and natural gas shipments, causing prices to soar.\n\n"
    "Treasury has tried to quell the impact of rising oil prices issuing temporary sanctions waivers on Russia oil and a one-time waiver on Iranian oil already at sea.\n\n"
    "The AP was making efforts to contact Chinese officials for comment on the sanctions.\n\n"
    "China has disagreed with previous U.S. sanctions, but its major companies and banks still comply with U.S. sanctions because they are more exposed to the U.S.-dominated financial system.\n\n"
    "After the U.S. earlier this month sanctioned a Chinese refinery accused of buying Iranian oil, Liu Pengyu, a spokesperson for China's embassy in Washington, said the use of the sanctions 'undermines international trade order and rules, disrupts normal economic and trade exchanges, and infringes upon the legitimate rights and interests of Chinese companies and individuals.'"
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

    print_header("LENS-019.5 Calibration Round 2 - Article 2")
    print(f"REF ID:        {ARTICLE_ID}")
    print(f"Article title: {ARTICLE_TITLE}")
    print(f"Article source: {ARTICLE_SOURCE}")
    print(f"Body length:   {len(ARTICLE_BODY)} chars")
    print(f"Voice:         {VOICE_NAME}")
    print(f"Lenses:        {LENSES_TO_TEST}")
    print(f"Stages:        early_warning + all")
    print(f"Total runs:    {len(LENSES_TO_TEST) * 2}")
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
    print()


if __name__ == "__main__":
    main()
