#!/usr/bin/env python3
"""
calibrate_rubric_article1.py — LENS-019.5 Calibration Round 4 (Article 1 re-test)

Article 1: "With tariffs stalled, Trump\'s China policy drifts"
  - source: Reuters (April 21, 2026, original LENS-019 calibration article)
  - byline: Michael Martina, Trevor Hunnicutt, David Brunnstrom (Washington + Beijing)
  - editing: Don Durfee, Alistair Bell
  - genre: Wire-service news analysis with subtle pretense

Re-running Article 1 against the v3 operations catalog + Cerebras+qwen-3-235b
that worked well on Article 3. Article 1 was originally calibrated against the
OLD 5-axis rubric (now obsolete). This run gives us same-infrastructure
comparison across all three articles:
  - Article 1 (Reuters) — wire-service with subtle pretense  ← THIS RUN
  - Article 2 (AP) — wire-service, relatively clean
  - Article 3 (Asia Times) — opinion column with explicit thesis

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
ARTICLE_TITLE = "With tariffs stalled, Trump\'s China policy drifts"
ARTICLE_SOURCE = "Reuters (reuters.com)"
ARTICLE_ID = "REF-20260421-A1"  # Reuters, April 21 2026, original LENS-019 calibration

VOICE_NAME = "Reuters Washington team (Martina/Hunnicutt/Brunnstrom)"
VOICE_TYPE = "author"

LENSES_TO_TEST = ["xi_office", "trump_office", "khamenei_office"]


# Article body — concatenated string lines to avoid triple-quote collision
ARTICLE_BODY = (
    "WASHINGTON, April 21 (Reuters) - When President Donald Trump returned to office in 2025, he vowed to use tariffs to reset relations with China, which he said was \"killing\" the United States with its trade policies.\n\n"
    "Now, more than a year into his second term, Trump's aggressive trade moves have not fundamentally altered Beijing's trade or military actions. Instead, Washington's China policy appears adrift, causing confusion among officials and driving contradictory decisions.\n\n"
    "The administration's erratic moves toward Beijing have been on full display in recent months. Those include adding top Chinese companies to a military blacklist only to withdraw the list moments later, and a decision by Trump to greenlight AI semiconductor sales to China within minutes of his government labeling Chinese access to them a national security threat.\n\n"
    "As Trump prepares for his planned May 14-15 visit to China to meet President Xi Jinping, the first such trip by an American president in eight years, critics argue such inconsistencies, coupled with his improvisational dealmaking style, have undermined the U.S. in its competition with Beijing.\n\n"
    "\"You have departments and agencies acting on their own accord, often with different objectives, and even at times in countervailing ways,\" said Ely Ratner, a former Assistant Secretary of Defense for Indo-Pacific Security Affairs.\n\n"
    "\"On any given day, it feels like the policy can zigzag in either direction,\" Ratner said.\n\n"
    "Responding to Reuters questions on the administration's approach to China, White House spokesperson Kush Desai said Trump's trade agenda had \"flipped the script\" on decades of failed policy that hollowed out the U.S. industrial base.\n\n"
    "\"By leveraging our economy - the biggest and best consumer market in the world - and his great relationship with President Xi, President Trump has empowered America to finally operate from a position of strength in global diplomatic and trade matters,\" Desai said.\n\n"
    "NO PLAN B\n\n"
    "Trump launched his second term China policy with a dramatic trade broadside, initially hiking tariffs on Chinese goods to around 145%.\n\n"
    "Beijing did not back down, however, and retaliated with tariff increases of its own.\n\n"
    "The countries eventually forged an uneasy detente after China, which holds a virtual monopoly on the refining and processing of the world's rare earths, threatened to choke off supplies of the minerals needed by U.S. industries.\n\n"
    "A February ruling by the Supreme Court invalidating many of Trump's duties further undercut the administration's strategy.\n\n"
    "\"Their entire original strategy was centered around using tariffs to pressure China into major concessions. That effort quickly ran aground,\" said Scott Kennedy, a China expert at the Center for Strategic and International Studies think tank. \"There has been no coherent Plan B.\"\n\n"
    "The tariffs did produce at least one result Trump has sought: the U.S. goods trade deficit with China decreased by 32% to $202 billion in 2025 compared to 2024, U.S. government data show.\n\n"
    "But tariffs have not changed Beijing's mercantilist trade policies, and their fitful use likely reduced industry incentive to reshore manufacturing, a major goal of Trump's America First approach. The U.S. lost 91,000 manufacturing jobs from February to December of last year.\n\n"
    "Treasury Secretary Scott Bessent and U.S. Trade Representative Jamieson Greer, who have run China policy instead of hawkish Secretary of State Marco Rubio, appear to have lowered expectations for an overhaul in commercial relations, shifting emphasis to a new \"managed trade.\"\n\n"
    "\"Where do we want to be with China? We want relations to be stable. We want our trade to be more balanced. We want it to be in non-sensitive goods,\" Greer said in March.\n\n"
    "In the face of Trump's turmoil, China has sought to portray itself as the responsible power.\n\n"
    "\"We ... stay committed to acting as a positive and stable force for good,\" its foreign ministry said in January when asked if Beijing benefited from the chaotic U.S. approach.\n\n"
    "CONFLICTING SIGNALS\n\n"
    "The administration's reversals haven't just been on tariffs.\n\n"
    "In December, Trump declared on social media that he had approved the controversial sale of advanced Nvidia H200 AI semiconductors to China, the very chips his Justice Department only 30 minutes earlier said were being smuggled to China, constituting a threat to national security.\n\n"
    "Two U.S. officials told Reuters those conflicting signals left them and others in the government flummoxed.\n\n"
    "In February, Trump's Pentagon blacklisted top Chinese technology companies for allegedly aiding the Chinese military, only to mysteriously withdraw the list an hour later with little explanation.\n\n"
    "In the fall, the Commerce Department issued rules to extend export controls to thousands of subsidiaries of Chinese companies, arguing it closed a significant loophole by which foreign companies could access sensitive technology. But the U.S. paused those measures, along with planned U.S. port fees for Chinese-built vessels intended to boost American shipbuilding, in the face of China's threat to restrict rare earths.\n\n"
    "\"These contradictions ultimately trace back to President Trump, who makes decisions in the moment, unconstrained by a broader strategy,\" said Zack Cooper, who studies U.S. strategy in Asia at the American Enterprise Institute think tank.\n\n"
    "'TAKING PAWNS'\n\n"
    "Some of Trump's actions have put Beijing on the back foot.\n\n"
    "His military operations in Iran and Venezuela have weakened two countries that have been close partners for China as well as significant oil suppliers.\n\n"
    "Trump in December approved $11 billion in weapons sales to Taiwan, a major boost for the democratically governed island China claims as its territory.\n\n"
    "He also pressured Panama to dislodge a Hong Kong port operator from around the Panama Canal and blockaded oil from reaching Communist-run Cuba.\n\n"
    "\"Iran was an extremely powerful signal to the Chinese that the United States continues to have overmatch,\" said Alex Gray, a former senior national security official during Trump's first term.\n\n"
    "But the costly war with Iran has burned through advanced missile stockpiles and redirected U.S. military assets away from Asia. And even the additional support for Taiwan has been tempered by fears that Trump might barter away U.S. backing for a favorable trade deal from Xi.\n\n"
    "\"If this is a chess match, the U.S. is taking pawns off the periphery rather than controlling the center of the board. Beijing doesn't like it, but it's an inconvenience rather than a strategic setback,\" said Jonathan Czin, a China expert at the Brookings Institution.\n\n"
    "Meanwhile, Trump's antagonism toward American allies - over the NATO alliance, tariffs and the Iran conflict - may erode the hard-earned consensus on the need to push back against China's actions on the global stage.\n\n"
    "To Beijing, the U.S. approach looks like institutional breakdown, said Wang Dong, a professor at China's Peking University, adding that China would not be diverted from its strategic course by short-term \"gambits.\"\n\n"
    "\"While transactional tactics and coercive signaling persist, they are increasingly overshadowed by deep coordination failures across the U.S. government,\" Wang said. \"This inconsistency erodes U.S. credibility.\"\n\n"
    "Reporting by Michael Martina, Trevor Hunnicutt and David Brunnstrom in Washington and the Beijing newsroom; Editing by Don Durfee and Alistair Bell\n\n"
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

    print_header("LENS-019.5 Calibration Round 4 - Article 1 (re-test on v3+qwen)")
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
