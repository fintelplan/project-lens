#!/usr/bin/env python3
"""
calibrate_rubric_article5_mee.py — LENS-019.5 Calibration Round 6 (Article 5 MEE)

Article 5: "War on Iran: Why Israel and the US are the ultimate losers"
  - REF ID: REF-20260419-0664 (from operator references file)
  - source: Middle East Eye (middleeasteye.net)
  - author: Hisham Bustani (Jordanian author/commentator/activist)
  - date: April 19, 2026
  - genre: Opinion essay, anti-US/Israel framing

Article 5 in 5-article calibration set. Tests catalog v3.1 against
opinion-genre advocacy from the OPPOSITE editorial direction of any
prior article — anti-US/Israel framing, pro-Iranian-resilience reading:
  - Article 1 (Reuters) — wire-service with subtle pretense
  - Article 2 (AP) — wire-service relatively clean
  - Article 3 (Asia Times) — opinion column anti-Trump
  - Article 4 (SCMP) — structural-economic news soft-Xi-favorable
  - Article 5 (Middle East Eye) — opinion column anti-US/Israel  ← THIS RUN

KEY symmetry test: does the rubric catch operations on anti-US/Israel
opinion writing the same way it catches them on anti-Trump opinion writing
(Article 3)? Asymmetric detection across editorial directions = directional
bias problem. Symmetric detection = catalog is genuinely viewpoint-agnostic.

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
ARTICLE_TITLE = "War on Iran: Why Israel and the US are the ultimate losers"
ARTICLE_SOURCE = "Middle East Eye (middleeasteye.net)"
ARTICLE_ID = "REF-20260419-0664"  # MEE, Hisham Bustani, April 19 2026

VOICE_NAME = "Hisham Bustani (MEE opinion contributor, Jordanian)"
VOICE_TYPE = "author"

LENSES_TO_TEST = ["xi_office", "trump_office", "khamenei_office"]


# Article body — concatenated string lines to avoid triple-quote collision
ARTICLE_BODY = (
    "Taking into account the continuously changing objectives of the US-Israeli war on Iran - which range from inciting internal turmoil and regime change, to dismantling Iran's civilian nuclear programme, eliminating its missile capacity and unconditionally opening the Strait of Hormuz - it is clear that none have been achieved. Rather, the campaign largely failed.\n\n"
    "Iran, despite suffering heavy civilian casualties and the assassination of first- and second-tier leadership, was able to maintain and even reinforce its governing authority.\n\n"
    "It conducted a sustained and gradually escalating asymmetrical campaign, placed the broader region under pressure, and demonstrated its capacity to disrupt global energy supplies by asserting control over the Strait of Hormuz. Given that the US declared a ceasefire without any visible prior negotiations with Iran, these factors could be interpreted as an Iranian victory.\n\n"
    "As for how US President Donald Trump might present this outcome as a victory, that is difficult to comprehend.\n\n"
    "His actions fit a broader pattern of empty threats, shifting strategies, inflammatory language and extreme rhetoric, including references to erasing Iranian civilisation. The US is today led by a group of largely non-expert, hyper-masculine figures attempting to preserve a declining global position. In doing so, they risk further weakening their own standing, while inadvertently strengthening their declared adversaries.\n\n"
    "This does not mean that Trump is unintelligent or unaware of his actions. Rather, he appears to correctly perceive the US as a declining power facing increasing competition, particularly from China.\n\n"
    "Since the end of World War Two, the US has acted as an aggressive, interventionist global power, frequently using overt and covert means to assert its influence, often in disregard of international law. What we are witnessing today is not new; similar patterns have been evident in the Middle East for decades.\n\n"
    "It is only more recently, as pressure has extended towards Europe - for example, in relation to Greenland - that Europeans have begun to recognise these dynamics as threatening, whereas they were long tolerated when applied elsewhere.\n\n"
    "Within this context, Trump's broader agenda for maintaining global dominance appears to include reshoring and controlling high-tech and AI industries, securing access to energy and rare earth resources, positioning the United States as a leading global exporter and key arbiter of oil and gas flows dominating key shipping and trade routes, reducing commitments to Europe, drawing Russia closer to the US and away from China, and granting Israel greater control in the Middle East to reduce US costs in the region.\n\n"
    "Yet this strategy is undermined by an overly aggressive and self-centred approach, making it difficult to execute the strategy effectively. As a result, many of Trump's actions have backfired, causing significant damage to both his own position and his broader strategic objectives.\n\n"
    "This has also had serious human consequences, contributing to global instability and loss of life.\n\n"
    "Another internal consequence of Trump's second term has been a noticeable shift within the right wing, particularly among segments of the Maga movement that oppose his current agenda and its close alignment with Israel.\n\n"
    "The Trump administration appears to have been influenced by Israeli Prime Minister Benjamin Netanyahu and his associates to engage in this aggression against Iran, in complete disregard of appeals to the contrary from Gulf allies, based on claims of a rapid and decisive victory that ultimately did not materialise.\n\n"
    "What we are now witnessing is not limited to dissatisfaction on the American left about a perceived 'Israel first' agenda. Prominent right-wing figures - most notably Tucker Carlson - have also begun to voice opposition. This emerging divide is gradually eroding Trump's support base and weakening his position domestically.\n\n"
    "It can thus be argued that Israel has emerged from this war a loser as well, with its most significant loss being the previously taken-for-granted support of the American public.\n\n"
    "Another arena in which Israel may be seen as having fallen short is its inability to fully disarm the Lebanese movement, Hezbollah. While it inflicted significant damage on the organisation's leadership and operational capacities, Hezbollah appears to have retained its ability to launch missiles and drones, as well as to confront Israeli ground incursions.\n\n"
    "Israel's most tangible success has instead been in deepening internal sectarian divisions in Lebanon, alongside the current Lebanese move towards direct negotiations with Israel from a position of marked weakness. In this sense, Lebanon has effectively conceded through political processes what Israel was unable to secure through military means.\n\n"
    "The Gulf states may also be considered among the relative losers of this confrontation. Having invested heavily in US security guarantees, they have been confronted with the reality that Israeli security interests take precedence in Washington's strategic calculus.\n\n"
    "The escalation has not only undermined the Gulf's carefully cultivated image as a stable and secure investment environment, but has also exposed vulnerabilities in its energy infrastructure.\n\n"
    "Continued uncertainty surrounding the Strait of Hormuz, coupled with the time required to repair and restore export capacities, risks prolonging economic disruption - while the United States stands to expand its own share of global energy markets through increased oil and gas exports.\n\n"
    "Meanwhile, the Strait of Hormuz now appears to be effectively under Iranian control - an unintended consequence that the Trump administration seemingly failed to anticipate, effectively creating a crisis where none previously existed, and reflecting yet another instance of US strategic overreach, compounded by its own restrictive posture towards maritime flows in the strait. This miscalculation echoes earlier US strategic errors, such as the 2003 invasion of Iraq, which ultimately strengthened Iran's influence within Iraq and contributed to the emergence of a weak, sectarian, fragmented and externally influenced political system.\n\n"
    "The global image of both Trump and the US has been significantly damaged. While such perceptions have long existed in parts of the Middle East and Global South, they are now increasingly visible in Europe as well.\n\n"
    "Whether this will translate into a loss of political power domestically remains uncertain. But Trump has already placed considerable strain on established US governance norms during his second term - and the extent to which this has been tolerated raises important questions about the resilience and nature of American democratic institutions.\n\n"
    "What seems more likely is that the Iranian government will become more entrenched and resolute in its position as reflected in its refusal to concede during recent negotiations in Pakistan, and its continued adherence to core strategic demands.\n\n"
    "Over the past decade, Iran has perceived multiple instances of the US acting deceptively and in bad faith: its unilateral withdrawal from the 2015 nuclear agreement, the military action during negotiations last June, and the latest attack during talks mediated by Oman.\n\n"
    "Under such circumstances, it is difficult to see why Iran would trust the US again. Similarly, the perceived inaction of European governments amid these developments raises questions about their credibility from Iran's perspective. The persistence of double standards in international dealings with Iran is unlikely to be accepted going forward.\n\n"
    "If the objective of the US-Israeli military actions was to weaken or destabilise the Iranian government, the outcome thus far appears to have been the opposite.\n\n"
    "The primary actors to emerge weakened from this endeavour are the US Gulf allies, who now face the burden of repairing infrastructural damage, potential losses in global energy market share, and prolonged uncertainty over the future security and governance of the Strait of Hormuz.\n\n"
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

    print_header("LENS-019.5 Calibration Round 6 - Article 5 (Middle East Eye)")
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
