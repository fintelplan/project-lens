#!/usr/bin/env python3
"""
calibrate_rubric_article7_reuters.py — Post-LENS-019.5 Bias Test (Article 7 Reuters via ET BrandEquity)

Article 7: "China turns Taiwan\'s own voices against it in information war" (Reuters)
  - REF ID: REF-20260419-0220 (from operator references file)
  - source: Reuters (originally), republished via ET BrandEquity (India)
  - author: Reuters Taiwan/Tokyo desk (uncredited)
  - date: April 18, 2026
  - genre: Wire-service investigative-style reporting on Chinese information warfare
  - bias-test relevance: opposite editorial direction from Article 6.
    Reuters frames Chinese narrative as warfare/strategy. Tests whether
    qwen-3-235b detects narrator-voice strategic-attribution, paid-voices
    asymmetric framing (IORG funded by US/EU governments; Bonnie Glaser
    at GMF think tank), and on-behalf-of-people pretense (KMT/DPP framings)
    when the article direction is anti-Beijing.

POST-LENS-019.5 BIAS TEST: pairs with Article 6 (Chosunbiz pro-Beijing
stenographic) on Taiwan topic from OPPOSITE editorial direction:
  - Article 6 (Chosunbiz) — pro-Beijing stenographic
  - Article 7 (Reuters) — anti-Beijing investigative framing  ← THIS RUN

KEY symmetry test: Does qwen-3-235b detect operations symmetrically across
opposite editorial directions on the SAME political topic (Taiwan)? If
catalog catches more operations on one direction than the other, that
indicates qwen-3 directional bias on China-sensitive content.

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
ARTICLE_TITLE = "China turns Taiwan\'s own voices against it in information war"
ARTICLE_SOURCE = "Reuters via ET BrandEquity (India republication)"
ARTICLE_ID = "REF-20260419-0220"  # Reuters via ET BrandEquity, April 18 2026

VOICE_NAME = "Reuters Taiwan/Tokyo desk (uncredited investigative)"
VOICE_TYPE = "author"

LENSES_TO_TEST = ["xi_office", "trump_office", "khamenei_office"]


# Article body — concatenated string lines to avoid triple-quote collision
ARTICLE_BODY = (
    "China is using social media to broadcast Taiwanese opposition voices. These messages aim to undermine Taiwan's government and discourage defense spending. Familiar voices are used to make propaganda more believable. This information warfare is part of Beijing's strategy to pressure Taiwan without military force. Taiwan's government is working to counter this growing influence.\n\n"
    "Highlights:\n\n"
    "- China's media amplifies Taiwanese opposition voices against the DPP.\n\n"
    "- KMT leader Cheng Li-wun met Xi Jinping amid rising tensions.\n\n"
    "- Taiwan counters China's information warfare with media literacy efforts.\n\n"
    "As Chinese warships and fighter jets staged massive drills around Taiwan in December, a parallel action was unfolding on smartphone screens.\n\n"
    "On Douyin, China's version of TikTok, a news outlet run by the Chinese Communist Party posted a 51-second video of Taiwan opposition leader Cheng Li-wun accusing President Lai Ching-te of inviting Chinese aggression. Lai, Cheng said, was \"dragging all 23 million of us\" in Taiwan into a \"dead end, a road to death\" by pursuing independence. The clip quickly surfaced on Facebook, YouTube and other platforms popular in Taiwan.\n\n"
    "Chinese state media outlets are increasingly amplifying Taiwanese critics of the island's ruling Democratic Progressive Party (DPP), including influencers and politicians linked to the opposition Kuomintang (KMT), according to five Taiwanese security officials and data from Taipei-based research group IORG that was shared with Reuters.\n\n"
    "China imports the public statements of leading KMT and other opposition figures that are critical of the Taiwan government and pumps them out in a torrent of anti-DPP messaging in Chinese state media and on social media platforms in China, according to the data and sources. Those clips are then reshared and often repackaged for consumption on platforms popular in Taiwan, including Facebook, TikTok and YouTube, as well as on Douyin, sometimes embellished or presented in ways that obscure China's hand.\n\n"
    "While China has in the past employed Taiwanese figures in its propaganda, it has turbocharged this information-warfare tactic, the Taiwan security officials said: Familiar voices and accents can sound more credible.\n\n"
    "The goal is to discredit a government Beijing accuses of seeking independence, the officials said. And, with the DPP seeking $40 billion in extra defense outlays, the campaign also appears aimed at convincing Taiwanese that China's military power is so overwhelming that it is futile for Taiwan to spend heavily on more American weapons, according to IORG and three of the security officials.\n\n"
    "China's Taiwan Affairs Office and defense ministry didn't respond to requests for comment about Beijing's information warfare.\n\n"
    "Taiwan's defense ministry told Reuters it is countering a massive increase in Chinese \"cognitive warfare\" by strengthening the armed forces' media-literacy skills and psychological resilience. President Lai's office added that cross-strait peace must be \"built on strength, not on concessions to authoritarian pressure.\"\n\n"
    "Facebook, TikTok and YouTube, which are blocked in China, didn't respond to questions about Chinese information warfare. Douyin also didn't respond to a request for comment.\n\n"
    "China considers Taiwan part of its territory and hasn't ruled out using military force to seize it. Taiwan's government rejects China's sovereignty claim, saying it is already an independent country called the Republic of China, its formal name. Beijing refuses to speak with the DPP administration, and calls Lai a \"separatist.\"\n\n"
    "While Chinese preparations for military action against Taiwan continue, the information warfare is part of Beijing's strategy of wearing down Taiwan without resorting to force. In this regard, Taiwan's opposition KMT provides a valuable opening for China: The party has moved to seek closer ties with Beijing in a bid to head off what it says is a crisis made worse by the DPP government's provocation of China.\n\n"
    "Cheng, the KMT leader, met Chinese President Xi Jinping this month in Beijing, where Xi told her the KMT and the Communist Party must \"consolidate political mutual trust\" and \"join hands to create a bright future of the motherland's reunification.\"\n\n"
    "In a statement to Reuters, the KMT said Cheng's visit to Beijing fulfilled a campaign pledge and continued a long-established tradition of top-level meetings between the KMT and the Communist Party. The two parties have many differences, but both believe disagreements should be resolved through dialogue, it added.\n\n"
    "Social Media Battleground\n\n"
    "Data provided to Reuters by IORG, also known as the Taiwan Information Environment Research Center, shows the mechanics of the Chinese campaign. The non-partisan group of social scientists and data analysts is funded in part by the U.S. and European governments, and academic institutions in Taiwan.\n\n"
    "Some 560,000 videos were posted on Douyin by 1,076 accounts run by official Communist Party media outlets in the fourth quarter of 2025. About 18,000 videos discussed Taiwan. IORG used facial-recognition technology to identify 57 Taiwanese figures in 2,730 clips, with results verified by IORG researchers and reviewed by Reuters.\n\n"
    "The number of videos featuring Taiwanese voices more than doubled from a year earlier during October and November, and monthly airtime jumped 164% to 369 minutes.\n\n"
    "Strikingly, of the top 25 Taiwanese figures in the Chinese videos, 13 are affiliated with the KMT, from current lawmakers and party representatives to former officials under past KMT-led governments. Two others are senior officials in a small party that supports unification with China, while 10 are influencers known for criticizing the governing DPP.\n\n"
    "Cheng, the KMT leader, was the top-ranked Taiwanese figure in the Chinese clips, featuring in 460 videos across 68 Douyin accounts and generating more than five million interactions, including likes, comments and shares. The videos amplified her calls for \"peace\" with China, her criticism of President Lai as a \"pawn\" of external forces, and her characterization of the DPP's stance on Taiwan independence as destructive. Once aired on Chinese state media and social media platforms, some of the clips were repackaged and posted on platforms popular in Taiwan.\n\n"
    "In its statement, the KMT said Cheng's comments reflected the mainstream aspirations of the Taiwanese people for peace. \"Even if mainland state media tend to incorporate more Taiwanese voices, this is based on the diversity of public opinion that already exists in Taiwan,\" it added.\n\n"
    "Various influencers were also heavily cited by the Chinese outlets. Among them were Holger Chen Chih-han, a bodybuilder popular with younger audiences, and five retired senior military officials known for criticizing the DPP and Taiwan's defenses.\n\n"
    "\"Happy birthday, motherland,\" Chen said on a YouTube livestream in late September, ahead of China's National Day. Short clips of the broadcast, in which he also said the people of Taiwan and China were \"one family,\" were later shared by Chinese state media outlets, including China News Service.\n\n"
    "Chen didn't respond to a request for comment.\n\n"
    "In one video posted by China News Service, former Taiwan Army Colonel Lai Yueh-chien claimed Chinese drones had \"entered\" Taiwan undetected during military exercises in December. Lai also suggested that China might conduct a decapitation strike against \"pro-independence leaders\" in their sleep. The video soon appeared on Facebook and YouTube.\n\n"
    "The assertion that Chinese drones had approached Taiwan first appeared in a video posted on a social media account run by China's military, according to IORG. Taiwan's defense ministry denied the drone claim.\n\n"
    "China News Service didn't respond to Reuters questions. Lai Yueh-chien declined to comment about his presence in Chinese state media.\n\n"
    "Taiwan's Mainland Affairs Council told Reuters the government hoped the retired military officers \"will be mindful of public perception\" and shouldn't echo Beijing's rhetoric. Moreover, it added, they \"must not forget the oath they once swore to be loyal\" to Taiwan.\n\n"
    "Psychological Targeting\n\n"
    "Support in Taiwan for maintaining the status quo indefinitely has risen eight points to 33.5% since 2020, while support for maintaining the status quo but moving toward independence has declined almost four points to 21.9%, according to a long-running annual survey series released in January by the Election Study Center at Taiwan's National Chengchi University. The combined proportion who want unification with China as soon as possible or wish to maintain the status quo but move toward unification has been relatively stable at around 7%.\n\n"
    "It's unclear whether the intensification of China's information warfare is having an impact. There has been no discernible shift in Taiwanese attitudes toward independence or unification since 2024, according to the annual survey data. This timeframe roughly coincides with the period of intensified information warfare examined by IORG. The DPP, China's principal political antagonist in Taiwan, lost its parliamentary majority in 2024 but has won the last three presidential elections.\n\n"
    "Still, the barrage of messaging \"creates an environment in which China can more easily win support, because its strategy really is to lower morale, instill a sense of psychological despair, convince people they have no future in being autonomous and their best option is to join up with China,\" said Bonnie Glaser, head of the Indo-Pacific program at the German Marshall Fund of the United States, a think tank that receives funding from U.S. and European governments and companies including tech and defense firms.\n\n"
    "Taiwan's intelligence officials recorded over 45,000 sets of inauthentic social-media accounts and 2.3 million pieces of disinformation on China-Taiwan issues last year, a January report by Taiwan's National Security Bureau said. It described the goals of Beijing's information warfare: to exacerbate divisions within Taiwan; weaken Taiwanese people's will to resist; and win support for China's stance.\n\n"
    "\"They want you to doubt the military and doubt Taiwan, to make you feel that no one will come to your help if war breaks out,\" one Taiwanese security official said of China's state media.\n\n"
    "A civil-defense handbook that Taiwan's government issued to households last year went so far as to state pre-emptively that amid heightened tensions with China, any claims of Taiwan's surrender must be considered false - a recognition that the battle is intensifying, even if no shots have been fired.\n\n"
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

    print_header("Post-LENS-019.5 Bias Test - Article 7 (Reuters, China info war on Taiwan)")
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
