#!/usr/bin/env python3
"""
calibrate_rubric_article3.py — LENS-019.5 Calibration Round 3

Article 3: "China Shock 2.0 jolts global economy as Trump does Xi\'s work"
  - REF ID: REF-20260419-0218 (from operator references file)
  - source: Asia Times (asiatimes.com)
  - author: William Pesek (Asia Times columnist)
  - date: April 17, 2026
  - genre: Opinion column / business analysis

This is genre #3 in calibration set:
  - Article 1 (Reuters) — wire-service news with subtle pretense
  - Article 2 (AP) — wire-service news, relatively clean
  - Article 3 (Asia Times) — OPINION column with explicit thesis

The thesis is in the headline: "Trump does Xi\'s work" — meaning Trump\'s
trade-war policies inadvertently strengthen Xi Office\'s strategic position.
This is critical-of-Trump framing that structurally serves Xi-Office-favorable
narrative. Tests whether rubric catches pretense in advocacy genre vs
catching only journalism-pretending-to-be-neutral.

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

sys.path.insert(0, "code")
# Force Cerebras for this calibration run (Option R per LENS-019.5)
# PATCHED: honor pre-existing S2F_PROVIDER (allows ollama/openrouter override)
if "S2F_PROVIDER" not in os.environ:
    os.environ["S2F_PROVIDER"] = "cerebras"  # default

from dotenv import load_dotenv
load_dotenv()

from lens_framing_rubrics import detect_operations_in_article


# Article metadata
ARTICLE_TITLE = "China Shock 2.0 jolts global economy as Trump does Xi\'s work"
ARTICLE_SOURCE = "Asia Times (asiatimes.com)"
ARTICLE_ID = "REF-20260419-0218"

VOICE_NAME = "William Pesek (Asia Times columnist)"
VOICE_TYPE = "author"

LENSES_TO_TEST = ["xi_office", "trump_office", "khamenei_office"]


# Article body — concatenated string lines to avoid triple-quote collision
ARTICLE_BODY = (
    "TOKYO — On top of the tariffs, wars and inflation upending the global economy, US chieftains are grappling with a new question: which tech companies might get 'BYD-ed' next? The reference here is to the Chinese electric-vehicle juggernaut that's zoomed past Elon Musk's Tesla and its peers to become No. 1 globally.\n\n"
    "The idea that the Shenzhen EV company was an aberration has since been dispelled by the 'DeepSeek shock,' which disrupted the artificial intelligence realm, and by a number of other startup successes, from Horizon Robotics to autonomous vehicle shop Qcraft.\n\n"
    "But as 2026 unfolds, and US President Donald Trump prioritizes trade wars over investing in raising America's tech game, China is not so quietly grabbing market share around the globe despite Trump's tariffs and trade curbs.\n\n"
    "And thanks to the 'Made in China 2025' program Xi Jinping launched in 2015, this isn't spin but economic reality. And this latest 'China shock', increasingly known as 'China shock 2.0', is becoming the talk of corporate boardrooms everywhere.\n\n"
    "The reason: 11 years on, the fruits of Xi's effort to expand China's footprint in EVs, AI, batteries, biotechnology, renewable energy, robotics, semiconductors and other future technologies are making more and more headlines in the Western media.\n\n"
    "As economist Rob Subbaraman at Nomura Holdings explains, the original 'China shock' was the epochal disruptions caused by the surge in imports from China after its 2001 entry into the World Trade Organization. The subsequent surge in foreign direct investment inflows helped transform China's cheap labor into the world's factory floor.\n\n"
    "Corporate America suddenly realized that China-made consumer goods were becoming ubiquitous in the West. By 2017, when the Trump 1.0 presidency began, China accounted for 22% of the US's total goods imports. While this dynamic helped tame global inflation, Subbaraman explains, it 'hollowed out its manufacturing industry, causing significant job losses.'\n\n"
    "Since then, 'China's supply-oriented fiscal approach of upgrading its industrial capacity and its deepening struggle to revive local consumer demand have given rise to the second China shock,' Subbaraman explains.\n\n"
    "This 'China shock 2.0 refers to the overcapacity in China that has led to price wars and an erosion of profit margins. Rather than retreating, China's highly competitive manufacturers have redirected sales from the deflationary environment at home to foreign markets.'\n\n"
    "Much of the discussion about this latest wave of Chinese competitiveness has focused on how it's altering economic dynamics inside Asia's biggest economy. As competition at home intensifies, Xi's Communist Party has been trying to clamp down on excessive price competition — what economists term 'anti-involution.'\n\n"
    "Because China is 'not so focused on boosting consumption at home, they're basically making way more stuff than they can sell in their own domestic market,' notes Brookings Institution economist Jon Czin. 'So a lot of that is getting pushed to Europe, to the United States, maybe less so to the United States over the past year. But to other parts of the world.'\n\n"
    "China's EV boom tells the story. In part thanks to surging oil prices since Trump launched a war with Iran, EVs are back. In March alone, Chinese exports of EVs and hybrids jumped a record 140% year-on-year to 349,000 units. BYD, the globe's biggest EV company, accounted for a third of the increase. Geely Automobile and Chery Automobile rounded out the top three exporters.\n\n"
    "This makes for quite a split-screen. Demand at home remains dire as a massive Chinese property crisis continues to weigh on consumer spending. In March, domestic sales of Chinese EVs and hybrids fell for the third consecutive month, dropping 14% year-on-year. BYD sales fell more than in March, while Musk's Tesla saw a 24% decline in China.\n\n"
    "EVs are just the vanguard of the ways in which this next wave of Chinese competitiveness is increasingly shaking up the global financial system.\n\n"
    "As Harvard economist Gordon Hanson observes, this latest global shock is one where 'China goes from underdog to favorite. Today, it is aggressively contesting the innovative sectors where the United States has long been the unquestioned leader: aviation, AI, telecommunications, microprocessors, robotics, nuclear and fusion power, quantum computing, biotech and pharma, solar, batteries.'\n\n"
    "To compete, Hanson notes, the US will need more than tariffs — it needs a 'better trade strategy,' including investment in key, innovative fields.\n\n"
    "This China shock 2.0 is also remaking Southeast Asia, now Beijing's biggest trading partner. Asia should be bracing for an even bigger 'China squeeze' going forward, notes economist Arvind Subramanian, a former top adviser to Indian Prime Minister Narendra Modi. The scale of China's ability to continue increasing its market share in higher-value-added, high-tech sectors will reverberate far and wide, he warns.\n\n"
    "China's move upmarket 'is squeezing out space for all the developing countries poorer than itself in these low-skilled sectors,' Subramanian explains. 'So the Asia model that China, Korea and Taiwan benefited from is now being squeezed out more and more.'\n\n"
    "For less developed economies in Southeast and South Asia, the next China shock exacerbates the risk of deindustrialization. For years now, developing Asia and emerging markets elsewhere have benefited from global supply chains.\n\n"
    "Most are likely to suffer an accelerating loss of export competitiveness as Chinese goods undercut industries across the board. Many manufacturing sectors might not survive the price pressure.\n\n"
    "In boardrooms from Tokyo to Seoul, chieftains are realizing that so much of what Japan and South Korea do well is becoming commoditized in real time. China is now a rival in cars, electronics, robots, ships and popular entertainment. Not just on price, but also on innovation. And with its own supply chains.\n\n"
    "Hence, the who's-getting-BYD-ed-next paranoia coursing through corporate suites everywhere. General Motors and Ford, for example, are increasingly in harm's way.\n\n"
    "In June 2025, Ford CEO Jim Farley warned that China's cost/quality ratio 'is far superior to what I see in the West' and his research into the mainland's potential in autos is 'the most humbling thing I've ever seen.' Bottom line, he said: 'We are in a global competition with China, and it's not just EVs. And if we lose this, we do not have a future at Ford.'\n\n"
    "Trump's move to scrap Washington's EV tax credit hardly helped. The end of the $7,500 credit for new EVs and $4,000 quickly reordered Detroit's priorities. The trade war Trump launched in early 2025 trashed supply chains that long relied on Canada and Mexico.\n\n"
    "US moves to scrap fuel-efficiency standards have seen Detroit prioritize big, gas-burning-engine vehicles like SUVs and trucks that do poorly abroad over EVs that do.\n\n"
    "As Detroit turned inward and away from the battery research and development that China is pioneering, BYD, Geely, Chery and other mainland rivals expanded aggressively into Australia, Brazil, India, Mexico, Thailand and beyond. All this threatens US automakers' international market share in ways only now becoming clear to officials in Washington.\n\n"
    "For now, 100% tariffs are keeping BYD out of the physical US market. The same goes for US regulatory complexity and the political pushback sure to come ahead of the 2026 Congressional elections and the 2028 presidential contest. Yet BYD and peers are doing brisk business in other key regions while churning out savvy, tech-driven models — some as cheap as US$10,000.\n\n"
    "As William Li, CEO of mainland EV company Nio, explains: 'The entire supply chain in China has completely changed since 2018.' He adds that 'costs across the supply chain, including batteries, have plummeted. In the past, we only needed to focus on making products. Now, everyone is confused, asking what is happening and why we've been sucked into a downward spiral.'\n\n"
    "China's recently unveiled Five-Year Plan for economic development from 2026 to 2030 suggests even greater state support across advanced industries, including biotechnology and robotics.\n\n"
    "'Beijing is doubling down on technologies with clear industrial applications. AI, semiconductors, and quantum technology remain central, while newly elevated sectors include embodied robots, brain-computer interfaces, commercial aerospace, satellite internet, low-altitude drones, and efforts to build out the domestic compute ecosystem,' notes Mingda Qiu, a Eurasia Group analyst. 'Several of these emerging priorities align with evolving industry trends.'\n\n"
    "By contrast, previously emphasized areas such as virtual reality and 'internet plus' have faded, Qiu says. Cloud computing, big data, and blockchain — once standalone priorities — now function as enabling infrastructure and are secondary to the push to expand domestic computing capacity.\n\n"
    "'The shift,' Qiu says, 'suggests a clear preference for technologies that can scale and a willingness to deprioritize concepts that have yet to translate into industrial deployment.'\n\n"
    "Also, China is betting on AI to boost both productivity and demand, Qiu notes. As such, the Qiu explains, the latest Five-Year Plan upgrades the goal of industrial transformation from 'digitization' to 'inteligentization,' embedding AI, big data and autonomous systems into machinery, manufacturing, and management to improve performance and advance Beijing's 'new quality productive forces' priority.\n\n"
    "Beijing's bet is that broader real-world application boosts demand for AI-related hardware and software, while reducing the risk of an AI bubble. All this means that, for all China's domestic challenges, it's not throttling back on efforts to build on Xi's 'Made in China 2025'\n\n"
    "As part of a recent series on the China shock 2.0, the Financial Times detailed how Asia is bracing for an intensification of the so-called 'flying geese' strategy that Japan harnessed from the 1930s on: a developing giant moving aggressively into higher-tech industries and leaving more labor- and energy-intensive industries behind.\n\n"
    "Thanks to China's advances, says Goldman Sachs economist Andrew Tilton, many Asian economies may need to reassess their growth models.\n\n"
    "'China's lopsided economic structure — muscular manufacturing, enervated consumption — is a feature of its macro policy and is set to have even bigger global consequences in the years ahead,' Tilton explains. 'Historically, the fastest-developing economies in Asia shed lower-value manufacturing activities to poorer neighbors as they moved up the technological ladder.'\n\n"
    "The idea behind the 'flying geese' model, he notes, is that less-developed economies like Korea, Taiwan, and eventually Southeast Asia and China were following behind the most advanced economy — at that time Japan.\n\n"
    "'But,' he explains, 'dragons don't fly in formation: China is far larger and its policymakers intend to build as large a manufacturing ecosystem as they can, and to limit the flow of technologies and core manufacturing out of China even as it moves up the value chain.'\n\n"
    "To be sure, some activities like apparel manufacturing and lower-value assembly have moved to Southeast Asia, particularly Vietnam, in part due to US tariff pressures. 'But China's policy is to keep as much control of the means of production as possible, particularly core technologies,' Tilton says.\n\n"
    "'Together with a protectionist shift by the single largest export market — the United States – and growing discomfort with the hollowing out of manufacturing in Europe, this has major implications for economic models elsewhere in Asia.'\n\n"
    "Other Asian exporters, it follows, may need to stay clear of China rather than follow it. 'Without differentiated sources of competitive advantage – like services trade for India, commodities for Indonesia and Malaysia, high-tech products for Taiwan and Korea– export-led growth will be increasingly difficult,' Tilton concludes.\n\n"
    "The trouble is that the financial system underneath China Inc.'s tech ambitions is being held back by the slow pace of reforms. As the headwinds now zooming Beijing's way intensify, Xi's party is likely to be even more focused on shoring up growth in the short run than economic retooling aimed at longer-term prosperity.\n\n"
    "In this way, the Iran war may be ushering in a lost period for economic reform at a moment when China can least afford to waste time. It's also true, though, that Xi's party is keeping eyes on the bigger prize of moving upmarket at an accelerating rate.\n\n"
    "As Trump champions a policy mix from the 1980s, China is ensuring it's ready not just to compete in the economy of the future but to dominate it.\n\n"

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

    print_header("LENS-019.5 Calibration Round 3 - Article 3")
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
