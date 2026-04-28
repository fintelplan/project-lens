#!/usr/bin/env python3
"""
calibrate_rubric_article1.py — LENS-019 Option I calibration, article 1 of 1

Scores the Reuters 'Trump's China policy drifts' article against two lenses:
  - xi_office (Template A, unelected-indefinite)
  - trump_office (Template B, elected-bounded)

Voice: Reuters Washington team (Michael Martina, Trevor Hunnicutt, David Brunnstrom).
Treated as a single byline-team entity for calibration purposes.

Per LR-080: this script does NOT write to Supabase. Pure scoring, pure output.
After calibration clean, delete this script and proceed to LENS-020 aggregator build.
"""

import sys
import os
import json
from dataclasses import asdict

sys.path.insert(0, 'code')
from dotenv import load_dotenv
load_dotenv()

from lens_framing_rubrics import score_article_voice_lens


ARTICLE_TITLE = "With tariffs stalled, Trump's China policy drifts"

ARTICLE_BODY = """WASHINGTON, April 21 (Reuters) - When President Donald Trump returned to office in 2025, he vowed to use tariffs to reset relations with China, which he said was "killing" the United States with its trade policies.

Now, more than a year into his second term, Trump's aggressive trade moves have not fundamentally altered Beijing's trade or military actions. Instead, Washington's China policy appears adrift, causing confusion among officials and driving contradictory decisions.

The administration's erratic moves toward Beijing have been on full display in recent months. Those include adding top Chinese companies to a military blacklist only to withdraw the list moments later, and a decision by Trump to greenlight AI semiconductor sales to China within minutes of his government labeling Chinese access to them a national security threat.

As Trump prepares for his planned May 14-15 visit to China to meet President Xi Jinping, the first such trip by an American president in eight years, critics argue such inconsistencies, coupled with his improvisational dealmaking style, have undermined the U.S. in its competition with Beijing.

"You have departments and agencies acting on their own accord, often with different objectives, and even at times in countervailing ways," said Ely Ratner, a former Assistant Secretary of Defense for Indo-Pacific Security Affairs.

"On any given day, it feels like the policy can zigzag in either direction," Ratner said.

Responding to Reuters questions on the administration's approach to China, White House spokesperson Kush Desai said Trump's trade agenda had "flipped the script" on decades of failed policy that hollowed out the U.S. industrial base.

"By leveraging our economy - the biggest and best consumer market in the world - and his great relationship with President Xi, President Trump has empowered America to finally operate from a position of strength in global diplomatic and trade matters," Desai said.

NO PLAN B

Trump launched his second term China policy with a dramatic trade broadside, initially hiking tariffs on Chinese goods to around 145%.

Beijing did not back down, however, and retaliated with tariff increases of its own.

The countries eventually forged an uneasy detente after China, which holds a virtual monopoly on the refining and processing of the world's rare earths, threatened to choke off supplies of the minerals needed by U.S. industries.

A February ruling by the Supreme Court invalidating many of Trump's duties further undercut the administration's strategy.

"Their entire original strategy was centered around using tariffs to pressure China into major concessions. That effort quickly ran aground," said Scott Kennedy, a China expert at the Center for Strategic and International Studies think tank. "There has been no coherent Plan B."

The tariffs did produce at least one result Trump has sought: the U.S. goods trade deficit with China decreased by 32% to $202 billion in 2025 compared to 2024, U.S. government data show.

But tariffs have not changed Beijing's mercantilist trade policies, and their fitful use likely reduced industry incentive to reshore manufacturing, a major goal of Trump's America First approach. The U.S. lost 91,000 manufacturing jobs from February to December of last year.

Treasury Secretary Scott Bessent and U.S. Trade Representative Jamieson Greer, who have run China policy instead of hawkish Secretary of State Marco Rubio, appear to have lowered expectations for an overhaul in commercial relations, shifting emphasis to a new "managed trade."

"Where do we want to be with China? We want relations to be stable. We want our trade to be more balanced. We want it to be in non-sensitive goods," Greer said in March.

In the face of Trump's turmoil, China has sought to portray itself as the responsible power.

"We ... stay committed to acting as a positive and stable force for good," its foreign ministry said in January when asked if Beijing benefited from the chaotic U.S. approach.

CONFLICTING SIGNALS

The administration's reversals haven't just been on tariffs.

In December, Trump declared on social media that he had approved the controversial sale of advanced Nvidia H200 AI semiconductors to China, the very chips his Justice Department only 30 minutes earlier said were being smuggled to China, constituting a threat to national security.

Two U.S. officials told Reuters those conflicting signals left them and others in the government flummoxed.

In February, Trump's Pentagon blacklisted top Chinese technology companies for allegedly aiding the Chinese military, only to mysteriously withdraw the list an hour later with little explanation.

In the fall, the Commerce Department issued rules to extend export controls to thousands of subsidiaries of Chinese companies, arguing it closed a significant loophole by which foreign companies could access sensitive technology. But the U.S. paused those measures, along with planned U.S. port fees for Chinese-built vessels intended to boost American shipbuilding, in the face of China's threat to restrict rare earths.

"These contradictions ultimately trace back to President Trump, who makes decisions in the moment, unconstrained by a broader strategy," said Zack Cooper, who studies U.S. strategy in Asia at the American Enterprise Institute think tank.

'TAKING PAWNS'

Some of Trump's actions have put Beijing on the back foot.

His military operations in Iran and Venezuela have weakened two countries that have been close partners for China as well as significant oil suppliers.

Trump in December approved $11 billion in weapons sales to Taiwan, a major boost for the democratically governed island China claims as its territory.

He also pressured Panama to dislodge a Hong Kong port operator from around the Panama Canal and blockaded oil from reaching Communist-run Cuba.

"Iran was an extremely powerful signal to the Chinese that the United States continues to have overmatch," said Alex Gray, a former senior national security official during Trump's first term.

But the costly war with Iran has burned through advanced missile stockpiles and redirected U.S. military assets away from Asia. And even the additional support for Taiwan has been tempered by fears that Trump might barter away U.S. backing for a favorable trade deal from Xi.

"If this is a chess match, the U.S. is taking pawns off the periphery rather than controlling the center of the board. Beijing doesn't like it, but it's an inconvenience rather than a strategic setback," said Jonathan Czin, a China expert at the Brookings Institution.

Meanwhile, Trump's antagonism toward American allies - over the NATO alliance, tariffs and the Iran conflict - may erode the hard-earned consensus on the need to push back against China's actions on the global stage.

To Beijing, the U.S. approach looks like institutional breakdown, said Wang Dong, a professor at China's Peking University, adding that China would not be diverted from its strategic course by short-term "gambits."

"While transactional tactics and coercive signaling persist, they are increasingly overshadowed by deep coordination failures across the U.S. government," Wang said. "This inconsistency erodes U.S. credibility."

Reporting by Michael Martina, Trevor Hunnicutt and David Brunnstrom in Washington and the Beijing newsroom; Editing by Don Durfee and Alistair Bell"""


def print_result(result, label):
    print()
    print("=" * 76)
    print(f"  {label}")
    print("=" * 76)
    d = asdict(result)
    # Pretty-print the score structure
    print(f"Status:         {d['status']}")
    print(f"State lens:     {d['state_actor_lens']}")
    print(f"Template:       {d['template']}")
    print(f"Rubric version: {d['rubric_version']}")
    print(f"Confidence:     {d['confidence']}")
    print(f"Not applicable: {d['not_applicable']}")
    print()
    print(f"Food for thought: {d['food_for_thought']}")
    print()
    axis_keys = [
        'axis_sympathy_democratic_movements',
        'axis_state_actor_legitimacy',
        'axis_blame_attribution',
        'axis_historical_context_completeness',
        'axis_sources_quoted_diversity',
    ]
    axis_short = {
        'axis_sympathy_democratic_movements':   'Axis 1 (Rights trajectory)',
        'axis_state_actor_legitimacy':          'Axis 2 (Apparatus legitimacy)',
        'axis_blame_attribution':               'Axis 3 (Cui Bono / Beneficiary)',
        'axis_historical_context_completeness': 'Axis 4 (Division framing)',
        'axis_sources_quoted_diversity':        'Axis 5 (Pretense vs service)',
    }
    for k in axis_keys:
        ax = d.get(k)
        if not ax:
            print(f"  {axis_short[k]}: (missing)")
            continue
        score = ax.get('score', '?')
        print(f"  {axis_short[k]}: {score}")
        print(f"    evidence:  {ax.get('evidence_phrase', '')[:180]}")
        print(f"    reasoning: {ax.get('reasoning', '')[:260]}")
        print()

    if d.get('error'):
        print(f"ERROR: {d['error']}")


def main():
    voice_name = "Reuters Washington team (Martina/Hunnicutt/Brunnstrom)"

    print()
    print("╔" + "═" * 74 + "╗")
    print("║  LENS-019 Calibration — Article 1: Trump's China policy drifts        ║")
    print("║  Voice: Reuters Washington team                                        ║")
    print("║  Scoring against: xi_office (Template A) + trump_office (Template B)  ║")
    print("╚" + "═" * 74 + "╝")

    # ── Score vs xi_office ──
    print("\n[calling rubric for xi_office ...]")
    r_xi = score_article_voice_lens(
        article_title=ARTICLE_TITLE,
        article_body=ARTICLE_BODY,
        article_source="Reuters via Google News",
        voice_name=voice_name,
        voice_type="author",
        state_actor_lens="xi_office",
    )
    print_result(r_xi, "RESULT — xi_office (Template A)")

    # Short pause so TPMGuard doesn't wait
    import time; time.sleep(3)

    # ── Score vs trump_office ──
    print("\n[calling rubric for trump_office ...]")
    r_trump = score_article_voice_lens(
        article_title=ARTICLE_TITLE,
        article_body=ARTICLE_BODY,
        article_source="Reuters via Google News",
        voice_name=voice_name,
        voice_type="author",
        state_actor_lens="trump_office",
    )
    print_result(r_trump, "RESULT — trump_office (Template B)")

    # ── Compare side by side ──
    print()
    print("╔" + "═" * 74 + "╗")
    print("║  COMPARISON TABLE                                                      ║")
    print("╚" + "═" * 74 + "╝")
    print()
    print(f"{'Axis':40s} | {'xi_office':>12s} | {'trump_office':>12s}")
    print("-" * 70)
    axis_keys = [
        ('axis_sympathy_democratic_movements',   'Axis 1: Rights trajectory'),
        ('axis_state_actor_legitimacy',          'Axis 2: Apparatus legitimacy'),
        ('axis_blame_attribution',               'Axis 3: Cui Bono'),
        ('axis_historical_context_completeness', 'Axis 4: Division framing'),
        ('axis_sources_quoted_diversity',        'Axis 5: Pretense v service'),
    ]
    d_xi = asdict(r_xi)
    d_tr = asdict(r_trump)
    for k, label in axis_keys:
        xi_ax = d_xi.get(k) or {}
        tr_ax = d_tr.get(k) or {}
        xi_score = xi_ax.get('score', 'n/a')
        tr_score = tr_ax.get('score', 'n/a')
        print(f"{label:40s} | {str(xi_score):>12s} | {str(tr_score):>12s}")
    print("-" * 70)
    print(f"{'Confidence':40s} | {d_xi.get('confidence', 'n/a'):>12} | {d_tr.get('confidence', 'n/a'):>12}")
    print(f"{'Not applicable':40s} | {str(d_xi.get('not_applicable', 'n/a')):>12s} | {str(d_tr.get('not_applicable', 'n/a')):>12s}")
    print()

    # ── Dump raw JSON for deeper inspection ──
    print("─" * 70)
    print("RAW JSON — xi_office:")
    print(json.dumps(asdict(r_xi), indent=2, ensure_ascii=False))
    print()
    print("RAW JSON — trump_office:")
    print(json.dumps(asdict(r_trump), indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
