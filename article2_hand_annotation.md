# Article 2 Hand-Annotation — Forensic Ground Truth

**Article:** "US imposes sanctions on a China-based oil refinery and 40 shippers over Iranian oil"
**REF ID:** REF-20260425-0050
**Source:** AP News (apnews.com)
**Author:** Fatima Hussein (AP, Treasury Department reporter)
**Date:** April 25, 2026

**Method:** Forensic margin-note method developed in LENS-019.5 with operator. Each annotation maps to operations catalog v3 (lens-OPS-001) where applicable. This annotation is INDEPENDENT of the rubric module output — it is the ground truth Claude reads against to calibrate.

**Voice scored:** Fatima Hussein, AP byline (institutional voice).

---

## Headline annotation

**"US imposes sanctions on a China-based oil refinery and 40 shippers over Iranian oil"**

> 【 Headline structure: `[Actor] [verb] [target description] over [reason description]`. Three observations:
> 
> (1) "US imposes" — country-as-actor framing. The US (people, country, government, Treasury, Trump Office) collapsed into one named actor. Should be "Trump Office's Treasury Department imposes" per PHI-003. **OP-024 candidate (post_suspect)**.
> 
> (2) "China-based oil refinery" — geographic descriptor that invites reader to mentally process the refinery as Chinese-owned/Chinese-operated. Hengli Petrochemical IS Chinese; the description is factually accurate. But the phrase choice "China-based" rather than "Hengli Petrochemical" foregrounds nationality. The refinery's specific identity (Hengli) is not in the headline; the country tag IS. Country-as-locator does work that specific-naming would not.
> 
> (3) "over Iranian oil" — telegraphs the Trump Office's framing. The sanctions are FOR buying Iranian oil. But "Iranian oil" itself is a politically loaded category — global oil trade has many origins, and the categorization "Iranian" makes the oil itself the target rather than the policy choices around it. Headline accepts Trump Office's premise that Iranian oil is contraband.
> 
> No conclusion-as-header or shock-vocabulary. Headline is descriptive. **No OP-002 (shock-start) detected**. 】

## Lede paragraph

**"WASHINGTON (AP) — President Donald Trump's administration is placing economic sanctions on a major China-based oil refinery and roughly 40 shipping companies and tankers involved in transporting Iranian oil."**

> 【 Specific-apparatus naming: "President Donald Trump's administration" — names the apparatus directly. Good per PHI-003. Compare to how the article will treat Xi Office later. This is OP-027 territory (asymmetric apparatus-naming) — let me hold that judgment until I see how Xi Office is described.
> 
> "involved in transporting Iranian oil" — the phrase "involved in" pre-emptively assigns culpability. The shipping companies "involved" — by this phrasing, transporting becomes participating. A neutral phrasing would be "transporting Iranian oil" without the modifier. The article is starting from Trump Office's legal framing. **OP-006 candidate (blind labeling without cause-analysis)** — the structural question of whether transporting legally-sourced (or contested-sourced) oil constitutes wrongdoing is not raised; the labeling that "involvement" = sanctionable is accepted as premise. 】

## Second paragraph — the threat reference

**"The move, announced Friday and first reported by The Associated Press, makes good on Trump's threat to impose secondary sanctions on companies and countries that do business with Iran."**

> 【 "Makes good on Trump's threat" — frames the sanctions as the fulfillment of a previously stated promise. This is reportorial, not analytical. But notice what's NOT here: any analysis of whether the threat was justified, whether secondary sanctions on third-country firms are legal under international law, whether this is a normal pressure tactic or an unusual escalation. The article accepts "Trump threatened, Trump delivered" as the analytical frame. The BIGGER question — what gives Trump Office authority to dictate to Chinese, Hong Kong, UAE, Omani financial institutions — is invisible.
> 
> **OP-013 candidate (layer-confusion in causal chain)** — the article reports the surface (sanctions imposed) without addressing the deeper layer (US extraterritorial reach over global financial system, structural conditions that make secondary sanctions enforceable, decades of US-dollar dominance that produced this leverage).
> 
> "his Republican administration's overall ramped-up campaign" — "ramped-up" is mild evaluative language. Not weaponized but not neutral. Acceptable. 】

## Strait of Hormuz paragraph

**"Concurrently, the U.S. this month imposed a physical blockade on the Strait of Hormuz, the Persian Gulf waterway that is crucial to global energy supplies."**

> 【 Important moment. Several observations:
> 
> (1) "U.S. ... imposed a physical blockade" — country-as-actor. **OP-024 candidate**. A physical blockade is an act of war under international law; the article uses neutral verb "imposed" as if it were an administrative measure. The vocabulary normalizes military action. This is a borderline **OP-006 (blind labeling)**: blockade IS the label, but its meaning under international law is not analyzed.
> 
> (2) "crucial to global energy supplies" — passive description of a fact. But the strategic implication (US Navy controlling a waterway through which 20-30% of world oil flows) is left as descriptive geography rather than examined as policy. The peoples of every oil-importing country are affected by this blockade, including peoples in countries not party to the US-Iran conflict. **OP-018 candidate (counterparty negative actions unsurfaced)** — Trump Office's blockade has consequences for many peoples; those consequences are not surfaced. **OP-004 candidate (subject-of-policy erased)** — the blockade affects peoples globally; peoples are absent.
> 
> (3) "Concurrently" — a single word doing important work. Connects the sanctions to the blockade as parallel actions. But "concurrently" implies coincidence or coordination. If coordination, then this is a designed multi-front pressure campaign — significant analytical content the article does not develop. **OP-006 (blind labeling)** again — labeling moves as "concurrent" without analyzing whether they are coordinated. 】

## Sanctions mechanism + summit timing

**"The sanctions, which cut off the companies from the U.S. financial system and penalize anyone who does business with them, come just a few weeks before President Donald Trump and China's Xi Jinping are due to meet in China."**

> 【 "China's Xi Jinping" — Xi is named with country-as-possessive, framing him as belonging to China. Compare to "Trump" earlier (named without "America's"). This is **OP-027 candidate (asymmetric apparatus-naming)** — Trump named freely, Xi prefixed with country-possessive.
> 
> Strategic timing observation: sanctions imposed days before a Trump-Xi summit. Two readings:
> 
> Reading A: Pressure-before-negotiation tactic — Trump Office deliberately escalating as leverage going into Xi meeting.
> Reading B: Trump Office acting independent of Xi summit calendar — agencies operating on their own timeline.
> 
> The article does not analyze WHICH reading. The timing is treated as incidental ("come just a few weeks before"). But the timing IS the analysis — it is one of the most important facts in the article. **OP-006 candidate** again — "concurrent" timing with summit reported as fact, not analyzed as choice.
> 
> Also: this is OP-007 territory (publication-as-intelligence-aid). Publishing detailed analysis of US sanctions strategy in the days before a summit gives Xi Office's analysts material to incorporate into their preparation. Less acute than the Reuters article on Article 1 (which catalogued Trump's decision-style); here the publication-effect is smaller because the sanctions are public anyway. **OP-007 weak/absent on Article 2**. 】

## Hengli identification + capacity

**"Included in Friday's sanctions is Hengli Petrochemical's facility in the port city of Dalian, which has a processing capacity of roughly 400,000 barrels of crude oil per day, making it one of the biggest independent refineries in China."**

> 【 Specific-apparatus naming! "Hengli Petrochemical" is named as the specific entity. **Counter-evidence to OP-024 / OP-027 elsewhere in the article.** When Trump Office sanctions a specific Chinese company, that company gets a specific name. This is good practice. The asymmetry I flagged earlier (Trump named, Xi country-possessive) is partially balanced by Hengli being specifically named.
> 
> **The 400,000 bbl/day fact** — this is a specific quantity. Like the 145% tariff in Article 1, the number sounds large. Is 400K bbl/day "huge" in global refining context? Total global oil refining capacity is ~100M bbl/day. So Hengli is 0.4% of global capacity. Significant but not overwhelming. The article says "one of the biggest independent refineries in China" — within China context, big. The article does not provide global context. **OP-011 candidate (quantity-without-justification)** — number reported without comparison baseline that lets reader evaluate magnitude. 】

## Treasury attribution paragraph

**"The Treasury Department says Hengli has received Iranian crude oil shipments since 2023 and has generated hundreds of millions of dollars in revenue for the Iranian military."**

> 【 "The Treasury Department says" — claim is sourced to Trump Office's Treasury. But the next clause ("has generated hundreds of millions of dollars in revenue for the Iranian military") is presented as fact rather than as Treasury's claim. The framing creates a sourcing illusion: only the first half of the sentence is attributed; the second half implicitly inherits the same factual standing.
> 
> If Treasury Department alleges the revenue figure, the article should attribute the entire claim. If the article has independent verification, it should say so. Neither happens. **OP-022 candidate (narrator-voice strategic-attribution without sourcing)** — the "for the Iranian military" causal chain is the analytically loaded part and it floats free of attribution.
> 
> "the Iranian military" — the IRGC? The regular Iranian armed forces? Khamenei Office? The actual end-recipient of revenue determines whether sanctions hit the right target. The vague label "Iranian military" treats Iranian military structure as monolithic. **OP-024 candidate (apparatus-people collapse)** at country level for Iran — though Khamenei Office is the relevant apparatus, the article treats "Iranian military" as a uniform actor. 】

## United Against Nuclear Iran source

**"The advocacy group United Against Nuclear Iran said in February 2025 that Hengli is one of dozens of Chinese purchasers of Iranian oil."**

> 【 Critical moment. "United Against Nuclear Iran" is named as an "advocacy group." That's accurate as far as it goes. But UANI is a US-based 501(c)(3) advocacy organization with specific funding sources, specific political positioning (anti-Iran-deal), and specific connections to US foreign policy networks. Mark Wallace (UANI CEO) has held senior positions in US government. The article presents UANI as a neutral source for the factual claim about Hengli being one of "dozens of Chinese purchasers."
> 
> The article includes UANI's affiliation but does not bracket what that affiliation means in the same way it would (or should) for, say, a Chinese state-affiliated source. Compare: the Liu Pengyu quote later in the article comes from "China's embassy in Washington" — affiliation flagged. UANI affiliation also flagged ("advocacy group") but the IMPLICATION of advocacy is different in reader's mind for "advocacy group" vs "embassy spokesperson."
> 
> **OP-009 candidate (asymmetric framing-verb assignment to paid voices)** — UANI is "said" (analytical voice). Liu Pengyu later "said" too — but the framing surrounding each shapes how readers process them. UANI gets neutral introduction. Liu Pengyu gets reactive framing implicitly. Light asymmetry; not as severe as Ratner-vs-Desai in Article 1 but present. 】

## "China is the biggest buyer" paragraph

**"China is the biggest buyer of Iranian oil, importing 80% to 90% of Iranian oil before the U.S.-Israeli war with Iran broke out, though the crude — transported by a shadow fleet of vessels — often has its origin obscured but arrives in China as oil from countries such as Malaysia. Smaller refineries, known as teapot refineries, typically are the buyers of Iranian oil."**

> 【 "China is the biggest buyer of Iranian oil" — country-as-actor framing for a market dynamic. Chinese teapot refineries (private, smaller) buy Iranian oil; Chinese state-owned majors mostly comply with US sanctions. The "China" subject collapses very different actors (state-owned majors vs private teapots) into one entity. **OP-024 candidate (post_suspect)** — the country-collapse is happening here.
> 
> "the U.S.-Israeli war with Iran" — phrase introduced casually as if reader already knows about it. This is significant context: there is an active US-Israel-Iran war. The article presupposes this. A reader unfamiliar might wonder which war exactly. Since this is not Article 2's main subject, the casual reference may be appropriate journalistically. But it does establish a frame where Iranian oil is "war oil" — making the moral case for sanctions seem stronger.
> 
> "shadow fleet of vessels — often has its origin obscured" — interesting fact, well-described. The phrase "shadow fleet" is loaded vocabulary (sounds illicit, illegal-by-vibe) but it's also factually used by sanctions analysts. Acceptable.
> 
> "teapot refineries" — Chinese term, used here. Gives the reader a specific class of actor. Good. Counter-evidence to OP-024 — when the article wants to be specific about Chinese sub-actors, it can be. 】

## Iran's demand paragraph

**"Iran has previously said that its demands for ending the war include the lifting of sanctions."**

> 【 "Iran has previously said" — country-as-actor for Iran. Khamenei Office, IRGC, Iranian Foreign Ministry, parliament — different actors with different positions. "Iran" collapses them. **OP-024 candidate**.
> 
> Also: "its demands for ending the war" — assumes Iran has unified demands. In reality, different factions within Khamenei Office structure have different demands. The article erases Iranian internal political diversity by treating "Iran" as monolithic.
> 
> **OP-029 candidate (people absent from their own story)** — Iranian peoples (whose lives are most affected by both the war and the sanctions) have no voice in this article. They are framed as the country's people, the country has demands, no Iranian individual speaks. 】

## Bessent quote 1

**"Treasury Secretary Scott Bessent said Friday that his agency 'will continue to constrict the network of vessels, intermediaries and buyers Iran relies on to move its oil to global markets.'"**

> 【 Bessent quote, named with title and agency. Specific. Good per PHI-003.
> 
> "his agency 'will continue to constrict'" — uses the future verb "will continue." This is committed forward-looking policy language. Within LENS-004 SHARED_RULES, projecting future apparatus behavior is something Project Lens itself avoids ("never predicts"). When the article reports such forward statements, it's appropriate to flag that this is policy commitment, not policy fact. The article does not flag.
> 
> "Iran relies on" — country-as-apparatus again. Khamenei Office or specific Iranian actors rely on; "Iran" the country does not "rely on" anything. **OP-024 candidate**. Routine industry baseline.
> 
> **No major operations beyond OP-024**. Quote is direct, attribution is clear, framing is reportorial. This is what good wire-service work looks like for a quoted official statement. 】

## Bessent letter paragraph

**"Earlier this month, Bessent's department sent a letter to financial institutions in China, Hong Kong, the UAE and Oman threatening to levy secondary sanctions for doing business with Iran and accusing those countries of allowing Iranian illicit activities to flow through their financial institutions."**

> 【 "those countries of allowing Iranian illicit activities" — country-as-apparatus across multiple actors at once. China, Hong Kong, UAE, Oman are categorized as countries "allowing" things — but it's their financial institutions, regulators, central banks, governments (different apparatus in each case) that "allow" or don't. **OP-024 candidate**, and meaningfully so — multiple peoples (Chinese, Emirati, Omani) are positioned as targets of Trump Office's accusation through their country-as-apparatus.
> 
> "Iranian illicit activities" — adjective "illicit" pre-emptively labels Iranian financial flows as illegal. From Trump Office's policy stance, they are. From international law, the matter is contested (US extraterritorial sanctions are not universally recognized as binding). The article uses "illicit" without bracketing as Trump Office's framing.
> 
> **OP-006 candidate (blind labeling)** — "illicit" applied to a contested category as if uncontested. **OP-022 candidate** — narrator voice characterizing Iranian financial flows as illicit without sourcing the characterization to Trump Office. 】

## Bessent quote 2 (April 15 briefing)

**"Bessent said during a White House press briefing on April 15 that the administration has told countries 'that if you are buying Iranian oil, that if Iranian money is sitting in your banks, we are now willing to apply secondary sanctions, which is a very stern measure.'"**

> 【 Direct quote. Attribution clear. Specific date. Good. Quote contains explicit threat language ("very stern measure") which the article reproduces verbatim. The article does not editorialize. Reportorial.
> 
> "we are now willing to apply secondary sanctions" — this is the unilateral assertion of US authority over third-country financial systems. The structural question (does the US have legal authority to do this? on what basis?) is not raised. **OP-013 candidate (layer-confusion)** — surface report (Bessent threatened) without deep layer analysis (US extraterritorial financial sanctions as a contested international-law tool that depends on dollar dominance). 】

## Energy turmoil + waivers

**"The sanctions come as the global energy trade is in turmoil as war around the Persian Gulf chokes off oil and natural gas shipments, causing prices to soar."**

**"Treasury has tried to quell the impact of rising oil prices issuing temporary sanctions waivers on Russia oil and a one-time waiver on Iranian oil already at sea."**

> 【 "war around the Persian Gulf chokes off oil and natural gas shipments" — passive voice for the war ("around the Persian Gulf" — locator, not actor). Who started the war? Who is conducting it? What is the cui bono of the war? The article reports the war's effects on energy markets without addressing its causes or beneficiaries. **OP-018 candidate** — counterparty (US-Israel-Iran war participants') actions and consequences unsurfaced beyond effect on prices.
> 
> "Treasury has tried to quell the impact" — Trump Office is portrayed as managing impacts it itself created. The contradiction (sanctions cause oil price impacts; same agency tries to "quell" the impacts) is reported without analysis. **OP-006 candidate (blind labeling)** — events labeled without analysis of whether they are coherent, contradictory, or strategically designed.
> 
> **OP-019 candidate (chronicle without solution-direction)** — the article describes turmoil and waivers without pointing toward what would resolve any of it. Pure status report. 】

## "AP was making efforts" paragraph

**"The AP was making efforts to contact Chinese officials for comment on the sanctions."**

> 【 Procedural disclosure. Good practice — tells the reader the article tried to get Chinese-side reaction and did not (yet) succeed. Compare to articles that just don't include the other side without disclosing the attempt. AP did the right thing here.
> 
> But notice: "Chinese officials" is the object of the contact attempt. Specific names? Specific apparatus? Foreign Ministry? Embassy in DC (which IS quoted later)? "Chinese officials" is country-as-collective. **OP-024 candidate** at low intensity.
> 
> **No major operations** in this paragraph. Light OP-024 only. Honest journalism. 】

## "China has disagreed" paragraph

**"China has disagreed with previous U.S. sanctions, but its major companies and banks still comply with U.S. sanctions because they are more exposed to the U.S.-dominated financial system."**

> 【 "China has disagreed" — narrator-voice claim about country posture. Country-as-apparatus framing. **OP-024 (post_suspect)**.
> 
> "its major companies and banks still comply" — observation that Chinese state-owned majors comply even when "China" disagrees. This is genuine analytical content — it reveals the gap between state position (disagreement) and corporate behavior (compliance). The reason given ("more exposed to the U.S.-dominated financial system") is a structural insight: dollar dominance produces compliance even from politically opposed actors.
> 
> This is one of the strongest analytical paragraphs in the article. It makes the structural case that US sanctions work because of dollar dominance, not because of Chinese agreement. Good.
> 
> But: the structural insight is presented in passive narrator voice as fact. No source. No expert quoted. No academic citation. The article ASSERTS structural-political analysis without supporting it. **OP-022 candidate (narrator-voice strategic-attribution without sourcing)** — even though the analytical claim is plausible and probably correct, attributing it without source is the same operation we flagged elsewhere.
> 
> **OP-013 (layer-confusion)** — actually, the OPPOSITE of OP-013. This paragraph DOES the deep-layer analysis (dollar dominance as structural enforcement). Counter-evidence to my earlier OP-013 flagging. The article is uneven — reports surface elsewhere, does structural here. 】

## Liu Pengyu quote

**"After the U.S. earlier this month sanctioned a Chinese refinery accused of buying Iranian oil, Liu Pengyu, a spokesperson for China's embassy in Washington, said the use of the sanctions 'undermines international trade order and rules, disrupts normal economic and trade exchanges, and infringes upon the legitimate rights and interests of Chinese companies and individuals.'"**

> 【 Critical comparison moment. Compare framings:
> 
> Bessent quote: introduced as "Treasury Secretary Scott Bessent said" — neutral attribution.
> 
> Liu Pengyu quote: introduced as "After the U.S. earlier this month sanctioned a Chinese refinery accused of buying Iranian oil, Liu Pengyu, a spokesperson for China's embassy in Washington, said..." — preceded by 22 words of context that frames the quote as RESPONSE to a prior US action.
> 
> This is **OP-009 (asymmetric framing-verb assignment)**! Bessent gets analytical-voice framing. Liu Pengyu gets reactive-voice framing through extended context-as-prefix. Same as Desai vs Ratner asymmetry in Article 1, just smaller. Bessent is presented as if speaking proactively (was actually responding to AP); Liu Pengyu is explicitly framed as responding.
> 
> Affiliation: "spokesperson for China's embassy in Washington" — affiliation flagged correctly. Embassy spokesperson IS officially-paid voice. Good per PHI-003 spirit.
> 
> "China's embassy" — country-as-possessive. **OP-024 candidate**.
> 
> The quote itself is boilerplate diplomatic language ("undermines international trade order"). Generic. The article includes it without follow-up: did AP ask "what specific harms to Chinese individuals?" or "what specific trade exchanges?" The boilerplate stands without verification. **OP-023 candidate (boilerplate quote published without follow-up validation)** — same as MFA quote in Article 1.
> 
> "Chinese companies and individuals" — Liu Pengyu's claim is that sanctions infringe rights of Chinese companies AND individuals. The article reports this claim. Whether sanctions on Hengli specifically harm Chinese individuals (workers at Hengli who lose jobs, Dalian residents whose port economy depends on Hengli, etc.) is not investigated. **OP-029 weakly** — Chinese individuals' actual experience absent. 】

---

## Summary of operations detected (hand-annotation ground truth)

### Early-warning operations detected

| Op ID | Operation | Where | Strength |
|---|---|---|---|
| OP-006 | Blind labeling without cause-analysis | Multiple: "involved in," "concurrent" timing, "illicit," "tried to quell" contradiction | Moderate-Strong |
| OP-009 | Asymmetric framing-verb assignment to paid voices | Bessent vs Liu Pengyu framings | Moderate |
| OP-011 | Quantity-without-justification | 400K bbl/day without global context | Light |
| OP-013 | Layer-confusion in causal chain | Surface reporting on extraterritorial sanctions / dollar dominance gap | Moderate (mixed — counter-evidence in "China has disagreed" paragraph) |
| OP-018 | Counterparty negative actions unsurfaced | War causes; blockade impacts on third-party peoples | Moderate |
| OP-019 | Chronicle without solution-direction | Article describes status without forward direction | Moderate |
| OP-022 | Narrator-voice strategic-attribution without sourcing | "Iranian military" revenue claim; "China has disagreed" structural analysis | Moderate-Strong |
| OP-023 | Boilerplate quote published without follow-up validation | Liu Pengyu quote, no follow-up question | Moderate |
| OP-029 | People absent from their own story | Iranian peoples, Chinese individuals affected by sanctions, third-country peoples affected by blockade | Moderate |

### Post-suspect operations (deepening evidence on already-flagged voices)

| Op ID | Operation | Where | Strength |
|---|---|---|---|
| OP-024 | Country-as-apparatus collapse | Throughout: "US imposes," "China is biggest buyer," "Iran has said," "China has disagreed" | Strong (industry baseline — multiple instances) |
| OP-025 | On-behalf-of-people pretense | Light — "Iran's demands," "Chinese companies' interests" framings | Light |
| OP-026 | Apparatus-criticism weaponized into people-targeting | "Iranian illicit activities" — Iranian peoples implicated through country-collapse criticism | Light-Moderate |

### Operations specifically NOT present (or weakly present)

- **OP-001 distract** — article does not distract from a substantive question; it reports the action straightforwardly
- **OP-002 shock-start** — headline is descriptive, not alarmist
- **OP-003 unverified premise as fact** — sanctions premise is reported, not asserted; some premises (illicit) are weak OP-006 not OP-003
- **OP-005 single-side analysis as bilateral** — article reports US side; does not pretend to be bilateral
- **OP-007 publication-as-intelligence-aid** — sanctions are public information; publication does not transfer strategic intel
- **OP-008 quote-context stripping** — quotes are short but adequately contextualized
- **OP-010 section-header as conclusion-disguised-as-description** — no section headers in this article (online format)
- **OP-012 frame substitution moral→political** — moral and structural considerations are largely absent rather than substituted
- **OP-014 selective institutional disclosure** — light; weak presence
- **OP-015 dramaticization of structural questions into personal-contest** — article is structural, not personality-driven
- **OP-016 personal-attribution framing of structural-national outcomes** — outcomes attributed to Trump Office collectively, not Trump personally
- **OP-017 people-impact reported without causal analysis** — no people-impact statistics in article (no jobs lost, etc.)
- **OP-020 person-naming-as-fact without evidence** — Bessent's role is established public position
- **OP-021 subordinate-stated commitments** — Bessent IS authorized speaker on Treasury actions
- **OP-027 asymmetric apparatus-naming across actors** — light: Trump Office named directly, Xi referenced via "China's Xi Jinping," but Hengli also named specifically. Mixed signal.
- **OP-028 tribal frame substitution for structural** — article is structural-economic, not tribal
- Lens 3 distinctive operations (OP-027, OP-028, OP-029) — OP-029 present at moderate strength; OP-027 and OP-028 absent or weak

### Honest findings about Article 2

**Article 2 is a noticeably less pretense-heavy article than Article 1 was.** Reading honestly:

- AP wire service delivers a relatively straight news report
- Specific apparatus naming (Hengli, Bessent, Liu Pengyu) demonstrates ability to be specific when reporting
- Country-as-apparatus framing is present (industry baseline) but not weaponized as in The Diplomat-style opinion pieces
- The Liu Pengyu vs Bessent asymmetry is real but lighter than Desai vs Ratner in Article 1
- The structural analysis paragraph ("China has disagreed... dollar-dominated financial system") is genuinely analytical content
- The article does NOT manufacture drama, does NOT use tribal vocabulary, does NOT engage in chess-metaphor framing

**Watch Alert prediction:** If S2-F module fires Watch Alert on Fatima Hussein based on this article alone, it is probably overcalling. This article shows roughly 5-6 early_warning operations at moderate strength — possibly enough to enter Watch suspect list, but if Hussein's other articles show similar patterns, the pattern would be wire-service-genre-baseline rather than voice-specific pretense. This is exactly the kind of voice that needs LENS-020 cross-topic-coherence test before any Clarity finding.

**Calibration test prediction:** I expect the rubric module to detect 5-9 operations on this article (vs my 9 hand-annotated). Higher detection count from the rubric would suggest the rubric is overcalling; lower count would suggest underdetection. Either is informative.

---

## Specific operations I am confident the rubric MUST detect

If the rubric module misses any of these, calibration fails on this article:

1. **OP-024** at high frequency (industry baseline, throughout the article) — should fire only at stage='all'
2. **OP-009** in the Bessent/Liu Pengyu asymmetric framing
3. **OP-022** somewhere — narrator voice asserts unattributed claims (the Iranian military revenue claim or the dollar-dominance analysis)
4. **OP-023** on the Liu Pengyu boilerplate without follow-up

If the rubric ALSO detects OP-006, OP-019, OP-029 at moderate strength, calibration is strong.

If the rubric detects OP-001, OP-002, OP-007, OP-008, OP-012, OP-015, OP-016 — those are FALSE POSITIVES on this article, suggesting the rubric is calibrated too aggressively.

If the rubric detects OP-027 or OP-028 strongly — review case-by-case but my read is these are absent or weak.

This is my honest forensic read. We compare against the rubric output when you run the calibration script.
