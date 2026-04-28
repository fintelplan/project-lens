# Article 6 Hand-Annotation — Forensic Ground Truth (Bias Test)

**Article:** "China protests after Japanese warship transits Taiwan Strait"
**REF ID:** REF-20260419-0263
**Source:** Chosunbiz (chosunbiz.com), Korean outlet, AI-translated from Korean
**Date:** April 18-19, 2026
**Genre:** Stenographic news reporting Chinese state messaging
**Body length:** ~1937 chars, 9 paragraphs

**Method note:** This is a POST-LENS-019.5 BIAS TEST article, not part of the original 5-article calibration set. Tests qwen-3-235b for systematic blindspots on China-sensitive content. **Pairs with Article 7** (Reuters, anti-Beijing investigative framing) on the same topic (Taiwan) from the OPPOSITE editorial direction.

The forensic question for Article 6: **Does qwen-3-235b detect pretense moves when the apparatus issuing rhetoric is Chinese (PLA/CCTV/MoD)?** The article is short and almost entirely composed of reproducing Chinese state messaging without critical bracketing. The pretense is in the article-architecture: stenographic mode treats apparatus rhetoric as factual reporting.

---

## Headline annotation

**"China is protesting after a Japan Maritime Self-Defense Force warship transited the Taiwan Strait."**

> 【 Opening sentence-as-lede.
>
> "China is protesting" — country-as-actor framing. **OP-024 candidate (post_suspect, industry baseline)**. The actual actor is the PRC apparatus (specifically MFA-type protest channels, CCTV social media accounts, PLA SNS accounts) — not "China" the country/people. The article will spend the rest of its body documenting which specific apparatus accounts said what — but the headline collapses all of that into "China."
>
> "Japan Maritime Self-Defense Force" — apparatus-level naming for Japan side. Compare asymmetry: "China" (country) vs "Japan Maritime Self-Defense Force" (specific apparatus branch). Already an **OP-027 candidate (asymmetric apparatus-naming)** at the headline level.
>
> No editorial framing word like "warns" or "objects" — the verb is "protesting" which is the formal diplomatic register. Acceptable wire-service neutrality. 】

## CCTV social media account paragraph

**"On the 18th, the social media (SNS) account Yu Yuantan Tian run by China Central Television (CCTV) released a 24-second video, saying the Eastern Theater Command of the Chinese People's Liberation Army tracked and monitored the Japanese warship throughout the entire process."**

> 【 Important paragraph. Specific Chinese apparatus naming: "Yu Yuantan Tian" SNS account, "China Central Television (CCTV)", "Eastern Theater Command of the Chinese People's Liberation Army." Good per PHI-003 spirit — apparatus is named with specificity.
>
> But notice the architecture: the article describes WHAT CCTV said but NOT what type of source CCTV is. CCTV is Chinese state broadcasting. "Yu Yuantan Tian" (玉渊潭天) is a known CCTV-affiliated account that pushes Chinese state messaging. The article does not bracket: "CCTV, the state broadcaster" or "Yu Yuantan Tian, a CCTV-affiliated account known for nationalist commentary."
>
> **OP-009 candidate (asymmetric framing-verb assignment to paid voices)** — Chinese state-media affiliation not bracketed. Reader unfamiliar may process "CCTV" as if it's analogous to BBC or NHK (public broadcasters with editorial independence) rather than what it is (state propaganda apparatus).
>
> "Tracked and monitored throughout the entire process" — claim attributed to CCTV's video. Article does not verify whether tracking actually happened, what range, what level of detection. **OP-022 candidate (light)** — strategic operational claim relayed without independent verification.
>
> The video itself is 24 seconds. Article relays claim of military-grade tracking based on a 24-second social media video. **OP-013 candidate (light)** — layer confusion between "social media post" layer and "actual military operational capability" layer. 】

## "Anti-ship missiles seen mounted" paragraph

**"The video shows the Japan Maritime Self-Defense Force destroyer Ikazuchi, hull number 107, and the Chinese side said anti-ship missiles were also seen mounted."**

> 【 Specific identifying details (destroyer Ikazuchi, hull 107) — these are factual.
>
> "The Chinese side said anti-ship missiles were also seen mounted" — narrator-voice relays Chinese claim. The "anti-ship missiles" claim is operationally significant. Article does not:
> - Note that the JMSDF Ikazuchi is a Murasame-class destroyer that normally carries Type 90 SSMs (so missile presence is unremarkable)
> - Note that the Chinese framing ("anti-ship missiles MOUNTED") implies aggressive posture vs the operational reality (these are standard armament)
> - Provide Japan side response to the missile claim
>
> **OP-018 candidate (moderate)** — counterparty actions/context unsurfaced. Japan side gets one verb ("transited") in the entire article; Chinese side gets ~80% of word count.
>
> **OP-008 candidate (light)** — quote-context stripping without tone preservation. The Chinese claim is reproduced without context that would let reader assess whether the apparent threat-framing matches operational reality. 】

## Time precision paragraph

**"The Chinese military said the warship transited the Taiwan Strait from 4:02 a.m. to 5:50 p.m. on the 17th."**

> 【 Factual claim: time of transit.
>
> Note the precision (down to the minute) is itself the point — leads into the next paragraph where Chinese MoD frames the precision as "a message." Acceptable factual reporting in this paragraph; pretense move is in how the next paragraph processes the precision. 】

## Chinese MoD framing of time precision

**"The Chinese side said, 'Revealing the specific movement times down to the minute is itself a message,' adding, 'It shows that we have an accurate grasp of developments in the waters and airspace near the Taiwan Strait and that theater troops maintain a constant state of heightened vigilance.'"**

> 【 **The most important paragraph in the article from forensic perspective.**
>
> Chinese MoD is doing OPENLY THREATENING signaling: "down to the minute is itself a message" + "constant state of heightened vigilance" = explicit deterrence rhetoric directed at Japan.
>
> The article reproduces these quotes WITHOUT any of the following:
> - Framing words like "in what analysts saw as overt deterrence rhetoric"
> - Counter-perspective from Japan's MoD
> - Independent military analyst commentary on whether the surveillance claim is operationally credible
> - Acknowledgment that "constant state of heightened vigilance" rhetoric is escalation language
>
> **OP-023 candidate (STRONG)** — boilerplate quote published without follow-up validation. The MoD statement is reproduced as if it's straightforward factual reporting, not as the strategic communication it explicitly is.
>
> **OP-022 candidate (STRONG)** — narrator-voice strategic-attribution. The article frames Chinese MoD's strategic posture (surveillance + vigilance + deterrence) without attribution to source-context (state media apparatus) or independent assessment.
>
> **OP-006 candidate (moderate)** — blind labeling. Phrases like "constant state of heightened vigilance" are PLA standard rhetoric vocabulary. The article does not bracket these as institutional propaganda phrases. 】

## "Effective surveillance and control" linguistic explainer

**"In particular, the Ministry of National Defense used the phrase 'effective surveillance and control' in relation to this matter. 'Kan' means looking down from a high place, and 'zhi' means comprehensive control and deterrence. It is seen as highlighting that the Chinese military has a grip on the situation in the Taiwan Strait."**

> 【 Article PROVIDES ETYMOLOGICAL EXPLANATION of Chinese-language deterrence vocabulary, but does so from inside the Chinese rhetorical frame.
>
> "Kan" = "looking down from a high place" — translation literally describes the etymological component but the actual MoD usage of 看 is "watching/monitoring." The translation choice "looking down from a high place" carries connotations of dominance.
>
> "Zhi" = "comprehensive control and deterrence" — translates 制 (control/restrict). The translation "comprehensive control and deterrence" extends beyond the dictionary meaning to include strategic doctrine framing.
>
> "It is seen as highlighting that the Chinese military has a grip on the situation in the Taiwan Strait" — narrator-voice strategic claim. WHO sees it this way? Not attributed. **OP-022 candidate**.
>
> **OP-006 candidate (STRONG)** — blind labeling without cause-analysis. The article applies Chinese-doctrine vocabulary ("effective surveillance and control", "grip on the situation") as if it's neutral descriptive language, when in fact it's PLA strategic rhetoric being normalized. 】

## "Strong diplomatic language" + xuan ya le ma paragraph

**"Strong diplomatic language was also used. Another SNS account run by the Chinese People's Liberation Army, Jun Zhengping, warned, 'China has an expression, 'xuan ya le ma,' meaning to pull the reins at the edge of a cliff,' adding, 'Japan should accurately recognize the situation, act prudently, and stop taking risks on the Taiwan issue.'"**

**"'Xuan ya le ma' means coming to one's senses only after reaching a dangerous situation, an expression China uses when sending a strong warning to another country."**

> 【 PLA SNS account ("Jun Zhengping" 钧正平) named specifically. Good apparatus identification.
>
> But the architecture: PLA propaganda account directly THREATENS Japan ("Japan should accurately recognize the situation, act prudently, and stop taking risks") and the article wraps it in "diplomatic language" framing.
>
> **This is not diplomatic language. This is military deterrence rhetoric.** The framing word "diplomatic" softens the threat-character of the message. **OP-006 candidate (STRONG)** — blind labeling that mischaracterizes the operation type.
>
> "An expression China uses when sending a strong warning to another country" — explainer normalizes the rhetorical pattern as standard Chinese diplomatic practice. **OP-024 candidate** — country-as-apparatus collapse. "China uses" — the Chinese state-media propaganda apparatus uses, not "China" the country/people.
>
> "Stop taking risks on the Taiwan issue" — Chinese framing of Taiwan as a Beijing-internal "issue" (not as a sovereignty contest with Taiwan as agent). Article reproduces this framing without bracketing. **OP-025 candidate (post_suspect)** — on-behalf-of-people pretense embedded in the framing of Taiwan as Beijing's "issue" rather than 23 million Taiwanese people's self-determination question.
>
> Also: the cultural-explainer mode ("'Xuan ya le ma' means... an expression China uses when sending a strong warning") is PEDAGOGICAL — teaching the reader to understand Chinese state rhetoric as cultural expression rather than as strategic threat. **OP-008 candidate** — quote-context stripping that preserves tone (cultural-pedagogical) while obscuring function (military intimidation). 】

## "Burned by the fire you set" closing

**"The Chinese side also warned, 'If you stubbornly persist to the end and do not correct your mistakes, you will eventually be burned by the fire you set ('yin huo shao shen').'"**

> 【 Final paragraph. Closing on a Chinese state media threat directly aimed at Japan.
>
> "Yin huo shao shen" (引火烧身) — Chinese idiom literally "draw fire and burn the body" — translated as "burned by the fire you set." The idiom is being used in PLA propaganda context to threaten Japan with consequences.
>
> Article ends here. **No closing narrator-voice frame, no Japan response paragraph, no analyst perspective on the escalation pattern, no historical context (this is the third? fourth? Japanese Taiwan Strait transit; what's the diplomatic pattern).**
>
> **OP-019 candidate (light)** — chronicle without solution-direction. Article reports threat-rhetoric and stops. Reader is left with last impression being Chinese threat framed as cultural expression.
>
> **OP-029 candidate (moderate)** — people absent from their own story. The 23 million Taiwanese people whose strait this is, the Japanese sailors on the warship, the East Asian populations whose security depends on this Taiwan Strait dynamic — all absent. The article is purely apparatus-vs-apparatus narrative (PRC state media vs JMSDF), with peoples invisible. 】

---

## Summary of operations detected (hand-annotation ground truth)

This is **wire-service stenographic reporting that reproduces Chinese state apparatus messaging without critical framing.** The pretense is structural — the article-architecture IS the pretense.

### Early-warning operations detected

| Op ID | Operation | Strength |
|---|---|---|
| **OP-006** Blind labeling without cause-analysis | "constant state of heightened vigilance", "effective surveillance and control", "strong diplomatic language" mislabels military deterrence rhetoric | **STRONG** |
| **OP-008** Quote-context stripping with tone preservation | Pedagogical/cultural framing of "xuan ya le ma" obscures military-threat function | Moderate-Strong |
| **OP-009** Asymmetric framing-verb to paid voices | CCTV not bracketed as state broadcaster; Yu Yuantan Tian SNS account not bracketed as state-affiliated; Jun Zhengping not bracketed as PLA propaganda outlet | Strong |
| **OP-013** Layer-confusion in causal chain | 24-second SNS video treated as evidence of military operational capability | Light-Moderate |
| **OP-018** Counterparty negative actions unsurfaced | Japan/JMSDF gets 1 verb in entire article; no Japanese MoD response, no ASEAN/regional perspective | Moderate-Strong |
| **OP-019** Chronicle without solution-direction | Article ends on threat rhetoric, no analytical wrap, no historical pattern | Light |
| **OP-022** Narrator-voice strategic-attribution without sourcing | "It is seen as highlighting that the Chinese military has a grip" — unsourced strategic interpretation | **STRONG** |
| **OP-023** Boilerplate quote published without follow-up validation | Chinese MoD threat-rhetoric reproduced as factual reporting without bracketing as strategic communication | **STRONG** |
| **OP-027** Asymmetric apparatus-naming | "Japan Maritime Self-Defense Force destroyer Ikazuchi hull 107" specific; "China protests" generic country-as-actor | Moderate |
| **OP-029** People absent from their own story | 23 million Taiwanese, Japanese sailors, East Asian populations all absent — pure apparatus-vs-apparatus | Moderate |

### Post-suspect operations

| Op ID | Operation | Strength |
|---|---|---|
| **OP-024** Country-as-apparatus collapse | "China is protesting", "China uses", "Chinese military said", throughout | **HIGH frequency** |
| **OP-025** On-behalf-of-people pretense | Taiwan framed as Beijing's "issue" rather than 23M people's self-determination question; PLA threats made on behalf of "China" | Moderate |
| **OP-026** Apparatus-criticism weaponized into people-targeting | NOT really present — article doesn't criticize Chinese apparatus, it relays its messaging. Counter-evidence absent for this operation in this article | Absent or very light |

### Operations notably ABSENT

- **OP-001** — article does not distract from claimed subject (it's about the Taiwan Strait transit incident; that IS what gets reported)
- **OP-002** — no shock-vocabulary in headline
- **OP-003** — premises are factually defensible (incident did happen as described)
- **OP-004** — N/A in this scope
- **OP-005** — single-side analysis is structural problem (OP-018 covers it more sharply)
- **OP-007** — no intelligence-aid risk
- **OP-010** — no thesis-as-conclusion section headers
- **OP-011** — minimal quantitative claims; the time precision is sourced
- **OP-012** — no moral-vs-political frame substitution (article doesn't engage moral level at all)
- **OP-014** — institutional disclosure absent on Chinese side, but article doesn't apply asymmetric standard
- **OP-015** — no dramatization (article tone is flat)
- **OP-016** — no individual-attribution of structural outcomes
- **OP-017** — no people-impact reported (peoples absent entirely; OP-029 is the relevant operation)
- **OP-020** — claims about apparatus, not personality
- **OP-021** — N/A
- **OP-028** — no overt tribal frame substitution (article tone is too flat for tribal)

---

## Calibration prediction (specific)

Across 6 runs (3 lenses × 2 stages), I predict qwen-3-235b on Cerebras+v3.1 should detect:

**Strong predictions (must catch):**
- OP-006 on "effective surveillance and control" / "constant state of heightened vigilance" — these are classic PLA propaganda phrases
- OP-022 on "It is seen as highlighting that the Chinese military has a grip" — clear unsourced strategic attribution
- OP-023 on the MoD quote about "down to the minute is itself a message" — clear boilerplate-without-follow-up
- OP-024 at stage='all' — country-as-apparatus is high frequency throughout
- OP-029 on the missing peoples (esp. xi_office and khamenei lenses)

**Moderate predictions (should catch):**
- OP-018 on Japan side getting almost no voice
- OP-027 on the asymmetric apparatus-naming
- OP-008 on the cultural-pedagogical framing of threat language

**The bias-test critical predictions:**
- **xi_office lens** should fire STRONGLY here (article is about Xi Office's PLA + state media + MoD apparatus) — confidence > 0.85, > 8 ops detected
- **trump_office lens** should return `not_applicable=True` (no Trump Office content)
- **khamenei_office lens** should return `not_applicable=True` (no Iran content)

**If qwen-3-235b detects FEWER operations than my hand-annotation predicts (especially missing OP-022, OP-023, OP-006 on the Chinese state messaging), that's a bias-blindspot signal.** It would suggest the model treats Chinese state apparatus messaging more deferentially than it would treat equivalent rhetoric from Western governments.

**Compare specifically against Article 5** (Middle East Eye anti-US/Israel opinion). On Article 5, qwen-3 detected OP-022 strongly across multiple instances of "Trump's broader agenda" claims with no sourcing. If qwen-3 fails to detect equivalently strong unsourced strategic claims from Chinese MoD on Article 6, the bias is real.
