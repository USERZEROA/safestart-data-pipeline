# Warehouse queries

Each question is one SQL file in `src/safestart_pipeline/warehouse/queries/`, run by its name:

```bash
python -m safestart_pipeline.warehouse query                      # the questions
python -m safestart_pipeline.warehouse query top_cited_sections   # one answer, as a Markdown table
```

The results below come from the warehouse built on 2026-11-23 from the lake's current versions (violations through
2026-11-21, accidents through 2026-11-16, production hours through 2026 Q3), eCFR title 30 amended to 2026-11-12,
and the crawl catalogs as of 2026-11-23. "Last 12 months" means up to the newest violation, 2026-11-21; "last
ten years" up to the newest accident. Each query ran in under 0.2 seconds.

## Which standards do MSHA inspectors cite most often, and what do they require?

`queries/top_cited_sections.sql`. The most cited standards are the conditions crews most often leave uncontrolled, so
SafeStart's pre-shift checks and its library should put their text first.

Combustible accumulations (75.400) and unguarded moving machine parts (56.14107) lead by far. Roof, face and rib
protection (75.202) is cited less often, but 71% of its citations are S&S, the highest share in the list; the quarterly
employment report (50.30) is cited often and never S&S.

| cfr_section | heading | citations | ss_share | proposed_penalty_musd | last_cited | text_begins |
|---|---|---:|---:|---:|---|---|
| 75.400 | Accumulation of combustible materials. | 148,461 | 0.265 | 155.8 | 2026-11-20 | Coal dust, including float coal dust deposited on rock-dusted surfaces, loose coal, and other combustible materials, shall be cleaned up and |
| 56.14107 | Moving machine parts. | 121,751 | 0.308 | 47 | 2026-11-19 | (a) Moving machine parts shall be guarded to protect persons from contacting gears, sprockets, chains, drive, head, tail, and takeup pulleys |
| 56.14100 | Safety defects; examination, correction and records. | 105,754 | 0.22 | 41.4 | 2026-11-20 | (a) Self-propelled mobile equipment to be used during a shift shall be inspected by the equipment operator before being placed in operation |
| 56.14132 | Horns and backup alarms. | 71,687 | 0.184 | 16.4 | 2026-11-18 | (a) Manually-operated horns or other audible warning devices provided on self-propelled mobile equipment as a safety feature shall be mainta |
| 56.12004 | Electrical conductors. | 70,414 | 0.104 | 24.2 | 2026-11-19 | Electrical conductors shall be of a sufficient size and current-carrying capacity to ensure that a rise in temperature resulting from normal |
| 75.370 | Mine ventilation plan; submission and approval. | 69,910 | 0.27 | 81 | 2026-11-20 | (a)(1) The operator shall develop and follow a ventilation plan approved by the district manager. The plan shall be designed to control meth |
| 56.20003 | Housekeeping. | 67,561 | 0.384 | 72.2 | 2026-11-20 | At all mining operations— (a) Workplaces, passageways, storerooms, and service rooms shall be kept clean and orderly; (b) The floor of every |
| 75.503 | Permissible electric face equipment; maintenance. | 65,046 | 0.159 | 26.5 | 2026-11-20 | [Statutory Provisions] The operator of each coal mine shall maintain in permissible condition all electric face equipment required by §§ 75. |
| 56.14112 | Construction and maintenance of guards. | 57,931 | 0.242 | 24.3 | 2026-11-19 | (a) Guards shall be constructed and maintained to— (1) Withstand the vibration, shock, and wear to which they will be subjected during norma |
| 75.202 | Protection from falls of roof, face and ribs. | 54,394 | 0.709 | 89.5 | 2026-11-20 | (a) The roof, face and ribs of areas where persons work or travel shall be supported or otherwise controlled to protect persons from hazards |
| 50.30 | Preparation and submission of MSHA Form 7000-2—Quarterly Employment and Coal Production Report. | 47,111 | 0 | 5.3 | 2026-11-20 | (a) Each operator of a mine in which an individual worked during any day of a calendar quarter shall complete a MSHA Form 7000-2 in accordan |
| 75.333 | Ventilation controls. | 46,995 | 0.054 | 14.7 | 2026-11-20 | (a) For purposes of this section, “doors” include any door frames. (b) Permanent stoppings or other permanent ventilation control devices co |
| 77.1605 | Loading and haulage equipment; installations. | 45,675 | 0.322 | 14.4 | 2026-11-19 | (a) Cab windows shall be of safety glass or equivalent, in good condition and shall be kept clean. (b) Mobile equipment shall be equipped wi |
| 75.220 | Roof control plan. | 42,269 | 0.436 | 57.6 | 2026-11-20 | (a)(1) Each mine operator shall develop and follow a roof control plan, approved by the District Manager, that is suitable to the prevailing |
| 56.12032 | Inspection and cover plates. | 42,097 | 0.151 | 17.7 | 2026-11-19 | Inspection and cover plates on electrical equipment and junction boxes shall be kept in place at all times except during testing or repairs. |

## Which cited section numbers have no text in today's Title 30, and how often and how recently are they cited?

`queries/cited_sections_missing_from_ecfr.sql`. SafeStart links each citation to the regulation's text; a number missing
from the current CFR (a standard since removed or renumbered, or a typing error) needs another source or a correction
before it can link.

90 of the 103 numbers without text were last cited before 2025, as expected of standards since removed or renumbered
(75.383, 75.1101-23, 56.5050). Five are still cited in 2026, among them 70.206 and 70.207, which today's Title 30 holds
only as the reserved range 70.206-70.207: a link to the current text cannot explain those citations.

| cfr_section | citations | last_cited | ss_share |
|---|---:|---|---:|
| 75.383 | 1,524 | 2008-04-07 | 0.039 |
| 70.207 | 1,008 | 2026-06-15 | 0.049 |
| 75.1101-23 | 625 | 2005-01-13 | 0.075 |
| 56.5050 | 496 | 2002-01-08 | 0.145 |
| 70.220 | 421 | 2014-07-31 | 0.012 |
| 71.220 | 217 | 2014-05-01 | 0.014 |
| 75.1002-1 | 114 | 2009-04-22 | 0.211 |
| 57.5050 | 94 | 2001-03-05 | 0.149 |
| 70.206 | 68 | 2026-07-30 | 0.147 |
| 37.4 | 37 | 2018-10-22 | 0 |
| 56.10000 | 34 | 2023-03-02 | 0.029 |
| 71.210 | 33 | 2014-06-19 | 0 |
| 50.301 | 32 | 2023-08-01 | 0 |
| 56.20012 | 30 | 2022-05-21 | 0.167 |
| 70.501 | 30 | 2007-08-16 | 0.9 |

## How has the share of violations designated significant and substantial (S&S) changed, in coal and in metal and non-metal mines?

`queries/ss_share_trend.sql`. An S&S violation is one the inspector judged reasonably likely to cause a serious injury;
a falling share means fewer of the conditions cited are the dangerous kind.

The S&S share of coal violations fell from 41.6% in 2000 to 17.6% in 2026, and at metal and non-metal mines from 30.5%
to 17.5%; since about 2015 the two commodities move together.

| year | coal_violations | coal_ss_share | mnm_violations | mnm_ss_share |
|---:|---:|---:|---:|---:|
| 2000 | 55,829 | 0.416 | 60,933 | 0.305 |
| 2001 | 66,544 | 0.4 | 58,118 | 0.273 |
| 2002 | 56,218 | 0.38 | 48,184 | 0.255 |
| 2003 | 55,796 | 0.388 | 52,867 | 0.241 |
| 2004 | 62,865 | 0.408 | 56,210 | 0.235 |
| 2005 | 68,046 | 0.387 | 58,711 | 0.229 |
| 2006 | 76,264 | 0.386 | 62,371 | 0.23 |
| 2007 | 83,097 | 0.341 | 59,900 | 0.214 |
| 2008 | 105,520 | 0.314 | 66,718 | 0.206 |
| 2009 | 100,489 | 0.297 | 71,098 | 0.29 |
| 2010 | 95,270 | 0.316 | 73,690 | 0.314 |
| 2011 | 92,133 | 0.309 | 63,242 | 0.255 |
| 2012 | 77,958 | 0.271 | 60,064 | 0.251 |
| 2013 | 62,566 | 0.26 | 54,947 | 0.237 |
| 2014 | 61,962 | 0.246 | 58,487 | 0.237 |
| 2015 | 48,504 | 0.229 | 58,263 | 0.243 |
| 2016 | 39,937 | 0.208 | 56,398 | 0.224 |
| 2017 | 46,208 | 0.207 | 57,742 | 0.215 |
| 2018 | 46,217 | 0.206 | 50,602 | 0.203 |
| 2019 | 46,392 | 0.19 | 52,067 | 0.197 |
| 2020 | 35,366 | 0.177 | 41,785 | 0.18 |
| 2021 | 35,434 | 0.191 | 41,128 | 0.189 |
| 2022 | 42,223 | 0.197 | 45,059 | 0.185 |
| 2023 | 45,234 | 0.185 | 49,703 | 0.188 |
| 2024 | 44,646 | 0.181 | 49,153 | 0.173 |
| 2025 | 37,834 | 0.166 | 45,997 | 0.174 |
| 2026 | 31,927 | 0.176 | 41,846 | 0.175 |

## How many reportable injuries to mine employees occur per 200,000 hours worked (100 full-time workers for a year), by year, in coal and in metal and non-metal mines?

`queries/injury_rate_by_year.sql`. Unlike a count, the rate stays comparable as employment changes; it is the baseline
against which a single mine's rate in its scorecard is judged.

The coal injury rate fell from 7.22 to about 3 per 200,000 hours, and has stayed near 3 since 2018; metal and non-metal
fell from 4.59 to about 1.6. 2026 counts its first three quarters, the ones with reported hours.

| year | coal_injuries | coal_hours_m | coal_rate | mnm_injuries | mnm_hours_m | mnm_rate |
|---:|---:|---:|---:|---:|---:|---:|
| 2000 | 5,680 | 157.3 | 7.22 | 8,658 | 377.2 | 4.59 |
| 2001 | 5,417 | 168.4 | 6.43 | 7,756 | 362.4 | 4.28 |
| 2002 | 5,211 | 163.8 | 6.36 | 6,852 | 343.1 | 3.99 |
| 2003 | 4,287 | 157 | 5.46 | 6,132 | 337.2 | 3.64 |
| 2004 | 4,401 | 166.3 | 5.29 | 6,269 | 346.4 | 3.62 |
| 2005 | 4,453 | 181.3 | 4.91 | 6,539 | 356.5 | 3.67 |
| 2006 | 4,462 | 188.9 | 4.72 | 6,090 | 365.2 | 3.34 |
| 2007 | 4,234 | 186.4 | 4.54 | 5,821 | 365.2 | 3.19 |
| 2008 | 4,267 | 200.2 | 4.26 | 5,288 | 348 | 3.04 |
| 2009 | 3,924 | 195 | 4.02 | 3,955 | 290.3 | 2.72 |
| 2010 | 3,824 | 199.2 | 3.84 | 3,797 | 296.1 | 2.56 |
| 2011 | 3,998 | 215.1 | 3.72 | 3,775 | 308.8 | 2.45 |
| 2012 | 3,529 | 199.8 | 3.53 | 3,804 | 318.9 | 2.39 |
| 2013 | 3,187 | 181.9 | 3.5 | 3,774 | 322.5 | 2.34 |
| 2014 | 2,986 | 171.6 | 3.48 | 3,765 | 328.4 | 2.29 |
| 2015 | 2,407 | 146 | 3.3 | 3,529 | 327.6 | 2.15 |
| 2016 | 1,835 | 112.9 | 3.25 | 3,298 | 320.6 | 2.06 |
| 2017 | 2,135 | 121.1 | 3.53 | 3,088 | 326.1 | 1.89 |
| 2018 | 1,940 | 124 | 3.13 | 3,176 | 338.1 | 1.88 |
| 2019 | 1,928 | 121.2 | 3.18 | 3,169 | 341.6 | 1.86 |
| 2020 | 1,353 | 89.4 | 3.03 | 2,726 | 321.4 | 1.7 |
| 2021 | 1,395 | 88.3 | 3.16 | 2,915 | 325.5 | 1.79 |
| 2022 | 1,552 | 99.7 | 3.11 | 2,871 | 337.7 | 1.7 |
| 2023 | 1,575 | 104.7 | 3.01 | 2,935 | 347.6 | 1.69 |
| 2024 | 1,491 | 100.7 | 2.96 | 2,992 | 352.4 | 1.7 |
| 2025 | 1,423 | 94.8 | 3 | 2,774 | 351.4 | 1.58 |
| 2026 | 1,013 | 68.1 | 2.98 | 2,129 | 265.1 | 1.61 |

## What kinds of accidents killed miners in the last ten years of data, in coal and in metal and non-metal mines?

`queries/fatal_accident_types.sql`. Fatalities concentrate in a few classifications; those are the hazards a pre-shift
assessment can least afford to miss.

Powered haulage (109) and machinery (75) caused 184 of the 316 deaths of the last ten years, 58%; falls of face, rib or
highwall kill more coal miners (12) than any classification but those two.

| classification | coal_fatalities | mnm_fatalities | fatalities | share |
|---|---:|---:|---:|---:|
| POWERED HAULAGE | 37 | 72 | 109 | 0.345 |
| MACHINERY | 25 | 50 | 75 | 0.237 |
| SLIP OR FALL OF PERSON | 4 | 21 | 25 | 0.079 |
| FALLING/SLIDING/ROLLING MATERIALS | 4 | 14 | 18 | 0.057 |
| FALL OF FACE/RIB/PILLAR/SIDE/HIGHWALL | 12 | 5 | 17 | 0.054 |
| ELECTRICAL | 5 | 11 | 16 | 0.051 |
| FALL OF ROOF OR BACK | 9 | 7 | 16 | 0.051 |
| OTHER | 2 | 13 | 15 | 0.047 |
| HANDLING OF MATERIALS | 0 | 9 | 9 | 0.028 |
| HOISTING | 1 | 2 | 3 | 0.009 |
| EXPLODING VESSELS UNDER PRESSURE | 0 | 2 | 2 | 0.006 |
| EXPLOSIVES AND BREAKING AGENTS | 0 | 2 | 2 | 0.006 |
| FIRE | 1 | 1 | 2 | 0.006 |
| IGNITION OR EXPLOSION OF GAS OR DUST | 1 | 1 | 2 | 0.006 |
| INUNDATION | 2 | 0 | 2 | 0.006 |
| NONPOWERED HAULAGE | 0 | 2 | 2 | 0.006 |
| HANDTOOLS (NONPOWERED) | 0 | 1 | 1 | 0.003 |

## In underground coal mining, which kinds of accidents happen most, and how many of them are serious or fatal (last ten years of data)?

`queries/underground_coal_accident_classes.sql`. The accident classification is the hazard a pre-shift assessment has to
address; ranking by serious outcomes rather than counts alone shows where a missed control hurts people most.

Handling materials has the most accidents and the most serious ones; powered haulage causes the most deaths (27), and
falls of roof are frequent but mostly not serious (26%).

| classification | accidents | serious | fatal | serious_share |
|---|---:|---:|---:|---:|
| HANDLING OF MATERIALS | 4,758 | 3,481 | 0 | 0.732 |
| SLIP OR FALL OF PERSON | 1,946 | 1,696 | 0 | 0.872 |
| MACHINERY | 1,692 | 1,153 | 10 | 0.681 |
| POWERED HAULAGE | 1,191 | 971 | 27 | 0.815 |
| FALL OF ROOF OR BACK | 3,387 | 880 | 9 | 0.26 |
| HANDTOOLS (NONPOWERED) | 1,378 | 627 | 0 | 0.455 |
| FALL OF FACE/RIB/PILLAR/SIDE/HIGHWALL | 346 | 291 | 9 | 0.841 |
| STEPPING OR KNEELING ON OBJECT | 226 | 216 | 0 | 0.956 |
| DISORDERS (REPEATED TRAUMA) | 334 | 191 | 0 | 0.572 |
| OTHER | 218 | 172 | 1 | 0.789 |
| STRIKING OR BUMPING | 190 | 98 | 0 | 0.516 |
| EXPLODING VESSELS UNDER PRESSURE | 97 | 73 | 0 | 0.753 |
| ELECTRICAL | 92 | 68 | 3 | 0.739 |
| FALLING/SLIDING/ROLLING MATERIALS | 93 | 62 | 0 | 0.667 |
| NONPOWERED HAULAGE | 23 | 19 | 0 | 0.826 |
| ALL OTHER OCCUPATIONAL ILLNESSES | 18 | 14 | 0 | 0.778 |
| DISORDERS (PHYSICAL AGENTS) | 17 | 8 | 0 | 0.471 |
| OCCUPATIONAL SKIN DISEASES | 9 | 7 | 0 | 0.778 |
| HOISTING | 379 | 5 | 0 | 0.013 |
| FIRE | 39 | 5 | 0 | 0.128 |
| DUST DISEASE OF LUNGS | 443 | 4 | 0 | 0.009 |
| IGNITION OR EXPLOSION OF GAS OR DUST | 101 | 3 | 0 | 0.03 |
| INUNDATION | 73 | 3 | 1 | 0.041 |
| POISONING (TOXIC MATERIALS) | 4 | 2 | 0 | 0.5 |
| EXPLOSIVES AND BREAKING AGENTS | 2 | 1 | 0 | 0.5 |
| ENTRAPMENT | 76 | 0 | 0 | 0 |
| IMPOUNDMENT | 1 | 0 | 0 | 0 |

## What penalty does MSHA propose per S&S violation in each district, over the last three years of data?

`queries/penalty_per_ss_by_district.sql`. The proposed penalty is how enforcement prices a hazard; a district whose S&S
citations cost far more or less than the others shows where the same designation weighs differently on an operator.

An S&S violation's proposed penalty averages $3,714 in coal district C04, over three times the $1,134 of coal district
C07. Every median is far below its mean: a few large penalties raise the averages.

| district | ss_violations | assessed | proposed_musd | penalty_per_ss | median_penalty |
|---|---:|---:|---:|---:|---:|
| C04 | 6,509 | 6,164 | 22.89 | 3,714 | 1,773 |
| C09 | 2,021 | 1,937 | 6.61 | 3,415 | 1,188 |
| C08 | 3,203 | 3,023 | 8.81 | 2,915 | 1,320 |
| M3 | 4,760 | 4,495 | 12.55 | 2,791 | 1,550 |
| M4 | 3,012 | 2,876 | 7.27 | 2,530 | 504 |
| M5 | 5,553 | 5,240 | 13.22 | 2,524 | 797 |
| C03 | 2,298 | 2,195 | 5.22 | 2,379 | 1,188 |
| M6 | 3,227 | 3,020 | 7.05 | 2,334 | 797 |
| M7 | 3,169 | 3,011 | 6.58 | 2,187 | 679 |
| C05 | 2,636 | 2,522 | 5.37 | 2,130 | 861 |
| C10 | 1,873 | 1,764 | 2.78 | 1,578 | 861 |
| M2 | 4,883 | 4,569 | 6.24 | 1,365 | 366 |
| C07 | 2,229 | 2,125 | 2.41 | 1,134 | 504 |

## Which active mines had the most S&S violations in the last 12 months, and what are their serious accidents and injury rates?

`queries/riskiest_active_mines.sql`. This is the scorecard SafeStart shows before a shift; where S&S citations and
injuries are both high, a thorough pre-shift assessment matters most.

All fifteen are underground coal mines. Marshall County Mine (WV) combines 154 S&S violations with 151 serious accidents
and an injury rate of 13.85, more than four times the coal average of about 3; No 7 Mine (AL) has the most S&S
violations but a rate of 1.5.

| mine_id | mine_name | state | coal_metal | mine_type | inspections_12m | violations_12m | ss_violations_12m | serious_accidents_12m | injury_rate_12m | top_sections |
|---|---|---|---|---|---:|---:|---:|---:|---:|---|
| 0101401 | No 7 Mine | AL | C | Underground | 108 | 727 | 233 | 8 | 1.5 | 75.400, 75.202, 75.220 |
| 0100851 | Oak Grove Mine | AL | C | Underground | 133 | 788 | 209 | 46 | 8.9 | 75.400, 75.220, 75.202 |
| 1517216 | CARDINAL | KY | C | Underground | 99 | 613 | 165 | 29 | 6.32 | 75.220, 75.370, 75.517 |
| 4601437 | MARSHALL COUNTY MINE | WV | C | Underground | 160 | 1,074 | 154 | 151 | 13.85 | 75.400, 75.333, 75.202 |
| 4609522 | ROAD FORK #52 MINE | WV | C | Underground | 114 | 800 | 153 | 13 | 4.73 | 75.370, 75.400, 75.342 |
| 0102901 | Shoal Creek Mine | AL | C | Underground | 96 | 497 | 146 | 4 | 1.46 | 75.400, 75.202, 75.333 |
| 0101247 | No 4 Mine | AL | C | Underground | 96 | 510 | 143 | 9 | 2.1 | 75.202, 75.400, 75.220 |
| 4609029 | Mountaineer II Mine | WV | C | Underground | 99 | 613 | 122 | 8 | 2.5 | 75.400, 75.512, 75.370 |
| 4601433 | Marion County Mine | WV | C | Underground | 154 | 552 | 122 | 56 | 8.75 | 75.400, 75.1403, 75.202 |
| 1103147 | Prairie Eagle-Underground | IL | C | Underground | 51 | 498 | 107 | 13 | 6.74 | 75.503, 75.400, 75.220 |
| 1103189 | MC#1 Mine | IL | C | Underground | 84 | 389 | 98 | 14 | 6.88 | 75.400, 75.370, 75.1731 |
| 4404856 | Buchanan Mine #1 | VA | C | Underground | 93 | 497 | 94 | 20 | 3.12 | 75.400, 75.370, 75.403 |
| 4605437 | Speed Mine | WV | C | Underground | 90 | 344 | 94 | 3 | 1.48 | 75.370, 75.400, 75.202 |
| 0103448 | Blue Creek Mine No. 1 | AL | C | Underground | 89 | 288 | 94 | 7 | 1.88 | 75.202, 75.400, 75.220 |
| 1502709 | Henderson County Mine | KY | C | Underground | 12 | 366 | 87 | 33 | 9.76 | 75.517, 75.503, 75.604 |

## Which mines' violation counts rose the most in the last 12 months compared with the 12 months before?

`queries/steepest_violation_rise.sql`. A sharp rise flags a mine whose conditions are getting worse now, before it shows
in its accident record; a crew starting work there should know.

Marshall County Mine also had the steepest rise, 320 more violations than the year before; Mesabi Metallics (MN) went
from 13 to 112.

| mine_id | mine_name | state | coal_metal | mine_type | status | prior_12m | last_12m | rise | ratio |
|---|---|---|---|---|---|---:|---:|---:|---:|
| 4601437 | MARSHALL COUNTY MINE | WV | C | Underground | Active | 754 | 1,074 | 320 | 1.4 |
| 4609029 | Mountaineer II Mine | WV | C | Underground | Active | 410 | 613 | 203 | 1.5 |
| 0100851 | Oak Grove Mine | AL | C | Underground | Active | 595 | 788 | 193 | 1.3 |
| 1103147 | Prairie Eagle-Underground | IL | C | Underground | Active | 318 | 498 | 180 | 1.6 |
| 4605252 | BECKLEY POCAHONTAS MINE | WV | C | Underground | Active | 330 | 510 | 180 | 1.5 |
| 1502709 | Henderson County Mine | KY | C | Underground | Active | 216 | 366 | 150 | 1.7 |
| 4404856 | Buchanan Mine #1 | VA | C | Underground | Active | 352 | 497 | 145 | 1.4 |
| 4609611 | Cedar Grove Mine No 3 | WV | C | Underground | Active | 71 | 198 | 127 | 2.8 |
| 2302171 | Sugar Creek Ug Mine | MO | M | Underground | Active | 99 | 219 | 120 | 2.2 |
| 4100283 | MARYNEAL QUARRY AND MILL | TX | M | Facility | Active | 187 | 306 | 119 | 1.6 |
| 1600352 | Gramercy Operation | LA | M | Facility | Active | 161 | 274 | 113 | 1.7 |
| 1000082 | Galena | ID | M | Underground | Active | 81 | 191 | 110 | 2.4 |
| 2602246 | Meikle Mine | NV | M | Underground | Active | 132 | 236 | 104 | 1.8 |
| 4601318 | Harrison County Mine | WV | C | Underground | Active | 254 | 355 | 101 | 1.4 |
| 2103751 | Mesabi Metallics Company LLC | MN | M | Surface | Active | 13 | 112 | 99 | 8.6 |

## What does the web corpus collected from the OSHA and MSHA websites consist of, by site and kind of document?

`queries/web_corpus_by_site_kind.sql`. SafeStart's library is built from these pages and files; how much is HTML, PDF,
office files or data decides which parsers and how much indexing the library needs, and the skipped count shows what was
left out.

MSHA's legacy host is mostly PDF by size (8,384 files, 25.8 GB). OSHA's skipped URLs (9,806 pages, 783 PDFs) are what
its crawl rules leave out on purpose (README, Crawling rules); its pages are 59% in the sitemap, MSHA's 73%.

| site | kind | urls | downloaded | downloaded_gb | skipped | sitemap_share |
|---|---|---:|---:|---:|---:|---:|
| arlweb | pdf | 8,639 | 8,384 | 25.81 | 0 | 0 |
| arlweb | page | 4,167 | 3,034 | 0.09 | 56 | 0 |
| arlweb | data | 663 | 594 | 0.21 | 61 | 0 |
| arlweb | office | 347 | 345 | 0.84 | 0 | 0 |
| arlweb | other | 43 | 37 | 1.3 | 0 | 0 |
| msha | page | 28,527 | 28,232 | 2.6 | 107 | 0.733 |
| msha | pdf | 3,712 | 3,706 | 3.11 | 0 | 0 |
| msha | other | 232 | 231 | 0.14 | 0 | 0 |
| msha | office | 31 | 31 | 0 | 0 | 0 |
| msha | data | 5 | 5 | 0.02 | 0 | 0 |
| osha | page | 46,554 | 34,168 | 3.63 | 9,806 | 0.587 |
| osha | pdf | 8,156 | 6,446 | 4.86 | 783 | 0.53 |
| osha | office | 700 | 345 | 0.81 | 347 | 0 |
| osha | data | 192 | 188 | 1.67 | 4 | 0 |
| osha | other | 502 | 10 | 0.01 | 492 | 0 |
