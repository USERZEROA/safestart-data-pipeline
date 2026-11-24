# Mine accident-risk model

`src/safestart_pipeline/ml/` predicts, for every mine that has been working, the probability of a serious accident in
the coming quarter, from what MSHA's open data showed about the mine in the year before. It also writes a short analysis
of the lake for these docs.

```bash
uv run python -m safestart_pipeline.ml train     # features, both models, metrics, importances, scores
uv run python -m safestart_pipeline.ml analyze   # the lake tables below, as CSV and Markdown
```

Both read the current version of each dataset in `<data root>/lake/msha_ogd` (`--lake` names another lake) and write to
`<data root>/models/accident_risk` and `<data root>/analysis` (`--out` names another folder), so a run can read a
shared data root it must not change.

## Problem

A serious accident is one whose degree of injury is 01 to 05: a death, a permanent disability, or days away from work
or of restricted work. It is the rule every component of the pipeline shares (`src/safestart_pipeline/definitions.py`),
so the model predicts what the warehouse, the stream and the dashboard count. The question is one an inspection program
or an operator asks at the start of each quarter: which mines are most likely to have one in the next three months? The
model gives each mine a probability; the riskiest 5 percent is the list one could act on.

## Data

The current versions of five lake datasets (`metrics.json` records the exact Parquet files of every run):

| Dataset | Rows | Used for |
|---|---|---|
| `mines` | 92,129 | coal/metal flag, commodity, mine type, state, district (and the name and status shown with the scores) |
| `inspections` | 1,165,877 | inspections begun |
| `violations` | 3,115,856 | citations issued, S&S citations, sections cited, proposed penalties |
| `accidents` | 276,013 | accidents and serious accidents (features and target) |
| `mines_prod_quarterly` | 2,784,528 | hours worked and average employees, per quarter and subunit (and whether a mine's newest report is in, shown with the scores) |

Operators' quarterly reports run from 2000Q1 to 2026Q3, the accidents to 2026-11-16 and the citations to 2026-11-21.

MSHA lists a few records twice: 51 violations of 2000 to 2010 once per contest docket, and inspection 4165469 of 2001
once per controller; the copies differ only in the docket or the controller and operator. Like the warehouse, the
model and the analysis read one copy of each (`features.scan`), so they count 3,115,805 violations and 1,165,876
inspections, and the analysis tables below equal the warehouse's marts.

## The panel and its features

One row per mine and calendar quarter `t` (`features.py`, a lazy Polars plan from the Parquet files). Every count and
amount covers only the four quarters before `t`, the window `t-4` to `t-1`; the year and season are `t`'s own, known in
advance, and the attributes are the mine's current values (Limits):

| Feature | Meaning |
|---|---|
| `inspections_4q` | inspections and investigations begun |
| `violations_4q`, `ss_violations_4q` | citations and orders issued; those significant and substantial by the shared mapping (`SIG_SUB`) |
| `proposed_penalty_4q` | dollars MSHA proposed, counted in the quarter the bill was printed |
| `cited_sections_4q` | distinct sections cited, read by the shared section rule (`56.14107(a)` is 56.14107, `75.1100-2(b)` is 75.1100-2) |
| `accidents_4q`, `serious_accidents_4q` | accident reports at the mine, and the serious ones |
| `hours_worked_4q`, `avg_employees_4q` | operator hours worked, and average employees per quarter |
| `hours_worked_1q`, `active_quarters_4q` | hours in the latest quarter, and quarters with any hours (is the mine still working?) |
| `year`, `calendar_quarter` | the target quarter's year (a trend term) and its quarter of the year (seasons) |
| `coal_metal`, `commodity`, `mine_type`, `state`, `district` | the mine's attributes: its coal or metal/nonmetal flag, MSHA's primary canvass (coal, sand and gravel, stone, metal, nonmetal), type, state and district |

The target is whether the mine had at least one serious accident in `t`, read from the degree code alone
(`definitions.SERIOUS_DEGREES`). The day counts play no part: MSHA fills them in once the miner is back at work (on
2026-11-23, 1,016 of 2026's 2,940 accidents of degree 03 to 05 had none yet), and they also appear on occupational
illnesses, natural-cause and other cases (degrees 07 to 10) that are not injuries.

The coal/metal flag follows the shared commodity rule: the mine's flag in the mines dataset rather than the flags on
its records, which disagree with it on 89,673 citations and 784 quarterly reports. A mine missing from the mines
dataset would take the flag of its newest production report, the records that put it in the population; none of
today's panel mines is missing there.

The population of quarter `t` is every mine that reported hours worked in at least one of the four prior quarters. It
covers 99.0 percent of the mine-quarters with a serious accident from 2001 to 2026Q3 (82,288 of 83,096); the rest are
new or reopened mines and sites without production reports.

The panel has 1,434,710 mine-quarters, from 2001Q1 (the first quarter with four quarters of reports before it) to
2026Q4, the open quarter after the newest reports, whose 12,722 rows have no target yet and are the ones scored. The
share of mine-quarters with a serious accident fell from 8.9 percent in 2001 to between 4.0 and 4.2 percent since 2020.

## Leakage guards

- **The window never includes its own quarter.** A source row of quarter `s` is spread to the target quarters `s+1` to
  `s+4` only. A test adds inspections, citations, a fatality and new production reports at and after a quarter and
  checks that no feature of that quarter or any earlier one changes.
- **Dated before `t`, but read from today's record.** Every feature counts what happened before `t`: inspections,
  citations and accidents by their dates, hours by the quarter worked, and proposed penalties by the quarter MSHA
  billed them (a median of 49 days after the citation in 2015 to 2024) rather than by the citation date, since a
  penalty not yet proposed was not known. The values, though, are the lake's current record, not what MSHA held when a
  score for `t` is made, 15 days into `t` once the quarterly reports of hours are due (30 CFR 50.30; accidents are due
  within ten working days, 30 CFR 50.20). That record keeps changing: the lake version of 2026-11-23 added 140
  quarterly reports for past quarters (109 for 2026Q3, with 188,887 hours) and changed the hours of 10, added 9
  accidents dated in earlier quarters (the oldest 2024-11-02) and changed the degree of injury of 34, the oldest dated
  2021-03-03 (three of them across the line between serious and not). Training and test quarters are built from this
  eventually complete history, so the test results below are an optimistic estimate for a score made at the time; the
  late quarterly reports matter most, since hours worked carry the most weight (see Limits).
- **No attribute that records the outcome.** The mine's current status is left out (a mine abandoned after a fatal
  accident would give its own past away); it appears only next to the scores.
- **A population fixed from the past.** Membership depends on the prior four quarters only, so the model is trained on
  the population it later scores, not on the mines that turned out to work in `t`.
- **A split by time.** Models learn from 2001 to 2021, are compared on 2022 and tested on 2023 and 2024; each mine
  appears in all three, but every evaluated quarter is later than every quarter learned from.

## Split and models

| Part | Target quarters | Mine-quarters | With a serious accident |
|---|---|---|---|
| train | 2001Q1 to 2021Q4 | 1,180,808 | 72,284 (6.1%) |
| validate | 2022Q1 to 2022Q4 | 50,682 | 2,120 (4.2%) |
| test | 2023Q1 to 2024Q4 | 101,641 | 4,286 (4.2%) |

2025Q1 to 2026Q3 are used only in the final refit.

- **Baseline**: logistic regression on log-scaled and standardized counts, the standardized year and one-hot
  categories.
- **Histogram gradient boosting** (`HistGradientBoostingClassifier`) with native categories, up to 500 iterations at a
  learning rate of 0.1, at least 200 rows per leaf, L2 regularization 1.0 and early stopping on a tenth of the training
  rows.

The model with the higher validation PR AUC is refitted with the same settings on all 1,421,988 labeled mine-quarters
(2001Q1 to 2026Q3) and scores 2026Q4. The test results below are those of the models fitted through 2021.

## Results on the real data

The results, the calibration and the importances below are this run's, as its own files record them:
[`docs/ml/metrics.json`](ml/metrics.json) and [`docs/ml/permutation_importance.csv`](ml/permutation_importance.csv).
The published run, below, says when and from what they were made and how to reproduce them.

Test quarters 2023 and 2024 (base rate 4.22 percent; the riskiest 5 percent is 5,083 mine-quarters):

| Model | ROC AUC | PR AUC | Precision, top 5% | Recall, top 5% | Brier | Mean predicted |
|---|---|---|---|---|---|---|
| Logistic regression | 0.9062 | 0.4764 | 0.4269 | 0.5063 | 0.02928 | 0.03835 |
| Gradient boosting | **0.9089** | **0.4784** | **0.4293** | **0.5091** | **0.02913** | 0.04417 |

Gradient boosting was selected on validation (PR AUC 0.4645 against 0.4569; ROC AUC 0.9056 against 0.9024). Among
the 5 percent of mine-quarters it ranks riskiest, 43 percent had a serious accident, ten times the base rate, and they
hold half of all the quarters with one. The baseline is close behind: most of the signal is in a few monotone
features that a linear model on log scales captures well.

Calibration of the selected model on the test quarters, by decile of predicted risk:

| Decile | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 |
|---|---|---|---|---|---|---|---|---|---|---|
| Predicted | 0.0004 | 0.0011 | 0.0021 | 0.0036 | 0.0062 | 0.0108 | 0.0190 | 0.0332 | 0.0665 | 0.2989 |
| Observed | 0.0007 | 0.0014 | 0.0018 | 0.0040 | 0.0067 | 0.0092 | 0.0177 | 0.0328 | 0.0625 | 0.2849 |

The year term is what keeps the levels right. Without it the same booster predicted a mean of 0.0494 on the test
quarters against 0.0422 observed (17 percent too high, because it averaged over the higher accident rates of the
2000s); with it the mean is 0.0442, and the validation PR AUC rose from 0.4615 to 0.4645. It does not improve the
order: without it the test PR AUC was 0.4789 against 0.4784.

Permutation importance, the drop in test PR AUC when one feature is shuffled (five shuffles;
[`permutation_importance.csv`](ml/permutation_importance.csv) has every feature, both models and the ROC AUC drop):

| Gradient boosting | PR AUC drop | Logistic regression | PR AUC drop |
|---|---|---|---|
| `hours_worked_1q` | 0.364 | `hours_worked_4q` | 0.191 |
| `accidents_4q` | 0.030 | `hours_worked_1q` | 0.080 |
| `serious_accidents_4q` | 0.019 | `serious_accidents_4q` | 0.021 |
| `hours_worked_4q` | 0.017 | `active_quarters_4q` | 0.014 |
| `avg_employees_4q` | 0.009 | `cited_sections_4q` | 0.013 |

Exposure dominates: a mine that works more hours has more chances of a serious accident. Past accidents come next.
Citations add little once hours and accidents are known, and the year term scores zero here because both test years lie
beyond the training years and fall in the same bin; it shifts the level, not the order within a period.

The 12,722 scores for 2026Q4 sum to 457 expected mines with a serious accident (2026's first three quarters had 516 a
quarter). The riskiest are large underground coal mines and the largest surface metal mines. 880 of the scores carry
`newest_report_missing`: those mines have no report for 2026Q3, so they look idle (Limits).

Run time and peak memory of `train` on the real lake with two threads (`POLARS_MAX_THREADS=2 OMP_NUM_THREADS=2`), as
the orchestrator runs it. On this workstation (Windows, 24 logical CPUs) the peak is the process's peak working set;
in the pipeline image (`safestart-pipeline:local`, Linux, run in a throwaway container with the lake mounted
read-only) it is the peak resident set (`ru_maxrss`), the figure the orchestrator's memory limit is sized by:

| Run | Where | Panel | Fits, evaluation, importances, refit and scores | Total | Peak memory |
|---|---|---|---|---|---|
| this run | workstation | 5.2 s | 153.1 s | 158.4 s | 2,166 MiB |
| this run, repeated | workstation | 5.3 s | 155.7 s | 161.1 s | 2,315 MiB |
| this code, two runs | image | 5.4, 5.5 s | 107.1, 109.6 s | 112.7, 115.2 s | 1,345, 1,339 MiB |
| previous run, and repeated | workstation | 3.7, 4.0 s | 140.9, 142.8 s | 144.6, 146.8 s | 2,034, 1,789 MiB |
| previous code, two runs | image | 3.6, 3.6 s | 98.1, 96.9 s | 101.7, 100.5 s | 1,345, 1,380 MiB |
| an earlier run, 24 threads | workstation | 1.4 s | 67.8 s | 69.2 s | 3,061 MiB |

Every run of a version of the code gave the same metrics and importances, on either system. In the image the peak
stayed where it was, 1,339 to 1,380 MiB (1.31 to 1.35 GiB); with Prefect and its local server beside it (about 400
MiB, docs/DEPLOYMENT.md), the largest comes to about 1,780 MiB of the orchestrator's 1,920 MiB limit. The workstation's
working set is higher and varies more between runs of the same code; there the pass that finds the repeated keys
(Data, above) lifted the panel build's own peak from 1,540-1,600 to 1,870-1,990 MiB in two runs of each, while in the
image it costs nothing measurable. The peak grows with the thread count because Polars and the booster keep buffers per
thread; the panel itself is 132 MiB. `analyze` writes its tables in about 2 seconds.

## What changed since the previous run

- **One copy of each record.** The previous run counted both copies of the 51 violations and the inspection MSHA
  lists twice (Data, above). Reading one copy changes 67 mine-quarters of 12 mines, all of them target quarters from
  2001 to 2011, so training rows only: their citations, S&S citations, inspections and penalties are lower, while the
  cited sections and the labels stay as they were. The boosters learn from those slightly different rows, and early
  stopping now ends the one trained through 2021 at 244 iterations instead of 216 and the final refit at 221 instead
  of 167. No validation or test row changed; the test results moved by at most 0.0012 (below), gradient boosting is
  still selected, and the 2026Q4 scores keep their order (a rank correlation of 0.997 with the previous scores; no
  score moved by more than 0.08). In the analysis tables 12 of the 54 S&S cells lost 51 citations of 2000 to 2010 in
  all (2002's coal citations went from 56,238 to 56,213), and two shares moved in the fourth decimal (2002 coal 0.3803
  to 0.3804, 2009 metal/nonmetal 0.2900 to 0.2901); 13 of the 25 most cited sections lost one to four citations
  (75.400 from 148,462 to 148,461, 75.370 from 69,914 to 69,910), and 75.333's penalties fell from $14,737,770 to
  $14,722,200. The tables now equal the warehouse's marts.
- **S&S by the shared mapping.** The features and the tables counted a violation as S&S when its flag was `Y`; they
  now read `definitions.SIG_SUB`, as the warehouse and the stream do. The lake's flags are `Y`, `N` or empty, so no
  count moved.
- **Scores without the newest report.** `risk_scores.parquet` gains `newest_report_missing` (Limits, below).

| Gradient boosting, test quarters | Previous run | This run |
|---|---|---|
| ROC AUC | 0.9091 | 0.9089 |
| PR AUC | 0.4786 | 0.4784 |
| Precision, top 5% | 0.4283 | 0.4293 |
| Recall, top 5% | 0.5079 | 0.5091 |
| Validation PR AUC | 0.4650 | 0.4645 |

The logistic regression's test results moved by 0.0002 at most. The run took 14 s longer on the workstation (11 to 15
s in the image): 1.5 s for the passes that find the repeated keys, the rest for the boosters' extra iterations, which
every permutation pass and the refit go through.

### The run before: the shared rules

That run took three rules from `definitions.py`, which states each rule once for every component, instead of
stating them in the model and the analysis:

- **Serious accident.** The target before it also counted any accident with lost or restricted days. 4,081 records
  counted only that way (2.5 percent of the 161,738 it called serious) are no longer serious: 3,542 occupational
  illnesses (degree 07), 430 natural-cause cases (08), 47 injuries to non-employees (09), 6 other cases (10) and 56
  without a degree. The labeled mine-quarters with a serious accident fell from 83,550 to 82,288; in the test quarters
  96 of 4,382 lost their label and none gained one.
- **Cited section.** The rule before it cut the citation at its first parenthesis. The shared pattern regroups 262 of
  the 3.1 million citations (`75.1107-16-b` is now 75.1107-16, `77.403-ag` 77.403, `56.5001/5(a)` 56.5001), and the
  distinct sections cited fall from 1,800 to 1,789; `cited_sections_4q` drops by one in 252 of the 1.4 million
  mine-quarters.
- **Coal/metal flag.** The panel already took it from the mines dataset, and every panel mine is there, so the feature
  was unchanged. The analysis tables used each record's own flag; they have used the mine's since (see below).

The selected model's test results moved from ROC AUC 0.9098, PR AUC 0.4868 and precision 0.4401 among the riskiest 5
percent to 0.9091, 0.4786 and 0.4283. The change was in the labels, not in how well the model ranks: that run's test
scores, checked against the labels before it, came within 0.003 of the model before it on every measure.

| Gradient boosting, test quarters | Run before | Shared rules | Their scores, the labels before |
|---|---|---|---|
| Positives (base rate) | 4,382 (4.31%) | 4,286 (4.22%) | 4,382 (4.31%) |
| ROC AUC | 0.9098 | 0.9091 | 0.9094 |
| PR AUC | 0.4868 | 0.4786 | 0.4858 |
| Precision, top 5% | 0.4401 | 0.4283 | 0.4379 |
| Recall, top 5% | 0.5105 | 0.5079 | 0.5080 |

49 of the 96 relabeled mine-quarters were among the 5,083 the model ranked riskiest, so the precision there fell by
about 0.01 with the base rate, and the lift stayed at ten times the base rate. The baseline moved the same way (PR AUC
0.4838 to 0.4764), gradient boosting was still selected, and the calibration and the importances kept their shape.

## Outputs (`<data root>/models/accident_risk/`)

| File | Content |
|---|---|
| `model.joblib` | `{"model", "features", "selected", "fitted_on"}`: the refitted scikit-learn pipeline, its input columns and the quarters it learned from |
| `metrics.json` | the split, both models' validation and test metrics with calibration deciles, the selection, the run record (lake files, panel rows, seconds, peak memory) |
| `permutation_importance.csv` | `model, feature, pr_auc_drop, pr_auc_drop_std, roc_auc_drop` |
| `risk_scores.parquet` | the open quarter: `mine_id, mine_name, status, period, risk_score, risk_rank`, every feature behind the score and `newest_report_missing` (no report for the newest quarter yet, so `hours_worked_1q` is 0) |

## The published run (`docs/ml/`)

[`docs/ml/metrics.json`](ml/metrics.json) and [`docs/ml/permutation_importance.csv`](ml/permutation_importance.csv) are
this run's `metrics.json` and `permutation_importance.csv`, copied with no value changed, so that its figures can be
checked without the data root; the model and the scores stay there. Neither file holds a local path: the run record
names the lake's files without their folder.

- **When.** `created_at` is 2026-11-23T20:27:15Z, the run in the first row of the run-time table above (`run`:
  `cpu_count` 24, `panel_seconds` 5.2, `train_seconds` 153.1, `peak_memory_mb` 2166.2).
- **Which build.** `run.lake` names the Parquet file of each dataset's current version in the lake version of
  2026-11-23: MSHA's files of that day (Last-Modified 12:50 GMT), built into the lake the same day. A lake file is
  named by the first 16 hex digits of the SHA-256 of the MSHA zip it was built from, so the names identify the
  downloads. The code is that of commit `f06d15e` ("Count each repeated MSHA key once in the model and analysis, and
  retrain"; What changed since the previous run, above), committed with the run's results 25 minutes after it; the
  packages are those `uv.lock` pins (scikit-learn 1.9.1, Polars 2.1.0, NumPy 2.5.5).
- **How to reproduce.** `train` builds the panel (`features.py`) and fits, tests, refits and writes (`train.py`), both
  in `src/safestart_pipeline/ml/`; `ssp ml` hands its arguments to `python -m safestart_pipeline.ml`. With the
  orchestrator's two threads, and `--out` so that the data root's own files stay as they are:

  ```bash
  uv sync --all-extras
  POLARS_MAX_THREADS=2 OMP_NUM_THREADS=2 uv run ssp ml train --out <folder>
  ```

  It reads each dataset's current version, so it gives these figures while the lake's current files are the five
  that `run.lake` names, and a later version's own figures after that. Without a lake,
  `uv run ssp pipeline --model` downloads MSHA's current files, builds the lake and trains
  ([Quick start](DOCUMENTATION.md#quick-start)). A rerun of this code on these files (2026-11-24 00:53 UTC,
  workstation, two threads) wrote both files again with the same values, apart from `created_at`, the timings and the
  peak memory.

## The lake in three tables (`analysis.py`)

`analyze` writes `ss_share_by_year.csv`, `injury_rate_by_year.csv`, `top_cited_sections.csv` and all three in
`tables.md`, by the shared rules: injuries are records of degree 01 to 06 and serious injuries those of degree 01 to 05,
a violation is S&S by `SIG_SUB`, a citation's section is the shared pattern's match (a citation naming no section is
left out), and every record counts once, under its mine's coal/metal flag in the mines dataset (its own flag only for a
mine missing there). Every cell from 2000 on equals the warehouse's marts (`mart_ss_trend`, `mart_injury_rate`,
`mart_top_cited_sections`). Injuries are those of the operator's own employees, over the operator's hours (the
quarterly reports hold no contractor hours per mine); 200,000 hours is 100 full-time workers for a year. 2026 covers
the quarters reported so far.

| Year | S&S share, coal | S&S share, metal/nonmetal | Injuries per 200,000 h, coal | metal/nonmetal | Serious, coal | metal/nonmetal |
|---|---|---|---|---|---|---|
| 2000 | 41.6% | 30.5% | 7.22 | 4.59 | 5.41 | 2.95 |
| 2005 | 38.7% | 22.9% | 4.91 | 3.67 | 3.40 | 2.44 |
| 2010 | 31.6% | 31.4% | 3.84 | 2.56 | 2.58 | 1.73 |
| 2015 | 22.9% | 24.3% | 3.30 | 2.15 | 2.30 | 1.48 |
| 2020 | 17.3% | 18.1% | 3.03 | 1.70 | 2.29 | 1.21 |
| 2024 | 18.8% | 17.0% | 2.96 | 1.70 | 2.20 | 1.21 |

Counting by the mine's flag moved the S&S shares from 2019 on, when thousands of citations a year began to carry a flag
other than their mine's (in 2020, 8,888 citations at metal/nonmetal mines carried the coal flag and 1,639 at coal mines
the metal flag, against 434 in all the years before 2019). By the records' own flags the 2024 shares were 18.1 and
17.3 percent; 2026's moved most, coal from 17.6 to 19.0 percent and metal/nonmetal from 17.5 to 16.7. Before 2019 no
share moved by more than about a hundredth of a point. The injury and serious-injury counts are unchanged (every
accident record agrees with its mine's flag, and no operator injury of degree 06 had day counts); the rates moved by at
most 0.01 where the hours of the 784 reports with the other flag changed sides.

The most cited sections of 30 CFR in the violations dataset, 2000 to November 2026 (headings from the eCFR title 30
XML). Penalties add up in whole cents, so a rerun writes the same file (float totals had made two sections' totals
differ by a dollar between runs).

| Section | Heading | Citations | S&S share | Proposed penalties |
|---|---|---|---|---|
| 75.400 | Accumulation of combustible materials | 148,461 | 26.5% | $155.8 million |
| 56.14107 | Moving machine parts | 121,751 | 30.8% | $47.0 million |
| 56.14100 | Safety defects; examination, correction and records | 105,754 | 22.0% | $41.4 million |
| 56.14132 | Horns and backup alarms | 71,687 | 18.4% | $16.4 million |
| 56.12004 | Electrical conductors | 70,414 | 10.4% | $24.2 million |
| 75.370 | Mine ventilation plan; submission and approval | 69,910 | 27.0% | $81.0 million |
| 56.20003 | Housekeeping | 67,561 | 38.4% | $72.2 million |
| 75.503 | Permissible electric face equipment; maintenance | 65,046 | 15.9% | $26.5 million |
| 56.14112 | Construction and maintenance of guards | 57,931 | 24.2% | $24.3 million |
| 75.202 | Protection from falls of roof, face and ribs | 54,394 | 70.9% | $89.5 million |

## Limits

- **The score is a probability, not a rate.** It grows with hours worked, so large mines lead the list even when their
  injury rate per hour is ordinary. Comparing mines of different sizes needs the observed rate against the predicted
  one, which this model does not report.
- **Current attributes.** The coal/metal flag, commodity, mine type, state and district are MSHA's values today; a mine
  whose type or district changed carries today's value back to 2001, and so would the newest report's flag of a mine
  missing from the mines dataset.
- **Records as they stand now.** A citation's S&S designation and penalty can change on contest, an accident's degree
  of injury is revised as the case develops, and reports and accidents arrive late; the lake holds the current record,
  not the one known at the time, so the test results overstate a score made at the time (Leakage guards).
- **Contractors.** Contractor accidents count in the features and the target (they happen at the mine), but contractor
  hours are not reported per mine, so the hours cover operator employees only.
- **Late reports.** A mine that has not yet filed the newest quarter's hours looks idle in `hours_worked_1q`, the
  feature the model leans on most, and scores lower until it does. Of the 12,722 mines scored for 2026Q4, 880 have no
  report for 2026Q3, and 501 of those reported hours for 2026Q2; for the fourth quarters of 2021 to 2025, whose reports
  are complete, the same counts were 572 to 652 and 243 to 312, so roughly 200 to 300 of today's are probably late
  rather than closed. `newest_report_missing` marks all 880 scores. Carrying the 2026Q2 hours forward would lift 5 of
  the 501 into the riskiest 5 percent: Fort Knox Mine (5001616), with 1.06 million hours in its three reported quarters
  and 3 serious accidents in the year, scores 0.068 (rank 1,487) and would score about 0.62 (rank about 83).
- **Who is not scored.** Mines without hours in the past four quarters (new or reopened mines) get no score; they hold
  about 1 percent of the serious-accident quarters.
- **Drift.** The year term carries the most recent level forward; a change in reporting or enforcement would need a
  refit, which a weekly run of `train` provides.
- **The definition of serious.** Following the shared rule, an illness, a natural-cause case or an injury to a
  non-employee is not serious even when it records lost days; such cases need a target of their own.
- **Precision.** Even among the riskiest 5 percent, more than half of the mine-quarters have no serious accident; the
  list says where to look, not what will happen.
