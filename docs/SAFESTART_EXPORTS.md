# SafeStart exports

The pipeline hands SafeStart, the pre-shift hazard assessment (JSA) application, a small set of files instead of its
warehouse: SafeStart needs no DuckDB and none of the warehouse's model, only the facts it shows a crew. This document is
the contract for those files.

```bash
python -m safestart_pipeline.warehouse build    # the warehouse (docs/ERD.md)
python -m safestart_pipeline.warehouse export   # <data root>/exports/safestart/, or --out <folder>
```

The export reads the warehouse read-only and writes three files into one folder. Each Parquet file is written under a
temporary name and renamed into place, and `manifest.json` is written last, so a manifest always describes complete
files. An importer reads the manifest first, checks each file's `sha256`, and refuses a `format` it does not know.

On 2026-11-23 the export took under a second: 92,129 mine profiles (1.5 MB) and 1,789 cited sections (0.4 MB).

The figures count by the definitions every pipeline component shares (`definitions.py`, docs/ERD.md), so a number in
SafeStart matches the same number on the pipeline's dashboard: a serious accident is degree of injury 01 to 05, an
injury is degree 01 to 06, a cited section is the section without its paragraph, and a mine's violations, accidents
and hours all count under the mine's own commodity.

## mine_profiles.parquet

One row per mine MSHA has registered, from the warehouse's `mart_mine_scorecard`. "12m" is the 12 months up to the
newest violation in the data (`window_12m` in the manifest), not the export date.

| Column | Type | Meaning |
|---|---|---|
| `mine_id` | VARCHAR | MSHA's seven-digit mine ID, the key SafeStart stores for a site |
| `mine_name`, `state`, `district` | VARCHAR | the mine's current name, state and MSHA district |
| `coal_metal` | VARCHAR | `C` coal, `M` metal and non-metal |
| `mine_type`, `status` | VARCHAR | `Facility`, `Surface` or `Underground`; `Active`, `Intermittent`, `Abandoned` ... |
| `latitude`, `longitude` | DOUBLE | the mine's location, null when MSHA's is missing or implausible |
| `inspections_12m` | BIGINT | inspections begun in the 12 months |
| `violations_12m`, `ss_violations_12m` | BIGINT | citations and orders issued, and those designated significant and substantial |
| `proposed_penalty_12m` | DOUBLE | penalties proposed for them, in dollars (recent ones may not be assessed yet) |
| `accidents_12m`, `serious_accidents_12m` | BIGINT | accident, injury and illness reports; the serious ones: a death, a permanent disability, or days away from work or restricted (degree 01 to 05), whatever the day counts |
| `hours_worked_12m` | DOUBLE | the operator's employee hours in the four latest reported quarters |
| `injury_rate_12m` | DOUBLE | the operator's employees' injuries (degree 01 to 06) per 200,000 hours in those quarters, counting the injuries only in the quarters the mine reported hours for (a report not yet filed adds neither hours nor injuries); null without hours |
| `top_sections_12m` | LIST(STRUCT(section VARCHAR, heading VARCHAR)) | up to five most cited sections in the 12 months, most cited first, with their title 30 headings (null heading: not in today's CFR) |

How SafeStart uses it:

- **JSA header.** When a crew starts a JSA at a site, SafeStart shows the mine's last year of enforcement: inspections,
  S&S violations, serious accidents and its injury rate, the evidence that this mine's conditions need attention now.
- **Suggested hazards.** `top_sections_12m` names the conditions inspectors keep finding at this mine (for example
  `75.400` combustible materials, `75.202` roof, face and ribs). SafeStart can offer each as a hazard to check in the
  JSA, with the heading as its label and the section's text from `cited_sections.parquet` as the requirement.
- **Site setup.** `mine_id`, name, state and location let a supervisor find and confirm the mine when adding a site.

## cited_sections.parquet

One row per section MSHA has ever cited, most cited first, with the text of today's Title 30 when it holds the section.

| Column | Type | Meaning |
|---|---|---|
| `section` | VARCHAR | the cited section, without paragraph (`56.14107`, `75.1100-2`) |
| `part` | INTEGER | its CFR part |
| `heading` | VARCHAR | its heading in today's Title 30 |
| `text` | VARCHAR | its text, one paragraph per line, without the heading or the source note |
| `amended_on` | DATE | the Federal Register date its text last changed |
| `in_current_cfr` | BOOLEAN | false for a cited number today's Title 30 does not hold (removed, renumbered or mistyped; 103 numbers) |
| `citations`, `citations_12m` | BIGINT | how often it was cited, ever and in the 12 months |
| `ss_citations`, `ss_share` | BIGINT, DOUBLE | how many of those citations were S&S, and their share |
| `proposed_penalty_total` | DOUBLE | penalties proposed for its citations, in dollars |
| `last_cited` | DATE | its newest citation |

How SafeStart uses it:

- **Regulation library.** The library shows a standard's heading and text with how often and how recently it is cited,
  and its `amended_on` date so a reader knows the text is current. `citations_12m` ranks search results toward the
  standards enforced now.
- **JSA controls.** A hazard a JSA suggests from `top_sections_12m` links to its requirement here; when
  `in_current_cfr` is false, SafeStart shows the citation without a link instead of a broken one.

## manifest.json

| Field | Meaning |
|---|---|
| `format` | the contract's version (1); a change that is not backward compatible raises it |
| `built_at`, `exported_at` | when the warehouse was built and the files written (UTC) |
| `window_12m` | the 12 months the `_12m` columns count: issue dates `after` this day `through` that one |
| `files` | per file: `rows`, `sha256`, and `columns` with their DuckDB types |
| `sources.lake` | each lake dataset the warehouse was built from: `dataset`, `version`, `rows`, `built_at` |
| `sources.ecfr` | each eCFR title: the XML's `sha256`, its `amended` date and its `sections` |
| `sources.crawl` | each website crawl: `urls` known, `downloaded`, `last_fetched_at` |

SafeStart shows "MSHA data through `window_12m.through`" next to every figure it takes from these files, and keeps the
manifest of the files it imported so a figure can be traced to the exact source versions.
