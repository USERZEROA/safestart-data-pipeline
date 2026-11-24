# Prefect run, 2026-11-23

One run of the daily batch flow, `safestart-batch`, as Prefect Cloud recorded it: the bulk downloads, the lake, the
warehouse and the SafeStart exports, the steps of the README's Orchestration section. The run "victorious-goat" was
started by hand, outside the flow's 14:00 UTC schedule, and executed by the orchestrator on the Google Cloud VM, whose
container mounts the data root at `/data`. Everything below comes from an export of the Prefect Cloud API: the flow
runs, every task run and every log line (127 for this run with its sub-flow, 127 for the next), without their ids. Times
are UTC.

| Flow run | Flow | Parameters | Started | Ended | Duration (min:s) | State |
|---|---|---|---|---|---|---|
| victorious-goat | `safestart-batch` | defaults | 2026-11-23 22:11:05.444 | 22:30:17.434 | 19:11.99 | Completed |
| lilac-barnacle, its sub-flow | `acquire-bulk-files` | `sources`: `msha-ogd`, `ecfr-govinfo` | 22:11:07.074 | 22:12:24.328 | 1:17.25 | Completed |
| dancing-puma, the next run | `safestart-batch` | defaults | 22:30:36.439 | 22:32:27.540 | 1:51.10 | Completed |
| rampant-dogfish, its sub-flow | `acquire-bulk-files` | `sources`: `msha-ogd`, `ecfr-govinfo` | 22:30:38.012 | 22:31:26.491 | 0:48.48 | Completed |

## Tasks

| Task run | Started | Ended | Duration (min:s) | Runs | State |
|---|---|---|---|---|---|
| acquire-bulk-files-0 (the sub-flow) | 22:11:07.074 | 22:12:24.328 | 1:17.25 | 1 | Completed |
| build-lake-e90 | 22:12:24.518 | 22:28:12.937 | 15:48.42 | 1 | Completed |
| build-warehouse-e80 | 22:28:12.951 | 22:30:14.224 | 2:01.27 | 1 | Completed |
| export-safestart-32c | 22:30:14.234 | 22:30:17.221 | 0:02.99 | 1 | Completed |

The lake task took most of the run because it built two new versions, of `violations` and `assessed_violations` (3.1 and
3.0 million rows; their log lines are below). With nothing new to build, the next run's lake task took 0.16 s.

## Downloads

The sub-flow read MSHA's open-data page and the fixed list of eCFR titles, then downloaded each of the 42 files in a
task of its own, one at a time: 42 task runs, each completed at its first attempt and taking 1.0 to 7.3 s, with the
pause of a second or more before the next request.

| Files | Downloads | Fetched in full, verified | Unchanged (304) | New version |
|---|---|---|---|---|
| MSHA dataset zips | 20 | 20 | 0 | 2: `AssessedViolations.zip`, `Violations.zip` |
| MSHA definition files | 20 | 0 | 20 | 0 |
| eCFR titles 29 and 30 (govinfo XML) | 2 | 0 | 2 | 0 |
| All | 42 | 20 | 22 | 2 |

"Fetched" means the source sent the whole file and it passed its format check (an intact zip, `1 member(s)`);
`new_version` says whether the bulk store held no version of that file with its SHA-256 yet. The other 18 zips matched
versions already held, so only `violations` and `assessed_violations` had a version for the lake to build. "Unchanged"
means the source answered the conditional request with 304 Not Modified.

## Log lines

The key lines, quoted as logged (times cut to the second). The lake logs one line per dataset: the 18 not shown end
`0 of 2 versions built now`. The 42 download lines are summarised above. The flow's last line is stamped 2.1 s after the
end time Prefect records for the run.

```text
22:11:05 Beginning flow run 'victorious-goat' for flow 'safestart-batch'
22:11:07 Beginning subflow run 'lilac-barnacle' for flow 'acquire-bulk-files'
22:11:09 msha-ogd lists 40 files
22:12:06 Violations.zip: {'url': 'https://arlweb.msha.gov/OpenGovernmentData/DataSets/Violations.zip', 'state': 'fetched', 'verified': True, 'note': '1 member(s)', 'new_version': True}
22:12:17 AssessedViolations.zip: {'url': 'https://arlweb.msha.gov/OpenGovernmentData/DataSets/AssessedViolations.zip', 'state': 'fetched', 'verified': True, 'note': '1 member(s)', 'new_version': True}
22:12:21 ecfr-govinfo lists 2 files
22:12:24 summary: {'ecfr-govinfo/unchanged': {'files': 2, 'verified': 2}, 'msha-ogd/fetched': {'files': 20, 'verified': 20}, 'msha-ogd/unchanged': {'files': 20, 'verified': 20}}
22:19:02 violations: 80be817e8f69bbb4.parquet current, 3115856 rows; 1 of 3 versions built now
22:28:12 assessed_violations: 75adc2715cfff6e8.parquet current, 3049923 rows; 1 of 3 versions built now
22:28:12 lake: 20 of 20 datasets have a current version; rebuilt assessed_violations, violations
22:28:22 staged 13216 eCFR sections
22:28:34 staged 102470 URLs from the crawl catalogs of osha, msha, arlweb
22:28:36 loaded mine in 1.7 s
22:28:47 loaded inspection in 10.9 s
22:29:17 loaded violation in 30.4 s
22:29:22 loaded accident in 5.1 s
22:30:10 loaded production_quarter in 47.2 s
22:30:10 loaded cfr_section in 0.4 s
22:30:11 loaded document in 1.4 s
22:30:14 warehouse built in 121 s: /data/warehouse/safestart.duckdb
22:30:14 warehouse /data/warehouse/safestart.duckdb built in 121.0 s; rows {'mine': 92129, 'inspection': 1165876, 'violation': 3115805, 'accident': 276013, 'production_quarter': 2784528, 'cfr_section': 13216, 'document': 102470, 'dataset_version': 42}
22:30:17 SafeStart exports in /data/exports/safestart: {'mine_profiles.parquet': 92129, 'cited_sections.parquet': 1789}
22:30:19 Finished in state Completed()
```

The lake's 20 dataset lines name each current version and its rows; together they make 18,057,074 current rows, the
README's figure. The VM's lake holds 42 versions, as the warehouse's `dataset_version` count shows: the two new ones on
top of the 40 that the README and [lake.md](lake.md) count. The warehouse's rows are the README's 3.1 million
violations, 1.2 million inspections and 276,013 accidents; its 102,470 crawl URLs and 13,216 eCFR sections (7,262 in
title 29, 5,954 in title 30) are those of [sources.md](sources.md).

## The next run

"dancing-puma" ran the same flow 19 seconds later and completed in 1:51.10 (min:s). Its sub-flow found all 42 files
unchanged, the lake built nothing, and the warehouse, rebuilt on every run, came out with the same row counts:

```text
22:31:26 summary: {'ecfr-govinfo/unchanged': {'files': 2, 'verified': 2}, 'msha-ogd/unchanged': {'files': 40, 'verified': 40}}
22:31:26 lake: 20 of 20 datasets have a current version; rebuilt none
22:32:26 warehouse built in 59 s: /data/warehouse/safestart.duckdb
22:32:27 SafeStart exports in /data/exports/safestart: {'mine_profiles.parquet': 92129, 'cited_sections.parquet': 1789}
```
