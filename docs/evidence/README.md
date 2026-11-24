# Evidence

The headline figures of the [README](../../README.md) trace back to records of 2026-11-23: a batch run as Prefect Cloud
recorded it, the pipeline's own completeness reports and lake manifests, and a count of the raw archive on disk. The
files here copy the counts out of those records, so a figure can be checked without the data root; they leave out local
paths and Prefect's run, workspace and account ids.

**[prefect-run-2026-11-23.md](prefect-run-2026-11-23.md)** shows a real run of the daily batch flow on Prefect Cloud,
"victorious-goat", started by hand and executed on the Google Cloud VM: its four tasks with their times, its sub-flow's
42 downloads (20 fetched, 22 unchanged, 2 new versions) and the log lines in which the lake, the warehouse and the
SafeStart exports report what they did (18,057,074 current lake rows; 92,129 mine profiles and 1,789 cited sections);
then the next run, "dancing-puma", which found nothing new. It comes from an export of the Prefect Cloud API: the flow
runs, their sub-flow runs, every task run and every log line.

**[sources.md](sources.md)** shows what the crawls and the bulk downloads hold: the counts of the newest completeness
report of each crawled site and of the bulk files, which `ssp verify <site>` writes to
`<data root>/reports/verify-<site>-<UTC time>.json` (102,470 URLs recorded, their final states, the distinct files and
the checks of those files on disk), then a count of the raw archive's files and bytes on disk (84,602 files, 44.6 GB).

**[lake.md](lake.md)** shows the lake: one row per MSHA dataset with the versions it keeps and its current version's
source file, first-seen time, rows and lines read (20 datasets, 40 versions, 18,057,074 current rows), from the
manifests `<data root>/lake/msha_ogd/<dataset>/manifest.json` that `ssp lake` writes.
