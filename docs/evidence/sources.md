# Sources

What the crawls and the bulk downloads hold in the workstation's data root. `ssp verify <site>` checks one crawl, or the
bulk downloads, against the files on disk and writes its report to `<data root>/reports/verify-<site>-<UTC time>.json`.
The tables give the counts of the newest report of each, all of 2026-11-23, then a count of the raw archive on disk.

| Copy | Report | Written (UTC) | Files on disk | Live check |
|---|---|---|---|---|
| OSHA | `verify-osha-20261123T145501Z.json` | 2026-11-23 14:55:01 | every file present and of its size; SHA-256 recomputed (`--rehash`) | today's sitemap (`--live`) |
| MSHA | `verify-msha-20261123T141933Z.json` | 2026-11-23 14:19:33 | every file present and of its size; SHA-256 recomputed (`--rehash`) | today's sitemaps (`--live`) |
| Legacy host (arlweb) | `verify-arlweb-20261123T151619Z.json` | 2026-11-23 15:16:19 | every file present and of its size | none: the host has no sitemap |
| Bulk downloads | `verify-bulk-20261123T132132Z.json` | 2026-11-23 13:21:32 | every file present and of its size; SHA-256 recomputed | today's open-data page, and each eCFR title against eCFR's own structure (`--live`) |

## Crawls

| | OSHA | MSHA | Legacy host (arlweb) | All |
|---|---|---|---|---|
| URLs recorded | 56,104 | 32,507 | 13,859 | 102,470 |
| fetched | 41,157 | 32,205 | 12,394 | 85,756 |
| redirect | 1,288 | 137 | 376 | 1,801 |
| not found | 1,988 | 56 | 972 | 3,016 |
| error after retries | 239 | 2 | 0 | 241 |
| skipped with a reason | 11,432 | 107 | 117 | 11,656 |
| waiting | 0 | 0 | 0 | 0 |
| listed in the site's sitemap | 31,669 | 20,911 | no sitemap | |
| not in the site's sitemap | 24,435 | 11,596 | 13,859 | 49,890 |
| distinct files of the fetched URLs | 41,085 | 32,165 | 11,255 | 84,505 by site, 84,481 across the sites (below) |
| files missing, of the wrong size, of the wrong SHA-256 | 0, 0, 0 | 0, 0, 0 | 0, 0, not recomputed | |
| verdict | complete | complete | complete | |

- Skipped, by reason: OSHA 10,012 not needed by the project, 1,242 disallowed by robots.txt, 178 aliases of another URL;
  MSHA 106 with a query string, 1 disallowed by robots.txt; the legacy host 61 excluded because the bulk downloads fetch
  them, 56 with a query string.
- Errors: OSHA's 239 are 238 answers of 403 and one of 500 (a misspelled link), by the list of OSHA's error URLs saved
  with the reports; MSHA's 2 are 403.
- Today's sitemaps: OSHA's lists 31,669 URLs, MSHA's 12 sitemaps list 20,909. Neither lists a URL the inventory lacks or
  a page changed after its download; MSHA's no longer list 2 URLs the inventory holds.
- Not in the sitemap, as each crawl store records how a URL was found: OSHA 23,909 through page links, 329 as redirect
  targets and 197 chemical-data pages (`/chemicaldata/<n>`) a probe added; MSHA 11,525 through page links, 2 as redirect
  targets and 69 added by hand; the legacy host 13,848 through page links, 8 as redirect targets and 3 added by hand.
- The newest legacy-host report did not recompute the SHA-256. The one before it, `verify-arlweb-20261123T122955Z.json`,
  did, over the same 12,394 fetched URLs and 11,255 files, and found none wrong.
- The reports count URLs and files, not bytes; the sizes are in the archive count below.

## Bulk downloads

| | MSHA open data | eCFR (govinfo XML) | All |
|---|---|---|---|
| files | 40: 20 dataset zips, 20 definition files | 2: titles 29 and 30 | 42 |
| last download, 2026-11-23 13:19 to 13:20 UTC | 20 fetched (the zips), 20 unchanged | 2 unchanged | 20 fetched, 22 unchanged |
| present, of the recorded size and SHA-256, passed the format check | 40 | 2 | 42 |
| bytes of the copies in use | 618,564,000 | 39,246,260 | 657,810,260 |

- Today's open-data page lists the 40 files the store holds: none new, delisted, gone or changed.
- The format check is made at download: an intact zip, well-formed XML, a non-empty text file.
- The Google Cloud VM keeps its own copy of the data root and runs the downloads itself: its run at 22:11 UTC took new
  versions of `Violations.zip` and `AssessedViolations.zip` ([prefect-run-2026-11-23.md](prefect-run-2026-11-23.md)),
  which the workstation's bulk store does not hold.
- eCFR: title 29 holds 7,262 sections and title 30 5,954, each exactly the official eCFR structure (none missing, none
  extra). Title 30 is amended to 2026-11-12, as eCFR is. Title 29 is amended to 2026-11-09 while eCFR shows an amendment
  of 2026-11-10, and govinfo's file has not changed since our copy, so the verdict is "not complete": "title 29: our
  file is amended to 2026-11-09, eCFR to 2026-11-10".

## Raw archive

Counted after the reports above: the files under `<data root>/raw/<folder>/objects/`, each named by its SHA-256, and
their sizes (GB = 10^9 bytes). No stored file is newer than the report on its folder, so the count and the reports
describe the same archive.

| Folder | Files | Bytes | GB |
|---|---|---|---|
| `raw/osha` | 41,086 | 10,930,608,183 | 10.93 |
| `raw/msha` | 32,198 | 5,876,533,223 | 5.88 |
| `raw/arlweb` | 11,255 | 26,513,581,236 | 26.51 |
| `raw/bulk` | 63 | 1,275,731,451 | 1.28 |
| All | 84,602 | 44,596,454,093 | 44.60 |

The archive keeps every file it stores, so it holds 34 crawl files more than the reports' distinct files of the fetched
URLs (84,539 against 84,505): OSHA's sitemap as last read (1), MSHA's 12 sitemaps as last read and as read before (24),
and the earlier copies of 9 MSHA pages that changed after their download (9). The 63 bulk files are the 62 versions the
bulk store records (two of each dataset zip, one of each definition file and eCFR title) and the open-data page as last
read.

The reports count distinct files per site, and each folder belongs to one site, so a file that two sites served is
stored in both. The two MSHA hosts served 24 of the same files, kept in both `raw/msha` and `raw/arlweb` (207,863,042
bytes); no file of `raw/osha` is in another folder. Across the three sites the fetched URLs therefore lead to 84,481
distinct files (84,505 - 24) of 43,100,291,284 bytes (43.1 GB), the figure the dashboard's Sources page shows, since it
counts each SHA-256 once whichever site served it. These come from a read-only listing, on 2026-11-24, of the folders
and of each crawl store's fetched URLs, which also found the folder counts above and the reports' distinct files per
site unchanged.

These are the documentation's figures: 102,470 URLs recorded, 85,756 downloaded, which lead to 84,481 distinct files
(43.1 GB), 84,539 crawl files (10.9, 5.9 and 26.5 GB) and 63 bulk files (1.3 GB), 84,602 files and 44.6 GB in all.
