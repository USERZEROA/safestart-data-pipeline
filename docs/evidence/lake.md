# Lake

The workstation's lake as built on 2026-11-23 (16:16 to 16:25 UTC, lake format 4): one row per MSHA dataset, from its
manifest, `<data root>/lake/msha_ogd/<dataset>/manifest.json`, which `ssp lake` writes. The current version is the one
the manifest names `current`, the newest download; its Parquet file is named after the first 16 hex digits of the source
file's SHA-256. "Lines read" counts the source file's data lines, all split plainly unless noted ("quoted": a quoted
field holds the separator); every version's rows equal its lines read and the row count in its Parquet file's footer.

| Dataset | Versions kept | Current version (source file) | First seen (UTC) | Rows | Lines read |
|---|---|---|---|---|---|
| `accidents` | 2 | `ee03c29abf62eb60` (`Accidents.zip`) | 2026-11-23 13:19:23 | 276,013 | 276,013 |
| `address_of_record` | 2 | `c2374e213882259a` (`AddressofRecord.zip`) | 2026-11-23 13:19:58 | 91,832 | 91,832 |
| `area_samples` | 2 | `41a60a0f794d7889` (`AreaSamples.zip`) | 2026-11-23 13:19:25 | 8,923 | 8,923 |
| `assessed_violations` | 2 | `3ede55666fdc6848` (`AssessedViolations.zip`) | 2026-11-23 13:20:26 | 3,049,923 | 3,049,923 |
| `civil_penalty_dockets` | 2 | `dff26377a22335b1` (`CivilPenaltyDocketsDecisions.zip`) | 2026-11-23 13:19:37 | 483,786 | 483,786 |
| `coal_dust_samples` | 2 | `bfc98f02eee78fae` (`CoalDustSamples.zip`) | 2026-11-23 13:19:32 | 3,051,517 | 3,051,517 |
| `conferences` | 2 | `38a9e5bb0845373f` (`Conferences.zip`) | 2026-11-23 13:19:34 | 162,657 | 162,657 |
| `contested_violations` | 2 | `fd6b1ba3ac66f230` (`ContestedViolations.zip`) | 2026-11-23 13:20:19 | 453,913 | 453,913 |
| `contractor_prod_quarterly` | 2 | `3a3081dc61047656` (`ContractorProdQuarterly.zip`) | 2026-11-23 13:19:40 | 1,373,178 | 1,373,178 (1,373,175 plain, 3 quoted) |
| `contractor_prod_yearly` | 2 | `314c2380276beda9` (`ContractorProdYearly.zip`) | 2026-11-23 13:19:42 | 281,717 | 281,717 |
| `controller_operator_history` | 2 | `03bd543956c63bad` (`ControllerOperatorHistory.zip`) | 2026-11-23 13:19:44 | 170,564 | 170,564 |
| `inspections` | 2 | `fdacb4346a936842` (`Inspections.zip`) | 2026-11-23 13:19:56 | 1,165,877 | 1,165,877 |
| `mines` | 2 | `c4ba9edb535e6bff` (`Mines.zip`) | 2026-11-23 13:20:00 | 92,129 | 92,129 |
| `mines_prod_quarterly` | 2 | `973b4cd33eccf208` (`MinesProdQuarterly.zip`) | 2026-11-23 13:19:51 | 2,784,528 | 2,784,528 |
| `mines_prod_yearly` | 2 | `6edb015d631c380f` (`MinesProdYearly.zip`) | 2026-11-23 13:19:47 | 659,470 | 659,470 |
| `noise_samples` | 2 | `8c267f12adb16640` (`NoiseSamples.zip`) | 2026-11-23 13:20:03 | 277,828 | 277,828 |
| `orders_107a` | 2 | `1b0f05ec81db6631` (`OrdersIssued.zip`) | 2026-11-23 13:20:29 | 3,885 | 3,885 |
| `personal_health_samples` | 2 | `aa998534e8eb2391` (`PersonalHealthSamples.zip`) | 2026-11-23 13:20:05 | 319,154 | 319,154 |
| `quartz_samples` | 2 | `91d18ed711552c58` (`QuartzSamples.zip`) | 2026-11-23 13:20:07 | 234,324 | 234,324 |
| `violations` | 2 | `7943d78c6aba1c23` (`Violations.zip`) | 2026-11-23 13:20:16 | 3,115,856 | 3,115,856 |
| All 20 datasets | 40 | | | 18,057,074 | 18,057,074 |

- Every dataset keeps two versions, one from each of the two MSHA releases the bulk store holds: Last-Modified Tuesday
  2026-11-17 13:54:20 to 13:54:25 GMT, first seen on 2026-11-18 between 00:29 and 00:30 UTC; and the current one,
  Last-Modified Monday 2026-11-23 12:50:14 to 12:50:20 GMT, first seen on 2026-11-23 between 13:19 and 13:20 UTC.
- The 40 Parquet files take 839,826,466 bytes (0.84 GB). With the 20 datasets and 18,057,074 current rows, these are the
  documentation's lake figures.
- The Google Cloud VM keeps its own copy of the data root. Its batch run of 2026-11-23 at 22:11 UTC took a third version
  of `violations` (`80be817e8f69bbb4`) and of `assessed_violations` (`75adc2715cfff6e8`), each with a new SHA-256 and as
  many rows as the current version above, so the VM's lake holds 42 versions whose current ones add up to the same
  18,057,074 rows ([prefect-run-2026-11-23.md](prefect-run-2026-11-23.md)).
