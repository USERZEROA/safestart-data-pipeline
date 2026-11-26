# IDS 706 final project rubric check

The assignment is worth 500 points and is due December 4, 2026 at 11:00 p.m. This November 26 check maps the ten scored criteria and each assignment instruction to evidence a reader can inspect. The November 23 build figures describe real sources. The quick start uses synthetic data.

The overall pipeline requirement is evidenced by the [architecture diagram](../DOCUMENTATION.md#architecture), the [implementation](../ARCHITECTURE.md) and the [eight engineering principles](../DOCUMENTATION.md#undercurrents-of-data-engineering). The stage contracts cover ingestion, storage, transformation, orchestration and analysis, including security and modularity.

**Local verification, November 26.** Ruff passed. Of 590 pytest cases, 559 passed, 12 PostgreSQL cases skipped without a test database, and 19 deployment-script cases failed when Git Bash could not create its Windows signal pipe (error 5). Starting Git Bash by itself reproduced the error. The dashboard and markup subset had 34 passes and one PostgreSQL skip. Streamlit AppTest opened all five views on the generated sample warehouse. Browser layout, on-screen motion and hosted availability await a separate check.

### Repository and collaboration

| Requirement · 60 points | Evidence | Status |
|---|---|---|
| All team members actively contribute to the shared GitHub repository | [Repository](https://github.com/USERZEROA/safestart-data-pipeline), [team roles](../../README.md#team) and [commit history](https://github.com/USERZEROA/safestart-data-pipeline/commits/main) | Evidenced for the declared one-person team. Haiwei Yu is the only author in the local main history. |
| Multiple commits per week from multiple members, November 16 through December 4 | Local main at `de86fe9`. Run `git log de86fe9 --format="%as %aN"`. November 16–22 has 17 commits and November 23–26 has 136. | Partial. Both periods have multiple commits, all by one person. November 30–December 4 is still ahead. |
| A clear README with project overview, setup and team roles | [Overview](../../README.md), [quick start](../../README.md#quick-start), [team](../../README.md#team) | Evidenced. |

### Data ingestion

| Requirement · 40 points | Evidence | Status |
|---|---|---|
| Use real data from multiple sources | [Six sources and acquisition methods](../DOCUMENTATION.md#sources-and-ingestion), [dated crawl and download counts](sources.md), [Federal Register watcher](../../src/safestart_pipeline/watch/federal_register.py) | Evidenced by source inventories, download receipts and the watcher implementation. |

### Data storage

| Requirement · 40 points | Evidence | Status |
|---|---|---|
| Store data in appropriate databases, lakes or warehouses | [Storage inventory](../DOCUMENTATION.md#storage), [versioned Parquet manifests](lake.md), [DuckDB schema](../../src/safestart_pipeline/warehouse/schema.sql), [PostgreSQL schema](../../src/safestart_pipeline/stream/schema.sql) | Evidenced. |
| Design and document the schema, including an ERD | [Relationships and data dictionary](../ERD.md) | Evidenced for the warehouse and stream store. |

### Data querying

| Requirement · 40 points | Evidence | Status |
|---|---|---|
| Write meaningful queries that answer questions or support downstream work | [Ten questions with answers](../QUERIES.md), [mine profiles and cited sections](../SAFESTART_EXPORTS.md) | Evidenced. |
| Use SQL or an equivalent query language | [Ten named SQL files](../../src/safestart_pipeline/warehouse/queries/), [analytical marts](../../src/safestart_pipeline/warehouse/marts.sql) | Evidenced. |

### Data transformation and analysis

| Requirement · 40 points | Evidence | Status |
|---|---|---|
| Consider basic analysis with Polars or PySpark | [Polars lake builder](../../src/safestart_pipeline/lake/msha_ogd.py), [Polars analysis](../../src/safestart_pipeline/ml/analysis.py), [row reconciliation](lake.md) | Implemented with Polars. |
| May include machine learning | [Risk model, time split and leakage checks](../ML.md), [published evaluation](../ml/metrics.json) | Optional component implemented and evaluated on held-out quarters. |

### Architecture and orchestration

| Requirement · 50 points | Evidence | Status |
|---|---|---|
| Use diagrams to define a clear project architecture | [Overview diagram](../../README.md#architecture), [full diagram](../DOCUMENTATION.md#architecture) | Evidenced with Mermaid diagrams. |
| Use an orchestration tool to manage task sequences | [Prefect batch flow](../../src/safestart_pipeline/flows/pipeline.py), [run log and task times](prefect-run-2026-11-23.md) | Evidenced by code and a recorded Prefect Cloud run. |

### Containerization, CI/CD and testing

| Requirement · 60 points | Evidence | Status |
|---|---|---|
| Provide a reproducible development environment | [Lockfile](../../uv.lock), [Dockerfile](../../Dockerfile), [Compose services](../../docker-compose.yml), [sample data generator](../../src/safestart_pipeline/sample.py) | Evidenced. |
| Implement continuous integration | [Workflow](../../.github/workflows/ci.yml) | Implemented. The lint and PostgreSQL test job precedes the Compose job. This check did not dispatch CI. |
| Test critical components and the entire system | [Component tests](../../tests/), [end-to-end job](../../.github/workflows/ci.yml), [Kafka-to-PostgreSQL smoke test](../../scripts/smoke_test.py) | Component and system coverage evidenced in the repository. The Compose job is a separate execution from local pytest. |

### Undercurrents of data engineering

| Requirement · 60 points | Evidence | Status |
|---|---|---|
| Reflect on scalability, modularity, reusability, observability, data governance, reliability, efficiency and security | [Eight principles with implementation examples](../DOCUMENTATION.md#undercurrents-of-data-engineering) | All eight addressed. Examples include bounded Parquet writes, shared event definitions, build checks, hash lineage, replay deduplication and container privileges. |
| Include a dedicated README or sub-README section explaining these principles with examples | [README principles table](../../README.md#undercurrents-of-data-engineering) | Evidenced by one example per principle. |

### Additional components and tools

| Requirement · 60 points | Evidence | Status |
|---|---|---|
| Use at least one additional component, such as a web app, dashboard, cloud platform or Databricks | [Five Streamlit views](../../src/safestart_pipeline/dashboard/app.py), [dashboard tests](../../tests/test_dashboard.py), [Google Cloud deployment](../DEPLOYMENT.md) | Dashboard implemented. The cloud configuration and historical run are documented. |

### Final video and documentation

| Requirement · 50 points | Evidence | Status |
|---|---|---|
| Provide the complete GitHub repository and its link | [Course repository](https://github.com/USERZEROA/safestart-data-pipeline), [setup and entry points](../../README.md#quick-start) | Repository link supplied. This redesign is local pending publication by the owner. |
| Deliver concise documentation under 15 pages, including a one-page executive summary, architecture, key components and implementation | [Repository PDF](../SafeStart-data-pipeline-documentation.pdf), [documentation source](../DOCUMENTATION.md) | Verified in the PDF. All of the executive summary, including its evidence paragraph, is on page 1 of 12. Contents and architecture follow. |
| Deliver a 5–10 minute narrative walkthrough of the pipeline and its key components | [Current walkthrough](../SafeStart-data-pipeline-video.mp4), [walkthrough synopsis](../DOCUMENTATION.md) | The current file is 485.871 s (8:06), with 1920 × 1080 video and audio. It follows a crew's pre-shift question through the pipeline. The separate re-cut is pending. |
| Review three classmates' final reports and submit comments following the December 7 release | Course peer-review assignments in Canvas, released December 7 | Pending the course release and the owner's submission of comments. |

### Local visual review on sample data

From the repository root in PowerShell, a fresh checkout can prepare the small local warehouse with the following commands. Skip sample generation and the build when `data/course-sample/warehouse/safestart.duckdb` already exists.

```powershell
$env:SAFESTART_DATA_ROOT = Join-Path $PWD 'data/course-sample'
$env:SAFESTART_WAREHOUSE_PATH = Join-Path $env:SAFESTART_DATA_ROOT 'warehouse/safestart.duckdb'
uv sync --locked --all-extras
uv run python -m safestart_pipeline.sample $env:SAFESTART_DATA_ROOT
uv run python -m safestart_pipeline.warehouse build
```

For a warehouse-only view, run the following two lines after setting the paths above. The closed loopback port shows the unavailable-stream state.

```powershell
$env:SAFESTART_POSTGRES_URL = 'postgresql://safestart@127.0.0.1:9/safestart'
uv run --no-sync streamlit run src/safestart_pipeline/dashboard/app.py --server.address 127.0.0.1 --server.port 8501 --server.headless true
```

For **populated Live screenshots**, use this isolated local Compose project with the same sample root. It runs the two consumers and their local dependencies. The version watcher reads the sample's two releases. The `.env` password created by the project launcher is reused. On a fresh checkout, create that file first with `python scripts/start.py --no-start --data-root data/course-sample`.

```powershell
$env:DATA_ROOT = (Resolve-Path 'data/course-sample').Path
$env:DASHBOARD_HOST_PORT = '18506'
$env:POSTGRES_HOST_PORT = '25436'
$env:KAFKA_HOST_PORT = '29096'
docker compose -p safestart-course-preview up --build --detach --wait --wait-timeout 300 dashboard storage-consumer analytics-consumer
# Open http://127.0.0.1:18506/?page=Live before sending the two batches.
docker compose -p safestart-course-preview run --rm --no-deps -T replay python -m safestart_pipeline.stream.replay --day-seconds 0 --until 2026-11-09
docker compose -p safestart-course-preview run --rm --no-deps -T watcher python -m safestart_pipeline.watch once --only versions
```

On a fresh stream store, the All totals settle at 8 inspections, 11 violations, 4 accidents, 2 orders, 4 dataset versions, 0 page changes and 0 regulations. The refresh is every 5 seconds and the totals cache lasts 15 seconds. Replaying the same events preserves the totals. Stop the preview with `docker compose -p safestart-course-preview stop`. The warehouse-only server stops with Ctrl+C.

| View | What to inspect in the light and dark themes |
|---|---|
| Live | Open All before the replay, then switch between Replay and Live. Inspect the counts, connectivity pulse, radar, empty intervals and unavailable state. New arrivals get a badge. Reopening a view must preserve the stored totals. |
| Enforcement | Change the period and record kind. Inspect observed and dashed forecast lines, anomaly marks when present, the district heatmap and its legend. |
| Standards | Select a citation and read its 30 CFR text. Check row selection and text contrast. The sample can have too few yearly violations for the S&S trend. |
| Mine brief | Search for `9900001` (Sample Ridge No. 1 Mine), then an unknown ID. Inspect the eight metrics, cited standards and accident detail. |
| Sources | Inspect the feed and check status, the version table and the corpus chart. A sample without an official run displays its empty state. |

For screenshot review, check all seven Live tiles on one row at 1440 × 900 and in two columns at 390 × 844. All five page tabs should fit at 390 px. At 360 px, scroll a wide table within its panel and check that the page itself stays within the viewport. Use the keyboard to check tab focus. Enable reduced motion and reload the page to inspect a static grid, radar, status light and final values.
