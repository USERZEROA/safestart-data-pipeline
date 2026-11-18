# SafeStart data pipeline

A data-engineering pipeline for the public U.S. safety sources behind SafeStart, a pre-shift hazard-assessment
application for mine crews. It inventories and downloads the public websites of the Mine
Safety and Health Administration (MSHA) and the Occupational Safety and Health Administration (OSHA), the Electronic
Code of Federal Regulations (titles 29 and 30), and MSHA's open enforcement datasets, then cleans, models and summarizes
them for the application's dashboards.

Work in progress (November 16 to December 4, 2026). Sections on the architecture, data model, orchestration, tests and
the data-engineering principles will be added as each stage lands.

## Sources

| Source | How it is acquired | Inventory |
|---|---|---|
| MSHA website (`www.msha.gov`) | crawl | sitemaps, then links |
| MSHA legacy host (`arlweb.msha.gov`) | crawl | the links the MSHA crawl recorded to it (the host has no home page, sitemap or directory listings) |
| OSHA website (`www.osha.gov`) | crawl | sitemaps, then links |
| eCFR titles 29 and 30 | bulk download of the govinfo XML (eCFR's own full-text API is closed to robots) | fixed list |
| MSHA open enforcement datasets | bulk download, every version kept | the open-data page's links |

## Crawling rules

- Every request follows the host's `robots.txt` (RFC 9309): its `Allow`/`Disallow` patterns and its `Crawl-delay`. OSHA
  asks for 10 seconds between requests and excludes every URL with a query string; MSHA sets no delay, so this crawler
  waits 1 second.
- One request at a time per host, a product-token User-Agent, conditional requests on re-checks, and backing off on
  `429`/`503` and on repeated `403` responses. A block is never worked around.
- Every URL the crawler learns of, from a sitemap, a page link or a redirect, ends in a recorded state: fetched (with its
  SHA-256), unchanged, redirect, not found, error after retries, or skipped with a reason (robots, page asset such as a
  style sheet or script, query string, size limit, or excluded because another source downloads it).
- Copies are complete: images, audio and video linked from the sites are kept like documents, and a file is skipped
  only above 2 GB or an hour of download, limits that stop a runaway response. `ssp fetch <site> <url> --max-mb <n>`
  downloads such a file on its own under the same robots.txt check and pacing. A crawl first re-queues what an earlier,
  stricter rule skipped (the former 50 MB limit and the former media rule).

## Completeness

`ssp verify <site>` reports whether a crawl is complete: every URL it learned of in a final state, every downloaded
file on disk with its recorded size (`--rehash` recomputes each SHA-256), where redirects and outside links lead, and,
with `--live`, whether today's sitemaps still match the inventory (added, dropped, or changed after download).
`ssp verify bulk` checks every bulk file's hash and format; with `--live` it compares the open-data page and each
file's size and date with the source today, and each eCFR title section by section with the official eCFR structure
and its latest amendment date. Each run writes its full report to `<data root>/reports/`. A live check never runs
against a host whose crawl is in progress.

## Quick start

```bash
uv sync --all-extras
uv run pytest
uv run ssp inventory msha   # robots.txt and sitemaps -> the URL inventory
uv run ssp crawl msha       # resumable; state lives in <data root>/state/crawl-msha.sqlite
uv run ssp status msha
uv run ssp verify msha --live
uv run ssp inventory arlweb # after the MSHA crawl: seeded from the links it recorded
```

On Windows, `scripts/start-crawl.ps1 -Site osha` starts a crawl that outlives the shell, from its own small environment
in `.runtime/`, so the development environment can change while a multi-day crawl continues; `scripts/stop-crawl.ps1`
stops it and the next start resumes.

The data root defaults to `./data`; set `SAFESTART_DATA_ROOT` or `data_root` in `pipeline.local.toml` to move it.

## License

MIT
