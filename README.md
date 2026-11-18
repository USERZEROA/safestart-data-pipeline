# SafeStart data pipeline

A data-engineering pipeline for the public U.S. safety sources behind SafeStart, a pre-shift hazard-assessment
application for mine crews. It inventories and downloads the public websites of the Mine
Safety and Health Administration (MSHA) and the Occupational Safety and Health Administration (OSHA), the Electronic
Code of Federal Regulations (titles 29 and 30), and MSHA's open enforcement datasets, then cleans, models and summarizes
them for the application's dashboards.

Work in progress (November 16 to December 4, 2026). Sections on the architecture, data model, orchestration, tests and
the data-engineering principles will be added as each stage lands.

## Crawling rules

- Every request follows the host's `robots.txt` (RFC 9309): its `Allow`/`Disallow` patterns and its `Crawl-delay`. OSHA
  asks for 10 seconds between requests and excludes every URL with a query string; MSHA sets no delay, so this crawler
  waits 1 second.
- One request at a time per host, a product-token User-Agent, conditional requests on re-checks, and backing off on
  `429`/`503` and on repeated `403` responses. A block is never worked around.
- Every URL the crawler learns of, from a sitemap, a page link or a redirect, ends in a recorded state: fetched (with its
  SHA-256), unchanged, redirect, not found, error after retries, or skipped with a reason (robots, media, size cap).

## Quick start

```bash
uv sync
uv run pytest
uv run ssp inventory msha   # robots.txt and sitemaps -> the URL inventory
uv run ssp crawl msha       # resumable; state lives in <data root>/state/crawl-msha.sqlite
uv run ssp status msha
```

The data root defaults to `./data`; set `SAFESTART_DATA_ROOT` or `data_root` in `pipeline.local.toml` to move it.

## License

MIT
