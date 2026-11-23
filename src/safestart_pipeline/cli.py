"""Command line: `ssp inventory|crawl|status|recheck|fetch|verify|lake|pipeline|warehouse|ml`.

`ssp pipeline` runs the batch flow once (and the model flow after it with --model); `ssp warehouse` and `ssp ml` pass
their arguments to `python -m safestart_pipeline.warehouse` and `python -m safestart_pipeline.ml`. These commands
import Prefect, DuckDB, Polars or scikit-learn only when they run: the crawler's own environment has only httpx, lxml
and protego.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from contextlib import ExitStack
from datetime import timedelta
from pathlib import Path

from .config import data_root, warehouse_path
from .crawl.crawler import Crawler, make_client, utc_now
from .crawl.locks import SiteLock
from .crawl.sites import SITES
from .verify import Polite, verify_bulk, verify_crawl


def setup_logging(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    handlers = [logging.FileHandler(path, encoding="utf-8"), logging.StreamHandler(sys.stderr)]
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s", handlers=handlers)
    logging.getLogger("httpx").setLevel(logging.WARNING)


def lock_for(root: Path, site: str) -> SiteLock:
    return SiteLock(root / "state" / f"crawl-{site}.lock")


def try_lock(stack: ExitStack, root: Path, site: str) -> bool:
    """Hold a site's lock for a live check, so it never adds requests to a crawl of that site in progress."""
    try:
        stack.enter_context(lock_for(root, site))
        return True
    except RuntimeError:
        return False


def summary(report: dict, path: Path) -> dict:
    if "site" in report:
        live = report.get("live_sitemap")
        live_keys = ("urls_today", "added", "dropped", "changed_after_fetch")
        return {
            "site": report["site"], "verdict": report["verdict"], "states": report["states"],
            "waiting": report["waiting"],
            "sitemap": {"urls": report["sitemap"]["urls"], "states": report["sitemap"]["states"]},
            "skipped": {reason: item["count"] for reason, item in report["skipped"].items()},
            "failed": {state: item["count"] for state, item in report["failed"].items()},
            "objects": {key: value for key, value in report["objects"].items() if not key.endswith("examples")},
            **({"live_sitemap": {key: live[key] for key in live_keys}} if live else {}),
            **({"live_check": report["live_check"]} if "live_check" in report else {}),
            "report": str(path),
        }
    listing = report.get("msha_listing_today")
    titles = report.get("ecfr_today")
    keys = ("title", "official_sections", "our_sections", "missing_sections", "extra_sections", "our_amended",
            "ecfr_latest_amended_on", "source_newer")
    return {
        "verdict": report["verdict"], "files": len(report["files"]), "problems": report["problems"],
        **({"msha_listing_today": {key: (len(value) if isinstance(value, list) else value)
                                   for key, value in listing.items()}} if listing else {}),
        **({"ecfr_today": [{key: title.get(key) for key in keys} for title in titles]} if titles else {}),
        **({"live_check": report["live_check"]} if "live_check" in report else {}),
        "report": str(path),
    }


def verify(root: Path, target: str, *, rehash: bool, live: bool) -> dict:
    with ExitStack() as stack, make_client() as client:
        checker, note = None, None
        if live:
            # The open-data files are on the host the arlweb crawl visits; a crawl's own live check needs its lock.
            lock_site = "arlweb" if target == "bulk" else target
            if try_lock(stack, root, lock_site):
                checker = Polite(client)
            else:
                note = f"{lock_site} crawl running: its host was not asked again"
                if target == "bulk":
                    checker = Polite(client)  # eCFR and govinfo are not crawled; only the MSHA listing waits
        if target == "bulk":
            report = verify_bulk(root, rehash=True, live=checker, check_listing=note is None)
        else:
            report = verify_crawl(SITES[target], root, rehash=rehash, live=checker)
        if note:
            report["live_check"] = note
    folder = root / "reports"
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"verify-{target}-{utc_now().strftime('%Y%m%dT%H%M%SZ')}.json"
    path.write_text(json.dumps(report, indent=1, default=str), encoding="utf-8")
    return summary(report, path)


def warehouse(argv: list[str]) -> int:
    """`python -m safestart_pipeline.warehouse` with these arguments; build and export hold the warehouse lock."""
    from .flows.locks import warehouse_lock
    from .warehouse.__main__ import main as warehouse_main

    if argv and argv[0] in ("build", "export"):
        with warehouse_lock(warehouse_path()):
            return warehouse_main(argv)
    return warehouse_main(argv)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="ssp", description="SafeStart data pipeline")
    commands = parser.add_subparsers(dest="command", required=True)
    for name, help_text in (
        ("inventory", "read robots.txt and the sitemaps (or other crawls' links) into the URL inventory"),
        ("crawl", "download due URLs (resumable)"),
        ("status", "URL counts by state"),
        ("recheck", "queue downloaded URLs for a conditional re-check"),
        ("fetch", "download chosen URLs now with their own size limit (large files approved one by one)"),
    ):
        command = commands.add_parser(name, help=help_text)
        command.add_argument("site", choices=sorted(SITES))
        if name == "crawl":
            command.add_argument("--max-requests", type=int)
            command.add_argument("--hours", type=float)
        if name == "fetch":
            command.add_argument("urls", nargs="+")
            command.add_argument("--max-mb", type=float, required=True)
            command.add_argument("--max-seconds", type=float, default=1800.0)
    check = commands.add_parser("verify", help="completeness report of a crawl or of the bulk downloads")
    check.add_argument("target", choices=[*sorted(SITES), "bulk"])
    check.add_argument("--rehash", action="store_true", help="recompute every stored file's SHA-256 (slow for a crawl)")
    check.add_argument("--live", action="store_true", help="also compare with the source as it is today")
    lake = commands.add_parser("lake", help="bring the typed Parquet lake up to date with the current downloads")
    lake.add_argument("--only", nargs="+", metavar="DATASET", help="build only these MSHA datasets")
    pipeline = commands.add_parser("pipeline", help="run the batch flow once: downloads, lake, warehouse, exports")
    pipeline.add_argument("--no-download", action="store_true", help="rebuild from the downloads already held")
    pipeline.add_argument("--model", action="store_true", help="then train the model and write the lake analysis")
    # Their own modules parse the rest, so `ssp warehouse --help` shows the warehouse module's help.
    commands.add_parser("warehouse", add_help=False, help="build | query [name] | export: the DuckDB warehouse")
    commands.add_parser("ml", add_help=False, help="train | analyze: the accident-risk model and the lake analysis")
    args, rest = parser.parse_known_args(argv)
    if args.command == "warehouse":
        return warehouse(rest)
    if args.command == "ml":
        from .ml.__main__ import main as ml_main

        return ml_main(rest)
    if rest:
        parser.error(f"unrecognized arguments: {' '.join(rest)}")
    root = data_root()
    if args.command == "pipeline":
        setup_logging(root / "logs" / "pipeline.log")
        from .flows.__main__ import run_once  # imported after the logging setup, which Prefect then keeps

        return run_once(["batch", "model"] if args.model else ["batch"], download=not args.no_download)
    if args.command == "lake":
        from .lake.msha_ogd import build_lake  # the analysis extra; the crawler's own environment does not need it

        setup_logging(root / "logs" / "lake.log")
        print(json.dumps({"msha_ogd": build_lake(root, args.only)}, indent=2, sort_keys=True))
        return 0
    if args.command == "verify":
        print(json.dumps(verify(root, args.target, rehash=args.rehash, live=args.live), indent=2, sort_keys=True))
        return 0
    if args.command == "status":
        crawler_store = Crawler.open_store(SITES[args.site], root)
        try:
            result: object = {
                "states": crawler_store.counts(),
                "skipped": crawler_store.skip_reasons(),
                "fetched_bytes": crawler_store.fetched_bytes(),
                "last_stop": crawler_store.get_meta("last_stop"),
            }
        finally:
            crawler_store.close()
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    setup_logging(root / "logs" / f"{args.site}.log")
    with lock_for(root, args.site), make_client() as client:
        crawler = Crawler(SITES[args.site], root, client)
        try:
            if args.command == "inventory":
                result = crawler.seed_from_sitemaps()
                if crawler.site.seed_from:
                    result = {**result, **crawler.seed_from_links(root)}
            elif args.command == "crawl":
                until = utc_now() + timedelta(hours=args.hours) if args.hours else None
                result = crawler.crawl(max_requests=args.max_requests, until=until)
            elif args.command == "fetch":
                result = crawler.fetch_now(args.urls, max_bytes=int(args.max_mb * 1024 * 1024),
                                           max_seconds=args.max_seconds)
            else:
                result = {"queued": crawler.store.recheck()}
        finally:
            crawler.store.close()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
