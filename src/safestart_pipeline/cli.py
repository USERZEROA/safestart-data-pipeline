"""Command line: `ssp inventory|crawl|status|recheck <site>`."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import timedelta
from pathlib import Path

from .config import data_root
from .crawl.crawler import Crawler, make_client, utc_now
from .crawl.locks import SiteLock
from .crawl.sites import SITES


def setup_logging(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    handlers = [logging.FileHandler(path, encoding="utf-8"), logging.StreamHandler(sys.stderr)]
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s", handlers=handlers)
    logging.getLogger("httpx").setLevel(logging.WARNING)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="ssp", description="SafeStart data pipeline")
    commands = parser.add_subparsers(dest="command", required=True)
    for name, help_text in (
        ("inventory", "read robots.txt and the sitemaps into the URL inventory"),
        ("crawl", "download due URLs (resumable)"),
        ("status", "URL counts by state"),
        ("recheck", "queue downloaded URLs for a conditional re-check"),
    ):
        command = commands.add_parser(name, help=help_text)
        command.add_argument("site", choices=sorted(SITES))
        if name == "crawl":
            command.add_argument("--max-requests", type=int)
            command.add_argument("--hours", type=float)
    args = parser.parse_args(argv)
    root = data_root()
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
    with SiteLock(root / "state" / f"crawl-{args.site}.lock"), make_client() as client:
        crawler = Crawler(SITES[args.site], root, client)
        try:
            if args.command == "inventory":
                result = crawler.seed_from_sitemaps()
            elif args.command == "crawl":
                until = utc_now() + timedelta(hours=args.hours) if args.hours else None
                result = crawler.crawl(max_requests=args.max_requests, until=until)
            else:
                result = {"queued": crawler.store.recheck()}
        finally:
            crawler.store.close()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
