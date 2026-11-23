"""`python -m safestart_pipeline.warehouse build | query [name] | export [--out DIR]`.

build reads the lake, the bulk store and the crawl catalogs under the data root (SAFESTART_DATA_ROOT), only reading
them, and writes the warehouse at warehouse_path() (SAFESTART_WAREHOUSE_PATH). query prints a named query's answer as a
Markdown table; without a name it lists the questions. export writes the SafeStart exports, by default under
<data root>/exports/safestart.
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import sys
from pathlib import Path

from ..config import data_root, warehouse_path
from .build import build
from .exports import export
from .named_queries import markdown, query_names, question, run_query


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m safestart_pipeline.warehouse", description=__doc__.split("\n")[0])
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("build", help="build the warehouse and print its row counts and checks")
    query = commands.add_parser("query", help="print a named query's answer (no name: list the questions)")
    query.add_argument("name", nargs="?")
    exporter = commands.add_parser("export", help="write the SafeStart exports")
    exporter.add_argument("--out", type=Path, help="output folder (default <data root>/exports/safestart)")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s", stream=sys.stderr)
    if isinstance(sys.stdout, io.TextIOWrapper):
        sys.stdout.reconfigure(errors="replace")  # a Windows console cannot print every character of the data

    if args.command == "build":
        print(json.dumps(build(data_root(), warehouse_path()), indent=1, default=str))
    elif args.command == "query" and args.name:
        try:
            columns, rows = run_query(warehouse_path(), args.name)
        except KeyError as problem:
            parser.error(str(problem.args[0]))
        print(markdown(columns, rows))
    elif args.command == "query":
        for name in query_names():
            print(f"{name}: {question(name)}")
    else:
        out = args.out or data_root() / "exports" / "safestart"
        print(json.dumps(export(warehouse_path(), out), indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
