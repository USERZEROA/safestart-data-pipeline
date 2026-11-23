"""Named queries: each question the warehouse answers is one SQL file in queries/, run by its file name.

A file starts with comment lines: the question, then why it matters. Keeping the questions as plain SQL lets an analyst
read, change and rerun them without Python, and lets docs/QUERIES.md quote exactly what was run.
"""

from __future__ import annotations

from pathlib import Path

from .build import connect

QUERIES = Path(__file__).parent / "queries"


def query_names() -> list[str]:
    return sorted(path.stem for path in QUERIES.glob("*.sql"))


def query_sql(name: str) -> str:
    if name not in query_names():
        raise KeyError(f"no query named {name!r}; the queries are: {', '.join(query_names())}")
    return (QUERIES / f"{name}.sql").read_text(encoding="utf-8")


def question(name: str) -> str:
    """The question a query answers: its leading comment, up to "Why it matters"."""
    lines = []
    for line in query_sql(name).splitlines():
        if not line.startswith("--") or line.startswith("-- Why it matters"):
            break
        lines.append(line[2:].strip())
    return " ".join(lines)


def run_query(warehouse: Path, name: str) -> tuple[list[str], list[tuple]]:
    """The column names and rows of a named query, run read-only against a built warehouse."""
    with connect(warehouse) as con:
        cursor = con.execute(query_sql(name))
        return [column[0] for column in cursor.description], cursor.fetchall()


def cell(value: object, *, grouped: bool = True) -> str:
    """A value as table text: numbers with thousands separators (unless `grouped` is off, as for years)."""
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        return ", ".join(cell(item) for item in value)
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    if isinstance(value, (int, float)):
        return f"{value:,}" if grouped else str(value)
    return " ".join(str(value).split()).replace("|", "\\|")


def markdown(columns: list[str], rows: list[tuple]) -> str:
    """A Markdown table, numbers right-aligned; a column named like a year is printed without separators."""
    numeric = [any(row[i] is not None for row in rows) and all(
        isinstance(row[i], (int, float)) and not isinstance(row[i], bool) for row in rows if row[i] is not None)
        for i in range(len(columns))]
    grouped = ["year" not in column and not column.endswith("_yr") for column in columns]
    lines = ["| " + " | ".join(columns) + " |", "|" + "|".join("---:" if right else "---" for right in numeric) + "|"]
    lines += ["| " + " | ".join(cell(value, grouped=grouped[i]) for i, value in enumerate(row)) + " |" for row in rows]
    return "\n".join(lines)
