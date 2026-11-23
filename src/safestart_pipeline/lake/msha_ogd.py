"""MSHA's open enforcement datasets as a typed Parquet lake.

Each bulk zip holds one pipe-delimited text file (CRLF lines, Windows-1252 text), and each dataset comes with a
definition file naming every column's Oracle type (VARCHAR2, DATE, NUMBER(p[,s])) and meaning. The lake keeps one
Parquet file per downloaded version, named after the source file's SHA-256, so a rebuild of an unchanged version does
nothing and every row traces back to the exact file it came from.

MSHA wraps text in double quotes but never escapes a quote inside it (`"54" ROLL CR. #4"`, `"Clary"s Sitework"`), and
one contractor name holds the separator itself (`"AZZ|Central Electric"`). A CSV reader that honors quotes therefore
runs one record into the next: on 2026-11-23 it kept 41,410 of the 92,129 mine records. Every record is one line, so
the lake splits each line itself: when a line has the header's number of separators a plain split is exact, and
otherwise a quoted field ends only at a quote followed by the separator or the line end. The manifest counts the lines
read each way, and a build fails if the Parquet row count differs from the source's line count.

Cleaning is deliberately small and recorded: surrounding spaces are trimmed, the values MSHA uses for "unknown" ("?",
"NO VALUE FOUND") and empty strings become nulls, and every value that does not parse as its declared type becomes a
null that is counted per column in the manifest rather than dropped without a trace.
"""

from __future__ import annotations

import codecs
import io
import json
import re
import sqlite3
import zipfile
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

# Lake name -> (bulk file name of the zip, bulk file name of its definition file). The names on MSHA's page do not
# follow one pattern, so the pairs are listed rather than guessed.
DATASETS: dict[str, tuple[str, str]] = {
    "mines": ("Mines.zip", "Mines_Definition_File.txt"),
    "address_of_record": ("AddressofRecord.zip", "AddressofRecord_Definition_File.txt"),
    "controller_operator_history": ("ControllerOperatorHistory.zip", "Controller_Operator_History_Definition_File.txt"),
    "inspections": ("Inspections.zip", "Inspections_Definition_File.txt"),
    "violations": ("Violations.zip", "violations_Definition_File.txt"),
    "assessed_violations": ("AssessedViolations.zip", "Assessed_Violations_Definition_File.txt"),
    "contested_violations": ("ContestedViolations.zip", "Contested_Violations_Definition_File.txt"),
    "civil_penalty_dockets": ("CivilPenaltyDocketsDecisions.zip", "Civil_Penalty_Dockets_And_Decisions_Definition_File.txt"),
    "conferences": ("Conferences.zip", "Conferences_Definition_File.txt"),
    "orders_107a": ("OrdersIssued.zip", "107(a)_Orders_Issued_Definition_File.txt"),
    "accidents": ("Accidents.zip", "Accidents_Definition_File.txt"),
    "mines_prod_quarterly": ("MinesProdQuarterly.zip", "MineSProdQuarterly_Definition_File.txt"),
    "mines_prod_yearly": ("MinesProdYearly.zip", "MineSProdYearly_Definition_File.txt"),
    "contractor_prod_quarterly": ("ContractorProdQuarterly.zip", "ContractorProdQuarterly_Definition_File.txt"),
    "contractor_prod_yearly": ("ContractorProdYearly.zip", "ContractorProdAnnual_Definition_File.txt"),
    "coal_dust_samples": ("CoalDustSamples.zip", "Coal_Dust_Sample_Definition_File.txt"),
    "quartz_samples": ("QuartzSamples.zip", "Quartz_Samples_Definition_File.txt"),
    "noise_samples": ("NoiseSamples.zip", "Noise_Samples_Definition_File.txt"),
    "personal_health_samples": ("PersonalHealthSamples.zip", "Personal_Health_Samples_Definition_File.txt"),
    "area_samples": ("AreaSamples.zip", "Area_Samples_Definition_File.txt"),
}
UNKNOWN_VALUES = ("", "?", "NO VALUE FOUND")
DATE_FORMAT = "%m/%d/%Y"
SOURCE_ENCODING = "cp1252"
# A version built under an older format is built again: 1 read the files with a quote-honoring CSV reader, 2 splits
# each line itself (see the module docstring), 3 applies TYPE_OVERRIDES.
LAKE_FORMAT = 3
# Declared types the data contradicts, each with its reason: (dataset, column) -> (Oracle length to use, why).
TYPE_OVERRIDES: dict[tuple[str, str], tuple[str, str]] = {
    ("contractor_prod_yearly", "AVG_EMPLOYEE_CNT"): (
        "9,2", "a yearly average of quarterly head counts, written with two decimals (1.33, .67) though declared NUMBER(9)"),
}
SEPARATOR = "|"
# The transcoded text separates fields with the ASCII unit separator, which MSHA's text never holds, so it needs no
# quoting at all.
LAKE_SEPARATOR = "\x1f"
# A few files hold bytes Windows-1252 leaves undefined (0x81, 0x8D, 0x8F, 0x90, 0x9D). Each becomes U+FFFD, and the
# manifest records how many there were.
DECODE_ERRORS = "safestart-count-replace"
_undecodable = [0]


def _count_and_replace(error: UnicodeError) -> tuple[str, int]:
    if not isinstance(error, UnicodeDecodeError):
        raise error
    _undecodable[0] += error.end - error.start
    return chr(0xFFFD) * (error.end - error.start), error.end


codecs.register_error(DECODE_ERRORS, _count_and_replace)


@dataclass(frozen=True)
class ColumnDef:
    source: str  # the column name as the file and the definition write it
    oracle_type: str  # VARCHAR2, DATE or NUMBER
    length: str  # "50", or "9,2" for NUMBER(9,2)
    description: str

    @property
    def name(self) -> str:
        return snake(self.source)

    @property
    def dtype(self) -> pl.DataType:
        if self.oracle_type == "DATE":
            return pl.Date()
        if self.oracle_type == "NUMBER":
            precision, _, scale = self.length.partition(",")
            if scale.strip() not in ("", "0"):
                return pl.Float64()
            return pl.Int64() if precision.strip().isdigit() and int(precision) <= 18 else pl.Float64()
        return pl.String()


def snake(name: str) -> str:
    return re.sub(r"[^0-9a-z]+", "_", name.strip().lower()).strip("_")


def read_definitions(path: Path) -> dict[str, ColumnDef]:
    """Column definitions by source column name, from a pipe-delimited definition file with a header row."""
    text = path.read_bytes().decode(SOURCE_ENCODING)
    columns: dict[str, ColumnDef] = {}
    for line in text.splitlines()[1:]:
        parts = [part.strip() for part in line.split("|")]
        if len(parts) < 4 or not parts[1]:
            continue
        description = "|".join(parts[4:]).strip() if len(parts) > 4 else ""
        columns[parts[1].upper()] = ColumnDef(parts[1].upper(), parts[2].upper(), parts[3], description)
    return columns


def unquote(field: str) -> str:
    return field[1:-1] if len(field) >= 2 and field[0] == '"' and field[-1] == '"' else field


def split_record(line: str, columns: int) -> tuple[list[str], str]:
    """The fields of one data line (its line ending removed) and how they were read: "plain" (the header's number of
    separators, so a plain split is exact), "quoted" (a quoted field holds the separator), or "padded"/"truncated"
    when even then the count of fields differs from the header's."""
    if line.count(SEPARATOR) == columns - 1:
        return [unquote(field) for field in line.split(SEPARATOR)], "plain"
    fields: list[str] = []
    i, n = 0, len(line)
    while True:
        end = -1
        if line.startswith('"', i):
            # A quoted field ends at the first quote followed by the separator or the line end.
            j = i + 1
            while (k := line.find('"', j)) != -1 and k + 1 < n and line[k + 1] != SEPARATOR:
                j = k + 1
            if k != -1:
                fields.append(line[i + 1:k])
                end = k + 1
        if end == -1:  # an unquoted field, or a quote that never closes: up to the next separator
            k = line.find(SEPARATOR, i)
            end = n if k == -1 else k
            fields.append(line[i:end])
        if end >= n:
            break
        i = end + 1  # past the separator
        if i == n:
            fields.append("")
            break
    how = "quoted"
    if len(fields) < columns:
        fields += [""] * (columns - len(fields))
        how = "padded"
    elif len(fields) > columns:
        fields = fields[:columns]
        how = "truncated"
    return fields, how


def transcode_member(zip_path: Path, target: Path) -> tuple[str, int, list[str], list[str], dict[str, int]]:
    """Write the zip's single text member to `target` as UTF-8 with unit-separated, unquoted fields: the normalized
    header, then one line per record. Returns the member's name, the count of undecodable bytes, the preamble lines
    dropped before the header, the header, and the count of records read each way (see `split_record`).

    One export (107(a) orders) is written for Excel: a "sep=|" line ended by a bare CR, a report title and a "Data as of"
    line come before the header. The header is the first line holding the separator after any "sep=" directive; its
    names are upper-cased and stripped, as the definitions write them ("Mine ID" there is "MINE ID"; another file
    writes " SUBUNIT"). Records end at LF only, so a stray CR inside a value stays inside it.
    """
    with zipfile.ZipFile(zip_path) as archive:
        members = [info for info in archive.infolist() if not info.is_dir()]
        if len(members) != 1:
            raise ValueError(f"{zip_path.name}: expected one file in the zip, found {len(members)}")
        _undecodable[0] = 0
        with archive.open(members[0]) as raw, target.open("w", encoding="utf-8", newline="\n") as out:
            text = io.TextIOWrapper(raw, encoding=SOURCE_ENCODING, errors=DECODE_ERRORS, newline="\n")
            preamble: list[str] = []
            line = text.readline()
            while line and (line.lower().startswith("sep=") or SEPARATOR not in line):
                preamble += [part.strip() for part in line.split("\r") if part.strip() and not part.lower().startswith("sep=")]
                line = text.readline()
            if not line:
                raise ValueError(f"{zip_path.name}: no header row with the '{SEPARATOR}' separator")
            header = [name.strip().strip('"').strip().upper() for name in line.rstrip("\r\n").split(SEPARATOR)]
            out.write(LAKE_SEPARATOR.join(header) + "\n")
            ways: Counter[str] = Counter()
            for line in text:
                line = line.rstrip("\r\n")
                if not line.strip():
                    continue
                fields, how = split_record(line, len(header))
                ways[how] += 1
                out.write(LAKE_SEPARATOR.join(field.replace(LAKE_SEPARATOR, " ") for field in fields) + "\n")
        return members[0].filename, _undecodable[0], preamble, header, dict(ways)


def clean(column: str) -> pl.Expr:
    trimmed = pl.col(column).str.strip_chars()
    return pl.when(trimmed.is_in(list(UNKNOWN_VALUES))).then(None).otherwise(trimmed)


def typed(column: str, definition: ColumnDef | None) -> pl.Expr:
    value = clean(column)
    if definition is None:
        return value
    if definition.oracle_type == "DATE":
        return value.str.strptime(pl.Date, DATE_FORMAT, strict=False)
    if definition.oracle_type == "NUMBER":
        return value.cast(definition.dtype, strict=False)
    return value


def build_dataset(
    name: str, zip_path: Path, definition_path: Path, out_dir: Path, source_sha256: str, source_name: str | None = None,
    *, make_current: bool = True,
) -> dict:
    """Typed Parquet of one dataset version; does nothing when that version is already in the lake in the current
    format. `make_current` marks it the dataset's current version in the manifest."""
    folder = out_dir / name
    folder.mkdir(parents=True, exist_ok=True)
    target = folder / f"{source_sha256[:16]}.parquet"
    manifest_path = folder / "manifest.json"
    manifest = {"dataset": name, "versions": []}
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    known = next((version for version in manifest["versions"] if version["source_sha256"] == source_sha256), None)
    if known and target.exists() and known.get("format") == LAKE_FORMAT:
        return {**known, "built": False}

    declared = read_definitions(definition_path)
    definitions = dict(declared)
    for (dataset, column), (length, _) in TYPE_OVERRIDES.items():
        if dataset == name and column in definitions:
            definitions[column] = ColumnDef(column, definitions[column].oracle_type, length, definitions[column].description)
    temp = folder / f".{source_sha256[:16]}.txt"
    try:
        member, undecodable, preamble, header, ways = transcode_member(zip_path, temp)
        frame = pl.scan_csv(temp, separator=LAKE_SEPARATOR, quote_char=None, infer_schema=False,
                            null_values=list(UNKNOWN_VALUES))
        columns = [(source, definitions.get(source)) for source in header]
        failures = frame.select(
            *[(clean(source).is_not_null() & typed(source, definition).is_null()).sum().alias(source)
              for source, definition in columns if definition is not None and definition.oracle_type != "VARCHAR2"],
            pl.len().alias("__rows"),
        ).collect(engine="streaming").row(0, named=True)
        source_lines = sum(ways.values())
        if failures["__rows"] != source_lines:
            raise ValueError(f"{name}: read {failures['__rows']} rows from {source_lines} source lines")
        partial = target.with_suffix(".parquet.partial")
        frame.select(*[typed(source, definition).alias(snake(source)) for source, definition in columns]).sink_parquet(
            partial, compression="zstd"
        )
        partial.replace(target)
    finally:
        temp.unlink(missing_ok=True)

    version = {
        "format": LAKE_FORMAT,
        "source_sha256": source_sha256,
        "source_file": source_name or zip_path.name,
        "member": member,
        "preamble": preamble,
        "undecodable_bytes": undecodable,
        "parquet": target.name,
        "rows": failures.pop("__rows"),
        "lines_read": ways,
        "built_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "columns": [
            {
                "name": snake(source),
                "source": source,
                "type": str(definition.dtype) if definition else "String",
                "declared": f"{declared[source].oracle_type}({declared[source].length})" if definition else None,
                "description": definition.description if definition else None,
                "cast_failures": failures.get(source, 0),
                **({"override": TYPE_OVERRIDES[(name, source)][1]} if (name, source) in TYPE_OVERRIDES else {}),
            }
            for source, definition in columns
        ],
        "undefined_columns": [source for source, definition in columns if definition is None],
        "unused_definitions": sorted(set(definitions) - set(header)),
    }
    manifest["versions"] = [item for item in manifest["versions"] if item["source_sha256"] != source_sha256] + [version]
    if make_current or "current" not in manifest:
        manifest["current"] = target.name
    manifest_path.write_text(json.dumps(manifest, indent=1), encoding="utf-8")
    return {**version, "built": True}


def mark_current(folder: Path, parquet: str) -> None:
    manifest_path = folder / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("current") != parquet:
        manifest["current"] = parquet
        manifest_path.write_text(json.dumps(manifest, indent=1), encoding="utf-8")


def object_path(data_root: Path, digest: str) -> Path:
    return data_root / "raw" / "bulk" / "objects" / digest[:2] / digest[2:4] / digest


def current_files(data_root: Path) -> dict[str, str]:
    """SHA-256 of the current, verified copy of every MSHA bulk file, by file name. A file the last check found unchanged
    (HTTP 304) keeps the copy it already had."""
    db = sqlite3.connect(f"file:{(data_root / 'state' / 'bulk.sqlite').as_posix()}?mode=ro", uri=True)
    try:
        rows = db.execute(
            "select name, sha256 from files where source = 'msha-ogd' and state in ('fetched', 'unchanged')"
            " and verified = 1"
        ).fetchall()
    finally:
        db.close()
    return {name: digest for name, digest in rows if digest}


def held_versions(data_root: Path, current: dict[str, str]) -> dict[str, list[str]]:
    """Every version held of each MSHA bulk file, oldest first and the current one last, by file name: the bulk store
    keeps each downloaded version, and the lake keeps a Parquet file for each, so consecutive versions can be compared."""
    db = sqlite3.connect(f"file:{(data_root / 'state' / 'bulk.sqlite').as_posix()}?mode=ro", uri=True)
    try:
        rows = db.execute(
            "select f.name, v.sha256 from versions v join files f on f.url = v.url"
            " where f.source = 'msha-ogd' order by v.first_seen_at, v.rowid"
        ).fetchall()
    finally:
        db.close()
    held: dict[str, list[str]] = {}
    for name, digest in rows:
        if digest and digest != current.get(name) and digest not in held.setdefault(name, []):
            held[name].append(digest)
    for name, digest in current.items():
        held.setdefault(name, []).append(digest)
    return held


def build_lake(data_root: Path, only: list[str] | None = None) -> dict[str, dict]:
    """Bring the lake up to date with every held version of every dataset (or of the ones named); the newest version
    is each dataset's current one. Definitions come from the current definition file."""
    files = current_files(data_root)
    versions = held_versions(data_root, files)
    out_dir = data_root / "lake" / "msha_ogd"
    results: dict[str, dict] = {}
    dictionary: list[dict] = []
    for name, (zip_name, definition_name) in DATASETS.items():
        if zip_name not in files or definition_name not in files:
            results[name] = {"built": False, "missing": [item for item in (zip_name, definition_name) if item not in files]}
            continue
        for definition in read_definitions(object_path(data_root, files[definition_name])).values():
            length, reason = TYPE_OVERRIDES.get((name, definition.source), (definition.length, None))
            lake_type = ColumnDef(definition.source, definition.oracle_type, length, definition.description).dtype
            dictionary.append({"dataset": name, **asdict(definition), "column": definition.name, "type": str(lake_type),
                               "override": reason})
        if only and name not in only:
            continue
        built = 0
        for digest in versions[zip_name]:
            version = build_dataset(name, object_path(data_root, digest), object_path(data_root, files[definition_name]),
                                    out_dir, digest, zip_name, make_current=False)
            built += bool(version["built"])
        mark_current(out_dir / name, version["parquet"])  # the last version is the current download
        results[name] = {key: version[key] for key in ("rows", "parquet")} | {
            "built": bool(version["built"]), "versions": len(versions[zip_name]), "versions_built": built,
            "cast_failures": sum(column["cast_failures"] for column in version["columns"]),
        }
    if dictionary:
        out_dir.mkdir(parents=True, exist_ok=True)
        # An explicit schema: "override" is null in all but a few late rows, which inference from the first rows misses.
        schema = {key: pl.String for key in ("dataset", "source", "oracle_type", "length", "description", "column", "type",
                                             "override")}
        pl.DataFrame(dictionary, schema=schema).write_parquet(out_dir / "_dictionary.parquet")
    return results
