"""MSHA's open enforcement datasets as a typed Parquet lake.

Each bulk zip holds one pipe-delimited text file (CRLF lines, Windows-1252 text), and each dataset comes with a
definition file naming every column's Oracle type (VARCHAR2, DATE, NUMBER(p[,s])) and meaning. The lake keeps one
Parquet file per downloaded version, named after the source file's SHA-256, so a rebuild of an unchanged version does
nothing, every row traces back to the exact file it came from, and consecutive versions can be compared.

MSHA wraps text in double quotes but never escapes a quote inside it (`"54" ROLL CR. #4"`, `"Clary"s Sitework"`), and
one contractor name holds the separator itself (`"AZZ|Central Electric"`). A CSV reader that honors quotes therefore
runs one record into the next: on 2026-11-23 it kept 41,410 of the 92,129 mine records. Every record is one line, so
the lake splits each line itself: a plain split when the line has the header's number of fields and none opens a quote
it does not close, otherwise a quoted field ends only at a quote followed by the separator or the line end. A line
that still does not give the header's fields (one too few, or a non-empty field too many) fails the version rather
than shifting values between columns, and a build fails if Polars reads back a different number of rows than the
splitter wrote.

A few lines of free text were pasted as UTF-8 into the Windows-1252 files (Mines.txt writes the degree sign as two
bytes): a line whose bytes form valid UTF-8 is decoded as UTF-8, any other as Windows-1252, and the manifest counts
both, along with the bytes Windows-1252 leaves undefined (each becomes U+FFFD). Definition files are decoded line by
line the same way, so a pasted description can neither garble nor stop the build.

Cleaning is deliberately small and recorded: surrounding spaces are trimmed, the values MSHA uses for "unknown" ("?",
"NO VALUE FOUND") and empty strings become nulls, and every value that does not parse as its declared type becomes a
null that is counted per column in the manifest rather than dropped without a trace.

Each dataset's outcome is logged, and a dataset that cannot be built (its definition file unreadable, its current
version broken) fails alone: the others are still built, and LakeBuildError, carrying every dataset's result, ends the
run.
"""

from __future__ import annotations

import codecs
import hashlib
import json
import logging
import os
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
# A version built under another format is built again: 1 read the files with a quote-honoring CSV reader, 2 splits each
# line itself, 3 applies TYPE_OVERRIDES, 4 fails on misread lines, guards the plain split, finds a header after a bare
# CR, drops a byte-order mark and decodes UTF-8 lines as UTF-8.
LAKE_FORMAT = 4
# Declared types the data contradicts, each with its reason: (dataset, column) -> (Oracle length to use, why).
TYPE_OVERRIDES: dict[tuple[str, str], tuple[str, str]] = {
    ("contractor_prod_yearly", "AVG_EMPLOYEE_CNT"): (
        "9,2", "a yearly average of quarterly head counts, written with two decimals (1.33, .67) though declared NUMBER(9)"),
}
SEPARATOR = "|"
# The transcoded text separates fields with the ASCII unit separator, which MSHA's text never holds, so it needs no
# quoting at all.
LAKE_SEPARATOR = chr(0x1F)
BYTE_ORDER_MARK = chr(0xFEFF)
log = logging.getLogger(__name__)


class LakeBuildError(RuntimeError):
    """Raised after a run in which the current version of one or more datasets could not be built; `results` holds
    every dataset's outcome, those built included."""

    def __init__(self, message: str, results: dict[str, dict] | None = None):
        super().__init__(message)
        self.results = results or {}


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


@dataclass(frozen=True)
class HeldVersion:
    sha256: str
    first_seen_at: str | None
    last_modified: str | None


def snake(name: str) -> str:
    return re.sub(r"[^0-9a-z]+", "_", name.strip().lower()).strip("_")


def read_definitions(path: Path) -> dict[str, ColumnDef]:
    """Column definitions by source column name, from a pipe-delimited definition file with a header row. Each line is
    decoded as a data line is (`decode_line`): pasted UTF-8 as UTF-8, any other text as Windows-1252."""
    columns: dict[str, ColumnDef] = {}
    counts: Counter[str] = Counter()
    for raw in path.read_bytes().splitlines()[1:]:
        parts = [part.strip() for part in decode_line(raw, counts).split("|")]
        if len(parts) < 4 or not parts[1]:
            continue
        description = "|".join(parts[4:]).strip() if len(parts) > 4 else ""
        columns[parts[1].upper()] = ColumnDef(parts[1].upper(), parts[2].upper(), parts[3], description)
    return columns


_undecodable = [0]


def _count_and_replace(error: UnicodeError) -> tuple[str, int]:
    if not isinstance(error, UnicodeDecodeError):
        raise error
    _undecodable[0] += error.end - error.start
    return chr(0xFFFD) * (error.end - error.start), error.end


DECODE_ERRORS = "safestart-count-replace"
codecs.register_error(DECODE_ERRORS, _count_and_replace)


def decode_line(raw: bytes, counts: Counter[str]) -> str:
    """One line's text: ASCII as is, valid UTF-8 as UTF-8 (pasted text), anything else as Windows-1252 with each byte
    it leaves undefined replaced by U+FFFD. `counts` tallies utf8_lines and undecodable_bytes."""
    if raw.isascii():
        return raw.decode("ascii")
    try:
        text = raw.decode("utf-8")
        counts["utf8_lines"] += 1
        return text
    except UnicodeDecodeError:
        pass
    before = _undecodable[0]
    text = raw.decode(SOURCE_ENCODING, errors=DECODE_ERRORS)
    if replaced := _undecodable[0] - before:
        counts["undecodable_bytes"] += replaced
    return text


def unquote(field: str) -> str:
    return field[1:-1] if len(field) >= 2 and field[0] == '"' and field[-1] == '"' else field


def _half_open(field: str) -> bool:
    """A field that starts a quote it does not close, which means the separator after it is inside the quotes."""
    return field[:1] == '"' and (len(field) < 2 or field[-1] != '"')


def split_record(line: str, columns: int) -> tuple[list[str], str]:
    """The fields of one data line (its line ending removed) and how they were read: "plain" (a plain split gives the
    header's number of fields and no field opens a quote it does not close), "quoted" (a quoted field holds the
    separator), "trailing" (extra fields beyond the header that are all empty), or "padded"/"truncated" when the
    fields still do not match the header (the caller fails the version on those)."""
    plain = line.split(SEPARATOR)
    if len(plain) == columns and not any(_half_open(field) for field in plain):
        return [unquote(field) for field in plain], "plain"
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
    if len(fields) == columns:
        return fields, "quoted"
    if len(fields) < columns:
        return fields + [""] * (columns - len(fields)), "padded"
    if not any(field.strip() for field in fields[columns:]):
        return fields[:columns], "trailing"
    return fields[:columns], "truncated"


def _header_from(lines, counts: Counter[str], name: str) -> tuple[list[str], list[str], list[str]]:
    """Find the header: the first line part holding the separator, after any "sep=" directive and title lines. Parts
    are split on bare CRs too, since the Excel-style export ends its "sep=|" line with a CR alone. Returns the preamble,
    the header names (upper-cased and stripped, as the definitions write them) and any records that followed the header
    inside the same LF-terminated chunk."""
    preamble: list[str] = []
    first = True
    for raw in lines:
        text = decode_line(raw, counts)
        if first:
            first = False
            for mark in (BYTE_ORDER_MARK, "\xef\xbb\xbf"):  # decoded as UTF-8, or as Windows-1252
                if text.startswith(mark):
                    text = text[len(mark):]
                    counts["byte_order_mark"] += 1
        parts = text.rstrip("\r\n").split("\r")
        for index, part in enumerate(parts):
            stripped = part.strip()
            if not stripped or stripped.lower().startswith("sep="):
                continue
            if SEPARATOR in part:
                header = [column.strip().strip('"').strip().upper() for column in part.split(SEPARATOR)]
                return preamble, header, [rest for rest in parts[index + 1:] if rest.strip()]
            preamble.append(stripped)
    raise ValueError(f"{name}: no header row with the '{SEPARATOR}' separator")


def transcode_member(zip_path: Path, target: Path) -> tuple[str, list[str], list[str], dict[str, int], dict[str, int]]:
    """Write the zip's single text member to `target` as UTF-8 with unit-separated, unquoted fields: the header, then one
    line per record. Returns the member's name, the preamble dropped before the header, the header, the records read
    each way (see `split_record`), and the decoding counts (utf8_lines, undecodable_bytes, byte_order_mark).

    One export (107(a) orders) is written for Excel: a "sep=|" line ended by a bare CR, a report title and a "Data as of"
    line come before the header, whose names are readable ("Mine ID" where the definition writes "MINE ID"). Records
    end at LF only, so a stray CR inside a value stays inside it.
    """
    with zipfile.ZipFile(zip_path) as archive:
        members = [info for info in archive.infolist() if not info.is_dir()]
        if len(members) != 1:
            raise ValueError(f"{zip_path.name}: expected one file in the zip, found {len(members)}")
        counts: Counter[str] = Counter()
        ways: Counter[str] = Counter()
        with archive.open(members[0]) as raw, target.open("w", encoding="utf-8", newline="\n") as out:
            lines = iter(raw)  # a binary member iterates on LF
            preamble, header, after = _header_from(lines, counts, zip_path.name)
            out.write(LAKE_SEPARATOR.join(header) + "\n")

            def write(text: str) -> None:
                fields, how = split_record(text, len(header))
                ways[how] += 1
                out.write(LAKE_SEPARATOR.join(field.replace(LAKE_SEPARATOR, " ") for field in fields) + "\n")

            for text in after:
                write(text)
            for line in lines:
                text = decode_line(line, counts).rstrip("\r\n")
                if text.strip():
                    write(text)
        return members[0].filename, preamble, header, dict(ways), dict(counts)


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


def overrides_for(name: str) -> list[str]:
    return sorted(column for dataset, column in TYPE_OVERRIDES if dataset == name)


def _write_json(path: Path, value: dict) -> None:
    """Write through a temporary file, so a crash never leaves a truncated manifest."""
    temp = path.with_name(path.name + ".tmp")
    temp.write_text(json.dumps(value, indent=1), encoding="utf-8")
    os.replace(temp, path)


def _read_manifest(folder: Path, name: str) -> dict:
    path = folder / "manifest.json"
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {"dataset": name, "versions": []}


def _store_version(folder: Path, name: str, version: dict) -> None:
    manifest = _read_manifest(folder, name)
    versions = [item for item in manifest["versions"] if item["source_sha256"] != version["source_sha256"]] + [version]
    manifest["versions"] = sorted(versions, key=lambda item: (item.get("first_seen_at") or "", item.get("built_at") or ""))
    _write_json(folder / "manifest.json", manifest)


def build_dataset(
    name: str, zip_path: Path, definition_path: Path, out_dir: Path, source_sha256: str, source_name: str | None = None,
    *, first_seen_at: str | None = None, last_modified: str | None = None, make_current: bool = True,
) -> dict:
    """Typed Parquet of one dataset version. Nothing is done when the lake already holds that version built in the
    current format from the same definition file and overrides. The manifest lists versions oldest first.
    `make_current` marks this version current; `build_lake` passes False and marks the newest once all are built, so a
    failed run never leaves an older version current."""
    folder = out_dir / name
    folder.mkdir(parents=True, exist_ok=True)
    target = folder / f"{source_sha256[:16]}.parquet"
    definition_sha256 = hashlib.sha256(definition_path.read_bytes()).hexdigest()
    manifest = _read_manifest(folder, name)
    known = next((version for version in manifest["versions"] if version["source_sha256"] == source_sha256), None)
    if (known and target.exists() and known.get("format") == LAKE_FORMAT
            and known.get("definition_sha256") == definition_sha256 and known.get("overrides") == overrides_for(name)):
        return {**known, "built": False}

    declared = read_definitions(definition_path)
    definitions = dict(declared)
    for column in overrides_for(name):
        if column in definitions:
            length = TYPE_OVERRIDES[(name, column)][0]
            definitions[column] = ColumnDef(column, definitions[column].oracle_type, length, definitions[column].description)
    temp = folder / f".{source_sha256[:16]}.txt"
    partial = target.with_suffix(".parquet.partial")
    try:
        member, preamble, header, ways, counts = transcode_member(zip_path, temp)
        misread = ways.get("padded", 0) + ways.get("truncated", 0)
        if misread:
            raise ValueError(f"{name}: {misread} lines do not split into the header's {len(header)} fields ({ways})")
        frame = pl.scan_csv(temp, separator=LAKE_SEPARATOR, quote_char=None, infer_schema=False,
                            null_values=list(UNKNOWN_VALUES))
        columns = [(source, definitions.get(source)) for source in header]
        failures = frame.select(
            *[(clean(source).is_not_null() & typed(source, definition).is_null()).sum().alias(source)
              for source, definition in columns if definition is not None and definition.oracle_type != "VARCHAR2"],
            pl.len().alias("__rows"),
        ).collect(engine="streaming").row(0, named=True)
        written = sum(ways.values())
        if failures["__rows"] != written:  # Polars must read back every line the splitter wrote
            raise ValueError(f"{name}: read {failures['__rows']} rows back from {written} written lines")
        frame.select(*[typed(source, definition).alias(snake(source)) for source, definition in columns]).sink_parquet(
            partial, compression="zstd"
        )
        partial.replace(target)
    finally:
        temp.unlink(missing_ok=True)
        partial.unlink(missing_ok=True)

    version = {
        "format": LAKE_FORMAT,
        "source_sha256": source_sha256,
        "source_file": source_name or zip_path.name,
        "first_seen_at": first_seen_at,
        "last_modified": last_modified,
        "definition_sha256": definition_sha256,
        "overrides": overrides_for(name),
        "member": member,
        "preamble": preamble,
        "parquet": target.name,
        "rows": failures.pop("__rows"),
        "lines_read": ways,
        "decoding": counts,
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
    _store_version(folder, name, version)
    if make_current:
        mark_current(folder, target.name)
    return {**version, "built": True}


def mark_current(folder: Path, parquet: str) -> None:
    manifest = _read_manifest(folder, folder.name)
    if manifest.get("current") != parquet:
        manifest["current"] = parquet
        _write_json(folder / "manifest.json", manifest)


def object_path(data_root: Path, digest: str) -> Path:
    return data_root / "raw" / "bulk" / "objects" / digest[:2] / digest[2:4] / digest


def held_files(data_root: Path) -> tuple[dict[str, str], dict[str, str], dict[str, list[HeldVersion]]]:
    """From one read of the bulk store: the current verified copy of every MSHA file by name (kept whatever the last
    check found: a 304, an error or a refusal leaves the held copy current), the last check's state by name, and every
    version held of each file, oldest first with the current one last."""
    db = sqlite3.connect(f"file:{(data_root / 'state' / 'bulk.sqlite').as_posix()}?mode=ro", uri=True)
    try:
        db.execute("begin")
        files = db.execute(
            "select name, sha256, state, fetched_at, last_modified from files"
            " where source = 'msha-ogd' and verified = 1 and sha256 is not null"
        ).fetchall()
        rows = db.execute(
            "select f.name, v.sha256, v.first_seen_at, v.last_modified from versions v join files f on f.url = v.url"
            " where f.source = 'msha-ogd' order by v.first_seen_at, v.rowid"
        ).fetchall()
        db.execute("commit")
    finally:
        db.close()
    current = {name: digest for name, digest, *_ in files}
    states = {name: state for name, _, state, *_ in files}
    held: dict[str, list[HeldVersion]] = {}
    seen: dict[str, HeldVersion] = {}
    for name, digest, first_seen, modified in rows:
        if digest and digest != current.get(name) and all(v.sha256 != digest for v in held.get(name, [])):
            held.setdefault(name, []).append(HeldVersion(digest, first_seen, modified))
        if digest == current.get(name):
            seen[name] = HeldVersion(digest, first_seen, modified)
    for name, digest, _, fetched_at, modified in files:
        held.setdefault(name, []).append(seen.get(name, HeldVersion(digest, fetched_at, modified)))
    return current, states, held


def current_files(data_root: Path) -> dict[str, str]:
    """SHA-256 of the current, verified copy of every MSHA bulk file, by file name."""
    return held_files(data_root)[0]


def dictionary_rows(name: str, definitions: dict[str, ColumnDef]) -> list[dict]:
    """The data dictionary's rows of one dataset: every declared column with the type the lake gives it."""
    rows = []
    for definition in definitions.values():
        length, reason = TYPE_OVERRIDES.get((name, definition.source), (definition.length, None))
        lake_type = ColumnDef(definition.source, definition.oracle_type, length, definition.description).dtype
        rows.append({"dataset": name, **asdict(definition), "column": definition.name, "type": str(lake_type),
                     "override": reason})
    return rows


def build_lake(data_root: Path, only: list[str] | None = None) -> dict[str, dict]:
    """Bring the lake up to date with every held version of every dataset (or of the ones named); the newest version
    is each dataset's current one. Definitions come from the current definition file. An older version that cannot be
    built is reported and skipped; if a dataset's definition file cannot be read or its current version cannot be
    built, the other datasets are still built and LakeBuildError is raised at the end (a definition file only fails
    the run when its dataset is among those built). Every dataset's outcome is logged."""
    files, states, held = held_files(data_root)
    out_dir = data_root / "lake" / "msha_ogd"
    results: dict[str, dict] = {}
    dictionary: list[dict] = []
    failed: dict[str, str] = {}
    for name, (zip_name, definition_name) in DATASETS.items():
        asked = not only or name in only
        if zip_name not in files or definition_name not in files:
            results[name] = {"built": False, "missing": [item for item in (zip_name, definition_name) if item not in files]}
            if asked:
                log.warning("%s: not built, no verified download of %s", name, " or ".join(results[name]["missing"]))
            continue
        definition_path = object_path(data_root, files[definition_name])
        try:
            definitions = read_definitions(definition_path)
        except (OSError, ValueError) as problem:
            error = f"definition file {definition_name}: {problem}"[:300]
            results[name] = {"built": False, "error": error, "last_check": states.get(definition_name)}
            if asked:
                failed[name] = error
            log.error("%s: not built, %s", name, error)
            continue
        dictionary += dictionary_rows(name, definitions)
        if not asked:
            continue
        *older, newest = held[zip_name]
        built, skipped = 0, []
        for version in older:
            try:
                outcome = build_dataset(name, object_path(data_root, version.sha256), definition_path, out_dir,
                                        version.sha256, zip_name, first_seen_at=version.first_seen_at,
                                        last_modified=version.last_modified, make_current=False)
                built += bool(outcome["built"])
            except (OSError, ValueError, zipfile.BadZipFile) as problem:
                skipped.append({"version": version.sha256[:16], "reason": str(problem)[:300]})
                log.warning("%s: older version %s skipped: %s", name, version.sha256[:16], skipped[-1]["reason"])
        try:
            outcome = build_dataset(name, object_path(data_root, newest.sha256), definition_path, out_dir, newest.sha256,
                                    zip_name, first_seen_at=newest.first_seen_at, last_modified=newest.last_modified,
                                    make_current=False)
        except (OSError, ValueError, zipfile.BadZipFile) as problem:
            failed[name] = str(problem)[:300]
            results[name] = {"built": False, "error": failed[name], "versions": len(held[zip_name]),
                             "skipped_versions": skipped, "last_check": states.get(zip_name)}
            log.error("%s: current version %s not built: %s", name, newest.sha256[:16], failed[name])
            continue
        built += bool(outcome["built"])
        mark_current(out_dir / name, outcome["parquet"])
        results[name] = {key: outcome[key] for key in ("rows", "parquet", "lines_read")} | {
            "built": bool(outcome["built"]), "versions": len(held[zip_name]), "versions_built": built,
            "cast_failures": sum(column["cast_failures"] for column in outcome["columns"]),
            "last_check": states.get(zip_name),
            **({"skipped_versions": skipped} if skipped else {}),
        }
        log.info("%s: %s current, %d rows; %d of %d versions built now", name, outcome["parquet"], outcome["rows"],
                 built, len(held[zip_name]))
    if dictionary:
        out_dir.mkdir(parents=True, exist_ok=True)
        # An explicit schema: "override" is null in all but a few late rows, which inference from the first rows misses.
        schema = {key: pl.String for key in ("dataset", "source", "oracle_type", "length", "description", "column", "type",
                                             "override")}
        temp = out_dir / "_dictionary.parquet.tmp"
        pl.DataFrame(dictionary, schema=schema).write_parquet(temp)
        os.replace(temp, out_dir / "_dictionary.parquet")
    if failed:
        raise LakeBuildError(f"current version not built for {sorted(failed)}: {failed}", results)
    return results
