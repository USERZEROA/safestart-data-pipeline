"""MSHA's open enforcement datasets as a typed Parquet lake.

Each bulk zip holds one pipe-delimited text file (quoted strings, CRLF lines, Windows-1252 text), and each dataset comes
with a definition file naming every column's Oracle type (VARCHAR2, DATE, NUMBER(p[,s])) and meaning. The lake keeps
one Parquet file per downloaded version, named after the source file's SHA-256, so a rebuild of an unchanged version
does nothing and every row traces back to the exact file it came from.

Cleaning is deliberately small and recorded: surrounding spaces are trimmed, the values MSHA uses for "unknown" ("?",
"NO VALUE FOUND") and empty strings become nulls, and every value that does not parse as its declared type becomes a
null that is counted per column in the manifest rather than dropped without a trace.
"""

from __future__ import annotations

import codecs
import io
import json
import re
import shutil
import sqlite3
import zipfile
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


def transcode_member(zip_path: Path, target: Path) -> tuple[str, int]:
    """Write the zip's single text member to `target` as UTF-8; returns its name and the count of undecodable bytes."""
    with zipfile.ZipFile(zip_path) as archive:
        members = [info for info in archive.infolist() if not info.is_dir()]
        if len(members) != 1:
            raise ValueError(f"{zip_path.name}: expected one file in the zip, found {len(members)}")
        _undecodable[0] = 0
        with archive.open(members[0]) as raw, target.open("w", encoding="utf-8", newline="") as out:
            text = io.TextIOWrapper(raw, encoding=SOURCE_ENCODING, errors=DECODE_ERRORS, newline="")
            shutil.copyfileobj(text, out, 1024 * 1024)
        return members[0].filename, _undecodable[0]


def header_of(path: Path) -> list[str]:
    with path.open(encoding="utf-8", newline="") as handle:
        return [name.strip().strip('"').upper() for name in handle.readline().rstrip("\r\n").split("|")]


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
    name: str, zip_path: Path, definition_path: Path, out_dir: Path, source_sha256: str, source_name: str | None = None
) -> dict:
    """Typed Parquet of one dataset version; does nothing when that version is already in the lake."""
    folder = out_dir / name
    folder.mkdir(parents=True, exist_ok=True)
    target = folder / f"{source_sha256[:16]}.parquet"
    manifest_path = folder / "manifest.json"
    manifest = {"dataset": name, "versions": []}
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    known = next((version for version in manifest["versions"] if version["source_sha256"] == source_sha256), None)
    if known and target.exists():
        return {**known, "built": False}

    definitions = read_definitions(definition_path)
    temp = folder / f".{source_sha256[:16]}.txt"
    try:
        member, undecodable = transcode_member(zip_path, temp)
        header = header_of(temp)
        frame = pl.scan_csv(temp, separator="|", quote_char='"', infer_schema=False, null_values=list(UNKNOWN_VALUES))
        columns = [(source, definitions.get(source)) for source in header]
        failures = frame.select(
            *[(clean(source).is_not_null() & typed(source, definition).is_null()).sum().alias(source)
              for source, definition in columns if definition is not None and definition.oracle_type != "VARCHAR2"],
            pl.len().alias("__rows"),
        ).collect(engine="streaming").row(0, named=True)
        partial = target.with_suffix(".parquet.partial")
        frame.select(*[typed(source, definition).alias(snake(source)) for source, definition in columns]).sink_parquet(
            partial, compression="zstd"
        )
        partial.replace(target)
    finally:
        temp.unlink(missing_ok=True)

    version = {
        "source_sha256": source_sha256,
        "source_file": source_name or zip_path.name,
        "member": member,
        "undecodable_bytes": undecodable,
        "parquet": target.name,
        "rows": failures.pop("__rows"),
        "built_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "columns": [
            {
                "name": snake(source),
                "source": source,
                "type": str(definition.dtype) if definition else "String",
                "declared": f"{definition.oracle_type}({definition.length})" if definition else None,
                "description": definition.description if definition else None,
                "cast_failures": failures.get(source, 0),
            }
            for source, definition in columns
        ],
        "undefined_columns": [source for source, definition in columns if definition is None],
        "unused_definitions": sorted(set(definitions) - set(header)),
    }
    manifest["versions"] = [item for item in manifest["versions"] if item["source_sha256"] != source_sha256] + [version]
    manifest["current"] = target.name
    manifest_path.write_text(json.dumps(manifest, indent=1), encoding="utf-8")
    return {**version, "built": True}


def object_path(data_root: Path, digest: str) -> Path:
    return data_root / "raw" / "bulk" / "objects" / digest[:2] / digest[2:4] / digest


def current_files(data_root: Path) -> dict[str, str]:
    """SHA-256 of the current, verified copy of every MSHA bulk file, by file name."""
    db = sqlite3.connect(f"file:{(data_root / 'state' / 'bulk.sqlite').as_posix()}?mode=ro", uri=True)
    try:
        rows = db.execute(
            "select name, sha256 from files where source = 'msha-ogd' and state = 'fetched' and verified = 1"
        ).fetchall()
    finally:
        db.close()
    return {name: digest for name, digest in rows if digest}


def build_lake(data_root: Path, only: list[str] | None = None) -> dict[str, dict]:
    """Bring the lake up to date with the current download of every dataset (or of the ones named)."""
    files = current_files(data_root)
    out_dir = data_root / "lake" / "msha_ogd"
    results: dict[str, dict] = {}
    dictionary: list[dict] = []
    for name, (zip_name, definition_name) in DATASETS.items():
        if zip_name not in files or definition_name not in files:
            results[name] = {"built": False, "missing": [item for item in (zip_name, definition_name) if item not in files]}
            continue
        for definition in read_definitions(object_path(data_root, files[definition_name])).values():
            dictionary.append({"dataset": name, **asdict(definition), "column": definition.name, "type": str(definition.dtype)})
        if only and name not in only:
            continue
        version = build_dataset(name, object_path(data_root, files[zip_name]), object_path(data_root, files[definition_name]),
                                out_dir, files[zip_name], zip_name)
        results[name] = {key: version[key] for key in ("built", "rows", "parquet")} | {
            "cast_failures": sum(column["cast_failures"] for column in version["columns"])
        }
    if dictionary:
        out_dir.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(dictionary).write_parquet(out_dir / "_dictionary.parquet")
    return results
