from __future__ import annotations

import hashlib
import json
import zipfile
from datetime import date

import polars as pl

from safestart_pipeline.acquire.bulk import BulkStore
from safestart_pipeline.lake.msha_ogd import build_dataset, build_lake, object_path, read_definitions

MINES_TXT = (
    "MINE_ID|CURRENT_MINE_NAME|CURRENT_STATUS_DT|NO_EMPLOYEES|LATITUDE|EXTRA_COL\r\n"
    '"0100003"|"O\'Neal Quarry & Mill"|"01/22/1979"|108|"33.177778"|"a"\r\n'
    '"0100004"|" Café Mine "|"?"|x|""|"NO VALUE FOUND"\r\n'
    '"0100005"|"Third"|"13/45/2001"|7|"-86.758889"|\r\n'
)
DEFINITION = (
    "TABLE_NAME|COLUMN_NAME|DATA_TYPE|DATA_LENGTH|FIELD_DESCRIPTION\r\n"
    "MINES|MINE_ID|VARCHAR2|7|Identification number assigned to the mine by MSHA.\r\n"
    "MINES|CURRENT_MINE_NAME|VARCHAR2|50|Name of the mine.\r\n"
    "MINES|CURRENT_STATUS_DT|DATE|10|Date the mine obtained the current status.\r\n"
    "MINES|NO_EMPLOYEES|NUMBER|5|Number of employees.\r\n"
    "MINES|LATITUDE|NUMBER|9,6|Latitude.\r\n"
    "MINES|RETIRED_COL|VARCHAR2|1|No longer in the file.\r\n"
)


def mines_zip(path):
    # "Third" carries a byte Windows-1252 leaves undefined, as a few of MSHA's real files do.
    blob = MINES_TXT.encode("cp1252").replace(b"Third", b"Thi" + bytes([0x8F]) + b"rd")
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("Mines.txt", blob)
    return path


def test_definitions_give_each_column_a_type(tmp_path):
    definition = tmp_path / "def.txt"
    definition.write_bytes(DEFINITION.encode("cp1252"))
    columns = read_definitions(definition)
    assert {name: str(column.dtype) for name, column in columns.items()} == {
        "MINE_ID": "String", "CURRENT_MINE_NAME": "String", "CURRENT_STATUS_DT": "Date", "NO_EMPLOYEES": "Int64",
        "LATITUDE": "Float64", "RETIRED_COL": "String",
    }
    assert columns["NO_EMPLOYEES"].name == "no_employees"


def test_a_dataset_becomes_typed_parquet_with_its_failures_counted(tmp_path):
    zip_path, definition = mines_zip(tmp_path / "Mines.zip"), tmp_path / "def.txt"
    definition.write_bytes(DEFINITION.encode("cp1252"))
    result = build_dataset("mines", zip_path, definition, tmp_path / "lake", "ab" * 32, "Mines.zip")
    assert result["built"] and result["rows"] == 3 and result["source_file"] == "Mines.zip"

    frame = pl.read_parquet(tmp_path / "lake" / "mines" / result["parquet"])
    assert dict(frame.schema) == {
        "mine_id": pl.String, "current_mine_name": pl.String, "current_status_dt": pl.Date, "no_employees": pl.Int64,
        "latitude": pl.Float64, "extra_col": pl.String,
    }
    rows = frame.rows(named=True)
    assert rows[0] == {"mine_id": "0100003", "current_mine_name": "O'Neal Quarry & Mill", "current_status_dt": date(1979, 1, 22),
                       "no_employees": 108, "latitude": 33.177778, "extra_col": "a"}
    # Windows-1252 text decoded, spaces trimmed, MSHA's "unknown" markers and unparseable values turned into nulls.
    assert rows[1] == {"mine_id": "0100004", "current_mine_name": "Café Mine", "current_status_dt": None, "no_employees": None,
                       "latitude": None, "extra_col": None}
    assert rows[2]["current_status_dt"] is None and rows[2]["latitude"] == -86.758889
    assert rows[2]["current_mine_name"] == "Thi" + chr(0xFFFD) + "rd" and result["undecodable_bytes"] == 1

    failures = {column["name"]: column["cast_failures"] for column in result["columns"]}
    assert failures["no_employees"] == 1 and failures["current_status_dt"] == 1 and failures["latitude"] == 0
    assert result["undefined_columns"] == ["EXTRA_COL"] and result["unused_definitions"] == ["RETIRED_COL"]

    manifest = json.loads((tmp_path / "lake" / "mines" / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["current"] == result["parquet"] and len(manifest["versions"]) == 1
    assert not list((tmp_path / "lake" / "mines").glob(".*.txt"))  # the transcoded text is not left behind

    again = build_dataset("mines", zip_path, definition, tmp_path / "lake", "ab" * 32, "Mines.zip")
    assert again["built"] is False and again["rows"] == 3


def test_the_lake_follows_the_bulk_catalog(tmp_path):
    root = tmp_path / "root"
    store = BulkStore(root / "state" / "bulk.sqlite")
    blobs = {
        "Mines.zip": mines_zip(tmp_path / "Mines.zip").read_bytes(),
        "Mines_Definition_File.txt": DEFINITION.encode("cp1252"),
    }
    for name, blob in blobs.items():
        digest = hashlib.sha256(blob).hexdigest()
        target = object_path(root, digest)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(blob)
        store.db.execute(
            "insert into files (url, source, name, listed_at, state, sha256, verified)"
            " values (?, 'msha-ogd', ?, '2026-11-19T00:00:00Z', 'fetched', ?, 1)",
            (f"https://example.test/{name}", name, digest),
        )
    store.close()

    results = build_lake(root)
    assert results["mines"]["built"] and results["mines"]["rows"] == 3 and results["mines"]["cast_failures"] == 2
    assert results["violations"] == {"built": False, "missing": ["Violations.zip", "violations_Definition_File.txt"]}
    dictionary = pl.read_parquet(root / "lake" / "msha_ogd" / "_dictionary.parquet")
    assert dictionary.filter(pl.col("column") == "no_employees")["description"].to_list() == ["Number of employees."]
    assert build_lake(root)["mines"]["built"] is False
