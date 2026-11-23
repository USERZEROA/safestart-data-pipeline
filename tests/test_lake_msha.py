from __future__ import annotations

import hashlib
import json
import zipfile
from datetime import date

import polars as pl
import pytest

from safestart_pipeline.acquire.bulk import BulkStore
from safestart_pipeline.lake.msha_ogd import (
    LakeBuildError,
    build_dataset,
    build_lake,
    object_path,
    read_definitions,
    split_record,
)

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


# The 107(a) export is written for Excel: a "sep=" line ended by a bare CR, a title and a "Data as of" line before the
# header, readable column names in mixed case (one with a leading space here), and a tab before IDs to keep their zeros.
ORDERS_CSV = (
    "sep=|\r"
    "107(a) Order Issued Report\r\n"
    "Data as of 20-NOV-26 - Violations issued through 20-NOV-26\r\n"
    "Coal or Metal|Mine ID|Operator Name (Violations)|Violation No.|Violation Date Issued| Termination Date\r\n"
    '"C"|"\t4609612"|"Onyx Energy, LLC"|"\t9837820"|"01/08/2025"|"01/08/2025"\r\n'
    '"M"|"\t0102147"|"Chaparral Sand & Gravel"|"\t8677568"|"07/17/2012"|""\r\n'
)
ORDERS_DEFINITION = (
    "TABLE_NAME|COLUMN_NAME|DATA_TYPE|DATA_LENGTH|FIELD_DESCRIPTION\r\n"
    "107(a) ORDERS ISSUED|COAL OR METAL|VARCHAR2|1|Coal or metal/non-metal mine.\r\n"
    "107(a) ORDERS ISSUED|MINE ID|VARCHAR2|7|Mine identification number.\r\n"
    "107(a) ORDERS ISSUED|OPERATOR NAME (VIOLATIONS)|VARCHAR2|60|Operator at the time of the violation.\r\n"
    "107(a) ORDERS ISSUED|VIOLATION NO.|VARCHAR2|7|Order number.\r\n"
    "107(a) ORDERS ISSUED|VIOLATION DATE ISSUED|DATE|10|Date the order was issued.\r\n"
    "107(a) ORDERS ISSUED|TERMINATION DATE|DATE|10|Date the order was terminated.\r\n"
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
    assert rows[2]["current_mine_name"] == "Thi" + chr(0xFFFD) + "rd" and result["decoding"]["undecodable_bytes"] == 1

    failures = {column["name"]: column["cast_failures"] for column in result["columns"]}
    assert failures["no_employees"] == 1 and failures["current_status_dt"] == 1 and failures["latitude"] == 0
    assert result["undefined_columns"] == ["EXTRA_COL"] and result["unused_definitions"] == ["RETIRED_COL"]

    manifest = json.loads((tmp_path / "lake" / "mines" / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["current"] == result["parquet"] and len(manifest["versions"]) == 1
    assert not list((tmp_path / "lake" / "mines").glob(".*.txt"))  # the transcoded text is not left behind

    again = build_dataset("mines", zip_path, definition, tmp_path / "lake", "ab" * 32, "Mines.zip")
    assert again["built"] is False and again["rows"] == 3


def test_an_export_written_for_excel_loses_its_preamble_and_keeps_its_types(tmp_path):
    zip_path, definition = tmp_path / "OrdersIssued.zip", tmp_path / "def.txt"
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr("107(a)OrdersIssued.csv", ORDERS_CSV.encode("cp1252"))
    definition.write_bytes(ORDERS_DEFINITION.encode("cp1252"))
    result = build_dataset("orders_107a", zip_path, definition, tmp_path / "lake", "cd" * 32, "OrdersIssued.zip")
    assert result["built"] and result["rows"] == 2 and result["member"] == "107(a)OrdersIssued.csv"
    assert result["preamble"] == ["107(a) Order Issued Report", "Data as of 20-NOV-26 - Violations issued through 20-NOV-26"]
    assert result["undefined_columns"] == [] and result["unused_definitions"] == []

    frame = pl.read_parquet(tmp_path / "lake" / "orders_107a" / result["parquet"])
    assert dict(frame.schema) == {
        "coal_or_metal": pl.String, "mine_id": pl.String, "operator_name_violations": pl.String, "violation_no": pl.String,
        "violation_date_issued": pl.Date, "termination_date": pl.Date,
    }
    rows = frame.rows(named=True)
    assert rows[0] == {"coal_or_metal": "C", "mine_id": "4609612", "operator_name_violations": "Onyx Energy, LLC",
                       "violation_no": "9837820", "violation_date_issued": date(2025, 1, 8), "termination_date": date(2025, 1, 8)}
    assert rows[1]["mine_id"] == "0102147" and rows[1]["termination_date"] is None  # leading zeros kept, empty is null


def test_a_record_keeps_its_unescaped_quotes_and_a_quoted_separator():
    # MSHA never escapes a quote inside text, and one contractor name holds the separator itself.
    assert split_record('"1102923"|"54" ROLL CR. #4"|"Pres"||304', 5) == (
        ["1102923", '54" ROLL CR. #4', "Pres", "", "304"], "plain")
    assert split_record('"A5304"|"AZZ|Central Electric"|"2012"|2', 4) == (["A5304", "AZZ|Central Electric", "2012", "2"], "quoted")
    assert split_record('"A"|"B|C"s"|"D"', 3) == (["A", 'B|C"s', "D"], "quoted")
    assert split_record('"A"|"B"|"C"|"D"', 3) == (["A", "B", "C"], "truncated")
    assert split_record('"A"|"B|C"', 4) == (["A", "B|C", "", ""], "padded")
    assert split_record('"A"|"B"|', 2) == (["A", "B"], "trailing")  # extra fields that are all empty
    # The header's count of fields alone does not make a plain split safe: here the quoted name holds the separator
    # and the last field is missing, so a plain split would shift values between columns.
    assert split_record('"A5304"|"AZZ|Central Electric"|0', 4) == (["A5304", "AZZ|Central Electric", "0", ""], "padded")


def build_member(tmp_path, member_bytes, definition_text, name="mines"):
    zip_path, definition = tmp_path / f"{name}.zip", tmp_path / f"{name}.def.txt"
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr("Data.txt", member_bytes)
    definition.write_bytes(definition_text.encode("cp1252"))
    return build_dataset(name, zip_path, definition, tmp_path / "lake", hashlib.sha256(member_bytes).hexdigest())


TWO_COLUMNS = "T|COLUMN_NAME|DATA_TYPE|DATA_LENGTH|D\r\nT|MINE_ID|VARCHAR2|7|Mine.\r\nT|NAME|VARCHAR2|50|Name.\r\n"


def test_a_line_that_does_not_fit_the_header_fails_the_version(tmp_path):
    with pytest.raises(ValueError, match="do not split"):
        build_member(tmp_path, b'MINE_ID|NAME\r\n"0100003"|"First"\r\n"0100004"\r\n', TWO_COLUMNS)
    assert not list((tmp_path / "lake" / "mines").glob("*.parquet*"))  # nothing half-written is left


def test_the_header_may_follow_a_sep_line_ended_by_a_bare_cr(tmp_path):
    result = build_member(tmp_path, b'sep=|\rMINE_ID|NAME\r\n"0100003"|"First"\r\n"0100004"|"Second"\r\n', TWO_COLUMNS)
    assert result["rows"] == 2 and result["undefined_columns"] == [] and result["preamble"] == []


def test_a_byte_order_mark_and_pasted_utf8_text_are_read_as_such(tmp_path):
    member = '﻿MINE_ID|NAME\r\n"0100003"|"Pit 45° North"\r\n'.encode("utf-8") + '"0100004"|"Café"\r\n'.encode("cp1252")
    result = build_member(tmp_path, member, TWO_COLUMNS)
    frame = pl.read_parquet(tmp_path / "lake" / "mines" / result["parquet"])
    assert frame["mine_id"].to_list() == ["0100003", "0100004"] and frame["name"].to_list() == ["Pit 45° North", "Café"]
    assert result["decoding"] == {"byte_order_mark": 1, "utf8_lines": 2}


def test_a_new_definition_file_rebuilds_the_version(tmp_path):
    member = b'MINE_ID|NAME\r\n"0100003"|"First"\r\n'
    assert build_member(tmp_path, member, TWO_COLUMNS)["built"]
    assert build_member(tmp_path, member, TWO_COLUMNS)["built"] is False
    assert build_member(tmp_path, member, TWO_COLUMNS.replace("Name.", "Mine name."))["built"]


CONTRACTORS_TXT = (
    "CONTRACTOR_ID|CONTRACTOR_NAME| SUBUNIT|HOURS_WORKED\r\n"
    '"R220"|"Clary"s Sitework & Development, Inc."|"STRIP, QUARY, OPEN PIT"|4266\r\n'
    '"A5304"|"AZZ|Central Electric"|"SURFACE AT UNDERGROUND"|0\r\n'
    '"A0452"|"Christensen"s Electrical Services"|"MILL OPERATION/PREPARATION PLANT"|272\r\n'
)
CONTRACTORS_DEFINITION = (
    "TABLE_NAME|COLUMN_NAME|DATA_TYPE|DATA_LENGTH|FIELD_DESCRIPTION\r\n"
    "CONTRACTOR_PROD|CONTRACTOR_ID|VARCHAR2|5|Contractor identifier.\r\n"
    "CONTRACTOR_PROD|CONTRACTOR_NAME|VARCHAR2|60|Contractor name.\r\n"
    "CONTRACTOR_PROD|SUBUNIT|VARCHAR2|40|Kind of work.\r\n"
    "CONTRACTOR_PROD|HOURS_WORKED|NUMBER|10|Hours worked.\r\n"
)


def test_stray_quotes_do_not_run_records_together(tmp_path):
    # A reader that honors quotes reads these three records as one: the quote after "Clary" opens a field that the
    # next record's quotes keep open.
    zip_path, definition = tmp_path / "ContractorProdQuarterly.zip", tmp_path / "def.txt"
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr("ContractorProdQuarterly.txt", CONTRACTORS_TXT.encode("cp1252"))
    definition.write_bytes(CONTRACTORS_DEFINITION.encode("cp1252"))
    result = build_dataset("contractor_prod_quarterly", zip_path, definition, tmp_path / "lake", "ef" * 32)
    assert result["rows"] == 3 and result["lines_read"] == {"plain": 2, "quoted": 1}
    frame = pl.read_parquet(tmp_path / "lake" / "contractor_prod_quarterly" / result["parquet"])
    assert frame["contractor_name"].to_list() == [
        'Clary"s Sitework & Development, Inc.', "AZZ|Central Electric", 'Christensen"s Electrical Services']
    assert frame["subunit"].to_list()[1] == "SURFACE AT UNDERGROUND" and frame["hours_worked"].to_list() == [4266, 0, 272]


def test_a_declared_type_the_data_contradicts_is_overridden_on_record(tmp_path):
    # MSHA declares the yearly contractor head count NUMBER(9), but it is an average written with two decimals.
    zip_path, definition = tmp_path / "ContractorProdYearly.zip", tmp_path / "def.txt"
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr("ContractorProdYearly.txt", 'CONTRACTOR_ID|AVG_EMPLOYEE_CNT\r\n"A0452"|1.33\r\n"A0453"|.67\r\n"A0454"|3\r\n')
    definition.write_bytes(("TABLE_NAME|COLUMN_NAME|DATA_TYPE|DATA_LENGTH|FIELD_DESCRIPTION\r\n"
                            "CP|CONTRACTOR_ID|VARCHAR2|5|Contractor.\r\nCP|AVG_EMPLOYEE_CNT|NUMBER|9|Average employees.\r\n").encode())
    result = build_dataset("contractor_prod_yearly", zip_path, definition, tmp_path / "lake", "12" * 32)
    frame = pl.read_parquet(tmp_path / "lake" / "contractor_prod_yearly" / result["parquet"])
    assert frame["avg_employee_cnt"].to_list() == [1.33, 0.67, 3.0]
    column = next(c for c in result["columns"] if c["name"] == "avg_employee_cnt")
    assert column["type"] == "Float64" and column["declared"] == "NUMBER(9)" and column["cast_failures"] == 0
    assert "two decimals" in column["override"]


def test_the_lake_follows_the_bulk_catalog(tmp_path):
    root = tmp_path / "root"
    store = BulkStore(root / "state" / "bulk.sqlite")
    blobs = {
        "Mines.zip": mines_zip(tmp_path / "Mines.zip").read_bytes(),
        "Mines_Definition_File.txt": DEFINITION.encode("cp1252"),
    }
    # The last check found the definition unchanged (HTTP 304): the copy already held is still the current one.
    states = {"Mines.zip": "fetched", "Mines_Definition_File.txt": "unchanged"}
    for name, blob in blobs.items():
        digest = hashlib.sha256(blob).hexdigest()
        target = object_path(root, digest)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(blob)
        store.db.execute(
            "insert into files (url, source, name, listed_at, state, sha256, verified)"
            " values (?, 'msha-ogd', ?, '2026-11-19T00:00:00Z', ?, ?, 1)",
            (f"https://example.test/{name}", name, states[name], digest),
        )
    store.close()

    results = build_lake(root)
    assert results["mines"]["built"] and results["mines"]["rows"] == 3 and results["mines"]["cast_failures"] == 2
    assert results["mines"]["versions"] == 1
    assert results["violations"] == {"built": False, "missing": ["Violations.zip", "violations_Definition_File.txt"]}
    dictionary = pl.read_parquet(root / "lake" / "msha_ogd" / "_dictionary.parquet")
    assert dictionary.filter(pl.col("column") == "no_employees")["description"].to_list() == ["Number of employees."]
    assert dictionary.schema["override"] == pl.String and dictionary["override"].null_count() == dictionary.height
    assert build_lake(root)["mines"]["built"] is False


def test_every_held_version_is_in_the_lake_and_the_newest_is_current(tmp_path):
    root = tmp_path / "root"
    store = BulkStore(root / "state" / "bulk.sqlite")
    old_blob = mines_zip(tmp_path / "old.zip").read_bytes()
    with zipfile.ZipFile(tmp_path / "new.zip", "w") as archive:  # the newer release adds a mine
        archive.writestr("Mines.txt", (MINES_TXT + '"0100006"|"Fourth"|"02/02/2002"|3|"30.5"|"b"\r\n').encode("cp1252"))
    new_blob = (tmp_path / "new.zip").read_bytes()
    definition = DEFINITION.encode("cp1252")
    for blob in (old_blob, new_blob, definition):
        target = object_path(root, hashlib.sha256(blob).hexdigest())
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(blob)
    for name, blob in (("Mines.zip", new_blob), ("Mines_Definition_File.txt", definition)):
        store.db.execute(
            "insert into files (url, source, name, listed_at, state, sha256, verified)"
            " values (?, 'msha-ogd', ?, '2026-11-23T00:00:00Z', 'fetched', ?, 1)",
            (f"https://example.test/{name}", name, hashlib.sha256(blob).hexdigest()),
        )
    # The new release is recorded first here on purpose: the manifest orders versions by when they were first seen.
    for blob, seen in ((new_blob, "2026-11-23T13:00:00Z"), (old_blob, "2026-11-18T00:00:00Z")):
        store.db.execute("insert into versions (url, sha256, content_length, first_seen_at) values (?, ?, ?, ?)",
                         ("https://example.test/Mines.zip", hashlib.sha256(blob).hexdigest(), len(blob), seen))
    # An even older version whose file is gone is reported and skipped, without stopping the build.
    store.db.execute("insert into versions (url, sha256, content_length, first_seen_at) values (?, ?, ?, ?)",
                     ("https://example.test/Mines.zip", "f" * 64, 1, "2026-11-10T00:00:00Z"))
    store.close()

    results = build_lake(root, only=["mines"])
    assert results["mines"]["versions"] == 3 and results["mines"]["versions_built"] == 2 and results["mines"]["rows"] == 4
    assert [item["version"] for item in results["mines"]["skipped_versions"]] == ["f" * 16]
    manifest = json.loads((root / "lake" / "msha_ogd" / "mines" / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["current"] == hashlib.sha256(new_blob).hexdigest()[:16] + ".parquet"
    assert [version["rows"] for version in manifest["versions"]] == [3, 4]  # oldest first

    # A version built under an older lake format is built again; the current version stays the newest.
    for version in manifest["versions"]:
        if version["rows"] == 3:
            version.pop("format")
    (root / "lake" / "msha_ogd" / "mines" / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    again = build_lake(root, only=["mines"])
    assert again["mines"]["versions_built"] == 1 and again["mines"]["built"] is False
    manifest = json.loads((root / "lake" / "msha_ogd" / "mines" / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["current"] == hashlib.sha256(new_blob).hexdigest()[:16] + ".parquet"


def test_a_current_version_that_cannot_be_built_fails_the_run_after_the_others(tmp_path):
    root = tmp_path / "root"
    store = BulkStore(root / "state" / "bulk.sqlite")
    blobs = {
        "Mines.zip": mines_zip(tmp_path / "Mines.zip").read_bytes(),
        "Mines_Definition_File.txt": DEFINITION.encode("cp1252"),
        "Accidents.zip": b"not a zip file",
        "Accidents_Definition_File.txt": DEFINITION.encode("cp1252"),
    }
    for name, blob in blobs.items():
        digest = hashlib.sha256(blob).hexdigest()
        target = object_path(root, digest)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(blob)
        store.db.execute(
            "insert into files (url, source, name, listed_at, state, sha256, verified)"
            " values (?, 'msha-ogd', ?, '2026-11-23T00:00:00Z', 'error', ?, 1)",  # the last check failed: copy kept
            (f"https://example.test/{name}", name, digest),
        )
    store.close()
    with pytest.raises(LakeBuildError, match="accidents"):
        build_lake(root)
    assert (root / "lake" / "msha_ogd" / "mines" / "manifest.json").exists()  # the other datasets were built
    assert (root / "lake" / "msha_ogd" / "_dictionary.parquet").exists()
