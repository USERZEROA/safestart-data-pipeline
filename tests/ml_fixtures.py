"""Synthetic lakes and panels for the ML tests: small, seeded, written to temporary directories."""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import polars as pl

from safestart_pipeline.ml.features import CATEGORICAL, NUMERIC, TARGET

SCHEMAS = {
    "mines": {"mine_id": pl.String, "current_mine_name": pl.String, "coal_metal_ind": pl.String,
              "current_mine_type": pl.String, "current_mine_status": pl.String, "state": pl.String,
              "district": pl.String, "primary_canvass": pl.String},
    "inspections": {"event_no": pl.String, "mine_id": pl.String, "inspection_begin_dt": pl.Date},
    "violations": {"violation_no": pl.String, "mine_id": pl.String, "violation_issue_dt": pl.Date,
                   "sig_sub": pl.String, "part_section": pl.String, "proposed_penalty": pl.Float64,
                   "bill_print_dt": pl.Date, "coal_metal_ind": pl.String},
    "accidents": {"document_no": pl.String, "mine_id": pl.String, "accident_dt": pl.Date,
                  "degree_injury_cd": pl.String, "days_lost": pl.Int64, "days_restrict": pl.Int64,
                  "contractor_id": pl.String, "coal_metal_ind": pl.String},
    "mines_prod_quarterly": {"mine_id": pl.String, "cal_yr": pl.Int64, "cal_qtr": pl.Int64, "subunit_cd": pl.String,
                             "hours_worked": pl.Int64, "avg_employee_cnt": pl.Float64, "coal_metal_ind": pl.String},
}


def write_lake(root: Path, rows: dict[str, list[dict]]) -> Path:
    """A lake of one version per dataset, laid out as `ssp lake` writes it (manifest naming the current Parquet)."""
    lake = root / "lake" / "msha_ogd"
    for name, schema in SCHEMAS.items():
        folder = lake / name
        folder.mkdir(parents=True, exist_ok=True)
        frame = pl.DataFrame(rows.get(name, []), schema=schema)
        frame.write_parquet(folder / "v1.parquet")
        manifest = {"dataset": name, "current": "v1.parquet",
                    "versions": [{"parquet": "v1.parquet", "rows": frame.height}]}
        (folder / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return lake


def mine(mine_id: str, coal_metal: str = "C", **values: str) -> dict:
    return {"mine_id": mine_id, "current_mine_name": f"Mine {mine_id}", "coal_metal_ind": coal_metal,
            "current_mine_type": "Underground", "current_mine_status": "Active", "state": "WV", "district": "C04",
            "primary_canvass": "Coal"} | values


def production(mine_id: str, first: tuple[int, int], last: tuple[int, int], hours: int = 1000, employees: float = 5.0,
               coal_metal: str = "C") -> list[dict]:
    rows = []
    year, quarter = first
    while (year, quarter) <= last:
        rows.append({"mine_id": mine_id, "cal_yr": year, "cal_qtr": quarter, "subunit_cd": "01",
                     "hours_worked": hours, "avg_employee_cnt": employees, "coal_metal_ind": coal_metal})
        year, quarter = (year + 1, 1) if quarter == 4 else (year, quarter + 1)
    return rows


def violation(number: str, mine_id: str, issued: date, *, ss: str = "N", section: str = "75.400",
              penalty: float | None = None, billed: date | None = None, coal_metal: str = "C") -> dict:
    return {"violation_no": number, "mine_id": mine_id, "violation_issue_dt": issued, "sig_sub": ss,
            "part_section": section, "proposed_penalty": penalty, "bill_print_dt": billed, "coal_metal_ind": coal_metal}


def accident(number: str, mine_id: str, when: date, degree: str | None, *, days_lost: int | None = None,
             days_restrict: int | None = None, contractor: str | None = None, coal_metal: str = "C") -> dict:
    return {"document_no": number, "mine_id": mine_id, "accident_dt": when, "degree_injury_cd": degree,
            "days_lost": days_lost, "days_restrict": days_restrict, "contractor_id": contractor,
            "coal_metal_ind": coal_metal}


def generated_lake(root: Path, mines: int = 40, seed: int = 0) -> Path:
    """Mines of random size reporting 2000Q1 to 2025Q2, with inspections, violations and accidents whose rates grow
    with size, so a model has something to learn."""
    rng = np.random.default_rng(seed)
    quarters = [(year, quarter) for year in range(2000, 2026) for quarter in range(1, 5)
                if (year, quarter) <= (2025, 2)]
    rows: dict[str, list[dict]] = {name: [] for name in SCHEMAS}
    for index in range(mines):
        mine_id = f"{index + 1:07d}"
        coal_metal = "C" if index % 2 else "M"
        rows["mines"].append(mine(mine_id, coal_metal, state=("WV", "KY", "AZ")[index % 3]))
        size = float(rng.lognormal(8.5, 1.0))  # hours per quarter
        for year, quarter in quarters:
            start = date(year, 3 * quarter - 2, 1)
            hours = int(size * rng.uniform(0.8, 1.2))
            rows["mines_prod_quarterly"] += production(mine_id, (year, quarter), (year, quarter), hours, hours / 500,
                                                       coal_metal)
            for n in range(rng.poisson(1.0)):
                rows["inspections"].append({"event_no": f"{mine_id}-{year}{quarter}-{n}", "mine_id": mine_id,
                                            "inspection_begin_dt": start + timedelta(days=int(rng.integers(0, 89)))})
            for n in range(rng.poisson(size / 4000)):
                issued = start + timedelta(days=int(rng.integers(0, 89)))
                rows["violations"].append(violation(
                    f"{mine_id}-{year}{quarter}-{n}", mine_id, issued, ss=str(rng.choice(["Y", "N"])),
                    section=str(rng.choice(["75.400", "75.202(a)", "56.14107(a)"])),
                    penalty=float(rng.integers(100, 5000)),
                    billed=issued + timedelta(days=50), coal_metal=coal_metal))
            if rng.random() < 1 - np.exp(-size / 30000):
                rows["accidents"].append(accident(f"{mine_id}-{year}{quarter}", mine_id,
                                                  start + timedelta(days=int(rng.integers(0, 89))), "03",
                                                  days_lost=int(rng.integers(1, 30)), coal_metal=coal_metal))
    return write_lake(root, rows)


def synthetic_panel(mines: int = 150, first_year: int = 2001, last_year: int = 2025, seed: int = 0) -> pl.DataFrame:
    """A panel shaped like `build_panel`'s, with a target that depends on hours worked and past serious accidents, and
    an open quarter (null target) after the last year."""
    rng = np.random.default_rng(seed)
    quarters = np.arange(first_year * 4, (last_year + 1) * 4 + 1)
    mine_ids = np.repeat([f"{index:07d}" for index in range(mines)], len(quarters))
    quarter = np.tile(quarters, mines)
    size = np.repeat(rng.lognormal(9, 1.2, mines), len(quarters))
    rows = len(quarter)
    numeric = {name: rng.poisson(2, rows).astype(float) for name in NUMERIC}
    numeric |= {"hours_worked_4q": size * 4, "hours_worked_1q": size, "avg_employees_4q": size / 500,
                "serious_accidents_4q": rng.poisson(size / 40000).astype(float)}
    logit = -6 + 0.5 * np.log(size) + 0.4 * numeric["serious_accidents_4q"]
    target = rng.random(rows) < 1 / (1 + np.exp(-logit))
    frame = pl.DataFrame({"mine_id": mine_ids, "quarter": quarter.astype(np.int32), **numeric,
                          **{name: rng.choice(["a", "b"], rows) for name in CATEGORICAL}, TARGET: target})
    return frame.with_columns(
        (pl.col("quarter") // 4).cast(pl.Int16).alias("year"),
        pl.when(pl.col("quarter") < quarters[-1]).then(pl.col(TARGET)).alias(TARGET),
    )
