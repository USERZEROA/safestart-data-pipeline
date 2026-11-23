"""MSHA's enforcement records as events: one mapping for the replay and the version watcher.

The replay (`stream/replay.py`, mode `replay`) sends the lake's history, and the version watcher (`watch/versions.py`,
mode `live`) the records each new dataset version adds. A record must become the same event whichever path finds it,
or the consumers would store and count it by whichever path came first. So everything an event takes from a record is
stated here once, for inspections, violations, 107(a) orders and accidents: the lake dataset, the key column, the
source date, the columns read, how a record becomes a title and details (the details as docs/ARCHITECTURE.md lists
them), the SQL that reads them, and `event()`. The paths differ only in `mode` and `source`.

- When: a violation's issue date, else the date it occurred; an inspection's begin date, else its end date; an order's
  issue date; an accident's date. A record without one is left out.
- Where: district, state and name come from the lake's current mines dataset. A 107(a) order whose mine is missing
  there keeps its own state, and a violation or order its own mine name.
- Commodity, a serious accident, the cited section and S&S follow `definitions`.
- One copy per key: MSHA's exports repeat a few keys (51 violation numbers and one inspection number in November 2026,
  their copies differing only in columns no event reads, such as the docket or the controller). The copy kept is the
  first by source date, undated last, then by every column the event reads, so both paths keep the same copy wherever
  the copies fall, and copies that tie make the same event.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date
from typing import Any

from ..definitions import SIG_SUB, cfr_section, commodity, is_serious
from .events import Event

Row = Mapping[str, Any]  # one record as `mine_sql` reads it
Details = dict[str, Any]

# A title names the degree of injury in words; the codes are MSHA's (see `definitions`).
DEGREES = {
    "00": "Accident without injury", "01": "Fatal accident", "02": "Permanent disability", "03": "Days-away injury",
    "04": "Days-away and restricted injury", "05": "Restricted-duty injury", "06": "Injury without lost time",
    "07": "Occupational illness", "08": "Natural-causes injury", "09": "Non-employee injury", "10": "Other injury",
}


def iso_date(value: date | None) -> str | None:
    return value.isoformat() if value else None


def describe_inspection(row: Row, place: str) -> tuple[str, Details]:
    return f"{row['activity'] or 'Inspection'} at {place}", {
        "activity_code": row["activity_code"], "activity": row["activity"],
        "begin_date": iso_date(row["inspection_begin_dt"]), "end_date": iso_date(row["inspection_end_dt"])}


def describe_violation(row: Row, place: str) -> tuple[str, Details]:
    section, sig_sub = cfr_section(row["part_section"]), SIG_SUB.get(row["sig_sub"])
    cited = f" of 30 CFR {section}" if section else ""
    return f"{'S&S violation' if sig_sub else 'Violation'}{cited} at {place}", {
        "event_no": row["event_no"], "cfr_section": section, "part_section": row["part_section"], "sig_sub": sig_sub,
        "likelihood": row["likelihood"], "injury_illness": row["inj_illness"], "negligence": row["negligence"],
        "proposed_penalty": row["proposed_penalty"]}


def describe_order(row: Row, place: str) -> tuple[str, Details]:
    return f"Imminent-danger order (107(a)) at {place}", {"termination_date": iso_date(row["termination_date"])}


def describe_accident(row: Row, place: str) -> tuple[str, Details]:
    degree, classification = row["degree_injury_cd"], row["classification"]
    what = f"{DEGREES.get(degree, 'Accident')} at {place}"
    return (f"{what}: {classification.capitalize()}" if classification else what), {
        "degree_injury_cd": degree, "degree_injury": row["degree_injury"], "serious": is_serious(degree),
        "classification": classification, "accident_type": row["accident_type"], "no_injuries": row["no_injuries"],
        "days_lost": row["days_lost"]}


@dataclass(frozen=True)
class Kind:
    """How the records of one lake dataset become events of one kind."""

    name: str  # the event kind
    dataset: str  # the lake dataset
    key: str  # the key column: the record's identifier, so the event id is `<name>:<key>`
    date: str  # SQL over the record `r`: when it happened at the source
    columns: tuple[str, ...]  # the record's columns that `describe` reads
    describe: Callable[[Row, str], tuple[str, Details]]  # (record, place) -> (title, details)
    own_coal_metal: str = "coal_metal_ind"  # the record's own coal/metal flag, for a mine the mines dataset lacks
    own_state: str | None = None  # the record's own state, likewise
    own_mine_name: str | None = None  # the record's own mine name, likewise


# In the order the replay sends a source day's records: inspections first (violations are cited during them).
KINDS = (
    Kind("inspection", "inspections", "event_no", "coalesce(r.inspection_begin_dt, r.inspection_end_dt)",
         ("activity_code", "activity", "inspection_begin_dt", "inspection_end_dt"), describe_inspection),
    Kind("violation", "violations", "violation_no", "coalesce(r.violation_issue_dt, r.violation_occur_dt)",
         ("event_no", "part_section", "sig_sub", "likelihood", "inj_illness", "negligence", "proposed_penalty"),
         describe_violation, own_mine_name="mine_name"),
    Kind("order_107a", "orders_107a", "violation_no", "r.violation_date_issued", ("termination_date",), describe_order,
         own_coal_metal="coal_or_metal", own_state="state_abbreviation", own_mine_name="mine_name"),
    Kind("accident", "accidents", "document_no", "r.accident_dt",
         ("degree_injury_cd", "degree_injury", "classification", "accident_type", "no_injuries", "days_lost"),
         describe_accident),
)
BY_NAME = {kind.name: kind for kind in KINDS}
BY_DATASET = {kind.dataset: kind for kind in KINDS}


def _own(column: str | None) -> str:
    return f"r.{column}" if column else "NULL::VARCHAR"


def select_sql(kind: Kind) -> str:
    """The select list over a record `r` of the kind's dataset: key, mine, source date and the record's own coal/metal
    flag, state and mine name under names every kind shares, then the kind's own columns."""
    return ", ".join((
        f"r.{kind.key} AS key", "r.mine_id", f"{kind.date} AS occurred",
        f"{_own(kind.own_coal_metal)} AS own_coal_metal", f"{_own(kind.own_state)} AS own_state",
        f"{_own(kind.own_mine_name)} AS own_mine_name", *(f"r.{column}" for column in kind.columns)))


def one_copy_sql(kind: Kind, relation: str) -> str:
    """The records of `relation` (SQL for rows of the kind's dataset, holding every copy of each key it holds), one
    copy per key: the first by source date, undated last, then by every other value the event reads."""
    owns = (kind.own_coal_metal, kind.own_state, kind.own_mine_name)
    read = (kind.date, "r.mine_id", *(_own(column) for column in owns if column), *(f"r.{c}" for c in kind.columns))
    order = ", ".join(f"{value} NULLS LAST" for value in read)
    return (f"SELECT {select_sql(kind)} FROM {relation} r WHERE r.{kind.key} IS NOT NULL"
            f" QUALIFY row_number() OVER (PARTITION BY r.{kind.key} ORDER BY {order}) = 1")


def mine_sql(records: str, mines: str) -> str:
    """`records` (SQL giving `select_sql`'s columns) with each record's mine from `mines` (SQL for the lake's mines
    dataset): its district, state, name and coal/metal flag, null for a mine the dataset lacks."""
    return (f"SELECT n.*, m.district AS mine_district, m.state AS mine_state, m.current_mine_name AS mine_name,"
            f" m.coal_metal_ind AS mine_coal_metal FROM ({records}) n LEFT JOIN {mines} m ON m.mine_id = n.mine_id")


def event(kind: Kind, row: Row, *, mode: str, source: Mapping[str, str]) -> Event:
    """The event of one record as `mine_sql` reads it."""
    name = row["mine_name"] or row["own_mine_name"]
    place = name or (f"mine {row['mine_id']}" if row["mine_id"] else "an unknown mine")
    title, details = kind.describe(row, place)
    return Event.new(
        kind=kind.name, key=row["key"], mode=mode, occurred_at=row["occurred"], title=title, details=details,
        source=source, mine_id=row["mine_id"], district=row["mine_district"],
        state=row["mine_state"] or row["own_state"],
        coal_metal=commodity(row["mine_coal_metal"], row["own_coal_metal"]))
