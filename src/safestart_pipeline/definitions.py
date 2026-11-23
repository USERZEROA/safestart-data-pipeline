"""Definitions every component shares, so the warehouse, the stream, the watchers, the model and the dashboard count
the same things the same way. Each rule is stated once, here; the components use the constants and the SQL helpers
below rather than restating a rule.

Degree of injury (MSHA's accidents dataset, `DEGREE_INJURY_CD`): 00 accident only (no injury), 01 fatality,
02 permanent total or partial disability, 03 days away from work only, 04 days away and restricted activity,
05 restricted activity only, 06 no days away and no restriction, 07 occupational illness not of degrees 01-06,
08 natural causes, 09 non-employees, 10 all other cases (including first aid).

- An injury is degree 01 to 06, the injuries MSHA's injury rates count.
- A serious accident is degree 01 to 05: a death, a permanent disability, or days away from work or of restricted
  work. The rule reads the degree, never the day counts: the degree is set when the accident is reported, while the
  day counts arrive once the miner is back at work (58 of the 66 days-away accidents new on 2026-11-23 had none yet),
  and the day counts also appear on illnesses and other cases (07 to 10) that are not injuries. Every serious accident
  is an injury.
- A fatality is degree 01.

The cited section of a violation is the eCFR section its citation names, without the paragraph: "56.14107(a)" is
56.14107, "75.1100-2(b)" is 75.1100-2 (hyphenated numbers are sections of their own), "56.5001(a)/.5005" is 56.5001,
and a citation that names no section ("104(d)(1)") has none.

Commodity (coal or metal/non-metal) is the mine's flag in MSHA's mines dataset, and a record's own flag only when its
mine is missing there. MSHA describes every dataset's flag as the commodity of the mine, and every accident and
inspection record agrees with its mine's flag, but 89,673 of the 3.1 million violations and 784 of the 2.8 million
quarterly production reports carry the other flag; one flag per mine keeps a mine's violations, accidents and hours
worked under the same commodity.
"""

from __future__ import annotations

import re

FATAL_DEGREE = "01"
SERIOUS_DEGREES = ("01", "02", "03", "04", "05")
INJURY_DEGREES = ("01", "02", "03", "04", "05", "06")
COMMODITIES = ("C", "M")
SIG_SUB = {"Y": True, "N": False}
CFR_SECTION_PATTERN = r"^[0-9]+[.][0-9]+(-[0-9]+)?"

_CFR_SECTION = re.compile(CFR_SECTION_PATTERN)


def is_fatal(degree: str | None) -> bool:
    return degree == FATAL_DEGREE


def is_serious(degree: str | None) -> bool:
    return degree in SERIOUS_DEGREES


def is_injury(degree: str | None) -> bool:
    return degree in INJURY_DEGREES


def cfr_section(part_section: str | None) -> str | None:
    """The section a citation names, without its paragraph, or None when it names none."""
    match = _CFR_SECTION.match(part_section.strip()) if part_section else None
    return match.group(0) if match else None


def commodity(mine_flag: str | None, own_flag: str | None) -> str | None:
    """The mine's coal/metal flag, else the record's own; None unless it is one of COMMODITIES."""
    flag = mine_flag if mine_flag in COMMODITIES else own_flag
    return flag if flag in COMMODITIES else None


# SQL (DuckDB) forms of the same rules. Column arguments are SQL expressions written by the caller, never user input.


def _sql_list(values: tuple[str, ...]) -> str:
    return "(" + ", ".join(f"'{value}'" for value in values) + ")"


def serious_sql(degree: str = "degree_injury_cd") -> str:
    return f"coalesce({degree} IN {_sql_list(SERIOUS_DEGREES)}, false)"


def injury_sql(degree: str = "degree_injury_cd") -> str:
    return f"coalesce({degree} IN {_sql_list(INJURY_DEGREES)}, false)"


def cfr_section_sql(part_section: str = "part_section") -> str:
    return f"nullif(regexp_extract(trim({part_section}), '{CFR_SECTION_PATTERN}'), '')"


def commodity_sql(mine_flag: str, own_flag: str) -> str:
    known = _sql_list(COMMODITIES)
    return (f"CASE WHEN {mine_flag} IN {known} THEN {mine_flag} WHEN {own_flag} IN {known} THEN {own_flag} END")
