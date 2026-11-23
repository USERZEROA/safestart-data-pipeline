-- The warehouse's tables (docs/ERD.md describes each). Keys are PRIMARY KEY constraints; a source row a key rejects is
-- kept in duplicate_row, and every rule the build checked is recorded in build_check, so nothing is dropped unseen.

CREATE TABLE mine (
    mine_id VARCHAR PRIMARY KEY,
    mine_name VARCHAR,
    coal_metal VARCHAR CHECK (coal_metal IN ('C', 'M')),
    mine_type VARCHAR,
    status VARCHAR,
    status_date DATE,
    state VARCHAR,
    county VARCHAR,
    district VARCHAR,
    primary_sic VARCHAR,
    primary_canvass VARCHAR,
    latitude DOUBLE CHECK (latitude BETWEEN -90 AND 90),
    longitude DOUBLE CHECK (longitude BETWEEN -180 AND 180),
    controller_id VARCHAR,
    controller_name VARCHAR,
    operator_id VARCHAR,
    operator_name VARCHAR
);

CREATE TABLE inspection (
    event_no VARCHAR PRIMARY KEY,
    mine_id VARCHAR,
    begin_date DATE,
    end_date DATE,
    activity_code VARCHAR,
    activity VARCHAR,
    coal_metal VARCHAR CHECK (coal_metal IN ('C', 'M'))
);

CREATE TABLE violation (
    violation_no VARCHAR PRIMARY KEY,
    event_no VARCHAR,
    mine_id VARCHAR,
    issue_date DATE,
    occur_date DATE,
    part_section VARCHAR,
    cfr_section VARCHAR,
    sig_sub BOOLEAN,
    likelihood VARCHAR,
    injury_illness VARCHAR,
    negligence VARCHAR,
    proposed_penalty DOUBLE,
    amount_paid DOUBLE,
    termination_date DATE,
    coal_metal VARCHAR CHECK (coal_metal IN ('C', 'M')),
    mine_type VARCHAR
);

-- contractor_id is beyond the contract: an injury rate over the hours mine operators report must leave out the
-- injuries of contractors' employees, whose hours are reported separately.
CREATE TABLE accident (
    document_no VARCHAR PRIMARY KEY,
    mine_id VARCHAR,
    accident_date DATE,
    degree_injury_cd VARCHAR,
    degree_injury VARCHAR,
    serious BOOLEAN NOT NULL,
    classification VARCHAR,
    accident_type VARCHAR,
    no_injuries INTEGER,
    days_lost INTEGER,
    days_restrict INTEGER,
    subunit VARCHAR,
    narrative VARCHAR,
    coal_metal VARCHAR CHECK (coal_metal IN ('C', 'M')),
    contractor_id VARCHAR
);

CREATE TABLE production_quarter (
    mine_id VARCHAR NOT NULL,
    cal_yr INTEGER NOT NULL,
    cal_qtr INTEGER NOT NULL CHECK (cal_qtr BETWEEN 1 AND 4),
    subunit_cd VARCHAR NOT NULL,
    avg_employees DOUBLE,
    hours_worked DOUBLE,
    coal_production DOUBLE,
    PRIMARY KEY (mine_id, cal_yr, cal_qtr, subunit_cd)
);

-- A section number is unique within a title but not across the two (29 CFR 1.1 and 30 CFR 1.1 both exist), so the
-- key includes the title. MSHA's citations are to title 30.
CREATE TABLE cfr_section (
    section VARCHAR NOT NULL,
    title INTEGER NOT NULL CHECK (title IN (29, 30)),
    part INTEGER,
    heading VARCHAR,
    text VARCHAR,
    amended_on DATE,
    PRIMARY KEY (title, section)
);

CREATE TABLE document (
    url VARCHAR PRIMARY KEY,
    site VARCHAR NOT NULL,
    state VARCHAR NOT NULL,
    kind VARCHAR NOT NULL CHECK (kind IN ('page', 'pdf', 'data', 'office', 'other')),
    content_type VARCHAR,
    bytes BIGINT,
    sha256 VARCHAR,
    fetched_at TIMESTAMP,
    in_sitemap BOOLEAN NOT NULL
);

CREATE TABLE dataset_version (
    dataset VARCHAR NOT NULL,
    version VARCHAR NOT NULL,
    rows BIGINT,
    built_at TIMESTAMP,
    is_current BOOLEAN NOT NULL,
    PRIMARY KEY (dataset, version)
);

-- Build metadata: when and from which eCFR editions the warehouse was built, every rule the build checked, and every
-- copy of a repeated key (the kept one flagged).
CREATE TABLE build_info (
    key VARCHAR PRIMARY KEY,
    value VARCHAR
);

CREATE TABLE build_check (
    name VARCHAR PRIMARY KEY,
    table_name VARCHAR NOT NULL,
    rows BIGINT NOT NULL,
    detail VARCHAR
);

CREATE TABLE duplicate_row (
    table_name VARCHAR NOT NULL,
    key VARCHAR NOT NULL,
    copy_no INTEGER NOT NULL,
    kept BOOLEAN NOT NULL,
    source_row VARCHAR NOT NULL,
    PRIMARY KEY (table_name, key, copy_no)
);
