-- The marts: views over the core tables, so they always agree with them and cost nothing to rebuild.
--
-- "12m" is the 12 months up to the newest violation issue date in the data (not today's date), so a scorecard built
-- from an older download describes that download's last year. Hours are reported per calendar quarter, so an injury
-- rate pairs a quarter's injuries with the same quarter's hours: the yearly rate counts only quarters with reported
-- hours, and the 12-month rate uses the four latest quarters with reported hours that end by the window's end.
-- An injury here is an injury by the shared rule (the is_injury macro the build makes from definitions.py) to a mine
-- operator's employee (no contractor), the people whose hours production_quarter holds; the rate is per 200,000 hours
-- (100 full-time workers for a year). A record counts under its coal_metal, which is its mine's commodity.

CREATE VIEW mart_top_cited_sections AS
SELECT
    v.cfr_section,
    any_value(s.heading) AS heading,
    count(*) AS citations,
    count(*) FILTER (WHERE v.sig_sub) AS ss_citations,
    count(*) FILTER (WHERE v.sig_sub) / count(*) AS ss_share,
    sum(v.proposed_penalty) AS proposed_penalty_total,
    max(v.issue_date) AS last_cited
FROM violation v
LEFT JOIN cfr_section s ON s.title = 30 AND s.section = v.cfr_section
WHERE v.cfr_section IS NOT NULL
GROUP BY v.cfr_section;

CREATE VIEW mart_mine_scorecard AS
WITH bounds AS (
    SELECT max(issue_date) - INTERVAL 12 MONTH AS window_start, max(issue_date) AS window_end FROM violation
), rate_quarters AS (
    SELECT max(p.cal_yr * 4 + p.cal_qtr - 1) AS last_quarter
    FROM production_quarter p, bounds b
    WHERE last_day(make_date(p.cal_yr, p.cal_qtr * 3, 1)) <= b.window_end
), inspections AS (
    SELECT i.mine_id, count(*) AS inspections
    FROM inspection i, bounds b
    WHERE i.begin_date > b.window_start AND i.begin_date <= b.window_end
    GROUP BY i.mine_id
), violations AS (
    SELECT v.mine_id, count(*) AS violations, count(*) FILTER (WHERE v.sig_sub) AS ss_violations,
           sum(v.proposed_penalty) AS proposed_penalty
    FROM violation v, bounds b
    WHERE v.issue_date > b.window_start AND v.issue_date <= b.window_end
    GROUP BY v.mine_id
), section_counts AS (
    SELECT v.mine_id, v.cfr_section, count(*) AS citations,
           row_number() OVER (PARTITION BY v.mine_id ORDER BY count(*) DESC, v.cfr_section) AS rank
    FROM violation v, bounds b
    WHERE v.issue_date > b.window_start AND v.issue_date <= b.window_end AND v.cfr_section IS NOT NULL
    GROUP BY v.mine_id, v.cfr_section
), top_sections AS (
    SELECT mine_id, list(cfr_section ORDER BY rank) AS sections
    FROM section_counts
    WHERE rank <= 5
    GROUP BY mine_id
), accidents AS (
    SELECT a.mine_id, count(*) AS accidents, count(*) FILTER (WHERE a.serious) AS serious_accidents
    FROM accident a, bounds b
    WHERE a.accident_date > b.window_start AND a.accident_date <= b.window_end
    GROUP BY a.mine_id
), hours AS (
    SELECT p.mine_id, sum(p.hours_worked) AS hours_worked
    FROM production_quarter p, rate_quarters r
    WHERE p.cal_yr * 4 + p.cal_qtr - 1 BETWEEN r.last_quarter - 3 AND r.last_quarter
    GROUP BY p.mine_id
), injuries AS (
    SELECT a.mine_id, count(*) AS injuries
    FROM accident a, rate_quarters r
    WHERE is_injury(a.degree_injury_cd) AND a.contractor_id IS NULL
      AND year(a.accident_date) * 4 + quarter(a.accident_date) - 1 BETWEEN r.last_quarter - 3 AND r.last_quarter
    GROUP BY a.mine_id
)
SELECT
    m.mine_id, m.mine_name, m.coal_metal, m.mine_type, m.status, m.state, m.district, m.latitude, m.longitude,
    coalesce(i.inspections, 0) AS inspections_12m,
    coalesce(v.violations, 0) AS violations_12m,
    coalesce(v.ss_violations, 0) AS ss_violations_12m,
    coalesce(v.proposed_penalty, 0) AS proposed_penalty_12m,
    coalesce(a.accidents, 0) AS accidents_12m,
    coalesce(a.serious_accidents, 0) AS serious_accidents_12m,
    coalesce(h.hours_worked, 0) AS hours_worked_12m,
    coalesce(j.injuries, 0) * 200000 / nullif(h.hours_worked, 0) AS injury_rate_12m,
    coalesce(t.sections, []::VARCHAR[]) AS top_sections_12m
FROM mine m
LEFT JOIN inspections i ON i.mine_id = m.mine_id
LEFT JOIN violations v ON v.mine_id = m.mine_id
LEFT JOIN accidents a ON a.mine_id = m.mine_id
LEFT JOIN hours h ON h.mine_id = m.mine_id
LEFT JOIN injuries j ON j.mine_id = m.mine_id
LEFT JOIN top_sections t ON t.mine_id = m.mine_id;

CREATE VIEW mart_ss_trend AS
SELECT
    year(issue_date) AS year,
    coal_metal,
    count(*) AS violations,
    count(*) FILTER (WHERE sig_sub) AS ss_violations,
    count(*) FILTER (WHERE sig_sub) / count(*) AS ss_share
FROM violation
WHERE issue_date IS NOT NULL
GROUP BY year(issue_date), coal_metal;

CREATE VIEW mart_injury_rate AS
WITH hours AS (
    SELECT p.cal_yr AS year, p.cal_qtr AS quarter, p.coal_metal, sum(p.hours_worked) AS hours_worked
    FROM production_quarter p
    GROUP BY p.cal_yr, p.cal_qtr, p.coal_metal
), injuries AS (
    SELECT year(a.accident_date) AS year, quarter(a.accident_date) AS quarter, a.coal_metal, count(*) AS injuries
    FROM accident a
    WHERE is_injury(a.degree_injury_cd) AND a.contractor_id IS NULL
    GROUP BY year(a.accident_date), quarter(a.accident_date), a.coal_metal
)
SELECT
    h.year,
    h.coal_metal,
    sum(coalesce(i.injuries, 0))::BIGINT AS injuries,
    sum(h.hours_worked) AS hours_worked,
    sum(coalesce(i.injuries, 0)) * 200000 / nullif(sum(h.hours_worked), 0) AS rate_per_200k
FROM hours h
LEFT JOIN injuries i ON i.year = h.year AND i.quarter = h.quarter AND i.coal_metal = h.coal_metal
GROUP BY h.year, h.coal_metal;

CREATE VIEW mart_district_year AS
WITH inspections AS (
    SELECT m.district, year(i.begin_date) AS year, count(*) AS inspections
    FROM inspection i JOIN mine m ON m.mine_id = i.mine_id
    WHERE i.begin_date IS NOT NULL
    GROUP BY m.district, year(i.begin_date)
), violations AS (
    SELECT m.district, year(v.issue_date) AS year, count(*) AS violations,
           count(*) FILTER (WHERE v.sig_sub) AS ss_violations
    FROM violation v JOIN mine m ON m.mine_id = v.mine_id
    WHERE v.issue_date IS NOT NULL
    GROUP BY m.district, year(v.issue_date)
), accidents AS (
    SELECT m.district, year(a.accident_date) AS year, count(*) AS accidents
    FROM accident a JOIN mine m ON m.mine_id = a.mine_id
    WHERE a.accident_date IS NOT NULL
    GROUP BY m.district, year(a.accident_date)
)
SELECT
    district,
    year,
    coalesce(i.inspections, 0) AS inspections,
    coalesce(v.violations, 0) AS violations,
    coalesce(v.ss_violations, 0) AS ss_violations,
    coalesce(a.accidents, 0) AS accidents
FROM inspections i
FULL JOIN violations v USING (district, year)
FULL JOIN accidents a USING (district, year);
