-- Which active mines had the most S&S violations in the last 12 months, and what are their serious accidents and
-- injury rates?
-- Why it matters: this is the scorecard SafeStart's pre-shift check could open with; where S&S citations and injuries
-- are both high, a thorough pre-shift assessment matters most.
SELECT
    mine_id, mine_name, state, coal_metal, mine_type,
    inspections_12m, violations_12m, ss_violations_12m, serious_accidents_12m,
    round(injury_rate_12m, 2) AS injury_rate_12m,
    top_sections_12m[1:3] AS top_sections
FROM mart_mine_scorecard
WHERE status = 'Active'
ORDER BY ss_violations_12m DESC, violations_12m DESC, mine_id
LIMIT 15;
