-- How many reportable injuries to mine employees occur per 200,000 hours worked (100 full-time workers for a year),
-- by year, in coal and in metal and non-metal mines?
-- Why it matters: unlike a count, the rate stays comparable as employment changes; it is the baseline against which a
-- single mine's rate in its scorecard is judged.
SELECT
    year,
    max(injuries) FILTER (WHERE coal_metal = 'C') AS coal_injuries,
    round(max(hours_worked) FILTER (WHERE coal_metal = 'C') / 1e6, 1) AS coal_hours_m,
    round(max(rate_per_200k) FILTER (WHERE coal_metal = 'C'), 2) AS coal_rate,
    max(injuries) FILTER (WHERE coal_metal = 'M') AS mnm_injuries,
    round(max(hours_worked) FILTER (WHERE coal_metal = 'M') / 1e6, 1) AS mnm_hours_m,
    round(max(rate_per_200k) FILTER (WHERE coal_metal = 'M'), 2) AS mnm_rate
FROM mart_injury_rate
GROUP BY year
ORDER BY year;
