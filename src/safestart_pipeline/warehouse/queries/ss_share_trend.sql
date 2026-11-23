-- How has the share of violations designated significant and substantial (S&S) changed, in coal and in metal and
-- non-metal mines?
-- Why it matters: an S&S violation is one the inspector judged reasonably likely to cause a serious injury; a falling
-- share means fewer of the conditions cited are the dangerous kind.
SELECT
    year,
    max(violations) FILTER (WHERE coal_metal = 'C') AS coal_violations,
    round(max(ss_share) FILTER (WHERE coal_metal = 'C'), 3) AS coal_ss_share,
    max(violations) FILTER (WHERE coal_metal = 'M') AS mnm_violations,
    round(max(ss_share) FILTER (WHERE coal_metal = 'M'), 3) AS mnm_ss_share
FROM mart_ss_trend
WHERE year >= 2000
GROUP BY year
ORDER BY year;
