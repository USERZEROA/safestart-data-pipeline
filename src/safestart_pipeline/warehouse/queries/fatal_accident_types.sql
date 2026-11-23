-- What kinds of accidents killed miners in the last ten years of data, in coal and in metal and non-metal mines?
-- Why it matters: fatalities concentrate in a few classifications; those are the hazards a pre-shift assessment can
-- least afford to miss.
WITH bounds AS (
    SELECT max(accident_date) AS day FROM accident
)
SELECT
    coalesce(a.classification, '(not classified)') AS classification,
    count(*) FILTER (WHERE a.coal_metal = 'C') AS coal_fatalities,
    count(*) FILTER (WHERE a.coal_metal = 'M') AS mnm_fatalities,
    count(*) AS fatalities,
    round(count(*) / sum(count(*)) OVER (), 3) AS share
FROM accident a, bounds b
WHERE is_fatal(a.degree_injury_cd) AND a.accident_date > b.day - INTERVAL 10 YEAR
GROUP BY ALL
ORDER BY fatalities DESC, classification;
