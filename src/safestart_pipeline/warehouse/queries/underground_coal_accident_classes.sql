-- In underground coal mining, which kinds of accidents happen most, and how many of them are serious or fatal
-- (last ten years of data)?
-- Why it matters: the accident classification is the hazard a pre-shift assessment has to address; ranking by serious
-- outcomes rather than counts alone shows where a missed control hurts people most.
WITH bounds AS (
    SELECT max(accident_date) AS day FROM accident
)
SELECT
    coalesce(a.classification, '(not classified)') AS classification,
    count(*) AS accidents,
    count(*) FILTER (WHERE a.serious) AS serious,
    count(*) FILTER (WHERE a.degree_injury_cd = '01') AS fatal,
    round(count(*) FILTER (WHERE a.serious) / count(*), 3) AS serious_share
FROM accident a, bounds b
WHERE a.coal_metal = 'C' AND a.subunit = 'UNDERGROUND' AND a.accident_date > b.day - INTERVAL 10 YEAR
GROUP BY ALL
ORDER BY serious DESC, accidents DESC, classification;
