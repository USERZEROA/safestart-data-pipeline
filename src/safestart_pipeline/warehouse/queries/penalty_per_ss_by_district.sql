-- What penalty does MSHA propose per S&S violation in each district, over the last three years of data?
-- Why it matters: the proposed penalty is how enforcement prices a hazard; a district whose S&S citations cost far
-- more or less than the others shows where the same designation weighs differently on an operator.
WITH bounds AS (
    SELECT max(issue_date) AS day FROM violation
)
SELECT
    m.district,
    count(*) AS ss_violations,
    count(v.proposed_penalty) AS assessed,
    round(sum(v.proposed_penalty) / 1e6, 2) AS proposed_musd,
    round(avg(v.proposed_penalty)) AS penalty_per_ss,
    round(median(v.proposed_penalty)) AS median_penalty
FROM violation v
JOIN mine m ON m.mine_id = v.mine_id
CROSS JOIN bounds b
WHERE v.sig_sub AND v.issue_date > b.day - INTERVAL 36 MONTH
GROUP BY m.district
ORDER BY penalty_per_ss DESC NULLS LAST, m.district;
