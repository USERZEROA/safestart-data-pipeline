-- Which mines' violation counts rose the most in the last 12 months compared with the 12 months before?
-- Why it matters: a sharp rise flags a mine whose conditions are getting worse now, before it shows in its accident
-- record; a crew starting work there should know.
WITH bounds AS (
    SELECT max(issue_date) AS day FROM violation
), counts AS (
    SELECT
        v.mine_id,
        count(*) FILTER (WHERE v.issue_date <= b.day - INTERVAL 12 MONTH) AS prior_12m,
        count(*) FILTER (WHERE v.issue_date > b.day - INTERVAL 12 MONTH) AS last_12m
    FROM violation v, bounds b
    WHERE v.issue_date > b.day - INTERVAL 24 MONTH AND v.issue_date <= b.day
    GROUP BY v.mine_id
)
SELECT
    c.mine_id, m.mine_name, m.state, m.coal_metal, m.mine_type, m.status,
    c.prior_12m, c.last_12m, c.last_12m - c.prior_12m AS rise,
    round(c.last_12m / greatest(c.prior_12m, 1), 1) AS ratio
FROM counts c
JOIN mine m ON m.mine_id = c.mine_id
ORDER BY rise DESC, c.mine_id
LIMIT 15;
