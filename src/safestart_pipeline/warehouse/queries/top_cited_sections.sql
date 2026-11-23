-- Which standards do MSHA inspectors cite most often, and what do they require?
-- Why it matters: the most cited standards are the conditions crews most often leave uncontrolled, so SafeStart's
-- pre-shift checks and its library should put their text first.
SELECT
    t.cfr_section,
    t.heading,
    t.citations,
    round(t.ss_share, 3) AS ss_share,
    round(t.proposed_penalty_total / 1e6, 1) AS proposed_penalty_musd,
    t.last_cited,
    left(s.text, 140) AS text_begins
FROM mart_top_cited_sections t
LEFT JOIN cfr_section s ON s.title = 30 AND s.section = t.cfr_section
ORDER BY t.citations DESC, t.cfr_section
LIMIT 15;
