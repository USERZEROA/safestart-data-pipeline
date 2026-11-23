-- Which cited section numbers have no text in today's Title 30, and how often and how recently are they cited?
-- Why it matters: SafeStart links each citation to the regulation's text; a number missing from the current CFR (a
-- standard since removed or renumbered, or a typing error) needs another source or a correction before it can link.
SELECT
    t.cfr_section,
    t.citations,
    t.last_cited,
    round(t.ss_share, 3) AS ss_share
FROM mart_top_cited_sections t
ANTI JOIN cfr_section s ON s.title = 30 AND s.section = t.cfr_section
ORDER BY t.citations DESC, t.cfr_section
LIMIT 15;
