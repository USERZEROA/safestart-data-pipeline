-- What does the web corpus collected from the OSHA and MSHA websites consist of, by site and kind of document?
-- Why it matters: SafeStart's library draws on the same sites; how much is HTML, PDF, office files or data
-- decides which parsers and how much indexing the library needs, and the skipped count shows what was left out.
SELECT
    site,
    kind,
    count(*) AS urls,
    count(*) FILTER (WHERE state IN ('fetched', 'unchanged')) AS downloaded,
    round(coalesce(sum(bytes) FILTER (WHERE state IN ('fetched', 'unchanged')), 0) / 1e9, 2) AS downloaded_gb,
    count(*) FILTER (WHERE state = 'skipped') AS skipped,
    round(count(*) FILTER (WHERE in_sitemap) / count(*), 3) AS sitemap_share
FROM document
GROUP BY site, kind
ORDER BY site, downloaded DESC, kind;
