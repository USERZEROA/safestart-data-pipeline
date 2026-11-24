"""The daily update of the official files: what the sites changed since the last run, found politely.

- run.py: a whole run (`ssp official run`) and `ssp official status`.
- daily.py: one crawl site's run (OSHA: sitemaps, hub pass, queue; arlweb: queue), through the crawler's own rules.
- sources.py: www.msha.gov's probe, the eCFR editions and the Federal Register's final rules.
- state.py: the run's record, state/official.sqlite.

What a run finds is kept: each crawl site's new, changed, gone, restored, moved and rejected URLs in the changes table
of its crawl store (named by the store's run), the held URLs it confirmed unchanged in their last_checked_at, each
source's status and counts in state/official.sqlite's sources table, and the eCFR and Federal Register hints in its
hints table.
"""
