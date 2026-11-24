"""The daily update of the official files: what the sites changed since the last run, found politely, published as a
signed daily feed that SafeStart reads.

- run.py: a whole run (`ssp official run`, the official flow in flows/official.py) and `ssp official status`.
- daily.py: one crawl site's run (OSHA: sitemaps, hub pass, queue; arlweb: queue), through the crawler's own rules.
- sources.py: www.msha.gov's probe, the eCFR editions and the Federal Register's final rules.
- state.py: the run's record, state/official.sqlite.
- feed.py: the signed manifests in exports/official/ (`ssp official manifest|keygen`); docs/OFFICIAL_FILES.md.

What a run finds is kept where the feed is built from: each crawl site's new, changed, gone, restored, moved and
rejected URLs in the changes table of its crawl store (named by the store's run), the held URLs it confirmed unchanged
in their last_checked_at, each source's status and counts in state/official.sqlite's sources table, and the eCFR and
Federal Register hints in its hints table. The manifests alone say what was published; official.sqlite records each
attempt to publish.
"""
