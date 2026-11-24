"""How often each live check runs (`python -m safestart_pipeline.watch`), stated once for the watchers and for the
dashboard, which shows when each check is next due. This module imports nothing heavy, so the dashboard can read it.

Dataset versions every 6 hours (MSHA publishes weekly, and the lake takes each new version when the batch flow runs),
MSHA's sitemaps every 6 hours, OSHA's once a day (it asks for 10 seconds between requests), the Federal Register every
hour, and the official files' feed every hour, so a manifest the daily run publishes at its end (about 04:30 UTC)
reaches the stream within the hour.
"""

from __future__ import annotations

from datetime import timedelta

SCHEDULE = {
    "versions": timedelta(hours=6),
    "sitemaps-msha": timedelta(hours=6),
    "sitemaps-osha": timedelta(days=1),
    "federal-register": timedelta(hours=1),
    "feed-official": timedelta(hours=1),
}
