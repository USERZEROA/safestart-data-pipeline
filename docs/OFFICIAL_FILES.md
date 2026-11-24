# Official files: the daily update and the feed for SafeStart

The official files are the MSHA and OSHA web pages and documents the crawls downloaded, the eCFR titles and the
Federal Register's rules. Every day the server asks the sites what changed since the day before, politely, and
publishes a signed list of what it found: the feed. SafeStart's importer, planned in SafeStart's own repository and
not built yet, is to treat that list as a hint only: to download every file it acts on again itself, from the
official host, so that its own record of where its bytes came from never depends on this server. A forged or broken
list can then at most cost SafeStart some wasted checks; it cannot put content into SafeStart, and only SafeStart's
own 404 or 410 withdraws a document there.

This document is the contract for the feed, format `safestart-official-updates/1`, and describes the daily run that
fills it. The code is `src/safestart_pipeline/official/` (`feed.py` writes the feed, `purge.py` deletes retired
originals) and `flows/official.py`.

## When it runs

| What | When (UTC) | Where |
|---|---|---|
| The official flow (`safestart-official/daily`) | every day at 16:30, after the 14:00 batch and the Sunday 15:00 model | the orchestrator on the server |
| Its deadline | the first 04:30 after the start, or 15 minutes before the batch or the model when one comes first | |
| The feed's manifest | at the end of every run that was not interrupted, even one that found nothing | `exports/official/` |
| The purge of retired originals | after the feed | the server's own copies only |
| SafeStart's import (planned, in SafeStart) | 06:00, inside its daily maintenance | SafeStart |

A run asks the sources in this order, the quick ones first so that OSHA's 10-second `Crawl-delay` gets the rest of the
window: `www.msha.gov` (one probe), the eCFR titles, the Federal Register, `arlweb.msha.gov` (at most two hours), then
`www.osha.gov` until the deadline. Each crawled site is asked under its own lock (`state/crawl-<site>.lock`); the feed
is written under `state/official.lock`; the run never takes the data root's `pipeline.lock`, so it never waits for the
batch or the model, or holds them up.

```bash
uv run ssp official run [--site osha] [--until 04:30] [--budget 500]   # a run now, then its manifest
uv run ssp official manifest                                           # a manifest now (a heartbeat if nothing is new)
uv run ssp official status                                             # the latest run, the queue, the feed, the purge
uv run ssp official purge [--delete]                                   # retired originals: reported, or deleted
uv run ssp official keygen --out <file>                                # a new signing key pair; prints the public key
python -m safestart_pipeline.flows run official                        # the whole flow once, under Prefect
```

## What a run asks, and the rules it keeps

| Source | What is asked | Status it can end with |
|---|---|---|
| `osha` (`www.osha.gov`) | robots.txt and the sitemap (3 requests); about 500 hub pages whose links reach 98.8% of the files a needed page links to; then new URLs, confirmations, URLs asked again for their return, and the rolling re-check, the URL due longest first; never a URL the not-needed rules name (below) | ok, partial, refused, unavailable, error, skipped |
| `arlweb` (`arlweb.msha.gov`) | the re-check of its held documents, at its 1-second delay, for at most two hours | the same |
| `msha` (`www.msha.gov`) | robots.txt and the first sitemap, once; it refuses the server, which is recorded and never worked around, and its pages are not updated automatically (owner, 2026-11-23) | refused, unavailable, error, skipped |
| `ecfr` (`www.govinfo.gov`) | nothing: the titles whose govinfo bulk edition the batch downloaded since the last run | ok, skipped |
| `federal_register` | the API's final rules that amend 29 or 30 CFR, published since the last complete read | ok, partial, refused, unavailable, error |

Every request goes through the crawler's own rules: robots.txt (read again daily), the larger of the site's pause and
its `Crawl-delay`, `Retry-After` up to an hour, conditional requests with the validators of a verified copy, and a stop
after a sustained block. A refusal (401, 403, 429, 451, a challenge page) is recorded and never worked around.

- **Validation.** Every 2xx body is checked against its first bytes before it is kept (`crawl/content.py`): a PDF needs
  `%PDF-` and an `%%EOF` near its end, an Office file a sound zip or OLE directory, XML and HTML must parse, text must
  not be empty, and every file must stay under the site's size limit. An HTML page where a file belongs is a soft
  error. A refused body never becomes an edition: the copy held before stays current and the refusal is listed under
  `rejected`.
- **Change.** A page changes when the SHA-256 of the text of its main content changes (scripts, styles, white space
  and the values Drupal changes on every request do not count); a file when its SHA-256 does. Only then is a new
  edition recorded (`changed`).
- **Gone.** A 404 or 410 for a held URL retires it at once (`gone`, `goneSince`); its original is deleted from this
  server 30 days later unless it comes back. A refusal, a 5xx or no answer retires nothing. A redirect is `moved`, and
  its target is asked as a new URL. A gone URL that answers with usable content again is `restored`.
- **Re-check.** OSHA asks for 10 seconds between requests, which leaves a run about 4,000 in its 12-hour window,
  against 27,263 held copies the project needs, so not everything can be asked every day: a file is due a week after
  its last confirmation and a page a month after, and each manifest's `cycle` says how far the re-check has come.
- **Not needed.** The crawl's not-needed rules (README, "Sources and ingestion") hold for the daily run too: a new URL,
  or one due to be asked for its return, that they name is skipped (`not_needed`) and never asked; a held copy they
  name, downloaded before the rule (on OSHA 13,855 of 41,118: paragraph-level interlinking pages, OSHA's copy of CFR
  text, alliance and grant pages and the files only they link to), is kept but marked when a run starts, so that it is
  never re-checked, confirmed or a hub, never in a manifest, and not in `cycle`. A copy the rules no longer name is
  asked about again.
- **Not backfilled.** A crawled site whose inventory still holds copies without a content hash (recorded before
  content hashes existed; `ssp status <site>` counts them as `copies_without_content_hash`) is not asked at all: the
  source is `skipped`, with the stop reason `not backfilled` and a note that says how many. The server keeps no
  originals, so such a copy cannot be compared with an answer in other bytes, and every page that came back so would
  be listed as `changed`. The inventory is backfilled where its originals are (`ssp official backfill-hashes <site>`)
  and then placed on the server (docs/DEPLOYMENT.md, "Placing a crawl inventory on the server").

## The feed

`<data root>/exports/official/` on the server, served read-only by its HTTPS gateway at
`https://<server>/feed/official/` (`deploy/Caddyfile`; only these files, no directory listing):

| File | Holds | Cached |
|---|---|---|
| `latest.json`, `latest.json.sig` | the newest manifest's exact bytes, and its signature | 5 minutes |
| `manifests/<sequence>.json`, `.json.sig` | every manifest of the last 45 days, and its signature; the newest is never removed | for a year, immutable |
| `public-key.txt` | the Ed25519 public key: base64 of its 32 raw bytes, nothing else | 5 minutes |

**Signature.** Ed25519 over the exact bytes of the JSON file; the `.sig` file holds the 64-byte signature in base64,
without a line break. SafeStart's planned importer is to pin the public key in its own configuration, so that it trusts
the feed by the key, not by the server it fetched it from.

**Writing.** Each file is written as `<name>.partial` and moved into place in one step, under `state/official.lock`, in
this order: each new manifest and its signature, then `public-key.txt` when it changed, then `latest.json` and
`latest.json.sig` last. A manifest, once written, is never written again. A publish that was interrupted is finished by
the next one: the newest manifest on disk gets whatever of its signature, the public key and `latest.json` it lacks,
then the next manifest follows it. Between the replacement of `latest.json` and that of its signature, a reader can for
a moment find the two not matching; it then reads `manifests/<sequence>.json` and its `.sig`, which are complete before
`latest.json` changes, or reads again a moment later.

### A manifest

Compact JSON in ASCII with a final line break. Its fields, in this order:

| Field | Meaning |
|---|---|
| `format` | `safestart-official-updates/1` |
| `sequence` | 1 for the first manifest, then one more for each manifest published, without gaps |
| `previousSequence`, `previousSha256` | the sequence and the SHA-256 of the exact bytes of the manifest before; null for the first |
| `generatedAt` | when the manifest was written (UTC, `2026-11-25T04:30:12Z`) |
| `window` | `{start, end}`: the manifest lists what was observed at or after `start` and before `end`; each window starts where the one before ended, so nothing falls between two manifests |
| `sources` | one object per source, in the order osha, msha, arlweb, ecfr, federal_register (below) |
| `entries` | the new, changed, gone, restored and moved URLs, in the order observed (below) |
| `ecfr` | hints for SafeStart's own eCFR check (below) |
| `checked` | held URLs confirmed current and unchanged in the window, sorted: a list of URLs (documents only, as the entries; the run's own count of its checks, in its note, `ssp official status` and the dashboard, is the same) |
| `rejected` | bodies refused as editions, the latest per URL (below) |
| `counts` | per source and in all (below) |

`sources[]`: `{source, host, status, note, requests, stopReason, cycle}`. `status` is `ok`, `partial` (the deadline or
the budget came before the sitemaps, the hubs, the new URLs and the confirmations were done, or a sitemap could not be
read), `refused`, `unavailable`, `error` or `skipped`. It describes the runs of the source that ended in the window: the
latest that asked anything gives the status, note and stop reason, and `requests` adds up all of them; a source no run
asked in the window is `skipped` with the stop reason `not asked`. `cycle` is `{held, checkedLast30Days, oldestCheckAt}`
for a crawled site, as its store stands at `generatedAt`: the held URLs not gone that the project needs, how many were
confirmed in the last 30 days, and the oldest confirmation; null for the other sources.

`entries[]`:

| Field | Meaning |
|---|---|
| `source`, `url` | `osha`, `arlweb` (or `msha`); the canonical https URL |
| `change` | `new`, `changed`, `gone`, `restored` or `moved` |
| `sha256`, `contentSha256` | the copy's SHA-256 and its change hash (main-text hash for a page, the SHA-256 for a file): the new copy for new, changed and restored; the copy held for gone and moved |
| `previousContentSha256` | for `changed` (and `restored` with new content), the change hash of the edition before; otherwise null |
| `bytes`, `contentType`, `httpStatus` | the copy's size and declared type, and the status of the answer |
| `detectedType` | what the bytes are: `html`, `pdf`, `docx`, `xlsx`, `pptx`, `doc`, `xls`, `ppt`, `zip`, `xml` or `text`; null only for a gone or moved URL whose copy was recorded before content hashes and never examined |
| `observedAt` | when the answer came |
| `target` | `moved` only: where the URL now leads |
| `goneSince` | `gone` only: when it was retired |

A URL that changed twice in one window is listed twice, in order. A change of a picture, a recording or another
binary file is counted (`otherTypes`) but not listed: SafeStart reads documents only.

`ecfr[]` holds two kinds of hint: `{title, issueDate, previousIssueDate}` for a title whose govinfo bulk edition
changed (dates as `2026-11-19`; the previous date null when no earlier edition is held), and `{title, documentNumber,
effectiveOn}` for a Federal Register final rule that amends title 29 or 30. A hint is listed once, in the window in
which it was found.

`rejected[]`: `{source, url, reason, httpStatus, contentType, detectedType}`, with `reason` one of `soft_error`,
`type_mismatch`, `too_large`, `unreadable` or `empty`. For a held URL the copy held before stays current.

`counts`: an object per source and `all`, each with `new`, `changed`, `gone`, `restored`, `moved`, `checked`,
`rejected`, `hints` and `otherTypes`, every one present (zero when none).

**Size.** At most 5,000 entries, 20,000 checked URLs and 2,000,000 bytes a manifest, under the 2 MiB per file that
SafeStart's planned importer is to read. A window with more is split by time into several manifests, published together
in sequence; one second's findings are never split. An OSHA entry is about 494 bytes (measured over the 41,118 held
copies), so about 3,900 entries fit a manifest: the byte limit splits a large window before the entry limit does. On a
copy of the OSHA inventory, a backlog of 41,108 confirmed URLs became three manifests (20,000, 20,000 and 1,108 URLs;
1.44 MB at most, 72 bytes a URL) in 0.6 seconds. A day's run asks at most about 4,000 OSHA URLs in its 12-hour window,
so its manifest is about 300 KB of checked URLs at most, plus its entries: one manifest a day.

A heartbeat: the manifest of a day on which nothing changed lists no entries, only the URLs confirmed, the sources'
statuses and the counts. A missed day is no gap: the next run's manifest has the next sequence and a window that starts
where the last one ended.

Abbreviated, with one entry (an illustration: the values are made up, the shape is the contract's):

```json
{"format":"safestart-official-updates/1","sequence":12,"previousSequence":11,
 "previousSha256":"fa9d89aa...","generatedAt":"2026-12-01T04:30:12Z",
 "window":{"start":"2026-11-30T04:30:11Z","end":"2026-12-01T04:30:12Z"},
 "sources":[{"source":"osha","host":"www.osha.gov","status":"ok","note":"2,803 requests, stopped: time limit; ...",
             "requests":2803,"stopReason":"time limit",
             "cycle":{"held":27263,"checkedLast30Days":12034,"oldestCheckAt":"2026-11-18T00:22:55Z"}},
            {"source":"msha","host":"www.msha.gov","status":"refused","note":"...; recorded, never worked around",
             "requests":2,"stopReason":"refused","cycle":null}, ...],
 "entries":[{"source":"osha","url":"https://www.osha.gov/laws-regs/standardinterpretations/2026-11-20","change":"new",
             "sha256":"6c9e...","contentSha256":"1d4b...","previousContentSha256":null,"bytes":48211,
             "contentType":"text/html; charset=UTF-8","detectedType":"html","httpStatus":200,
             "observedAt":"2026-11-30T17:02:41Z"}],
 "ecfr":[{"title":29,"issueDate":"2026-11-26","previousIssueDate":"2026-11-19"}],
 "checked":["https://www.osha.gov/...", ...],"rejected":[],
 "counts":{"all":{"new":1,"changed":0,"gone":0,"restored":0,"moved":0,"checked":2511,"rejected":0,"hints":1,
                  "otherTypes":0},"osha":{...},...}}
```

### Checking a manifest

```python
import base64, hashlib, httpx
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

feed = "https://<server>/feed/official"
data = httpx.get(f"{feed}/latest.json").content
signature = base64.b64decode(httpx.get(f"{feed}/latest.json.sig").content)
Ed25519PublicKey.from_public_bytes(base64.b64decode(PINNED_PUBLIC_KEY)).verify(signature, data)  # raises if not
previous_sha256 = hashlib.sha256(previous_bytes).hexdigest()  # equals the next manifest's previousSha256
```

In Node (SafeStart): `crypto.verify(null, data, crypto.createPublicKey({key: Buffer.concat([Buffer.from(
"302a300506032b6570032100", "hex"), raw]), format: "der", type: "spki"}), signature)`, where `raw` is the decoded
public key. Both were checked against the feed this code publishes.

### The signing key

`uv run ssp official keygen --out <file>` writes a new key pair to a new file, readable by its owner only: a line
`OFFICIAL_FEED_SIGNING_KEY=<base64 of the 32-byte private key>` and the public key in a comment. It prints only the
public key. The private key goes into the server's `.env` (docs/DEPLOYMENT.md, "The official-files feed") and nowhere
else; Compose passes it to the orchestrator alone. It is never logged, and a key that is not base64 of 32 bytes is an
error whose message does not quote it. Without the key a run still records everything it finds and writes the warning
"OFFICIAL_FEED_SIGNING_KEY is not set" (the log, `ssp official status`, the dashboard) but publishes nothing, and its
purge deletes nothing (below); the first signed manifest then covers everything since the first run. A new key signs
from the next manifest on; since SafeStart's importer is to pin the public key, a new key will need a SafeStart change.

## Retention and the purge of originals

- Manifests: 45 days, by `generatedAt`; the newest always stays.
- Originals (`raw/<site>/objects/`): an object is deleted 30 days after every URL and every edition that references its
  SHA-256 was retired by a 404 or 410, and only while all of them stay retired. Any other reference keeps it: a held
  current copy (files are stored by content, so one object can serve several URLs), an earlier edition of a URL still
  current (its history), a URL moved, refused or retired more recently, a sitemap's body. Objects nothing references
  (a body kept by a run that died before recording it) are counted, never deleted. Each site is purged under its crawl
  lock. The flow deletes every day on the server; `ssp official purge` reports what it would delete unless given
  `--delete`. Each purge is recorded (`ssp official status`, the dashboard) with every object it deleted, and each
  deletion is logged; an object that cannot be deleted fails the purge, and the others are deleted all the same.
- Only the server's copies are deleted. The flow deletes only where the feed's signing key is set, which only the
  server's `.env` holds; anywhere else, a workstation that runs the orchestrator on its archive among them, its purge
  is a dry run. The bucket and the workstation keep their copies: the server can only read the bucket, and the
  workstation is never touched. The boot copy (`deploy/gcp/startup.sh`) never brings `raw/<site>/` to the server, so
  nothing purged comes back; on the server most originals were never there (it holds only what its own runs
  downloaded), and the purge counts those as not on this host.

## Where to see it

- The dashboard's Sources page, under "Official files": when the latest run started and ended, what it found (new,
  changed, gone, restored, moved, refused files, unchanged), each source's status and note, the hosts that refused,
  the feed's newest manifest and its age, a warning when that manifest is more than 36 hours old or the latest publish
  failed or had no key, and the latest purge.
- `uv run ssp official status`: the same as JSON, with each crawl site's queue and re-check cycle.
- Prefect Cloud: the flow's run, a task per source, the feed and the purge, each with its log; a failed source, feed or
  purge fails the run, and Prefect can notify.
- SafeStart's planned import, in SafeStart's own repository, is to warn its administrators when the feed is more than
  36 hours old, its signature or chain fails, or its decisions wait.

## When something fails

| What happens | What is recorded | What to do |
|---|---|---|
| A source fails (an exception) | that source `error`, the others still run, the run fails at the end | read the run's log; the next run goes on from the inventory |
| A host refuses or stops answering | `refused` or `unavailable` in the manifest; nothing is retired | nothing: refusals are never worked around, held copies stay current |
| The run or the server stops midway | the answer being recorded is rolled back; no manifest that day | nothing: the next run resumes, and its manifest covers both days |
| The publish is cut off | the newest manifest on disk lacks its signature or `latest.json` | nothing: the next publish finishes it first |
| No signing key | `unsigned` in `ssp official status` and on the dashboard; the purge a dry run | install the key; the next manifest covers everything waiting |
| An original cannot be deleted | the purge `error`, with what it deleted and what it could not; the run fails | look at the file's permissions; the next purge tries again |
| A file in `exports/official/manifests/` is not a manifest | `error` on every publish, nothing more published | look at it; remove or restore the file by hand |
| The feed is older than 36 hours | the dashboard's warning (and, once built, SafeStart's) | look at the orchestrator's official runs |
| A crawl holds a site's lock | the site `skipped` (after 10 minutes), its purge skipped | nothing, or stop that crawl |
| A site's inventory was not backfilled | the site `skipped` (`not backfilled`), with how many copies lack a content hash | backfill it where its originals are and place it on the server (docs/DEPLOYMENT.md) |

## Limits

- OSHA's 10-second `Crawl-delay` leaves the daily run about 4,000 requests in its 12-hour window, so it cannot ask
  every held URL every day: pages come round about monthly and files about weekly. A change is found when its URL is
  next asked, or sooner when a hub page or the sitemap points to it.
- `www.msha.gov` refuses the server, and its pages are not updated by this run; the feed says so every day. A
  workstation is served its sitemap, so a run there records `msha` as `skipped`, never `refused`.
- A page that keeps navigation inside its main content changes when the navigation does: in the acceptance run of
  2026-11-24, 1 of 66 OSHA hub pages (a Safe + Sound Week page that went from 7 copies of its section menu to 3, with
  the same text otherwise) was listed as `changed`; the 42 hub pages that came back in new bytes with the same text
  were not, and the 2 other changes were real edits. SafeStart compares the text itself before it makes an edition.
- OSHA serves 9 Federal Register PDFs cut off inside an incremental update (the first revision is whole, the
  signature that follows is not); they are refused as `unreadable` and never offered as editions.
- A Federal Register hint names the CFR title, not the part: a Wage and Hour rule (29 CFR 500, 795, 825) is a hint as
  much as an OSHA rule is. It is only to start SafeStart's own eCFR check.
- `checked` names URLs without a time: a reader knows only that each was confirmed within the manifest's window,
  normally a day.
- A new server starts a new chain at sequence 1; SafeStart's importer is to treat that as a broken chain until told.
- A key change will need a SafeStart change, since SafeStart's importer is to pin the public key.
- The purge deletes only the server's copies; the bucket and the workstation keep everything.
