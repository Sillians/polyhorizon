# Forecast publication contract

The product promises **post-close forecasts**, not live intraday updates. The
30-minute frequency describes feature and prediction bars, not publication.

- Kafka and Spark collect intraday observations. After the actual NYSE close,
  Spark completes a version-pinned dataset; every product symbol must include
  its closing bar (event timestamp equals bar end).
- That completion triggers `feature-store-after-close`, which has no independent
  cron schedule. PostgreSQL → Feast → Redis publication runs once per completed
  session; retries may republish the same release. Early closes follow the exchange
  calendar rather than a fixed 16:00 timer.
- Serving allows 60 minutes after close for publication, configurable through
  `publication.publication_delay_minutes`. This is an availability target, not a
  guarantee that jobs will finish: missed publication fails closed with HTTP 503
  and Retry-After, rather than returning an old forecast as current.
- Before the deadline, the previous session remains valid; a new completed
  session may be served sooner. At the deadline the current closing bar is
  required. Weekends, holidays, DST and early closes use the NYSE calendar.
- Both cache hits and newly fetched feature windows are checked. Missing,
  timezone-less, future and partial-session cutoffs are rejected. Stale cache
  entries are bypassed to check the source before declaring unavailability.
- `/metadata` publishes the mode, publication frequency, delay and freshness
  policy. The frontend labels post-close cadence and displays wall-clock age
  without treating normal market closures as stale intraday data.

The freshness check is a cutoff check, not proof of complete historical bars or
atomic cross-store publication; those remain responsibilities of the versioned
release validator and publisher. This change does not enable an intraday SLA.
