# Dev Rel Command Center — Claude Instructions

@AGENTS.md

## Project Context

This is a Dev Rel analytics dashboard built on MooseStack. It demonstrates real-time data ingestion (GitHub webhooks) combined with historical batch data (18 weeks of marketing snapshots). The data model is designed to show both leading indicators (GitHub activity) and lagging indicators (SEO, traffic, pipeline MQLs) in a unified ClickHouse backend.

## Project Layout

```
app/
  ingest/
    githubModels.ts      — GithubEvent (raw webhook) + GithubProcessed (normalized)
    githubTransforms.ts  — Fan-out transform: GithubEvent → GithubProcessed
    marketingModels.ts   — WeeklySnapshot + AiReferralEvent
  views/
    githubMetrics.ts     — GithubDailyMV (star/fork/issue counts by day)
    marketingTrends.ts   — MarketingWeeklySummaryMV + AiSourceWeeklyMV
  apis/
    devrel.ts            — /api/devrel-health, /api/github-signals, /api/marketing-trends
  workflows/
    snapshotSync.ts      — Weekly Temporal workflow, runs backfill.py --latest-only
  index.ts               — Exports all primitives (required for MooseStack discovery)

scripts/
  backfill.py            — Loads 18 JSON snapshots → POST /ingest/WeeklySnapshot + /ingest/AiReferralEvent
  webhook_adapter.py     — Flask proxy on :3001, extracts X-GitHub-Event header, forwards to :4000

dashboard/
  index.html             — Single-file Chart.js dashboard, polls APIs every 10–30s
```

## Data Models

### GitHub Pipeline

- `GithubEvent` → raw webhook receiver (`stream: true, ingestApi: true, table: false`)
- `GithubProcessed` → normalized, stored in ClickHouse (`stream: true, ingestApi: false, table: true`)
  - orderByFields: `["eventId", "timestamp", "eventType"]` — Key field MUST be first
- Transform: `GithubEventPipeline.stream.addTransform(GithubProcessedPipeline.stream, fn, { deadLetterQueue })`

### Marketing Pipeline

- `WeeklySnapshot` — one row per week per site, all KPIs flattened
  - orderByFields: `["snapshotId", "snapshotDate", "site"]`
- `AiReferralEvent` — one row per AI source per snapshot
  - orderByFields: `["eventId", "snapshotDate", "aiSource"]`

## Known Constraints

- **Key field must lead orderByFields** — MooseStack maps `Key<string>` to ClickHouse PRIMARY KEY, which must be a prefix of the ORDER BY. Always put the Key field first in orderByFields.
- **ingestApi requires stream** — `ingestApi: true` without `stream: true` throws "Ingest API needs a stream to write to" at runtime.
- **ClickHouse alias shadowing** — Don't alias a column with the same name as the source column in SELECT (e.g., `toString(date) as date`). Use a different alias (`dateStr`) to avoid WHERE clause confusion.
- **Export from index.ts** — MooseStack only discovers primitives that are exported from `app/index.ts`. New pipelines, views, APIs, and workflows must be added there.

## Running Locally

```bash
# Start the full stack
moose dev

# Load historical data (idempotent)
python scripts/backfill.py

# Start webhook adapter (for live GitHub events)
python scripts/webhook_adapter.py

# Serve dashboard
cd dashboard && python3 -m http.server 3000
```

## Verifying Data

```bash
# Check table row counts
curl -s "http://panda:pandapass@localhost:18123/?database=local" \
  --data "SELECT 'WeeklySnapshot' as t, count() FROM WeeklySnapshot_0_0 UNION ALL SELECT 'AiReferralEvent', count() FROM AiReferralEvent_0_0 UNION ALL SELECT 'GithubProcessed', count() FROM GithubProcessed_0_0"

# Test API endpoints
curl http://localhost:4000/api/devrel-health | jq .
curl "http://localhost:4000/api/github-signals?windowDays=30" | jq .recentEvents
curl "http://localhost:4000/api/marketing-trends?site=cloudquery.io" | jq .weeklyTrend[0]

# Inject a test GitHub star event
curl -s -X POST http://localhost:4000/ingest/GithubEvent \
  -H "Content-Type: application/json" \
  -d '{"deliveryId":"test-001","timestamp":"2026-04-24T12:00:00Z","eventType":"star","repo":"cloudquery/cloudquery","actor":"testuser","action":"created","rawPayload":"{}"}'
```

## Backfill Data Source

Historical snapshots are read from `/Users/joe/Documents/dev/cloudquery/marketing-skills/seo-data/snapshots/`. The 18 JSON files span Feb–Apr 2026 and have 6+ different structural formats — `backfill.py` uses priority-chain extraction for each metric to normalize them.

## Ports

| Port | Service |
|------|---------|
| 4000 | MooseStack API + ingest |
| 3000 | Dashboard (python http.server) |
| 3001 | Webhook adapter (Flask) |
| 18123 | ClickHouse HTTP API |
| 9000 | Redpanda (Kafka) |
