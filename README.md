# Dev Rel Command Center

A real-time developer relations analytics dashboard built on [MooseStack](https://docs.fiveonefour.com/moose) + ClickHouse. Combines live GitHub activity streaming with 18 weeks of historical marketing data into a single unified view.

[![MooseStack](https://img.shields.io/badge/built_with-MooseStack-5b21b6)](https://docs.fiveonefour.com/moose)
[![ClickHouse](https://img.shields.io/badge/database-ClickHouse-FFCC01)](https://clickhouse.com)

## What It Does

**Real-time layer** — GitHub webhooks flow into ClickHouse in under a second. Star, fork, and issue events appear in the live feed as they happen.

**Historical layer** — 18 weeks of real CloudQuery marketing snapshots: web traffic (Plausible), search rankings (GSC), AI referrals (ChatGPT · Claude · Perplexity), HubSpot pipeline MQLs, and platform WAU.

**Unified APIs** — Three REST endpoints query across both layers simultaneously. ClickHouse handles mixed real-time + historical queries in milliseconds.

## Architecture

```
GitHub webhook → POST /ingest/GithubEvent
                    └→ transform (fan-out)
                        └→ GithubProcessed table
                            └→ GithubDailyMV (star velocity)
                                └→ /api/github-signals

Python backfill → POST /ingest/WeeklySnapshot   (18 weeks, Feb–Apr 2026)
               → POST /ingest/AiReferralEvent   (per AI source per week)
                    └→ MarketingWeeklySummaryMV
                    └→ AiSourceWeeklyMV
                        └→ /api/marketing-trends

Both layers → /api/devrel-health (unified summary KPIs)
           → dashboard/index.html  (Chart.js, polls every 10s)
```

## Prerequisites

- [MooseStack CLI](https://docs.fiveonefour.com/moose/getting-started/quickstart): `npm i -g @514labs/moose-cli`
- Docker Desktop (running)
- Python 3.9+ with `requests` (`pip install requests`)
- Flask for the webhook adapter (`pip install flask requests`)

## Running the Stack

### 1. Start MooseStack

```bash
moose dev
```

Starts ClickHouse (port 18123), Redpanda streaming, Temporal workflows, and the API server on `localhost:4000`. All ingest endpoints and APIs register automatically from the TypeScript definitions.

### 2. Load historical data

```bash
python scripts/backfill.py
```

Reads 18 JSON snapshot files and POSTs them to the ingest endpoints. Idempotent — safe to re-run.

### 3. Start the webhook adapter (for live GitHub events)

```bash
python scripts/webhook_adapter.py
```

Runs a thin Flask proxy on port 3001 that receives GitHub webhooks and forwards them to MooseStack, extracting the `X-GitHub-Event` header that GitHub puts outside the body.

### 4. Serve the dashboard

```bash
cd dashboard && python3 -m http.server 3000
```

Open [http://localhost:3000](http://localhost:3000).

## GitHub Webhook Setup (for live demo)

1. Create a demo GitHub repo
2. Expose port 3001 via ngrok: `ngrok http 3001`
3. In your repo → **Settings → Webhooks → Add webhook**:
   - URL: `https://<ngrok-id>.ngrok.io/webhook`
   - Content type: `application/json`
   - Events: Stars, Forks, Issues, Pull requests
4. Star/fork the repo — events appear in the dashboard within seconds

## API Endpoints

| Endpoint | Cache | Description |
|----------|-------|-------------|
| `GET /api/devrel-health` | 60s | KPI summary: GitHub velocity + latest marketing snapshot + top AI sources |
| `GET /api/github-signals?windowDays=30` | 30s | Daily star/fork/issue stats + live event feed |
| `GET /api/marketing-trends?site=cloudquery.io` | 1h | Weekly traffic trend + AI source breakdown |

## Key Files

| File | Purpose |
|------|---------|
| `app/ingest/githubModels.ts` | GitHub event + normalized processed model |
| `app/ingest/githubTransforms.ts` | Fan-out transform: GithubEvent → GithubProcessed |
| `app/ingest/marketingModels.ts` | WeeklySnapshot + AiReferralEvent models |
| `app/views/githubMetrics.ts` | GithubDailyMV — star/fork/issue counts by day |
| `app/views/marketingTrends.ts` | MarketingWeeklySummaryMV + AiSourceWeeklyMV |
| `app/apis/devrel.ts` | Three REST APIs with MooseCache cache-aside |
| `app/workflows/snapshotSync.ts` | Weekly Temporal workflow to refresh snapshots |
| `scripts/backfill.py` | One-time backfill of 18 historical snapshots |
| `scripts/webhook_adapter.py` | Flask proxy: GitHub → MooseStack ingest |
| `dashboard/index.html` | Single-file dashboard (no build step) |

## Why This Stack

**Why ClickHouse?** Columnar storage — the 18-week aggregate query that powers the AI referrals chart runs in milliseconds regardless of row count. Mixed real-time + historical queries in the same SQL.

**Why streaming?** GitHub stars are a leading indicator. They appear 3–7 days before traffic shows up in GSC. Streaming gives you that signal in real time rather than waiting for the next weekly snapshot.

**Why MooseStack?** The TypeScript interface IS the pipeline definition. No YAML, no config files, no separate schema registry. The type is the schema — add a field to the interface and MooseStack handles the migration.

## Demo Script

1. `moose dev` — show ClickHouse + Redpanda + Temporal spinning up, point out auto-generated ingest endpoints at `/ingest/GithubEvent`, `/ingest/WeeklySnapshot`
2. `python scripts/backfill.py` — 18 weeks of real data loads in seconds; explain the schema normalization challenge across 6+ format variants
3. `moose query "SELECT aiSource, SUM(visitors) FROM AiReferralEvent_0_0 GROUP BY aiSource ORDER BY SUM(visitors) DESC"` — show `claude.ai` appearing as a referral source in March 2026 and growing (GEO becoming real)
4. Set up ngrok, star the repo — watch the event appear in the live feed
5. Open [http://localhost:3000](http://localhost:3000) — walk through the unified view

---

Built with [MooseStack](https://docs.fiveonefour.com/moose) by [514 labs](https://www.fiveonefour.com/).
