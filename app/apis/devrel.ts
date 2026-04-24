import { Api, MooseCache } from "@514labs/moose-lib";
import { GithubDailyMV } from "../views/githubMetrics";
import { AiSourceWeeklyMV, MarketingWeeklySummaryMV } from "../views/marketingTrends";
import { GithubProcessedPipeline } from "../ingest/githubModels";
import { WeeklySnapshotPipeline } from "../ingest/marketingModels";
import { tags } from "typia";

// ── /api/github-signals ────────────────────────────────────────────────────

interface GithubSignalsParams {
  repo?: string;
  windowDays?: number & tags.Type<"int32">;
  limit?: number & tags.Type<"int32">;
}

interface GithubDailyStat {
  dateStr: string;
  eventType: string;
  action: string;
  eventCount: number;
}

interface RecentEvent {
  eventId: string;
  timestamp: string;
  eventType: string;
  repo: string;
  actor: string;
  action: string;
}

interface GithubTotals {
  starsLast7d: number;
  forksLast7d: number;
  issuesOpenedLast7d: number;
  totalEventsTracked: number;
}

interface GithubSignalsResponse {
  dailyStats: GithubDailyStat[];
  starTotal: number;
  forkTotal: number;
  issueTotal: number;
  recentEvents: RecentEvent[];
}

export const GithubSignalsApi = new Api<GithubSignalsParams, GithubSignalsResponse>(
  "github-signals",
  async ({ windowDays = 30, limit = 10 }, { client, sql }) => {
    const cache = await MooseCache.get();
    const cacheKey = `github-signals:${windowDays}:${limit}`;

    const cached = await cache.get<GithubSignalsResponse>(cacheKey);
    if (cached) return cached;

    const mvTable = GithubDailyMV.targetTable;
    const mvCols = mvTable.columns;
    const pTable = GithubProcessedPipeline.table!;
    const pCols = pTable.columns;

    const dailyCursor = await client.query.execute<GithubDailyStat>(
      sql.statement`SELECT
        toString(${mvCols.date}) as dateStr,
        ${mvCols.eventType},
        ${mvCols.action},
        ${mvCols.eventCount}
      FROM ${mvTable}
      WHERE ${mvCols.date} >= today() - ${windowDays}
      ORDER BY ${mvCols.date} DESC, ${mvCols.eventType}`
    );

    const totalsCursor = await client.query.execute<GithubTotals>(
      sql.statement`SELECT
        countIf(${pCols.eventType} = 'star' AND ${pCols.action} = 'created') as starsLast7d,
        countIf(${pCols.eventType} = 'fork') as forksLast7d,
        countIf(${pCols.eventType} = 'issues' AND ${pCols.action} = 'opened') as issuesOpenedLast7d,
        count() as totalEventsTracked
      FROM ${pTable}
      WHERE ${pCols.timestamp} >= now() - INTERVAL 7 DAY`
    );

    const recentCursor = await client.query.execute<RecentEvent>(
      sql.statement`SELECT
        toString(${pCols.eventId}) as eventId,
        formatDateTime(${pCols.timestamp}, '%Y-%m-%dT%H:%i:%SZ', 'UTC') as timestamp,
        ${pCols.eventType},
        ${pCols.repo},
        ${pCols.actor},
        ${pCols.action}
      FROM ${pTable}
      ORDER BY ${pCols.timestamp} DESC
      LIMIT ${limit}`
    );

    const dailyData: GithubDailyStat[] = await dailyCursor.json();
    const totalsData: GithubTotals[] = await totalsCursor.json();
    const recentData: RecentEvent[] = await recentCursor.json();

    const totals = totalsData[0] ?? { starsLast7d: 0, forksLast7d: 0, issuesOpenedLast7d: 0, totalEventsTracked: 0 };

    const result: GithubSignalsResponse = {
      dailyStats: dailyData,
      starTotal: totals.starsLast7d,
      forkTotal: totals.forksLast7d,
      issueTotal: totals.issuesOpenedLast7d,
      recentEvents: recentData,
    };

    await cache.set(cacheKey, result, 30); // 30s cache — near real-time
    return result;
  }
);

// ── /api/marketing-trends ─────────────────────────────────────────────────

interface MarketingTrendsParams {
  site?: string;
  limit?: number & tags.Type<"int32">;
}

interface MarketingTrendRow {
  snapshotDate: string;
  site: string;
  visitors: number;
  gscClicks: number;
  gscAvgPosition: number;
  hubspotOrganicMqls: number;
  aiTotalReferrals: number;
  platformWau: number;
  healthScore: number;
  aioLostClicks: number;
  aioPct: number;
  rankingImproved: number;
  rankingDeclined: number;
}

interface AiSourceRow {
  snapshotDate: string;
  aiSource: string;
  totalVisitors: number;
}

interface MarketingTrendsResponse {
  weeklyTrend: MarketingTrendRow[];
  aiSourceBreakdown: AiSourceRow[];
}

export const MarketingTrendsApi = new Api<MarketingTrendsParams, MarketingTrendsResponse>(
  "marketing-trends",
  async ({ site = "cloudquery.io", limit = 20 }, { client, sql }) => {
    const cache = await MooseCache.get();
    const cacheKey = `marketing-trends:${site}:${limit}`;

    const cached = await cache.get<MarketingTrendsResponse>(cacheKey);
    if (cached) return cached;

    const summaryTable = MarketingWeeklySummaryMV.targetTable;
    const sCols = summaryTable.columns;
    const aiTable = AiSourceWeeklyMV.targetTable;
    const aCols = aiTable.columns;

    const trendCursor = await client.query.execute<MarketingTrendRow>(
      sql.statement`SELECT
        toString(${sCols.snapshotDate}) as snapshotDate,
        ${sCols.site},
        ${sCols.visitors},
        ${sCols.gscClicks},
        ${sCols.gscAvgPosition},
        ${sCols.hubspotOrganicMqls},
        ${sCols.aiTotalReferrals},
        ${sCols.platformWau},
        ${sCols.healthScore},
        ${sCols.aioLostClicks},
        ${sCols.aioPct},
        ${sCols.rankingImproved},
        ${sCols.rankingDeclined}
      FROM ${summaryTable}
      WHERE ${sCols.site} = ${site}
      ORDER BY ${sCols.snapshotDate} ASC
      LIMIT ${limit}`
    );

    const aiCursor = await client.query.execute<AiSourceRow>(
      sql.statement`SELECT
        toString(${aCols.snapshotDate}) as snapshotDate,
        ${aCols.aiSource},
        ${aCols.totalVisitors}
      FROM ${aiTable}
      WHERE ${aCols.site} = ${site}
      ORDER BY ${aCols.snapshotDate} ASC, ${aCols.totalVisitors} DESC
      LIMIT ${limit * 10}`
    );

    const trendData: MarketingTrendRow[] = await trendCursor.json();
    const aiData: AiSourceRow[] = await aiCursor.json();

    const result: MarketingTrendsResponse = {
      weeklyTrend: trendData,
      aiSourceBreakdown: aiData,
    };

    await cache.set(cacheKey, result, 3600); // 1h cache — weekly data
    return result;
  }
);

// ── /api/devrel-health ────────────────────────────────────────────────────

interface DevRelHealthParams {
  site?: string;
}

interface LatestSnapshot {
  latestSnapshotDate: string;
  visitors: number;
  gscClicks: number;
  gscAvgPosition: number;
  hubspotOrganicMqls: number;
  platformWau: number;
}

interface AiTopSource {
  source: string;
  visitors: number;
}

interface DevRelHealthResponse {
  asOf: string;
  github: {
    starsLast7d: number;
    forksLast7d: number;
    issuesOpenedLast7d: number;
    totalEventsTracked: number;
  };
  marketing: {
    latestSnapshotDate: string;
    visitors: number;
    gscClicks: number;
    gscAvgPosition: number;
    hubspotOrganicMqls: number;
    platformWau: number;
  };
  aiReferrals: {
    total: number;
    topSources: AiTopSource[];
  };
}

export const DevRelHealthApi = new Api<DevRelHealthParams, DevRelHealthResponse>(
  "devrel-health",
  async ({ site = "cloudquery.io" }, { client, sql }) => {
    const cache = await MooseCache.get();
    const cacheKey = `devrel-health:${site}`;

    const cached = await cache.get<DevRelHealthResponse>(cacheKey);
    if (cached) return cached;

    const pTable = GithubProcessedPipeline.table!;
    const pCols = pTable.columns;
    const snapTable = WeeklySnapshotPipeline.table!;
    const sCols = snapTable.columns;
    const aiTable = AiSourceWeeklyMV.targetTable;
    const aCols = aiTable.columns;

    const githubCursor = await client.query.execute<GithubTotals>(
      sql.statement`SELECT
        countIf(${pCols.eventType} = 'star' AND ${pCols.action} = 'created') as starsLast7d,
        countIf(${pCols.eventType} = 'fork') as forksLast7d,
        countIf(${pCols.eventType} = 'issues' AND ${pCols.action} = 'opened') as issuesOpenedLast7d,
        count() as totalEventsTracked
      FROM ${pTable}
      WHERE ${pCols.timestamp} >= now() - INTERVAL 7 DAY`
    );

    const snapCursor = await client.query.execute<LatestSnapshot>(
      sql.statement`SELECT
        toString(${sCols.snapshotDate}) as latestSnapshotDate,
        ${sCols.visitors},
        ${sCols.gscClicks},
        ${sCols.gscAvgPosition},
        ${sCols.hubspotOrganicMqls},
        ${sCols.platformWau}
      FROM ${snapTable}
      WHERE ${sCols.site} = ${site}
      ORDER BY ${sCols.snapshotDate} DESC
      LIMIT 1`
    );

    const aiCursor = await client.query.execute<AiTopSource>(
      sql.statement`SELECT
        ${aCols.aiSource} as source,
        sum(${aCols.totalVisitors}) as visitors
      FROM ${aiTable}
      WHERE ${aCols.site} = ${site}
        AND ${aCols.snapshotDate} >= today() - 30
      GROUP BY ${aCols.aiSource}
      ORDER BY visitors DESC
      LIMIT 5`
    );

    const githubData: GithubTotals[] = await githubCursor.json();
    const snapData: LatestSnapshot[] = await snapCursor.json();
    const aiData: AiTopSource[] = await aiCursor.json();

    const gh = githubData[0] ?? { starsLast7d: 0, forksLast7d: 0, issuesOpenedLast7d: 0, totalEventsTracked: 0 };
    const snap = snapData[0] ?? { latestSnapshotDate: "", visitors: 0, gscClicks: 0, gscAvgPosition: 0, hubspotOrganicMqls: 0, platformWau: 0 };

    const result: DevRelHealthResponse = {
      asOf: new Date().toISOString(),
      github: {
        starsLast7d: gh.starsLast7d,
        forksLast7d: gh.forksLast7d,
        issuesOpenedLast7d: gh.issuesOpenedLast7d,
        totalEventsTracked: gh.totalEventsTracked,
      },
      marketing: {
        latestSnapshotDate: snap.latestSnapshotDate,
        visitors: snap.visitors,
        gscClicks: snap.gscClicks,
        gscAvgPosition: snap.gscAvgPosition,
        hubspotOrganicMqls: snap.hubspotOrganicMqls,
        platformWau: snap.platformWau,
      },
      aiReferrals: {
        total: aiData.reduce((sum, r) => sum + r.visitors, 0),
        topSources: aiData,
      },
    };

    await cache.set(cacheKey, result, 60); // 1min cache
    return result;
  }
);

// ── /api/system-stats ─────────────────────────────────────────────────────

interface SystemStatsParams {}

interface TableStat {
  table: string;
  rows: number;
  compressed_bytes: number;
}

interface SystemStatsResponse {
  asOf: string;
  tables: TableStat[];
  totalRows: number;
  totalBytes: number;
}

export const SystemStatsApi = new Api<SystemStatsParams, SystemStatsResponse>(
  "system-stats",
  async (_params, { client, sql }) => {
    const cursor = await client.query.execute<TableStat>(
      sql.statement`SELECT
        name as table,
        total_rows as rows,
        total_bytes as compressed_bytes
      FROM system.tables
      WHERE database = currentDatabase()
        AND name IN (
          'GithubProcessed',
          'WeeklySnapshot',
          'AiReferralEvent',
          'GithubDailyMetric',
          'MarketingWeeklySummary',
          'AiSourceWeekly'
        )
      ORDER BY total_rows DESC`
    );

    const tables: TableStat[] = await cursor.json();

    return {
      asOf: new Date().toISOString(),
      tables,
      totalRows: tables.reduce((s, t) => s + t.rows, 0),
      totalBytes: tables.reduce((s, t) => s + t.compressed_bytes, 0),
    };
  }
);
