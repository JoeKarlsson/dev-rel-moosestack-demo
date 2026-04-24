import typia from "typia";
import { MaterializedView, sql, DateTime } from "@514labs/moose-lib";
import {
  WeeklySnapshotPipeline,
  AiReferralEventPipeline,
} from "../ingest/marketingModels";

// AI referral totals by source over time — the "is claude.ai growing?" chart
interface AiSourceWeekly {
  snapshotDate: DateTime;
  site: string;
  aiSource: string;
  totalVisitors: number & typia.tags.Type<"int64">;
}

const aiTable = AiReferralEventPipeline.table!;
const aiCols = aiTable.columns;

export const AiSourceWeeklyMV = new MaterializedView<AiSourceWeekly>({
  tableName: "AiSourceWeekly",
  materializedViewName: "AiSourceWeekly_MV",
  orderByFields: ["snapshotDate", "site", "aiSource"],
  selectStatement: sql.statement`SELECT
    ${aiCols.snapshotDate},
    ${aiCols.site},
    ${aiCols.aiSource},
    sum(${aiCols.visitors}) as totalVisitors
  FROM ${aiTable}
  GROUP BY ${aiCols.snapshotDate}, ${aiCols.site}, ${aiCols.aiSource}
  `,
  selectTables: [aiTable],
});

// Weekly traffic + search metrics summary — the main marketing trend table
interface MarketingWeeklySummary {
  snapshotDate: DateTime;
  site: string;
  visitors: number & typia.tags.Type<"int64">;
  gscClicks: number & typia.tags.Type<"int64">;
  gscAvgPosition: number;
  hubspotOrganicMqls: number & typia.tags.Type<"int64">;
  aiTotalReferrals: number & typia.tags.Type<"int64">;
  platformWau: number & typia.tags.Type<"int64">;
  healthScore: number & typia.tags.Type<"int64">;
  aioLostClicks: number & typia.tags.Type<"int64">;
  aioPct: number;
  rankingImproved: number & typia.tags.Type<"int64">;
  rankingDeclined: number & typia.tags.Type<"int64">;
}

const snapshotTable = WeeklySnapshotPipeline.table!;
const snapCols = snapshotTable.columns;

export const MarketingWeeklySummaryMV = new MaterializedView<MarketingWeeklySummary>(
  {
    tableName: "MarketingWeeklySummary",
    materializedViewName: "MarketingWeeklySummary_MV",
    orderByFields: ["snapshotDate", "site"],
    selectStatement: sql.statement`SELECT
    ${snapCols.snapshotDate},
    ${snapCols.site},
    sum(${snapCols.visitors}) as visitors,
    sum(${snapCols.gscClicks}) as gscClicks,
    anyIf(${snapCols.gscAvgPosition}, ${snapCols.gscAvgPosition} > 0) as gscAvgPosition,
    sum(${snapCols.hubspotOrganicMqls}) as hubspotOrganicMqls,
    sum(${snapCols.aiTotalReferrals}) as aiTotalReferrals,
    sum(${snapCols.platformWau}) as platformWau,
    max(${snapCols.healthScore}) as healthScore,
    max(${snapCols.aioLostClicks}) as aioLostClicks,
    max(${snapCols.aioPct}) as aioPct,
    max(${snapCols.rankingImproved}) as rankingImproved,
    max(${snapCols.rankingDeclined}) as rankingDeclined
  FROM ${snapshotTable}
  GROUP BY ${snapCols.snapshotDate}, ${snapCols.site}
  `,
    selectTables: [snapshotTable],
  }
);
