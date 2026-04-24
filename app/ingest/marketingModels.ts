import { IngestPipeline, Key, DateTime } from "@514labs/moose-lib";

// One row per weekly snapshot — all key marketing metrics flattened for easy trend queries
export interface WeeklySnapshot {
  snapshotId: Key<string>; // "cloudquery.io_2026-04-17"
  snapshotDate: DateTime;
  site: string;
  // Plausible (web analytics)
  visitors: number;
  pageviews: number;
  bounceRate: number;
  visitDuration: number;
  // Google Search Console
  gscClicks: number;
  gscImpressions: number;
  gscCtr: number;
  gscAvgPosition: number;
  // Ahrefs (SEO)
  ahrefsOrganicTraffic: number;
  ahrefsReferringDomains: number;
  // HubSpot (pipeline)
  hubspotTotalMqls: number;
  hubspotOrganicMqls: number;
  // Platform product metrics (Metabase)
  platformMau: number;
  platformWau: number;
  platformNewTeams: number;
  // AI referral aggregate
  aiTotalReferrals: number;
  // Ingestion metadata
  dataSource: string; // "backfill" | "weekly_workflow"
}

// One row per AI referral source per snapshot — enables per-source trend queries
export interface AiReferralEvent {
  eventId: Key<string>; // "cloudquery.io_2026-04-17_chatgpt.com"
  snapshotDate: DateTime;
  site: string;
  aiSource: string; // "chatgpt.com" | "claude.ai" | "perplexity.ai" | "kagi.com"
  visitors: number;
}

export const WeeklySnapshotPipeline = new IngestPipeline<WeeklySnapshot>(
  "WeeklySnapshot",
  {
    table: { orderByFields: ["snapshotId", "snapshotDate", "site"] },
    stream: true,
    ingestApi: true,
  }
);

export const AiReferralEventPipeline = new IngestPipeline<AiReferralEvent>(
  "AiReferralEvent",
  {
    table: { orderByFields: ["eventId", "snapshotDate", "aiSource"] },
    stream: true,
    ingestApi: true,
  }
);
