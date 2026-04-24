import {
  IngestPipeline,
  Key,
  OlapTable,
  DeadLetterModel,
  DateTime,
} from "@514labs/moose-lib";

// Raw GitHub webhook payload — normalized by the webhook adapter before ingestion
export interface GithubEvent {
  deliveryId: Key<string>; // X-GitHub-Delivery header
  timestamp: DateTime;
  eventType: string; // "star" | "fork" | "issues" | "pull_request"
  repo: string; // "owner/repo"
  actor: string; // GitHub username
  action: string; // "created" | "opened" | "closed"
  rawPayload: string; // JSON.stringify of original webhook body
}

// Normalized event — one row per GitHub event, queryable by eventType
export interface GithubProcessed {
  eventId: Key<string>; // = deliveryId
  timestamp: DateTime;
  eventType: string;
  repo: string;
  actor: string;
  action: string;
  issueNumber: number; // 0 when not an issue event
  issueTitle: string; // "" when not an issue event
  forkFullName: string; // "" when not a fork event
}

export const githubDeadLetterTable = new OlapTable<DeadLetterModel>(
  "GithubDeadLetter",
  { orderByFields: ["failedAt"] }
);

// Receives raw GitHub webhook payloads — stream only, transforms fan out to GithubProcessed
export const GithubEventPipeline = new IngestPipeline<GithubEvent>(
  "GithubEvent",
  {
    table: false,
    stream: true,
    ingestApi: true,
    deadLetterQueue: { destination: githubDeadLetterTable },
  }
);

// Normalized events stored in ClickHouse — queried by APIs and materialized views
export const GithubProcessedPipeline = new IngestPipeline<GithubProcessed>(
  "GithubProcessed",
  {
    table: { orderByFields: ["eventId", "timestamp", "eventType"] },
    stream: true,
    ingestApi: false,
  }
);
