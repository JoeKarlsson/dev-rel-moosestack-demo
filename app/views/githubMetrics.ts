import typia from "typia";
import { MaterializedView, sql, DateTime } from "@514labs/moose-lib";
import { GithubProcessedPipeline } from "../ingest/githubModels";

// Events grouped by day + type + action — powers the star velocity chart and issue health view
interface GithubDailyMetric {
  date: DateTime;
  eventType: string;
  action: string;
  eventCount: number & typia.tags.Type<"int64">;
}

const processedTable = GithubProcessedPipeline.table!;
const cols = processedTable.columns;

export const GithubDailyMV = new MaterializedView<GithubDailyMetric>({
  tableName: "GithubDailyMetric",
  materializedViewName: "GithubDailyMetric_MV",
  orderByFields: ["date", "eventType", "action"],
  selectStatement: sql.statement`SELECT
    toDate(${cols.timestamp}) as date,
    ${cols.eventType},
    ${cols.action},
    count(${cols.eventId}) as eventCount
  FROM ${processedTable}
  GROUP BY toDate(${cols.timestamp}), ${cols.eventType}, ${cols.action}
  `,
  selectTables: [processedTable],
});
