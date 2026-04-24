import {
  GithubEventPipeline,
  GithubProcessedPipeline,
  GithubEvent,
  GithubProcessed,
} from "./githubModels";

GithubEventPipeline.stream!.addTransform(
  GithubProcessedPipeline.stream!,
  async (event: GithubEvent): Promise<GithubProcessed> => {
    let issueNumber = 0;
    let issueTitle = "";
    let forkFullName = "";

    if (event.eventType === "issues" || event.eventType === "pull_request") {
      try {
        const payload = JSON.parse(event.rawPayload);
        const item = payload.issue ?? payload.pull_request ?? {};
        issueNumber = item.number ?? 0;
        issueTitle = item.title ?? "";
      } catch {
        // rawPayload malformed — leave defaults
      }
    }

    if (event.eventType === "fork") {
      try {
        const payload = JSON.parse(event.rawPayload);
        forkFullName = payload.forkee?.full_name ?? "";
      } catch {
        // rawPayload malformed — leave defaults
      }
    }

    return {
      eventId: event.deliveryId,
      timestamp: event.timestamp,
      eventType: event.eventType,
      repo: event.repo,
      actor: event.actor,
      action: event.action,
      issueNumber,
      issueTitle,
      forkFullName,
    };
  },
  { deadLetterQueue: GithubEventPipeline.deadLetterQueue }
);
