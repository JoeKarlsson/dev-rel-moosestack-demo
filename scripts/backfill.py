#!/usr/bin/env python3
"""
Backfill historical CloudQuery marketing snapshots into MooseStack.

Reads all weekly JSON snapshots from the marketing-skills repo and POSTs
each one to the MooseStack ingest endpoints as WeeklySnapshot and
AiReferralEvent records.

Handles the 6+ schema variants that evolved across the 18 snapshots
(Feb–Apr 2026) by trying field names in priority order.

Usage:
  python3 scripts/backfill.py
  python3 scripts/backfill.py --latest-only
  python3 scripts/backfill.py --ingest-url http://localhost:4000
"""

import argparse
import json
import os
import sys
import urllib.request
import urllib.error
from datetime import datetime, timezone
from pathlib import Path

SNAPSHOTS_DIR = Path(__file__).resolve().parent.parent.parent.parent / \
    "cloudquery" / "marketing-skills" / "seo-data" / "snapshots"

DEFAULT_INGEST_URL = "http://localhost:4000"
SITE = "cloudquery.io"


def post_json(url: str, payload: dict) -> bool:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status in (200, 201, 204)
    except urllib.error.HTTPError as e:
        print(f"  HTTP {e.code}: {e.read().decode()[:200]}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"  Error: {e}", file=sys.stderr)
        return False


# ── Field extractors — each tries multiple key paths in priority order ──

def extract_visitors(d: dict) -> int:
    p = d.get("plausible", {})
    for path in [
        lambda p: p.get("visitors"),
        lambda p: p.get("agg_w1", {}).get("visitors"),
        lambda p: p.get("aggregate", {}).get("visitors"),
        lambda p: p.get("aggregate_m1", {}).get("visitors"),
        lambda p: p.get("visitors_w1"),
    ]:
        v = path(p)
        if v is not None:
            return int(v)
    return 0


def extract_pageviews(d: dict) -> int:
    p = d.get("plausible", {})
    for path in [
        lambda p: p.get("pageviews"),
        lambda p: p.get("agg_w1", {}).get("pageviews"),
        lambda p: p.get("aggregate", {}).get("pageviews"),
        lambda p: p.get("aggregate_m1", {}).get("pageviews"),
        lambda p: p.get("pageviews_w1"),
    ]:
        v = path(p)
        if v is not None:
            return int(v)
    return 0


def extract_bounce_rate(d: dict) -> float:
    p = d.get("plausible", {})
    for path in [
        lambda p: p.get("bounce_rate"),
        lambda p: p.get("agg_w1", {}).get("bounce_rate"),
        lambda p: p.get("aggregate", {}).get("bounce_rate"),
        lambda p: p.get("aggregate_m1", {}).get("bounce_rate"),
    ]:
        v = path(p)
        if v is not None:
            return float(v)
    return 0.0


def extract_visit_duration(d: dict) -> float:
    p = d.get("plausible", {})
    for path in [
        lambda p: p.get("visit_duration"),
        lambda p: p.get("agg_w1", {}).get("visit_duration"),
        lambda p: p.get("aggregate", {}).get("visit_duration"),
        lambda p: p.get("aggregate_m1", {}).get("visit_duration"),
    ]:
        v = path(p)
        if v is not None:
            return float(v)
    return 0.0


def extract_gsc(d: dict) -> dict:
    g = d.get("gsc", {})
    if g.get("unavailable"):
        return {"clicks": 0, "impressions": 0, "ctr": 0.0, "avg_position": 0.0}

    clicks = (
        g.get("total_clicks") or g.get("clicks") or g.get("clicks_w1")
        or (g.get("w1") or {}).get("clicks") or g.get("clicks_m1") or 0
    )
    impressions = (
        g.get("total_impressions") or g.get("impressions") or g.get("impressions_w1")
        or (g.get("w1") or {}).get("impressions") or g.get("impressions_m1") or 0
    )
    # CTR field changed name across versions
    ctr = (
        g.get("ctr") or g.get("avg_ctr") or g.get("ctr_w1")
        or (g.get("w1") or {}).get("ctr") or g.get("avg_ctr_m1") or 0.0
    )
    avg_position = (
        g.get("avg_position") or g.get("position") or g.get("avg_position_w1")
        or (g.get("w1") or {}).get("avg_position") or g.get("avg_position_m1") or 0.0
    )
    # Totals block used by HTML generator
    totals_w1 = (g.get("totals") or g.get("aggregate") or {}).get("w1", {})
    if totals_w1:
        clicks = clicks or totals_w1.get("clicks", 0)
        impressions = impressions or totals_w1.get("impressions", 0)
        ctr = ctr or totals_w1.get("ctr", 0.0)
        avg_position = avg_position or totals_w1.get("avg_position", 0.0)

    return {
        "clicks": int(clicks or 0),
        "impressions": int(impressions or 0),
        "ctr": float(ctr or 0.0),
        "avg_position": float(avg_position or 0.0),
    }


def extract_ahrefs(d: dict) -> dict:
    ah = d.get("ahrefs", {})
    traffic = (
        ah.get("total_traffic") or ah.get("overview_traffic")
        or ah.get("ahrefs_organic_traffic") or ah.get("organic_traffic_ahrefs")
        or ah.get("organic_traffic") or 0
    )
    # referring_domains may be an int count OR a list of domain objects
    rd_raw = ah.get("referring_domains")
    if isinstance(rd_raw, list):
        rd_raw = len(rd_raw)
    ref_domains = (
        rd_raw or ah.get("referring_domains_flat")
        or ah.get("referring_domains_count") or ah.get("total_ref_domains") or 0
    )
    return {
        "organic_traffic": int(traffic or 0),
        "referring_domains": int(ref_domains or 0),
    }


def extract_hubspot(d: dict) -> dict:
    hs = d.get("hubspot", {})
    if hs.get("status") == "unavailable" or not hs:
        return {"total_mqls": 0, "organic_mqls": 0}
    return {
        "total_mqls": int(hs.get("total_mqls") or hs.get("raw_total_mqls") or 0),
        "organic_mqls": int(hs.get("organic_mqls") or hs.get("raw_organic_mqls") or 0),
    }


def extract_platform(d: dict) -> dict:
    plat = d.get("platform", {})
    if not plat or plat.get("_available") is False:
        return {"mau": 0, "wau": 0, "new_teams": 0}
    return {
        "mau": int(plat.get("mau") or 0),
        "wau": int(plat.get("wau") or 0),
        "new_teams": int(plat.get("new_teams_this_week") or 0),
    }


def extract_ai_sources(d: dict) -> dict[str, int]:
    """
    Returns {source_name: visitor_count} handling all schema variants:
    - flat int: {"chatgpt": 84}
    - object: {"chatgpt.com": {"visitors": 12}}
    - metrics array: [{"dimensions": ["chatgpt.com"], "metrics": [73]}]
    - nested week keys: {"total_w1": 26, "by_source_w1": {"chatgpt.com": 12}}
    """
    ai = d.get("ai_referrals", {})
    sources: dict[str, int] = {}

    NAME_MAP = {
        "chatgpt": "chatgpt.com",
        "perplexity": "perplexity.ai",
        "claude_ai": "claude.ai",
        "Perplexity": "perplexity.ai",
        "kagi": "kagi.com",
    }

    def add(name: str, count: int):
        canonical = NAME_MAP.get(name, name)
        sources[canonical] = sources.get(canonical, 0) + int(count or 0)

    raw_sources = (
        ai.get("sources") or ai.get("by_source_w1") or ai.get("by_source_m1")
    )

    if isinstance(raw_sources, dict):
        for k, v in raw_sources.items():
            if isinstance(v, dict):
                add(k, v.get("visitors", 0))
            elif isinstance(v, (int, float)):
                add(k, v)
    elif isinstance(raw_sources, list):
        # metrics array: [{"dimensions": ["chatgpt.com"], "metrics": [73, ...]}]
        for row in raw_sources:
            dims = row.get("dimensions", [])
            metrics = row.get("metrics", [])
            if dims and metrics:
                add(dims[0], metrics[0])
    else:
        # Flat keys directly on ai_referrals object
        for key in ("chatgpt", "perplexity", "claude_ai", "kagi"):
            v = ai.get(key)
            if v is not None:
                add(key, v)

    # month/week variants
    for key in ("sources_m1", "sources_w1"):
        extra = ai.get(key)
        if isinstance(extra, list):
            for row in extra:
                dims = row.get("dimensions", [])
                metrics = row.get("metrics", [])
                if dims and metrics and not sources:
                    add(dims[0], metrics[0])

    return sources


def extract_health_score(d: dict) -> int:
    hs = d.get("health_score", {})
    if isinstance(hs, dict):
        return int(hs.get("total") or 0)
    if isinstance(hs, (int, float)):
        return int(hs)
    return 0


def extract_aio(d: dict) -> dict:
    aio = d.get("aio_analysis", {})
    if not aio:
        return {"lost_clicks": 0, "pct": 0.0}
    return {
        "lost_clicks": int(aio.get("estimated_lost_clicks") or 0),
        "pct": float(aio.get("affected_pct") or 0.0),
    }


def extract_ranking_changes(d: dict) -> dict:
    rc = d.get("ranking_changes", {})
    if not rc:
        return {"improved": 0, "declined": 0}
    imp = rc.get("improved", [])
    dec = rc.get("declined", [])
    return {
        "improved": len(imp) if isinstance(imp, list) else int(imp or 0),
        "declined": len(dec) if isinstance(dec, list) else int(dec or 0),
    }


def normalize_snapshot_date(d: dict, filename: str) -> str:
    """Extract ISO date string from snapshot metadata or filename."""
    date_str = d.get("_snapshot_date") or d.get("_date")
    if date_str:
        return date_str
    # Fall back to filename stem (e.g. 2026-04-17.json → 2026-04-17)
    return Path(filename).stem


def build_snapshot_record(d: dict, filename: str) -> dict:
    date_str = normalize_snapshot_date(d, filename)
    gsc = extract_gsc(d)
    ah = extract_ahrefs(d)
    hs = extract_hubspot(d)
    plat = extract_platform(d)
    ai_sources = extract_ai_sources(d)
    aio = extract_aio(d)
    rc = extract_ranking_changes(d)
    ai_total = int(d.get("ai_referrals", {}).get("total")
                   or d.get("ai_referrals", {}).get("total_w1")
                   or d.get("ai_referrals", {}).get("total_m1")
                   or sum(ai_sources.values())
                   or 0)

    return {
        "snapshotId": f"{SITE}_{date_str}",
        "snapshotDate": f"{date_str}T00:00:00Z",
        "site": SITE,
        "visitors": extract_visitors(d),
        "pageviews": extract_pageviews(d),
        "bounceRate": extract_bounce_rate(d),
        "visitDuration": extract_visit_duration(d),
        "gscClicks": gsc["clicks"],
        "gscImpressions": gsc["impressions"],
        "gscCtr": gsc["ctr"],
        "gscAvgPosition": gsc["avg_position"],
        "ahrefsOrganicTraffic": ah["organic_traffic"],
        "ahrefsReferringDomains": ah["referring_domains"],
        "hubspotTotalMqls": hs["total_mqls"],
        "hubspotOrganicMqls": hs["organic_mqls"],
        "platformMau": plat["mau"],
        "platformWau": plat["wau"],
        "platformNewTeams": plat["new_teams"],
        "aiTotalReferrals": ai_total,
        "healthScore": extract_health_score(d),
        "aioLostClicks": aio["lost_clicks"],
        "aioPct": aio["pct"],
        "rankingImproved": rc["improved"],
        "rankingDeclined": rc["declined"],
        "dataSource": "backfill",
    }


def build_ai_referral_records(d: dict, filename: str) -> list[dict]:
    date_str = normalize_snapshot_date(d, filename)
    sources = extract_ai_sources(d)
    records = []
    for source, visitors in sources.items():
        if visitors > 0:
            records.append({
                "eventId": f"{SITE}_{date_str}_{source}",
                "snapshotDate": f"{date_str}T00:00:00Z",
                "site": SITE,
                "aiSource": source,
                "visitors": visitors,
            })
    return records


def main():
    parser = argparse.ArgumentParser(description="Backfill marketing snapshots into MooseStack")
    parser.add_argument("--latest-only", action="store_true", help="Only ingest the most recent snapshot")
    parser.add_argument("--ingest-url", default=DEFAULT_INGEST_URL, help="MooseStack base URL")
    parser.add_argument("--dry-run", action="store_true", help="Print records without POSTing")
    args = parser.parse_args()

    snapshot_url = f"{args.ingest_url}/ingest/WeeklySnapshot"
    ai_url = f"{args.ingest_url}/ingest/AiReferralEvent"

    if not SNAPSHOTS_DIR.exists():
        print(f"Snapshots directory not found: {SNAPSHOTS_DIR}", file=sys.stderr)
        sys.exit(1)

    files = sorted([
        f for f in SNAPSHOTS_DIR.iterdir()
        if f.suffix == ".json" and f.is_file()
    ])

    if args.latest_only:
        files = files[-1:]

    print(f"Processing {len(files)} snapshot(s) from {SNAPSHOTS_DIR}")
    print(f"Ingesting to: {args.ingest_url}")
    print()

    ok_count = 0
    err_count = 0

    for f in files:
        print(f"  {f.name} ...", end=" ", flush=True)
        try:
            with open(f) as fh:
                d = json.load(fh)

            snapshot = build_snapshot_record(d, f.name)
            ai_records = build_ai_referral_records(d, f.name)

            if args.dry_run:
                print("DRY RUN")
                print(f"    snapshot: visitors={snapshot['visitors']}, gscClicks={snapshot['gscClicks']}, aiTotal={snapshot['aiTotalReferrals']}")
                print(f"    ai sources: {[r['aiSource']+':'+str(r['visitors']) for r in ai_records]}")
                ok_count += 1
                continue

            snap_ok = post_json(snapshot_url, snapshot)
            ai_ok = all(post_json(ai_url, rec) for rec in ai_records)

            if snap_ok and ai_ok:
                print(f"OK (visitors={snapshot['visitors']}, ai_sources={len(ai_records)})")
                ok_count += 1
            else:
                print("PARTIAL ERROR")
                err_count += 1

        except Exception as e:
            print(f"ERROR: {e}", file=sys.stderr)
            err_count += 1

    print()
    print(f"Done: {ok_count} succeeded, {err_count} failed")
    if err_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
