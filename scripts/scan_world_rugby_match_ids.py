#!/usr/bin/env python3

import argparse
import csv
import json
import socket
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Tuple


DEFAULT_DATA_ROOT = Path(
    "/Users/macbookprom1/Library/Mobile Documents/com~apple~CloudDocs/1 PROJECTS/rwc livery/data"
)
API_BASE = "https://api.wr-rims-prod.pulselive.com/rugby/v3/match"


def fetch_summary(match_id: int, timeout: float) -> Optional[Dict]:
    url = f"{API_BASE}/{match_id}/summary"
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise
    except (urllib.error.URLError, socket.timeout, TimeoutError):
        return None


def scan_one(match_id: int, timeout: float) -> Tuple[int, Optional[Dict], Optional[str]]:
    try:
        payload = fetch_summary(match_id, timeout)
        if payload is None:
            return match_id, None, None
        row = summarize_match(payload)
        return match_id, row, None
    except ValueError:
        return match_id, None, "unusable summary payload"
    except Exception as exc:  # pragma: no cover - defensive logging for live scans
        return match_id, None, str(exc)


def summarize_match(summary_payload: Dict) -> Dict:
    match = summary_payload.get("match")
    if not isinstance(match, dict):
        raise ValueError("Summary payload does not contain a usable match object")
    teams = match.get("teams", [])
    venue = match.get("venue") or {}
    phase = match.get("eventPhaseId") or {}
    scores = match.get("scores") or [None, None]

    home_team = teams[0] if len(teams) > 0 else {}
    away_team = teams[1] if len(teams) > 1 else {}

    return {
        "match_id": match.get("matchId"),
        "competition": match.get("competition"),
        "event_phase": match.get("eventPhase"),
        "phase_type": phase.get("type"),
        "phase_subtype": phase.get("subType"),
        "match_date": (match.get("time") or {}).get("label"),
        "venue_name": venue.get("name"),
        "venue_city": venue.get("city"),
        "venue_country": venue.get("country"),
        "attendance": match.get("attendance"),
        "home_team": home_team.get("name"),
        "away_team": away_team.get("name"),
        "home_score": scores[0],
        "away_score": scores[1],
        "status": match.get("status"),
        "outcome": match.get("outcome"),
        "summary_url": f"{API_BASE}/{match.get('matchId')}/summary",
    }


def write_outputs(data_root: Path, label: str, rows: List[Dict]) -> Tuple[Path, Path]:
    scan_dir = data_root / "processed" / "match_scans"
    scan_dir.mkdir(parents=True, exist_ok=True)

    json_path = scan_dir / f"{label}.json"
    csv_path = scan_dir / f"{label}.csv"

    json_path.write_text(json.dumps(rows, indent=2, ensure_ascii=True), encoding="utf-8")

    fieldnames = [
        "match_id",
        "competition",
        "event_phase",
        "phase_type",
        "phase_subtype",
        "match_date",
        "venue_name",
        "venue_city",
        "venue_country",
        "attendance",
        "home_team",
        "away_team",
        "home_score",
        "away_score",
        "status",
        "outcome",
        "summary_url",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    return json_path, csv_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scan a range of World Rugby match IDs and keep the successful summaries."
    )
    parser.add_argument("start_id", type=int, help="First match id to test")
    parser.add_argument("end_id", type=int, help="Last match id to test (inclusive)")
    parser.add_argument(
        "--timeout",
        type=float,
        default=3.0,
        help="Seconds to wait for each API response before skipping the id",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=16,
        help="Number of concurrent requests to run",
    )
    parser.add_argument(
        "--data-root",
        default=str(DEFAULT_DATA_ROOT),
        help="Top-level data folder for scan outputs",
    )
    args = parser.parse_args()

    if args.end_id < args.start_id:
        raise SystemExit("end_id must be greater than or equal to start_id")
    if args.workers < 1:
        raise SystemExit("workers must be at least 1")

    data_root = Path(args.data_root).expanduser().resolve()
    rows = []
    checked = args.end_id - args.start_id + 1
    match_ids = list(range(args.start_id, args.end_id + 1))

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        future_map = {
            executor.submit(scan_one, match_id, args.timeout): match_id
            for match_id in match_ids
        }
        for future in as_completed(future_map):
            match_id, row, error = future.result()
            if row:
                rows.append(row)
                print(
                    f"FOUND {match_id}: {row['home_team']} v {row['away_team']} ({row['match_date']})",
                    flush=True,
                )
            elif error:
                print(f"SKIP  {match_id}: {error}", flush=True)
            else:
                print(f"MISS  {match_id}", flush=True)

    rows.sort(key=lambda item: int(item["match_id"]))

    label = f"match_scan_{args.start_id}_{args.end_id}"
    json_path, csv_path = write_outputs(data_root, label, rows)

    print()
    print(f"Checked {checked} ids")
    print(f"Found {len(rows)} matches")
    print(f"Saved JSON index to {json_path}")
    print(f"Saved CSV index to {csv_path}")


if __name__ == "__main__":
    main()
