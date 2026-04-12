#!/usr/bin/env python3

import argparse
import csv
import json
import time
import urllib.error
from pathlib import Path
from typing import Dict, List

import fetch_world_rugby_match as match_tools


DEFAULT_INDEX_CSV = (
    match_tools.DEFAULT_DATA_ROOT / "processed" / "world_cup_official_match_index.csv"
)


def read_index_rows(index_csv: Path) -> List[Dict[str, str]]:
    with index_csv.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def should_include(row: Dict[str, str], years: List[str], limit: int, selected_count: int) -> bool:
    if years and row["tournament_year"] not in years:
        return False
    if limit and selected_count >= limit:
        return False
    return True


def fetch_with_retry(url: str, retries: int, sleep_seconds: float) -> Dict:
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            return match_tools.fetch_json(url)
        except (urllib.error.URLError, TimeoutError, ValueError) as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(sleep_seconds)
    raise last_error  # type: ignore[misc]


def save_failure_report(path: Path, failures: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(failures, indent=2, ensure_ascii=True), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Bulk import official Rugby World Cup matches into raw JSON and SQLite."
    )
    parser.add_argument(
        "--data-root",
        default=str(match_tools.DEFAULT_DATA_ROOT),
        help="Top-level data folder for raw JSON and SQLite outputs",
    )
    parser.add_argument(
        "--index-csv",
        default=str(DEFAULT_INDEX_CSV),
        help="Official match index CSV to import from",
    )
    parser.add_argument(
        "--years",
        nargs="*",
        default=[],
        help="Optional tournament years to import, for example 1999 2003 2007",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional number of matches to import for testing",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="Number of attempts per API request before marking a match as failed",
    )
    parser.add_argument(
        "--retry-sleep",
        type=float,
        default=0.5,
        help="Seconds to pause between retries",
    )
    args = parser.parse_args()

    data_root = Path(args.data_root).expanduser().resolve()
    index_csv = Path(args.index_csv).expanduser().resolve()
    rows = read_index_rows(index_csv)

    raw_summary_dir = data_root / "raw" / "match_summaries"
    raw_timeline_dir = data_root / "raw" / "match_timelines"
    processed_dir = data_root / "processed"
    failure_report = processed_dir / "world_cup_import_failures.json"
    database_path = processed_dir / "rwc_matches.sqlite"

    selected_rows: List[Dict[str, str]] = []
    for row in rows:
        if should_include(row, args.years, args.limit, len(selected_rows)):
            selected_rows.append(row)

    failures: List[Dict[str, str]] = []
    imported = 0

    for index, row in enumerate(selected_rows, start=1):
        match_id = row["match_id"]
        summary_url = row["summary_url"]
        timeline_url = row["timeline_url"]

        try:
            summary_payload = fetch_with_retry(summary_url, args.retries, args.retry_sleep)
            timeline_payload = fetch_with_retry(timeline_url, args.retries, args.retry_sleep)

            match_tools.save_json(raw_summary_dir / f"summary_{match_id}.json", summary_payload)
            match_tools.save_json(raw_timeline_dir / f"timeline_{match_id}.json", timeline_payload)
            match_tools.build_database(database_path, summary_payload, timeline_payload)

            imported += 1
            print(
                f"[{index}/{len(selected_rows)}] imported {match_id}: "
                f"{row['home_team_name']} v {row['away_team_name']} ({row['match_date']})"
            )
        except Exception as exc:  # pragma: no cover - live API/import errors are reported to file
            failures.append(
                {
                    "match_id": match_id,
                    "tournament_year": row["tournament_year"],
                    "home_team_name": row["home_team_name"],
                    "away_team_name": row["away_team_name"],
                    "match_date": row["match_date"],
                    "summary_url": summary_url,
                    "timeline_url": timeline_url,
                    "error": str(exc),
                }
            )
            print(
                f"[{index}/{len(selected_rows)}] failed {match_id}: "
                f"{row['home_team_name']} v {row['away_team_name']} -> {exc}"
            )

    save_failure_report(failure_report, failures)

    print()
    print(f"Imported {imported} matches into {database_path}")
    print(f"Saved raw summaries to {raw_summary_dir}")
    print(f"Saved raw timelines to {raw_timeline_dir}")
    print(f"Saved failure report to {failure_report}")


if __name__ == "__main__":
    main()
