#!/usr/bin/env python3

import argparse
import csv
import json
import re
import urllib.request
from pathlib import Path
from typing import Dict, List, Tuple


DEFAULT_DATA_ROOT = Path(
    "/Users/macbookprom1/Library/Mobile Documents/com~apple~CloudDocs/1 PROJECTS/rwc livery/data"
)
API_BASE = "https://api.wr-rims-prod.pulselive.com/rugby/v3"
USER_AGENT = "Mozilla/5.0"
API_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "application/json",
    "account": "worldrugby",
    "X-Pulse-Application-Name": "worldrugby",
    "X-Pulse-Application-Version": "v1.6.30",
}

ARCHIVE_PAGES = {
    "1987": "https://www.rugbyworldcup.com/2027/en/past-tournaments/1987",
    "1991": "https://www.rugbyworldcup.com/2027/en/past-tournaments/1991",
    "1995": "https://www.rugbyworldcup.com/2027/en/past-tournaments/1995",
    "1999": "https://www.rugbyworldcup.com/2027/en/past-tournaments/1999",
    "2003": "https://www.rugbyworldcup.com/2027/en/past-tournaments/2003",
    "2007": "https://www.rugbyworldcup.com/2027/en/past-tournaments/2007",
    "2011": "https://www.rugbyworldcup.com/2027/en/past-tournaments/2011",
    "2015": "https://www.rugbyworldcup.com/2027/en/past-tournaments/2015",
    "2019": "https://www.rugbyworldcup.com/2027/en/past-tournaments/2019",
    "2023": "https://www.rugbyworldcup.com/2027/en/past-tournaments/2023",
    "2027": "https://www.rugbyworldcup.com/2027/en/matches",
}


def fetch_text(url: str) -> str:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=30) as response:
        return response.read().decode("utf-8", errors="ignore")


def fetch_json(url: str) -> Dict:
    request = urllib.request.Request(url, headers=API_HEADERS)
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def extract_tournament_id(html: str) -> str:
    match = re.search(r'data-tournament-id="([^"]+)"', html)
    if not match:
        raise ValueError("Could not find data-tournament-id in page HTML")
    return match.group(1)


def flatten_match_row(year: str, tournament_id: str, schedule_payload: Dict, match: Dict) -> Dict:
    teams = match.get("teams") or []
    scores = match.get("scores") or [None, None]
    venue = match.get("venue") or {}
    phase = match.get("eventPhaseId") or {}

    home_team = teams[0] if len(teams) > 0 else {}
    away_team = teams[1] if len(teams) > 1 else {}
    match_id = match.get("matchId")

    return {
        "tournament_year": year,
        "tournament_id": tournament_id,
        "competition": (schedule_payload.get("event") or {}).get("label"),
        "match_id": match_id,
        "match_alt_id": match.get("matchAltId"),
        "description": match.get("description"),
        "event_phase": match.get("eventPhase"),
        "phase_type": phase.get("type"),
        "phase_subtype": phase.get("subType"),
        "match_date": (match.get("time") or {}).get("label"),
        "venue_name": venue.get("name"),
        "venue_city": venue.get("city"),
        "venue_country": venue.get("country"),
        "attendance": match.get("attendance"),
        "home_team_id": home_team.get("id"),
        "home_team_name": home_team.get("name"),
        "away_team_id": away_team.get("id"),
        "away_team_name": away_team.get("name"),
        "home_score": scores[0],
        "away_score": scores[1],
        "status": match.get("status"),
        "outcome": match.get("outcome"),
        "match_url": f"https://www.world.rugby/beta/en/match/{match_id}",
        "summary_url": f"{API_BASE}/match/{match_id}/summary",
        "timeline_url": f"{API_BASE}/match/{match_id}/timeline",
    }


def write_json(path: Path, payload: Dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")


def write_csv(path: Path, rows: List[Dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        fieldnames: List[str] = []
    else:
        fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def sort_key(row: Dict) -> Tuple[str, str, str]:
    return (
        row["tournament_year"],
        row["match_date"] or "",
        str(row["match_id"]),
    )


def harvest_year(year: str, page_url: str, raw_dir: Path) -> Tuple[str, Dict, List[Dict]]:
    html = fetch_text(page_url)
    tournament_id = extract_tournament_id(html)
    schedule_url = f"{API_BASE}/event/{tournament_id}/schedule"
    schedule_payload = fetch_json(schedule_url)

    write_json(raw_dir / f"tournament_{year}_schedule.json", schedule_payload)

    rows = [
        flatten_match_row(year, tournament_id, schedule_payload, match)
        for match in schedule_payload.get("matches", [])
    ]
    return tournament_id, schedule_payload, rows


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Harvest official Rugby World Cup schedules from tournament archive pages."
    )
    parser.add_argument(
        "--years",
        nargs="*",
        default=list(ARCHIVE_PAGES.keys()),
        help="Tournament years to harvest",
    )
    parser.add_argument(
        "--data-root",
        default=str(DEFAULT_DATA_ROOT),
        help="Top-level data folder for raw and processed outputs",
    )
    args = parser.parse_args()

    years = [year for year in args.years if year in ARCHIVE_PAGES]
    if not years:
        raise SystemExit("No valid years supplied")

    data_root = Path(args.data_root).expanduser().resolve()
    raw_dir = data_root / "raw" / "official_schedules"
    processed_dir = data_root / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    all_rows: List[Dict] = []
    tournament_index: List[Dict] = []

    for year in years:
        page_url = ARCHIVE_PAGES[year]
        tournament_id, schedule_payload, rows = harvest_year(year, page_url, raw_dir)
        tournament_index.append(
            {
                "tournament_year": year,
                "tournament_id": tournament_id,
                "competition": (schedule_payload.get("event") or {}).get("label"),
                "match_count": len(rows),
                "source_page": page_url,
                "schedule_url": f"{API_BASE}/event/{tournament_id}/schedule",
            }
        )
        all_rows.extend(rows)
        print(f"{year}: tournament {tournament_id} -> {len(rows)} matches")

    all_rows.sort(key=sort_key)

    write_csv(processed_dir / "world_cup_official_match_index.csv", all_rows)
    write_json(processed_dir / "world_cup_official_match_index.json", all_rows)
    write_csv(processed_dir / "world_cup_tournament_index.csv", tournament_index)
    write_json(processed_dir / "world_cup_tournament_index.json", tournament_index)

    print()
    print(f"Saved master match index to {processed_dir / 'world_cup_official_match_index.csv'}")
    print(f"Saved tournament index to {processed_dir / 'world_cup_tournament_index.csv'}")


if __name__ == "__main__":
    main()
