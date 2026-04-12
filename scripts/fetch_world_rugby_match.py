#!/usr/bin/env python3

import argparse
import json
import sqlite3
import urllib.request
from pathlib import Path


DEFAULT_DATA_ROOT = Path(
    "/Users/macbookprom1/Library/Mobile Documents/com~apple~CloudDocs/1 PROJECTS/rwc livery/data"
)
API_BASE = "https://api.wr-rims-prod.pulselive.com/rugby/v3/match"


def fetch_json(url: str) -> dict:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def save_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")


def create_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS matches (
            match_id TEXT PRIMARY KEY,
            match_alt_id TEXT,
            description TEXT,
            competition TEXT,
            event_phase TEXT,
            phase_type TEXT,
            phase_subtype TEXT,
            match_date TEXT,
            venue_id TEXT,
            venue_name TEXT,
            venue_city TEXT,
            venue_country TEXT,
            attendance INTEGER,
            home_team_id TEXT,
            home_team_name TEXT,
            home_team_abbreviation TEXT,
            away_team_id TEXT,
            away_team_name TEXT,
            away_team_abbreviation TEXT,
            home_score INTEGER,
            away_score INTEGER,
            status TEXT,
            outcome TEXT
        );

        CREATE TABLE IF NOT EXISTS officials (
            match_id TEXT,
            role TEXT,
            official_id TEXT,
            official_name TEXT,
            PRIMARY KEY (match_id, role, official_id)
        );

        CREATE TABLE IF NOT EXISTS lineups (
            match_id TEXT,
            team_id TEXT,
            team_name TEXT,
            player_id TEXT,
            player_name TEXT,
            squad_role TEXT,
            shirt_number TEXT,
            position_code TEXT,
            position_label TEXT,
            is_replacement INTEGER,
            order_index INTEGER,
            PRIMARY KEY (match_id, team_id, player_id, squad_role, order_index)
        );

        CREATE TABLE IF NOT EXISTS scoring_events (
            match_id TEXT,
            team_id TEXT,
            team_name TEXT,
            event_family TEXT,
            phase TEXT,
            event_time_secs INTEGER,
            event_time_label TEXT,
            type_code TEXT,
            type_label TEXT,
            points INTEGER,
            player_id TEXT,
            player_alt_id TEXT,
            player_name TEXT
        );

        CREATE TABLE IF NOT EXISTS timeline_events (
            match_id TEXT,
            event_index INTEGER,
            team_id TEXT,
            team_name TEXT,
            phase TEXT,
            event_time_secs INTEGER,
            event_time_label TEXT,
            type_code TEXT,
            type_label TEXT,
            event_group TEXT,
            points INTEGER,
            player_id TEXT,
            player_alt_id TEXT,
            player_name TEXT,
            link_id INTEGER,
            PRIMARY KEY (match_id, event_index)
        );
        """
    )


def delete_existing_rows(connection: sqlite3.Connection, match_id: str) -> None:
    for table_name in ("officials", "lineups", "scoring_events", "timeline_events"):
        connection.execute(f"DELETE FROM {table_name} WHERE match_id = ?", (match_id,))


def player_lookup_from_summary(summary_payload: dict) -> dict:
    lookup = {}
    for team_block in summary_payload.get("teams", []):
        for entry in team_block.get("teamList", {}).get("list", []):
            player = entry.get("player") or {}
            player_id = player.get("id")
            if player_id:
                lookup[player_id] = (player.get("name") or {}).get("display")
    return lookup


def insert_match(connection: sqlite3.Connection, summary_payload: dict) -> str:
    match = summary_payload["match"]
    teams = match.get("teams", [])
    scores = match.get("scores") or [None, None]
    venue = match.get("venue") or {}
    phase = match.get("eventPhaseId") or {}

    home_team = teams[0] if len(teams) > 0 else {}
    away_team = teams[1] if len(teams) > 1 else {}

    connection.execute(
        """
        INSERT OR REPLACE INTO matches (
            match_id,
            match_alt_id,
            description,
            competition,
            event_phase,
            phase_type,
            phase_subtype,
            match_date,
            venue_id,
            venue_name,
            venue_city,
            venue_country,
            attendance,
            home_team_id,
            home_team_name,
            home_team_abbreviation,
            away_team_id,
            away_team_name,
            away_team_abbreviation,
            home_score,
            away_score,
            status,
            outcome
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            match.get("matchId"),
            match.get("matchAltId"),
            match.get("description"),
            match.get("competition"),
            match.get("eventPhase"),
            phase.get("type"),
            phase.get("subType"),
            (match.get("time") or {}).get("label"),
            venue.get("id"),
            venue.get("name"),
            venue.get("city"),
            venue.get("country"),
            match.get("attendance"),
            home_team.get("id"),
            home_team.get("name"),
            home_team.get("abbreviation"),
            away_team.get("id"),
            away_team.get("name"),
            away_team.get("abbreviation"),
            scores[0],
            scores[1],
            match.get("status"),
            match.get("outcome"),
        ),
    )
    return match["matchId"]


def insert_officials(connection: sqlite3.Connection, match_id: str, summary_payload: dict) -> None:
    for official_entry in summary_payload.get("officials", []):
        official = official_entry.get("official") or {}
        name = (official.get("name") or {}).get("display")
        connection.execute(
            """
            INSERT OR REPLACE INTO officials (match_id, role, official_id, official_name)
            VALUES (?, ?, ?, ?)
            """,
            (
                match_id,
                official_entry.get("position"),
                official.get("id"),
                name,
            ),
        )


def insert_lineups(connection: sqlite3.Connection, match_id: str, summary_payload: dict) -> None:
    match_teams = summary_payload.get("match", {}).get("teams", [])
    for team_index, team_block in enumerate(summary_payload.get("teams", [])):
        team_meta = match_teams[team_index] if team_index < len(match_teams) else {}
        team_id = team_meta.get("id")
        team_name = team_meta.get("name")
        for entry in team_block.get("teamList", {}).get("list", []):
            player = entry.get("player") or {}
            connection.execute(
                """
                INSERT OR REPLACE INTO lineups (
                    match_id,
                    team_id,
                    team_name,
                    player_id,
                    player_name,
                    squad_role,
                    shirt_number,
                    position_code,
                    position_label,
                    is_replacement,
                    order_index
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    match_id,
                    team_id,
                    team_name,
                    player.get("id"),
                    (player.get("name") or {}).get("display"),
                    player.get("role"),
                    entry.get("number"),
                    entry.get("position"),
                    entry.get("positionLabel"),
                    1 if player.get("isReplacement") else 0,
                    entry.get("order"),
                ),
            )


def insert_scoring_events(connection: sqlite3.Connection, match_id: str, summary_payload: dict) -> None:
    scoring_groups = ("Con", "Pen", "Try", "dropGoals")
    match_teams = summary_payload.get("match", {}).get("teams", [])
    player_lookup = player_lookup_from_summary(summary_payload)

    for team_index, team_block in enumerate(summary_payload.get("teams", [])):
        team_meta = match_teams[team_index] if team_index < len(match_teams) else {}
        team_id = team_meta.get("id")
        team_name = team_meta.get("name")
        scoring = team_block.get("scoring") or {}
        for scoring_group in scoring_groups:
            for event in scoring.get(scoring_group, []):
                connection.execute(
                    """
                    INSERT INTO scoring_events (
                        match_id,
                        team_id,
                        team_name,
                        event_family,
                        phase,
                        event_time_secs,
                        event_time_label,
                        type_code,
                        type_label,
                        points,
                        player_id,
                        player_alt_id,
                        player_name
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        match_id,
                        team_id,
                        team_name,
                        scoring_group,
                        event.get("phase"),
                        ((event.get("time") or {}).get("secs")),
                        ((event.get("time") or {}).get("label")),
                        event.get("type"),
                        event.get("typeLabel"),
                        event.get("points"),
                        event.get("playerId"),
                        event.get("playerAltId"),
                        player_lookup.get(event.get("playerId")),
                    ),
                )


def insert_timeline_events(
    connection: sqlite3.Connection,
    match_id: str,
    summary_payload: dict,
    timeline_payload: dict,
) -> None:
    match_teams = summary_payload.get("match", {}).get("teams", [])
    player_lookup = player_lookup_from_summary(summary_payload)

    for index, event in enumerate(timeline_payload.get("timeline", []), start=1):
        team_index = event.get("teamIndex")
        team_meta = match_teams[team_index] if isinstance(team_index, int) and team_index < len(match_teams) else {}
        connection.execute(
            """
            INSERT OR REPLACE INTO timeline_events (
                match_id,
                event_index,
                team_id,
                team_name,
                phase,
                event_time_secs,
                event_time_label,
                type_code,
                type_label,
                event_group,
                points,
                player_id,
                player_alt_id,
                player_name,
                link_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                match_id,
                index,
                team_meta.get("id"),
                team_meta.get("name"),
                event.get("phase"),
                ((event.get("time") or {}).get("secs")),
                ((event.get("time") or {}).get("label")),
                event.get("type"),
                event.get("typeLabel"),
                event.get("group"),
                event.get("points"),
                event.get("playerId"),
                event.get("playerAltId"),
                player_lookup.get(event.get("playerId")),
                event.get("link"),
            ),
        )


def build_database(db_path: Path, summary_payload: dict, timeline_payload: dict) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path)
    try:
        create_schema(connection)
        match_id = insert_match(connection, summary_payload)
        delete_existing_rows(connection, match_id)
        insert_officials(connection, match_id, summary_payload)
        insert_lineups(connection, match_id, summary_payload)
        insert_scoring_events(connection, match_id, summary_payload)
        insert_timeline_events(connection, match_id, summary_payload, timeline_payload)
        connection.commit()
    finally:
        connection.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch World Rugby match data and build a local SQLite proof of concept."
    )
    parser.add_argument("match_id", nargs="?", default="2183", help="World Rugby match id")
    parser.add_argument(
        "--data-root",
        default=str(DEFAULT_DATA_ROOT),
        help="Top-level data folder for raw JSON and SQLite outputs",
    )
    args = parser.parse_args()

    match_id = str(args.match_id)
    data_root = Path(args.data_root).expanduser().resolve()
    raw_dir = data_root / "raw"
    processed_dir = data_root / "processed"

    summary_url = f"{API_BASE}/{match_id}/summary"
    timeline_url = f"{API_BASE}/{match_id}/timeline"

    summary_payload = fetch_json(summary_url)
    timeline_payload = fetch_json(timeline_url)

    save_json(raw_dir / f"summary_{match_id}.json", summary_payload)
    save_json(raw_dir / f"timeline_{match_id}.json", timeline_payload)
    build_database(processed_dir / "rwc_matches.sqlite", summary_payload, timeline_payload)

    print(f"Saved raw summary to {raw_dir / f'summary_{match_id}.json'}")
    print(f"Saved raw timeline to {raw_dir / f'timeline_{match_id}.json'}")
    print(f"Updated SQLite database at {processed_dir / 'rwc_matches.sqlite'}")


if __name__ == "__main__":
    main()
