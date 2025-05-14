import os
import time
import sqlite3
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from nba_api.stats.static import players, teams
from nba_api.stats.endpoints import playergamelog, commonteamroster
from src.logger import setup_logger
import json

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH = os.path.join(BASE_DIR, "nba_stats.db")
SCHEMA_PATH = os.path.join(BASE_DIR, "schema", "player_game_logs.sql")

logger = setup_logger(debug=True)

MAX_WORKERS = 5
SEASON = "2024-25"

# Step 1 — build team/player lookup
def build_player_team_lookup():
    team_map = {}
    logger.info("🔎 Building player → team map")
    for team in teams.get_teams():
        team_id = team["id"]
        try:
            df = commonteamroster.CommonTeamRoster(team_id=team_id).get_data_frames()[0]
            for _, row in df.iterrows():
                team_map[row["PLAYER_ID"]] = {
                    "team_id": team_id,
                    "team_abbreviation": team["abbreviation"],
                    "team_name": team["full_name"]
                }
        except Exception as e:
            logger.warning(f"❌ Failed to load roster for {team['full_name']}: {e}")
        time.sleep(0.6)
    logger.info(f"✅ Loaded {len(team_map)} player-team entries")
    return team_map

# Step 2 — initialize table
def ensure_table_exists(conn):
    with open(SCHEMA_PATH, "r") as f:
        create_sql = f.read()
    conn.execute(create_sql)
    conn.commit()

# Step 3 — fetch existing keys
def get_existing_keys(conn):
    query = "SELECT player_id, game_id FROM player_game_logs"
    df = pd.read_sql(query, conn)
    return set(zip(df["player_id"], df["game_id"]))

# Step 4 — fetch and insert data for one player
def fetch_and_insert_player(player, conn, existing_keys, team_map):
    player_id = player["id"]
    player_name = player["full_name"]

    try:
        df = playergamelog.PlayerGameLog(player_id=player_id, season=SEASON).get_data_frames()[0]

        if df.empty:
            return f"⚠️ No data for {player_name}"

        df.columns = [col.lower() for col in df.columns]
        df["player_id"] = player_id
        df["player_name"] = player_name
        df["game_date"] = pd.to_datetime(df["game_date"]).dt.date

        # Add team info
        team_info = team_map.get(player_id, {})
        df["team_id"] = team_info.get("team_id")
        df["team_abbreviation"] = team_info.get("team_abbreviation")
        df["team_name"] = team_info.get("team_name")

        # Re-align columns
        expected_cols = [row[1] for row in conn.execute("PRAGMA table_info(player_game_logs);")]
        df = df.reindex(columns=expected_cols, fill_value=None)

        # Dedupe
        df["key"] = list(zip(df["player_id"], df["game_id"]))
        df = df[~df["key"].isin(existing_keys)]
        df.drop(columns=["key"], inplace=True)

        if not df.empty:
            df.to_sql("player_game_logs", conn, if_exists="append", index=False)
            return f"✅ Inserted {len(df)} rows for {player_name}"
        else:
            return f"🟰 No new rows for {player_name}"

    except Exception as e:
        return f"❌ Failed for {player_name} — {e}"

# Step 5 — threaded backfill runner
def threaded_backfill():
    logger.info("🚀 Starting full backfill with threading")

    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    ensure_table_exists(conn)
    existing_keys = get_existing_keys(conn)
    team_map = build_player_team_lookup()
    all_players = players.get_active_players()

    insert_results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(fetch_and_insert_player, p, conn, existing_keys, team_map)
            for p in all_players
        ]

        for f in tqdm(as_completed(futures), total=len(futures), desc="📦 Backfilling"):
            insert_results.append(f.result())

    conn.close()

    # Summary
    inserted = [r for r in insert_results if r.startswith("✅")]
    skipped = [r for r in insert_results if r.startswith("🟰")]
    errors = [r for r in insert_results if r.startswith("❌")]

    logger.info(f"🎯 Inserted rows: {len(inserted)}")
    logger.info(f"🟰 Skipped (already exists): {len(skipped)}")
    logger.info(f"🔥 Errors: {len(errors)}")

    # Write failed players to JSON for retrying later
    failed = [p for p, msg in zip(all_players, insert_results) if msg.startswith("❌")]

    with open("failed_players.json", "w") as f:
        json.dump(failed, f, indent=2)

    logger.info(f"📁 Saved {len(failed)} failed players to failed_players.json")

    for err in errors:
        logger.warning(err)

if __name__ == "__main__":
    threaded_backfill()
