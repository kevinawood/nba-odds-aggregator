import os
import time
import json
import sqlite3
import pandas as pd
from tqdm import tqdm
from nba_api.stats.endpoints import playergamelog
from src.logger import setup_logger

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH = os.path.join(BASE_DIR, "nba_stats.db")
SCHEMA_PATH = os.path.join(BASE_DIR, "schema", "player_game_logs.sql")
FAILED_FILE = os.path.join(BASE_DIR, "failed_players.json")
SEASON = "2024-25"
BATCH_PAUSE = 10  # seconds to pause between batches
BATCH_SIZE = 20   # how many players to hit before pausing

logger = setup_logger(debug=True)

def ensure_table_exists(conn):
    with open(SCHEMA_PATH, "r") as f:
        create_sql = f.read()
    conn.execute(create_sql)
    conn.commit()

def get_existing_keys(conn):
    query = "SELECT player_id, game_id FROM player_game_logs"
    df = pd.read_sql(query, conn)
    return set(zip(df["player_id"], df["game_id"]))

def retry_with_backoff(fn, retries=3, base_delay=1.0):
    for i in range(retries):
        try:
            return fn()
        except Exception as e:
            wait = base_delay * (2 ** i) + 0.3
            logger.warning(f"⏳ Retry {i+1}/{retries} after error: {e}. Waiting {wait:.2f}s...")
            time.sleep(wait)
    raise Exception("All retries failed.")

def fetch_and_insert(conn, player, existing_keys):
    player_id = player["id"]
    player_name = player["full_name"]

    try:
        df = retry_with_backoff(lambda: playergamelog.PlayerGameLog(player_id=player_id, season=SEASON).get_data_frames()[0])
        if df.empty:
            return f"⚠️ No data for {player_name}", False

        df.columns = [col.lower() for col in df.columns]
        df["player_id"] = player_id
        df["player_name"] = player_name
        df["game_date"] = pd.to_datetime(df["game_date"]).dt.date

        # Remove existing keys
        df["key"] = list(zip(df["player_id"], df["game_id"]))
        df = df[~df["key"].isin(existing_keys)]
        df.drop(columns=["key"], inplace=True)

        if not df.empty:
            df.to_sql("player_game_logs", conn, if_exists="append", index=False)
            return f"✅ Inserted {len(df)} rows for {player_name}", True
        else:
            return f"🟰 No new rows for {player_name}", True

    except Exception as e:
        return f"❌ Failed for {player_name}: {e}", False

if __name__ == "__main__":
    logger.info("🔁 Starting retry backfill for failed players")

    if not os.path.exists(FAILED_FILE):
        logger.error("❌ No failed_players.json file found. Nothing to retry.")
        exit(1)

    with open(FAILED_FILE, "r") as f:
        retry_players = json.load(f)

    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    ensure_table_exists(conn)
    existing_keys = get_existing_keys(conn)

    still_failed = []
    total_success = 0

    for i, player in enumerate(tqdm(retry_players, desc="♻️ Retrying")):
        msg, success = fetch_and_insert(conn, player, existing_keys)
        logger.info(msg)

        if not success:
            still_failed.append(player)
        else:
            total_success += 1

        # Optional pause between batches
        if (i + 1) % BATCH_SIZE == 0:
            logger.info(f"🛑 Pausing for {BATCH_PAUSE}s to avoid rate-limiting...")
            time.sleep(BATCH_PAUSE)

    conn.close()

    logger.info(f"🎉 Retry complete. Total successful: {total_success}, Remaining failures: {len(still_failed)}")

    with open(FAILED_FILE, "w") as f:
        json.dump(still_failed, f, indent=2)

    logger.info(f"📁 Updated {FAILED_FILE} with remaining failures.")
