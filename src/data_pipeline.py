import datetime
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from nba_api.stats.endpoints import commonteamroster

import nba_utils
from src.logger import setup_logger
import sqlite3

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
output_dir = os.path.join(BASE_DIR, "data", "player_logs")
os.makedirs(output_dir, exist_ok=True)

logger = setup_logger(debug=True)
TEAM_ID_MAP = nba_utils.get_team_ids()

successful_players = []
failed_players = []
all_stats = []

MAX_WORKERS = 5


def fetch_player_stats(player, team_id, team_abbreviation, team_name):
    player_name = player["PLAYER"]
    player_id = player["PLAYER_ID"]
    logger.debug(f"🔍 Fetching recent games for {player_name} (ID: {player_id})")

    try:
        player_stats = nba_utils.get_recent_games_for_player(
            player_id, num_games=nba_utils.number_of_games
        )

        if player_stats.empty or player_stats["PTS"].isnull().all():
            logger.debug(f"⚠️ No valid data for {player_name} — skipping.")
            failed_players.append((player_name, player_id))
            return None

        player_stats["TEAM_ID"] = team_id
        player_stats["TEAM_ABBREVIATION"] = team_abbreviation
        player_stats["TEAM_NAME"] = team_name

        logger.debug(f"✅ Retrieved {len(player_stats)} rows for {player_name}")
        player_stats["PLAYER_NAME"] = player_name
        successful_players.append((player_name, player_id))
        return player_stats

    except Exception as e:
        logger.warning(f"❌ Failed for {player_name} (ID: {player_id}) — error: {e}")
        failed_players.append((player_name, player_id))
        return None


def pull_stats_by_date(target_date, force=False):
    logger.info(f"🚀 Starting data pull for games played on: {target_date}")

    output_path = os.path.join(output_dir, f"stats_{target_date}.csv")
    if os.path.exists(output_path) and not force:
        logger.info(f"📁 Stats already pulled for {target_date} — skipping.")
        return

    teams = nba_utils.get_team_ids_by_date(target_date)
    if not teams:
        logger.warning(f"No NBA games found for {target_date}.")
        return

    logger.info(f"📅 Found {len(teams)} teams that played on {target_date}.")

    for team_id in teams:
        try:
            roster = commonteamroster.CommonTeamRoster(
                team_id=team_id
            ).get_data_frames()[0]

            if roster.empty or "PLAYER" not in roster.columns:
                logger.error(f"❌ Roster for team ID {team_id} is empty or malformed.")
                continue

            team_name = TEAM_ID_MAP.get(team_id, "Unknown")
            team_abbreviation = nba_utils.TEAM_ABBR_MAP.get(team_id, "UNK")
            logger.info(f"📦 Fetching players from team: {team_name} (ID: {team_id})")

        except Exception as e:
            logger.error(f"❌ Failed to fetch roster for team ID {team_id}: {e}")
            continue

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [
                executor.submit(
                    fetch_player_stats, player, team_id, team_abbreviation, team_name
                )
                for _, player in roster.iterrows()
            ]
            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    all_stats.append(result)

    if all_stats:
        df_all = pd.concat(all_stats)
        df_all.columns = [col.lower() for col in df_all.columns]

        # Save CSV
        df_all.to_csv(output_path, index=False)
        logger.info(f"✅ Successfully saved CSV to: {output_path}")

        try:
            db_path = os.path.join(BASE_DIR, "nba_stats.db")
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            cursor.execute("PRAGMA synchronous = OFF;")  # Optional speed boost
            cursor.execute("PRAGMA journal_mode = WAL;")
            cursor.execute("PRAGMA foreign_keys = ON;")

            schema_path = os.path.join(BASE_DIR, "schema", "player_game_logs.sql")
            print(schema_path)
            with open(schema_path, "r") as f:
                create_table_sql = f.read()

            cursor.execute(create_table_sql)
            conn.commit()  # Force commit even if no insert

            # Normalize and validate columns
            expected_cols = [
                col[1] for col in cursor.execute("PRAGMA table_info(player_game_logs);")
            ]
            actual_cols = df_all.columns.tolist()
            df_all = df_all[[col for col in actual_cols if col in expected_cols]]
            logger.debug(f"Final cols to insert: {df_all.columns.tolist()}")

            if df_all.empty or len(df_all.columns) == 0:
                logger.error("🚨 DataFrame is empty after filtering — nothing to insert into DB.")
                return
            logger.debug(f"Filtered DataFrame shape: {df_all.shape}")

            df_all.to_sql(
                "player_game_logs",
                conn,
                if_exists="append",
                index=False,
                method="multi"
            )
            logger.info(
                f"🗃 Inserted {len(df_all)} rows into the database: nba_stats.db"
            )
            conn.commit()
            conn.close()

            if os.path.exists(db_path):
                logger.info(f"✅ DB file successfully written at: {db_path}")
            else:
                logger.error("🚨 DB file still not found — something blocked creation.")

        except Exception as e:
            logger.error(f"❌ Failed to insert into database: {e}")

        logger.info(f"✅ Pull complete. Successful players: {len(successful_players)}")
        logger.info(f"❌ Failed players: {len(failed_players)}")
        if failed_players:
            failed_names = ", ".join(
                [f"{name} (ID: {pid})" for name, pid in failed_players]
            )
            logger.warning(f"🧾 Failed player list:\n{failed_names}")
    else:
        logger.warning("📭 No valid player stats to save.")


if __name__ == "__main__":
    today = datetime.datetime.now().date()
    pull_stats_by_date(today, force=True)
