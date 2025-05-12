import datetime
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from nba_api.stats.endpoints import commonteamroster

import nba_utils
from src.logger import setup_logger

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
output_dir = os.path.join(BASE_DIR, "data", "player_logs")
os.makedirs(output_dir, exist_ok=True)

logger = setup_logger(debug=True)
TEAM_ID_MAP = nba_utils.get_team_ids()

successful_players = []
failed_players = []
all_stats = []

MAX_WORKERS = 5

def fetch_player_stats(player):
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

        logger.debug(f"✅ Retrieved {len(player_stats)} rows for {player_name}")
        player_stats["PLAYER_NAME"] = player_name
        successful_players.append((player_name, player_id))
        return player_stats

    except Exception as e:
        logger.warning(f"❌ Failed for {player_name} (ID: {player_id}) — error: {e}")
        failed_players.append((player_name, player_id))
        return None

def pull_stats_by_date(target_date):
    logger.info(f"🚀 Starting data pull for games played on: {target_date}")

    output_path = os.path.join(output_dir, f"stats_{target_date}.csv")
    if os.path.exists(output_path):
        logger.info(f"📁 Stats already pulled for {target_date} — skipping.")
        return

    teams = nba_utils.get_team_ids_by_date(target_date)
    if not teams:
        logger.warning(f"No NBA games found for {target_date}.")
        return

    logger.info(f"📅 Found {len(teams)} teams that played on {target_date}.")

    for team_id in teams:
        try:
            roster = commonteamroster.CommonTeamRoster(team_id=team_id).get_data_frames()[0]
            team_name = TEAM_ID_MAP.get(team_id, "Unknown")
            logger.info(f"📦 Fetching players from team: {team_name} (ID: {team_id})")
        except Exception as e:
            logger.error(f"❌ Failed to fetch roster for team ID {team_id}: {e}")
            continue

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(fetch_player_stats, player) for _, player in roster.iterrows()]
            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    all_stats.append(result)

    if all_stats:
        df_all = pd.concat(all_stats)
        df_all.to_csv(output_path, index=False)
        logger.info(f"✅ Successfully saved CSV to: {output_path}")
    else:
        logger.warning("📭 No valid player stats to save.")

    # Final summary
    logger.info(f"✅ Pull complete. Successful players: {len(successful_players)}")
    logger.info(f"❌ Failed players: {len(failed_players)}")

    if failed_players:
        failed_names = ", ".join([f"{name} (ID: {pid})" for name, pid in failed_players])
        logger.warning(f"🧾 Failed player list:\n{failed_names}")

if __name__ == "__main__":
    today = datetime.datetime.now().date()
    pull_stats_by_date(today)