import datetime
import os
import time

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


def pull_stats_by_date(target_date):
    logger.info(f"🚀 Starting data pull for games played on: {target_date}")

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
            team_name = TEAM_ID_MAP.get(team_id, "Unknown")
            logger.info(f"📦 Fetching players from team: {team_name} (ID: {team_id})")

        except Exception as e:
            logger.error(f"❌ Failed to fetch roster for team ID {team_id}: {e}")
            continue

        for _, player in roster.iterrows():
            player_name = player["PLAYER"]
            player_id = player["PLAYER_ID"]
            logger.debug(
                f"🔍 Fetching recent games for {player_name} (ID: {player_id})"
            )

            try:
                player_stats = nba_utils.get_recent_games_for_player(
                    player_id, num_games=nba_utils.number_of_games
                )

                if player_stats.empty or player_stats["PTS"].isnull().all():
                    logger.debug(f"⚠️ No valid data for {player_name} — skipping.")
                    failed_players.append((player_name, player_id))
                    continue

                logger.debug(f"✅ Retrieved {len(player_stats)} rows for {player_name}")
                player_stats["PLAYER_NAME"] = player_name
                all_stats.append(player_stats)
                successful_players.append((player_name, player_id))
                time.sleep(2)

            except Exception as e:
                logger.warning(
                    f"❌ Failed for {player_name} (ID: {player_id}) — error: {e}"
                )
                failed_players.append((player_name, player_id))
                continue

    if all_stats:
        df_all = pd.concat(all_stats)
        output_path = os.path.join(output_dir, f"stats_{target_date}.csv")
        df_all.to_csv(output_path, index=False)
        logger.info(f"✅ Successfully saved CSV to: {output_path}")
    else:
        logger.warning("📭 No valid player stats to save.")

    # Final summary
    logger.info(f"✅ Pull complete. Successful players: {len(successful_players)}")
    logger.info(f"❌ Failed players: {len(failed_players)}")

    if failed_players:
        failed_names = ", ".join(
            [f"{name} (ID: {pid})" for name, pid in failed_players]
        )
        logger.warning(f"🧾 Failed player list:\n{failed_names}")


if __name__ == "__main__":
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    pull_stats_by_date(yesterday.date())
