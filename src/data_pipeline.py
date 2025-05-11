from nba_api.stats.endpoints import commonteamroster, scoreboardv2
from src.nba_utils import get_recent_games_for_player

import pandas as pd
import datetime
import time
from src.logger import setup_logger
import os

logger = setup_logger()

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
output_dir = os.path.join(BASE_DIR, "data", "player_logs")
os.makedirs(output_dir, exist_ok=True)

def get_today_team_ids():
    today = datetime.datetime.today().strftime("%m/%d/%Y")
    sb = scoreboardv2.ScoreboardV2(game_date=today)
    games = sb.get_data_frames()[0]
    team_ids = set(games['HOME_TEAM_ID']).union(set(games['VISITOR_TEAM_ID']))
    return list(team_ids)


def pull_today_starters_stats():
    logger.info("🚀 Starting data pull for today's games...")

    teams_today = get_today_team_ids()
    if not teams_today:
        logger.warning("No NBA games found for today.")
        return

    logger.info(f"📅 Found {len(teams_today)} teams playing today.")
    all_stats = []

    for team_id in teams_today:
        try:
            roster = commonteamroster.CommonTeamRoster(team_id=team_id).get_data_frames()[0]
            team_name = roster['TEAM_NAME'].iloc[0] if not roster.empty else "Unknown"
            logger.info(f"📦 Fetching players from team: {team_name} (ID: {team_id})")

        except Exception as e:
            logger.error(f"❌ Failed to fetch roster for team ID {team_id}: {e}")
            continue

        for _, player in roster.iterrows():
            player_name = player['PLAYER']
            player_id = player['PLAYER_ID']
            try:
                logger.debug(f"🔍 Fetching recent games for {player_name} (ID: {player_id})")
                player_stats = get_recent_games_for_player(player_id, num_games=5)

                if player_stats.empty or player_stats['PTS'].isnull().all():
                    logger.debug(f"⚠️ No valid data for {player_name} — skipping.")
                    continue

                logger.debug(f"✅ Retrieved {len(player_stats)} rows for {player_name}")
                player_stats['PLAYER_NAME'] = player_name
                all_stats.append(player_stats)
                time.sleep(1.2)
            except Exception as e:
                logger.warning(f"⚠️ Skipping {player_name} (ID: {player_id}) due to error: {e}")
                continue

    if all_stats:
        df_all = pd.concat(all_stats)
        df_all.dropna(how="all", inplace=True)

        path = os.path.join(output_dir, f"stats_{datetime.datetime.now().date()}.csv")
        df_all.to_csv(path, index=False)
        logger.info(f"✅ Pulled data for {len(all_stats)} players.")
        logger.info(f"📁 Data saved to: {path}")
    else:
        logger.warning("📭 No valid player stats pulled.")

if __name__ == "__main__":
    pull_today_starters_stats()
