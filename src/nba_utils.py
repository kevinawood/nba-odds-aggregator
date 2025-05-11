from nba_api.stats.static import teams
from nba_api.stats.endpoints import leaguegamefinder, playergamelog
import pandas as pd
import time

def get_team_ids():
    return {team['full_name']: team['id'] for team in teams.get_teams()}

def get_recent_games_for_player(player_id, num_games=5, season='2025'):
    log = playergamelog.PlayerGameLog(player_id=player_id, season=season)
    df = log.get_data_frames()[0]
    return df.head(num_games)
