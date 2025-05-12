from nba_api.stats.static import teams
from nba_api.stats.endpoints import playergamelog, scoreboardv2
from nba_api.stats.library.http import NBAStatsHTTP
import time
from datetime import datetime

def get_team_ids():
    return {team['id']: team['full_name'] for team in teams.get_teams()}

def get_team_ids_by_date(date: datetime.date):
    game_date = date.strftime("%m/%d/%Y")
    sb = scoreboardv2.ScoreboardV2(game_date=game_date)
    games = sb.get_data_frames()[0]
    team_ids = set(games['HOME_TEAM_ID']).union(set(games['VISITOR_TEAM_ID']))
    return list(team_ids)


def get_recent_games_for_player(player_id, num_games=5, season=None):
    if season is None:
        season = get_current_season_string()
    try:
        # Monkeypatch headers to avoid detection
        NBAStatsHTTP._HEADERS = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/117.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Referer': 'https://www.nba.com/',
            'Host': 'stats.nba.com',
            'Origin': 'https://www.nba.com',
            'x-nba-stats-origin': 'stats',
            'x-nba-stats-token': 'true'
        }

        log = playergamelog.PlayerGameLog(player_id=player_id, season=season, timeout=15)
        df = log.get_data_frames()[0]
        time.sleep(2)
        return df.head(num_games)

    except Exception as e:
        raise RuntimeError(f"Failed to get data for player {player_id}: {e}")


def get_current_season_string():
    today = datetime.today()
    year = today.year

    if today.month >= 10:
        return f"{year}-{str(year + 1)[-2:]}"  # e.g. '2024-25'
    else:
        return f"{year - 1}-{str(year)[-2:]}"  # e.g. '2023-24' for Jan–Sep
