import time
from datetime import datetime

from nba_api.stats.endpoints import playergamelog, scoreboardv2
from nba_api.stats.library.http import NBAStatsHTTP
from nba_api.stats.static import teams

number_of_games = 15

TEAM_ABBR_MAP = {
    1610612737: "ATL",
    1610612738: "BOS",
    1610612751: "BKN",
    1610612766: "CHA",
    1610612741: "CHI",
    1610612739: "CLE",
    1610612742: "DAL",
    1610612743: "DEN",
    1610612765: "DET",
    1610612744: "GSW",
    1610612745: "HOU",
    1610612754: "IND",
    1610612746: "LAC",
    1610612747: "LAL",
    1610612763: "MEM",
    1610612748: "MIA",
    1610612749: "MIL",
    1610612750: "MIN",
    1610612740: "NOP",
    1610612752: "NYK",
    1610612760: "OKC",
    1610612753: "ORL",
    1610612755: "PHI",
    1610612756: "PHX",
    1610612757: "POR",
    1610612758: "SAC",
    1610612759: "SAS",
    1610612761: "TOR",
    1610612741: "UTA",
    1610612764: "WAS",
}


def get_team_ids():
    return {team["id"]: team["full_name"] for team in teams.get_teams()}


def get_team_ids_by_date(date: datetime.date):
    game_date = date.strftime("%m/%d/%Y")
    sb = scoreboardv2.ScoreboardV2(game_date=game_date)
    games = sb.get_data_frames()[0]
    team_ids = set(games["HOME_TEAM_ID"]).union(set(games["VISITOR_TEAM_ID"]))
    return list(team_ids)


def get_recent_games_for_player(player_id, num_games=number_of_games, season=None):
    if season is None:
        season = get_current_season_string()
    try:
        # Monkeypatch headers to avoid detection
        NBAStatsHTTP._HEADERS = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/117.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://www.nba.com/",
            "Host": "stats.nba.com",
            "Origin": "https://www.nba.com",
            "x-nba-stats-origin": "stats",
            "x-nba-stats-token": "true",
        }

        log = playergamelog.PlayerGameLog(
            player_id=player_id, season=season, timeout=15
        )
        df = log.get_data_frames()[0]
        time.sleep(3)
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
