import pandas as pd
from nba_api.stats.endpoints import playergamelog

def test_single_player_pull():
    df = playergamelog.PlayerGameLog(player_id=2544, season="2024-25").get_data_frames()[0]  # LeBron
    assert not df.empty, "Player log is empty"
    assert "PTS" in df.columns, "PTS column missing"
