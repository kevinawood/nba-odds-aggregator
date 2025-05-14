import pandas as pd
import os
import sqlite3
# Load the same DB your dashboard uses
DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "nba_stats.db"))


def test_load_player_data():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM player_game_logs", conn)
    conn.close()

    assert not df.empty, "DataFrame is empty"
    assert df["player_id"].nunique() > 50, "Too few unique players"
    assert df["pts"].notna().all(), "Missing point values"


def test_top_5_by_points():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM player_game_logs", conn)
    conn.close()
    df["game_date"] = pd.to_datetime(df["game_date"])
    recent_df = df.sort_values("game_date", ascending=False).groupby("player_name").head(5)
    top_players = (
        recent_df.groupby("player_name")["pts"]
        .mean()
        .sort_values(ascending=False)
        .head(5)
        .index.tolist()
    )

    assert len(top_players) == 5, "Top player logic didn't return 5 players"
    assert all(isinstance(p, str) for p in top_players), "Top players not names"


def test_rolling_average_logic():
    data = {
        "game_date": pd.date_range("2024-01-01", periods=5, freq="D"),
        "pts": [10, 20, 30, 40, 50]
    }
    df = pd.DataFrame(data)
    rolled = df["pts"].rolling(3, min_periods=1).mean().round(1).tolist()

    expected = [10.0, 15.0, 20.0, 30.0, 40.0]
    assert rolled == expected, f"Rolling average mismatch: {rolled} != {expected}"
