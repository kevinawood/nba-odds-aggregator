import os
import sqlite3

def test_player_game_logs_schema():
    DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "nba_stats.db"))
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Check table exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='player_game_logs'")
    assert cursor.fetchone(), "player_game_logs table not found"

    # Check critical columns exist
    cursor.execute("PRAGMA table_info(player_game_logs)")
    columns = [row[1] for row in cursor.fetchall()]
    for col in ["player_id", "game_id", "pts"]:
        assert col in columns

    conn.close()
