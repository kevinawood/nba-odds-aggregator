import os
import pandas as pd
import sqlite3
from glob import glob

DB_PATH = "nba_stats.db"
DATA_FOLDER = os.path.abspath("data/player_logs")

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Create table if not exists
cursor.executescript("""
CREATE TABLE IF NOT EXISTS player_game_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    player_id INTEGER,
    player_name TEXT,
    season_id TEXT,
    team_id INTEGER,
    team_abbreviation TEXT,
    team_name TEXT,
    game_id TEXT,
    game_date TEXT,
    matchup TEXT,
    wl TEXT,
    min INTEGER,
    pts INTEGER,
    fgm INTEGER,
    fga INTEGER,
    fg_pct REAL,
    fg3m INTEGER,
    fg3a INTEGER,
    fg3_pct REAL,
    ftm INTEGER,
    fta INTEGER,
    ft_pct REAL,
    oreb INTEGER,
    dreb INTEGER,
    reb INTEGER,
    ast INTEGER,
    stl INTEGER,
    blk INTEGER,
    tov INTEGER,
    pf INTEGER,
    plus_minus REAL
);
""")
conn.commit()

# Load all CSVs
csv_files = sorted(glob(os.path.join(DATA_FOLDER, "*.csv")))
print(f"Found {len(csv_files)} CSVs to import...")

for file in csv_files:
    df = pd.read_csv(file)

    # Normalize column names
    df.columns = [col.strip().upper() for col in df.columns]

    # Skip empty
    if df.empty:
        continue

    # Fix column names to match schema
    df = df.rename(columns={
        'PLAYER_NAME': 'player_name',
        'PLAYER_ID': 'player_id',
        'SEASON_ID': 'season_id',
        'TEAM_ID': 'team_id',
        'TEAM_ABBREVIATION': 'team_abbreviation',
        'TEAM_NAME': 'team_name',
        'GAME_ID': 'game_id',
        'GAME_DATE': 'game_date',
        'MATCHUP': 'matchup',
        'WL': 'wl',
        'MIN': 'min',
        'PTS': 'pts',
        'FGM': 'fgm',
        'FGA': 'fga',
        'FG_PCT': 'fg_pct',
        'FG3M': 'fg3m',
        'FG3A': 'fg3a',
        'FG3_PCT': 'fg3_pct',
        'FTM': 'ftm',
        'FTA': 'fta',
        'FT_PCT': 'ft_pct',
        'OREB': 'oreb',
        'DREB': 'dreb',
        'REB': 'reb',
        'AST': 'ast',
        'STL': 'stl',
        'BLK': 'blk',
        'TOV': 'tov',
        'PF': 'pf',
        'PLUS_MINUS': 'plus_minus'
    })

    expected_cols = [col[1] for col in cursor.execute("PRAGMA table_info(player_game_logs);")]
    df = df[[col for col in df.columns if col in expected_cols]]

    try:
        df.to_sql("player_game_logs", conn, if_exists="append", index=False)
    except Exception as e:
        print(f"❌ Error writing {file}: {e}")

print("✅ All data imported successfully.")

conn.close()