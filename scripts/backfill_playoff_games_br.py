import os
import sqlite3
import pandas as pd
from src.improved_nba_fetcher import BasketballReferenceFetcher
from src.logger import setup_logger
from datetime import datetime

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH = os.path.join(BASE_DIR, "nba_stats.db")
SCHEMA_PATH = os.path.join(BASE_DIR, "schema", "player_game_logs.sql")

logger = setup_logger(debug=True)

fetcher = BasketballReferenceFetcher()

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Get all unique player_id, player_name, season_id from the DB for 2024-25
players = pd.read_sql_query("""
    SELECT DISTINCT player_id, player_name, season_id
    FROM player_game_logs
    WHERE season_id = '2025'
""", conn)

for _, row in players.iterrows():
    player_id = row['player_id']
    player_name = row['player_name']
    season = row['season_id']
    # Guess Basketball Reference ID
    parts = player_name.lower().split()
    if len(parts) < 2:
        continue
    last, first = parts[-1], parts[0]
    br_id = f"{last[:5]}{first[:2]}01"
    try:
        br_log = fetcher.get_player_full_gamelog(br_id, season)
        if br_log.empty:
            logger.info(f"No BR data for {player_name}")
            continue
        # Only keep games after the last date in the DB for this player
        last_db_date = pd.read_sql_query(f"""
            SELECT MAX(game_date) as last_date FROM player_game_logs
            WHERE player_id = ? AND season_id = ?
        """, conn, params=(player_id, season)).iloc[0]['last_date']
        if pd.isna(last_db_date):
            last_db_date = '1900-01-01'
        br_log = br_log[br_log['Date'] > pd.to_datetime(last_db_date)]
        if br_log.empty:
            logger.info(f"No new playoff games for {player_name}")
            continue
        # Add player info
        br_log['player_id'] = player_id
        br_log['player_name'] = player_name
        br_log['season_id'] = season
        # Map columns to match schema
        col_map = {
            'Date': 'game_date',
            'Team': 'team_abbreviation',
            'Opp': 'opponent',
            'MP': 'min',
            'PTS': 'pts',
            'FG': 'fgm',
            'FGA': 'fga',
            'FG%': 'fg_pct',
            '3P': 'fg3m',
            '3PA': 'fg3a',
            '3P%': 'fg3_pct',
            'FT': 'ftm',
            'FTA': 'fta',
            'FT%': 'ft_pct',
            'ORB': 'oreb',
            'DRB': 'dreb',
            'TRB': 'reb',
            'AST': 'ast',
            'STL': 'stl',
            'BLK': 'blk',
            'TOV': 'tov',
            'PF': 'pf',
            'GmSc': 'gmsc',
            '+/-': 'plus_minus',
        }
        for old, new in col_map.items():
            if old in br_log.columns:
                br_log[new] = br_log[old]
        # Add missing columns
        for col in ['team_id', 'team_name', 'game_id', 'matchup', 'wl']:
            if col not in br_log.columns:
                br_log[col] = None
        # Reorder columns to match schema
        expected_cols = [col[1] for col in cursor.execute("PRAGMA table_info(player_game_logs);")]
        br_log = br_log[[col for col in expected_cols if col in br_log.columns]]
        # Insert
        br_log.to_sql("player_game_logs", conn, if_exists="append", index=False)
        logger.info(f"Inserted {len(br_log)} new playoff games for {player_name}")
    except Exception as e:
        logger.warning(f"Failed for {player_name}: {e}")

conn.close() 