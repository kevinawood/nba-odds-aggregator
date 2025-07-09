import os
import sqlite3
import pandas as pd
from src.improved_nba_fetcher import BasketballReferenceFetcher
from src.logger import setup_logger

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH = os.path.join(BASE_DIR, "nba_stats.db")
SCHEMA_PATH = os.path.join(BASE_DIR, "schema", "player_game_logs.sql")

logger = setup_logger(debug=True)

class BRDataPipeline:
    def __init__(self, db_path=DB_PATH, schema_path=SCHEMA_PATH):
        self.db_path = db_path
        self.schema_path = schema_path
        self.fetcher = BasketballReferenceFetcher()

    def setup_database(self):
        conn = sqlite3.connect(self.db_path)
        with open(self.schema_path, "r") as f:
            create_table_sql = f.read()
        conn.execute(create_table_sql)
        conn.commit()
        return conn

    def insert_gamelog(self, df: pd.DataFrame, conn: sqlite3.Connection):
        if df.empty:
            logger.warning("No data to insert.")
            return 0
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
            if old in df.columns:
                df[new] = df[old]
        # Add missing columns
        for col in ['player_id', 'player_name', 'season_id', 'team_id', 'team_name', 'game_id', 'matchup', 'wl']:
            if col not in df.columns:
                df[col] = None
        # Reorder columns to match schema
        cursor = conn.cursor()
        expected_cols = [col[1] for col in cursor.execute("PRAGMA table_info(player_game_logs);")]
        df = df[[col for col in expected_cols if col in df.columns]]
        # Insert
        df.to_sql("player_game_logs", conn, if_exists="append", index=False)
        logger.info(f"Inserted {len(df)} rows.")
        return len(df)

    def fetch_and_store_player(self, player_id: str, player_name: str, season: str = "2025"):
        logger.info(f"Fetching game log for {player_name} ({player_id}) season {season}")
        df = self.fetcher.get_player_full_gamelog(player_id, season)
        if df.empty:
            logger.warning(f"No data for {player_name} ({player_id})")
            return 0
        # Add player info
        df['player_id'] = player_id
        df['player_name'] = player_name
        df['season_id'] = season
        # Save
        conn = self.setup_database()
        count = self.insert_gamelog(df, conn)
        conn.close()
        return count

if __name__ == "__main__":
    pipeline = BRDataPipeline()
    # Example: LeBron James, 2025 season
    pipeline.fetch_and_store_player("jamesle01", "LeBron James", "2025") 