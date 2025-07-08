import datetime
import os
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import List, Optional, Tuple, Dict, Any

import pandas as pd
from nba_api.stats.endpoints import commonteamroster

from src import nba_utils
from src.logger import setup_logger

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
OUTPUT_DIR = os.path.join(BASE_DIR, "data", "player_logs")
DB_PATH = os.path.join(BASE_DIR, "nba_stats.db")
SCHEMA_PATH = os.path.join(BASE_DIR, "schema", "player_game_logs.sql")

os.makedirs(OUTPUT_DIR, exist_ok=True)

logger = setup_logger(debug=True)
TEAM_ID_MAP = nba_utils.get_team_ids()

MAX_WORKERS = 5


@dataclass
class ProcessingResult:
    """Container for processing results and statistics."""
    successful_players: List[Tuple[str, int]]
    failed_players: List[Tuple[str, int]]
    all_stats: List[pd.DataFrame]
    total_rows_processed: int
    total_rows_inserted: int


class DatabaseManager:
    """Handles database operations for the data pipeline."""
    
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
    
    def setup_database(self) -> sqlite3.Connection:
        """Initialize database connection and create table if needed."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Performance optimizations
        cursor.execute("PRAGMA synchronous = OFF;")
        cursor.execute("PRAGMA journal_mode = WAL;")
        cursor.execute("PRAGMA foreign_keys = ON;")
        
        # Create table if it doesn't exist
        with open(SCHEMA_PATH, "r") as f:
            create_table_sql = f.read()
        cursor.execute(create_table_sql)
        conn.commit()
        
        return conn
    
    def get_existing_keys(self, conn: sqlite3.Connection) -> set:
        """Get existing player_id, game_id combinations to prevent duplicates."""
        existing_keys = pd.read_sql_query(
            "SELECT player_id, game_id FROM player_game_logs", conn
        )
        return set(zip(existing_keys["player_id"], existing_keys["game_id"]))
    
    def insert_data(self, df: pd.DataFrame, conn: sqlite3.Connection) -> int:
        """Insert data into database, returning number of rows inserted."""
        if df.empty:
            return 0
        
        # Get expected columns from database schema
        expected_cols = [
            col[1] for col in conn.cursor().execute("PRAGMA table_info(player_game_logs);")
        ]
        
        # Filter DataFrame to only include expected columns
        actual_cols = df.columns.tolist()
        filtered_df = df[[col for col in actual_cols if col in expected_cols]]
        
        if filtered_df.empty:
            logger.error("DataFrame is empty after filtering columns")
            return 0
        
        # Remove duplicates
        existing_keys = self.get_existing_keys(conn)
        before_count = len(filtered_df)
        
        # Create key tuples for deduplication
        key_tuples = list(zip(filtered_df["player_id"], filtered_df["game_id"]))
        mask = ~pd.Series(key_tuples).isin(existing_keys)
        filtered_df = filtered_df[mask]
        
        after_count = len(filtered_df)
        logger.info(f"Removed {before_count - after_count} duplicate rows")
        
        if not filtered_df.empty:
            filtered_df.to_sql("player_game_logs", conn, if_exists="append", index=False)
            conn.commit()
            return len(filtered_df)
        
        return 0


class DataProcessor:
    """Handles data processing and transformation."""
    
    @staticmethod
    def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """Normalize DataFrame column names and structure."""
        if df.empty:
            return df
        
        # Convert column names to lowercase
        df.columns = [col.lower() for col in df.columns]
        return df
    
    @staticmethod
    def validate_player_stats(player_stats: pd.DataFrame, player_name: str) -> bool:
        """Validate that player stats contain meaningful data."""
        if player_stats.empty:
            logger.debug(f"No data for {player_name}")
            return False
        
        if "PTS" not in player_stats.columns:
            logger.debug(f"No PTS column for {player_name}")
            return False
        
        if bool(player_stats["PTS"].isnull().all()):
            logger.debug(f"No valid scoring data for {player_name}")
            return False
        
        return True


class PlayerStatsFetcher:
    """Handles fetching player statistics."""
    
    def __init__(self):
        self.successful_players: List[Tuple[str, int]] = []
        self.failed_players: List[Tuple[str, int]] = []
    
    def fetch_player_stats(
        self, 
        player: Dict[str, Any], 
        team_id: int, 
        team_abbreviation: str, 
        team_name: str
    ) -> Optional[pd.DataFrame]:
        """Fetch statistics for a single player."""
        player_name = player["PLAYER"]
        player_id = player["PLAYER_ID"]
        
        logger.debug(f"Fetching recent games for {player_name} (ID: {player_id})")
        
        try:
            player_stats = nba_utils.get_recent_games_for_player(
                player_id, num_games=nba_utils.number_of_games
            )
            
            if not DataProcessor.validate_player_stats(player_stats, player_name):
                self.failed_players.append((player_name, player_id))
                return None
            
            # Add team information
            player_stats["TEAM_ID"] = team_id
            player_stats["TEAM_ABBREVIATION"] = team_abbreviation
            player_stats["TEAM_NAME"] = team_name
            player_stats["PLAYER_NAME"] = player_name
            
            logger.debug(f"Retrieved {len(player_stats)} rows for {player_name}")
            self.successful_players.append((player_name, player_id))
            return player_stats
            
        except Exception as e:
            logger.warning(f"Failed for {player_name} (ID: {player_id}) — error: {e}")
            self.failed_players.append((player_name, player_id))
            return None


class TeamRosterFetcher:
    """Handles fetching team rosters."""
    
    @staticmethod
    def fetch_team_roster(team_id: int) -> Optional[pd.DataFrame]:
        """Fetch roster for a specific team."""
        try:
            roster = commonteamroster.CommonTeamRoster(team_id=team_id).get_data_frames()[0]
            
            if roster.empty or "PLAYER" not in roster.columns:
                logger.error(f"Roster for team ID {team_id} is empty or malformed")
                return None
            
            return roster
            
        except Exception as e:
            logger.error(f"Failed to fetch roster for team ID {team_id}: {e}")
            return None


class DataPipeline:
    """Main data pipeline orchestrator."""
    
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.stats_fetcher = PlayerStatsFetcher()
        self.roster_fetcher = TeamRosterFetcher()
    
    def process_team_players(
        self, 
        team_id: int, 
        team_abbreviation: str, 
        team_name: str
    ) -> List[pd.DataFrame]:
        """Process all players for a given team."""
        roster = self.roster_fetcher.fetch_team_roster(team_id)
        if roster is None:
            return []
        
        logger.info(f"Fetching players from team: {team_name} (ID: {team_id})")
        
        all_stats = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [
                executor.submit(
                    self.stats_fetcher.fetch_player_stats,
                    player.to_dict(), 
                    team_id, 
                    team_abbreviation, 
                    team_name
                )
                for _, player in roster.iterrows()
            ]
            
            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    all_stats.append(result)
        
        return all_stats
    
    def save_to_csv(self, df: pd.DataFrame, target_date: datetime.date) -> str:
        """Save DataFrame to CSV file."""
        output_path = os.path.join(OUTPUT_DIR, f"stats_{target_date}.csv")
        df.to_csv(output_path, index=False)
        logger.info(f"Successfully saved CSV to: {output_path}")
        return output_path
    
    def pull_stats_by_date(self, target_date: datetime.date, force: bool = False) -> ProcessingResult:
        """Main method to pull and process statistics for a given date."""
        logger.info(f"Starting data pull for games played on: {target_date}")
        
        # Check if data already exists
        output_path = os.path.join(OUTPUT_DIR, f"stats_{target_date}.csv")
        if os.path.exists(output_path) and not force:
            logger.info(f"Stats already pulled for {target_date} — skipping")
            return ProcessingResult([], [], [], 0, 0)
        
        # Get teams that played on the target date
        teams = nba_utils.get_team_ids_by_date(target_date)
        if not teams:
            logger.warning(f"No NBA games found for {target_date}")
            return ProcessingResult([], [], [], 0, 0)
        
        logger.info(f"Found {len(teams)} teams that played on {target_date}")
        
        # Process all teams
        all_stats = []
        for team_id in teams:
            team_name = TEAM_ID_MAP.get(team_id, "Unknown")
            team_abbreviation = nba_utils.TEAM_ABBR_MAP.get(team_id, "UNK")
            
            team_stats = self.process_team_players(team_id, team_abbreviation, team_name)
            all_stats.extend(team_stats)
        
        # Process results
        if all_stats:
            df_all = pd.concat(all_stats, ignore_index=True)
            df_all = DataProcessor.normalize_dataframe(df_all)
            
            # Save to CSV
            self.save_to_csv(df_all, target_date)
            
            # Save to database
            rows_inserted = self._save_to_database(df_all)
            
            return ProcessingResult(
                successful_players=self.stats_fetcher.successful_players,
                failed_players=self.stats_fetcher.failed_players,
                all_stats=all_stats,
                total_rows_processed=len(df_all),
                total_rows_inserted=rows_inserted
            )
        else:
            logger.warning("No valid player stats to save")
            return ProcessingResult([], [], [], 0, 0)
    
    def _save_to_database(self, df: pd.DataFrame) -> int:
        """Save DataFrame to database."""
        try:
            conn = self.db_manager.setup_database()
            rows_inserted = self.db_manager.insert_data(df, conn)
            conn.close()
            
            logger.info(f"Inserted {rows_inserted} new rows into the database")
            return rows_inserted
            
        except Exception as e:
            logger.error(f"Failed to insert into database: {e}")
            return 0


def main():
    """Main entry point."""
    pipeline = DataPipeline()
    today = datetime.datetime.now().date() - datetime.timedelta(days=1)
    
    result = pipeline.pull_stats_by_date(today, force=True)
    
    # Log summary
    logger.info(f"Pull complete. Successful players: {len(result.successful_players)}")
    logger.info(f"Failed players: {len(result.failed_players)}")
    
    if result.failed_players:
        failed_names = ", ".join([f"{name} (ID: {pid})" for name, pid in result.failed_players])
        logger.warning(f"Failed player list:\n{failed_names}")


if __name__ == "__main__":
    main()
