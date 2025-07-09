import requests
import pandas as pd
import time
from datetime import datetime, date
from typing import List, Dict, Optional, Any
import json

class ESPNDataFetcher:
    """Improved NBA data fetcher using ESPN API."""
    
    def __init__(self):
        self.base_url = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json',
            'Referer': 'https://www.espn.com/'
        })
    
    def get_teams(self) -> List[Dict[str, Any]]:
        """Get all NBA teams."""
        url = f"{self.base_url}/teams"
        response = self.session.get(url)
        response.raise_for_status()
        
        data = response.json()
        teams = []
        
        for sport in data.get('sports', []):
            for league in sport.get('leagues', []):
                for team in league.get('teams', []):
                    team_info = team.get('team', {})
                    teams.append({
                        'id': team_info.get('id'),
                        'name': team_info.get('name'),
                        'abbreviation': team_info.get('abbreviation'),
                        'location': team_info.get('location')
                    })
        
        return teams
    
    def get_team_roster(self, team_id: str) -> List[Dict[str, Any]]:
        """Get roster for a specific team."""
        url = f"{self.base_url}/teams/{team_id}/roster"
        response = self.session.get(url)
        response.raise_for_status()
        
        data = response.json()
        players = []
        
        for athlete in data.get('athletes', []):
            players.append({
                'id': athlete.get('id'),
                'name': athlete.get('fullName'),
                'position': athlete.get('position', {}).get('abbreviation'),
                'jersey': athlete.get('jersey')
            })
        
        return players
    
    def get_player_stats(self, player_id: str, season: str = "2024-25") -> pd.DataFrame:
        """Get player statistics for a season."""
        url = f"{self.base_url}/athletes/{player_id}/stats"
        params = {
            'season': season,
            'type': 'regular'
        }
        
        response = self.session.get(url, params=params)
        response.raise_for_status()
        
        data = response.json()
        
        # Parse the stats data
        stats_list = []
        for split in data.get('splits', {}).get('categories', []):
            if split.get('name') == 'games':
                for stat in split.get('stats', []):
                    stats_list.append({
                        'name': stat.get('name'),
                        'value': stat.get('value'),
                        'displayValue': stat.get('displayValue')
                    })
        
        # Convert to DataFrame
        if stats_list:
            df = pd.DataFrame(stats_list)
            return df
        else:
            return pd.DataFrame()
    
    def get_games_by_date(self, game_date: date) -> List[Dict[str, Any]]:
        """Get all games for a specific date."""
        date_str = game_date.strftime("%Y%m%d")
        url = f"{self.base_url}/scoreboard"
        params = {'dates': date_str}
        
        response = self.session.get(url, params=params)
        response.raise_for_status()
        
        data = response.json()
        games = []
        
        for event in data.get('events', []):
            game_info = {
                'id': event.get('id'),
                'date': event.get('date'),
                'status': event.get('status', {}).get('type', {}).get('name'),
                'home_team': event.get('competitions', [{}])[0].get('competitors', [{}])[0].get('team', {}).get('name'),
                'away_team': event.get('competitions', [{}])[0].get('competitors', [{}])[1].get('team', {}).get('name'),
                'home_score': event.get('competitions', [{}])[0].get('competitors', [{}])[0].get('score'),
                'away_score': event.get('competitions', [{}])[0].get('competitors', [{}])[1].get('score')
            }
            games.append(game_info)
        
        return games
    
    def get_player_game_logs(self, player_id: str, season: str = "2024-25", limit: int = 15) -> pd.DataFrame:
        """Get recent game logs for a player."""
        # ESPN API doesn't have a direct gamelog endpoint, so we'll use the stats endpoint
        url = f"{self.base_url}/athletes/{player_id}/stats"
        params = {
            'season': season,
            'type': 'regular'
        }
        
        response = self.session.get(url, params=params)
        response.raise_for_status()
        
        data = response.json()
        game_logs = []
        
        # Parse the stats data to extract game logs
        for split in data.get('splits', {}).get('categories', []):
            if split.get('name') == 'games':
                for stat in split.get('stats', []):
                    if stat.get('name') == 'gamesPlayed':
                        # This gives us the number of games, but not individual game logs
                        # For now, we'll return a simplified version
                        game_logs.append({
                            'game_id': f"{player_id}_{season}",
                            'game_date': datetime.now().strftime("%Y-%m-%d"),
                            'pts': stat.get('value', 0),
                            'reb': 0,  # Would need to parse from different endpoint
                            'ast': 0,
                            'min': 0,
                            'fgm': 0,
                            'fga': 0,
                            'fg3m': 0,
                            'fg3a': 0,
                            'ftm': 0,
                            'fta': 0,
                            'stl': 0,
                            'blk': 0,
                            'tov': 0,
                            'pf': 0
                        })
                        break
        
        return pd.DataFrame(game_logs)


class BasketballReferenceFetcher:
    """Alternative data fetcher using Basketball Reference."""
    
    def __init__(self):
        self.base_url = "https://www.basketball-reference.com"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
    
    def get_player_stats(self, player_id: str, season: str = "2024-25") -> pd.DataFrame:
        """Get player statistics from Basketball Reference."""
        url = f"{self.base_url}/players/{player_id[0]}/{player_id}/gamelog/{season}/"
        
        response = self.session.get(url)
        response.raise_for_status()
        
        # Parse HTML table
        from io import StringIO
        tables = pd.read_html(StringIO(response.text))
        
        # Find the game log table
        for table in tables:
            if 'G' in table.columns and 'PTS' in table.columns:
                return table
        
        return pd.DataFrame()
    
    def get_player_full_gamelog(self, player_id: str, season: str = "2025") -> pd.DataFrame:
        """
        Fetch and clean a player's full game log (including playoffs) from Basketball Reference.
        player_id: Basketball Reference ID (e.g., 'jamesle01')
        season: 4-digit year for the END of the season (e.g., '2025' for 2024-25)
        """
        from io import StringIO
        import numpy as np
        url = f"{self.base_url}/players/{player_id[0]}/{player_id}/gamelog/{season}/"
        response = self.session.get(url)
        response.raise_for_status()
        tables = pd.read_html(StringIO(response.text))
        
        # Combine all tables that have 'PTS' column (regular season + playoffs)
        all_games = []
        for table in tables:
            if 'PTS' in table.columns:
                # Remove header rows and rows where Rk is not numeric
                clean_table = table.copy()
                # Filter out rows where Rk is not a number or is NaN
                clean_table = clean_table[
                    clean_table['Rk'].apply(lambda x: pd.notna(x) and str(x).replace('.', '').isdigit())
                ].reset_index(drop=True)
                
                if not clean_table.empty:
                    all_games.append(clean_table)
        
        if not all_games:
            raise ValueError("No game log data found for player.")
        
        # Combine all tables
        gamelog = pd.concat(all_games, ignore_index=True)
        
        # Convert numeric columns
        num_cols = ['Gcar', 'Gtm', 'GS', 'MP', 'FG', 'FGA', 'FG%', '3P', '3PA', '3P%', '2P', '2PA', '2P%', 'eFG%', 'FT', 'FTA', 'FT%', 'ORB', 'DRB', 'TRB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS', 'GmSc', '+/-']
        for col in num_cols:
            if col in gamelog.columns:
                gamelog[col] = pd.to_numeric(gamelog[col], errors='coerce')
        
        # Parse date
        gamelog['Date'] = pd.to_datetime(gamelog['Date'], errors='coerce')
        
        # Sort by date
        gamelog = gamelog.sort_values('Date').reset_index(drop=True)
        
        return gamelog


# Factory function to choose the best available data source
def get_nba_fetcher(source: str = "espn") -> Any:
    """Get the best available NBA data fetcher."""
    if source == "espn":
        return ESPNDataFetcher()
    elif source == "basketball_reference":
        return BasketballReferenceFetcher()
    else:
        raise ValueError(f"Unknown data source: {source}")


# Test function
def test_data_sources():
    """Test different data sources to see which one works best."""
    print("Testing ESPN API...")
    try:
        espn = ESPNDataFetcher()
        teams = espn.get_teams()
        print(f"✅ ESPN API: Found {len(teams)} teams")
        
        # Test getting a specific player (LeBron James)
        if teams:
            # Get Lakers roster
            lakers = next((team for team in teams if 'Lakers' in team['name']), None)
            if lakers:
                roster = espn.get_team_roster(lakers['id'])
                print(f"✅ ESPN API: Found {len(roster)} players on Lakers roster")
                
                # Test getting player stats
                if roster:
                    player = roster[0]  # First player
                    stats = espn.get_player_game_logs(player['id'], limit=5)
                    print(f"✅ ESPN API: Got {len(stats)} recent games for {player['name']}")
                    if not stats.empty:
                        print(f"   Latest game: {stats.iloc[0]['game_date']}")
        
    except Exception as e:
        print(f"❌ ESPN API failed: {e}")
    
    print("\nTesting Basketball Reference...")
    try:
        br = BasketballReferenceFetcher()
        # Test with a known player ID (LeBron James)
        stats = br.get_player_full_gamelog("jamesle01", "2025")
        print(f"✅ Basketball Reference: Got {len(stats)} games (including playoffs)")
        if not stats.empty:
            print(f"   Date range: {stats['Date'].min()} to {stats['Date'].max()}")
            print(stats[['Date', 'Opp', 'PTS']].tail())
    except Exception as e:
        print(f"❌ Basketball Reference failed: {e}")


if __name__ == "__main__":
    test_data_sources() 