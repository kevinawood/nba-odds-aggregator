import requests
import pandas as pd
from bs4 import BeautifulSoup, Tag, Comment
import time

SEASON = 2024  # Change to desired season end year
TEAM_LIST_URL = f"https://www.basketball-reference.com/leagues/NBA_{SEASON}.html"
BASE_URL = "https://www.basketball-reference.com"

# Get all team abbreviations for the season
resp = requests.get(TEAM_LIST_URL)
soup = BeautifulSoup(resp.text, "lxml")

# Find the 'per_game-team' table, including inside comments
def get_per_game_team_table(soup):
    table = soup.find("table", {"id": "per_game-team"})
    if table:
        return table
    # Search inside comments
    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        if not isinstance(comment, str):
            continue
        comment_soup = BeautifulSoup(comment, "lxml")
        table = comment_soup.find("table", {"id": "per_game-team"})
        if table:
            return table
    return None

team_table = get_per_game_team_table(soup)
team_abbrs = []
if isinstance(team_table, Tag):
    for row in team_table.find_all("tr"):
        if not isinstance(row, Tag):
            continue
        th = row.find("th", {"data-stat": "team_name"})
        if th and isinstance(th, Tag) and th.a and isinstance(th.a, Tag):
            href = th.a.get("href", "")
            if not isinstance(href, str):
                href = str(href)
            parts = href.split("/")
            abbr = parts[2] if len(parts) > 2 else None
            if abbr:
                team_abbrs.append(abbr)
else:
    print("Could not find per_game-team table!")
    exit(1)

print(f"Found {len(team_abbrs)} teams: {team_abbrs}")

player_set = set()
player_list = []
for abbr in team_abbrs:
    url = f"{BASE_URL}/teams/{abbr}/{SEASON}.html"
    print(f"Scraping roster: {url}")
    resp = requests.get(url)
    soup = BeautifulSoup(resp.text, "lxml")
    roster_table = soup.find("table", {"id": "roster"})
    if not isinstance(roster_table, Tag):
        print(f"No roster table for {abbr}")
        continue
    for row in roster_table.find_all("tr"):
        if not isinstance(row, Tag):
            continue
        th = row.find("th", {"data-stat": "player"})
        if th and isinstance(th, Tag) and th.a and isinstance(th.a, Tag):
            name = th.text.strip()
            href = th.a.get("href", "")
            if not isinstance(href, str):
                href = str(href)
            br_id = href.split("/")[-1].replace(".html", "") if href else None
            if br_id and br_id not in player_set:
                player_set.add(br_id)
                player_list.append({"player_id": br_id, "player_name": name})
    time.sleep(0.5)  # Be polite to the server

# Save as CSV
player_df = pd.DataFrame(player_list)
player_df.to_csv(f"br_player_list_{SEASON}.csv", index=False)

print(f"Found {len(player_list)} unique players for {SEASON}")
print(player_list[:10]) 