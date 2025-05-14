import sqlite3
import os

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH = os.path.join(BASE_DIR, "nba_stats.db")

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

print("📌 Adding indexes...")

cursor.execute("CREATE INDEX IF NOT EXISTS idx_game_date ON player_game_logs (game_date);")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_player_id ON player_game_logs (player_id);")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_player_game ON player_game_logs (player_id, game_id);")

conn.commit()
conn.close()

print("✅ Indexes created successfully.")
