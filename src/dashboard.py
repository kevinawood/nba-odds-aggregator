import streamlit as st
import pandas as pd
import os
from datetime import datetime

st.set_page_config(page_title="NBA Betting Dashboard", layout="wide")
st.title("🏀 NBA Player Stats Dashboard")

# Get latest CSV
DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data/player_logs"))
files = [f for f in os.listdir(DATA_DIR) if f.endswith(".csv")]
latest_file = sorted(files)[-1]
df = pd.read_csv(os.path.join(DATA_DIR, latest_file))

st.sidebar.markdown(f"### Showing data from: `{latest_file}`")

# Filters
players = st.multiselect("Select Players", sorted(df['PLAYER_NAME'].unique()))
filtered = df[df['PLAYER_NAME'].isin(players)] if players else df

# Display Table
st.subheader("📊 Player Game Logs")
st.dataframe(filtered[['PLAYER_NAME', 'GAME_DATE', 'PTS', 'REB', 'AST', 'MIN', 'MATCHUP']])

# Show Average Stats
st.subheader("📈 Averages Over Last 5 Games")
avg_stats = filtered.groupby('PLAYER_NAME')[['PTS', 'REB', 'AST']].mean().round(2)
avg_stats = avg_stats.applymap(lambda x: f"{x:.2f}")
st.table(avg_stats)
