import streamlit as st
import pandas as pd
from datetime import datetime

# Load today's player logs
df = pd.read_csv(f"data/player_logs/stats_{datetime.now().date()}.csv")

st.title("NBA Prop Betting Dashboard")

# Filters
players = st.multiselect("Select Players", sorted(df['PLAYER_NAME'].unique()))
filtered = df[df['PLAYER_NAME'].isin(players)] if players else df

# Basic Stats Display
st.write("Last 5 Game Stats")
st.dataframe(
    filtered[["PLAYER_NAME", "GAME_DATE", "PTS", "REB", "AST", "MIN", "MATCHUP"]]
)

# Add visual trendlines
st.line_chart(filtered.groupby("GAME_DATE")[["PTS", "REB", "AST"]].mean())
