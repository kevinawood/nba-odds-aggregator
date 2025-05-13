import os

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

import nba_utils
import sqlite3

st.set_page_config(page_title="NBA Betting Dashboard", layout="wide")
st.title("🏀 NBA Player Stats Dashboard")

# Load from DB
DB_PATH = os.path.join(os.path.dirname(__file__), "..", "nba_stats.db")

# Connect and query
conn = sqlite3.connect(DB_PATH)
df = pd.read_sql_query("SELECT * FROM player_game_logs", conn)
conn.close()

# Normalize date just in case
df["game_date"] = pd.to_datetime(df["game_date"]).dt.date

# Sidebar filter
dates = sorted(df["game_date"].dropna().unique(), reverse=True)
selected_date = st.sidebar.selectbox("📅 Select game date:", dates)

# Filter to selected date
df = df[df["game_date"] == selected_date]

# Player filter
all_players = sorted(df["player_name"].dropna().unique())
selected_players = st.sidebar.multiselect("👤 Select Players", all_players, default=all_players)

# Filter DataFrame
filtered = df[df["player_name"].isin(selected_players)]

# Show sidebar info
st.sidebar.markdown(f"**Game date selected:** `{selected_date}`")
st.sidebar.markdown(f"**Players loaded:** `{len(selected_players)}`")

# Display Table
st.subheader("📊 Player Game Logs")
st.dataframe(
    filtered[["player_name", "game_date", "pts", "reb", "ast", "min", "matchup"]],
    height=400,
)

# Show Average Stats
st.subheader(f"📈 Averages Over Last {nba_utils.number_of_games} Games")
avg_stats = filtered.groupby("player_name")[["pts", "reb", "ast"]].mean().round(2)
avg_stats = avg_stats.map(lambda x: f"{x:.2f}")
st.table(avg_stats)

# 📉 Player Trends
if selected_players:
    st.subheader("📉 Stat Trendlines (Last 5 Games + 3-Game Rolling Avg)")

    stat_options = ["pts", "reb", "ast"]
    selected_stats = st.multiselect(
        "Choose stats to plot:", stat_options, default=stat_options
    )

    for player in selected_players:
        player_data = filtered[filtered["player_name"] == player].copy()
        player_data["game_date"] = pd.to_datetime(player_data["game_date"])
        player_data = player_data.sort_values("game_date")

        st.markdown(f"### {player}")
        fig = go.Figure()

        for stat in selected_stats:
            # Raw stat line
            fig.add_trace(
                go.Scatter(
                    x=player_data["game_date"],
                    y=player_data[stat],
                    mode="lines+markers",
                    name=f"{stat}",
                    line=dict(width=2),
                )
            )

            # 3-game rolling average line
            fig.add_trace(
                go.Scatter(
                    x=player_data["game_date"],
                    y=player_data[stat].rolling(window=3, min_periods=1).mean(),
                    mode="lines",
                    name=f"{stat} (3-game avg)",
                    line=dict(dash="dash"),
                )
            )

        fig.update_layout(
            height=500,
            margin=dict(t=10, b=40),
            legend=dict(orientation="h"),
            xaxis_title="Game Date",
            yaxis_title="Stat Value",
            template="plotly_dark",
        )
        st.plotly_chart(fig, use_container_width=True)