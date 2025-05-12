import os

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

import nba_utils

st.set_page_config(page_title="NBA Betting Dashboard", layout="wide")
st.title("🏀 NBA Player Stats Dashboard")

# Get latest CSV
DATA_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../data/player_logs")
)
files = [f for f in os.listdir(DATA_DIR) if f.endswith(".csv")]
latest_file = sorted(files)[-1]
df = pd.read_csv(os.path.join(DATA_DIR, latest_file))

st.sidebar.markdown(f"### Showing data from: `{latest_file}`")

# Filters
players = st.multiselect("Select Players", sorted(df["PLAYER_NAME"].unique()))
filtered = df[df["PLAYER_NAME"].isin(players)] if players else df

# Display Table
st.subheader("📊 Player Game Logs")
st.dataframe(
    filtered[["PLAYER_NAME", "GAME_DATE", "PTS", "REB", "AST", "MIN", "MATCHUP"]],
    height=400,
)

# Show Average Stats
st.subheader(f"📈 Averages Over Last {nba_utils.number_of_games} Games")
avg_stats = filtered.groupby("PLAYER_NAME")[["PTS", "REB", "AST"]].mean().round(2)
avg_stats = avg_stats.applymap(lambda x: f"{x:.2f}")
st.table(avg_stats)

# 📉 Player Trends
if players:
    if players:
        st.subheader("📉 Stat Trendlines (Last 5 Games + 3-Game Rolling Avg)")

        stat_options = ["PTS", "REB", "AST"]
        selected_stats = st.multiselect(
            "Choose stats to plot:", stat_options, default=stat_options
        )

        for player in players:
            player_data = filtered[filtered["PLAYER_NAME"] == player].copy()
            player_data["GAME_DATE"] = pd.to_datetime(player_data["GAME_DATE"])
            player_data = player_data.sort_values("GAME_DATE")

            st.markdown(f"### {player}")
            fig = go.Figure()

            for stat in selected_stats:
                # Raw stat line
                fig.add_trace(
                    go.Scatter(
                        x=player_data["GAME_DATE"],
                        y=player_data[stat],
                        mode="lines+markers",
                        name=f"{stat}",
                        line=dict(width=2),
                    )
                )

                # 3-game rolling average line
                fig.add_trace(
                    go.Scatter(
                        x=player_data["GAME_DATE"],
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
