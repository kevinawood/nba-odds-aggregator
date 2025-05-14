import os
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import sqlite3

st.set_page_config(page_title="NBA Betting Dashboard", layout="wide")
st.title("🏀 NBA Player Stats Dashboard")

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "nba_stats.db")
conn = sqlite3.connect(DB_PATH)
df = pd.read_sql_query("SELECT * FROM player_game_logs", conn)
conn.close()
total_players = df["player_id"].nunique()

# Normalize dates
df["game_date"] = pd.to_datetime(df["game_date"])

# Sidebar controls
st.sidebar.markdown("### 🎯 Player & Chart Controls")

# --- Calculate top performers ---
recent_df = df.sort_values("game_date", ascending=False).groupby("player_name").head(5)
top_players = (
    recent_df.groupby("player_name")["pts"]
    .mean()
    .sort_values(ascending=False)
    .head(5)
    .index.tolist()
)

# --- Sidebar player selection ---
all_players = sorted(df["player_name"].dropna().unique())
selected_players = st.sidebar.multiselect(
    "👤 Search and select players",
    all_players,
    default=top_players
)

# Optional: preview who was auto-selected
st.sidebar.markdown("#### 🔝 Default: Top 5 by Points (last 5 games)")
for i, name in enumerate(top_players, 1):
    st.sidebar.markdown(f"{i}. {name}")


game_limit = st.sidebar.slider("🎮 Number of recent games to show", min_value=5, max_value=20, value=15, step=1)

st.sidebar.markdown("### 📊 Stats to Plot")

stats = ["pts", "reb", "ast"]
selected_stats = []

for stat in stats:
    col1, col2 = st.sidebar.columns([1, 1])
    with col1:
        show_raw = st.checkbox(f"{stat} (raw)", value=True, key=f"{stat}_raw")
    with col2:
        show_avg = st.checkbox(f"{stat} (avg)", value=True, key=f"{stat}_avg")

    if show_raw:
        selected_stats.append((stat, "raw"))
    if show_avg:
        selected_stats.append((stat, "avg"))

st.sidebar.markdown(f"**Players loaded:** `{len(selected_players)}`")
st.sidebar.markdown(f"### 📋 Total unique players in DB: `{total_players}`")

# Plot trendlines
if selected_players:
    st.subheader(f"📈 Stat Trendlines (Last {game_limit} Games + 3-Game Rolling Avg)")

    for player in selected_players:
        player_data = df[df["player_name"] == player].sort_values("game_date", ascending=False).head(game_limit)
        player_data = player_data.sort_values("game_date")

        if player_data.empty:
            st.write(f"⚠️ No data available for {player}")
            continue

        # Create the chart BEFORE the layout blocks
        fig = go.Figure()
        for stat, mode in selected_stats:
            if mode == "raw":
                fig.add_trace(go.Scatter(
                    x=player_data["game_date"],
                    y=player_data[stat],
                    mode="lines+markers",
                    name=stat,
                    line=dict(width=2),
                ))
            elif mode == "avg":
                fig.add_trace(go.Scatter(
                    x=player_data["game_date"],
                    y=player_data[stat].rolling(3, min_periods=1).mean(),
                    mode="lines",
                    name=f"{stat} (3-game avg)",
                    line=dict(dash="dash"),
                ))

        # Get player ID
        player_id = df[df["player_name"] == player]["player_id"].iloc[0]
        img_url = f"https://cdn.nba.com/headshots/nba/latest/1040x760/{player_id}.png"

        # Display side-by-side layout
        left, right = st.columns([1, 6])  # widen the chart column a bit
        with left:
            st.image(img_url, width=160, caption=player)  # you can bump this up if needed
        with right:
            st.markdown(f"### {player}")
            fig.update_layout(
                height=500,
                margin=dict(t=10, b=40),
                legend=dict(orientation="h"),
                xaxis_title="Game Date",
                yaxis_title="Stat Value",
                template="plotly_dark",
            )
            st.plotly_chart(fig, use_container_width=True)

