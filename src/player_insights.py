import pandas as pd


def calculate_player_insights(df, game_limit=5):
    df = df.copy()
    df["game_date"] = pd.to_datetime(df["game_date"])

    insights = []

    for player_id, group in df.groupby("player_id"):
        group = group.sort_values("game_date")

        # Season average
        season_avg = group[["pts", "reb", "ast"]].mean()

        # Rolling avg over last N games
        recent = group.tail(game_limit)
        recent_avg = recent[["pts", "reb", "ast"]].mean()

        # % Delta vs season
        deltas = ((recent_avg - season_avg) / season_avg * 100).round(2)

        insights.append({
            "player_id": player_id,
            "player_name": group["player_name"].iloc[0],
            "games_played": len(group),
            "avg_pts_recent": recent_avg["pts"],
            "avg_pts_season": season_avg["pts"],
            "pts_delta_pct": deltas["pts"],
            "avg_reb_recent": recent_avg["reb"],
            "avg_reb_season": season_avg["reb"],
            "reb_delta_pct": deltas["reb"],
            "avg_ast_recent": recent_avg["ast"],
            "avg_ast_season": season_avg["ast"],
            "ast_delta_pct": deltas["ast"],
        })

    return pd.DataFrame(insights)
