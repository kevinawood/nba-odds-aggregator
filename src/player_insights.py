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

def calculate_prop_hit_rates(df, game_limits=[5, 10, 15]):
    props = {
        "pts": 15.5,
        "reb": 6.5,
        "ast": 4.5,
    }

    df = df.copy()
    df["game_date"] = pd.to_datetime(df["game_date"])

    all_data = []

    for player_id, group in df.groupby("player_id"):
        group = group.sort_values("game_date", ascending=False)
        row = {
            "player_id": player_id,
            "player_name": group["player_name"].iloc[0],
        }

        for stat, line in props.items():
            for window in game_limits:
                recent = group.head(window)
                if recent.empty:
                    continue

                hit_count = (recent[stat] > line).sum()
                total = len(recent)
                pct = round((hit_count / total) * 100, 1) if total else 0

                label = f"{stat}_L{window}"
                row[label] = f"{hit_count}/{total} ({pct}%)"

        all_data.append(row)

    return pd.DataFrame(all_data)

def generate_prop_summary_table(df, props={"pts": 15.5, "reb": 6.5, "ast": 4.5}, windows=[5, 10, 15]):
    df = df.copy()
    df["game_date"] = pd.to_datetime(df["game_date"])
    results = []

    for player_id, group in df.groupby("player_id"):
        group = group.sort_values("game_date", ascending=False)
        player_name = group["player_name"].iloc[0]

        for stat, line in props.items():
            row = {
                "player": player_name,
                "prop": f"{stat.upper()} ({line})"
            }

            hit_sequence = []

            for window in windows:
                recent = group.head(window)
                if recent.empty:
                    row[f"L{window}"] = "N/A"
                    continue

                hit_count = (recent[stat] > line).sum()
                pct = round((hit_count / len(recent)) * 100)
                emoji = "✅" if pct >= 70 else "⚠️" if pct >= 40 else "❌"
                row[f"L{window}"] = f"{hit_count}/{len(recent)} {emoji}"
                hit_sequence.extend(recent[stat] > line)

            # Trend summary
            row["Trend"] = "🔥" if all(hit_sequence[:5]) else "↘️" if hit_sequence[:5].count(True) == 0 else "↔️"

            results.append(row)

    return pd.DataFrame(results)
