from nba_api.stats.endpoints import leaguegamefinder
import pandas as pd
import sqlite3
import time

TEAM_IDS = [
    1610612747, 1610612737, 1610612738, 1610612739, 1610612740,
    1610612741, 1610612742, 1610612743, 1610612744, 1610612745,
    1610612746, 1610612748, 1610612749, 1610612750, 1610612751,
    1610612752, 1610612753, 1610612754, 1610612755, 1610612756,
    1610612757, 1610612758, 1610612759, 1610612760, 1610612761,
    1610612762, 1610612763, 1610612764, 1610612765, 1610612766
]

SEASON = "2022-23"

all_games = []

for team_id in TEAM_IDS:
    print(f"Fetching team {team_id}")

    games = leaguegamefinder.LeagueGameFinder(
        team_id_nullable=team_id,
        season_nullable=SEASON
    )

    df = games.get_data_frames()[0]

    columns_needed = [
        "GAME_ID", "GAME_DATE", "TEAM_ID",
        "TEAM_NAME", "MATCHUP", "WL", "PTS"
    ]

    df_clean = df[columns_needed].copy()
    df_clean["GAME_DATE"] = pd.to_datetime(df_clean["GAME_DATE"])

    all_games.append(df_clean)

    time.sleep(1.5)  # rate-limit safety

final_df = pd.concat(all_games, ignore_index=True)

conn = sqlite3.connect("nba_data.db")
final_df.to_sql("team_games", conn, if_exists="append", index=False)
conn.close()

print(f"Saved {len(final_df)} rows to sqlite3")
