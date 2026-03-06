from nba_api.stats.endpoints import boxscoreadvancedv2,boxscoretraditionalv2,leaguegamefinder
from requests.exceptions import ReadTimeout
import pandas as pd
import sqlite3
import time

# ------------------------
# DATABASE CONNECTION
# ------------------------
conn = sqlite3.connect("nba_data.db")

# ------------------------
# LOAD EXISTING GAME IDS (avoid re-pulling)
# ------------------------
try:
    existing_games = pd.read_sql(
        "SELECT DISTINCT GAME_ID FROM `20-25_games`",
        conn
    )["GAME_ID"].astype(str).tolist()
    print(f"Loaded {len(existing_games)} existing games from DB")
except Exception:
    existing_games = []
    print("No existing table found, starting fresh")

# ------------------------
# CONSTANTS
# ------------------------
TEAM_IDS = [
    1610612747,# 1610612737, 1610612738, 1610612739, 1610612740,
   # 1610612741, 1610612742, 1610612743, 1610612744, 1610612745,
   # 1610612746, 1610612748, 1610612749, 1610612750, 1610612751,
   # 1610612752, 1610612753, 1610612754, 1610612755, 1610612756,
   # 1610612757, 1610612758, 1610612759, 1610612760, 1610612761,
   # 1610612762, 1610612763, 1610612764, 1610612765, 1610612766
]

#SEASONS = [f"{y}-{str(y+1)[2:]}" for y in range(2020, 2025)]
SEASONS = ["2024-25"]

# ------------------------
# MAIN LOOP
# ------------------------
for team_id in TEAM_IDS:
    for season in SEASONS:
        print(f"Team {team_id} | Season {season}")

        games = leaguegamefinder.LeagueGameFinder(
            team_id_nullable=team_id,
            season_nullable=season
        )

        df_games = games.get_data_frames()[0]

        for game_id in df_games["GAME_ID"]:
            game_id = str(game_id)

            # Skip if already stored
            if game_id in existing_games:
                continue

            try:
                box = boxscoreadvancedv2.BoxScoreAdvancedV2(
                    game_id=game_id,
                    timeout=60
                )

                df_adv = box.get_data_frames()[0]

                # Filter to this team
                df_team = df_adv[df_adv["TEAM_ID"] == team_id]

                if df_team.empty:
                    continue

                cols = [
                    "GAME_ID", "TEAM_ID", "TEAM_NAME", "WL",
                    "PLUS_MINUS", "ORTG", "DRTG",
                    "AST", "REB", "STL", "BLK", "TOV", "PTS"
                ]

                df_team = df_team[cols]

                df_team.to_sql(
                    "20-25_games",
                    conn,
                    if_exists="append",
                    index=False
                )

                existing_games.append(game_id)
                print(f"Saved game {game_id}")

                time.sleep(3)  # NBA API needs this

            except ReadTimeout:
                print(f"⏱ Timeout on game {game_id}, skipping")
                time.sleep(10)
                continue

            except Exception as e:
                print(f"Error on game {game_id}: {e}")
                time.sleep(5)
                continue

# ------------------------
# CLEANUP
# ------------------------
conn.close()
print("🏁 Historical advanced stats load complete")
