# ==========================================
# NBA POST–ALL-STAR DATA PIPELINE (STABLE)
# ==========================================

from nba_api.stats.endpoints import leaguegamefinder
from nba_api.stats.endpoints import boxscoretraditionalv2
from nba_api.stats.library.http import NBAStatsHTTP

import pandas as pd
import sqlite3
import time

# ------------------------------------------
# Fix 1: Browser Headers (Prevents Blocking)
# ------------------------------------------
NBAStatsHTTP.headers = {
    "Host": "stats.nba.com",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X)",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.nba.com/",
}

# ------------------------------------------
# CONFIG
# ------------------------------------------
SEASON = "2025-26"
ALL_STAR_BREAK_END = pd.to_datetime("2026-03-05")
MAX_RETRIES = 3
DB_NAME = "nba_data.db"

print("Starting pipeline...")

# ==========================================
# STEP 1: PULL FULL SEASON ONCE (NOT 30x)
# ==========================================

retries = 0
success = False

while retries < MAX_RETRIES and not success:
    try:
        print("Fetching season data...")
        games = leaguegamefinder.LeagueGameFinder(
            season_nullable=SEASON,
            league_id_nullable="00",
            timeout=60
        )
        success = True
    except Exception as e:
        retries += 1
        print(f"Retry {retries} due to: {e}")
        time.sleep(5)

if not success:
    print("Failed to fetch season data.")
    exit()

df = games.get_data_frames()[0]

columns_needed = [
    "GAME_ID",
    "GAME_DATE",
    "TEAM_ID",
    "TEAM_NAME",
    "MATCHUP",
    "WL",
    "PTS"
]

df_clean = df[columns_needed].copy()
df_clean["GAME_DATE"] = pd.to_datetime(df_clean["GAME_DATE"])

# ------------------------------------------
# Filter Post–All-Star Games
# ------------------------------------------
df_clean = df_clean[
    df_clean["GAME_DATE"] >= ALL_STAR_BREAK_END
]

if df_clean.empty:
    print("No games found after All-Star break.")
    exit()

# Remove duplicate games (each appears twice)
unique_game_ids = df_clean["GAME_ID"].unique()

print(f"Total post–All-Star games: {len(unique_game_ids)}")

# ==========================================
# STEP 2: CONNECT TO DATABASE
# ==========================================

conn = sqlite3.connect(DB_NAME)

# Get already stored game IDs to prevent duplicates
existing_games = pd.read_sql(
    "SELECT DISTINCT GAME_ID FROM team_games",
    conn
)["GAME_ID"].tolist()

# Only fetch new games
game_ids_to_fetch = [
    gid for gid in unique_game_ids
    if gid not in existing_games
]

print(f"New games to fetch: {len(game_ids_to_fetch)}")

if not game_ids_to_fetch:
    print("Database already up to date.")
    conn.close()
    exit()

# ==========================================
# STEP 3: FETCH BOX SCORES
# ==========================================

all_player_boxscores = []
all_team_boxscores = []

for game_id in game_ids_to_fetch:

    print(f"Fetching box score for {game_id}")

    retries = 0
    success = False

    while retries < MAX_RETRIES and not success:
        try:
            boxscore = boxscoretraditionalv2.BoxScoreTraditionalV2(
                game_id=game_id,
                timeout=60
            )

            frames = boxscore.get_data_frames()

            if len(frames) < 2:
                print(f"Incomplete data for {game_id}")
                break

            player_stats = frames[0]
            team_stats = frames[1]

            player_stats["GAME_ID"] = game_id
            team_stats["GAME_ID"] = game_id

            all_player_boxscores.append(player_stats)
            all_team_boxscores.append(team_stats)

            success = True
            time.sleep(3)  # safer rate limiting

        except Exception as e:
            retries += 1
            print(f"Retry {retries} for {game_id} due to: {e}")
            time.sleep(5)

    if not success:
        print(f"Skipping {game_id} after {MAX_RETRIES} attempts.")

# ==========================================
# STEP 4: SAVE TO DATABASE
# ==========================================

print("Saving to SQLite...")

# Filter team rows to only new games
new_team_rows = df_clean[
    df_clean["GAME_ID"].isin(game_ids_to_fetch)
]

new_team_rows.to_sql(
    "team_games",
    conn,
    if_exists="append",
    index=False
)

if all_player_boxscores:
    player_boxscore_df = pd.concat(all_player_boxscores, ignore_index=True)
    player_boxscore_df.to_sql(
        "player_boxscores",
        conn,
        if_exists="append",
        index=False
    )

if all_team_boxscores:
    team_boxscore_df = pd.concat(all_team_boxscores, ignore_index=True)
    team_boxscore_df.to_sql(
        "team_boxscores",
        conn,
        if_exists="append",
        index=False
    )

conn.commit()
conn.close()

print("Pipeline completed successfully.")
