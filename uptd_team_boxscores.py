# -------------------------------------------------------
# NBA POST–ALL-STAR DATA INGESTION PIPELINE
# -------------------------------------------------------
# This pipeline:
# 1. Pulls all NBA games for a given season
# 2. Filters games occurring after the All-Star break
# 3. Checks which games are already stored in the database
# 4. Fetches player and team box scores for only NEW games
# 5. Saves the results into SQLite tables
# -------------------------------------------------------

# NBA API endpoints used to pull game and boxscore data
from nba_api.stats.endpoints import leaguegamefinder
from nba_api.stats.endpoints import boxscoretraditionalv2

# Allows overriding request headers to prevent NBA API blocking
from nba_api.stats.library.http import NBAStatsHTTP

# Data processing
import pandas as pd

# SQLite database connection
import sqlite3

# Used to slow API calls to prevent rate limiting
import time


# -------------------------------------------------------
# NBA API Browser Headers
# -------------------------------------------------------
# The NBA stats API blocks many non-browser requests.
# These headers mimic a real browser request to avoid
# HTTP 403 errors or connection timeouts.
# -------------------------------------------------------
NBAStatsHTTP.headers = {
    "Host": "stats.nba.com",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X)",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.nba.com/",
}


# -------------------------------------------------------
# CONFIGURATION VARIABLES
# -------------------------------------------------------

# NBA season to pull
SEASON = "2025-26"

# Only collect games after this date (post All-Star break)
ALL_STAR_BREAK_END = pd.to_datetime("2026-03-05")

# Number of retries allowed if API requests fail
MAX_RETRIES = 3

# SQLite database file
DB_NAME = "nba_data.db"

print("Starting pipeline...")


# -------------------------------------------------------
# STEP 1: FETCH ALL GAMES FOR THE SEASON
# -------------------------------------------------------
# This uses LeagueGameFinder to retrieve all NBA games
# played during the specified season.
# -------------------------------------------------------

retries = 0
success = False

while retries < MAX_RETRIES and not success:
    try:
        print("Fetching season data...")

        games = leaguegamefinder.LeagueGameFinder(
            season_nullable=SEASON,
            league_id_nullable="00",   # 00 = NBA
            timeout=60
        )

        success = True

    except Exception as e:
        retries += 1
        print(f"Retry {retries} due to: {e}")
        time.sleep(5)

# Exit if API request completely failed
if not success:
    print("Failed to fetch season data.")
    exit()


# Convert API response into a pandas dataframe
df = games.get_data_frames()[0]


# -------------------------------------------------------
# STEP 2: CLEAN GAME DATA
# -------------------------------------------------------

# Select only columns relevant for modeling/storage
columns_needed = [
    "GAME_ID",       # unique game identifier
    "GAME_DATE",     # date game occurred
    "TEAM_ID",       # team id
    "TEAM_NAME",     # team name
    "MATCHUP",       # matchup info (home vs away)
    "WL",            # win/loss
    "PTS"            # team points
]

df_clean = df[columns_needed].copy()

# Convert date column to datetime format
df_clean["GAME_DATE"] = pd.to_datetime(df_clean["GAME_DATE"])


# -------------------------------------------------------
# STEP 3: FILTER POST–ALL-STAR GAMES
# -------------------------------------------------------

df_clean = df_clean[
    df_clean["GAME_DATE"] >= ALL_STAR_BREAK_END
]

# Stop pipeline if no games match filter
if df_clean.empty:
    print("No games found after All-Star break.")
    exit()


# Each NBA game appears twice (once per team)
# Extract unique game IDs to prevent duplicate processing
unique_game_ids = df_clean["GAME_ID"].unique()

print(f"Total post–All-Star games: {len(unique_game_ids)}")


# -------------------------------------------------------
# STEP 4: CONNECT TO DATABASE
# -------------------------------------------------------

conn = sqlite3.connect(DB_NAME)


# Pull already stored games to avoid duplicates
existing_games = pd.read_sql(
    "SELECT DISTINCT GAME_ID FROM team_games",
    conn
)["GAME_ID"].tolist()


# Determine which games still need to be fetched
game_ids_to_fetch = [
    gid for gid in unique_game_ids
    if gid not in existing_games
]

print(f"New games to fetch: {len(game_ids_to_fetch)}")


# If no new games exist, stop pipeline
if not game_ids_to_fetch:
    print("Database already up to date.")
    conn.close()
    exit()


# -------------------------------------------------------
# STEP 5: FETCH BOX SCORES
# -------------------------------------------------------

all_player_boxscores = []
all_team_boxscores = []

for game_id in game_ids_to_fetch:

    print(f"Fetching box score for {game_id}")

    retries = 0
    success = False

    while retries < MAX_RETRIES and not success:
        try:

            # Request traditional box score endpoint
            boxscore = boxscoretraditionalv2.BoxScoreTraditionalV2(
                game_id=game_id,
                timeout=60
            )

            frames = boxscore.get_data_frames()

            # Validate API response
            if len(frames) < 2:
                print(f"Incomplete data for {game_id}")
                break

            # Frame[0] = player statistics
            player_stats = frames[0]

            # Frame[1] = team statistics
            team_stats = frames[1]

            # Add GAME_ID column explicitly
            player_stats["GAME_ID"] = game_id
            team_stats["GAME_ID"] = game_id

            # Store results
            all_player_boxscores.append(player_stats)
            all_team_boxscores.append(team_stats)

            success = True

            # Sleep to prevent NBA API rate limiting
            time.sleep(3)

        except Exception as e:
            retries += 1
            print(f"Retry {retries} for {game_id} due to: {e}")
            time.sleep(5)

    if not success:
        print(f"Skipping {game_id} after {MAX_RETRIES} attempts.")


# -------------------------------------------------------
# STEP 6: SAVE DATA TO SQLITE
# -------------------------------------------------------

print("Saving to SQLite...")

# Save team-level game data
new_team_rows = df_clean[
    df_clean["GAME_ID"].isin(game_ids_to_fetch)
]

new_team_rows.to_sql(
    "team_games",
    conn,
    if_exists="append",
    index=False
)


# Save player boxscores
if all_player_boxscores:
    player_boxscore_df = pd.concat(all_player_boxscores, ignore_index=True)

    player_boxscore_df.to_sql(
        "player_boxscores",
        conn,
        if_exists="append",
        index=False
    )


# Save team boxscores
if all_team_boxscores:
    team_boxscore_df = pd.concat(all_team_boxscores, ignore_index=True)

    team_boxscore_df.to_sql(
        "team_boxscores",
        conn,
        if_exists="append",
        index=False
    )


# Commit database changes and close connection
conn.commit()
conn.close()

print("Pipeline completed successfully.")
