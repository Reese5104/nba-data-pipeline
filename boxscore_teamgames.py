# NBA API endpoints for pulling game and box score data
from nba_api.stats.endpoints import leaguegamefinder
from nba_api.stats.endpoints import boxscoretraditionalv2

# Data handling
import pandas as pd

# Database storage
import sqlite3

# Used to slow down API requests to prevent rate limiting
import time

# NBA season to collect
SEASON = "2025-26"

# Number of retry attempts per game if request fails
MAX_RETRIES = 3

# Official NBA team IDs for all 30 teams
TEAM_IDS = [
    1610612747, 1610612737, 1610612738, 1610612739, 1610612740,
    1610612741, 1610612742, 1610612743, 1610612744, 1610612745,
    1610612746, 1610612748, 1610612749, 1610612750, 1610612751,
    1610612752, 1610612753, 1610612754, 1610612755, 1610612756,
    1610612757, 1610612758, 1610612759, 1610612760, 1610612761,
    1610612762, 1610612763, 1610612764, 1610612765, 1610612766
]


# This will store all team-level game records
all_games = []

print("Fetching team game data...")

# Loop through each NBA team
for team_id in TEAM_IDS:
    print(f"Fetching team {team_id}")

    # Pull all games for this team in the specified season
    games = leaguegamefinder.LeagueGameFinder(
        team_id_nullable=team_id,
        season_nullable=SEASON
    )

    # Convert API response into a pandas DataFrame
    df = games.get_data_frames()[0]

    # Keep only relevant columns for modeling and storage
    columns_needed = [
        "GAME_ID",       # Unique game identifier
        "GAME_DATE",     # Date of game
        "TEAM_ID",       # Team identifier
        "TEAM_NAME",     # Team name
        "MATCHUP",       # Home/Away matchup info
        "WL",            # Win/Loss result
        "PTS"            # Points scored
    ]

    # Create a cleaned copy
    df_clean = df[columns_needed].copy()

    # Convert game date to datetime format
    df_clean["GAME_DATE"] = pd.to_datetime(df_clean["GAME_DATE"])

    # Store cleaned data
    all_games.append(df_clean)

    # Sleep to avoid hitting NBA API rate limits
    time.sleep(1.5)


# Combine all team data into one DataFrame
final_df = pd.concat(all_games, ignore_index=True)

# Each game appears twice (once per team),
# so extract unique game IDs to avoid duplicate box score pulls
unique_game_ids = final_df["GAME_ID"].unique()

print(f"Total unique games: {len(unique_game_ids)}")


# Store detailed player and team box scores
all_player_boxscores = []
all_team_boxscores = []

print("Fetching box scores...")

# Loop through each unique game
for game_id in unique_game_ids:

    print(f"Fetching box score for {game_id}")

    retries = 0
    success = False

    # Retry loop in case of network/API failure
    while retries < MAX_RETRIES and not success:
        try:
            # Request traditional box score data
            boxscore = boxscoretraditionalv2.BoxScoreTraditionalV2(
                game_id=game_id
            )

            # Get returned DataFrames
            frames = boxscore.get_data_frames()

            # Validate response structure
            # Frame[0] = player stats
            # Frame[1] = team stats
            if len(frames) < 2:
                print(f"Incomplete data for {game_id}")
                break

            player_stats = frames[0]
            team_stats = frames[1]

            # Add GAME_ID explicitly for relational joins
            player_stats["GAME_ID"] = game_id
            team_stats["GAME_ID"] = game_id

            # Store results
            all_player_boxscores.append(player_stats)
            all_team_boxscores.append(team_stats)

            success = True

            # Pause to avoid API rate limiting
            time.sleep(2.5)

        except Exception as e:
            # If request fails, increment retry counter
            retries += 1
            print(f"Retry {retries} for {game_id} due to: {e}")

            # Longer pause before retrying
            time.sleep(5)

    # If all retries fail, skip this game
    if not success:
        print(f"Skipping {game_id} after {MAX_RETRIES} failed attempts.")


print("Saving to SQLite...")

# Combine all player and team box scores
player_boxscore_df = pd.concat(all_player_boxscores, ignore_index=True)
team_boxscore_df = pd.concat(all_team_boxscores, ignore_index=True)

# Connect (or create) SQLite database file
conn = sqlite3.connect("nba_data.db")

# Save base team game data
final_df.to_sql("team_games", conn, if_exists="append", index=False)

# Save player-level box score data
player_boxscore_df.to_sql("player_boxscores", conn, if_exists="append", index=False)

# Save team-level box score data
team_boxscore_df.to_sql("team_boxscores", conn, if_exists="append", index=False)

# Close database connection
conn.close()

print("All data saved successfully.")
