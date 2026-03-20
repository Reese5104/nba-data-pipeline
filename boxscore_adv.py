
# Import NBA API endpoints for game and box score data
from nba_api.stats.endpoints import (
    leaguegamefinder,          # Gets list of games
    boxscoretraditionalv3,     # Traditional box score stats
    boxscoreadvancedv3         # Advanced box score stats
)

# Low-level HTTP handler used by nba_api
from nba_api.stats.library.http import NBAStatsHTTP

# Standard libraries
import pandas as pd           # Data manipulation
import sqlite3                # Database storage
import time                   # Sleep/delay
import random                 # Random user agents / delays


# SQLite database name
DB_NAME = "nba_data.db"


# List of user agents to rotate (helps avoid blocking)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Mozilla/5.0 (X11; Linux x86_64)"
]


# Sets HTTP headers to mimic a real browser request
# Also resets session to avoid stale connections
def set_headers():

    NBAStatsHTTP.headers = {
        "Host": "stats.nba.com",
        "User-Agent": random.choice(USER_AGENTS),  # rotate agent
        "Referer": "https://www.nba.com/",
        "Accept": "application/json, text/plain, */*"
    }

    NBAStatsHTTP._session = None  # reset session


# Retries API calls with exponential backoff if they fail
def retry(func, retries=5):

    for attempt in range(retries):

        try:
            return func()

        except Exception as e:

            # Wait longer after each failed attempt
            wait = 4 + attempt * 4 + random.uniform(1,3)

            print(f"Retry {attempt+1}/{retries}:", e)

            set_headers()  # refresh headers

            time.sleep(wait)

    return None  # failed after retries


# Converts nested dictionary columns into flat columns
# Example: {"a":1,"b":2} -> col_a, col_b
def flatten_dict_columns(df):

    for col in df.columns:

        # Check if column contains dictionaries
        if df[col].apply(lambda x: isinstance(x, dict)).any():

            # Expand dictionaries into separate columns
            expanded = pd.json_normalize(df[col])

            # Rename new columns with prefix
            expanded.columns = [f"{col}_{c}" for c in expanded.columns]

            # Replace original column with expanded columns
            df = pd.concat([df.drop(columns=[col]), expanded], axis=1)

    return df


# Fetch all game IDs for a given NBA season
def get_season_games(season):

    print("Fetching season games")

    finder = leaguegamefinder.LeagueGameFinder(
        season_nullable=season,
        league_id_nullable="00"  # NBA league ID
    )

    df = finder.get_data_frames()[0]

    # Extract unique game IDs
    games = df["GAME_ID"].unique().tolist()

    print(len(games), "games found")

    return games


# Pulls both traditional and advanced stats for one game
def fetch_game(game_id):

    # Random delay to avoid rate limiting
    time.sleep(random.uniform(1,2))

    try:

        # TRADITIONAL STATS
        traditional = retry(lambda:
            boxscoretraditionalv3.BoxScoreTraditionalV3(
                game_id=game_id,
                timeout=120
            )
        )

        if traditional is None:
            return None

        trad = traditional.get_dict()

        # Extract player data
        home_players = trad["boxScoreTraditional"]["homeTeam"]["players"]
        away_players = trad["boxScoreTraditional"]["awayTeam"]["players"]

        player_df = pd.DataFrame(home_players + away_players)

        # Extract team stats
        home_team = trad["boxScoreTraditional"]["homeTeam"]["statistics"]
        away_team = trad["boxScoreTraditional"]["awayTeam"]["statistics"]

        team_df = pd.DataFrame([home_team, away_team])


        # ADVANCED STATS
        advanced = retry(lambda:
            boxscoreadvancedv3.BoxScoreAdvancedV3(
                game_id=game_id,
                timeout=120
            )
        )

        if advanced:

            adv = advanced.get_dict()

            home_adv = adv["boxScoreAdvanced"]["homeTeam"]["players"]
            away_adv = adv["boxScoreAdvanced"]["awayTeam"]["players"]

            adv_player = pd.DataFrame(home_adv + away_adv)

            # Remove redundant name columns
            adv_player = adv_player.drop(
                columns=["firstName","familyName"],
                errors="ignore"
            )

            # Merge advanced stats with traditional stats
            player_df = player_df.merge(
                adv_player,
                on="personId",
                how="left"
            )


        # Rename columns for consistency
        player_df.rename(columns={
            "personId":"PLAYER_ID",
            "teamId":"TEAM_ID"
        }, inplace=True)

        team_df.rename(columns={
            "teamId":"TEAM_ID"
        }, inplace=True)

        # Add game ID to both tables
        player_df["GAME_ID"] = game_id
        team_df["GAME_ID"] = game_id

        # Flatten nested dictionary columns
        player_df = flatten_dict_columns(player_df)
        team_df = flatten_dict_columns(team_df)

        return player_df, team_df


    except Exception as e:

        print("Failed game", game_id, e)

        return None


# Runs full pipeline for a season
def run_pipeline(season):

    set_headers()

    # Connect to SQLite database
    conn = sqlite3.connect(DB_NAME)

    # Get all game IDs
    games = get_season_games(season)

    player_frames = []
    team_frames = []

    failed_games = []

    # Loop through all games
    for i, game_id in enumerate(games):

        print(f"Game {i+1}/{len(games)}:", game_id)

        # Reset session every 50 games (helps prevent blocking)
        if i % 50 == 0:
            set_headers()
            print("Session reset")

        result = fetch_game(game_id)

        if result is None:
            failed_games.append(game_id)
            continue

        # Store results in memory
        player_frames.append(result[0])
        team_frames.append(result[1])


    # Combine all dataframes
    player_df = pd.concat(player_frames, ignore_index=True)
    team_df = pd.concat(team_frames, ignore_index=True)


    # Save to SQLite database
    player_df.to_sql("player_boxscores", conn, if_exists="append", index=False)

    team_df.to_sql("team_boxscores", conn, if_exists="append", index=False)

    conn.close()

    print("Pipeline complete")
    print("Failed games:", failed_games)


# Entry point: runs pipeline for selected season
if __name__ == "__main__":

    run_pipeline("2025-26")
