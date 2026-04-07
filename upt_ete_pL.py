# Core libraries
import pandas as pd
import sqlite3
import time
import random

# NBA API endpoints
from nba_api.stats.endpoints import leaguegamefinder, boxscoretraditionalv3, boxscoreadvancedv3
from nba_api.stats.library.http import NBAStatsHTTP

# CONFIGURATION
DB_NAME = "nba_data.db"  # SQLite database file

# Seasons to collect 
SEASONS = ["2021-22", "2022-23", "2023-24", "2024-25", "2025-26"]

# PREVENT API BLOCKING
def set_headers():
    """
    Sets browser-like request headers to avoid being blocked
    by stats.nba.com. Rotates User-Agent for variability.
    """
    NBAStatsHTTP.headers = {
        "Host": "stats.nba.com",
        "User-Agent": random.choice([
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
            "Mozilla/5.0 (X11; Linux x86_64)"
        ]),
        "Referer": "https://www.nba.com/",
        "Accept": "application/json, text/plain, */*"
    }

    # Reset session so new headers apply immediately
    NBAStatsHTTP._session = None

# RETRY LOGIC
def retry(func, retries=3):
    """
    Attempts to execute an API call multiple times with
    exponential backoff to handle timeouts or rate limits.
    """
    for i in range(retries):
        try:
            return func()
        except Exception as e:
            wait = 2 + i * 2  # Increasing delay: 2s, 4s, 6s
            print(f"Retry {i+1}: {e}")

            # Reset headers in case request was blocked
            set_headers()

            time.sleep(wait)

    return None  # Return None if all retries fail

# GET COMPLETED GAMES FOR A SEASON
def get_games(season):
    """
    Retrieves all completed games for a given season.
    Filters out games without results (WL is null).
    """
    finder = leaguegamefinder.LeagueGameFinder(
        season_nullable=season,
        league_id_nullable="00"
    )

    df = finder.get_data_frames()[0]

    # Keep only completed games
    df = df[df["WL"].notna()]

    # Extract unique GAME_ID + GAME_DATE
    games = df[["GAME_ID", "GAME_DATE"]].drop_duplicates()

    print(f"{len(games)} completed games found for season {season}")
    return games

# FETCH SINGLE GAME DATA
def fetch_game(game_id):
    """
    Fetches both traditional and advanced box score stats
    for a single NBA game.

    Returns:
    - DataFrame with 2 rows (home + away team)
    """
    # Random delay to reduce API rate-limit risk
    time.sleep(random.uniform(0.3, 0.8))

    try:
        # TRADITIONAL STATS
        trad = retry(lambda: boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=game_id))

        if trad is None:
            return None

        data = trad.get_dict().get("boxScoreTraditional", {})

        home = data.get("homeTeam", {})
        away = data.get("awayTeam", {})

        home_stats = home.get("statistics")
        away_stats = away.get("statistics")

        # Ensure both teams have valid stats
        if not home_stats or not away_stats:
            return None

        # Build dataframe (2 rows: home + away)
        team_df = pd.DataFrame([
            {"TEAM_ID": home.get("teamId"), "HOME": 1, **home_stats},
            {"TEAM_ID": away.get("teamId"), "HOME": 0, **away_stats}
        ])

        team_df["GAME_ID"] = game_id

        # ADVANCED STATS 
        advanced = retry(lambda: boxscoreadvancedv3.BoxScoreAdvancedV3(game_id=game_id))

        if advanced:
            adv = advanced.get_dict().get("boxScoreAdvanced", {})

            adv_df = pd.DataFrame([
                {"TEAM_ID": home.get("teamId"), **adv.get("homeTeam", {}).get("statistics", {})},
                {"TEAM_ID": away.get("teamId"), **adv.get("awayTeam", {}).get("statistics", {})}
            ])

            # Merge advanced stats into base table
            team_df = team_df.merge(
                adv_df,
                on="TEAM_ID",
                how="left",
                suffixes=("", "_adv")
            )

        return team_df

    except Exception as e:
        print(f"Failed game {game_id}: {e}")
        return None

# BUILD TEAM TABLE 
def build_team_table(season, existing_game_ids):
    """
    Builds team-level dataset for a season, but ONLY fetches
    games not already stored in the database.
    """
    set_headers()

    games = get_games(season)

    # Filter out already stored games
    new_games = games[~games["GAME_ID"].isin(existing_game_ids)]

    if new_games.empty:
        print(f"No new games to fetch for season {season}")
        return None

    rows = []

    # Fetch each new game
    for _, row in new_games.iterrows():
        df = fetch_game(row["GAME_ID"])

        if df is None:
            continue

        # Attach game date
        df["GAME_DATE"] = row["GAME_DATE"]
        rows.append(df)

    if not rows:
        return None

    # Combine into single dataframe
    team_df = pd.concat(rows, ignore_index=True)
    return team_df

# BUILD GAME-LEVEL FEATURES
def build_game_features(team_df):
    """
    Converts team-level data into matchup-level data:
    - Joins each team with its opponent
    - Creates win/loss label
    - Adds point differential
    """
    opp_df = team_df.copy()

    # Self-join to match teams within same game
    merged = team_df.merge(opp_df, on="GAME_ID", suffixes=("", "_OPP"))

    # Remove rows where team matched with itself
    merged = merged[merged["TEAM_ID"] != merged["TEAM_ID_OPP"]]

    # Target variable: WIN (1 = win, 0 = loss)
    merged["WIN"] = (merged["points"] > merged["points_OPP"]).astype(int)

    # Additional feature: scoring margin
    merged["POINT_DIFF"] = merged["points"] - merged["points_OPP"]

    return merged

# ADD ROLLING FEATURES (TEAM MOMENTUM)
def add_rolling_features(df, window=5):
    """
    Adds rolling averages for recent performance.
    Uses previous games only (shifted) to prevent data leakage.
    """
    df = df.sort_values(["TEAM_ID", "GAME_DATE"])

    stats = ["points", "reboundsTotal", "assists"]

    for stat in stats:
        df[f"{stat}_rolling"] = df.groupby("TEAM_ID")[stat].transform(
            lambda x: x.shift(1).rolling(window).mean()
        )

    # Replace NaN values from early games
    df.fillna(0, inplace=True)

    return df

# MAIN PIPELINE (INCREMENTAL MULTI-SEASON UPDATE)
if __name__ == "__main__":

    # Connect to database
    conn = sqlite3.connect(DB_NAME)

    # LOAD EXISTING GAME IDS (PREVENT DUPLICATES)
    try:
        existing_games_df = pd.read_sql("SELECT GAME_ID FROM model_dataset", conn)
        existing_game_ids = existing_games_df["GAME_ID"].tolist()
    except Exception:
        # Table doesn't exist yet
        existing_game_ids = []

    all_team_rows = []
    all_game_rows = []

    # LOOP THROUGH SEASONS
    for season in SEASONS:

        # Fetch only new games
        team_df = build_team_table(season, existing_game_ids)

        if team_df is None:
            continue

        # Build modeling dataset
        game_df = build_game_features(team_df)

        # Add rolling performance features
        game_df = add_rolling_features(game_df)

        all_team_rows.append(team_df)
        all_game_rows.append(game_df)

    # SAVE NEW DATA TO DATABASE
    if all_team_rows:

        # Combine all new rows
        team_df_full = pd.concat(all_team_rows, ignore_index=True)
        game_df_full = pd.concat(all_game_rows, ignore_index=True)

        # Append (NOT replace) to preserve historical data
        team_df_full.to_sql("team_stats", conn, if_exists="append", index=False)
        game_df_full.to_sql("model_dataset", conn, if_exists="append", index=False)

        print("Database updated")

    # Close DB connection
    conn.close()
