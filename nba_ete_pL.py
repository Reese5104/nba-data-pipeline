# Core libraries
import pandas as pd
import sqlite3
import time
import random

# NBA API endpoints
from nba_api.stats.endpoints import leaguegamefinder, boxscoretraditionalv3, boxscoreadvancedv3
from nba_api.stats.library.http import NBAStatsHTTP

DB_NAME = "nba_data.db"  # SQLite database file

# SET CUSTOM HEADERS (AVOID NBA API BLOCKING)
def set_headers():
    """
    Sets browser-like headers for NBA API requests to reduce
    the chance of being blocked (common issue with stats.nba.com).
    Rotates user-agent strings for variability.
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

    # Reset session so new headers take effect
    NBAStatsHTTP._session = None

# RETRY LOGIC
def retry(func, retries=3):
    """
    Executes a function with retry logic and exponential backoff.
    Useful for handling API timeouts or temporary failures.
    """
    for i in range(retries):
        try:
            return func()
        except Exception as e:
            # Exponential backoff: 1s, 2s, 4s (+ randomness)
            wait = (2 ** i) + random.random()
            print(f"Retry {i+1}: {e} – waiting {wait:.1f}s")

            # Reset headers in case request was blocked
            set_headers()

            time.sleep(wait)

    return None  # Return None if all retries fail

# GET ALL COMPLETED GAMES FOR A SEASON
def get_games(season):
    """
    Retrieves all completed NBA games for a given season.
    Filters out:
    - Games without results (WL is null)
    - Future scheduled games
    """
    finder = leaguegamefinder.LeagueGameFinder(
        season_nullable=season,
        league_id_nullable="00"
    )

    df = finder.get_data_frames()[0]

    # Keep only completed games
    df = df[df["WL"].notna()]

    # Remove future games (safety check)
    df = df[pd.to_datetime(df["GAME_DATE"]) <= pd.Timestamp.today()]

    # Extract unique games
    games = df[["GAME_ID", "GAME_DATE"]].drop_duplicates()

    print(f"{len(games)} completed games found for season {season}")
    return games

# FETCH SINGLE GAME DATA (TEAM + ADVANCED STATS)
def fetch_game(game_id):
    """
    Fetches both traditional and advanced box score stats
    for a single NBA game.

    Returns:
    - DataFrame with one row per team (home + away)
    """
    # Small delay to avoid rate limiting
    time.sleep(random.uniform(0.3, 0.8))

    try:
        # TRADITIONAL STATS
        trad = retry(lambda: boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=game_id))

        # Validate response
        if not trad or not trad.get_dict() or "boxScoreTraditional" not in trad.get_dict():
            print(f"Game {game_id} – no traditional stats found")
            return None

        data = trad.get_dict()["boxScoreTraditional"]

        home = data.get("homeTeam")
        away = data.get("awayTeam")

        # Ensure both teams exist
        if not home or not away or not home.get("statistics") or not away.get("statistics"):
            print(f"Game {game_id} – missing home/away stats")
            return None

        # Create team-level dataframe (2 rows: home + away)
        team_df = pd.DataFrame([
            {"TEAM_ID": home.get("teamId"), "HOME": 1, **home["statistics"]},
            {"TEAM_ID": away.get("teamId"), "HOME": 0, **away["statistics"]}
        ])

        team_df["GAME_ID"] = game_id

        # ADVANCED STATS 
        advanced = retry(lambda: boxscoreadvancedv3.BoxScoreAdvancedV3(game_id=game_id))

        if advanced and advanced.get_dict() and "boxScoreAdvanced" in advanced.get_dict():
            adv = advanced.get_dict()["boxScoreAdvanced"]

            adv_df = pd.DataFrame([
                {"TEAM_ID": home.get("teamId"), **adv.get("homeTeam", {}).get("statistics", {})},
                {"TEAM_ID": away.get("teamId"), **adv.get("awayTeam", {}).get("statistics", {})}
            ])

            # Merge advanced stats onto traditional stats
            team_df = team_df.merge(
                adv_df,
                on="TEAM_ID",
                how="left",
                suffixes=("", "_adv")
            )

        print(f"Game {game_id} fetched")
        return team_df

    except Exception as e:
        print(f"Failed game {game_id}: {e}")
        return None

# BUILD TEAM-LEVEL DATA FOR ENTIRE SEASON
def build_team_table(season):
    """
    Builds a full dataset of team-level stats for a season.
    Each game contributes 2 rows (home + away team).
    """
    set_headers()

    games = get_games(season)
    rows = []
    failed_games = []

    # Loop through every game
    for i, row in games.iterrows():
        df = fetch_game(row["GAME_ID"])

        if df is None:
            failed_games.append(row["GAME_ID"])
            continue

        # Attach game date
        df["GAME_DATE"] = row["GAME_DATE"]
        rows.append(df)

    print(f"{len(failed_games)} games failed to fetch for season {season}: {failed_games}")

    if not rows:
        raise ValueError(f"No valid games fetched for season {season}")

    # Combine all games into single dataframe
    team_df = pd.concat(rows, ignore_index=True)
    return team_df

# BUILD GAME-LEVEL FEATURES (TEAM VS OPPONENT)
def build_game_features(team_df):
    """
    Converts team-level data into matchup-level data:
    - Joins each team with its opponent
    - Creates win/loss label
    - Computes point differential
    """
    opp_df = team_df.copy()

    # Self-join on GAME_ID to pair teams
    merged = team_df.merge(opp_df, on="GAME_ID", suffixes=("", "_OPP"))

    # Remove self-joins (team vs itself)
    merged = merged[merged["TEAM_ID"] != merged["TEAM_ID_OPP"]]

    # Target variable: win (1) or loss (0)
    merged["WIN"] = (merged["points"] > merged["points_OPP"]).astype(int)

    # Additional feature
    merged["POINT_DIFF"] = merged["points"] - merged["points_OPP"]

    return merged

# ADD ROLLING FEATURES (RECENT PERFORMANCE)
def add_rolling_features(df, window=5):
    """
    Adds rolling averages for key stats based on previous games.
    Helps model capture team momentum/form.
    """
    df = df.sort_values(["TEAM_ID", "GAME_DATE"])

    stats = ["points", "reboundsTotal", "assists"]

    for stat in stats:
        df[f"{stat}_rolling"] = df.groupby("TEAM_ID")[stat].transform(
            lambda x: x.shift(1).rolling(window).mean()
        )

    # Replace NaNs from early games
    df.fillna(0, inplace=True)

    return df

# MAIN PIPELINE (MULTI-SEASON COLLECTION)
if __name__ == "__main__":

    # Seasons to collect
    seasons = ["2021-22", "2022-23", "2023-24", "2024-25", "2025-26"]

    all_team_dfs = []
    all_game_dfs = []

    # Loop through each season
    for season in seasons:
        print(f"\n=== Collecting data for season {season} ===")

        # Team-level stats
        team_df = build_team_table(season)

        # Convert to game-level dataset
        game_df = build_game_features(team_df)

        # Add rolling features
        game_df = add_rolling_features(game_df)

        all_team_dfs.append(team_df)
        all_game_dfs.append(game_df)

    # COMBINE ALL SEASONS
    combined_team_df = pd.concat(all_team_dfs, ignore_index=True)
    combined_game_df = pd.concat(all_game_dfs, ignore_index=True)

    # SAVE TO DATABASE
    conn = sqlite3.connect(DB_NAME)

    # Raw team stats
    combined_team_df.to_sql("team_stats", conn, if_exists="replace", index=False)

    # Model-ready dataset
    combined_game_df.to_sql("model_dataset", conn, if_exists="replace", index=False)

    conn.close()

    print("\nAll seasons collected and saved")
