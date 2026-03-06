# This script:
# 1. Retrieves all active NBA players for the current season
# 2. Pulls their full career regular-season statistics
# 3. Adds season-based date fields for better database design
# 4. Aggregates career totals per player
# 5. Saves both detailed and aggregated tables to SQLite

from nba_api.stats.static import players  # For player lookups
from nba_api.stats.endpoints import commonallplayers, playercareerstats
import pandas as pd
import sqlite3
import time

SEASON = "2025-26"       # Current season used to filter active players
DB_NAME = "nba_data.db"  # SQLite database file

# Initialize Database: Tables, Primary Keys, Indexes
def initialize_database(conn):
    """
    Create tables for season-level stats and aggregated career totals
    with primary keys and an index for fast filtering.
    Safe to run repeatedly.
    """
    cursor = conn.cursor()

    # Season-level stats: 1 row per player per season
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS player_career_season_stats (
            PLAYER_ID INTEGER,
            SEASON_ID TEXT,
            PLAYER_NAME TEXT,
            TEAM_ID INTEGER,
            GP INTEGER,
            PTS INTEGER,
            REB INTEGER,
            AST INTEGER,
            YEAR INTEGER,
            SEASON_FORMAT TEXT,
            PRIMARY KEY (PLAYER_ID, SEASON_ID)
        );
    """)

    # Career totals: 1 row per player
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS player_career_totals (
            PLAYER_ID INTEGER PRIMARY KEY,
            PLAYER_NAME TEXT,
            GAMES_PLAYED INTEGER,
            PTS INTEGER,
            REB INTEGER,
            AST INTEGER,
            PTS_per_game REAL
        );
    """)

    # Index for faster filtering by year
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_career_year
        ON player_career_season_stats (YEAR);
    """)

    conn.commit()


# Fetch All Current NBA Players
print("Fetching list of current players...")

all_players = commonallplayers.CommonAllPlayers(
    league_id="00",  # NBA
    season=SEASON,
    is_only_current_season=1
).get_data_frames()[0]

# Rename columns for clarity
all_players = all_players.rename(columns={
    "PERSON_ID": "PLAYER_ID",
    "DISPLAY_FIRST_LAST": "PLAYER_NAME"
})

print(f"Total current players: {len(all_players)}")

# Fetch Career Stats for Each Player
career_data = []  # List to store season-level stats per player

for _, player in all_players.iterrows():
    pid = player["PLAYER_ID"]
    name = player["PLAYER_NAME"]

    print(f"Fetching career stats for {name} ({pid})")

    try:
        # Pull full career stats (season-by-season)
        career = playercareerstats.PlayerCareerStats(player_id=pid)
        df = career.get_data_frames()[0]  # first dataframe = season-level stats

        # Skip empty responses
        if df.empty:
            continue

        # Add identifiers for relational integrity
        df["PLAYER_ID"] = pid
        df["PLAYER_NAME"] = name

        career_data.append(df)

        # Pause between API calls to prevent throttling
        time.sleep(1.5)

    except Exception as e:
        print(f"Error retrieving {name}: {e}")
        continue

# Combine All Players Into One DataFrame
print("Combining all career data...")

# Remove empty DataFrames to prevent warnings
career_data = [df for df in career_data if not df.empty]

career_df = pd.concat(career_data, ignore_index=True)

# Add YEAR and SEASON_FORMAT Columns
print("Adding YEAR and SEASON_FORMAT columns...")

# Remove rows with missing SEASON_ID
career_df = career_df[career_df["SEASON_ID"].notna()]

# Extract starting year from SEASON_ID (handles formats like "22003")
career_df["YEAR"] = career_df["SEASON_ID"].astype(str).str.extract(r'(\d{4})')[0]
career_df = career_df[career_df["YEAR"].notna()]
career_df["YEAR"] = career_df["YEAR"].astype(int)

# Calculate ending year
career_df["YEAR_END"] = career_df["YEAR"] + 1

# Format season as "2003-04"
career_df["SEASON_FORMAT"] = (
    career_df["YEAR"].astype(str) + "-" + career_df["YEAR_END"].astype(str).str[-2:]
)

# Drop temporary YEAR_END column
career_df.drop(columns=["YEAR_END"], inplace=True)

# Aggregate Career Totals Per Player
print("Calculating career aggregates...")

agg = career_df.groupby("PLAYER_ID").agg({
    "PLAYER_NAME": "first",  # Keep name
    "PTS": "sum",            # Total career points
    "REB": "sum",            # Total career rebounds
    "AST": "sum",            # Total career assists
    "GP": "sum"              # Total career games played
}).reset_index()

agg = agg.rename(columns={"GP": "GAMES_PLAYED"})
agg["PTS_per_game"] = (agg["PTS"] / agg["GAMES_PLAYED"]).round(2)

# Save to SQLite Database with Duplicate Protection 
print("Saving data to SQLite...")

conn = sqlite3.connect(DB_NAME)

# Initialize tables safely
initialize_database(conn)

cursor = conn.cursor()

# Get all player IDs in current batch
player_ids = tuple(career_df["PLAYER_ID"].unique())

if player_ids:

    # DELETE existing rows for these players
    # This prevents duplicate PRIMARY KEY errors
    placeholders = ",".join(["?"] * len(player_ids))

    cursor.execute(
        f"DELETE FROM player_career_season_stats WHERE PLAYER_ID IN ({placeholders})",
        player_ids
    )

    cursor.execute(
        f"DELETE FROM player_career_totals WHERE PLAYER_ID IN ({placeholders})",
        player_ids
    )

    conn.commit()

    # Prepare season-level DataFrame to match table schema
    career_df_clean = career_df[[
        "PLAYER_ID",
        "SEASON_ID",
        "PLAYER_NAME",
        "TEAM_ID",
        "GP",
        "PTS",
        "REB",
        "AST",
        "YEAR",
        "SEASON_FORMAT"
    ]]

    # Append season-level stats
    career_df_clean.to_sql("player_career_season_stats", conn, if_exists="replace", index=False)

    # Append aggregated career totals
    agg.to_sql("player_career_totals", conn, if_exists="replace", index=False)

conn.close()

print("Database successfully updated!")
