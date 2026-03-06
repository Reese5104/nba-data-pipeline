# NBA API endpoints for pulling scoreboard and boxscore data
from nba_api.stats.endpoints import scoreboardv2, boxscoretraditionalv2

# Allows us to override request headers (prevents API blocking/timeouts)
from nba_api.stats.library.http import NBAStatsHTTP

# Standard libraries
from datetime import datetime
import sqlite3
import pandas as pd
import time


# -------------------------------------------------------------------
# API REQUEST HEADERS
# -------------------------------------------------------------------
# The NBA stats API blocks requests that don't look like a browser.
# These headers mimic a real browser so the API accepts our request.
NBAStatsHTTP.headers = {
    "Host": "stats.nba.com",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X)",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.nba.com/",
}


# -------------------------------------------------------------------
# DATABASE NAME
# -------------------------------------------------------------------
DB_NAME = "nba_data.db"


# -------------------------------------------------------------------
# CREATE LIVE GAMES TABLE
# -------------------------------------------------------------------
# This table stores the current state of games today.
# It updates continuously as the pipeline runs.
def initialize_live_table(conn):

    conn.execute("""
        CREATE TABLE IF NOT EXISTS live_games (
            GAME_ID TEXT PRIMARY KEY,
            GAME_DATE TEXT,
            GAME_STATUS_TEXT TEXT,
            HOME_TEAM_ID INTEGER,
            HOME_TEAM_NAME TEXT,
            VISITOR_TEAM_ID INTEGER,
            VISITOR_TEAM_NAME TEXT,
            HOME_TEAM_SCORE INTEGER,
            VISITOR_TEAM_SCORE INTEGER,
            LAST_UPDATED TEXT
        );
    """)

    conn.commit()


# -------------------------------------------------------------------
# FETCH SCOREBOARD (WITH RETRIES)
# -------------------------------------------------------------------
# Sometimes the NBA API fails temporarily.
# This function retries the request up to 3 times.
def fetch_scoreboard(today):

    for attempt in range(3):

        try:
            return scoreboardv2.ScoreboardV2(
                game_date=today,
                league_id="00",   # 00 = NBA
                timeout=30
            )

        except Exception:

            print(f"Scoreboard retry {attempt + 1}")
            time.sleep(5)

    print("Scoreboard failed.")
    return None


# -------------------------------------------------------------------
# SAVE FINAL GAME DATA
# -------------------------------------------------------------------
# Once a game is finished we store the full boxscore
# into permanent tables:
#
# team_games
# player_boxscore
# team_boxscore
#
def save_final_game(game_id, conn):

    print(f"Saving FINAL game {game_id}")

    # ---------------------------------------------------------------
    # FETCH BOXSCORE WITH RETRIES
    # ---------------------------------------------------------------
    # Sometimes a game is marked "Final" but the boxscore
    # endpoint is not populated yet.
    for attempt in range(3):

        try:

            box = boxscoretraditionalv2.BoxScoreTraditionalV2(
                game_id=game_id,
                timeout=30
            ).get_data_frames()

            player_stats = box[0].copy()
            team_stats = box[1].copy()

            # If team stats are empty, wait and retry
            if team_stats.empty or len(team_stats) < 2:

                print(f"Boxscore not ready for {game_id}, retrying...")
                time.sleep(5)
                continue

            break

        except Exception:

            print(f"Boxscore retry {attempt+1}")
            time.sleep(5)

    else:

        print(f"Skipping game {game_id} (boxscore unavailable)")
        return


    # ---------------------------------------------------------------
    # ADD GAME_ID COLUMN
    # ---------------------------------------------------------------
    player_stats["GAME_ID"] = game_id
    team_stats["GAME_ID"] = game_id


    # ---------------------------------------------------------------
    # TEAM_GAMES TABLE
    # ---------------------------------------------------------------
    home = team_stats.iloc[0]
    away = team_stats.iloc[1]

    team_games = pd.DataFrame([{

        "GAME_ID": game_id,
        "GAME_DATE": home["GAME_DATE_EST"],
        "GAME_STATUS_TEXT": "Final",
        "HOME_TEAM_ID": home["TEAM_ID"],
        "VISITOR_TEAM_ID": away["TEAM_ID"],
        "HOME_TEAM_SCORE": int(home["PTS"]),
        "VISITOR_TEAM_SCORE": int(away["PTS"])

    }])


    # Remove old row if exists
    conn.execute("DELETE FROM team_games WHERE GAME_ID = ?", (game_id,))

    team_game.to_sql(
        "team_games",
        conn,
        if_exists="append",
        index=False
    )


    # ---------------------------------------------------------------
    # PLAYER BOXSCORE TABLE
    # ---------------------------------------------------------------
    player_cols = [

        "GAME_ID",
        "PLAYER_ID",
        "TEAM_ID",
        "PLAYER_NAME",
        "MIN",
        "PTS",
        "REB",
        "AST",
        "STL",
        "BLK",
        "TOV"
    ]

    conn.execute(
        "DELETE FROM player_boxscore WHERE GAME_ID = ?",
        (game_id,)
    )

    player_stats[player_cols].to_sql(
        "player_boxscores",
        conn,
        if_exists="append",
        index=False
    )


    # ---------------------------------------------------------------
    # TEAM BOXSCORE TABLE
    # ---------------------------------------------------------------
    team_cols = [

        "GAME_ID",
        "TEAM_ID",
        "TEAM_NAME",
        "PTS",
        "REB",
        "AST",
        "TOV"
    ]

    conn.execute(
        "DELETE FROM team_boxscore WHERE GAME_ID = ?",
        (game_id,)
    )

    team_stats[team_cols].to_sql(
        "team_boxscores",
        conn,
        if_exists="append",
        index=False
    )

    conn.commit()


# -------------------------------------------------------------------
# MAIN PIPELINE
# -------------------------------------------------------------------
def run_pipeline():

    # Format today's date for NBA API
    today = datetime.today().strftime("%m/%d/%Y")

    # Timestamp for tracking updates
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print(f"Fetching games for {today}")

    # Fetch scoreboard
    scoreboard = fetch_scoreboard(today)

    if scoreboard is None:
        return


    # ---------------------------------------------------------------
    # EXTRACT DATAFRAMES
    # ---------------------------------------------------------------
    games_df = scoreboard.get_data_frames()[0]
    linescore_df = scoreboard.get_data_frames()[1]

    if games_df.empty:

        print("No games today.")
        return


    live_rows = []


    # ---------------------------------------------------------------
    # PROCESS EACH GAME
    # ---------------------------------------------------------------
    for _, game in games_df.iterrows():

        game_id = game["GAME_ID"]
        status = game["GAME_STATUS_TEXT"]

        home_id = game["HOME_TEAM_ID"]
        away_id = game["VISITOR_TEAM_ID"]

        # Match team rows from linescore table
        home = linescore_df[linescore_df["TEAM_ID"] == home_id].iloc[0]
        away = linescore_df[linescore_df["TEAM_ID"] == away_id].iloc[0]


        # Handle missing scores safely
        home_score = int(home["PTS"]) if pd.notna(home["PTS"]) else 0
        away_score = int(away["PTS"]) if pd.notna(away["PTS"]) else 0


        live_rows.append({

            "GAME_ID": game_id,
            "GAME_DATE": today,
            "GAME_STATUS_TEXT": status,

            "HOME_TEAM_ID": home_id,
            "HOME_TEAM_NAME": home["TEAM_NAME"],

            "VISITOR_TEAM_ID": away_id,
            "VISITOR_TEAM_NAME": away["TEAM_NAME"],

            "HOME_TEAM_SCORE": home_score,
            "VISITOR_TEAM_SCORE": away_score,

            "LAST_UPDATED": timestamp

        })


    live_df = pd.DataFrame(live_rows)


    # ---------------------------------------------------------------
    # DATABASE CONNECTION
    # ---------------------------------------------------------------
    conn = sqlite3.connect(DB_NAME)

    initialize_live_table(conn)


    # ---------------------------------------------------------------
    # UPDATE LIVE GAMES TABLE
    # ---------------------------------------------------------------
    for game_id in live_df["GAME_ID"]:

        conn.execute(
            "DELETE FROM live_games WHERE GAME_ID = ?",
            (game_id,)
        )

    conn.commit()

    live_df.to_sql(
        "live_games",
        conn,
        if_exists="append",
        index=False
    )


    # ---------------------------------------------------------------
    # PROCESS FINAL GAMES
    # ---------------------------------------------------------------
    finals = live_df[
        live_df["GAME_STATUS_TEXT"].str.contains("Final")
    ]

    for game_id in finals["GAME_ID"]:

        save_final_game(game_id, conn)

        # Sleep prevents API rate limits
        time.sleep(1.5)


    conn.close()

    print("Pipeline complete.")


# -------------------------------------------------------------------
# SCRIPT ENTRY POINT
# -------------------------------------------------------------------
# This ensures the pipeline only runs when the file
# is executed directly.
if __name__ == "__main__":

    run_pipeline()
