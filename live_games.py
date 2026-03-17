from nba_api.stats.endpoints import scoreboardv3
from nba_api.stats.library.http import NBAStatsHTTP
from datetime import datetime
import sqlite3                  
import pandas as pd             
import time                     



# These headers imitate a normal browser request.
# Without them, the NBA API frequently blocks scripts.
NBAStatsHTTP.headers = {

    # Required API host
    "Host": "stats.nba.com",

    # Pretend the request is coming from a web browser
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X)",

    # Accept JSON responses
    "Accept": "application/json, text/plain, */*",

    # Required referrer checked by the NBA API
    "Referer": "https://www.nba.com/",
}



# Local SQLite file where game data will be stored.
DB_NAME = "nba_data.db"



# DATABASE INITIALIZATION
def initialize_live_table(conn):

    conn.execute("""

        CREATE TABLE IF NOT EXISTS live_games (

            GAME_ID TEXT PRIMARY KEY,      -- Unique NBA identifier for each game

            GAME_DATE TEXT,                -- Date of the game

            GAME_STATUS_TEXT TEXT,         -- Status (Scheduled, Q1, Halftime, Final)

            HOME_TEAM_ID INTEGER,
            HOME_TEAM_NAME TEXT,

            VISITOR_TEAM_ID INTEGER,
            VISITOR_TEAM_NAME TEXT,

            HOME_TEAM_SCORE INTEGER,
            VISITOR_TEAM_SCORE INTEGER,

            LAST_UPDATED TEXT              -- Timestamp of most recent pipeline run
        );

    """)

    # Save schema changes
    conn.commit()



# FETCH SCOREBOARD DATA
# Retrieves today's NBA games from the ScoreboardV3 endpoint.
def fetch_scoreboard(today):

    for attempt in range(3):

        try:

            # Request scoreboard data
            scoreboard = scoreboardv3.ScoreboardV3(

                game_date=today,   # date to retrieve games for
                league_id="00",    # NBA league identifier
                timeout=30         # max request time in seconds
            )

            # Convert API response into a Python dictionary
            return scoreboard.get_dict()

        except Exception:

            print(f"Scoreboard retry {attempt+1}")

            # Wait before retrying request
            time.sleep(5)

    # If all retries fail
    print("Scoreboard failed.")
    return None


    # MAIN PIPELINE
def run_pipeline():

    # SET DATE AND TIMESTAMP
    # Format today's date for API request
    today = datetime.today().strftime("%Y-%m-%d")

    # Timestamp for when the pipeline was executed
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print(f"Fetching games for {today}")



    # EXTRACT DATA FROM API
    scoreboard = fetch_scoreboard(today)

    # Stop pipeline if API request failed
    if scoreboard is None:
        return


    # Extract list of games from API response
    games = scoreboard["scoreboard"]["games"]

    # Exit if no games scheduled today
    if len(games) == 0:

        print("No games today.")
        return


    # List that will hold processed game rows
    live_rows = []



    # TRANSFORM API DATA
    # Convert each game object into a structured row
    for game in games:

        game_id = game["gameId"]             # unique game identifier
        status = game["gameStatusText"]      # game status text

        # Team data
        home = game["homeTeam"]
        away = game["awayTeam"]

        # Current score (default 0 if not present)
        home_score = home.get("score", 0)
        away_score = away.get("score", 0)


        # Build row dictionary
        live_rows.append({

            "GAME_ID": game_id,
            "GAME_DATE": today,
            "GAME_STATUS_TEXT": status,

            "HOME_TEAM_ID": home["teamId"],
            "HOME_TEAM_NAME": home["teamName"],

            "VISITOR_TEAM_ID": away["teamId"],
            "VISITOR_TEAM_NAME": away["teamName"],

            "HOME_TEAM_SCORE": int(home_score),
            "VISITOR_TEAM_SCORE": int(away_score),

            "LAST_UPDATED": timestamp
        })


    # Convert rows into a pandas DataFrame
    live_df = pd.DataFrame(live_rows)



    # LOAD DATA INTO DATABASE

    # Open SQLite connection
    conn = sqlite3.connect(DB_NAME)

    # Ensure table exists
    initialize_live_table(conn)


    # Remove old snapshot so table always reflects
    # the most recent state of today's games
    conn.execute("DELETE FROM live_games")
    conn.commit()


    # Remove duplicate games (safety check)
    live_df = live_df.drop_duplicates(subset=["GAME_ID"])


    # Insert new rows into the database
    live_df.to_sql("live_games", conn, if_exists="append", index=False)


    # Close database connection
    conn.close()

    print("Live games table updated.")



# This ensures the pipeline only runs when the file is executed directly and not when it is imported elsewhere.
if __name__ == "__main__":

    run_pipeline()
