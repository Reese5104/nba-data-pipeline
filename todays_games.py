from nba_api.stats.endpoints import scoreboardv3, boxscoretraditionalv3


# IMPORT HTTP OVERRIDE
# The NBA stats API blocks many automated requests unless they look like they are coming from a browser.
# This library allows us to override the request headers so the API accepts our requests.
from nba_api.stats.library.http import NBAStatsHTTP


# STANDARD PYTHON LIBRARIES
from datetime import datetime     # used for timestamps
import sqlite3                    # lightweight SQL database
import pandas as pd               # data manipulation
import time                       # used for API rate limiting


# API REQUEST HEADERS
# These headers mimic a real web browser request.
# Without these headers, the NBA API will often block requests.
NBAStatsHTTP.headers = {

    "Host": "stats.nba.com",

    # Pretend to be a browser
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X)",

    "Accept": "application/json, text/plain, */*",

    # Required because NBA API checks where the request came from
    "Referer": "https://www.nba.com/",
}


# SQLite database file that stores all NBA data locally.
DB_NAME = "nba_data.db"


# CREATE LIVE GAMES TABLE
def initialize_live_table(conn):

    conn.execute("""

        CREATE TABLE IF NOT EXISTS live_games (

            GAME_ID TEXT PRIMARY KEY,      -- Unique NBA game identifier
            GAME_DATE TEXT,                -- Date of the game
            GAME_STATUS_TEXT TEXT,         -- Game status (Scheduled, Q1, Halftime, Final)

            HOME_TEAM_ID INTEGER,
            HOME_TEAM_NAME TEXT,

            VISITOR_TEAM_ID INTEGER,
            VISITOR_TEAM_NAME TEXT,

            HOME_TEAM_SCORE INTEGER,
            VISITOR_TEAM_SCORE INTEGER,

            LAST_UPDATED TEXT              -- Timestamp when data was last refreshed
        );

    """)

    conn.commit()


# FETCH SCOREBOARD DATA
# This function retrieves today's games from the NBA API.
# Because the API occasionally fails or times out,
# the function retries the request up to 3 times.
def fetch_scoreboard(today):

    for attempt in range(3):

        try:

            scoreboard = scoreboardv3.ScoreboardV3(

                game_date=today,   # date we want data for
                league_id="00",    # 00 = NBA
                timeout=30         # request timeout in seconds
            )

            # Convert the API response into a Python dictionary
            return scoreboard.get_dict()

        except Exception:

            print(f"Scoreboard retry {attempt+1}")

            # Wait before retrying request
            time.sleep(5)

    print("Scoreboard failed.")
    return None


# SAVE FINAL GAME DATA
# When a game finishes, we retrieve the full boxscore and
# store it in permanent historical tables.
# These tables include:
#   team_games
#   player_boxscores
#   team_boxscores
def save_final_game(game_id, conn):

    print(f"Saving FINAL game {game_id}")


    # Boxscores sometimes appear several seconds after
    # a game is marked "Final", so we retry if needed.
    for attempt in range(3):

        try:

            box = boxscoretraditionalv3.BoxScoreTraditionalV3(

                game_id=game_id,
                timeout=30

            ).get_dict()


            # Convert JSON data to pandas DataFrames
            player_stats = pd.DataFrame(
                box["boxScoreTraditional"]["playerStats"]
            )

            team_stats = pd.DataFrame(
                box["boxScoreTraditional"]["teamStats"]
            )


            # If team stats aren't ready yet, retry
            if team_stats.empty or len(team_stats) < 2:

                print("Boxscore not ready, retrying...")
                time.sleep(5)
                continue

            break


        except Exception:

            print(f"Boxscore retry {attempt+1}")
            time.sleep(5)


    else:

        print(f"Skipping game {game_id}")
        return


    # RENAME API COLUMNS
    # The NBA API uses camelCase naming.
    # We convert to SQL-friendly uppercase names.
    player_stats.rename(columns={

        "playerId": "PLAYER_ID",
        "teamId": "TEAM_ID",
        "name": "PLAYER_NAME",
        "minutes": "MIN",
        "points": "PTS",
        "reboundsTotal": "REB",
        "assists": "AST",
        "steals": "STL",
        "blocks": "BLK",
        "turnovers": "TOV"

    }, inplace=True)


    team_stats.rename(columns={

        "teamId": "TEAM_ID",
        "teamName": "TEAM_NAME",
        "points": "PTS",
        "reboundsTotal": "REB",
        "assists": "AST",
        "turnovers": "TOV"

    }, inplace=True)


    # Add GAME_ID column so tables can be joined later
    player_stats["GAME_ID"] = game_id
    team_stats["GAME_ID"] = game_id


    # CREATE TEAM GAME SUMMARY
    home = team_stats.iloc[0]
    away = team_stats.iloc[1]


    team_games = pd.DataFrame([{

        "GAME_ID": game_id,
        "GAME_STATUS_TEXT": "Final",

        "HOME_TEAM_ID": home["TEAM_ID"],
        "VISITOR_TEAM_ID": away["TEAM_ID"],

        "HOME_TEAM_SCORE": int(home["PTS"]),
        "VISITOR_TEAM_SCORE": int(away["PTS"])

    }])


    # Remove existing entry if pipeline previously ran
    conn.execute("DELETE FROM team_games WHERE GAME_ID = ?", (game_id,))


    # Insert final game result
    team_games.to_sql("team_games", conn, if_exists="append", index=False)


    # SAVE PLAYER BOXSCORES
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
        "DELETE FROM player_boxscores WHERE GAME_ID = ?",
        (game_id,)
    )


    player_stats[player_cols].to_sql("player_boxscores", conn, if_exists="append", index=False)


    # SAVE TEAM BOXSCORES
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
        "DELETE FROM team_boxscores WHERE GAME_ID = ?",
        (game_id,)
    )


    team_stats[team_cols].to_sql("team_boxscores", conn, if_exists="append", index=False)


    conn.commit()


# MAIN PIPELINE FUNCTION
# Extract data from NBA API
# Transform JSON into DataFrames
# Load results into SQLite tables
def run_pipeline():

    # Format today's date for API request
    today = datetime.today().strftime("%Y-%m-%d")

    # Timestamp used for live table updates
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print(f"Fetching games for {today}")


    # Pull scoreboard data
    scoreboard = fetch_scoreboard(today)

    if scoreboard is None:
        return


    games = scoreboard["scoreboard"]["games"]

    if len(games) == 0:

        print("No games today.")
        return


    live_rows = []


    # PROCESS EACH GAME
    for game in games:

        game_id = game["gameId"]
        status = game["gameStatusText"]

        home = game["homeTeam"]
        away = game["awayTeam"]

        home_score = home.get("score", 0)
        away_score = away.get("score", 0)


        # Build row for live_games table
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


    # Convert list of dictionaries into DataFrame
    live_df = pd.DataFrame(live_rows)


    # DATABASE CONNECTION
    conn = sqlite3.connect(DB_NAME)

    initialize_live_table(conn)


    # Refresh live snapshot table
    conn.execute("DELETE FROM live_games")
    conn.commit()


    # Remove duplicates if API returns them
    live_df = live_df.drop_duplicates(subset=["GAME_ID"])


    live_df.to_sql("live_games", conn, if_exists="append", index=False)


    # PROCESS FINAL GAMES
    finals = live_df[
        live_df["GAME_STATUS_TEXT"].str.contains("Final")
    ]


    for game_id in finals["GAME_ID"]:

        save_final_game(game_id, conn)

        # Prevent NBA API rate limiting
        time.sleep(2)


    conn.close()

    print("Pipeline complete.")


# This ensures the pipeline runs only when the script is executed directly.
if __name__ == "__main__":

    run_pipeline()
