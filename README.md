# NBA Data Pipeline

A Python data pipeline that collects NBA game data using the nba_api and stores player and team box scores in a SQLite database for sports analytics and modeling.

---

## Overview

This project automates the collection of NBA game statistics using the NBA Stats API.  
The pipeline retrieves game information, player statistics, and team box scores, then stores the results in a structured SQLite database.

The goal of this project is to create a reliable dataset for:

- sports analytics
- predictive modeling
- machine learning projects
- basketball performance analysis

---

## Features

- Collects NBA game data using nba_api
- Retrieves player box scores
- Retrieves team box scores
- Tracks live games for the current day
- Updates game results once games are final
- Stores structured data in SQLite
- Prevents duplicate game inserts
- Includes retry logic to handle API failures
- Uses rate limiting to avoid API blocking

---

## Tech Stack

- Python
- Pandas
- SQLite
- nba_api
- Git

---

## Project Structure

```
sports-model/
│
├── boxscore_teamgames.py      # Historical pipeline that collects team and player box scores
├── career_stats.py            # Collects career statistics for NBA players
├── todays_games.py            # Tracks and stores today's NBA games
├── uptd_team_boxscores.py     # Updates database with final box scores after games finish
└── README.md                  # Project documentation
```

---

## Script Descriptions

### boxscore_teamgames.py

Pulls historical NBA game data and retrieves player and team box scores using the NBA Stats API.  
The cleaned data is saved to a SQLite database.

---

### career_stats.py

Collects career statistics for NBA players.  
This data can be used to enrich datasets for modeling or player analysis.

---

### todays_games.py

Tracks live NBA games for the current day.  
This script monitors game status and prepares games for final stat ingestion.

---

### uptd_team_boxscores.py

Updates the database with final team and player box scores once games are completed.

---

## Pipeline Flow

1. Fetch NBA games from the NBA Stats API
2. Identify games not already stored in the database
3. Retrieve player and team box scores
4. Clean and structure the data using pandas
5. Store results in SQLite tables

---

## Database Tables

### team_games

Stores basic game-level information.

Columns include:

- GAME_ID
- GAME_DATE
- TEAM_ID
- TEAM_NAME
- MATCHUP
- WL
- PTS

---

### player_boxscores

Stores player statistics for each game.

Examples:

- PLAYER_ID
- PLAYER_NAME
- TEAM_ID
- MIN
- PTS
- AST
- REB
- FG_PCT

---

### team_boxscores

Stores team-level statistics for each game.

Examples:

- TEAM_ID
- GAME_ID
- PTS
- REB
- AST
- FG_PCT
- TURNOVERS

---

## Running the Project

Install dependencies:

```bash
pip install nba_api pandas
```

Run a pipeline script:

```bash
python boxscore_teamgames.py
```

or

```bash
python todays_games.py
```

The scripts will fetch NBA data and update the SQLite database.

---

## Data Source

This project uses the NBA Stats API through the Python package:

nba_api

The API provides official NBA statistics including:

- game results
- player statistics
- team statistics

---

## Future Improvements

- Automate daily pipeline runs
- Add machine learning models for game prediction
- Create player performance dashboards
- Deploy pipeline to a cloud environment
- Build advanced analytics features

---

## Author

Reese Farquharson  
Computer Science Major  
West Virginia Wesleyan College  

Interested in:

- Data Science
- Sports Analytics
- Cybersecurity
