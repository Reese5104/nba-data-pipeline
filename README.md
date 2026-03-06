### Steps on GitHub

1. Go to your repository.
2. Click **Add file** → **Create new file**.
3. Name the file:

```
README.md
```

4. Paste the following:

```markdown
# NBA Data Pipeline

A Python data pipeline that collects NBA game data using the nba_api and stores player and team box scores in a SQLite database for sports analytics and modeling.

---

## Overview

This project automates the process of collecting NBA game data and storing it in a structured database. The pipeline pulls data from the NBA Stats API, retrieves player and team box scores, and saves the results into a SQLite database.

The goal of this project is to build a reliable sports data pipeline that can support analytics models, betting models, or machine learning applications.

---

## Features

- Collects NBA game data using nba_api
- Retrieves player box scores
- Retrieves team box scores
- Stores data in SQLite
- Prevents duplicate game inserts
- Includes retry logic to handle API errors
- Rate limiting to prevent API blocking

---

## Tech Stack

- Python
- Pandas
- SQLite
- nba_api
- Git / GitHub

---

## Project Structure

```

sports-model/
│
├── pipeline.py
├── todays_games.py
├── nba_data.db
└── README.md

````

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

### team_boxscores
Stores team-level game statistics.

Examples:
- TEAM_ID
- GAME_ID
- PTS
- REB
- AST

---

## Running the Pipeline

Run the pipeline using:

```bash
python pipeline.py
````

The pipeline will:

1. Fetch NBA games
2. Retrieve box scores
3. Save the data into the SQLite database

---

## Future Improvements

* Automate daily updates
* Add predictive modeling
* Build sports analytics dashboards
* Deploy pipeline to the cloud

---

## Author

Reese Farquharson
Computer Science Major
West Virginia Wesleyan College

Interests:

* Sports Analytics
* Data Science
* Cybersecurity

```


