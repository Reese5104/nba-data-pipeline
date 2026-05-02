# 🏀 NBA Data Pipeline & Prediction System

[![Python](https://img.shields.io/badge/Python-3.14-blue)](https://www.python.org/)
[![SQLite](https://img.shields.io/badge/SQLite-database-orange)](https://www.sqlite.org/)
[![NBA API](https://img.shields.io/badge/NBA_API-Stats-green)](https://github.com/swar/nba_api)
[![XGBoost](https://img.shields.io/badge/XGBoost-ML-red)](https://xgboost.readthedocs.io/)

A Python-based data pipeline and machine learning system that collects NBA game data, stores player and team box scores in SQLite, and predicts upcoming game outcomes in real-time.

## Overview

This project automates NBA data collection and analytics using the NBA Stats API.
It retrieves game data, player statistics, team box scores, advanced metrics, and generates **real-time predictions** using ML models.

The goal is to provide a **complete analytics pipeline** for:

* Sports analytics
* Predictive modeling
* Machine learning applications
* Basketball performance analysis

---

## Features

* Collects historical and live NBA game data
* Retrieves player, team, and advanced box scores
* Tracks live games and updates final results
* Extracts advanced player metrics for modeling
* Generates real-time game predictions
* Stores structured data in SQLite
* Prevents duplicate entries
* Retry logic and rate limiting for API reliability

---

## Project Structure

```
sports-model/
│
├── boxscore_teamgames.py      # Historical pipeline for player and team box scores
├── boxscore_adv.py            # Advanced player statistics pipeline
├── career_stats.py            # Player career statistics collection
├── todays_games.py            # Tracks and stores today's NBA games
├── live_games.py              # Retrieves live NBA schedule and game status
├── uptd_team_boxscores.py     # Updates database with final box scores
│
├── live_game_prediction.py    # Real-time game prediction script
├── nba_ete_pL.py              # End-to-end ML training pipeline
├── upt_ete_pL.py              # Updates and retrains prediction model
├── game_model_training.py     # Standalone model training script
│
└── README.md                  # Project documentation
```

---

## Script Descriptions

### Data Collection & Processing

* **boxscore_teamgames.py** – Historical NBA game data ingestion
* **boxscore_adv.py** – Advanced player metrics extraction
* **career_stats.py** – Player career statistics collection
* **todays_games.py** – Tracks current-day NBA games
* **live_games.py** – Pulls live schedules and statuses
* **uptd_team_boxscores.py** – Updates database with final game stats

### Machine Learning & Prediction

* **live_game_prediction.py** – Generates real-time predictions for NBA games
* **nba_ete_pL.py** – End-to-end ML training: extraction → feature engineering → training → evaluation
* **upt_ete_pL.py** – Updates and retrains prediction models with new data
* **game_model_training.py** – Standalone script for training the NBA game prediction model

  **Description:**

  * Loads historical NBA game data from SQLite (`nba_data.db`)
  * Cleans and converts minutes played into numeric format
  * Forces numeric types for core stats: points, rebounds, assists, minutes
  * Computes **rolling averages and standard deviations** for teams and opponents
  * Creates **differential features** (team stats minus opponent stats)
  * Adds contextual features: home/away indicator, win/loss streaks
  * Trains an **XGBoost classifier** to predict game outcomes
  * Evaluates the model using accuracy, ROC-AUC, and confusion matrix
  * Saves trained model and feature set as `nba_model.pkl` for later prediction

---

## Pipeline Flow

1. Data Collection: `boxscore_teamgames.py`, `boxscore_adv.py`
2. Data Enrichment: `career_stats.py`
3. Live Tracking: `live_games.py`, `todays_games.py`
4. Data Finalization: `uptd_team_boxscores.py`
5. Machine Learning: `nba_ete_pL.py` → `upt_ete_pL.py` → `game_model_training.py` → `live_game_prediction.py`

**Result:** A fully automated NBA analytics + prediction pipeline

---

## Database Tables

### team_games

Columns: `GAME_ID`, `GAME_DATE`, `TEAM_ID`, `TEAM_NAME`, `MATCHUP`, `WL`, `PTS`

### player_boxscores

Columns: `PLAYER_ID`, `PLAYER_NAME`, `TEAM_ID`, `MIN`, `PTS`, `AST`, `REB`, `FG_PCT`

### team_boxscores

Columns: `TEAM_ID`, `GAME_ID`, `PTS`, `REB`, `AST`, `FG_PCT`, `TURNOVERS`

---

## Model Performance

The ML pipeline uses **XGBoost** for game predictions. Example metrics from testing:

| Metric           | Value                  |
| ---------------- | ---------------------- |
| Accuracy         | 0.6161                 |
| ROC-AUC          | 0.6589                 |
| Confusion Matrix | [[800 565], [486 887]] |

**Top features contributing to predictions:**

* PIE (Player Impact Estimate)
* Estimated Offensive Rating
* Assist to Turnover

> These metrics are calculated using historical game and player statistics to predict future outcomes.

---

## Running the Project

Install dependencies:

```bash
pip install nba_api pandas scikit-learn xgboost
```

Run a pipeline script:

```bash
python boxscore_teamgames.py
python todays_games.py
```

Run ML / prediction scripts:

```bash
python nba_ete_pL.py             # Collect data for model
python upt_ete_pL.py             # Update data for model
python game_model_training.py    # Train standalone game prediction model
python live_game_prediction.py   # Predict games
```

---

## Data Source

* NBA Stats API via `nba_api`
* Provides official NBA game results, player statistics, team statistics, and advanced metrics

---

## Future Improvements

* Automate daily pipeline runs
* Add dashboards for player and team performance
* Deploy pipeline to cloud infrastructure
* Parallelize data collection and ML training for faster updates
* Expand ML models for team/player prediction and betting simulations

---

## Author

**Reese Farquharson**
Computer Science Major
West Virginia Wesleyan College

**Interests:** Data Science, Sports Analytics, Cybersecurity

---
