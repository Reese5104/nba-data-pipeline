# Core data handling
import pandas as pd
import numpy as np
import sqlite3

# Model saving/loading
import pickle
import os

# Machine learning model
import xgboost as xgb

# Evaluation metrics
from sklearn.metrics import accuracy_score, roc_auc_score, confusion_matrix

# Date handling + NBA API
from datetime import datetime
from nba_api.stats.endpoints import scoreboardv3

# CONFIGURATION CONSTANTS
DB_PATH = "nba_data.db"      # SQLite database path
MODEL_PATH = "nba_model.pkl" # Saved trained model
ROLLING_WINDOW = 25          # Number of past games used for rolling stats

# LOAD DATA
def load_historical_games():
    """
    Loads historical NBA game data from SQLite database.
    Data is ordered chronologically to preserve time-series integrity.
    """
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM model_dataset ORDER BY GAME_DATE", conn)
    conn.close()
    return df

# CLEANING HELPERS
def safe_minutes(val):
    """
    Converts minutes played into float format.
    Handles formats like:
    - "34" → 34.0
    - "34:21" → 34.35
    - invalid → 0.0
    """
    try:
        return float(val)
    except ValueError:
        try:
            parts = val.split(":")
            if len(parts) == 2:
                return int(parts[0]) + int(parts[1]) / 60
            else:
                return float(parts[0])
        except:
            return 0.0

def convert_minutes(df):
    """
    Applies safe_minutes conversion to all columns containing 'minutes'.
    """
    for col in df.columns:
        if "minutes" in col.lower():
            df[col] = df[col].astype(str).apply(lambda x: safe_minutes(x))
    return df

def force_numeric(df, cols):
    """
    Forces selected columns to numeric type.
    Invalid values are coerced into NaN.
    """
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

# ROLLING FEATURES
def add_rolling_features(df, features):
    """
    Creates rolling mean and standard deviation features
    for both team and opponent using past games only (shifted).
    """
    df = df.sort_values("GAME_DATE").copy()

    for f in features:
        # Ensure column exists
        if f not in df.columns:
            df[f] = 0

        # Convert to numeric and fill missing
        df[f] = pd.to_numeric(df[f], errors="coerce").fillna(0)

        # Rolling average (team)
        df[f"{f}_rolling"] = (
            df.groupby("TEAM_ID")[f]
            .transform(lambda x: x.shift(1).rolling(ROLLING_WINDOW, min_periods=1).mean())
        )

        # Rolling average (opponent)
        df[f"{f}_rolling_OPP"] = (
            df.groupby("TEAM_ID_OPP")[f]
            .transform(lambda x: x.shift(1).rolling(ROLLING_WINDOW, min_periods=1).mean())
        )

        # Rolling standard deviation (team)
        df[f"{f}_std"] = (
            df.groupby("TEAM_ID")[f]
            .transform(lambda x: x.shift(1).rolling(ROLLING_WINDOW, min_periods=1).std())
        ).fillna(0)

        # Rolling standard deviation (opponent)
        df[f"{f}_std_OPP"] = (
            df.groupby("TEAM_ID_OPP")[f]
            .transform(lambda x: x.shift(1).rolling(ROLLING_WINDOW, min_periods=1).std())
        ).fillna(0)

    return df

# FEATURE ENGINEERING
def prepare_features(df):
    """
    Main feature engineering pipeline:
    - Cleans data
    - Generates rolling stats
    - Creates differential features (team vs opponent)
    - Adds contextual features (home, streaks)
    """
    df = convert_minutes(df)

    # Core statistical features
    features = ["points", "reboundsTotal", "assists", "minutes"]
    df = force_numeric(df, features)

    # Ensure opponent stats are numeric
    for f in features:
        opp_col = f"{f}_OPP"
        if opp_col in df.columns:
            df[opp_col] = pd.to_numeric(df[opp_col], errors="coerce")

    df = df.fillna(0)

    # Add rolling statistics
    df = add_rolling_features(df, features)

    # DIFFERENTIAL FEATURES (team - opponent)
    for f in features:
        df[f"{f}_diff"] = df[f"{f}_rolling"] - df[f"{f}_rolling_OPP"]
        df[f"{f}_std_diff"] = df[f"{f}_std"] - df[f"{f}_std_OPP"]

    # HOME/AWAY INDICATOR
    if "HOME_TEAM_ID" in df.columns:
        df['is_home'] = (df['TEAM_ID'] == df['HOME_TEAM_ID']).astype(int)
    else:
        df['is_home'] = 0

    # WIN/LOSS STREAK FEATURES
    if "WIN" in df.columns:
        df['win_streak'] = df.groupby('TEAM_ID')['WIN'].transform(
            lambda x: x.shift(1).rolling(ROLLING_WINDOW, min_periods=1).sum()
        )
        df['loss_streak'] = df.groupby('TEAM_ID')['WIN'].transform(
            lambda x: (1 - x).shift(1).rolling(ROLLING_WINDOW, min_periods=1).sum()
        )
    else:
        df['win_streak'] = 0
        df['loss_streak'] = 0

    # Final feature set
    feature_cols = []
    for f in features:
        feature_cols += [f"{f}_diff", f"{f}_std_diff"]
    feature_cols += ['is_home', 'win_streak', 'loss_streak']

    # Feature matrix and labels
    X = df[feature_cols].fillna(0)
    y = df["WIN"].astype(int) if "WIN" in df.columns else pd.Series([0]*len(df))

    return X, y, feature_cols

# TRAIN MODEL
def train_model():
    """
    Trains XGBoost model on historical data and evaluates performance.
    Saves trained model + feature list.
    """
    df = load_historical_games()
    X, y, feature_cols = prepare_features(df)

    # Time-based split (NOT random)
    split = int(len(X) * 0.8)
    X_train, X_test = X.iloc[:split], X.iloc[split:]
    y_train, y_test = y.iloc[:split], y.iloc[split:]

    # Model configuration
    model = xgb.XGBClassifier(
        n_estimators=250,
        max_depth=4,
        learning_rate=0.04,
        subsample=0.8,
        colsample_bytree=0.8,
        eval_metric="logloss",
        use_label_encoder=False,
        random_state=42
    )

    print(f"Training model on {len(X_train)} games...")
    model.fit(X_train, y_train)

    # Evaluation
    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]

    print("\n--- Model Evaluation ---")
    print(f"Accuracy: {accuracy_score(y_test, y_pred):.4f}")
    print(f"ROC-AUC : {roc_auc_score(y_test, y_proba):.4f}")
    print("Confusion Matrix:\n", confusion_matrix(y_test, y_pred))

    # Save model
    with open(MODEL_PATH, "wb") as f:
        pickle.dump({"model": model, "features": feature_cols}, f)

    print("\nModel saved\n")
    return model

# BUILD LIVE FEATURES
def build_live_features(team_id, opp_id):
    """
    Builds feature vector for a single live matchup using
    most recent ROLLING_WINDOW games for both teams.
    """
    conn = sqlite3.connect(DB_PATH)

    # Get recent games
    team_df = pd.read_sql_query(
        f"SELECT * FROM model_dataset WHERE TEAM_ID={team_id} ORDER BY GAME_DATE DESC LIMIT {ROLLING_WINDOW}",
        conn
    )
    opp_df = pd.read_sql_query(
        f"SELECT * FROM model_dataset WHERE TEAM_ID={opp_id} ORDER BY GAME_DATE DESC LIMIT {ROLLING_WINDOW}",
        conn
    )

    conn.close()

    row = {}

    # Compute averages and standard deviations
    for f in ["points", "reboundsTotal", "assists", "minutes"]:
        team_avg = pd.to_numeric(team_df[f], errors="coerce").mean() if f in team_df else 0
        opp_avg = pd.to_numeric(opp_df[f], errors="coerce").mean() if f in opp_df else 0
        team_std = pd.to_numeric(team_df[f], errors="coerce").std() if f in team_df else 0
        opp_std = pd.to_numeric(opp_df[f], errors="coerce").std() if f in opp_df else 0

        row[f"{f}_diff"] = team_avg - opp_avg
        row[f"{f}_std_diff"] = team_std - opp_std

    # Context features (simplified for live prediction)
    row['is_home'] = 1
    row['win_streak'] = 0
    row['loss_streak'] = 0

    return pd.DataFrame([row]).fillna(0)

# LOCK LOGIC
def get_confidence_label(prob, lock_threshold=0.65, likely_threshold=0.55):
    """
    Converts probability into human-readable confidence tier.
    """
    if prob >= lock_threshold:
        return "Lock"
    elif prob >= likely_threshold:
        return "Likely"
    elif prob <= (1 - lock_threshold):
        return "Upset Possible"
    else:
        return "Toss-Up"

# PREDICT TODAY
def predict_today(model):
    """
    Pulls today's NBA games and generates predictions.
    Saves results to database.
    """
    print("\nRunning today's NBA predictions...\n")

    today_str = datetime.today().strftime("%Y-%m-%d")

    # Fetch today's games
    board = scoreboardv3.ScoreboardV3(game_date=today_str)
    teams_df = board.get_data_frames()[2]

    if teams_df.empty:
        print("No NBA games scheduled today")
        return

    # Map team IDs to names
    team_map = dict(zip(teams_df["teamId"], teams_df["teamName"]))
    results = []

    # Loop through each game
    for gid in teams_df["gameId"].unique():
        g = teams_df[teams_df["gameId"] == gid]

        if len(g) != 2:
            continue

        home_id, away_id = g["teamId"].values

        try:
            # Build features and predict
            X_home = build_live_features(home_id, away_id)
            prob = model.predict_proba(X_home)[0][1]

            # Confidence labels
            home_conf = get_confidence_label(prob)
            away_conf = get_confidence_label(1 - prob)

            # Store result
            results.append({
                "GAME_ID": gid,
                "HOME_TEAM_ID": home_id,
                "AWAY_TEAM_ID": away_id,
                "HOME_TEAM_NAME": team_map.get(home_id),
                "AWAY_TEAM_NAME": team_map.get(away_id),
                "PREDICTION_DATE": today_str,
                "HOME_WIN_PROB": round(prob * 100, 2),
                "AWAY_WIN_PROB": round((1 - prob) * 100, 2),
                "PREDICTED_WINNER": team_map.get(home_id) if prob > 0.5 else team_map.get(away_id),
                "HOME_CONFIDENCE": home_conf,
                "AWAY_CONFIDENCE": away_conf
            })

        except Exception as e:
            print(f"Error on game {gid}: {e}")

    # Save predictions
    if results:
        df = pd.DataFrame(results)

        conn = sqlite3.connect(DB_PATH)
        df.to_sql("predictions_today", conn, if_exists="replace", index=False)
        conn.close()

        print("\nPredictions saved to predictions_today\n")

        # Display key results
        print(df[['GAME_ID', 'HOME_TEAM_NAME', 'AWAY_TEAM_NAME', 
                  'HOME_WIN_PROB', 'HOME_CONFIDENCE', 
                  'AWAY_WIN_PROB', 'AWAY_CONFIDENCE', 
                  'PREDICTED_WINNER']])
    else:
        print("No predictions generated")

# MAIN EXECUTION
if __name__ == "__main__":
    model = train_model()

    if model is not None:
        predict_today(model)
