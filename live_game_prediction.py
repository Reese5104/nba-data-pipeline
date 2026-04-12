import pandas as pd
import numpy as np
import sqlite3
import pickle
import xgboost as xgb

from sklearn.metrics import accuracy_score, roc_auc_score, confusion_matrix
from datetime import datetime
from nba_api.stats.endpoints import scoreboardv3

DB_PATH = "nba_data.db"          # SQLite database containing historical NBA games
MODEL_PATH = "nba_model.pkl"     # Serialized trained model output file
ROLLING_WINDOW = 20               # Number of past games used for rolling averages

# Core statistical features used as base inputs for model
FEATURES = [
    "offensiveRating",
    "defensiveRating",
    "netRating",
    "trueShootingPercentage",
    "effectiveFieldGoalPercentage",
    "pace",
    "possessions",
    "assistToTurnover",
    "reboundPercentage",
    "PIE"
]

# ELO RATING SYSTEM (TEAM STRENGTH MODEL)
# Elo is used as a secondary rating system capturing team strength over time


def init_elo():
    # Creates empty dictionary for storing team Elo ratings
    return {}


def expected_score(elo_a, elo_b):
    # Converts Elo difference into win probability
    # Logistic function based on standard Elo formula
    return 1 / (1 + 10 ** ((elo_b - elo_a) / 400))


def update_elo(elo_a, elo_b, margin, home_adv=50, k=20):
    # Updates Elo ratings based on game result and margin of victory
    # margin-of-victory scaling improves sensitivity to blowouts

    # Log scaling prevents extreme swings for large margins
    mov_factor = np.log(abs(margin) + 1) * (1.2 if margin > 0 else 1.0)

    # Expected probability of team A winning
    exp_a = expected_score(elo_a + home_adv, elo_b)

    # Update rule: increase if outperform expectation, decrease otherwise
    new_a = elo_a + k * mov_factor * (1 - exp_a)
    new_b = elo_b + k * mov_factor * (0 - (1 - exp_a))

    return new_a, new_b

# FATIGUE FEATURE 
# Measures team workload based on number of recent games played
def compute_fatigue(team_games):
    # Normalized fatigue score (0 = rested, 1 = heavily fatigued)
    return min(len(team_games) / ROLLING_WINDOW, 1.0)

# Pulls historical NBA game data from SQLite database
def load_historical_games():
    conn = sqlite3.connect(DB_PATH)

    # model_dataset must contain preprocessed game-level stats
    df = pd.read_sql_query(
        "SELECT * FROM model_dataset ORDER BY GAME_DATE",
        conn
    )

    conn.close()
    return df

# DATA CLEANING 
def convert_minutes(df):
    # converting string minutes to numeric if needed
    return df


def force_numeric(df, cols):
    # Ensures all selected feature columns are numeric
    # Non-numeric values are coerced into NaN
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

# ROLLING FEATURE 
# shift(1) prevents using future data (no leakage) by shifting values

def add_rolling_features(df, features):
    # Ensure chronological order before computing rolling statistics
    df = df.sort_values("GAME_DATE").copy()

    for f in features:
        # Clean feature values
        df[f] = pd.to_numeric(df[f], errors="coerce").fillna(0)

        # TEAM rolling average 
        df[f"{f}_rolling"] = (
            df.groupby("TEAM_ID")[f]
            .transform(lambda x: x.shift(1).rolling(ROLLING_WINDOW, min_periods=1).mean())
        )

        # OPPONENT rolling average
        df[f"{f}_rolling_OPP"] = (
            df.groupby("TEAM_ID_OPP")[f]
            .transform(lambda x: x.shift(1).rolling(ROLLING_WINDOW, min_periods=1).mean())
        )

    return df

# FEATURE ENGINEERING 
# Converts raw dataset into ML-ready feature matrix

def prepare_features(df):
    # basic cleaning
    df = convert_minutes(df)
    df = force_numeric(df, FEATURES)

    # rolling statistics
    df = add_rolling_features(df, FEATURES)

    feature_cols = []

    #derive comparative team features
    for f in FEATURES:
        # Difference between team and opponent form
        df[f"{f}_diff"] = df[f"{f}_rolling"] - df[f"{f}_rolling_OPP"]

        # Raw rolling values for model signal retention
        df[f"{f}_team"] = df[f"{f}_rolling"]
        df[f"{f}_opp"] = df[f"{f}_rolling_OPP"]

        # Track feature names for model input
        feature_cols += [
            f"{f}_diff",
            f"{f}_team",
            f"{f}_opp"
        ]

    # HOME ADVANTAGE FEATURE
    df["is_home"] = 1

    # WIN STREAK FEATURE
    if "WIN" in df.columns:
        df["win_streak"] = df.groupby("TEAM_ID")["WIN"].transform(
            lambda x: x.shift(1).rolling(ROLLING_WINDOW, min_periods=1).sum()
        )
    else:
        df["win_streak"] = 0

    df["loss_streak"] = 0

    feature_cols += ["is_home", "win_streak", "loss_streak"]

    # Final ML matrices
    X = df[feature_cols].fillna(0)
    y = df["WIN"].astype(int) if "WIN" in df.columns else pd.Series([0] * len(df))

    return X, y, feature_cols

# MODEL TRAINING XGBOOST
def train_model():
    # Load dataset from database
    df = load_historical_games()

    # Convert raw data into ML features
    X, y, feature_cols = prepare_features(df)

    # Time-based split prevents leakage from future games
    split = int(len(X) * 0.8)

    X_train, X_test = X.iloc[:split], X.iloc[split:]
    y_train, y_test = y.iloc[:split], y.iloc[split:]

    # Initialize gradient boosting model
    model = xgb.XGBClassifier(
        n_estimators=300,
        max_depth=4,
        learning_rate=0.04,
        subsample=0.8,
        colsample_bytree=0.8,
        eval_metric="logloss"
    )


    # Train model
    model.fit(X_train, y_train)

    # Generate predictions
    preds = model.predict(X_test)
    probs = model.predict_proba(X_test)[:, 1]

    # MODEL EVALUATION
    print("\n--- MODEL PERFORMANCE ---")
    print("Accuracy:", accuracy_score(y_test, preds))
    print("ROC-AUC:", roc_auc_score(y_test, probs))
    print("Confusion Matrix:\n", confusion_matrix(y_test, preds))

    # Save trained model + feature schema
    with open(MODEL_PATH, "wb") as f:
        pickle.dump({"model": model, "features": feature_cols}, f)

    return model, feature_cols

# LIVE FEATURES
# Constructs real-time feature vectors for upcoming games

def build_live_features(team_id, opp_id):
    conn = sqlite3.connect(DB_PATH)

    # Pull recent team performance window
    team_df = pd.read_sql_query(
        f"SELECT * FROM model_dataset WHERE TEAM_ID={team_id} ORDER BY GAME_DATE DESC LIMIT {ROLLING_WINDOW}",
        conn
    )

    # Pull opponent performance window
    opp_df = pd.read_sql_query(
        f"SELECT * FROM model_dataset WHERE TEAM_ID={opp_id} ORDER BY GAME_DATE DESC LIMIT {ROLLING_WINDOW}",
        conn
    )

    conn.close()

    row = {}

    # Compute real-time statistical comparisons
    for f in FEATURES:
        row[f"{f}_diff"] = team_df[f].mean() - opp_df[f].mean()
        row[f"{f}_team"] = team_df[f].mean()
        row[f"{f}_opp"] = opp_df[f].mean()

    # Static features
    row["is_home"] = 1
    row["win_streak"] = 0
    row["loss_streak"] = 0

    return pd.DataFrame([row]).fillna(0)

# CONFIDENCE LOGIC
# Converts probability output into human-readable confidence tiers

def get_confidence_label(prob):
    edge = abs(prob - 0.5)

    if edge >= 0.20:
        return "Lock"
    elif edge >= 0.10:
        return "Likely"
    else:
        return "Toss-Up"

# LIVE GAME PREDICTION 

def predict_today(model, feature_cols):
    print("\n--- TODAY'S PREDICTIONS ---\n")

    today = datetime.today().strftime("%Y-%m-%d")

    # Fetch live NBA schedule
    board = scoreboardv3.ScoreboardV3(game_date=today)
    teams_df = board.get_data_frames()[2]

    team_map = dict(zip(teams_df["teamId"], teams_df["teamName"]))

    results = []

    for gid in teams_df["gameId"].unique():
        g = teams_df[teams_df["gameId"] == gid]

        if len(g) != 2:
            continue

        home_id, away_id = g["teamId"].values

        # Build feature vector for matchup
        X = build_live_features(home_id, away_id)

        # Ensure feature alignment with training
        X = X.reindex(columns=feature_cols, fill_value=0)

        # Predict win probability
        prob = model.predict_proba(X)[0][1]

        results.append({
            "GAME_ID": gid,
            "HOME": team_map[home_id],
            "AWAY": team_map[away_id],
            "HOME_PROB": round(prob * 100, 2),
            "AWAY_PROB": round((1 - prob) * 100, 2),
            "PREDICTION": team_map[home_id] if prob > 0.5 else team_map[away_id],
            "CONFIDENCE": get_confidence_label(prob),
            "DATE": today
        })

    df = pd.DataFrame(results)

    print(df)

    # Store predictions in database for tracking
    conn = sqlite3.connect(DB_PATH)
    df.to_sql("predictions_today", conn, if_exists="replace", index=False)
    conn.close()

if __name__ == "__main__":
    model, feature_cols = train_model()
    predict_today(model, feature_cols)
