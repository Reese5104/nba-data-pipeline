import pandas as pd
import numpy as np
import sqlite3
import pickle
import xgboost as xgb

from sklearn.metrics import accuracy_score, roc_auc_score, confusion_matrix
from datetime import datetime, timedelta
from nba_api.stats.endpoints import scoreboardv3

DB_PATH = "nba_data.db"          # database with historical games
MODEL_PATH = "nba_model.pkl"     # saved model file
ROLLING_WINDOW = 20              # rolling average window size

# base statistical features used for model
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

# FIND NEXT NBA GAME DAY
def get_next_available_game_day(start_date, max_days=10):

    # loop forward to find next valid NBA game date
    for i in range(max_days):
        check_date = (start_date + timedelta(days=i)).strftime("%Y-%m-%d")

        try:
            board = scoreboardv3.ScoreboardV3(game_date=check_date)
            frames = board.get_data_frames()

            # ensure API response is valid
            if len(frames) < 3:
                continue

            teams_df = frames[2]

            # return first valid game day
            if not teams_df.empty:
                return check_date, teams_df

        except Exception:
            continue

    return None, None


# LOAD DATA
def load_historical_games():

    # connect to SQLite database
    conn = sqlite3.connect(DB_PATH)

    # load model dataset
    df = pd.read_sql_query(
        "SELECT * FROM model_dataset ORDER BY GAME_DATE",
        conn
    )

    conn.close()
    return df


# CLEAN DATA
def force_numeric(df, cols):

    # convert selected columns to numeric
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return df


# BUILD ROLLING FEATURES
def add_rolling_features(df, features):

    # sort by date to prevent leakage
    df = df.sort_values("GAME_DATE").copy()

    for f in features:

        # clean values
        df[f] = pd.to_numeric(df[f], errors="coerce").fillna(0)

        # team rolling average
        df[f"{f}_rolling"] = (
            df.groupby("TEAM_ID")[f]
            .transform(lambda x: x.shift(1).rolling(ROLLING_WINDOW, min_periods=1).mean())
        )

        # opponent rolling average
        df[f"{f}_rolling_OPP"] = (
            df.groupby("TEAM_ID_OPP")[f]
            .transform(lambda x: x.shift(1).rolling(ROLLING_WINDOW, min_periods=1).mean())
        )

    return df


# FEATURE ENGINEERING
def prepare_features(df):

    # clean numeric data
    df = force_numeric(df, FEATURES)

    # build rolling stats
    df = add_rolling_features(df, FEATURES)

    feature_cols = []

    # create team vs opponent comparisons
    for f in FEATURES:

        if f"{f}_rolling" not in df.columns or f"{f}_rolling_OPP" not in df.columns:
            continue

       # create difference feature (team vs opponent advantage)
        df[f"{f}_diff"] = df[f"{f}_rolling"] - df[f"{f}_rolling_OPP"]

        # store team's rolling average (Home Team Strength)
        df[f"{f}_team"] = df[f"{f}_rolling"]
        
        # store opponent's rolling average (OPP strength)
        df[f"{f}_opp"] = df[f"{f}_rolling_OPP"]

        # create ratio feature (relative strength, How much better)
        # +1e-5 prevents division by zero
        df[f"{f}_ratio"] = df[f"{f}_rolling"] / (df[f"{f}_rolling_OPP"] + 1e-5)

        feature_cols += [
            f"{f}_diff",
            f"{f}_team",
            f"{f}_opp",
            f"{f}_ratio"
        ]

    # home advantage feature 
    df["is_home"] = 1

    # win streak feature 
    df["win_streak"] = (
        df.groupby("TEAM_ID")["WIN"]
        .transform(lambda x: x.shift(1).rolling(ROLLING_WINDOW, min_periods=1).sum())
        if "WIN" in df.columns else 0
    )

    # loss streak
    df["loss_streak"] = (
        df.groupby("TEAM_ID")["WIN"]
        .transform(lambda x: (1 - x).shift(1).rolling(ROLLING_WINDOW, min_periods=1).sum())
)
    feature_cols += ["is_home", "win_streak", "loss_streak"]

    # final ML matrices
    X = df[feature_cols].fillna(0)
    y = df["WIN"].astype(int) if "WIN" in df.columns else pd.Series([0] * len(df))

    return X, y, feature_cols


# TRAIN MODEL
def train_model():

    # load dataset
    df = load_historical_games()

    # build features
    X, y, feature_cols = prepare_features(df)

    # time-based split (prevents leakage)
    split = int(len(X) * 0.8)

    X_train, X_test = X.iloc[:split], X.iloc[split:]
    y_train, y_test = y.iloc[:split], y.iloc[split:]

    # XGBoost model
    model = xgb.XGBClassifier(
        n_estimators=300,
        max_depth=4,
        learning_rate=0.04,
        subsample=0.8,
        colsample_bytree=0.8,
        eval_metric="logloss"
    )

    # train model
    model.fit(X_train, y_train)

    # predictions
    preds = model.predict(X_test)
    probs = model.predict_proba(X_test)[:, 1]

    # evaluation
    print("\n--- MODEL PERFORMANCE ---")
    print("Accuracy:", accuracy_score(y_test, preds))
    print("ROC-AUC:", roc_auc_score(y_test, probs))
    print("Confusion Matrix:\n", confusion_matrix(y_test, preds))

    # save model
    with open(MODEL_PATH, "wb") as f:
        pickle.dump({"model": model, "features": feature_cols}, f)

    return model, feature_cols


# LIVE FEATURES
def build_live_features(team_id, opp_id):

    # connect to DB
    conn = sqlite3.connect(DB_PATH)

    # get recent team data
    team_df = pd.read_sql_query(
        f"SELECT * FROM model_dataset WHERE TEAM_ID={team_id} ORDER BY GAME_DATE DESC LIMIT {ROLLING_WINDOW}",
        conn
    )

    # get recent opponent data
    opp_df = pd.read_sql_query(
        f"SELECT * FROM model_dataset WHERE TEAM_ID={opp_id} ORDER BY GAME_DATE DESC LIMIT {ROLLING_WINDOW}",
        conn
    )

    conn.close()

    row = {}

    # build feature differences
    for f in FEATURES:

        # compute team's recent average for stat rolling window 
        row[f"{f}_team"] = team_df[f].tail(ROLLING_WINDOW).mean()

        # compute opponent's recent average for the same stat
        row[f"{f}_opp"] = opp_df[f].tail(ROLLING_WINDOW).mean()

        # compute difference (team advantage over opponent)
        row[f"{f}_diff"] = row[f"{f}_team"] - row[f"{f}_opp"]

    # static features
    # indicate this team is playing at home (1 = home, 0 = away)
    row["is_home"] = 1
    
    # placeholder for recent wins (not dynamically calculated here)
    row["win_streak"] = 0
    
    # placeholder for recent losses (not dynamically calculated here)
    row["loss_streak"] = 0

    return pd.DataFrame([row]).fillna(0)


# CONFIDENCE LABELS
def get_confidence_label(prob):

    # distance from 0.5 determines confidence
    edge = abs(prob - 0.5)

    if edge >= 0.20:
        return "Lock"
    elif edge >= 0.10:
        return "Likely"
    else:
        return "Toss-Up"


# PREDICTION PIPELINE
def predict_today(model, feature_cols):

    # start date
    start_date = datetime.today()

    # find next valid NBA game day
    game_date, teams_df = get_next_available_game_day(start_date)

    if teams_df is None:
        print("No NBA games found in next 10 days.")
        return

    print(f"Using game date: {game_date}")

    # map team IDs to names
    team_map = dict(zip(teams_df["teamId"], teams_df["teamName"]))

    results = []

    # loop games
    for gid in teams_df["gameId"].unique():

        g = teams_df[teams_df["gameId"] == gid]

        if len(g) != 2:
            continue

        home_id, away_id = g["teamId"].values

        # build feature vector
        X = build_live_features(home_id, away_id)

        # align with training features
        X = X.reindex(columns=feature_cols, fill_value=0)

        # predict probability
        prob = model.predict_proba(X)[0][1]

        results.append({
            "GAME_ID": gid,
            "HOME": team_map[home_id],
            "AWAY": team_map[away_id],
            "HOME_PROB": round(prob * 100, 2),
            "AWAY_PROB": round((1 - prob) * 100, 2),
            "PREDICTION": team_map[home_id] if prob > 0.5 else team_map[away_id],
            "CONFIDENCE": get_confidence_label(prob),
            "DATE_USED": game_date
        })

    df = pd.DataFrame(results)

    print(df)

    # skip empty output
    if df.empty:
        print("No predictions generated.")
        return

    # save to database
    conn = sqlite3.connect(DB_PATH)
    df.to_sql("predictions_today", conn, if_exists="replace", index=False)
    conn.close()


if __name__ == "__main__":
    model, feature_cols = train_model()
    predict_today(model, feature_cols)
