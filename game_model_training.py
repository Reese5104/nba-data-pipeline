# IMPORTS
import pandas as pd             # Data manipulation
import numpy as np              # Numeric computations
import sqlite3                  # SQLite database access
import pickle                   # Save/load models
import xgboost as xgb           # Machine learning model
from sklearn.metrics import accuracy_score, roc_auc_score, confusion_matrix  # Model evaluation

# ------------------------------
# CONFIGURATION
# ------------------------------
DB_PATH = "nba_data.db"        # Path to SQLite database containing historical games
MODEL_PATH = "nba_model.pkl"   # Path to save trained XGBoost model
ROLLING_WINDOW = 25            # Number of past games used for rolling statistics

# LOAD HISTORICAL DATA
def load_historical_games():
    """
    Load historical NBA games from SQLite database.
    Data is ordered chronologically by GAME_DATE to preserve time-series integrity.
    """
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM model_dataset ORDER BY GAME_DATE", conn)
    conn.close()
    return df

# MINUTES CLEANING
def safe_minutes(val):
    """
    Converts minutes played into float format.
    Handles:
      - "34" -> 34.0
      - "34:21" -> 34 + 21/60
      - invalid/missing -> 0.0
    """
    if val is None:
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    try:
        if ":" in val:
            mins, secs = val.split(":")
            return float(mins) + float(secs)/60
        else:
            return float(val)
    except:
        return 0.0

def convert_minutes(df):
    """
    Apply safe_minutes conversion to all columns containing 'minutes'.
    Ensures rolling statistics can be computed without type errors.
    """
    for col in df.columns:
        if "minutes" in col.lower():
            df[col] = df[col].astype(str).apply(safe_minutes)
    return df

# FORCE NUMERIC COLUMNS
def force_numeric(df, cols):
    """
    Ensures specified columns are numeric.
    Invalid entries are coerced to 0.
    """
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df

# ROLLING FEATURES
def add_rolling_features(df, features):
    """
    Compute rolling averages and standard deviations for each team and opponent.
    Uses a shifted rolling window to prevent lookahead bias.
    """
    df = df.sort_values("GAME_DATE").copy()
    for f in features:
        # Ensure column exists and is numeric
        if f not in df.columns:
            df[f] = 0
        df[f] = pd.to_numeric(df[f], errors="coerce").fillna(0)

        # Rolling mean for team
        df[f"{f}_rolling"] = df.groupby("TEAM_ID")[f]\
            .transform(lambda x: x.shift(1).rolling(ROLLING_WINDOW, min_periods=1).mean())
        # Rolling mean for opponent
        df[f"{f}_rolling_OPP"] = df.groupby("TEAM_ID_OPP")[f]\
            .transform(lambda x: x.shift(1).rolling(ROLLING_WINDOW, min_periods=1).mean())

        # Rolling standard deviation for team
        df[f"{f}_std"] = df.groupby("TEAM_ID")[f]\
            .transform(lambda x: x.shift(1).rolling(ROLLING_WINDOW, min_periods=1).std()).fillna(0)
        # Rolling standard deviation for opponent
        df[f"{f}_std_OPP"] = df.groupby("TEAM_ID_OPP")[f]\
            .transform(lambda x: x.shift(1).rolling(ROLLING_WINDOW, min_periods=1).std()).fillna(0)
    return df

# FEATURE ENGINEERING
def prepare_features(df):
    """
    Main feature engineering pipeline:
      - Converts minutes columns to float
      - Forces numeric types for core stats
      - Adds rolling averages and std dev
      - Computes differential features (team vs opponent)
      - Adds contextual features (home/away, streaks)
      - Returns X (features), y (target), and feature column names
    """
    df = convert_minutes(df)
    df = df.fillna(0)

    features = ["points", "reboundsTotal", "assists", "minutes"]
    df = force_numeric(df, features)

    # Ensure opponent stats are numeric
    for f in features:
        opp_col = f"{f}_OPP"
        if opp_col in df.columns:
            df[opp_col] = pd.to_numeric(df[opp_col], errors="coerce").fillna(0)

    # Add rolling features
    df = add_rolling_features(df, features)

    # Compute differential features
    for f in features:
        df[f"{f}_diff"] = df[f"{f}_rolling"] - df[f"{f}_rolling_OPP"]
        df[f"{f}_std_diff"] = df[f"{f}_std"] - df[f"{f}_std_OPP"]

    # Home/Away indicator
    if "HOME_TEAM_ID" in df.columns:
        df['is_home'] = (df['TEAM_ID'] == df['HOME_TEAM_ID']).astype(int)
    else:
        df['is_home'] = 0

    # Win/Loss streak features
    if "WIN" in df.columns:
        df['win_streak'] = df.groupby('TEAM_ID')['WIN']\
            .transform(lambda x: x.shift(1).rolling(ROLLING_WINDOW, min_periods=1).sum())
        df['loss_streak'] = df.groupby('TEAM_ID')['WIN']\
            .transform(lambda x: (1-x).shift(1).rolling(ROLLING_WINDOW, min_periods=1).sum())
    else:
        df['win_streak'] = 0
        df['loss_streak'] = 0

    # Feature columns
    feature_cols = []
    for f in features:
        feature_cols += [f"{f}_diff", f"{f}_std_diff"]
    feature_cols += ['is_home', 'win_streak', 'loss_streak']

    # Create feature matrix and labels
    X = df[feature_cols].fillna(0)
    y = df["WIN"].astype(int) if "WIN" in df.columns else pd.Series([0]*len(df))
    return X, y, feature_cols

# TRAIN MODEL
def train_model():
    """
    Trains an XGBoost classifier on historical games.
    Evaluates performance on test set and saves model.
    """
    df = load_historical_games()
    X, y, feature_cols = prepare_features(df)

    # Time-based train/test split (80/20)
    split = int(len(X) * 0.8)
    X_train, X_test = X.iloc[:split], X.iloc[split:]
    y_train, y_test = y.iloc[:split], y.iloc[split:]

    # XGBoost model configuration
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

    # Evaluate model
    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]

    print("\n--- Model Evaluation ---")
    print(f"Accuracy: {accuracy_score(y_test, y_pred):.4f}")
    print(f"ROC-AUC : {roc_auc_score(y_test, y_proba):.4f}")
    print("Confusion Matrix:\n", confusion_matrix(y_test, y_pred))

    # Save model to disk
    with open(MODEL_PATH, "wb") as f:
        pickle.dump({"model": model, "features": feature_cols}, f)
    print(f"\nModel saved to {MODEL_PATH}\n")

    return model

# MAIN EXECUTION
if __name__ == "__main__":
    # Run training when script is executed directly
    train_model()
