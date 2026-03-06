import sqlite3

# Create a fresh database
conn = sqlite3.connect("nba_data.db")
cursor = conn.cursor()

# Create the table
cursor.execute("""
CREATE TABLE IF NOT EXISTS team_games (
    game_id TEXT PRIMARY KEY,
    game_date TEXT,
    team_id INTEGER,
    team_name TEXT,
    matchup TEXT,
    WL TEXT,
    points INTEGER
)
""")

conn.commit()
conn.close()

print("✅ SQLite DB and table created successfully!")
