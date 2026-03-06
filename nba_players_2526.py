from nba_api.stats.static import players
from nba_api.stats.endpoints import commonplayerinfo
import pandas as pd
import time

# Step 1: Get all active NBA players
all_players = players.get_active_players()

# Step 2: Create an empty list to store all player data
player_data = []

# Step 3: Loop through each player
for p in all_players:
    player_id = p['id']
    player_name = p['full_name']
    print(f"Fetching data for {player_name}...")

    try:
        # Step 4: Fetch player information
        info = commonplayerinfo.CommonPlayerInfo(player_id=player_id)
        df = info.get_data_frames()[0]

        # Step 5: Build a dictionary for this player’s information
        player_entry = {
            'PLAYER_ID': player_id,
            'PLAYER_NAME': player_name,
            'TEAM_NAME': df.loc[0, 'TEAM_NAME'],
            'POSITION': df.loc[0, 'POSITION'],
            'HEIGHT': "'" + str(df.loc[0,"HEIGHT"]),
            'WEIGHT': df.loc[0, 'WEIGHT'],
            'COUNTRY': df.loc[0, 'COUNTRY'],
            'D.O.B': df.loc[0, 'BIRTHDATE'],
            'DRAFT_CLASS': df.loc[0, 'DRAFT_YEAR'],
            'YEARS_IN_LEAGUE': int(df.loc[0, 'TO_YEAR']) - int(df.loc[0, 'FROM_YEAR']) + 1
        }

        # Step 6: Add this player’s record to the main list
        player_data.append(player_entry)

    except Exception as e:
        # Step 7: Handle any errors that occur (e.g., API rate limit, missing data)
        print(f"Error fetching data for {player_name}: {e}")

    # Step 8: Respect NBA API rate limit — avoid too many requests per second
    time.sleep(0.5)

# Step 9: Convert the full list to a pandas DataFrame
players_df = pd.DataFrame(player_data)

# Step 10: Save the DataFrame as a CSV file
players_df.to_csv("nba_players_2526.csv", index=False)

# Step 11: Confirm success
print("CSV file Created")
