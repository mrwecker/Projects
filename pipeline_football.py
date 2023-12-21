import pandas as pd
import requests
import json
from datetime import datetime
import os
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# Function to get player difficulty data from API 1
def get_player_stats(player_id):
    url = f'https://fantasy.premierleague.com/api/element-summary/{player_id}/'
    response = requests.get(url)

    data = response.json().get('fixtures', []) 

#only selected the 3 columns from this API that I need to aggregate later on. The player, gameweek and difficulty score 
    fixtures_data = [
        {
            'event_name': fixture.get('event_name', None),
            'difficulty': fixture.get('difficulty', None),
        }
        for fixture in data
    ]

    return pd.DataFrame({
        'PlayerID': [player_id] * len(fixtures_data),
        'EventName': [fixture['event_name'] for fixture in fixtures_data],
        'Difficulty': [fixture['difficulty'] for fixture in fixtures_data],
    })

# LOAD API 2 to get player, team and events data. Lots of data here, needs to be cut down
url = 'https://fantasy.premierleague.com/api/bootstrap-static/'
response = requests.get(url)
response = json.loads(response.content)

players = response['elements']
teams = response['teams']
events = response['events']
players_df = pd.DataFrame(players)
teams_df = pd.DataFrame(teams)
events_df = pd.DataFrame(events)

#clean up the deadline_time column. Ended up not needed, but worth it being done for future analysis. 
events_df['gameweek_date'] = pd.to_datetime(events_df['deadline_time']).dt.date
events_df = events_df.drop(columns=['deadline_time'])

#join players and team to have a clearer picture of players. Need to see team data together
player_teams_data = pd.merge(left=players_df, right=teams_df, how='left', left_on='team', right_on='id', suffixes=('_player', '_team'))

#there were 120+ columns so I just chose and organised the columns I wanted for easier readability and referencing. Probs an easier way but suited me
selected_columns_nums = [14, 12, 21, 8, 18, 93, 58, 80, 13, 15, 16, 0, 20, 82, 26, 27, 28, 33, 34, 37, 38, 39, 52, 54, 55, 56, 64, 72, 74, 102, 103]
selected_df = player_teams_data.iloc[:, selected_columns_nums]

# Fetch player difficulty data from first API. Since there is 1 url and 600+ players, loop is needed to gather them all together
player_ids = players_df["id"]
all_player_data = []

for player_id in player_ids:
    player_stats = get_player_stats(player_id)
    
    if player_stats is not None:
        all_player_data.append(player_stats)

# add all the player data into a single DataFrame
difficulty_df = pd.concat(all_player_data, ignore_index=True)

# DataFrames to CSV files as I had issues with sqlite3 -_-. Didn't allow me to work with dicts and lists. 
# in the end, just used python, but will used sql for quick referencing
events_df.to_csv("events_data.csv", index=False)
players_df.to_csv("players_data.csv", index=False)
teams_df.to_csv("teams_data.csv", index=False)
selected_df.to_csv("selected_player_teams_data.csv", index=False)
difficulty_df.to_csv("player_difficulty_data.csv", index=False) 


# Load environment variables from .env file
load_dotenv("snowflake.env")

# Snowflake credentials
user = os.getenv("SNOWFLAKE_USER")
password = os.getenv("SNOWFLAKE_PASSWORD")
account = os.getenv("SNOWFLAKE_ACCOUNT")
warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
database = os.getenv("SNOWFLAKE_DATABASE")
schema = os.getenv("SNOWFLAKE_SCHEMA")

# Connect to Snowflake
con = snowflake.connector.connect(
    user=user,
    password=password,
    account=account,
    database=database,
    schema=schema
)

# Upload DataFrames to Snowflake. Didn't exactly need this, but it was much easier to do an sql query for quick refs
write_pandas(con, events_df, "EVENTS_DATA", auto_create_table=True)
write_pandas(con, players_df, "PLAYERS_DATA", auto_create_table=True)
write_pandas(con, teams_df, "TEAMS_DATA", auto_create_table=True)
write_pandas(con, selected_df, "SELECTED_PLAYER_TEAMS_DATA", auto_create_table=True)
write_pandas(con, selected_df, "PLAYER_DIFFICULTY_DATA", auto_create_table=True)

#closed the connection
if con:
    con.close()