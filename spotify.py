from spotipy.oauth2 import SpotifyClientCredentials
import spotipy
import pandas as pd
import matplotlib.pyplot as plt
import re
from os import listdir
import pymysql
import json
import time
from tqdm import tqdm

# Set up Client Credentials
sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
    client_id='********',  # Replace with your Client ID
    client_secret='*************'  # Replace with your Client Secret
))

# Creatr MySQL Database Connection& create a database
connection = pymysql.connect(
    host='****',
    user='*****', 
    password='********'  
)

cursor = connection.cursor()

# Create a Database (if not exists)
cursor.execute("CREATE DATABASE IF NOT EXISTS my_spotify_db")


cursor.execute("use my_spotify_db")
#cursor.execute("CREATE TABLE IF NOT EXISTS tracks(id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,trackName VARCHAR(255),artist VARCHAR(255),datetime DATETIME,mins_played INT)")

with open("./my_spotify_data/StreamingHistory_music_0.json",encoding="utf8") as file:
    data = json.load(file) 

# +
 # Insert data into MySQL
for record in data:
    insert_query = """
    INSERT INTO tracks (trackName, artist, datetime, mins_played)
    VALUES (%s, %s, %s, %s)
        """
    cursor.execute(insert_query, (
            record["trackName"],
            record['artistName'],
            record['endTime'],
            record['msPlayed']/60000))
    
connection.commit()


# -

# Fetching the data from the selected table using SQL query
RawData= pd.read_sql_query('''select * from tracks''', connection)
RawData

cursor.execute("ALTER TABLE tracks ADD COLUMN cleantrackName VARCHAR(255)")
cursor.execute("UPDATE tracks SET cleantrackName = REGEXP_REPLACE(trackName,'\\\\(From.*|- From.*|- .*','')")


Data= pd.read_sql_query('''select * from tracks''', connection)
Data.duplicated(subset = ["cleantrackName"])

cursor.execute("ALTER TABLE tracks ADD COLUMN track_id VARCHAR(255)")

# +
# ✅ Step 1: Fetch unique `cleanTrackName` and `artistName` using GROUP BY
cursor.execute("""
    SELECT cleantrackName, artist
    FROM tracks 
    WHERE track_id IS NULL 
    GROUP BY cleantrackName, artist
""")
unique_tracks = cursor.fetchall()  # Fetch only unique track names and artists

print(f"Unique tracks to process: {len(unique_tracks)}")

# +

# ✅ Step 2: Batch Processing (One API Call Per Track in Batches)
batch_size = 10  # Number of tracks processed in one batch
track_id_cache = {}  # Dictionary to store fetched track IDs

# Split tracks into batches
batches = [unique_tracks[i : i + batch_size] for i in range(0, len(unique_tracks), batch_size)]

for batch in tqdm(batches, desc="Fetching Track IDs", unit="batch"):
    update_values = []  # Store values for bulk update in MySQL

    for track in batch:
        track_name = track[0]
        artist_name = track[1]
        query = f"track:{track_name} artist:{artist_name}"
        cache_key = (track_name, artist_name)

        if cache_key in track_id_cache:
            spotify_track_id = track_id_cache[cache_key]  # Reuse cached track ID
        else:
            try:
                # 🔎 Query Spotify API (One Track at a Time)
                result = sp.search(q=query, type="track", limit=1)

                # 🎵 If a result is found, store track ID in cache
                if result["tracks"]["items"]:
                    spotify_track_id = result["tracks"]["items"][0]["id"]
                    track_id_cache[cache_key] = spotify_track_id  # Save to cache
                else:
                    spotify_track_id = None  # No track found

            except Exception as e:
                print(f"Error fetching track '{track_name}' by '{artist_name}': {e}")
                spotify_track_id = None

        # Store for bulk update
        update_values.append((spotify_track_id, track_name, artist_name))

    # ✅ Step 3: Bulk Update MySQL (All Processed Tracks at Once)
    cursor.executemany(
        "UPDATE tracks SET track_id = %s WHERE cleantrackName = %s AND artist = %s",
        update_values
    )
    connection.commit()  # Commit after batch update

    # 🕒 Sleep every 10 batch requests to avoid Spotify API rate limiting
    if len(track_id_cache) % (10 * batch_size) == 0:
        time.sleep(2)

# ✅ Step 4: Close database connection
cursor.close()
connection.close()

print("\n🎉 Batch fetching complete! Track IDs updated in MySQL. 🚀")
# -

Data= pd.read_sql_query('''select * from tracks''', connection)
Data

cursor.execute("ALTER TABLE tracks ADD COLUMN (album VARCHAR (255),popularity INT,duration FLOAT) ")

# +
cursor.execute("SELECT track_id FROM tracks WHERE track_id IS NOT NULL")
#track_detail = cursor.fetchall() this give tuple change that to list
track_ids = [row[0] for row in cursor.fetchall()]  # Convert to list

print(f"Tracks to process: {len(track_ids)}")
# -

# check a sample 
sp.track(Data['track_id'][24])

# +
batch_size = 50  # Spotify API supports max 50 track IDs per call
batches = [track_ids[i : i + batch_size] for i in range(0, len(track_ids), batch_size)]

for batch in tqdm(batches, desc="Fetching Album, duration & Popularity", unit="batch"):
    try:
        # 🔎 Query Spotify API for up to 50 track details
        result = sp.tracks(batch)  # Returns a dict, result["tracks"] is a list

        update_values = []  # Store values for bulk update in MySQL

        # ✅ Extract album and popularity from each track in result["tracks"]
        for track_data in result["tracks"]:
            track_id = track_data["id"]
            album = track_data["album"]["name"]
            popularity = track_data["popularity"]
            duration = track_data['duration_ms']

            # ✅ Store for bulk update
            update_values.append((album, popularity, duration, track_id))

        # ✅ Step 3: Bulk Update MySQL with album & popularity
        cursor.executemany(
            "UPDATE tracks SET album = %s, popularity = %s, duration = %s WHERE track_id = %s",
            update_values
        )
        connection.commit()  # Commit batch update

    except Exception as e:
        print(f"Error fetching batch: {e}")

    # 🕒 Sleep every 10 batch requests to avoid Spotify API rate limits
    if len(update_values) % (10 * batch_size) == 0:
        time.sleep(2)

# ✅ Step 4: Close database connection
cursor.close()
connection.close()

print("\n🎉 Album, Duration & Popularity data updated in MySQL! 🚀")
# -

df = pd.read_sql_query('''select * from tracks''', connection)
df


