{
"num_songs": 1,
"artist_id": "ARJIE2Y1187B994AB7",
"artist_latitude": null,
"artist_longitude": null,
"artist_location": "",
"artist_name": "Line Renaud",
"song_id": "SOUPIRU12A6D4FA1E1",
"title": "Der Kleine Dompfaff",
"duration": 152.92036,
"year": 0
}

song_data → transform
log_data → transform
log_json_path

song-data
song_id
title
duration
year
artist_id

artist-data
artist_id
name
location
latitude
longitude

log_data

fact_songplays
songplay_id
start_time
user_id
level
song_id
artist_id
session_id
location
user_agent

dim_users
user_id
first_name
last_name
gender
level

dim_songs
song_id
title
artist_id
year
duration

dim_artists
artist_id
name
location
latitude
longitude

dim_time
start_time
hout
day
week
month
year
weekday
