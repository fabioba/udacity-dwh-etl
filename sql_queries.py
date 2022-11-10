"""
This module includes the queries used to create the tables

Author: Fabio Barbazza
Date: Nov, 2022
"""
import configparser


# read config dwh
config = configparser.ConfigParser()
config.read('config/dwh.cfg')

# queries to drop table
songs_table_drop = "DROP TABLE IF EXISTS songs"
songplays_table_drop = "DROP TABLE IF EXISTS songplays"
users_table_drop = "DROP TABLE IF EXISTS users"
artists_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"


# queries to create tables
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id bigint IDENTITY(0,1) PRIMARY KEY,
        start_time timestamp NOT NULL,
        user_id VARCHAR NOT NULL, 
        level VARCHAR NOT NULL, 
        song_id VARCHAR NOT NULL, 
        artist_id VARCHAR NOT NULL, 
        session_id VARCHAR NOT NULL, 
        location VARCHAR NOT NULL, 
        user_agent VARCHAR NOT NULL
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id VARCHAR NOT NULL PRIMARY KEY, 
        first_name VARCHAR NOT NULL, 
        last_name VARCHAR NOT NULL, 
        gender VARCHAR NOT NULL, 
        level VARCHAR NOT NULL
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id VARCHAR NOT NULL PRIMARY KEY, 
        title VARCHAR NOT NULL, 
        artist_id VARCHAR NOT NULL, 
        year INT NOT NULL, 
        duration FLOAT NOT NULL
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id VARCHAR NOT NULL PRIMARY KEY, 
        name VARCHAR NOT NULL, 
        location VARCHAR NOT NULL, 
        latitude FLOAT NOT NULL, 
        longitude FLOAT NOT NULL
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        time_id bigint IDENTITY(0,1) PRIMARY KEY, 
        start_time timestamp NOT NULL, 
        hour int NOT NULL, 
        day int NOT NULL, 
        week int NOT NULL, 
        month int NOT NULL, 
        weekday VARCHAR NOT NULL
    )
""")

# staging tables
staging_songs_table_create= ("""
    CREATE TABLE IF NOT EXISTS songs_staging(
        songplay_id VARCHAR NOT NULL PRIMARY KEY,
        start_time timestamp NOT NULL,
        user_id VARCHAR NOT NULL, 
        level VARCHAR NOT NULL, 
        song_id VARCHAR NOT NULL, 
        artist_id VARCHAR NOT NULL, 
        session_id VARCHAR NOT NULL, 
        location VARCHAR NOT NULL, 
        user_agent VARCHAR NOT NULL
    )
""")

staging_logs_table_create = ("""
    CREATE TABLE IF NOT EXISTS logs_staging(
        songplay_id VARCHAR NOT NULL PRIMARY KEY,
        start_time timestamp NOT NULL,
        user_id VARCHAR NOT NULL, 
        level VARCHAR NOT NULL, 
        song_id VARCHAR NOT NULL, 
        artist_id VARCHAR NOT NULL, 
        session_id VARCHAR NOT NULL, 
        location VARCHAR NOT NULL, 
        user_agent VARCHAR NOT NULL
    )
""")

staging_events_copy = ("""
copy logs_staging from {} 
credentials 'aws_iam_role={}'
gzip region 'us-east-1';
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'])

staging_songs_copy = ("""
copy songs_staging from {}
credentials 'aws_iam_role={}'
gzip region 'us-east-1';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# fact table
songplay_table_insert = ("""
    INSERT INTO songplays(
            select 
                logs_staging.registration,
                logs_staging.userId,
                logs_staging.level,
                songs_staging.song_id,
                songs_staging.artist_id,
                logs_staging.sessionId,
                logs_staging.location,
                logs_staging.userAgent
            from logs_staging
            left join songs_staging
            on logs_staging.artist=song.artist_name
        ) on conflict (userId) do nothing
""")

user_table_insert = ("""
    INSERT INTO users(
        select 
            userId,
            firstName,
            lastName,
            gender,
            level
        from logs_staging
        qualify row_number() over(partition by userId)=1
    ) on conflict (userId) do nothing
""")


song_table_insert = ("""
    INSERT INTO songs(
        select 
            song_id,
            title,
            artist_id,
            year,
            duration
        from songs_staging
    ) on conflict (song_id) do nothing
""")

artist_table_insert = ("""
    INSERT INTO artists(
        select 
            artist_id,
            artist_name,
            artist_location,
            artist_latitude,
            artist_longitude
        from songs_staging
    ) on conflict (artist_id) do nothing
""")

time_table_insert = ("""
    INSERT INTO time(
        select 
            start_time,
            hour,
            day,
            week,
            month,
            weekday,
            logs_staging
        from songs_staging
    ) on conflict (time_id) do nothing
""")

# QUERY LISTS
create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, staging_songs_table_create, staging_logs_table_create]
drop_table_queries = [songs_table_drop, songplays_table_drop, users_table_drop, artists_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
