class SqlQueries:
    artists_table_create = """
        DROP TABLE IF EXISTS public.artists;
        
        CREATE TABLE public.artists (
            artist_id varchar(256) NOT NULL,
            name varchar(256),
            location varchar(256),
            latitude numeric(18,0),
            longitude numeric(18,0)
        )
    """

    songplays_table_create = """
        DROP TABLE IF EXISTS public.songplays;
        
        CREATE TABLE public.songplays (
            playid varchar(32) NOT NULL,
            start_time timestamp NOT NULL,
            userid int4 NOT NULL,
            "level" varchar(256),
            songid varchar(256),
            artistid varchar(256),
            sessionid int4,
            location varchar(256),
            user_agent varchar(256),
            CONSTRAINT songplays_pkey PRIMARY KEY (playid)
        )
    """

    songs_table_create = """
        DROP TABLE IF EXISTS public.songs;
        
        CREATE TABLE public.songs (
            songid varchar(256) NOT NULL,
            title varchar(512),
            artistid varchar(256),
            "year" int4,
            duration numeric(18,0),
            CONSTRAINT songs_pkey PRIMARY KEY (songid)
        )
    """

    staging_events_table_create = """
        DROP TABLE IF EXISTS public.staging_events;
        
        CREATE TABLE IF NOT EXISTS public.staging_events (
            artist varchar(256),
            auth varchar(256),
            firstname varchar(256),
            gender varchar(256),
            iteminsession int4,
            lastname varchar(256),
            length numeric(18,0),
            "level" varchar(256),
            location varchar(256),
            "method" varchar(256),
            page varchar(256),
            registration numeric(18,0),
            sessionid int4,
            song varchar(256),
            status int4,
            ts int8,
            useragent varchar(256),
            userid int4
        )
    """

    staging_songs_table_create = """
        DROP TABLE IF EXISTS public.staging_songs;
        
        CREATE TABLE public.staging_songs (
            num_songs int4,
            artist_id varchar(256),
            artist_name varchar(512),
            artist_latitude numeric(18,0),
            artist_longitude numeric(18,0),
            artist_location varchar(512),
            song_id varchar(256),
            title varchar(512),
            duration numeric(18,0),
            "year" int4
        )
    """

    time_table_create = """
        DROP TABLE IF EXISTS public."time";
       
        CREATE TABLE public."time" (
            start_time timestamp NOT NULL,
            "hour" int4,
            "day" int4,
            week int4,
            "month" varchar(256),
            "year" int4,
            weekday varchar(256),
            CONSTRAINT time_pkey PRIMARY KEY (start_time)
        );  
    """

    users_table_create = """
        DROP TABLE IF EXISTS public.users;
        
        CREATE TABLE IF NOT EXISTS public.users (
            userid int4 NOT NULL,
            first_name varchar(256),
            last_name varchar(256),
            gender varchar(256),
            "level" varchar(256),
            CONSTRAINT users_pkey PRIMARY KEY (userid)
        )
    """

    songplay_table_insert = """
        INSERT INTO public.songplays (
            start_time,
            user_id,
            level,
            song_id,
            artist_id,
            session_id,
            location,
            user_agent
        )
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM public.staging_events
            WHERE page='NextSong') events
            LEFT JOIN public.staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """

    user_table_insert = """
        INSERT INTO public.users (
            userid,
            firstname,
            lastname,
            gender,
            level
        )
        SELECT distinct userid, firstname, lastname, gender, level
        FROM public.staging_events
        WHERE page='NextSong'
    """

    song_table_insert = """
        INSERT INTO public.songs (
            song_id,
            title,
            artist_id,
            year,
            duration
        )
        SELECT distinct song_id, title, artist_id, year, duration
        FROM public.staging_songs
    """

    artist_table_insert = """
        INSERT INTO public.artists (
            artist_id,
            artist_name,
            artist_location,
            artist_latitude,
            artist_longitude
        )
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM public.staging_songs
    """

    time_table_insert = """
        INSERT INTO public.time (
            start_time,
            hour,
            day,
            week,
            month,
            year,
            weekday
        )
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM public.songplays
    """
