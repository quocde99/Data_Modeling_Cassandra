# Project 2 - Data Modeling with Cassandra (Sparkify)
## Overview
This project provided with part of the ETL pipeline that transfers data from a set of CSV files within a directory to create a streamlined CSV file to model and insert data into Apache Cassandra tables.
Technologies: Python(pandas,cassandra).
## Project Structure
1. **create_db.py** create session, keyspace, tables.
2. **etl.py** reads and processes files from event data and loads them into your tables in cassandra.
3. **cql.py** contains sql query(drop,create and test query).
4. **event_data** contains 30 file csv same struct.
5. **event_datafile_new.csv** in process we combine data from event_data to this file.
6. **images** image struct data.
## Schema
Sparkify event database schema answers the first set of customer questions:
"What artist and song was listened during certain session?",
"What was the artist, song and user during certain session?"",
"What users have listened certain song?"
1. **songs_by_session**(sessionId,iteminSession,artist_name,song_name,song_length).
2. **artists_by_session**(userid,sessionid,iteminSession,artist_name,song,first_name_user,last_name_user).
3. **history_listen1**(song,userid,first_name_user,last_name_user) VALUES (%s, %s, %s, %s).
## How to use
- Install Python and Cassandra
- Run command line **python create_db.py**(create keyspace)
- Run command line **python etl.py**