"""
Author: De Pham 
Time start: 22/03/2022
Done: 24/03/2022
Declare list query (create database,table,drop table)
"""
####### drop table #######
drop_table_songs = "DROP TABLE IF EXISTS songs_by_session"
drop_table_artist = "DROP TABLE IF EXISTS artists_by_session"
drop_table_history = "DROP TABLE IF EXISTS history_listen1"
#### create table ######
create_keyspace = """CREATE KEYSPACE Udacity_music
WITH replication = {'class': ‘Simple Strategy’, 'replication_factor' : 1}"""
# create table
## table query1
create_song_tables = """
create table if not exists songs_by_session(
                            sessionId int,
                            iteminSession int,
                            artist_name text,
                            song_name text,
                            song_length float,
                            primary key(sessionId,iteminSession))"""
## table query 2
create_table_artist = """
create table if not exists artists_by_session(
                            userid int,
                            sessionid int,
                            iteminSession int,
                            artist_name text,
                            song text,
                            first_name_user text,
                            last_name_user text,
                            primary key((userid,sessionid),iteminSession));"""
##table  query 3
create_table_history = """
create table if not exists history_listen1(
                            song text,
                            userid int,
                            first_name_user text,
                            last_name_user text,
                            primary key(song,userid));"""
#### insert table ####
## insert table 1 
insert_table1 = """
INSERT INTO songs_by_session(sessionId,iteminSession,artist_name,song_name,song_length) VALUES (%s, %s, %s, %s, %s)
""" 
## insert table 2
insert_table2 = """
INSERT INTO artists_by_session(userid,sessionid,iteminSession,artist_name,song,first_name_user,last_name_user)
VALUES (%s, %s, %s, %s, %s, %s, %s);
"""
## insert table 3
insert_table3 = """
INSERT INTO history_listen1(song,userid,first_name_user,last_name_user) VALUES (%s, %s, %s, %s);
"""
### query check data
select_query1 = "SELECT artist_name, song_name, song_length FROM  songs_by_session where sessionId = 338 and itemInSession = 4"
select_query2 = "select artist_name,first_name_user,last_name_user from artists_by_session where userid = 10 and sessionid = 182"
select_query3 = "select first_name_user,last_name_user from history_listen1 where song='All Hands Against His Own'"
# query list
drop_database=[drop_table_songs,drop_table_artist,drop_table_history]
create_tables_queries=[create_song_tables,create_table_artist,create_table_history]
check_query=[select_query1,select_query2,select_query3]
