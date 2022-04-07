"""
Author: De Pham 
Time start: 22/03/2022
Done: 24/03/2022
create database
"""
from cassandra.cluster import Cluster
from cql import drop_database,create_tables_queries

def create_database(server):
    """
    create session 
    input server (example : '127.0.0.1')
    return session
    """
    cluster = Cluster([server])
    # To establish connection and begin executing queries, need a session
    session = cluster.connect()
    # create keyspace
    session.execute("DROP KEYSPACE IF EXISTS udacity_music")
    create_keyspace = """
    CREATE KEYSPACE IF NOT EXISTS udacity_music
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
    """
    session.execute(create_keyspace)
    #set keyspace
    session.set_keyspace('udacity_music')
    print('Connect session success !')
    return session,cluster
def drop_table(session):
    """
    Drops each table using the queries in `drop_database` list.
    INPUTS: session variable connect database
    """
    print("Drop table")
    for query in drop_database:
        session.execute(query)
        print('Drop table sucess !')

def create_tables(session):
    """
    Creates each table using the queries in `create_tables_queries` list. 
    INPUTS: session variable connect database
    """
    print("Create table")
    for query in create_tables_queries:
        session.execute(query)
        print('Create table sucess !')
def main():
    """
    - create session
    - create keyspace
    - drop database
    - create tables
    """
    session,cluster = create_database('127.0.0.1')
    drop_table(session)
    create_tables(session)
    session.shutdown()
    cluster.shutdown()
if __name__=="__main__":
    main()