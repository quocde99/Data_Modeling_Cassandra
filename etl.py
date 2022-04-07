"""
Author: De Pham 
Time start: 22/03/2022
Done: 24/03/2022
etl process
"""
from cassandra.cluster import Cluster
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv
from cql import *
from cql import check_query
def process_event_data(session,file):
    """
    process event data to 3 table in cassandra 
    INPUTS:
    * session: variable session
    * file: csv file
    """
    with open(file, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
            #insert data
            session.execute(insert_table1,(int(line[8]), int(line[3]), line[0], line[9], float(line[5])))
            session.execute(insert_table2,(int(line[10]),int(line[8]),int(line[3]), line[0],line[9],line[1],line[4]))
            session.execute(insert_table3,(line[9],int(line[10]),line[1],line[4]))    
        print("Success load data to Cassandra !")

def test_query(session,query_list):
    """
    test query check data loaded to Cassandra
    INPUTS: 
    *session:variable session
    *query_list: query check data
    """
    for query in query_list:
        print("Query",query_list.index(query)+1)
        rows= session.execute(query)
        for row in rows:
            print(row)
def process_data(filepath):
    """
    This procedure processes a csv file whose filepath has been provided as an arugment.
    INPUTS: 
    * filepath the file path to the event data file
    """
    for root, dirs, files in os.walk(filepath):
        # join the file path and roots with the subdirectories using glob
        file_path_list = glob.glob(os.path.join(root,'*'))
        print('{} files found in {}'.format(len(file_path_list), filepath))
    # initiating an empty list of rows that will be generated from each file
    full_data_rows_list = [] 
    # for every filepath in the file path list 
    for f in file_path_list:
        with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
            # creating a csv reader object 
            csvreader = csv.reader(csvfile) 
            next(csvreader)
            # extracting each data row one by one and append it     
            for line in csvreader:
                full_data_rows_list.append(line) 
            
    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)
    with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                'level','location','sessionId','song','userId'])
        for row in full_data_rows_list:
            if (row[0] == ''):
                continue
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))
    with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
        print(sum(1 for line in f),"Rows")
    print("Success process data to csv file !")
def main():
    """
    - connect the database
    - process event database
    """
    process_data(filepath='/home/workspace/event_data')
    # connect database
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    session.set_keyspace('udacity_music')
    # load data to csv file
    process_event_data(session,"event_datafile_new.csv")
    # check data
    test_query(session,check_query)
    print("___Done___ !")
    session.shutdown()
    cluster.shutdown()
if __name__=="__main__":
    main()