import cx_Oracle
from datetime import datetime, timedelta
from ryan_tools import *
import ryan_tools
import pandas as pd

username = None
password = None
database_ip = None
def connect():
    con = cx_Oracle.connect(username,password,database_ip)
    return con.cursor()

def get_dictonary( listo_items ):

    diction = {}
    for item in listo_items:
        diction [ str(item[0]) ] = item[1]
        
    return diction


def get_all_tables():
    cur = connect()
    cur.execute('select table_name from all_tables')
    for items in cur:
        print(items)
    cur.connection.close()


def print_header(table_name):
    cur = connect()
    cur.execute('select * from ' + table_name)
    for item in cur.description:
        print(s_s(item[0], 50), item[1])
    cur.connection.close()


def print_overlapping(table_1, table_2):
    cur = connect()
    cur.execute('select * from ' + table_1)
    one = []
    for item in cur.description:
        one.append(item)
    cur.execute('select * from ' + table_2)
    
    two = []
    for item in cur.description:
        two.append(item)
    for item in one:
        for item2 in two:
            if item[0] == item2[0]:
                print( item )

    cur.connection.close()



def search_database(word):
    cur = connect()
    cur.execute('select table_name from all_tables')
    names = []
    for items in cur:
        names.append(items)
    for name in names:
        if word.lower() in name[0].lower():
            print(name)
    cur.connection.close()        
        
def search_labels(word):
    cur = connect()
    cur.execute('select table_name from all_tables')
    names = []
    for items in cur:
        names.append(items)
    for name in names:
        
        try:
         cur.execute('select * from ' + name[0])
        except cx_Oracle.DatabaseError:
            continue

        
        keys = []
        for item in cur.description:
            keys.append([s_s(item[0], 50), item[1]])
        for key in keys:
            if word.lower() in key[0].lower():
                print('Table:       ' , name)
                print('Coluumn:     ' , key)
                print('\n')
    cur.connection.close()
            
        

def print_data( table_name ,  column_name = '*', no = 50,):
    cur = connect()
    cur.execute('Select ' + column_name + ' from ' + table_name )
    for item in cur.fetchmany(no):
        print(item)
    cur.connection.close()

    
