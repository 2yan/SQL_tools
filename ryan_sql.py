import pypyodbc
from datetime import datetime, timedelta
import pandas as pd
from ryan_tools import * 
username = None
password = None
database_ip = None


def complete_table_name(phrase):
    search_phrase = '[' + phrase + ']'
    try:
        cur = connect()
        x = cur.execute('SELECT * FROM ' + search_phrase)
        cur.close()
        return phrase
    except Exception as e:
        s = str(e)
    if phrase in s and 'Invalid object name' in s:
        possible = search_database(phrase)
        if len(possible)== 1:
            return possible[0]
                
    raise KeyError('NO Single Match found for name. POssible Names =' +' , '.join(possible))




        

def get_config():
    config = pd.read_csv('Config.csv', index_col = 'keys' )
    server = config.loc['server', 'values']
    database = config.loc['database', 'values']
    return server, database 
server, database = get_config()


def get_columns(table):
    table = complete_table_name(table)
    cur = connect()
    cur.execute('select * from ' + table)
    columns = []
    for item in cur.description:
        columns.append(item[0])
    return columns



def check_categorical(table, in_data = None, max_categories = 30):
    data = in_data
    table = complete_table_name(table)
    columns = get_columns(table)
    for column in columns:
        if in_data == None:

            data = get_data(table, columns =  [column])

        length = len(data.groupby(column))
        if  length <= max_categories and length > 0 :
            print(column)
            print(data[column].value_counts())
            sea.countplot(x = column, data = data )
            sea.plt.show()
            print('_________________________________\n\n')
            
    
def get_connection():
    con = pypyodbc.connect(driver = 'SQL Server',server = server , database = database)
    return con

def connect():
    con = pypyodbc.connect(driver = 'SQL Server',server = server , database = database)
    return con.cursor()



def get_dictonary( listo_items ):

    diction = {}
    for item in listo_items:
        diction [ str(item[0]) ] = item[1]
        
    return diction
 
    
def get_data(table, columns = ['*'], where = None , number = None ):
    table = complete_table_name(table)
    columns = ' , '.join(columns)
    sql = 'SELECT ' + columns + ' From ' + table
    
        
    if where != None:
        sql = sql + ' WHERE ' + where

    cur = connect()
    try:
        cur.execute(sql )
    except pypyodbc.ProgrammingError as e:

        print( 'SQL: ' + sql )
        raise(e)
        
    columns = []
    for item in cur.description:
        columns.append(item[0])
    if number != None:
        data = pd.DataFrame( cur.fetchmany(number), columns = columns)
    if number == None:
        data = pd.DataFrame( cur.fetchall(), columns = columns)
    return data


def get_all_tables():
    cur = connect()
    cur.execute('SELECT * FROM sys.Tables')
    x = pd.DataFrame()
    i = 0
    for items in cur:
        x.loc[i, 'name'] = items[0]
        i = i + 1
    cur.connection.close()
    return x


def print_header(table_name):
    table_name = complete_table_name(table_name)
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
    cur.execute('SELECT * FROM sys.Tables')
    names = []
    results = []
    for items in cur:
        names.append(items)
    for name in names:
        if word.lower() in name[0].lower():
            results.append(name[0])
    cur.connection.close()
    return results
        
def search_labels(word):
    cur = connect()
    cur.execute('SELECT * FROM sys.Tables')
    names = []
    for items in cur:
        names.append(items)
    for name in names:
        
        try:
            cur.execute('select * from ' +'['+ name[0] + ']')
        except pypyodbc.ProgrammingError:
            pass
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

server, database = get_config()
cur =connect()
con = get_connection()
