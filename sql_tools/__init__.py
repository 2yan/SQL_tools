import pypyodbc
from datetime import datetime, timedelta
from ryan_tools import * 
import pandas as pd

username = None
password = None
database_ip = None

#These Values have to be set, you can use load_config to do so. 
__server = None
__database = None



    
def get_connection():
    '''Theoretically this can be overwritten to use any sql library
    returns a connection Object often used by pandas.read_sql()
    Nice and convenient. '''
    if __server == None or __database == None:
        raise Exception(' PLEASE SET SERVER AND DATABSE VALUES FIRST USING LOAD_CONFIG' )
    con = pypyodbc.connect(driver = 'SQL Server',server = __server , database = __database)
    return con

def connect():
    '''returns a cursor. It should probably be called get_cursor()
     relies on the get_connection() function. '''
    con = get_connection()
    return con.cursor()


def load_config( server = None, database = None, file_name = None):
    'Filename will be depreciated at some point soon. Just use server and database. '
    global __server
    global __database
    if file_name != None:
        config = pd.read_csv(file_name, index_col = 'keys' )
        __server = config.loc['server', 'values']
        __database = config.loc['database', 'values']
        return
    if (server != None) and (database != None):
        __server = server
        __database = database
    
def construct_sql(column_dict = None, join_dict = None , where_dict = None ):
    'This is a work in progress'
    SQL = ''

    if column_dict != None:
        SQL = SQL + ' SELECT '
        cols = [] 
        for key in column_dict.keys(): 
            for column in column_dict[key]:
                cols.append(key+ '.' + column)
        SQL = SQL +' '+  ','.join( cols)
        
    
    return SQL
def complete_table_name(phrase):
    'Allows for incomplete table names to be typed in other parts of the code IF they are unique'
    search_phrase = '[' + phrase + ']'
    try:
        cur = connect()
        x = cur.execute('SELECT * FROM ' + search_phrase)
        cur.close()
        return phrase
    except Exception as e:
        s = str(e)
    possible = []
    if phrase in s and 'Invalid object name' in s:
        possible = search_database(phrase)
        if len(possible)== 1:
            return possible[0]
    if len(possible) > 1:            
        raise Warning('NO Single Match found for name. POssible Names =' +' , '.join(possible))
    return search_phrase

def get_schema():
    ' returns the schema of the databse'
    schema = pd.read_sql('SELECT * FROM INFORMATION_SCHEMA.COLUMNS', get_connection() )
    return schema


def id_match(table_a, table_b, __second = None ):
    results = []
    if __second == None:
        x = id_match( table_b, table_a, 'Second' )
        if len(x) != 0:
            results.append(x)
    if 'id' in get_columns(table_a):
        for column in get_columns(table_b):
            if column.lower() == table_a.lower() + 'id':
                results.append({table_a+'.id': table_b + '.' + column})

    return results

def get_possible_table_joins( column_name ):
    ' prints all overlapping column name in the two tables'
    schema = get_schema()
    results = schema[schema['column_name'].str.contains(column_name, case = False )]
    return pd.DataFrame(results['table_name'].unique(), columns = ['table_name'])

def find_column_that_contains(table_name, find_me, exact = True):
    'searches for a column that contains a keyword or string. Warning: you need to add the % flags yourself. '
    cur = connect()
    table_name = complete_table_name( table_name)
    columns = get_columns(table_name)
    result = []
    if exact:
        if type(find_me) == str:
            find_me ='\'' + find_me + '\''
        for column in columns:
            where = table_name + '.' +  column + '=' + find_me 

            try:
                sql = 'SELECT ' + column + ' From ' + table_name + ' WHERE '+ where 
                data = pd.read_sql( sql, get_connection() )
                if len(data) > 0:
                    result.append(column)
            except pd.io.sql.DatabaseError:
                pass
    if not exact:
        if type(find_me) == str:
            find_me ='\'' + find_me.lower() + '\''
        for column in columns:
            where = 'lower(' + table_name + '.' +  column +') ' +  ' like ' + find_me
            try:
                sql = 'SELECT ' + column + ' From ' + table_name + ' WHERE '+ where
                cur.execute(sql)
                data = cur.fetchone()
                if data != None:
                    result.append(column)
            except:
                pass
    return result


def get_columns(table):
    'Returns the columns within a table'
    table = complete_table_name(table)
    cur = connect()
    cur.execute('select * from ' + table)
    columns = []
    for item in cur.description:
        columns.append(item[0].lower())
    return columns


def check_categorical(table, in_data = None, max_categories = 30):
    '''This needs seaborn installed to work, it's  function to explore and create graphs of categories within the chosen table.
     you can give in_data to make the computation faster, (IF you've allready pulled the data)'''
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
            
def get_dictonary( listo_items ):
    'might be the same as dict(zip())?'

    diction = {}
    for item in listo_items:
        diction [ str(item[0]) ] = item[1]
        
    return diction

def gen_where(where ):
    SQL = ''
    if type(where) == str:
        return where
    
    if where != {}:
        for key in where.keys():
            if 'WHERE' not in SQL:
                WHERE = ' WHERE ' + key + ' = ' + '\'' + where[key] + '\''
                SQL = SQL + WHERE
            else:
                WHERE = ' AND ' + key + ' = ' + '\'' + where[key] + '\''
                SQL = SQL + WHERE
    return SQL 
    
def get_data(table, columns = ['*'], where = '' , number = None ):
    ' Same as SELECT COLUMNS FROM TABLE, the where argument can be a dictionary with columns as keys and == values, or can just be a where statement'
    table = complete_table_name(table)
    columns = ' , '.join(columns)
    SQL = 'SELECT ' + columns + ' FROM ' + table + gen_where(where)
                    
    cur = connect()
    try:
        cur.execute(SQL )
    except pypyodbc.ProgrammingError as e:

        print( 'SQL: ' + SQL )
        raise(e)
        
    columns = []
    for item in cur.description:
        columns.append(item[0])
    if number != None:
        data = pd.DataFrame( cur.fetchmany(number), columns = columns)
    if number == None:
        data = pd.DataFrame( cur.fetchall(), columns = columns)
    return data

def temp_list_str(name, item_list, cast_item = str):
    '''Creates a temporary list command to be joined with other commands
    [Confirmed working Mircosoft SQL Server 2008 + and postgresql 9.5.2+]
     If it works on your platform, reach out to me and I will add it to these notes. 2yan@outlook.com'''
    first_command = '(values '

    for num in range(0, len(item_list) ):
        item = item_list[num]
        item = cast_item(item)
        if type(item) == str:
            item = '\'' + item +  '\''
        item_list[num] = '(' + str(item) + ')'

    other_commands = ','.join(item_list)
    return first_command + other_commands + ') v(' + name + ')'
    

def get_all_tables():
    'returns a list of every table in the database'
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
    'prints the header in a nice readable fashion. Use get_columns() to get a return value'
    table_name = complete_table_name(table_name)
    cur = connect()
    cur.execute('select * from ' + table_name)
    
    for item in cur.description:
        print(s_s(item[0], 50), item[1])
        
    cur.connection.close()


def print_overlapping(table_1, table_2):
    'prints common column names between to tables'
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
    'searches the whole database for tables'
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
        
def search_labels(word, tables = [] , exact = False):
    'searches for columns within the whole database of tables, specify tables argument to only look in tables containing a certian word.'
    
    schema = get_schema()
    data = schema[schema['column_name'].str.contains(word, case = False )]
    if exact:
        data = data[data['column_name'].str.lower() == word.lower() ] 
    final = pd.DataFrame()
    final['table_name'] = data['table_name']
    final['column_name'] = data['column_name']
    final['data_type'] = data['data_type']
    if len(tables) > 0:
        final = final[final['table_name'].str.contains('|'.join(tables), case = False)]
    return final
        

def print_data( table_name ,  column_name = '*', no = 50,):
    cur = connect()
    cur.execute('Select ' + column_name + ' from ' + table_name )
    for item in cur.fetchmany(no):
        print(item)
    cur.connection.close()
    


