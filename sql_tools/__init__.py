import pypyodbc
from datetime import datetime, timedelta
from ryan_tools import * 
import pandas as pd

username = None
password = None
database_ip = None

#These Values have to be set.

__server = None
__database = None



    
def get_connection():
    if __server == None or __database == None:
        raise Exception(' PLEASE SET SERVER AND DATABSE VALUES FIRST USING LOAD_CONFIG' )
    con = pypyodbc.connect(driver = 'SQL Server',server = __server , database = __database)
    return con

def connect():
    con = get_connection()
    return con.cursor()


def load_config( server = None, database = None, file_name = None):
    #Filename is will be depreciated at some point soon. Just use errver and database. 
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
    return phrase

def get_schema():
    # returns the schema of the databse
    schema = pd.read_sql('SELECT * FROM INFORMATION_SCHEMA.COLUMNS', get_connection() )
    return schema




def get_possible_table_joins( column_name ):
    # prints all overlapping column name in the two tables
    schema = get_schema()
    results = schema[schema['column_name'].str.contains(column_name, case = False )]
    return pd.DataFrame(results['table_name'].unique(), columns = ['table_name'])

def find_column_that_contains(table_name, find_me, exact = True):
    #searches for a column that contains a keyword or string. Warning: you need to add the % flags yourself. 
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
    #Returns the columns within a table
    table = complete_table_name(table)
    cur = connect()
    cur.execute('select * from ' + table)
    columns = []
    for item in cur.description:
        columns.append(item[0])
    return columns


def check_categorical(table, in_data = None, max_categories = 30):
    #This needs seaborn installed to work
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

def create_list_command(name, column, item_list):
	#creates a temporary list command to be joined with other commands
	if name.startswith('#') == False:
		name = '#' + name

	for num in range(0, len(item_list) ):
		item = item_list[num]
		if type(item) == str:
			item = '\'' + item '\''
		item_list[num] = ' select ' + str(item_list[num]) + ' as ' + column

	for thing in item_list:
		print(thing)
	first_command = str(item_list[0]) +' into ' + name
	item_list_2 = item_list[1:len(item_list)]
	other_commands = ' union '.join(item_list_2)
	return first_command + ' union ' + other_commands
    

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
        
def search_labels(word, exact = False):
    schema = get_schema()
    data = schema[schema['column_name'].str.contains(word, case = False )]
    if exact:
        data = data[data['column_name'].str.lower() == word.lower() ] 
    final = pd.DataFrame()
    final['table_name'] = data['table_name']
    final['column_name'] = data['column_name']
    final['data_type'] = data['data_type']
    return final
        

def print_data( table_name ,  column_name = '*', no = 50,):
    cur = connect()
    cur.execute('Select ' + column_name + ' from ' + table_name )
    for item in cur.fetchmany(no):
        print(item)
    cur.connection.close()
    

