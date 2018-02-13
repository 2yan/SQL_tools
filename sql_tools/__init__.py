import ryan_tools as rt
import pandas as pd
import pickle

class IcePick():
    library = None
    method = None
    arguments = None
    keyword_arguments = None

    def __init__(self, library ,*args,  **kwargs):
        self.library = library
        self.method = library.connect
        self.arguments = args
        self.keyword_arguments = kwargs

    def get_connection(self):
        con = self.method(*self.arguments, **self.keyword_arguments)
        return con

    def get_cursor(self):
        '''returns a cursor. It should probably be called get_cursor()
         relies on the get_connection() function. '''
        con = self.get_connection()
        return con.cursor()

    def __construct_sql(self, column_dict = None, join_dict = None , where_dict = None ):
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

    def complete_table_name(self, phrase):
        if '[' in phrase:
            return phrase
        'Allows for incomplete table names to be typed in other parts of the code IF they are unique'
        search_phrase = '[' + phrase.lower() + ']'
        try:
            cur = self.get_cursor()
            cur.execute('SELECT * FROM ' + search_phrase)
            cur.close()
            return search_phrase
        except Exception as e:
            s = str(e)
        possible = []
        if phrase in s and 'Invalid object name' in s:
            possible = self.search_database(phrase)
            if len(possible)== 1:
                return '[' + possible[0] + ']'
        if len(possible) > 1:
            raise Warning('NO Single Match found for name. POssible Names =' +' , '.join(possible))
        return search_phrase

    def get_schema(self):
        ' returns the schema of the databse'
        schema = pd.read_sql('SELECT * FROM INFORMATION_SCHEMA.COLUMNS', self.get_connection() )
        return schema



    def id_match(self, table_a, table_b = None, __first__ = True):
        ''' Check this'''
        schema = self.get_schema()

        to_find_a = table_a + 'id'
        result = schema[schema['column_name'].str.lower() == to_find_a.lower()][['table_name', 'column_name']]
        result['start_id'] = table_a + '.' + 'id'
        result['end_id'] = result['table_name'] + '.' + result['column_name']
        result['start'] = table_a
        result['end'] = result['table_name']

        if type(table_b) != type(None):
            result = result[result['table_name'].str.lower() ==table_b]
            result = result[['start_id', 'end_id', 'start', 'end']]
            #result_end = table_b

            if __first__ == True:
                result_2 = self.id_match(table_b, table_a, False)
                result = result.append(result_2)

        result = result[['start_id', 'end_id', 'start', 'end']]
        return result



    def get_possible_table_joins(self,  column_name ):
        ' prints all overlapping column name in the two tables'
        schema = self.get_schema()
        results = schema[schema['column_name'].str.contains(column_name, case = False )]
        return pd.DataFrame(results['table_name'].unique(), columns = ['table_name'])

    def find_column_that_contains(self, table_name, find_me, exact = True):
        'searches for a column that contains a keyword or string. You can use % if you want to make non exact searches more specific.'
        result = []
        columns = self.get_columns(table_name)

        for column in columns:
            sql = 'SELECT TOP 1 [{}] FROM [{}] where [{}]'.format(column,table_name, column)
            
            if exact:
                sql = sql + " = '{}'".format(find_me) 

            if not exact:
                if '%' not in find_me:
                    find_me = '%{}%'.format(find_me)
                sql = sql + " like '{}'".format(find_me)
            try:
                check = self.read_sql(sql, allowed_failures= 0)
            except Exception:
                check = []
                pass
            if len(check) == 1:
                result.append(column)
        return result


    def get_columns(self, table):
        'Returns the columns within a table'
        table = self.complete_table_name(table)
        
        cur = self.get_cursor()
        cur.execute('select * from ' + table)
        columns = []
        for item in cur.description:
            columns.append(item[0].lower())
        return columns

    def describe_table(self, table):
        'Returns the columns within a table'
        table = self.complete_table_name(table)
        
        cur = self.get_cursor()
        cur.execute('select * from ' + table)
        description = pd.DataFrame(cur.description)
        return description
    
    
    
    def check_categorical(self, table, in_data = None, max_categories = 30):
        '''This needs seaborn installed to work, it's  function to explore and create graphs of categories within the chosen table.
         you can give in_data to make the computation faster, (IF you've allready pulled the data)'''
        import seaborn as sea
        data = in_data
        table = self.complete_table_name(table)
        columns = self.get_columns(table)
        for column in columns:
            if in_data == None:

                data = self.get_data(table, columns =  [column])

            length = len(data.groupby(column))
            if  length <= max_categories and length > 0 :
                print(column)
                print(data[column].value_counts())
                sea.countplot(x = column, data = data )
                sea.plt.show()
                print('_________________________________\n\n')

    def get_dictonary(self, listo_items ):
        'might be the same as dict(zip())?'

        diction = {}
        for item in listo_items:
            diction [ str(item[0]) ] = item[1]

        return diction

    def gen_where(self, where ):
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

    def get_data(self, table, columns = ['*'], where = '' , number = None ):
        ' Same as SELECT COLUMNS FROM TABLE, the where argument can be a dictionary with columns as keys and == values, or can just be a where statement'
        table = self.complete_table_name(table)
        columns = ' , '.join(columns)
        SQL = 'SELECT ' + columns + ' FROM ' + table + self.gen_where(where)

        cur = self.get_cursor()
        try:
            cur.execute(SQL )
        except Exception as e:

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

    def temp_list_str(self, name, item_list, cast_item = str):
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


    def get_all_tables(self):
        'returns a list of every table in the database'
        x = self.get_schema()['table_name'].unique()
        return x


    def print_header(self, table_name):
        'prints the header in a nice readable fashion. Use get_columns() to get a return value'
        table_name = self.complete_table_name(table_name)
        cur = self.get_cursor()
        cur.execute('select * from ' + table_name)

        for item in cur.description:
            print(rt.s_s(item[0], 50), item[1])

        cur.connection.close()


    def print_overlapping(self,table_1, table_2):
        'prints common column names between to tables'
        table_1 = self.complete_table_name(table_1)
        table_2 = self.complete_table_name(table_2)

        cur =self.get_cursor()
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

    def examine_column(self, column, tables):
        'Returns value counts in column name'
        total = {}
        for table in tables:
            if column.lower() in self.get_columns(table):
                x = self.get_data(table, [column])
                total[table] = x[column].value_counts()
        return total


    def search_database(self, word):
        'searches the whole database for tables'
        cur = self.get_cursor()
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

    def search_labels(self, word, tables = [] , exact = False):
        'searches for columns within the whole database of tables, specify tables argument to only look in tables containing a certian word.'

        schema = self.get_schema()
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


    def print_data(self,  table_name ,  column_name = '*', no = 50):
        'just print the header of some data really no biggie.'
        cur = self.get_cursor()
        table_name = self.complete_table_name(table_name)
        cur.execute('Select ' + column_name + ' from ' + table_name )
        for item in cur.fetchmany(no):
            print(item)
        cur.connection.close()


    def add_relationship(self, start, end, start_id, end_id):
        'This needs work don\'t use yet'
        start = start.lower()
        end = end.lower()
        start_id = start_id.lower()
        end_id = end_id.lower()
        try:
            relationships = pickle.load(open('relationships.dataframe', 'rb'))
            relationships.loc[len(relationships), ['start', 'end', 'start_id', 'end_id']] = [start, end, start_id, end_id]
            pickle.dump(relationships, open('relationships.dataframe', 'wb'))
        except FileNotFoundError:
            relationships = pd.DataFrame(columns = ['start', 'end', 'start_id', 'end_id'])
            relationships.loc[0, ['start', 'end', 'start_id', 'end_id']] = [start, end, start_id, end_id]
            pickle.dump(relationships, open('relationships.dataframe', 'wb'))
        return

    def get_relationships(self,start = None, end = None):
        'This needs work don\'t use yet'
        data = pickle.load(open('relationships.dataframe', 'rb'))
        if start != None:
            data = data[data['start'] == start.lower()]
        if end != None:
            data = data[data['end'] == end.lower()]
        return data

    def read_sql(self, sql, allowed_failures = 10):

        tries = 0
        while True:
                try:
                    return pd.read_sql(sql, self.get_connection())
                except self.library.DatabaseError as e:
                    print(sql)
                    tries = tries + 1
                    if tries > allowed_failures:
                        raise e
                    pass