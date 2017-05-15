# SQL_tools
Useful tools for analysing a SQL database



##Simple How to :

Install ryan_tools.  ( `pip install https://github.com/2yan/ryan_tools` )

Install this library ( `pip install https://github.com/2yan/SQL_tools` )

you need to write up some sort of connection function:


`def connect_func():
	return sql_driver.connect('user', pass', 'database', 'table')`

then pass that function to ryan_sql with 
`set_connection(connect_func)`

once that's done, you're ready to go. 


Note: Currently sql_tools designed to import pypyodbc but DOES NOT need to do it if you're using some other connection driver, so either edit it out or install it to avoid the onload crash. 


# HOW DOES THIS WORK?

Basically you have some sort of library that handles connecting to the <whatever_flavor> SQL database, this set of tools requires you to already have that connecting with python to the datbase bit figured out, all you need to do is provide a connection to the database and ensure the Schema pulling works. get_schema() function might have to be overwritten.


the get_schema() function has to return a dataframe with two columns: table_name, and column_name



after that you have a very powerful library for doing things in a sql database:
samples: 

id_match(table_a, table_b)

print_overlapping

get_columns(table_name)

search_database( table_name)

search_labels( column_name )

get_all_tables()

gen_where(where_dict)

find_column_that_contains(table_name, word_to_find)

and more!

- More complete examples with descriptions coming in at some point, for now you can explore the library by calling help( sql_tools )
and help( sql_tools.function ) where function is the function name you want help for, the majority of them have explanations. 

Final Note:
you probably want to ensure you have some understanding of pandas dataframes as this library relies heavily on them. 








