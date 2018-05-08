# SQL_tools



Useful tools for analysing a SQL database with Pandas
If you're a data analyst or doing anything with a sql_database then this library is for you. 

Feature Sportlights:
Let you find WHICH column contains a word.
Match_ids from one table to another and tell you ALL the tables that have the id of the one you specify. 
See which columns overlap between two tables. 
Search for a table name
search for a coulmn name
Autocomplete table names ( If the name is unique enough )



## Simple How to :

#### python library to connect to database  
Google is your friend, you need to know what what sql server you are connecting to first. 

### HOW TO INSTALL  

git clone https://github.com/2yan/sql_tools.git  
cd sql_tools  
pip install .  


## Loading up and taking off 
import sql_tools

### THE QUICK EXAMPLE: 
MIRCROSFT SQL SERVER

> ip = sql_tools.IcePick(pypyodbc, driver = 'SQL Server', server = 'ServerName', database = 'DatabaseName')

### The Longer explanation.


Once you've gotten to this bit you need to import the appropriate database connection driver
I like to use psycopg2 for a postgresql server 
and Pypyodbc for Microsoft SQL Server.

 
You just pass that to the IcePickconstructor to get your IcePick object
sql_tools.IcePick( driverlibrary, arguments )
Arguments are positional arguments followed by keyword arguments. 
(Arguments refering to the arguments that the connection function takes. i.e (my_username, my_password,my_database, server = 'servername') 

once that's done, you're ready to go. 


## More explanation?
Basically you have some sort of library that handles connecting to the <whatever_flavor> SQL database, this set of tools requires you to already have that connecting with python to the datbase bit figured out, all you need to do is provide a connection to the database and ensure the Schema pulling works. The get_schema() function might have to be overwritten but is confirmed to be working with
postgresql and Microsoft sql server.

- the get_schema() function has to return a dataframe with two columns: table_name, and column_name, so if your database has a 
weird way of getting the schema then you just set sql_tools.get_schema = your_own_function_to_get_schema_information 
and everything should work. 



after that you have a very powerful library for doing things in a sql database:
samples: 
find_column_that_contains(table_name, word_to_find)

id_match(table_a, table_b)

print_overlapping

get_columns(table_name)

search_database( table_name)

search_labels( column_name )

get_all_tables()

gen_where(where_dict)



and more!

More complete examples with descriptions coming in at some point, for now you can explore the library by calling help( IcePick)
and help( IcePick.function ) where function is the function name you want help for; the majority of them have explanations. 

Final Note:
you probably want to ensure you have some understanding of pandas dataframes as this library relies heavily on them. 








