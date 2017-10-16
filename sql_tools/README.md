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
### Dependancies:

#### ryan_tools:

git clone https://github.com/2yan/ryan_tools.git
cd ryan_tools
pip3 install .


#### pandas
pip3 install pandas

#### python library to connect to database
Google is your friend, you need to know what what sql server you are connecting to first. 

### HOW TO INSTALL
sql_tools
https://github.com/2yan/SQL_tools.git
cd sql_tools
pip3 install .


## Loading up and taking off 
import sql_tools as st

Once you've gotten to this bit you need to import the appropriate database connection driver
I like to use psycopg2 for a postgresql server 
and Pypyodbc for Microsoft SQL Server.

Your connection function is just driverlibrary.connect
with 'driverlibrary' being whatever python library you're using to connect. 

then you just pass that to set_connection_method( driverlibrary.connect, arguments )
Arguments are positional arguments followed by keyword arguments. 
(Arguments refering to the arguments that the connection function takes. i.e (my_username, my_password,my_database, server = 'servername') 


once that's done, you're ready to go. 



## HOW DOES THIS WORK?

Basically you have some sort of library that handles connecting to the <whatever_flavor> SQL database, this set of tools requires you to already have that connecting with python to the datbase bit figured out, all you need to do is provide a connection to the database and ensure the Schema pulling works. The get_schema() function might have to be overwritten but is confirmed to be working with
postgresql and Microsoft sql server.


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

More complete examples with descriptions coming in at some point, for now you can explore the library by calling help( sql_tools )
and help( sql_tools.function ) where function is the function name you want help for; the majority of them have explanations. 

Final Note:
you probably want to ensure you have some understanding of pandas dataframes as this library relies heavily on them. 








