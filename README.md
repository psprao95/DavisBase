## Supported Commands
All commands are case insensitive;

USE DATABASE database_name;                      Changes current database.

CREATE DATABASE database_name;                   Creates an empty database.

SHOW DATABASES;                                  Displays all databases.

DROP DATABASE database_name;                     Deletes a database.

SHOW TABLES;                                     Displays all tables in current database.

DESC table_name;                                 Displays table schema.

CREATE TABLE table_name ( column_name> <datatype> [PRIMARY KEY | NOT NULL] )                    Creates a table in current database
        
        
DROP TABLE table_name;                           Deletes a table data and its schema.
        
SELECT <column_list> FROM table_name  [WHERE rowid = <value>];           Display records whose rowid is <id>
        
        
INSERT INTO table_name    [(<column1>, ...)] VALUES (<value1>, <value2>, ...);                        Inserts a record into the table.
       
        
DELETE FROM table_name [WHERE condition];        Deletes a record from a table

UPDATE table_name SET <conditions>  [WHERE condition];           Updates a record from a table
       
        
VERSION;                                         Display current database engine version.

HELP;                                            Displays help information.

EXIT;                                            Exits the program
