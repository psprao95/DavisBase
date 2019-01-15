## Supported Commands
All commands are case insensitive;

USE DATABASE database_name;                      Changes current database
CREATE DATABASE database_name;                   Creates an empty database
SHOW DATABASES;                                  Displays all databases
DROP DATABASE database_name;                     Deletes a database
SHOW TABLES;                                     Displays all tables in current database
DESC table_name;                                 Displays table schema
CREATE TABLE table_name (                        Creates a table in current database
        column_name> <datatype> [PRIMARY KEY | NOT NULL]");
DROP TABLE table_name;                           Deletes a table data and its schema
SELECT <column_list> FROM table_name             Display records whose rowid is <id>
        [WHERE rowid = <value>];");
INSERT INTO table_name                           Inserts a record into the table
        [(<column1>, ...)] VALUES (<value1>, <value2>, ...);
DELETE FROM table_name [WHERE condition];        Deletes a record from a table
UPDATE table_name SET <conditions>                     Updates a record from a table
        [WHERE condition];");
VERSION;                                         Display current database engine version
HELP;                                            Displays help information
EXIT;                                            Exits the program
