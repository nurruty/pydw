# pydw

This package is aimed to help the coding of ETL scripts as stored procedures. Writing the code as a sequence of steps, the pakcage will return a script written in SQL syntax wich will execute the desired logic.
Wrapping the syntax of each specific DBMS, the same python transformation can be excecuted in several DBs using SQL-Server, Oracle, MySQL, etc.

## Version 1.0
This is te first version of the package. 
* Only SQL Server syntax is supported. 
* No useful tests yet
* LoadingTables, Selecting/Filtering values, Joining Tables is supported.
* Specific logic of Slowly Changing Dimensions of types 1 and 2 is supported.
