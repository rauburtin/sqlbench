# sqlbench

A very simple python program that can simulate hundred of users on a simple server.

The program executes processes. Each process represents a user.
Each process execute a list of scenarios. A scenario is a list of tasks.
Each task can execute several sql statements.

The program is submitted with a bunch of sample sql statements working on top of a sample sqlite database.

The excution time of each task (bunvh of sql statements) is written, at the end, to a sqlite database.

Use the filltables.py program to create and populate the sample database.

Modify the sqlbench.py to change the ramp of processes and run it

You can create an Excel file to analyse the results from the result database.
An exemple (results_from_db_samples.xlsx) is provided. Install an ODBC driver for sqlite to use it.