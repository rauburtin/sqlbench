# sqlbench

A very simple python program that can simulate hundred of users on a simple server.

The program executes processes. Each process represents a user.
Each process execute a list of scenarios. A scenario is a list of tasks.
Each task can execute several sql statements.

The program is submitted with a bunch of sample sql statements working on top of a sample sqlite database.

Use the filltables.py program to create and populate the sample database.

Modify the sqlbench.py to change the ramp of processes and run it