# Projects
wish.c - Wish is a basic shell written in c which takes many of the same classical commands as bash (ie. 'cat test.txt' or 'ls') and preforms
the expected behavior by making system calls such as fork() and exec(). To accomplish this, Wish searches through paths in order
to find the specified executable.

nqueens.py - nqueens is written in python and, given an arbitrary board size n and a static point, utilizes a hill climbing algorithm
to return the board state where n queens are in positions where they are not able to attack one another.

BPIndex.cpp - BPIndex, written in C++, is a grouping of functions which build and maintain a B+ tree implementation for a database index. 
BPIndex contains the necessary functions to insert and scan a range for key-value pairs, and splits leaf and non-leaf nodes when they have reached 
capacity. Delete() is not included as it was not part of the project specifications.
