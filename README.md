This project designs and implements an efficient multi-column indexing and search algorithm for Hbase, the Hadoop big data store. It builds indexes for multiple columns of a table, and search the table for any combination of equality of these columns in O(log(n)) time, where n is the number of rows in the table.

The project provides a library with APIs to create, insert to and search indexed tables. It also provides two command line scripts for users to interactively perform these tasks.

The indexing and search algorithm in this project can support certain range search on the indexed columns, but cannot support all possible combinations of equality and range conditions yet. For now range search is not implemented in this project.
