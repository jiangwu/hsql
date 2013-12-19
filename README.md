This project provides efficient multi-column equality search for hbase. For any search based on multiple indexed columns, it takes O(log(n)) time where n is the number of row in the table.
The core of this project is an index creating and query algorithm. For m index column, the algorithm generates 2^(m-1) indexes. For any of the 2^m combination of search conditions, the query algorithm searches the indexes with time complexity O(log(n)).
The project also supports OR in the search condition. 
The project provides two command line scripts. admin.sh is used to create and delete tables with indexes, and console.sh is used to insert and search the indexed tables.

