

### Example

| OrderID | Total | OrderDate  |
|---------|-------|------------|
| 1       | $250  | 02/12/2020 |
| 2       | $80   | 02/15/2020 |
| 3       | $420  | 02/18/2020 |
| ..      | ..    | ..         |

If indecis have been made for the OrderID column and total column the folder structure would look like this:

```
orderTable
  |
  |--schema.sql
  |--indecis_partition0
  |     |--OrderID
  |     |    |--1
  |     |    |  |--1.csv
  |     |    |  '--23.header
  |     |    |--2
  |     |    |  |--2.csv
  |     |    |  '--12.header
  |     |    |--3
  |     |    |  |--3.csv
  |     |    |  '--15.header
  |     |--total
  |     |    |--1
  |     |    |  |--$250.csv
  |     |    |  '--5.header
  |     |    |--1
  |     |    |  |--$80.csv
  |     |    |  '--45.header
  |     |    |--1
  |     |    |  |--$420.csv
  |     |    |  '--2.header
  |--orderTable.csv
  '--orderTable.empties
```
The index csv files contain addresses to each line that has that value for that column. The header files contain a number which indicates the length of the index.csv file rows. Their name contains the number of rows in each index csv file. This can be used to optimize queries, in that the most restricting condition can be used to retrieve necessary data, and the rest can be applied as filters.
Each row of each index has the following syntax:
``` 
[line bytes]partition
```
Specifying the database partition and line number for the associated row. Line is the data type long so the first 8 bytes will be the long value for the line number. The rest of the string will be the partition name.

4 whitespace characters will be added to the partition name, so that the number of partitions can grow up to 9999. The partition name for orderTable will be as follows for subsequent partitions:
``` 
orderTable.csv
orderTable1.csv
orderTable2.csv
orderTable3.csv
...
orderTable9999.csv
```
Hopefully 10,000 partitions would be obscene for any normal use. The max size of a table is Long.MAX_VALUE or 9,223,372,036,854,775,807 bytes. (The java RandomAccess reader takes an offset in bytes which is a long type) 

A somewhat large row of data (with a few large text columns) could be ~2000 chars or 4000 bytes. So a table with rows of that size could hold 2,305,843,009,213,693 rows. With ten thousand partitions the entire 'table' could hold 23,058,430,092,136,930,000 rows which is about 23 quintillion rows. If each of the 10,000 partitions where completely filled up then you could theoretically store 92,233,720.368 petabytes of data. (obscene, larger than the internet 2023)

A single table could store 9223.37 petabytes of data. One table should be fine for any normal usage, but partitioning can add benefits in the terms of parallelization.





