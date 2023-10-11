

### Example

| OrderID | Total | OrderDate  |
|---------|-------|------------|
| 1       | $250  | 02/12/2020 |
| 2       | $80   | 02/15/2020 |
| 3       | $420  | 02/18/2020 |
| ..      | ..    | ..         |

If indices have been made for the OrderID column and total column the folder structure would look like this:

```
orderTable
  |
  |--schema.sql
  |--indecis_partition0
  |     |--OrderID
  |     |    |--1.index
  |     |    |--2.index
  |     |    '--3.index
  |     |--total
  |     |    |--$250.index
  |     |    |--$80.index
  |     |    '--$420.index
  |--orderTable.csv
  |--orderTable.rowLengths
  '--orderTable.empties
```
The index .index files contain addresses to each line that has that value for that column. You can calculate the number of indices by dividing the file size by 8. This can be used to optimize queries, in that the most restricting condition can be used to retrieve necessary data, and the rest can be applied as filters.
The index is made up of appended bytes. Each 8 bytes represents a long which represents a row in the table:
``` 
[route 1 bytes][route 2 bytes]...
```

The number of partitions can grow up to 9999. The partition name for orderTable will be as follows for subsequent partitions:
``` 
orderTable.csv
orderTable1.csv
orderTable2.csv
orderTable3.csv
...
orderTable9999.csv
```
Hopefully 10,000 partitions would be obscene for any normal use. The max size of a table is Long.MAX_VALUE or 9,223,372,036,854,775,807 bytes. (The java RandomAccess reader takes an offset in bytes which is a long type) 

A somewhat large row of data (with a few large text columns) could be ~2000 chars or 4000 bytes. So a table with rows of that size could hold 2,305,843,009,213,693 rows. With ten thousand partitions the entire 'table' could hold 23,058,430,092,136,930,000 rows which is about 23 quintillion rows. 

If each of the 10,000 partitions where completely filled up, then you could theoretically store 92,233,720.368 petabytes of data. (obscene, larger than the internet 2023)

A single table could store 9223.37 petabytes of data. One table should be fine for any normal usage, but partitioning can add benefits in the terms of parallelization.

If you notice the empties file above:
```
|--orderTable.csv
'--orderTable.empties
```
This file stores empty rows in the table. Whenever a row is deleted in the table csv file the row is filled with whitespace. In order to reuse the space in the table, and index is appended to the empties file. When a value is inserted a random index in the empties will be retrieved (switching random 8 bytes with last 8 bytes and truncating file).

The first 8 bytes of the empties file contains the number of rows in the table. 


