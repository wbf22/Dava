

# Database

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
  |--indecis_orderTable
  |     |--OrderID
  |     |    |--1.index
  |     |    |--2.index
  |     |    '--3.index
  |     |--Total
  |     |    |-- -250
  |     |    |    |--c.count
  |     |    |    '--80.index
  |     |    |--+250
  |     |    |    |--c.count
  |     |    |    |--250.index
  |     |    |    '--420.index
  |     |--OrderDate
  |     |    |--2020
  |     |    |    |--02/12/2020.index
  |     |    |    |--02/15/2020.index
  |     |    |    '--02/18/2020.index
  |--orderTable.csv
  |--orderTable.rowLengths
  '--orderTable.empties
```
## Indices
Indices are created for every column. They are basically a file that stores a row number for each value of that column in the table. For example, above there is an .index file for each value of OrderID. Those files store a row number for the table row that has the corresponding OrderID. 

<details>
<summary>How it works</summary>

---

Each .index is made up of appended bytes (representing rows in the table with that index value). Each 8 bytes represents a long which represents a row in the table:
``` 
[route 1, bytes][route 2, bytes]...
```
The index .index files contain addresses to each line that has that value for that column. You can calculate the number of indices by dividing the file size by 8. The index size can be used to optimize queries, in that the most restricting condition can be used to retrieve necessary data, and the rest can be applied as filters.

---
</details>


<details>
<summary>Warning</summary>

---

**Never modify indices** as Dava has a very specific structure and will fail if the expected index or values have been corrupted. Index files themselves are raw bytes which are parsed into longs. Any modification of those files will render indices useless and queries may return very strange values or fail.

---
</details>


### MODES
Database modes help customize a database setup for your use case. With our algorithms there is a trade-off between storage space and speed. Different modes can save space but cost more in query speeds.

When running a query each of the modes find the smallest exclusive index (a condition that must be fulfilled in query result) in the query and start with the result of that index as the initial subset of table rows.

<details>
<summary>Example</summary>

---

For example, if you have this query:

```sql
select * from my_table where name='John' and amount=7
```
If the index for 'name='John'' is smaller than for 'amount=7', then first all rows with a name of John will be retrieved. Those rows will then be filtered for where the amount is 7.

---
</details>

Using 'limit' or 'offset' can be used to cut down the size of the query, when the query has the potential to return a large number of rows.

<details>
<summary>How it Works</summary>

---
If 'limit' or 'offset' is used, then the operation goes until the offset and limit are fulfilled. This may involve doing multiple table reads but should be faster if the table is very large.

---
</details>

Here are the different modes for indices

- default
    + Indices are made for every column
- storage-sensitive
    + Every time a query is performed, an index is saved
- manual
    + Indices are only created on primary key and marked columns
- light
    + No indices or table meta files are maintained ( ideal for small tables / need to manually modify csv file )



Tables can be converted between modes, but this conversion can take a long time if the table is very large and lots of indices need to be created. 

<details>
<summary>How it Works</summary>

---
When converting, Dava determines if indices need to be created, and if so, a new table is created and each row of the old table is inserted using the new mode. This ensures all the necessary indices are created and the data remains the same.

---
</details>


### Special Indices
OrderId is an example of the standard approach for indices:
```
  |     |--OrderID
  |     |    |--1.index
  |     |    |--2.index
  |     |    '--3.index
```
Certain column types have special formats for their indices. 

Special Indices
- dates
- numbers
- text (optional)

'text' column will have a normal index unless it has a @TokenIndex annotation.

<details>
<summary>How it Works</summary>

---


### Text Indices
If specified with an @TokenIndex annotation, each row will be tokenized and indices will be created for each token. For example, storing the sentence 'The quick brown fox' will create the following indices:

``` 
  |     |--MyTextColumn
  |     |    |--The.index
  |     |    |--quick.index
  |     |    |--brown.index
  |     |    '--fox.index
```

If the sentence 'The quick blue cow' was added the existing indices would be added too and new indices would be created for the new words:

``` 
  |     |--MyTextColumn
  |     |    |--The.index
  |     |    |--quick.index
  |     |    |--brown.index
  |     |    |--blue.index
  |     |    |--fox.index
  |     |    '--cow.index
```



### Date Indices
Look like this
``` 
  |     |--OrderDate
  |     |    |--2020
  |     |    |    |--02/12/2020.index
  |     |    |    |--02/15/2020.index
  |     |    |    '--02/18/2020.index
```
Dates are converted to a time zone utc if applicable, and then a java LocalDate. They are then sorted into a year folder and the corresponding date index. Very large databases with million+ insertions per day might have some slowness if a query on dates is performed with before or after. However, if a database that large is partitioned it won't be as bad. (it's likely one that large would be partitioned)

We choose this design since reading all N indices from a single file is like 500x faster than reading N indicies from N files. (like how we do numbers below) Since dates are rarely accessed by specific values, but rather by before and after, it's probably more efficient to get dates this way.

### Numeric Indices
Look like this
``` 
  |     |--Total
  |     |    |-- -250
  |     |    |    |--c.count
  |     |    |    '--80.index
  |     |    |--+250
  |     |    |    |--c.count
  |     |    |    |--250.index
  |     |    |    '--420.index
```
Each time a new index is created it's sorted through the folder as either less than, or greater than or equal, to the folder value. Once the folder is too large (million+) then the folder is subdivided again. The .count file helps speed up the query and is used to check if the subdivision is too large.


---
</details>



## Table Partitions
Tables can be partitioned (made into two or more smaller tables) in order to split a large database across distributed servers. This can help query execution times as multiple machines can work on the same query in parallel. 

Tables can also be partitioned locally. This may or may not improve performance, though it's more likely too if the database is very large. Dava uses java's 'parallelStream()' stream method on every query operation using multiple threads to run the operation on partitions in parallel.


<details>
<summary>Details</summary>

---

The number of partitions can grow up from the initial table to partition 9999. The partition name for orderTable will be as follows for subsequent partitions:
``` 
orderTable.csv
orderTable1.csv
orderTable2.csv
orderTable3.csv
...
orderTable9999.csv
```

Each partition has it's own index folder which contains all indices for the partition named as follows:
```
indecis_[parition_name]
```

---
</details>



## Storage Sizes
A database can have up to 10,000 partitions, would be obscene for any normal use. The max size of a table is Long.MAX_VALUE or 9,223,372,036,854,775,807 bytes. (The java RandomAccess reader takes an offset in bytes which is a long type) 

A somewhat large row of data (with a few large text columns) could be ~2000 chars or 4000 bytes. So a table with rows of that size could hold 2,305,843,009,213,693 rows. With ten thousand partitions the entire 'table' could hold 23,058,430,092,136,930,000 rows which is about 23 quintillion rows. 

If each of the 10,000 partitions where completely filled up, then you could theoretically store 92,233,720.368 petabytes of data. (obscene, larger than the internet 2023)

A single table could store 9223.37 petabytes of data. One table should be fine for any normal usage, but partitioning can add benefits in the terms of parallelization. (marginal benefits or worse on same machine, much better on distributed servers)

## Empties File
If you notice the empties file above:
```
|--orderTable.csv
'--orderTable.empties
```
This file stores empty rows in the table. Whenever a row is deleted in the table csv file, the row is filled with whitespace. In order to reuse the space in the table, and index is appended to the empties file. When a value is inserted a random index in the empties will be retrieved (switching random 8 bytes with last 8 bytes and truncating file).

The first 8 bytes of the empties file contains the number of rows in the table. 



