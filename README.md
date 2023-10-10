# Dava
Java managed database. 

For the intended benefits:
- db can be incorporated naturally into an app without ports or internet calls (faster database access, no need for libraries)
- fast complex queries
- schema database
- ability to run scripts to load data
- ability to run tests
- sql style syntax
- transaction management
- distribute across servers
- encryption at rest

Less suitable use cases
- You intend the database to be accessed by more than one app
- if your hardware is limiting, running an app and database on the same machine may impact performance if the database is very large.
- you want a schemaless database


## How it works

### Tables
Tables are stored in csv files. Most database tables should have just one csv file but the max length in characters of a table is the max value of java long (due to RandomAccessFile.java for io writes). If a table has more than that length it will be partitioned into two tables. Tables can also be partitioned with a configuration if you'd like to distribute the tables accross multiple servers. 

Each table has an indexes folder with indexes generated for that table.

The schema for the table is defined in a class annotated with @Table. This is translated into an internal definition of the table written in sql. Manual updates to this file will cause schema changes The columns are inferred from the fields of the class.


### Indexes
Each table has an indexes folder with a table for each value of each column that has been queried recently. For example, say you have this table:

| OrderID | Total  | OrderDate  |
|---------|--------|------------|
| 1       | $250   | 02/12/2020 |
| 2       | $80    | 02/15/2020 |
| 3       | $420   | 02/18/2020 |

If you only access by 'OrderID (the primary key) you will not have any generated indexes for this table. However if you query by 'Total' or by 'OrderDate' an index will be generated for that item so queries like that are faster in the future. You can specify on your table whether you'd prefer to have indexes always made for certain columns, otherwise they'll only be created when needed.


### Nested Objects
Nested objects such as 'product' in this 'order' object:
```json
{
  "orderId" : 1,
  "total": 40,
  "orderDate": "02/19/2022",
  "product": {
    "name": "Jumbo Jelly Bean",
    "price": 40
  }
}
```
are always stored in a separate table. If an @Table object has a nested object that is not a standard java type (some custom object), it must also have an @Table annotation. 

If part of an transaction fails then changes to objects will be undone.






### Notes