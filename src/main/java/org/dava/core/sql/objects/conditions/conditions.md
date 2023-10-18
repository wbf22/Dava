# Conditions
Supported conditions
- Equals
- Null
- GreaterThan
- LessThan
- After
- Before
- In
- Like

Conditions rely on indices to be applied quickly. Depending on the mode of the database
they are either used as filters, or converted to indices to find data.


## MODES
Each of these modes are applied on >= 1,000,000 lines at a time in rounds until the whole table/index is processed.

If 'limit' or 'offset' is used, then the operation goes until the offset and limit are fulfilled.


### default
Indices are created for every column

Steps:
- all conditions are converted to table indices
- smallest index of each 'AND' is accessed and returned
- conditions are applied on result as filters


### storage-sensitive
Every time a query is performed, an index is saved

Steps:
- if any condition has an index then it's return; otherwise all rows are returned
- conditions are applied on result as filters



### manual
Indices are only created on primary key and marked columns

Steps:
- if any condition has an index then it's return; otherwise all rows are returned
- conditions are applied on result as filters

