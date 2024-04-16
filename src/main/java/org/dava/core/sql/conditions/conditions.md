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

The different modes apply conditions differently:

#### default
Query Steps:
- all conditions are converted to table indices
- smallest index of each 'AND' is accessed and returned
- conditions are applied on result as filters

#### storage-sensitive
Query Steps:
- if any condition has an index then it's returned; otherwise all rows are returned
- conditions are applied on result as filters

#### manual
Query Steps:
- if any condition has an index then it's returned; otherwise all rows are returned
- conditions are applied on result as filters

#### light
Query Steps:
- get all rows
- apply conditions as filters


