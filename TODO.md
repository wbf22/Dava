# Todo
- Add constraint checking in marshalling service or somewhere else
- Change how indices are made
  - text values should have annotation to cause different indexing
- Handle updates
- Make server option
- Make distributed partitions option
- Make sql option interface
- Make master sql for table
- Implement Rollback operations
- Flyway type startup option
- Handle commas in text columns
- Handle edge cases near max table size
- Multiple table constraints, cascade etc...
- Have create and modified date for each row?
- Consider cleaning up table or logging something if empties file is huge
- Could be fun to make a little id generator

# ToTest
- queries without limit's or offsets
- Different modes more thoroughly 
- test every thing with multiple partitions
- use cache on all FileUtil operations

