# Todo MVP
- handle transactions in the repository class (look for annotation)
- figure out how to include java docs in deploy
- build jar and deploy to github packages
- try top level query cache instead
- Add constraint checking in marshalling service or somewhere else
- Multiple table constraints, cascade etc...


# Todo
- Change how indices are made
  - text values should have annotation to cause different indexing
    - could do text values with a embedding style thing
  - support LIKE queries
- Storage Sensitive mode (make indices based on usage)
- Make server option
- Make distributed partitions option
- Make sql option interface
- Make master sql for table
- Flyway type startup option
- Handle edge cases near max table size
- Have create and modified date for each row?
- Consider cleaning up table or logging something if empties file is huge
- Spatial Queries for geo locations?
- graph based database?
- handle partitioning when table is too large
- allow table names to have schema levels (products.product instead of just product)
- make a message queue set up
- make a cache (redis like) set up

# Done
- consider sorting dates like numeric values using timestamps
- Add transactions. Pass a value in inserts or updates to tell database to append rollback strings instead of overwriting
- On restart after crash, scan numeric partitions for any partially complete repartitions and finish the work
- Also log numeric rollback stuff to a seperate file
- get rid of fileutil cache (slower)
- cache won't work currently with concurrency
- handle partial lines in rollbacks
- ensure rollback of partial writes work
- combine Insert and delete operations into batch class
- Handle updates
- Implement Rollback operations



# ToTest
- test every thing with multiple partitions
- table annotation with or without name
