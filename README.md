# Common Spark GraphX Implementations

## Note to user
`
You can clone this project, and publishLocal to your .ivy2 cache.
Then, pull this project in as a dependency. You'll need to pull in Spark, as well.
`

### Connected Components
* Classic Graph Algo
    * GraphX has a ready-made implementation
    * however, the type-signature (return type) can be unclear to first-time users
    * we simplify the return type, along with explanation & tests  