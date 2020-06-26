# spark-base-impl
A Basic/Template Implementation of Spark-Base

### Building a Spark-Base project
`spark-base` does not have a major release yet. It is still in `1.0.0-SNAPSHOT`.
For now, you could `git clone git@github.com:MrMizz/spark-base.git` locally, and simply `sbt publishLocal`. 
Once you have `spark-base` in your local `ivy2` cache, you can pull it into your next Spark project.
    
Then you can assemble your implementation of `spark-base` with `sbt assembly`.
This will produce a fat jar, containing all of your `src/main/` and external dependencies in one place.
A fat jar makes deploying a spark-based app to a cluster, or even locally, much easier. There's no need to
specify a list of dependencies to the `--jars` argument in `spark-submit`.

### Running locally
You can submit a job like this    
```aidl
spark-submit \
--class in.tap.base.spark.impl.Main \
target/scala-2.11/spark-base-impl-assembly-1.0.0-SNAPSHOT.jar \
--step basic \
--in1 src/test/resources/basic/in/ \
--in1-format json \
--out1 src/test/resources/basic/out \
--out1-format json
```