
Spark Parallel Execution
===================================================

Experiments around executing spark queries in parallel.

Two methods are used here: -

* [ParallelWithFutures](src/main/scala/net/martinprobson/example/spark/ParallelWithFutures.scala) uses standard Scala Futures and callbacks.

* [ParallelWithCats](src/main/scala/net/martinprobson/example/spark/ParallelWithCats.scala) uses [Cats Effect](https://typelevel.org/cats-effect/) library to 
wrap the spark context in a [Resource](https://typelevel.org/cats-effect/docs/std/resource) and `parTraverseN` to control level of parallelism.

## ParallelWithFutures
Uncomment the line : -
```scala
    // Uncomment the following line to pause the code and allow the Spark UI to be viewed 
//scala.io.StdIn.readLine()
```
to view the Spark UI.

## ParallelWithCats
Note: Use method `closeSparkSessionWithPause` to keep the Spark UI active.

### Example Spark Job Output with parTraverseN(1)

![Screenshot](parTraverse-0.png)

### Example Spark Job Output with parTraverseN(10)

![Screenshot](parTraverse-1.png)

## Notes

### Running on Java 17+
Add the following to Java options to run : -
```
--add-exports java.base/sun.nio.ch=ALL-UNNAMED
```
