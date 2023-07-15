
Spark Parallel Execution
===================================================

Experiments around executing spark queries in parallel.

Three methods are used here: -

* [ParallelWithFutures](src/main/scala/net/martinprobson/example/spark/future/ParallelWithFutures.scala) uses standard Scala Futures and callbacks.

* [ParallelWithCats](src/main/scala/net/martinprobson/example/spark/cats/ParallelWithCats.scala) uses [Cats Effect](https://typelevel.org/cats-effect/) library to 
wrap the spark context in a [Resource](https://typelevel.org/cats-effect/docs/std/resource) and `parTraverseN` to control level of parallelism.
  
* [ParallelWithZIO](src/main/scala/net/martinprobson/example/spark/zio/ParallelWithZio.scala) uses [ZIO](https://zio.dev/) to wrap the spark context in a [ZIO Scope](https://zio.dev/reference/resource/scope) and use `foreachPar` to run the queries in parallel. The [ZIOAspect.parallel](https://zio.dev/api/zio/ZIOAspect$.html#parallel(n:Int):zio.ZIOAspect[Nothing,Any,Nothing,Any,Nothing,Any]) aspect controls the level of parallelism.

## ParallelWithFutures
Uncomment the line : -
```scala
    // Uncomment the following line to pause the code and allow the Spark UI to be viewed 
//scala.io.StdIn.readLine()
```
to view the Spark UI.

## ParallelWithCats/ParallelWithZIO
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

## ToDo
1. Lift the SparkEnv config into a ZIO layer also. See [zio.config](src/main/scala/net/martinprobson/example/spark/zio/config) package.
