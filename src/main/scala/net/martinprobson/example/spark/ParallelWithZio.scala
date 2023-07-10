package net.martinprobson.example.spark


import zio.*
import zio.ZIOAspect.parallel
import org.apache.spark.sql.{DataFrame, SparkSession}

object ParallelWithZio extends ZIOApplication with SparkEnv {

  val PARALLEL = 100

  case class Result(tag: String, count: Long, df: DataFrame)

  def openSparkSession: Task[SparkSession] = ZIO.logInfo("Opening spark session")
    *> ZIO.attempt(spark)
  def closeSparkSessionWithPause(sparkSession: SparkSession): Task[Unit] =
    ZIO.logInfo(s"closing sparkSession") *> ZIO.attempt(scala.io.StdIn.readLine()) *> ZIO.attempt(
      sparkSession.close()
    )
  def closeSparkSession(sparkSession: SparkSession): Task[Unit] =
    ZIO.logInfo(s"closing sparkSession") *> ZIO.attempt(sparkSession.close())

  def closeSparkSession2: SparkSession => ZIO[Any, Nothing, Unit] = s =>
    ZIO.logInfo(s"closing sparkSession") *> ZIO.attempt(s.close()).catchAll(_ => ZIO.unit)

  val scopedSparkSession: ZIO[Any with Scope, Throwable, SparkSession] = ZIO.acquireRelease(openSparkSession)(closeSparkSession2)

  override def run: Task[Unit] = {
    ZIO.scoped {
      scopedSparkSession.flatMap{ spark =>
        for {
          titlesDf <- ZIO.attempt(spark.read.json(getClass.getResource("/data/titles.json").getFile))
          _ <- ZIO.attempt(titlesDf.createTempView("titles"))
          _ <- ZIO.attempt(titlesDf.cache()) *> ZIO.logInfo("Here I am!")
          employeesDf <- ZIO.attempt(spark.read.json(getClass.getResource("/data/employees.json").getFile))
          _ <- ZIO.attempt(employeesDf.createTempView("employees"))
          _ <- ZIO.attempt(employeesDf.cache())
          r <- ZIO.foreachPar(functions) { func => func(spark).either } @@ parallel(PARALLEL)
          _ <- ZIO.attempt(r.foreach {
                    case Right(df) => ZIO.logInfo(s"Result is ${df}")
                    case Left(ex) =>
                       ZIO.logError(s"Failed with exception: ${ex.toString} - ${ex.getMessage}")
                } )
          _ <- ZIO.logInfo("DONE!")
       } yield ()
      }

    }
  }

//  override def runold: IO[Unit] =
//    Resource.make(openSparkSession)(sparkSession => closeSparkSession(sparkSession)).use { spark =>
//      for {
//        titlesDf <- IO(spark.read.json(getClass.getResource("/data/titles.json").getFile))
//        _ <- IO(titlesDf.createTempView("titles"))
//        _ <- IO(titlesDf.cache())
//        employeesDf <- IO(spark.read.json(getClass.getResource("/data/employees.json").getFile))
//        _ <- IO(employeesDf.createTempView("employees"))
//        _ <- IO(employeesDf.cache())
//        r <- functions.parTraverseN(PARALLEL) { func => func(spark).attempt }
//        _ <- IO(r.foreach {
//          case Right(df) => logger.info(s"Result is ${df}")
//          case Left(ex) =>
//            logger.error(s"Failed with exception: ${ex.toString} - ${ex.getMessage}")
//        })
//        _ <- IO(logger.info("DONE!"))
//      } yield ()
//    }

  val functions: List[SparkSession => Task[DataFrame]] = {
    List(
      s => runQuery(s,"select distinct title from titles"),
      s => runQuery(s,"select t.title,e.birth_date from titles t join employees e on e.birth_date = t.from_date"),
      s => runQuery(s,"select distinct title from titles"),
      s => runQuery(s,"select t.title,e.birth_date from titles t join employees e on e.birth_date = t.from_date"),
      s => runSlowQuery(s,"select distinct title from titles"),
      s => runQuery(s,"select distinct title from titles"),
      s => runQuery(s,"select distinct title from titles"),
      s => runQuery(s,"A load of rubbish"),
      s => runSlowQuery(s,"select distinct title from titles"),
      s => runQuery(s,"select * from employees"),
      s => runQuery(s,"select * from titles")
    )
  }

  def runQuery(session: SparkSession, query: String): Task[DataFrame] = for {
    _ <- ZIO.logInfo(s"In runQuery $query")
    df <- ZIO.attempt(session.sql(query))
    c <- ZIO.attempt(df.count())
    _ <- ZIO.logInfo(s"Count = $c")
  } yield df

  def runSlowQuery(session: SparkSession, query: String): Task[DataFrame] = for {
    _ <- ZIO.logInfo(s"In runSlowQuery $query")
    _ <- ZIO.sleep(10.seconds)
    df <- ZIO.attempt(session.sql(query))
    _ <- ZIO.attempt(df.count())
  } yield df
}
