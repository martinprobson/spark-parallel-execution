package net.martinprobson.example.spark.zio

import net.martinprobson.example.spark.common.SparkEnv
import org.apache.spark.sql.{DataFrame, SparkSession}
import zio.*
import zio.ZIOAspect.parallel

object ParallelWithZio extends ZIOApplication with SparkEnv {

  val PARALLEL = 100

  case class Result(tag: String, count: Long, df: DataFrame)

  def program: ZIO[SparkSession, Throwable, Unit] = {
        for {
          titlesDf <- ZIO.attempt(spark
            .read
            .json(getClass.getResource("/data/titles.json").getFile))
          _ <- ZIO.attempt(titlesDf.createTempView("titles"))
          _ <- ZIO.attempt(titlesDf.cache()) *> ZIO.logInfo("Here I am!")
          employeesDf <- ZIO.attempt(spark
            .read
            .json(getClass.getResource("/data/employees.json").getFile))
          _ <- ZIO.attempt(employeesDf.createTempView("employees"))
          _ <- ZIO.attempt(employeesDf.cache())
          r <- ZIO.foreachPar(functions) { func => func(spark).either } @@ parallel(PARALLEL)
          _ <- ZIO.attempt(r.foreach {
                    case Right(df) => ZIO.logInfo(s"Result is $df")
                    case Left(ex) =>
                       ZIO.logError(s"Failed with exception: ${ex.toString} - ${ex.getMessage}")
                } )
          _ <- ZIO.logInfo("DONE!")
       } yield ()
  }
  override def run: Task[Unit] = program.provide(SparkLayerLive.layer)

  val functions: List[SparkSession => Task[DataFrame]] = {
    List(
      s => runQuery(s,"select distinct title from titles"),
      s => runQuery(s,"select t.title,e.birth_date from " +
        "titles t join employees e on e.birth_date = t.from_date"),
      s => runQuery(s,"select distinct title from titles"),
      s => runQuery(s,"select t.title,e.birth_date from " +
        "titles t join employees e on e.birth_date = t.from_date"),
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
