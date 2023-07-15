package net.martinprobson.example.spark.cats

import cats.effect.implicits.*
import cats.effect.{IO, IOApp, Resource}
import net.martinprobson.example.spark.common.{Logging, SparkEnv}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.concurrent.duration.DurationInt

object ParallelWithCats extends IOApp.Simple with Logging with SparkEnv {

  val PARALLEL = 100

  case class Result(tag: String, count: Long, df: DataFrame)

  def openSparkSession: IO[SparkSession] = IO(logger.info("Opening spark session")) >> IO(spark)
  def closeSparkSessionWithPause(sparkSession: SparkSession): IO[Unit] =
    IO(logger.info(s"closing sparkSession")) >> IO(scala.io.StdIn.readLine()) >> IO(
      sparkSession.close()
    )
  def closeSparkSession(sparkSession: SparkSession): IO[Unit] =
    IO(logger.info(s"closing sparkSession")) >> IO(sparkSession.close())

  override def run: IO[Unit] =
    Resource.make(openSparkSession)(sparkSession => closeSparkSession(sparkSession)).use { spark =>
      for {
        titlesDf <- IO(spark.read.json(getClass.getResource("/data/titles.json").getFile))
        _ <- IO(titlesDf.createTempView("titles"))
        _ <- IO(titlesDf.cache())
        employeesDf <- IO(spark.read.json(getClass.getResource("/data/employees.json").getFile))
        _ <- IO(employeesDf.createTempView("employees"))
        _ <- IO(employeesDf.cache())
        r <- functions.parTraverseN(PARALLEL) { func => func(spark).attempt }
        _ <- IO(r.foreach {
          case Right(df) => logger.info(s"Result is $df")
          case Left(ex) =>
            logger.error(s"Failed with exception: ${ex.toString} - ${ex.getMessage}")
        })
        _ <- IO(logger.info("DONE!"))
      } yield ()
    }

  val functions: List[SparkSession => IO[DataFrame]] = {
    List(
      s => runQuery(s,"select distinct title from titles"),
      s => runQuery(s,"select t.title,e.birth_date " +
        "from titles t join employees e on e.birth_date = t.from_date"),
      s => runQuery(s,"select distinct title from titles"),
      s => runQuery(s,"select t.title,e.birth_date " +
        "from titles t join employees e on e.birth_date = t.from_date"),
      s => runSlowQuery(s,"select distinct title from titles"),
      s => runQuery(s,"select distinct title from titles"),
      s => runQuery(s,"select distinct title from titles"),
      s => runQuery(s,"A load of rubbish"),
      s => runSlowQuery(s,"select distinct title from titles"),
      s => runQuery(s,"select * from employees"),
      s => runQuery(s,"select * from titles")
    )
  }

  def runQuery(session: SparkSession, query: String): IO[DataFrame] = for {
    _ <- IO(logger.info(s"In runQuery $query"))
    df <- IO(session.sql(query))
    _ <- IO(df.count())
  } yield df

  def runSlowQuery(session: SparkSession, query: String): IO[DataFrame] = for {
    _ <- IO(logger.info(s"In runSlowQuery $query"))
    _ <- IO.sleep(10.seconds)
    df <- IO(session.sql(query))
    _ <- IO(df.count())
  } yield df
}
