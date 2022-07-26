package net.martinprobson.example.spark

import cats.effect.implicits.*
import cats.effect.{IO, IOApp, Resource}
import org.apache.spark.sql.{DataFrame, SparkSession}

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
        r <- functions.parTraverseN(PARALLEL) { func => func(spark).attempt }
        _ <- IO(r.foreach {
          case Right(result) => logger.info(s"Count is $result")
          case Left(ex) =>
            logger.error(s"Failed with exception: ${ex.toString} - ${ex.getMessage}")
        })
        _ <- IO(logger.info("DONE!"))
      } yield ()
    }

  val functions: List[SparkSession => IO[Result]] =
    List(
      s => countTitles(s),
      s => countEmployees(s),
      s => countTitles(s),
      s => countEmployees(s),
      s => countTitles(s),
      s => countEmployees(s),
      s => countTitles(s),
      s => countEmployees(s),
      s => countTitles(s),
      s => countEmployees(s),
      s => countTitles(s),
      s => countEmployees(s),
      s => countTitles(s),
      s => countEmployees(s),
      s => countTitles(s),
      s => countEmployees(s),
      s => countTitles(s),
      s => countEmployees(s),
      s => countTitles(s),
      s => countEmployees(s)
    )

  def countTitles(spark: SparkSession): IO[Result] = for {
    _ <- IO(logger.info(s"Using SparkSession ${spark.version}"))
    titlesDF <- IO {
      spark.read
        .json(getClass.getResource("/data/titles.json").getFile)
    }
    rowCount <- IO { titlesDF.count() }
    _ <- IO { titlesDF.cache() }
    _ <- IO(logger.info(s"titlesDF count = $rowCount"))
    result <- IO(Result("countTitles", rowCount, titlesDF))
  } yield result

  def countEmployees(spark: SparkSession): IO[Result] = for {
    _ <- IO(logger.info(s"Using SparkSession ${spark.version}"))
    empDF <- IO {
      spark.read
        .json(getClass.getResource("/data/employees.json").getFile)
    }
    rowCount <- IO { empDF.count() }
    _ <- IO { empDF.cache() }
    _ <- IO(logger.info(s"empDF count = $rowCount"))
    result <- IO(Result("countEmployees", rowCount, empDF))
  } yield result
}
