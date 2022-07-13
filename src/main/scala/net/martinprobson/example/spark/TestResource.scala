package net.martinprobson.example.spark

import cats.effect.{IO, IOApp, Resource}
import org.apache.spark.sql.SparkSession

object TestResource extends IOApp.Simple with Logging with SparkEnv {

  def openSparkSession: IO[SparkSession] = IO(spark)
  def closeSparkSession(sparkSession: SparkSession): IO[Unit] =
    IO(logger.info(s"closing sparkSession")) >> IO(sparkSession.close())

  override def run: IO[Unit] =
    Resource.make(openSparkSession)(sparkSession => closeSparkSession(sparkSession)).use { spark =>
      for {
        f1 <- countTitles(spark).start
        f2 <- countEmployees(spark).start
        r1 <- f1.joinWith(IO(0L))
        r2 <- f2.joinWith(IO(0L))
        _ <- IO(logger.info(s"Titles count = $r1 Employees count = $r2"))
      } yield ()
    }

  def countTitles(spark: SparkSession): IO[Long] = for {
    _ <- IO(logger.info(s"Using SparkSession ${spark.version}"))
    titlesDF <- IO {
      spark.read
        .json(getClass.getResource("/data/titles.json").getFile)
    }
    rowCount <- IO { titlesDF.count() }
    _ <- IO(logger.info(s"titlesDF count = $rowCount"))

  } yield (rowCount)

  def countEmployees(spark: SparkSession): IO[Long] = for {
    _ <- IO(logger.info(s"Using SparkSession ${spark.version}"))
    empDF <- IO {
      spark.read
        .json(getClass.getResource("/data/employees.json").getFile)
    }
    rowCount <- IO { empDF.count() }
    _ <- IO(logger.info(s"empDF count = $rowCount"))

  } yield (rowCount)
}
