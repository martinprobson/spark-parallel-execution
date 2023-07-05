package net.martinprobson.example.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.concurrent.{Executors, TimeUnit}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}
import scala.util.{Failure, Success, Try}

object ParallelWithFutures extends Logging with SparkEnv {

  def main(args: Array[String]): Unit = {
    versionInfo.foreach(logger.info(_))
    val titlesDF = spark.read.json(getClass.getResource("/data/titles.json").getFile)
    titlesDF.createTempView("titles")
    titlesDF.cache()
    val employeesDF = spark.read.json(getClass.getResource("/data/employees.json").getFile)
    employeesDF.createTempView("employees")
    employeesDF.cache()

    implicit val ec: ExecutionContextExecutorService =
      ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(10))

    List(
      "select t.title,e.birth_date from titles t join employees e on e.birth_date = t.from_date",
      "select distinct title from titles",
      "select t.title,e.birth_date from titles t join employees e on e.birth_date = t.from_date",
      "select distinct title from titles",
      "select distinct title from titles",
      "select distinct title from titles",
      "A load of rubbish",
      "select distinct title from titles",
      "select * from employees",
      "select * from titles"
    )
      .foreach(query => Future {
        runQuery(spark, query)
      }.onComplete(processResult))

    // Uncomment the following line to pause the code and allow the Spark UI to be viewed
    //scala.io.StdIn.readLine()
    // Wait long enough for the queries to complete.....
    ec.awaitTermination(10, TimeUnit.SECONDS)
    logger.info("About to call spark.stop()")
    spark.stop()
    logger.info("called spark.stop()")
    logger.info("about to shutdown ec")
    ec.shutdown()
    logger.info("shutdown ec")
  }

  def runQuery(session: SparkSession, query: String): DataFrame = {
    logger.info(s"In runQuery $query")
    val df = session.sql(query)
    df.count()
    df
  }

  def processResult[A](result: Try[A]): Unit = result match {
    case Failure(exception) => logger.error("Failed with: " + exception.getMessage)
    case Success(df)        => logger.info("Succeed with: " + df)
  }
}
