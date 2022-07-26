package net.martinprobson.example.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}
import scala.util.{Failure, Success, Try}

object ParallelWithFutures extends App with Logging with SparkEnv {
  versionInfo.foreach(logger.info(_))

  val titlesDF = spark.read.json(getClass.getResource("/data/titles.json").getFile)
  titlesDF.createTempView("titles")
  titlesDF.cache()
  val employeesDF = spark.read.json(getClass.getResource("/data/employees.json").getFile)
  employeesDF.createTempView("employees")
  employeesDF.cache()

  implicit val ec: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(1))

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
    .foreach(query => Future { runQuery(spark, query) }.onComplete(processResult))

  scala.io.StdIn.readLine()
  spark.stop()

  def runQuery(session: SparkSession, query: String): DataFrame = {
    val df = session.sql(query)
    df.count()
    df
  }

  def processResult[A](result: Try[A]): Unit = result match {
    case Failure(exception) => logger.error("Failed with: " + exception.getMessage)
    case Success(df)        => logger.info("Succeed with: " + df)
  }
}
