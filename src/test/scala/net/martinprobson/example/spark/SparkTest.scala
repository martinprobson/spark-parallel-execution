package net.martinprobson.example.spark

import net.martinprobson.example.spark.common.{Logging, Util}
import org.apache.spark.sql.SparkSession
import org.scalatest.Outcome
import org.scalatest.funsuite.FixtureAnyFunSuite

import java.io.InputStream

class SparkTest extends FixtureAnyFunSuite with Logging {

  type FixtureParam = SparkSession

  def withFixture(test: OneArgTest): Outcome = {
    val sparkSession = SparkSession
      .builder()
      .appName("Test-Spark-Local")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .master("local[*]")
      .getOrCreate()
    try {
      withFixture(test.toNoArgTest(sparkSession))
    } finally sparkSession.stop()
  }

  test("empsRDD rowcount") { spark =>
    val empsRDD = spark.sparkContext.parallelize(Util.getInputData("/data/employees.json"), 5)
    assert(empsRDD.count() === 1000)
  }

  test("titlesRDD rowcount") { spark =>
    val titlesRDD = spark.sparkContext.parallelize(Util.getInputData("/data/titles.json"), 5)
    assert(titlesRDD.count() === 1470)
  }

  test("titlesDF rowcount") { spark =>
    val titlesDF = spark.read.json(getClass.getResource("/data/titles.json").getFile)
    assert(titlesDF.count() === 1470)
  }

  test("employeesDF rowcount") { spark =>
    val employeesDF = spark.read.json(getClass.getResource("/data/employees.json").getFile)
    assert(employeesDF.count() === 1000)
  }

  test("Join employeesDF/titleDF ") { spark =>
    val titlesDF = spark.read.json(getClass.getResource("/data/titles.json").getFile)
    val employeesDF = spark.read.json(getClass.getResource("/data/employees.json").getFile)
    val empTitlesDF = employeesDF.join(titlesDF, Seq("emp_no"), "inner")
    assert(empTitlesDF.count() === 1470)
  }

}
