package net.martinprobson.example.spark

object Main extends App with Logging with SparkEnv {
  import spark.implicits._
  versionInfo.foreach(logger.info(_))

  val titlesDF = spark.read.json(getClass.getResource("/data/titles.json").getFile).repartition(10)
  titlesDF.createTempView("titles")
  //titlesDF.cache()
  val employeesDF = spark.read.json(getClass.getResource("/data/employees.json").getFile).repartition(10)
  employeesDF.createTempView("employees")
  //employeesDF.cache()
  //val resDF = spark.sql("select t.title,e.gender from titles t join employees e on e.emp_no = t.emp_no")
  val resDF = spark.sql("select t.title,e.birth_date from titles t join employees e on e.birth_date = t.from_date")
  resDF.show(10000,false)
  //spark.sql("select distinct title from titles").show(1000,false)
  scala.io.StdIn.readLine()
  spark.stop()
}
