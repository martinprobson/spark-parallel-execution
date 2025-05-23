package net.martinprobson.example.spark.common

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

trait SparkEnv {
  lazy private[spark] val conf = ConfigFactory.load
  lazy private[spark] val spark = getSession

  /** Return some information on the environment we are running in.
    */
  private[spark] def versionInfo: Seq[String] = {
    val sc = getSession.sparkContext
    val scalaVersion = scala.util.Properties.scalaPropOrElse("version.number", "unknown")

    val versionInfo = s"""
                         |---------------------------------------------------------------------------------
                         | Scala version: $scalaVersion
                         | Spark version: ${sc.version}
                         | Spark master : ${sc.master}
                         | Spark running locally? ${sc.isLocal}
                         | Default parallelism: ${sc.defaultParallelism}
                         | UI: ${sc.uiWebUrl}
                         |---------------------------------------------------------------------------------
                         |""".stripMargin

    versionInfo.split("\n").toIndexedSeq
  }

  /** Return spark session object
    *
    * NOTE Add .master("local") to enable debug via an IDE or add as a VM option at runtime
    * -Dspark.master="local[*]"
    */
  private def getSession: SparkSession = {
    val sparkSession = SparkSession
      .builder()
      //.config("spark.sql.autoBroadcastJoinThreshold","-1")
      .master(conf.getString("spark-parallel-execution.master"))
      .config("spark.driver.bindAddress","127.0.0.1")
      .appName(conf.getString("spark-parallel-execution.app_name"))
      .getOrCreate()
    sparkSession
  }

  /*
   * Dump spark configuration for the current spark session.
   */
  private[spark] def getAllConf: String = {
    getSession.conf.getAll.map { case (k, v) =>
      "Key: [%s] Value: [%s]" format (k, v)
    } mkString ("", "\n", "\n")
  }
}
