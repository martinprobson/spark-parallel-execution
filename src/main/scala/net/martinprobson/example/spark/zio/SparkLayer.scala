package net.martinprobson.example.spark.zio

import net.martinprobson.example.spark.common.SparkEnv
import org.apache.spark.sql.SparkSession
import zio.{Task, ZIO, ZLayer}

object SparkLayer extends SparkEnv {
  def openSparkSession: Task[SparkSession] = ZIO.logInfo("Opening spark session")
    *> ZIO.attempt(spark)

  def closeSparkSessionWithPause(sparkSession: SparkSession): Task[Unit] =
    ZIO.logInfo(s"closing sparkSession") *> ZIO.attempt(scala.io.StdIn.readLine()) *> ZIO.attempt(
      sparkSession.close()
    )

  def closeSparkSession: SparkSession => ZIO[Any, Nothing, Unit] = s =>
    ZIO.logInfo(s"closing sparkSession") *> ZIO.attempt(s.close()).ignore
}

object SparkLayerLive {
  val layer: ZLayer[Any, Throwable, SparkSession] =
    ZLayer.scoped {
      ZIO.acquireRelease {
        SparkLayer.openSparkSession
      } {
            SparkLayer.closeSparkSession
      }
    }
}


