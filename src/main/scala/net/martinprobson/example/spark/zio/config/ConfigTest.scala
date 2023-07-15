package net.martinprobson.example.spark.zio.config

import net.martinprobson.example.spark.zio.ZIOApplication
import zio.{Task, ZIO}

object ConfigTest extends ZIOApplication  {
  private def program: ZIO[SparkConfig, Throwable, Unit] = for {
    _ <- ZIO.logInfo("Start")
    cfg <- ZIO.service[SparkConfig]
    _ <- ZIO.logInfo(s" Master: ${cfg.master}")
    _ <- ZIO.logInfo(s" app_name: ${cfg.appName}")
  } yield  ()
  override def run: Task[Unit] = program.provide(SparkConfig.layer)
}
