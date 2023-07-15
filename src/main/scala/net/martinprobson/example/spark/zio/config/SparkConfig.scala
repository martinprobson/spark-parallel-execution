package net.martinprobson.example.spark.zio.config

import zio.*
import zio.config.magnolia.deriveConfig
import zio.config.typesafe.TypesafeConfigProvider
import zio.config.{ConfigOps, toSnakeCase}

final case class SparkConfig(appName: String, master: String)

object SparkConfig {
  val layer: ZLayer[Any, Config.Error, SparkConfig] = ZLayer {
    TypesafeConfigProvider.fromResourcePath
      .load(
        deriveConfig[SparkConfig]
          .mapKey(toSnakeCase)
          .nested("spark-parallel-execution")
      )
  }
}
