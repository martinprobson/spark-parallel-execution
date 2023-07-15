val sparkVersion = "3.3.0"

// Remove provided so we can execute locally
//val spark = Seq(
//  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
//  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
//  "org.apache.spark" %% "spark-yarn" % sparkVersion % Provided
//)
val spark = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-yarn" % sparkVersion
)

val zio = Seq(
  "dev.zio" %% "zio" %  "2.0.15",
  "dev.zio" %% "zio-logging" %  "2.1.13",
  "dev.zio" %% "zio-logging-slf4j" % "2.1.13",
  "dev.zio" %% "zio-config" % "4.0.0-RC12",
  "dev.zio" %% "zio-config-magnolia" % "4.0.0-RC12",
  "dev.zio" %% "zio-config-typesafe" % "4.0.0-RC12"
)

val logging = Seq(
  "org.slf4j" % "slf4j-api" % "2.0.5",
  "ch.qos.logback" % "logback-classic" % "1.4.7",
  "ch.qos.logback" % "logback-core" % "1.4.7",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5"
)

val catsEffect = Seq("org.typelevel" %% "cats-effect" % "3.4.8")

val config = Seq("com.typesafe" % "config" % "1.4.2", "com.github.andr83" %% "scalaconfig" % "0.7")

val test = Seq(
  "org.scalactic" %% "scalactic" % "3.2.15" % Test,
  "org.scalatest" %% "scalatest" % "3.2.15" % Test
)

lazy val spark_example = (project in file("."))
  .enablePlugins(JavaAppPackaging)
  .settings(
    name := "spark-parallel-execution",
    version := "0.0.1-SNAPSHOT",
    libraryDependencies ++= logging,
    libraryDependencies ++= spark,
    libraryDependencies ++= catsEffect,
    libraryDependencies ++= zio,
    libraryDependencies ++= config,
    libraryDependencies ++= test,
    scalaVersion := "2.13.8"
  )

//set spark_example / Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat

scalacOptions ++= Seq(
  "-deprecation", // Emit warning and location for usages of deprecated APIs.
  "-explaintypes", // Explain type errors in more detail.
  "-Xfatal-warnings", // Fail the compilation if there are any warnings.
  "-Xsource:3", // Warn for Scala 3 features
  "-Ywarn-dead-code" // Warn when dead code is identified.
)

Test / fork := true

fork in run := true

val java20Options = Seq("--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED")

javaOptions in run ++= java20Options

javacOptions ++= Seq("-source", "20", "-target", "20", "-Xlint")

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*)       => MergeStrategy.discard
  case n if n.startsWith("reference.conf") => MergeStrategy.concat
  case _                                   => MergeStrategy.first
}
