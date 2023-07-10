package net.martinprobson.example.spark

import zio.*
import zio.ZIOAspect.parallel

object Parallel extends ZIOApplication {

  def task(i: Int): Task[Int] =
    for {
      _ <- ZIO.logInfo(s"Start task $i")
      _ <- ZIO.sleep(zio.Duration.fromSeconds(1))
      _ <- ZIO.logInfo(s"Done task $i")
      r <- ZIO.succeed(i)
    } yield r

  val tasks = Range(1, 100).inclusive.toList.map { i => task(i) }

  val run: Task[Unit] = for {
    _ <- ZIO.logInfo("Start")
    r <- ZIO.collectAllPar(tasks) @@ parallel(10)
    _ <- ZIO.logInfo(s"Done result is $r")
  } yield ()
}
