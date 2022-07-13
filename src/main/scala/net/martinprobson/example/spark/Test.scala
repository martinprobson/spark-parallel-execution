package net.martinprobson.example.spark

import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext.*
import scala.concurrent.{ExecutionContext, Future}

object Test extends App with Logging {

  logger.info("Hello world!")
  val ec = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(2))
  Range.inclusive(1, 10).map(i => Future { foo(i) }(ec))

  scala.io.StdIn.readLine()
  ec.shutdown()

  def foo(id: Int): Unit = {
    logger.info(s"In foo $id")
    Thread.sleep(1000 * 2)
    logger.info(s"Exiting foo $id")
  }
}
