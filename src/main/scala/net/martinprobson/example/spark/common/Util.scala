package net.martinprobson.example.spark.common

import java.io.InputStream

object Util {
    def getInputData(name: String): Seq[String] = {
        val is: InputStream = getClass.getResourceAsStream(name)
        scala.io.Source.fromInputStream(is).getLines().toSeq
    }
}
