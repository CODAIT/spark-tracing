package org.apache.spark.instrument

import com.typesafe.config._
import java.io.File

import org.apache.spark.sql.SparkSession

object Processor {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Spark-Tracing Process").getOrCreate()
    import spark.implicits._

    val config = {
      val configFile = args.headOption.getOrElse(throw new RuntimeException("Config file required as first argument"))
      ConfigFactory.parseFile(new File(configFile))
    }
    val in = spark.read.text(config.getString("props.output")).as[String].rdd.map(x => EventTree(x))
    in.map(_(1)).distinct.foreach(println(_))
  }
}
