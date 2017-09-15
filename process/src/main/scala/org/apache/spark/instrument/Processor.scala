package org.apache.spark.instrument

import com.typesafe.config._
import java.io.File
import org.apache.spark.instrument.blocks._
import org.apache.spark.sql.SparkSession

/* 1. Resolution
 * 2. Output
 * 3. Statistics
 * 4. Filtering
 * 5. Transforms
 */

object Processor {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Spark Trace Processing").getOrCreate()
    import spark.implicits._

    val configs = args.map(fname => ConfigFactory.parseFile(new File(fname)))
    val inputs = configs.map(config => {
      spark.read.text(config.getString("props.output")).as[String].rdd.map(x => EventTree(x))
    })
    val resolve = new ServiceMap(inputs)
    val in = inputs.reduce(_.union(_))
    // TODO Filtering
    // TODO Transforms
    val blocks: Set[OutputBlockGenerator] = Set(Axes, RPCs, Events, Spans)
    val out = new Output(new File("/tmp/spark-trace.out"))
    blocks.foreach(gen => out.addBlock(gen.apply(in, resolve)))
    out.close()
  }
}
