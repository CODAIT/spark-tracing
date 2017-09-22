package org.apache.spark.instrument

import com.typesafe.config._
import java.io.File
import org.apache.spark.sql.SparkSession

object Processor {
  def configProp(config: Config, name: String): String =
    Option(config.getString("props." + name)).getOrElse(throw new RuntimeException(s"Missing config property $name"))
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Spark trace processing").getOrCreate()
    import spark.implicits._
    val configs = args.map(fname => ConfigFactory.parseFile(new File(fname)))
    assert(configs.length > 0, "At least one input config is required")
    val inputs = configs.map { config =>
      val cur = spark.read.text(configProp(config, "traceout")).as[String].rdd.map(x => EventTree(x))
      val start = cur.map(_(2).get.get.toLong).min
      cur.map(_.update(Seq(2), t => EventLeaf((t.get.get.toLong - start).toString)))
    }
    val resolve = new ServiceMap(inputs)
    val in = inputs.reduce(_.union(_))
    val blocks: Set[OutputBlock] = Set(
      new TimeRange(in),
      new Axes(resolve),
      new RPCs(in, resolve),
      new Events(in, resolve),
      new Spans(in, resolve),
      new Stats("count", Seq(StatJVMs, StatExecs, StatJobs, StatTasks, StatBlockUpdates), Seq(ColCount), in, resolve),
      new Stats("dist", Seq(StatJVMStart, StatExecLife, StatTaskLength,  StatRPCCount, StatInstrOver),
        Seq(0, 25, 50, 75, 100).map(new ColPercentile(_)) ++ Seq(ColArgMin, ColArgMax), in, resolve)
    )
    val out = new Output(new File(configProp(configs.head, "result")))
    blocks.foreach(block => out.addBlock(block))
    out.close()
  }
}
