package org.apache.spark.instrument

import com.typesafe.config._
import java.io.File
import org.apache.spark.instrument.blocks._
import org.apache.spark.sql.SparkSession

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
    // Statistics:
    // count: execs started, jobs, tasks, block updates
    // count, min, 25/50/75%, max, min at, max at: JVM start time, RPCs sent, exec lifetime, task duration
    val blocks: Set[OutputBlock] = Set(
      new TimeRange(in),
      new Axes(resolve),
      new RPCs(in, resolve),
      new Events(in, resolve),
      new Spans(in, resolve),
      //new Stats("count", Seq(), Seq(ColCount), in, resolve),
      new Stats("dist", Seq(StatJVMStart, StatExecLife, StatRPCCount, StatInstrOver),
        Seq(ColCount) ++ Seq(0, 25, 50, 75, 100).map(new ColPercentile(_)) ++ Seq(ColArgMin, ColArgMax), in, resolve)
    )
    val out = new Output(new File("/tmp/spark-trace.out"))
    blocks.foreach(block => out.addBlock(block))
    out.close()
  }
}
