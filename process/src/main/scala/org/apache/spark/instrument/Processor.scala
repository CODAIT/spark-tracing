package org.apache.spark.instrument

import com.typesafe.config._
import java.io.File
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConverters._

object Processor {
  def configProp(config: Config, name: String): String =
    if (! config.hasPath("props." + name)) throw new RuntimeException(s"Missing config property $name")
    else config.getString("props." + name)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Spark trace processing").getOrCreate()
    import spark.implicits._
    require(args.length >= 1, "An input configuration file is required")
    val config = ConfigFactory.parseFile(new File(args.head))
    val traceFiles = configProp(config, "traceout") +: args.tail
    val inputs = traceFiles.map { file =>
      val cur = spark.read.text(file).as[String].rdd.map(x => EventTree(x))
      val start = cur.map(_(2).get.get.toLong).min
      cur.map(_.update(Seq(2), t => EventLeaf((t.get.get.toLong - start).toString)))
    }
    val transforms = {
      def isTransformable(tracer: Config) = Set("event", "span").contains(tracer.getString("type"))
      def confToFormatPair(pkg: String, conf: Config): (String, Option[String]) = {
        val className = pkg + "." + conf.getString("class")
        val methodName = conf.getString("method")
        val fullName =
          if (className.endsWith("." + methodName)) className // Constructor
          else className + "." + methodName
        val fmtString = if (conf.hasPath("format")) Some(conf.getString("format")).filter(_ != "") else None
        fullName -> fmtString
      }
      config.getObject("targets").keySet().asScala.flatMap(key =>
        config.getConfig("targets").getObjectList("\"" + key + "\"").asScala.map(_.toConfig)
          .filter(isTransformable).map(confToFormatPair(key, _))
      ).filter(_._2.isDefined).toMap.map(x => (x._1, new FmtSpec(x._2.get))) // Can't use mapValues because it returns a non-serializable view
    }
    val resolve = new ServiceMap(inputs)
    val in = inputs.reduce(_.union(_)).cache
    val blocks: Set[OutputBlock] = Set(
      new TimeRange(in),
      new Axes(resolve),
      new RPCs(in, resolve),
      new Events(in, resolve, transforms),
      new Spans(in, resolve, transforms),
      new Stats("count", Seq(StatJVMs, StatExecs, StatJobs, StatTasks, StatBlockUpdates), Seq(ColCount), in, resolve),
      new Stats("dist", Seq(StatJVMStart, StatExecLife, StatTaskLength,  StatRPCCount, StatInstrOver),
        Seq(0, 25, 50, 75, 100).map(new ColPercentile(_)) ++ Seq(ColArgMin, ColArgMax), in, resolve)
    )
    val out = new Output(new File(configProp(config, "result")))
    blocks.foreach(block => out.addBlock(block))
    out.close()
  }
}
