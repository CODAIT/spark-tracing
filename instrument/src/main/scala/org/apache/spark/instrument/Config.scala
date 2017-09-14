package org.apache.spark.instrument

import com.typesafe.config._
import java.io.File
import scala.collection.JavaConverters._

object Config {
  private val config = {
    val missingConfig = "Set the system property instrument.config to the location of the configuration file"
    val configFile = new File(sys.props.get("instrument.config").getOrElse(throw new RuntimeException(missingConfig)))
    if (! configFile.exists || ! configFile.canRead || ! configFile.isFile) throw new RuntimeException("Couldn't open instrumentation configuration")
    ConfigFactory.parseFile(configFile)
  }

  def get[T](prop: String): Option[T] = {
    val subconf = config.getConfig("props")
    if (subconf.hasPath(prop)) Some(subconf.getAnyRef(prop).asInstanceOf[T]) else None
  }

  def targets(): Map[String, Seq[Config]] = config.getObject("targets").keySet().asScala.map(key =>
    (key, config.getConfig("targets").getObjectList("\"" + key + "\"").asScala.map(_.toConfig)) // Gross
  ).toMap

  val trackOverhead: Boolean = get[Boolean]("overhead").getOrElse(false)
}
