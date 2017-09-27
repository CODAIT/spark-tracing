/* Copyright 2017 IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
