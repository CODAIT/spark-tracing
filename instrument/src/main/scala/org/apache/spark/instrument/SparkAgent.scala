package org.apache.spark.instrument

import java.io.File
import java.lang.instrument.Instrumentation
import com.typesafe.config.ConfigFactory

object SparkAgent {
  def premain(args: String, instrumentation: Instrumentation): Unit = {
    if (args == null || args == "") throw new RuntimeException("Run this instrumentation with -javaagent:/path/to/instrumentation.jar=/path/to/config.conf")
    val configFile = new File(args)
    if (! configFile.exists || ! configFile.canRead || ! configFile.isFile) throw new RuntimeException("Couldn't open instrumentation configuration")
    val config = ConfigFactory.parseFile(configFile)
    instrumentation.addTransformer(new ClassInstrumenter(config))
    TraceWriter.init(config.getString("output"))
  }
}
