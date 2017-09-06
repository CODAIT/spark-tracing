package org.apache.spark.instrument

import java.lang.instrument.Instrumentation

object SparkAgent {
  def premain(args: String, instrumentation: Instrumentation): Unit = {
    instrumentation.addTransformer(new ClassInstrumenter())
    //Runtime.getRuntime.addShutdownHook(new Thread() { override def run(): Unit = ExitLogger.log(0) })
  }
}
