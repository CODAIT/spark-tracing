package org.apache.spark.instrument

import java.lang.instrument.Instrumentation

object SparkAgent {
  def premain(args: String, instrumentation: Instrumentation): Unit = {
    instrumentation.addTransformer(new ClassInstrumenter())
  }
}
