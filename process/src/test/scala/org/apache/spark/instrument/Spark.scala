package org.apache.spark.instrument

import org.apache.spark.{SparkConf, SparkContext}

object Spark {
  val sc: SparkContext = SparkContext.getOrCreate(new SparkConf().setMaster("local[2]").setAppName("Spark-Tracing build test"))
}
