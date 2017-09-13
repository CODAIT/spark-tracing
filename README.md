# Spark-Tracing

This repository provides a tool that allows users of Apache Spark to instrument it at runtime to record and visualize internal
events for the purposes of benchmarking and performance tuning.  It consists of two parts: an instrumentation package and a
visualization Jupyter notebook.

## Usage

The instrumentation package is in the `instrumentation` directory in the project root.  It is written in Scala and requires SBT to
compile.  Switching into the directory and running `sbt assembly` should be sufficient to create the tracing assembly JAR file in
`/instrumentation/target/scala-2.11`.

To use the instrumentation package, the JAR should be placed in the same location on every machine in the Spark cluster.  Be sure
to use the assembly JAR, not the package JAR.  Then, make the following changes to `spark-defaults.conf`:

  - Set `spark.driver.extraJavaOptions` and `spark.executor.extraJavaOptions` to `-javaagent:/path/to/instrument-assembly.jar
    -Dinstrument.config=/path/to/instrumentation.conf`.  If running on YARN, also set `spark.yarn.am.extraJavaOptions` to this same
    value.

  - If you want listener events (you probably do), also set `spark.extraListeners` to `org.apache.spark.SparkFirehoseListener`.

Spark jobs should now run with instrumentation applied.  After running, one or more `.tsv` files should appear in the directory
specified by the `props.output` configuration option on each machine that ran a Spark component.  These need to be collected into
one directory on the machine where the visualization notebook will be run.

The output files contain one line per logged event, with a timestamp and short description for each.  These results may be useful by
themselves or when analyzed by various external tools; however, this project also provides a Jupyter notebook for visualizing the
results.  In Jupyter, open the `/Spark Tracing.ipynb` notebook and run all cells in order except the bottom-most one.  It may be
necessary to install some Python libraries, such as Pandas and Bokeh, to do this.  Then, set the `base` variable near the top of the
last cell to the full path of the directory containing the `.tsv` files from the run.  Run this cell, and run statistics and an
interactive sequence diagram should be automatically generated.

