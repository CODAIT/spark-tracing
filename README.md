# Spark-Tracing

This repository provides a tool that allows users of Apache Spark to instrument it at runtime to record and visualize internal
events for the purposes of benchmarking and performance tuning.  It consists of two parts: an instrumentation package and a
visualization Jupyter notebook.

## Usage

The instrumentation package is in the `instrumentation` directory in the project root.  It is written in Scala and requires SBT to
compile.  Switching into the directory and running `sbt package` should be sufficient to create the tracing package JAR file in
`/instrumentation/target/scala-2.11`.

To use the instrumentation package, the JAR should be placed in the same location on every machine in the Spark cluster.  Then, the
following configuration options need to be added to `spark-defaults.conf`.  Some of the options may need to be added with `--conf`
directives on the invocation command-line; I'm investigating why this is the case.

  - `spark.driver.extraJavaOptions` = `-javaagent:/path/to/instrumentation.jar`
  - `spark.executor.extraJavaOptions` = `-javaagent:/path/to/instrumentation.jar`
  - (On YARN) `spark.yarn.am.extraJavaOptions` = `-javaagent:/path/to/instrumentation.jar`

Spark jobs should now run with instrumentation applied.  After running, one or more `.tsv` files should appear in `/tmp/spark-trace`
on each machine that ran a Spark component.  These need to be collected into one directory on the machine where the visualization
notebook will be run.

The output files contain one line per logged event, with a timestamp and short description for each.  These results may be useful by
themselves or when analyzed by various external tools; however, this project also provides a Jupyter notebook for visualizing the
results.  In Jupyter, open the `/Spark Tracing.ipynb` notebook and run all cells in order except the bottom-most one.  It may be
necessary to install some Python libraries, such as Pandas and Bokeh, to do this.  Then, set the `base` variable near the top of the
last cell to the full path of the directory containing the `.tsv` files from the run.  Run this cell, and run statistics and an
interactive sequence diagram should be automatically generated.

