#! /bin/bash
set -eu

scalaver="2.11"
sparkver="2.3.0-SNAPSHOT" #$(cat $basedir/docs/_config.yml | grep '^SPARK_VERSION:' | cut -d ' ' -f 2)
hadoopver="2.7.4"
hadoopvershort=$(echo "$hadoopver" | cut -d '.' -f -2)

basedir="$HOME/code/spark"
distdir="$basedir/mindist"
resultdir="$HOME/code/spark-tracing/runs"
#cluster="dynalloc"
cluster="stcindia-node-"
nnodes=4
domain="fyre.ibm.com"
master="${cluster}1.$domain"
slaves="${cluster}{2..$nnodes}.$domain"
traceout="/tmp/spark-trace"
localhadoop="/opt/hadoop"
port="5010"
nexecs=20

if [[ "$1" = "remote" ]]
	then local=0
	user="dev-user"
	dest="/home/matt/spark"
	sparkbench="$dest/../spark-bench/bin/spark-bench.sh"
	benchout="/user/$user/benchmark.csv"
	benchdest="/home/matt/benchmark.csv"
	shift
else
	[[ "$1" = "local" ]] && shift
	local=1
	user="matt"
	dest=$distdir
	sparkbench="$basedir/../spark-bench/bin/spark-bench.sh"
	benchout="$traceout/benchmark.csv"
fi

component="$1"
[[ "$component" = "instrument" || "$component" = "spark" || "$component" = "process" ]] || exit 1
shift

while [[ $# > 0 ]]
	do action="$1"
	shift
	case "$action" in
	"dist")
		if [[ "$component" = "spark" ]]
			then pushd "$basedir"
			"dev/make-distribution.sh" --name spark-tracing -Pyarn -Phive -Phadoop-$hadoopvershort -Dhadoop.version=$hadoopver
			popd
		fi
		;;
	"clean")
		if [[ "$component" = "instrument" ]]
			then pushd instrument
			sbt clean
		elif [[ "$component" = "process" ]]
			then pushd process
			sbt clean
		elif [[ "$component" = "spark" ]]
			then pushd "$basedir"
			"build/mvn" -Phive -Pyarn -Phadoop-$hadoopvershort -Dhadoop.version=$hadoopver -DskipTests clean
		fi
		popd
		;;
	"build")
		if [[ "$component" = "instrument" ]]
			then pushd instrument
			sbt assembly
		elif [[ "$component" = "process" ]]
			then pushd process
			sbt assembly
		elif [[ "$component" = "spark" ]]
			then pushd "$basedir"
			"build/mvn" -Phive -Pyarn -Phadoop-$hadoopvershort -Dhadoop.version=$hadoopver -DskipTests package
		fi
		popd
		;;
	"conf")
		javaagent="-javaagent:$dest/instrument/instrument-assembly-1.0.jar -Dinstrument.config=$dest/instrument/standard.conf"
		cat <<- ! > $basedir/conf/spark-defaults.conf
		spark.master.ui.port $port
		spark.worker.ui.port $port
		spark.hadoop.yarn.timeline-service.enabled false
		spark.executor.instances 1
		spark.executor.memory 512m
		spark.yarn.jars local:$dest/jars/*

		spark.dynamicAllocation.cachedExecutorIdleTimeout 60s
		spark.dynamicAllocation.enabled true
		spark.dynamicAllocation.monitor.enabled true
		spark.dynamicAllocation.executorIdleTimeout 20s
		spark.dynamicAllocation.initialExecutors $nexecs
		spark.dynamicAllocation.maxExecutors $nexecs
		spark.dynamicAllocation.minExecutors $nexecs
		spark.dynamicAllocation.schedulerBacklogTimeout 1s
		spark.dynamicAllocation.sustainedSchedulerBacklogTimeout 1s
		spark.shuffle.service.enabled true
		#spark.eventLog.enabled true
		spark.executor.cores 1
		spark.executor.instances $nexecs
		spark.task.cpus 1
		!
		cp $basedir/conf/log4j.properties{.template,}
		cat <<- ! >> $basedir/conf/log4j.properties
		log4j.rootCategory=INFO, console
		!
		cat <<- ! > $basedir/conf/spark-env.sh
		SPARK_HOME=$dest
		HADOOP_CONF_DIR=/etc/hadoop/conf
		!
		if [[ "$component" = "instrument" ]]
			then cat <<- ! >> $basedir/conf/spark-defaults.conf
			spark.driver.extraJavaOptions $javaagent -Diop.version=4.3.0.0
			spark.yarn.am.extraJavaOptions $javaagent -Diop.version=4.3.0.0
			spark.executor.extraJavaOptions $javaagent -Diop.version=4.3.0.0
			spark.extraListeners org.apache.spark.SparkFirehoseListener
			!
			cat <<- ! > $distdir/instrument/benchmark.conf
			spark-bench = {
				#repeat = 10
				spark-submit-config = [{
					workload-suites = [{
						benchmark-output = "$benchout"
						workloads = [
							{
								name = "timedsleep"
								partitions = 20
								sleepms = 50
							}
						]
					}]
				}]
			}
			!
		fi
		rm -rf $distdir/examples/jars/* $distdir/yarn/*
		ln -s ../../common/network-yarn/target/scala-$scalaver/spark-${sparkver}-yarn-shuffle.jar $distdir/yarn
		;;
	"upload")
		[[ "$local" = "1" ]] && continue
		rsync -rlv --copy-unsafe-links --delete --progress $distdir/ $user@$master:$dest
		ssh -t $user@$master "for host in $slaves; do rsync -rlv --copy-unsafe-links --delete $dest/ \$host:$dest; done"
		;;
	"run")
		if [[ "$local" = "0" ]]
			then ssh -t $user@$master "sudo rm -rf $traceout; sudo rm -rf $benchdest; for host in $slaves; do ssh -t \$host sudo rm -rf $traceout; done"
			ssh -t $user@$master "mkdir $benchdest"
			#for i in {1..10}; do
			ssh -t $user@$master "SPARK_HOME=$dest SPARK_MASTER_HOST=yarn $sparkbench $dest/instrument/benchmark.conf"
			ssh -t $user@$master "hdfs dfs -get $benchout/\\*.csv $benchdest"
			ssh -t $user@$master "hdfs dfs -rm -r $benchout"
			#done
		else
			if [[ "$component" = "instrument" ]]
				then rm -rf $traceout
				SPARK_HOME=$distdir SPARK_MASTER_HOST=yarn $sparkbench $distdir/instrument/benchmark.conf
			elif [[ "$component" = "process" ]]
				then $distdir/bin/spark-submit --master yarn process/target/scala-2.11/process-assembly-1.0.jar instrument/src/main/resources/standard.conf
			fi
		fi
		;;
	"collect")
		[[ "$local" = "1" ]] && continue
		ssh -t $user@$master "sudo chown -R dev-user $traceout; for host in $slaves; do ssh -t \$host sudo chown -R dev-user $traceout; done" || true
		ssh -t $user@$master "for host in $slaves; do rsync -rlv \$host:$traceout/ $traceout; done" || true
		localout=$resultdir/remote
		rm -rf $localout
		mkdir $localout
		rsync -rlv --progress $user@$master:$traceout/ $localout
		rsync -rlv --progress $user@$master:$benchdest $localout || true
		ssh -t $user@$master "rm -r $traceout; rm -r $benchdest; for host in $slaves; do ssh -t \$host rm -r $traceout; done" || true
		;;
	"yarn")
		[[ "$local" = "0" ]] && continue
		"$localhadoop/sbin/stop-yarn.sh" || true
		YARN_USER_CLASSPATH="$distdir/yarn/spark-$sparkver-yarn-shuffle.jar" "$localhadoop/sbin/yarn-daemon.sh" start resourcemanager
		YARN_USER_CLASSPATH="$distdir/yarn/spark-$sparkver-yarn-shuffle.jar" "$localhadoop/sbin/yarn-daemon.sh" start nodemanager
		;;
	*)
		echo "Unknown action $1"
		exit 1
		;;
	esac
done

