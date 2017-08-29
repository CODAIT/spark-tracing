#! /bin/bash
set -eu

scalaver="2.11"
sparkver="2.3.0-SNAPSHOT" #$(cat $basedir/docs/_config.yml | grep '^SPARK_VERSION:' | cut -d ' ' -f 2)
hadoopver="2.8.1"
hadoopvershort=$(echo "$hadoopver" | cut -d '.' -f -2)

basedir="$HOME/code/spark"
distdir="$basedir/mindist"
resultdir="$HOME/code/spark-tracing/runs"
#cluster="dynalloc"
cluster="stcindia-node-"
nnodes=4
domain="fyre.ibm.com"
user="dev-user"
master="${cluster}1.$domain"
slaves="${cluster}{2..$nnodes}.$domain"
dest="/test/spark-tracing"
traceout="/tmp/spark-trace"
port="5010"

testclass="org.apache.spark.examples.SparkPi"
testjar="spark-examples_${scalaver}-$sparkver.jar"
iters=20

while [[ $# > 0 ]]
	do case "$1" in
	#"dist")
	#	"$basedir/dev/make-distribution.sh" --name spark-tracing -Pyarn -Phive -Phadoop-$hadoopvershort -Dhadoop.version=$hadoopver
	#	;;
	#"clean")
	#	"$basedir/build/mvn" -Phive -Pyarn -Phadoop-$hadoopvershort -Dhadoop.version=$hadoopver -DskipTests clean
	#	;;
	#"build")
	#	"$basedir/build/mvn" -Phive -Pyarn -Phadoop-$hadoopvershort -Dhadoop.version=$hadoopver -DskipTests package
	#	;;
	"build")
		pushd "instrument"
		sbt package
		popd
		;;
	"conf")
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
		spark.dynamicAllocation.initialExecutors 1
		spark.dynamicAllocation.maxExecutors 8
		spark.dynamicAllocation.minExecutors 1
		spark.dynamicAllocation.schedulerBacklogTimeout 1s
		spark.dynamicAllocation.sustainedSchedulerBacklogTimeout 1s
		spark.shuffle.service.enabled true
		#spark.eventLog.enabled true
		spark.executor.cores 1
		spark.task.cpus 1

		#spark.driver.extraJavaOptions -javaagent:$dest/instrument_2.11-1.0.jar
		#spark.yarn.am.extraJavaOptions -javaagent:$dest/instrument_2.11-1.0.jar
		spark.executor.extraJavaOptions -javaagent:$dest/instrument_2.11-1.0.jar
		!
		cp $basedir/conf/log4j.properties{.template,}
		cat <<- ! >> $basedir/conf/log4j.properties
		log4j.rootCategory=INFO, console
		!
		cat <<- ! >> $basedir/conf/spark-env.sh
		HADOOP_CONF_DIR=/etc/hadoop/conf
		!

		rm -rf $distdir/examples/jars/* $distdir/yarn/*
		ln -s ../../../examples/target/scala-$scalaver/jars/$testjar $distdir/examples/jars
		ln -s ../../common/network-yarn/target/scala-$scalaver/spark-${sparkver}-yarn-shuffle.jar $distdir/yarn
		;;
	"upload")
		rsync -av --copy-unsafe-links --delete --progress $distdir/ $user@$master:$dest
		ssh -t $user@$master "cat /etc/spark2/conf/spark-defaults.conf | sed '/spark.yarn.archive/s/^/#/' >> $dest/conf/spark-defaults.conf"
		ssh -t $user@$master "for host in $slaves; do rsync -av --copy-unsafe-links --delete $dest/ \$host:$dest; done"
		;;
	"remote")
		ssh -t $user@$master "sudo rm -rf $traceout; for host in $slaves; do ssh -t \$host sudo rm -rf $traceout; done"
		#ssh -t $user@$master "$dest/sbin/stop-all.sh; for host in $slaves; do ssh \$host $dest/sbin/stop-all.sh; done"
		#ssh -t $user@$master "$dest/sbin/start-master.sh; for host in $slaves; do ssh \$host $dest/sbin/start-slave.sh $master:7077; done"
		#firefox http://$master:$port &
		ssh -t $user@$master sudo -iu notebook SPARK_PRINT_LAUNCH_COMMAND=1 $dest/bin/spark-submit --conf spark.driver.extraJavaOptions="-javaagent:$dest/instrument_2.11-1.0.jar" --conf spark.yarn.am.extraJavaOptions="-javaagent:$dest/instrument_2.11-1.0.jar" --master yarn --class "$testclass" "$dest/examples/jars/$testjar" $iters
		;;
	"collect")
		ssh -t $user@$master "sudo chown -R dev-user $traceout; for host in $slaves; do ssh -t \$host sudo chown -R dev-user $traceout; done"
		ssh -t $user@$master "for host in $slaves; do rsync -av \$host:$traceout/ $traceout; done"
		localout="$resultdir/remote"
		rm -rf "$localout"
		mkdir "$localout"
		rsync -av --progress $user@$master:$traceout/ "$localout"
		ssh -t $user@$master "rm -r $traceout; for host in $slaves; do ssh -t \$host rm -r $traceout; done"
		;;
	"local")
		#$distdir/sbin/stop-all.sh
		rm -rf $traceout
		$distdir/bin/spark-submit --master yarn --class "$testclass" "$distdir/examples/jars/$testjar" $iters
		;;
	"standalone")
		$distdir/sbin/stop-all.sh
		rm -rf $traceout
		#$distdir/sbin/start-all.sh
		$distdir/sbin/start-master.sh
		$distdir/sbin/start-slave.sh spark://localhost:7077

		#firefox http://localhost:$port &
		$distdir/bin/spark-submit --master spark://localhost:7077 --class "$testclass" "$distdir/examples/jars/$testjar" $iters
		;;
	*)
		echo "Unknown action $1"
		exit 1
		;;
	esac
	shift
done

