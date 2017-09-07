#! /bin/bash
set -eu

scalaver="2.11"
sparkver="2.3.0-SNAPSHOT" #$(cat $basedir/docs/_config.yml | grep '^SPARK_VERSION:' | cut -d ' ' -f 2)
hadoopver="2.8.1"
hadoopvershort=2.7 #$(echo "$hadoopver" | cut -d '.' -f -2)

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
localhadoop="/opt/hadoop"
port="5010"
local=1

testclass="org.apache.spark.examples.SparkPi"
testjar="spark-examples_${scalaver}-$sparkver.jar"
iters=20

while [[ $# > 0 ]]
	do case "$1" in
	"spark-dist")
		pushd "$basedir"
		"dev/make-distribution.sh" --name spark-tracing -Pyarn -Phive -Phadoop-$hadoopvershort -Dhadoop.version=$hadoopver
		popd
		;;
	"spark-clean")
		pushd "$basedir"
		"build/mvn" -Phive -Pyarn -Phadoop-$hadoopvershort -Dhadoop.version=$hadoopver -DskipTests clean
		popd
		;;
	"spark-build")
		pushd "$basedir"
		"build/mvn" -Phive -Pyarn -Phadoop-$hadoopvershort -Dhadoop.version=$hadoopver -DskipTests package
		popd
		;;
	"local")
		local=1
		;;
	"remote")
		local=0
		;;
	"clean")
		pushd "instrument"
		sbt clean
		popd
		;;
	"build")
		pushd "instrument"
		sbt assembly
		popd
		;;
	"conf")
		rdest="$dest"
		[[ "$local" = "1" ]] && rdest="$distdir"
		javaagent="-javaagent:$rdest/instrument/instrument-assembly-1.0.jar=$rdest/instrument/standard.conf"
		cat <<- ! > $basedir/conf/spark-defaults.conf
		spark.master.ui.port $port
		spark.worker.ui.port $port
		spark.hadoop.yarn.timeline-service.enabled false
		spark.executor.instances 1
		spark.executor.memory 512m
		spark.yarn.jars local:$rdest/jars/*

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

		spark.driver.extraJavaOptions $javaagent -Diop.version=4.3.0.0
		spark.yarn.am.extraJavaOptions $javaagent -Diop.version=4.3.0.0
		spark.executor.extraJavaOptions $javaagent -Diop.version=4.3.0.0
		!
		cp $basedir/conf/log4j.properties{.template,}
		cat <<- ! >> $basedir/conf/log4j.properties
		log4j.rootCategory=INFO, console
		!
		cat <<- ! > $basedir/conf/spark-env.sh
		SPARK_HOME=$rdest
		HADOOP_CONF_DIR=/etc/hadoop/conf
		!

		rm -rf $distdir/examples/jars/* $distdir/yarn/*
		ln -s ../../../examples/target/scala-$scalaver/jars/$testjar $distdir/examples/jars
		ln -s ../../common/network-yarn/target/scala-$scalaver/spark-${sparkver}-yarn-shuffle.jar $distdir/yarn
		;;
	"upload")
		[[ "$local" = "1" ]] && continue
		rsync -av --copy-unsafe-links --delete --progress $distdir/ $user@$master:$dest
		ssh -t $user@$master "for host in $slaves; do rsync -av --copy-unsafe-links --delete $dest/ \$host:$dest; done"
		;;
	"run")
		if [[ "$local" = "0" ]]; then
			ssh -t $user@$master "sudo rm -rf $traceout; for host in $slaves; do ssh -t \$host sudo rm -rf $traceout; done"
			ssh -t $user@$master sudo -iu notebook $dest/bin/spark-submit --master yarn --class "$testclass" "$dest/examples/jars/$testjar" $iters
		else
			rm -rf $traceout
			$distdir/bin/spark-submit --master yarn --class "$testclass" "$distdir/examples/jars/$testjar" $iters
		fi
		;;
	"collect")
		[[ "$local" = "1" ]] && continue
		ssh -t $user@$master "sudo chown -R dev-user $traceout; for host in $slaves; do ssh -t \$host sudo chown -R dev-user $traceout; done" || true
		ssh -t $user@$master "for host in $slaves; do rsync -av \$host:$traceout/ $traceout; done" || true
		localout="$resultdir/remote"
		rm -rf "$localout"
		mkdir "$localout"
		rsync -av --progress $user@$master:$traceout/ "$localout"
		ssh -t $user@$master "rm -r $traceout; for host in $slaves; do ssh -t \$host rm -r $traceout; done" || true
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
	shift
done

