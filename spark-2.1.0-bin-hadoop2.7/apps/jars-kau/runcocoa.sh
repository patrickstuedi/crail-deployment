#!/bin/bash
DS=kdda
FS=crail
SH=crail
BC=crail
MODE=vanilla
NUM_IT=50
H_FACT=0.5

DSS="kdda"
MODES="crail"
NES="4"


#DEFAULT_JAR=/tmp/cocoa-2.0-jar-with-dependencies.jar
DEFAULT_JAR=/tmp/cocoa-2.0.jar
EXTRA_JARS="/home/kau/crail-deployment/spark/extra-jars/breeze_2.11-0.12.jar,/home/kau/crail-deployment/spark/extra-jars/breeze-macros_2.11-0.12.jar"

#export SPARK_HOME=$(readlink -f ~/zac-deployment/spark-2.0.0)
#export CRAIL_HOME=$(readlink -f ~/zac-deployment/crail-1.0)
LOGDIR=runs/cocoa-$(date +%Y%m%d-%H%M%S)
mkdir -p $LOGDIR

# Cleanup
cd $CRAIL_HOME
bin/stop-crail.sh
cd -
for i in $(cat $CRAIL_HOME/conf/slaves); do
    sudo ssh $i "echo 128 > /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages"
    sudo ssh $i rm -rf /local/crail/\*
done


# Start test
for DS in $DSS; do
    for MODE in $MODES; do 
	if [ "$MODE" = "vanilla" ]; then
	    cd $SPARK_HOME
	    rm conf
	    ln -s conf-kau-zac-vanilla conf
	    FS=hdfs
	    SH=spark
	    BC=spark
	    cd -
	else
	    cd $SPARK_HOME
	    rm conf
	    ln -s conf-kau-zac-crail conf
	    FS=crail
	    SH=crail
	    BC=crail
	    cd -
	fi

	for NE in $NES; do
	    if [ "$MODE" = "crail" ]; then
		cd $CRAIL_HOME
		bin/stop-crail.sh
		for i in $(cat $CRAIL_HOME/conf/slaves); do
		    sudo ssh $i "echo $[96*1024] > /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages"
		    sudo ssh $i rm -rf /local/crail/\*
		done
		bin/start-crail.sh
		bin/crail fs -copyFromLocal /home/tpa/$DS /
		bin/crail fs -ls /
		cd -
	    else
		cd $HADOOP_HOME
		bin/hadoop fs -copyFromLocal /home/tpa/$DS /
		bin/hadoop fs -ls /
		cd -
	    fi

#	    JAR=~/crail-deployment/coco-${DS}-${FS}-kryo.jar
#	    JAR=~/zac-deployment/coco-${DS}-${FS}-short-kryo.jar
	    JAR=$DEFAULT_JAR

	    echo "$(date) Submitting Spark Job for $JAR..."
	    timeout 2h $SPARK_HOME/bin/spark-submit \
		--deploy-mode cluster \
		--master yarn \
		--driver-memory 32g \
		--executor-memory 32g  \
		--executor-cores 1 \
		--num-executors $NE \
		--class cocoa.driver --jars $EXTRA_JARS,$JAR $JAR $FS $DS $NUM_IT $H_FACT 2>&1 | tee "${LOGDIR}/log.ne=${NE}.ds=${DS}.fs=${FS}.sh=${SH}.bc=${BC}"
	    echo "$(date) Spark Job Finished."
	    APP=$(cat "${LOGDIR}/log.ne=${NE}.ds=${DS}.fs=${FS}.sh=${SH}.bc=${BC}" | grep "Submitting application" | cut -d " " -f 8)
	    sudo ./collect.sh $APP stdout
	    mv $APP.stdout "${LOGDIR}/stdout.ne=${NE}.ds=${DS}.fs=${FS}.sh=${SH}.bc=${BC}"
	    mv $APP.stderr "${LOGDIR}/stderr.ne=${NE}.ds=${DS}.fs=${FS}.sh=${SH}.bc=${BC}"

	    if [ "$MODE" = "crail" ]; then
		$CRAIL_HOME/bin/crail fs -du -h /spark/shuffle | tee -a "${LOGDIR}/log.ne=${NE}.ds=${DS}.fs=${FS}.sh=${SH}.bc=${BC}"
		$CRAIL_HOME/bin/crail fs -du -h /spark/broadcast | tee -a "${LOGDIR}/log.ne=${NE}.ds=${DS}.fs=${FS}.sh=${SH}.bc=${BC}"
	    fi
	done
    done
done
