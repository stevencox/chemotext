$SPARK_HOME/conf/spark-env.sh

LOGS=/home/scox/dev/chemotext/logs

rm -rf $LOGS
export JAVA_HOME=/usr/java/jdk1.7.0_60

#This configuration is known to work UPPMAX milou
export SPARK_LOCAL_DIRS=$LOGS/local #$(mktemp -d)
export SPARK_WORKER_DIR=$LOGS/worker #$(mktemp -d)
export SPARK_LOG_DIR=$LOGS/tmp #"$TMPDIR/spark-logs"
mkdir -p $SPARK_LOG_DIR
mkdir -p $SPARK_LOCAL_DIRS
mkdir -p $SPARK_WORKER_DIR

echo local: $SPARK_LOCAL_DIRS
echo worker: $SPARK_WORKER_DIRS