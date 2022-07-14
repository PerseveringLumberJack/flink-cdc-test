source /home/ubuntu/.bashrc
export JAVA_HOME=/opt/java/jdk1.8.0_281
export PATH=$PATH:$JAVA_HOME/bin
echo $JAVA_HOME
d=`date  +%Y%m%d`

/opt/flink-1.13.1/bin/flink run -p 4 -c com.leomaster.process.RandomSource -d /opt/trend_batch/flink_batch_trend_oper-0.0.1-SNAPSHOT.jar date=${d}