#!/bin/bash
export SPARK_MAJOR_VERSION=2
source $1
spark-submit --jars $JARS_FILE --class $CLASS --master $MASTER --executor-memory $EXECUTOR_MEMORY --total-executor-cores $TOTAL_EXECUTOR_CORES $WRITER_JAR_FILE $WRITER_CONF_FILE
