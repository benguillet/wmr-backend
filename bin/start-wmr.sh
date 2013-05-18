#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/wmr-config.sh

# Check if Hadoop is running
if ! ps auxx | grep "hadoop" | grep -vq grep; then
    echo "ERROR: you must start Hadoop before starting WebMapReduce" >&2
    echo "Please run (as the hadoop user) $HADOOP_HOME/bin/start-all.sh" >&2
    exit 1
fi

"$HADOOP_HOME"/bin/hadoop-daemon.sh start edu.stolaf.cs.wmrserver.ThriftServer
