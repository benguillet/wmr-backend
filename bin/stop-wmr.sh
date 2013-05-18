#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/wmr-config.sh

"$HADOOP_HOME"/bin/hadoop-daemon.sh stop edu.stolaf.cs.wmrserver.ThriftServer
