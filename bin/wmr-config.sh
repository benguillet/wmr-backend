bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

if [ -z "$HADOOP_HOME" ]; then
	if which hadoop &>/dev/null; then
		HADOOP_HOME=$(cd $(dirname $(which hadoop))/.. && pwd)
	else
		echo "ERROR: \$HADOOP_HOME could not be determined!" >&2
		echo "Please set this variable to the correct path." >&2
		exit 1
	fi
fi
export HADOOP_HOME

if [ -z "$WMR_HOME" ]; then
	WMR_HOME=`cd "$bin/.."; pwd`
fi
export WMR_HOME

HADOOP_CLASSPATH=$WMR_HOME/conf:$WMR_HOME/lib
for jar in $WMR_HOME/*.jar; do
	HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$jar
done
for jar in $WMR_HOME/lib/*.jar; do
	HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$jar
done
export HADOOP_CLASSPATH  


export HADOOP_OPTS="$HADOOP_OPTS -Dwmr.home.dir=$WMR_HOME"
