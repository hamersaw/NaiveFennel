#!/bin/sh
APPLICATION="nfennel-source-node"
VERSION="0.1-SNAPSHOT"

JAVA_OPTS="-Dorg.slf4j.simpleLogger.defaultLogLevel=info"

CLASSPATH=""
BASEDIR=$(dirname $0)
HOSTNAME=$(hostname)
if [ -f $BASEDIR/build/libs/$APPLICATION-$VERSION.jar ]; then
    CLASSPATH="$BASEDIR/build/libs/$APPLICATION-$VERSION.jar"
else
    echo "unable to find $APPLICATION-$VERSION.jar."
    exit 1
fi

java -cp $CLASSPATH $JAVA_OPTS com.bushpath.nfennel.source_node.Main $@ > $BASEDIR/$HOSTNAME.log 2>&1 &
echo $! > $BASEDIR/$HOSTNAME.pid
