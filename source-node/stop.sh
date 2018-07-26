#!/bin/sh
BASEDIR=$(dirname $0)
HOSTNAME=$(hostname)

if [ -f "$BASEDIR/$HOSTNAME.pid" ]
then
    PID=$(cat "$BASEDIR/$HOSTNAME.pid")
    kill $PID
    rm "$BASEDIR/$HOSTNAME.pid"
fi
