#!/bin/sh
BASEDIR=$(dirname $0)

# clone okde-java repository
mkdir $BASEDIR/libs
git clone https://github.com/hamersaw/rutils.git $BASEDIR/libs/rutils

# build project 
DIR=`pwd`
cd $BASEDIR/libs/rutils
./setup.sh
gradle build
cd $DIR

# copy jar and LICENSE file to libs
cp $BASEDIR/libs/rutils/build/libs/rutils-0.1-SNAPSHOT.jar $BASEDIR/libs
cp $BASEDIR/libs/rutils/libs/okde-java-license $BASEDIR/libs/

# remove git repository
rm -rf $BASEDIR/libs/rutils
