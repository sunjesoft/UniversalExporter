#!/bin/bash

PACKAGE_HOME=`pwd`



DEPENDENCY_LIBS=${PACKAGE_HOME}/lib/log4j-1.2.15.jar:${PACKAGE_HOME}/lib/json-simple-1.1.1.jar:${PACKAGE_HOME}/lib/opencsv-3.8.jar:${PACKAGE_HOME}/lib/commons-codec-1.11.jar

echo ${DEPENDENCY_LIBS}
javac -g -cp ${DEPENDENCY_LIBS} src/com/sunjesoft/util/*.java

rm -rf com 

mkdir -p com/sunjesoft/util/

cp src/com/sunjesoft/util/*.class com/sunjesoft/util
