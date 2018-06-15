#!/bin/bash

PACKAGE_HOME=`pwd`
DEPENDENCY_LIBS=${PACKAGE_HOME}/lib/log4j-1.2.15.jar:${PACKAGE_HOME}/lib/json-simple-1.1.1.jar:${PACKAGE_HOME}/lib/opencsv-3.8.jar:${PACKAGE_HOME}/lib/commons-codec-1.11.jar

DRIVERS=${PACKAGE_HOME}/drivers/ojdbc6.jar:${GOLDILOCKS_HOME}/lib/goldilocks6.jar  #Add JDBC Drivers ....   

ALL_LIBS=$DEPENDENCY_LIBS:$DRIVERS

echo ${ALL_LIBS}
java -cp ${ALL_LIBS}:. com.sunjesoft.util.Main exp ./conf/exporter.json
