#!/bin/bash

PACKAGE_HOME=`pwd`
DEPENDENCY_LIBS=${PACKAGE_HOME}/lib/log4j-1.2.15.jar:${PACKAGE_HOME}/lib/json-simple-1.1.1.jar

DRIVERS=${PACKAGE_HOME}/drivers/ojdbc6.jar:${PACKAGE_HOME}/drivers/sundb6.jar  #Add JDBC Drivers ....   

ALL_LIBS=$DEPENDENCY_LIBS:$DRIVERS

echo ${ALL_LIBS}
java -cp ${ALL_LIBS}:. com.sunjesoft.util.AES $1
