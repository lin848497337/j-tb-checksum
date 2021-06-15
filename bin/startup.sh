#!/bin/sh

MEMORY=1024
PERM_MEMORY=256

case "$(uname)" in
Linux)
  BASE_DIR=$(readlink -f $(dirname $0))
  ;;
*)
  BASE_DIR=$(
    cd $(dirname $0)
    pwd
  )
  ;;
esac
BASE_DIR=${BASE_DIR}/../

JAVA_OPTS=" -server -Xms${MEMORY}m -Xmx${MEMORY}m -Xss1m"
JAVA_OPTS="${JAVA_OPTS} -XX:PermSize=${PERM_MEMORY}m -XX:MaxPermSize=${PERM_MEMORY}m"
#JAVA_OPTS="${JAVA_OPTS} -XX:NewSize=128m -XX:MaxNewSize=256m"
JAVA_OPTS="${JAVA_OPTS} -XX:+UseParallelGC"
JAVA_OPTS="${JAVA_OPTS} -XX:-UseAdaptiveSizePolicy -XX:SurvivorRatio=2 -XX:NewRatio=1 -XX:ParallelGCThreads=6"
JAVA_OPTS="${JAVA_OPTS} -XX:-OmitStackTraceInFastThrow"

JAVA_OPTS="${JAVA_OPTS} -Djava.net.preferIPv4Stack=true"
JAVA_OPTS="${JAVA_OPTS} -XX:+PrintGCDetails"
JAVA_OPTS="${JAVA_OPTS} -XX:+PrintGCDateStamps"

CLASSPATH=""

for jar in $(ls ${BASE_DIR}/lib/*.jar); do
  CLASSPATH="${CLASSPATH}:""${jar}"
done

CLASSPATH="${BASE_DIR}/conf:$CLASSPATH"

java ${JAVA_OPTS} -classpath ${CLASSPATH}:. tb.checksum.Bootstrap  1>>defaultLog 2>&1 &
