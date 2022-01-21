#!/bin/sh

VERSION=5.17.0-SNAPSHOT
command=${1:-restart}

set -e

logs() {
  cd assembly/target/
  tail -f ./broker1/data/activemq.log ./broker2/data/activemq.log
}

copy_and_start() {
  rm -rf apache-activemq-${VERSION}/bin

  cd assembly/target/
  tar xf apache-activemq-${VERSION}-bin.tar.gz

  rm -rf broker1 broker2
  mv apache-activemq-${VERSION} broker1
  cp -R broker1 broker2

  cp ../../config1.xml ./broker1/conf/activemq.xml
  cp ../../users1.properties ./broker1/conf/users.properties
  cp ../../groups1.properties ./broker1/conf/groups.properties
  cp ../../login.config ./broker1/conf/login.config
#  cp ../../log4j.properties ./broker1/conf/log4j.properties

  cp ../../config2.xml ./broker2/conf/activemq.xml
  cp ../../users2.properties ./broker2/conf/users.properties
  cp ../../groups2.properties ./broker2/conf/groups.properties
  cp ../../login.config ./broker2/conf/login.config
  cp ../../console2.xml ./broker2/conf/jetty.xml
#  cp ../../log4j.properties ./broker2/conf/log4j.properties

  sed -i.bak 's/#ACTIVEMQ_DEBUG_OPTS/ACTIVEMQ_DEBUG_OPTS/g' ./broker1/bin/env
  sed -i.bak 's/#ACTIVEMQ_DEBUG_OPTS/ACTIVEMQ_DEBUG_OPTS/g' ./broker2/bin/env
  sed -i.bak 's/address=5005/address=5006/g' ./broker2/bin/env
  sed -i.bak 's|ACTIVEMQ_SUNJMX_CONTROL=""|ACTIVEMQ_SUNJMX_CONTROL="--jmxurl service:jmx:rmi:///jndi/rmi://127.0.0.1:1099/jmxrmi"|g' ./broker1/bin/env
  sed -i.bak 's|ACTIVEMQ_SUNJMX_CONTROL=""|ACTIVEMQ_SUNJMX_CONTROL="--jmxurl service:jmx:rmi:///jndi/rmi://127.0.0.1:1089/jmxrmi"|g' ./broker2/bin/env

  cd ../../
  start
}

start() {
  cd assembly/target/

  ./broker1/bin/activemq start
  ./broker2/bin/activemq start

  cd ../../
  logs
}

stop() {
  set +e
  cd assembly/target/

  ./broker1/bin/activemq stop
  ./broker2/bin/activemq stop
}

kill_mq() {
  pkill -f 'activemq'
}

case "$command" in
  restart)
    stop & copy_and_start
    ;;
  start)
    copy_and_start
    ;;
  soft_start)
    start
    ;;
  stop)
    stop
    ;;
  kill)
    kill_mq
    ;;
  logs)
    logs
    ;;
  *)
    echo "Unknown command $command"
    exit $?
esac
