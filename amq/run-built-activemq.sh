#!/bin/sh

VERSION=5.17.0-SNAPSHOT
command=${1:-restart}

set -e

logs() {
  echo "Tailing the logs in $PWD"
  cd assembly/target/
  tail -f ./broker1/data/activemq.log ./broker2/data/activemq.log
}

copy_and_start() {
  echo "Running in $PWD"
  cd assembly/target/
  echo "extracting archive"
  tar xf apache-activemq-${VERSION}-bin.tar.gz

  echo "clear existing ActiveMQ"
  rm -rf broker1 broker2
  echo "'deploying' ActiveMQ"
  mv apache-activemq-${VERSION} broker1
  cp -R broker1 broker2

  echo "copying configs..."
  cp ../../amq/my-config.xml ./broker1/conf/activemq.xml
  cp ../../amq/my-log4j.properties ./broker1/conf/log4j.properties

  cp ../../amq/my-config2.xml ./broker2/conf/activemq.xml
  cp ../../amq/my-console2.xml ./broker2/conf/jetty.xml
  cp ../../amq/my-log4j.properties ./broker2/conf/log4j.properties

  sed -i.bak 's/#ACTIVEMQ_DEBUG_OPTS/ACTIVEMQ_DEBUG_OPTS/g' ./broker1/bin/env
  sed -i.bak 's/#ACTIVEMQ_DEBUG_OPTS/ACTIVEMQ_DEBUG_OPTS/g' ./broker2/bin/env
  sed -i.bak 's/address=5005/address=5006/g' ./broker2/bin/env
  # The following JMX adjustments don't seem to work, but this should is a starting point for the future...
  sed -i.bak 's|ACTIVEMQ_SUNJMX_CONTROL=""|ACTIVEMQ_SUNJMX_CONTROL="--jmxurl service:jmx:rmi:///jndi/rmi://127.0.0.1:1099/jmxrmi"|g' ./broker1/bin/env
  sed -i.bak 's|ACTIVEMQ_SUNJMX_CONTROL=""|ACTIVEMQ_SUNJMX_CONTROL="--jmxurl service:jmx:rmi:///jndi/rmi://127.0.0.1:1089/jmxrmi"|g' ./broker2/bin/env

  echo "This line is from startActiveMQ... just kidding, but we're running the broker"
  ./broker1/bin/activemq start
  ./broker2/bin/activemq start

  cd ../../
  logs
}

stop() {
  echo "stopping the broker via stop script"
  set +e
  cd assembly/target/

  ./broker1/bin/activemq stop
  ./broker2/bin/activemq stop
}

kill_mq() {
  echo "killing any process with 'activemq' in it"
  pkill -f 'activemq'
}

case "$command" in
  restart)
    stop & copy_and_start
    ;;
  start)
    copy_and_start
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
