#!/usr/bin/env bash
set -e


echo "Check: environment variables are set"
required_envs=(
  "JAVA_HOME"
  "HADOOP_HOME"
)
for env in "${required_envs[@]}"; do
  if [ -z "${!env}" ]; then
    echo "  $env is not set"
    exit 1
  else
    echo "  $env is set to ${!env}"
  fi
done
echo "  => Passed"

echo "Info: Java version"
java --version |& awk '{ print "  "$0 }'
echo "  => Passed"

echo "Info: Maven version"
mvn --version |& awk '{ print "  "$0 }'
echo "  => Passed"

echo "Info: Hadoop version"
$HADOOP_HOME/bin/hadoop version |& awk '{ print "  "$0 }'
echo "  => Passed"

echo ""
echo "All checks passed."
exit 0
