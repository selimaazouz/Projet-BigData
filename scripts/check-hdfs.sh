#!/usr/bin/env bash
set -e

echo "Check: HDFS has been formatted locally"
hadoop_tmp=$("$HADOOP_HOME/bin/hdfs" getconf -confKey hadoop.tmp.dir)
if [ -z $hadoop_tmp ]; then
  echo "  Could not get hadoop.tmp.dir"
  exit 1
else
  echo "  hadoop.tmp.dir is $hadoop_tmp"
  echo "  => Passed"
fi
hdfs_version_file="$hadoop_tmp/dfs/name/current/VERSION"
if [ -f $hdfs_version_file ]; then
  echo "  Found $hdfs_version_file"
  echo "  => Passed"
else
  echo "  Could not find $hdfs_version_file"
  echo "  => HDFS has not been formatted"
  exit 1
fi

echo "Check: HDFS is running on port 9000"

if (echo >/dev/tcp/localhost/9000) &>/dev/null; then
  echo "  Port 9000 is open"
  echo "  => Passed"
else
  echo "  => Port 9000 is not open"
  exit 1
fi

echo "Check: user home exists in HDFS"

if "$HADOOP_HOME/bin/hdfs" dfs -test -d "/user/$USER"; then
  echo "  /user/$USER was found in HDFS."
  echo "  => Passed"
else
  echo "  => /user/$USER does not exist in HDFS."
  exit 1
fi

echo ""
echo "All checks passed."
exit 0
