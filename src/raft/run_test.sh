#!/bin/bash

realpath() {
    [[ $1 = /* ]] && echo "$1" || echo "$PWD/${1#./}"
}

TEST_CASE=$1
echo $TEST_CASE

SHELL_FOLDER=$(dirname $(realpath "$0"))
PARENT_SHELL_FOLDER=$(dirname "$SHELL_FOLDER")
GRAND_PARENT_SHELL_FOLDER=$(dirname "$PARENT_SHELL_FOLDER")

export GOPATH=$GRAND_PARENT_SHELL_FOLDER:$GOPATH

PROJECT_ROOT=$SHELL_FOLDER

cd $PROJECT_ROOT

success=0

for i in {1..10000}
do
  echo "running $i test"
  go test -run ${TEST_CASE} > ./raft.log
  if [[ $? -eq 0 ]]
  then
    success=$(( success + 1 ))
  else
    exit 1
  fi
done

echo "success: ${success}"
