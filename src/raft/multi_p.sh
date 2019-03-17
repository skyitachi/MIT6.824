#!/bin/bash

for i in {1..30}
do
   ./multi_test.sh "${i}.txt" &
   sleep 5
done
