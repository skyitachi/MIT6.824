#!/bin/bash

for i in {1..2}
do
   bash ./multi_test.sh "${i}.txt" &
   sleep 10
done
