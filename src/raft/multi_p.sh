#!/bin/bash

for i in {1..10}
do
   bash ./multi_test.sh "${i}.txt" &
   sleep 25
done
