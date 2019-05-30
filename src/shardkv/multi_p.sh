#!/bin/bash

for i in {1..1}
do
   bash ./multi_test.sh "${i}.txt" &
   sleep 5
done
