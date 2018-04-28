#!/bin/bash
for ((i=0;i<80;i++))
do
	go test -run 2C > tmp
	cat tmp >> temp
	cat tmp | grep PASS >> result.txt
done
wc -l  result.txt

