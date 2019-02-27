#!/bin/bash

do_test_all(){
	for ((i=0;i<10;i++))
	do
		go test -run JoinLeave > ccc$1_$i.log &
	done
}
do_test_all 0 &





