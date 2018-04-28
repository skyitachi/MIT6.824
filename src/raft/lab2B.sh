#!/bin/bash

do_test_all(){
	for ((i=0;i<5;i++))
	do 
		go test > aaa$1_$i.log &
	done
}
do_test_all 0 &
do_test_all 1 &
do_test_all 2 &
do_test_all 3 &
do_test_all 4 &

