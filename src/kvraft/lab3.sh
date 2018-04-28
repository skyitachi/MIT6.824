#!/bin/bash

do_test_all(){
	for ((i=0;i<5;i++))
	do 
		go test -run 'TestUnreliable3A' > bbb$1_$i.log  
	done
}
do_test_all 0 &


