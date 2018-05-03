#!/bin/bash

do_test_all(){
	for ((i=0;i<10;i++))
	do 
		go test -run 'TestFigure8Unreliable2C'> aaa$1_$i.log &
	done
}
do_test_all 0 &
do_test_all 1 &
do_test_all 2 &
do_test_all 3 &
do_test_all 4 &
do_test_all 5 &
do_test_all 6 &
do_test_all 7 &
do_test_all 8 &
do_test_all 9 &
do_test_all 10 &
# do_test_all 11 &
# do_test_all 12 &
# do_test_all 13 &
# do_test_all 14 &
# do_test_all 15 &
# do_test_all 16 &
# do_test_all 17 &
# do_test_all 18 &
# do_test_all 19 &
# do_test_all 20 &
# do_test_all 21 &
# do_test_all 22 &
# do_test_all 23 &
# do_test_all 24 &
# do_test_all 25 &
# do_test_all 26 &
# do_test_all 27 &
# do_test_all 28 &
# do_test_all 29 &
# do_test_all 30 &
# do_test_all 31 &
# do_test_all 32 &
# do_test_all 33 &
# do_test_all 34 &
# do_test_all 35 &
# do_test_all 36 &
# do_test_all 37 &
# do_test_all 38 &
# do_test_all 39 &
# do_test_all 40 &
# do_test_all 41 &
# do_test_all 42 &
# do_test_all 43 &
# do_test_all 44 &
# do_test_all 45 &
# do_test_all 46 &
# do_test_all 47 &
# do_test_all 48 &
# do_test_all 49 &
