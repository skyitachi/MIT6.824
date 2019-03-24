#!/bin/bash
if [[ ! -n "$1" ]]; then
    echo "no parameter"
    exit
fi

echo $1
rm -rf $1

for i in {0..10000}
do

 tmp=`go test -run Linearizable`
    if [[ "${tmp}" =~ "FAIL" ]];
    then
        echo "${tmp}" > $1
        break
    fi
    if [[ "${tmp}" =~ "warning" ]];
    then
        echo "${tmp}" > $1
        break
    fi

#    tmp=`go test -run TestBasicAgree2B`
#    if [[ "${tmp}" =~ "FAIL" ]];
#    then
#        echo "${tmp}" > $1
#        break
#    fi
#    if [[ "${tmp}" =~ "warning" ]];
#    then
#        echo "${tmp}" > $1
#        break
#    fi
#
#    tmp=`go test -run 2A`
#    if [[ "${tmp}" =~ "FAIL" ]];
#    then
#        echo "${tmp}" > $1
#        break
#    fi
#    if [[ "${tmp}" =~ "warning" ]];
#    then
#        echo "${tmp}" > $1
#        break
#    fi
#
#    tmp=`go test -run 2B`
#    if [[ "${tmp}" =~ "FAIL" ]];
#    then
#        echo "${tmp}" > $1
#        break
#    fi
#    if [[ "${tmp}" =~ "warning" ]];
#    then
#        echo "${tmp}" > $1
#        break
#    fi
#
#    tmp=`go test -run TestPersist12C`
#    if [[ "${tmp}" =~ "FAIL" ]];
#    then
#        echo "${tmp}" > $1
#        break
#    fi
#    if [[ "${tmp}" =~ "warning" ]];
#    then
#        echo "${tmp}" > $1
#        break
#    fi
#
#    tmp=`go test -run TestPersist22C`
#    if [[ "${tmp}" =~ "FAIL" ]];
#    then
#        echo "${tmp}" > $1
#        break
#    fi
#    if [[ "${tmp}" =~ "warning" ]];
#    then
#        echo "${tmp}" > $1
#        break
#    fi
#
#    tmp=`go test -run TestPersist32C`
#    if [[ "${tmp}" =~ "FAIL" ]];
#    then
#        echo "${tmp}" > $1
#        break
#    fi
#    if [[ "${tmp}" =~ "warning" ]];
#    then
#        echo "${tmp}" > $1
#        break
#    fi
#
#    tmp=`go test -run TestFigure82C`
#    if [[ "${tmp}" =~ "FAIL" ]];
#    then
#        echo "${tmp}" > $1
#        break
#    fi
#    if [[ "${tmp}" =~ "warning" ]];
#    then
#        echo "${tmp}" > $1
#        break
#    fi

#    tmp=`go test -run TestUnreliableAgree2C`
#    if [[ "${tmp}" =~ "FAIL" ]];
#    then
#        echo "${tmp}" > $1
#        break
#    fi
#    if [[ "${tmp}" =~ "warning" ]];
#    then
#        echo "${tmp}" > $1
#        break
#    fi
#
#    tmp=`go test -run TestFigure8Unreliable2C`
#    if [[ "${tmp}" =~ "FAIL" ]];
#    then
#        echo "${tmp}" > $1
#        break
#    fi
#    if [[ "${tmp}" =~ "warning" ]];
#    then
#        echo "${tmp}" > $1
#        break
#    fi
#
#    tmp=`go test -run TestReliableChurn2C`
#    if [[ "${tmp}" =~ "FAIL" ]];
#    then
#        echo "${tmp}" > $1
#        break
#    fi
#    if [[ "${tmp}" =~ "warning" ]];
#    then
#        echo "${tmp}" > $1
#        break
#    fi
#
#    tmp=`go test -run TestUnreliableChurn2C`
#    if [[ "${tmp}" =~ "FAIL" ]];
#    then
#        echo "${tmp}" > $1
#        break
#    fi
#    if [[ "${tmp}" =~ "warning" ]];
#    then
#        echo "${tmp}" > $1
#        break
#    fi

    echo "$1 : ${i} done"
done
echo "${i} round" >> $1
cat $1
pkill -9 bash
