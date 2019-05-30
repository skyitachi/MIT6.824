#!/bin/bash
if [[ ! -n "$1" ]]; then
    echo "no parameter"
    exit
fi

echo $1
rm -rf $1

for i in {0..10000}
do

 tmp=`go test -run TestSnapshot -timeout 11s`
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

    echo "$1 : ${i} done"
done
echo "${i} round" >> $1
cat $1
