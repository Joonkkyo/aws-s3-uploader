#!/bin/bash
mkdir logdir

for ((i=1;i<=$1; i++))
do
	mkdir /origin/data$i
	nohup ./file_generator --dircount 6 --depth 3 --filecount 1000 --rate 70 25 5 --target /origin/data$i --csv Y >$(pwd)/logdir/log$i.out &
	sleep 1
	#echo var value :$i
done
