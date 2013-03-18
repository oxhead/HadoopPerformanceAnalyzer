#!/bin/bash


GANGLIA=152.7.99.59
CLUSTER=HDFS
HOST=152.7.99.59
MIN15AGO=`date --date="15 minutes ago" +%s` 
CMD="curl --silent 'http://$GANGLIA/ganglia/graph.php?c=$CLUSTER&h=$HOST&v=&m=network_report&cs=$MIN15AGO&csv=1' > a.csv"
echo $CMD
eval $CMD
#curl --silent "http://$GANGLIA/ganglia/graph.php?c=$CLUSTER&h=$HOST&v=&m=&cs=$MIN15AGO&csv=1" | awk -F, '{sum+=$2} END { print "Average = ",sum/NR}'
