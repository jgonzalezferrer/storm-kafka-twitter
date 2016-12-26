#!/bin/bash

param=$1

IFS=',' read -a myarray <<< "$param"

 

for i in "${myarray[@]}"

do

        #do whatever you want with the lang

        kafka-topics.sh --create --topic $i --zookeeper cesvima141G1H2:2181 --partition 1 --replication-factor 1

done
