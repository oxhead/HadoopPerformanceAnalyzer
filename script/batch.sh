#!/bin/bash

number=10
size=1GB
job=terasort
pattern=.*kinmen.*
memory=512
map_size="-Dmapreduce.map.memory.mb=${memory}"

function usage
{
    echo "usage: batch [[[-n jobs ] [-s sizes]] | [-h]]"
}

function submit_jobs
{
for (( i=1; i<=${number}; i++ ))
do

(
        if [ ${job} == "benchmark" ]
        then   
                data_choice=$(($i%5+1))
                echo "NetworkIntensiveJob ${i}: ${size}"
                cmd="bin/hadoop jar ~/HadoopBenchmark.jar my.oxhead.hadoop.benchmark.NetworkIntensiveJob -Dmapred.job.map.memory.mb=${memory} /dat
aset/wikipedia_${size}_${data_choice} /benchmark/output_${size}_${i}"
                #echo $cmd
                eval $cmd
                continue
        fi

        data_choice=$(($i%5+1))
        if [ ${job} == "hybrid" ]
        then
                job_type=$(($i%3))
                if [ ${job_type} == 0 ]
                then
                        echo "Terasort ${i}: ${size}"
                        cmd="bin/hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar terasort ${map_size} /terasort/input_${size}_${data_choice} /terasort/output_${size}_${i}"
                        eval $cmd
                elif [ ${job_type} == 1 ]
                then
                        echo "Grep ${i}: ${size}, ${pattern}"
                        cmd="bin/hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar grep ${map_size} /dataset/wikipedia_${size}_${data_choice} /grep/output_${size}_${i} ${pattern}"
                        eval $cmd
                elif [ ${job_type} == 2 ]
                then
                        echo "Wordcount ${i}: ${size}"
                        cmd="bin/hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar wordcount ${map_size} /dataset/wikipedia_${size
}_${data_choice} /wordcount/output_${size}_${i}"
                        eval $cmd
                fi
        elif [ ${job} == "terasort" ]
        then
                echo "Terasort ${i}: ${size}"
                cmd="bin/hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar terasort ${map_size} /terasort/input_${size}_${data_choice} /terasort/output_${size}_${i}"
                eval $cmd
        elif [ ${job} == "grep" ]
        then
                echo "Grep ${i}: ${size}, ${pattern}"
                cmd="bin/hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar grep ${map_size} /dataset/wikipedia_${size}_${data_choice} /grep/output_${size}_${i} ${pattern}"
                eval $cmd
        elif [ ${job} == "wordcount" ]
        then
                echo "Wordcount ${i}: ${size}"
                cmd="bin/hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar wordcount ${map_size} /dataset/wikipedia_${size}_${data_choice} /wordcount/output_${size}_${i}"
                eval $cmd
        fi
) 2>&1 &

done
}

###### main

while [ "$1" != "" ]; do
    case $1 in
        -n | --number )        shift
                                number=$1
                                ;;
        -s | --size )           shift
                                size=$1
                                ;;
        -j | --job )            shift
                                job=$1
                                ;;
        -p | --pattern )        shift
                                pattern=$1
                                ;;
        -m | --memory )         shift
                                memory=$1
                                ;;
        -h | --help )           usage
                                exit
                                ;;
        * )                     usage
                                exit 1
    esac
    shift
done

submit_jobs
