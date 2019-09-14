#!/bin/bash

starting_win_len=1000
increment=10
parallelism=1
operators=( "sum" "count" "max" "max-count" "arithmetic-mean" "geometric-mean" "sample-stddev" "arg-max" )
rate=-10
slide=1
num_keys=1

win_len=$starting_win_len
for i in `seq 1 4`;
do
    win_len=$(( win_len*increment ))
    for j in `seq 1 32`;
    do
        for k in "${operators[@]}"
        do
            echo ./test_inc -l 1 -r $rate -w $win_len -s $slide -p $j --operator=$k
            ./test_inc -l 1 -r $rate -w $win_len -s $slide -p $j --operator=$k
        done
    done
done

win_len=$starting_win_len
for i in `seq 1 4`;
do
    win_len=$(( win_len*increment ))
    for j in `seq 1 32`;
    do
        for k in "${operators[@]}"
        do
            echo ./test_fat -l 1 -r $rate -w $win_len -s $slide -p $j --operator=$k
            ./test_fat -l 1 -r $rate -w $win_len -s $slide -p $j --operator=$k
        done
    done
done

starting_batch_len=100

win_len=$starting_win_len
for i in `seq 1 4`;
do
    win_len=$(( win_len*increment ))
    batch_len=$starting_batch_len
    for l in `seq 1 4`;
    do
        batch_len=$(( batch_len*increment ))
        for j in `seq 1 32`;
        do
            for k in "${operators[@]}"
            do
                echo ./test_bfat -l 1 -r $rate -w $win_len -s $slide -b $batch_len -p $j --operator=$k --rebuild
                ./test_bfat -l 1 -r $rate -w $win_len -s $slide -b $batch_len -p $j --operator=$k --rebuild
            done
        done
    done
done

win_len=$starting_win_len
for i in `seq 1 4`;
do
    win_len=$(( win_len*increment ))
    batch_len=$starting_batch_len
    for l in `seq 1 4`;
    do
        batch_len=$(( batch_len*increment ))
        for j in `seq 1 32`;
        do
            for k in "${operators[@]}"
            do
                echo ./test_bfat -l 1 -r $rate -w $win_len -s $slide -b $batch_len -p $j --operator=$k
                ./test_bfat -l 1 -r $rate -w $win_len -s $slide -b $batch_len -p $j --operator=$k
            done
        done
    done
done

