#!/bin/bash
Script for batch loading data into MongoDB

while getopts v: option
do
    case "${option}"
    in
    v) vector=${OPTARG};;
    esac
done


for d in 1 10 50 100;
do
	for h in 2 4 6 8;
        do
		#echo "${chunk}" and "${tiles}"
		args=("-host localhost" "-d research" "-p 27020" "-c ${vector}_${d}_hash_${h}" "-f hash_${h}" "-o /group/vector_datasets/mongo_loading_${vector}_${d}M_hash_${h}_d7_17.csv" "csv --txt /group/vector_datasets/geom_csv/synthetic_households_${d}per_hashed.csv" "--geom geom_text --delimiter ;") 
		echo ${args[@]}
                #python3 mongo_loader.py  ${args[@]}
	done
done



