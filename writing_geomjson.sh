#!/bin/bash
Script for batch loading data into MongoDB

while getopts v: option
do
    case "${option}"
    in
    v) vector=${OPTARG};;
    esac
done


for d in 1 10 50;
do


		#echo "${chunk}" and "${tiles}"
		args=("-host localhost" "-d research" "-p 27020" "-c ${vector}_${d}_hash_${h}" "-f hash_${h}" "-o /group/vector_datasets/mongo_loading_${vector}_${d}M_hash_${h}_d7_27.csv" "csv --txt /group/vector_datasets/geom_csv/randpoints_${d}m_hashed.csv" "--geom geom_text --delimiter ;") 
		echo ${args[@]}
                python3 mongo_loader.py  ${args[@]}

done



