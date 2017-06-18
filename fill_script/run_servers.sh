#!/bin/bash
rethinkdb &
for ((i=1;i<$1;i++))
do
   rethinkdb --port-offset $i --directory rethinkdb_data$i --join localhost:29015 &
done