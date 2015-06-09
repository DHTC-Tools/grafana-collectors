#!/bin/bash

myName=$1

[[ -z ${myName} ]] && exit

myPathBIN=/usr/local/collect
myPathLOG=/var/log/collect

# The only argument is the partial name of the collection script
myCollect=${myPathBIN}/collect-${myName}.py
myLog=${myPathLOG}/collect-${myName}.log


echo "$(date) Collect Begins"
#python ${myCollect} 2>>${myLog} | nc -w 120 webapps.mwt2.org 2003
python ${myCollect} 2>>${myLog} | pv -q | nc -w 120 webapps.mwt2.org 2003
echo "$(date) Collect Ends"
