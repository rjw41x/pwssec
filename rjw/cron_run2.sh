#!/bin/bash
# Cron starts before midnight (set to 11:20 PM currently) to get the day (date) to process.  
# Begins processing one hour later - after midnight presumably
# clock on the machine is off MT time by 6 hours.  Not sure how it is set or where to
# changed offset to sleep 6.5 hours to insure processing is truly done for the day

# set the environment
source ~/.bashrc

YR=$(date +%Y)
MO=$(date +%m)
DY=$(date +%d)

PROC_DIR=/var/vcap/store/gpadmin/pwssec
DATE_STR="${YR}.${MO}.${DY}"

sleep 22200
cd $PROC_DIR/../rjw

./run_batch2.sh  $PROC_DIR $DATE_STR
