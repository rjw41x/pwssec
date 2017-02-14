#!/bin/bash

if [ "$DEBUG_SCRIPT" = "True" ]
then
    set -x
fi

if [ -z "$PGDATABASE" ]
then
    export PGDATABASE=pwssec
fi

clean_ext_tbl () {
    # clean up external table
    psql -c "drop external table ext_uaa_load;"
    if [ $? != 0 ]
    then
        log "FAIL:  failed to drop external table"
    fi
}

clean_up () {
    # clean up the working files
    rm /tmp/gzfiles$$ /tmp/gzrunfiles$$ /tmp/cr_ext_tbl$$.sql /tmp/load$$.sql /tmp/loc$$.awk /tmp/load$$.out > /dev/null 2>&1
    # clean up the external table
    clean_ext_tbl
    # clean up the gpfdist
    kill $gp_proc 
    return
}

usage() {
    echo "$0: /local/Directory date_string"
    echo "/local/Directory must be writable"
    echo "date_string = YYYY.MM.DD"
    echo "Issue is $1"
    clean_up
    exit 2
}

LOG_FILE=~/logs/load_uaa.out

if [ ! -d $(dirname $LOG_FILE) ]
then
    echo "Logs directory does not exist.  Aborting"
    exit 1
fi

log() {
    echo $* >> $LOG_FILE
}
log_f() {
    cat $* >> $LOG_FILE
}

log "========================== START $0 $* run $(date) =========================="

if [ $# -lt 2 ]
then
    usage "num_args"
fi

if [ ! -d "$1" ]
then
    log "FAIL:  process directory must exist"
    usage "process directory must exist"
else
    LOCAL_DIR=$1
    LOAD_DIR=$1/load_files
fi

if [ ! -d $LOAD_DIR ]
then
    log "FAIL:  $LOAD_DIR does not exist, aborting"
    usage "$LOAD_DIR does not exist, aborting"
fi
log "LOAD_DIR is $LOAD_DIR"

# RJW - has to be adjusted on every platform.  several included here
TEST_DATE=$(echo $2 | sed 's,\.,/,g')
# validate the passed in date format
# RJW _ Mac only version
# date -j -f "%Y.%m.%d" "$2" > /dev/null 2>&1
# another version
# date  -d $TEST_DATE > /dev/null 2>&1
date --date="$TEST_DATE"  > /dev/null 2>&1
if [ $? == 1 ]
then
    log "FAIL:  date_format $2"
    usage "date_format"
else
    DATE_STR=$2
fi

grep "$DATE_STR SUCCESS" $LOG_FILE > /dev/null 2>&1
if [ $? = 0 ]
then
    log "load already run successfully for $DATE_STR, aborting load"
    echo "load already run successfully for $DATE_STR, aborting load"
    exit 0
fi

# slice and dice the date string
DATE_PATH=$(echo $DATE_STR | sed -e "s,\.,/,g")
DATE_PATH_PARTS=$(echo $DATE_STR | awk -F"." '{ printf("%s %s %s", $1, $2, $3 );}' )
LOCAL_PATH=$LOCAL_DIR
CUR_PATH=$(pwd)

# go to where the data is supposed to be
cd $LOAD_DIR
if [ $? != 0 ]
then
    log "FAIL:  cannot cd to $LOAD_DIR"
    usage "cannot cd to $LOAD_DIR"
fi

ls -1c *.gz > /tmp/gzfiles$$
ls -1c *${DATE_STR}*.gz > /tmp/gzrunfiles$$
if [ $(wc -l /tmp/gzfiles$$ | awk '{ print $1 }') -gt $(wc -l /tmp/gzrunfiles$$ | awk '{ print $1 }') ]
then
    log "extra files in $LOAD_DIR"
    # echo "There are $(wc -l /tmp/gzfiles$$ | awk '{print $1}') extra files"  >> $LOG_FILE
    log "end extra files in $LOAD_DIR"
fi

# kill any existing gpfdist procs on 8081 - supports reloading a days data
proc=$(ps -ef | grep '\-p 8081' | awk '{ print $2 }' 2> /dev/null )
kill -HUP $proc > /dev/null 2>&1
# start up gpfdist procs for loading
gpfdist -d $(pwd) -p 8081 -l ~/logs/gpfd.log 2> /dev/null &
sleep 2
gp_proc=$!
ps -fp $gp_proc > /dev/null 2>&1
if [ $? != 0 ]
then
    ps -fu gpadmin 2> /dev/null | grep "gpfd.*8081" > /dev/null 2>&1
    if [ $? != 0 ]
    then
        log "FAIL: gpfdist -d $(pwd) -p 8081 did not start properly"
        usage "gpfdist -d $(pwd) -p 8081 did not start properly"
    else
        log "gpfdist already started on port 8081"
    fi
fi

# create the external table
# create location string
cat > /tmp/loc$$.awk << EOF
BEGIN { LOC_STRING=""; } { 
    if( LOC_STRING == "" )
        LOC_STRING=sprintf("%s%s/%s%s", quote, "gpfdist://mdw:8081", \$1, quote );
    else
        LOC_STRING=sprintf("%s, %s%s/%s%s", LOC_STRING, quote, "gpfdist://mdw:8081", \$1, quote );
} END { print LOC_STRING }
EOF
LOCATION=$(awk -v quote="'" -f /tmp/loc$$.awk /tmp/gzrunfiles$$ )
if [ "$DEBUG_SCRIPT" = "True" ]
then
    log "$LOCATION"
fi

# create ext table sql
cat > /tmp/cr_ext_tbl$$.sql << EOF
DROP EXTERNAL TABLE IF EXISTS ext_new_uaa;
CREATE EXTERNAL TABLE ext_new_uaa ( 
at_raw text,
tags varchar,
timer varchar,
log_ts timestamp ,
idx int,
src_ip inet,
job varchar,
vm varchar,
host varchar,
program varchar,
deployment varchar,
region_name varchar,
latitude varchar,
geo_ip inet,
area_code varchar,
continent_code varchar,
country_code3 varchar,
country_code2 varchar,
city_name varchar,
longitude varchar,
timezone varchar,
country_name varchar,
postal_code varchar,
real_region_name varchar,
dma_code varchar,
location varchar,
origin varchar,
thread_name varchar,
entry_type varchar,
pid int,
remote_address inet,
identity_zone_id varchar,
data varchar,
principal varchar,
raw_text text)
LOCATION ( $LOCATION )
FORMAT 'TEXT' ( DELIMITER '|' NULL '' )
LOG ERRORS INTO err_uaa_load segment reject limit 1000;
EOF
psql -f /tmp/cr_ext_tbl$$.sql
if [ $? != 0 ]
then
    log "FAIL:  failed to create external table"
    cat /tmp/cr_ext_tbl$$.sql >> $LOG_FILE
    usage "Failed to create external table"
fi
# create load sql
# echo "insert into uaa_parsed (raw, geoip, tags, uaa, source, host, ip, vm, log_timestamp, principal, event_list, origin_text, caller, clientid, client, uaa_user, remote_addr, batch_id ) 
# select raw, geoip, tags, uaa, source, host, ip, vm, case when log_timestamp = '' then NULL::timestamp else log_timestamp::timestamp end, principal, event_list, origin_text, caller, clientid, client, uaa_user, remote_addr, '$DATE_STR' from  ext_uaa_load;" > /tmp/load$$.sql

echo "insert into new_uaa ( at_raw, tags, timer, log_ts, idx, src_ip, job, vm, host, program, deployment, region_name, latitude, geo_ip,
area_code, continent_code, country_code3, country_code2, city_name, longitude, timezone, country_name, postal_code,
real_region_name, dma_code, location, origin, thread_name, entry_type, pid, remote_address, identity_zone_id, data,
principal, raw_text, batch_id ) select *, '$DATE_STR' from ext_new_uaa;" > /tmp/load$$.sql

psql -f /tmp/load$$.sql > /tmp/load$$.out 2>&1
if [ $? != 0 ]
then
    log "FAIL:  sql load failed"
    log_f /tmp/load$$.sql
    usage "FAIL:  sql load failed"
else
    grep 'ERROR' /tmp/load$$.out > /dev/null 2>&1
    if [ $? = 0 ]
    then
        log "LOAD FAILED"
        log_f /tmp/load$$.out
        usage "LOAD FAILED"
    fi
fi

clean_up
echo "SUCCESS: $DATE_STR loads worked"
log "SUCCESS: $DATE_STR loads worked"
log "=========================== FINISH $0 $(date) ========================="
exit 0
