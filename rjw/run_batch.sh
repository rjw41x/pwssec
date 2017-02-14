#!/bin/bash

# run file to string together download, process and load of uaa data
# each script exits with 0 on success, non-0 on failure

if [ "$DEBUG_SCRIPT" = "True" ]
then
    set -x
fi

SKIP_D=true
SKIP_P=true
LOG_FILE=~/logs/run_all_uaa.out
LOAD_DIR_NAME="load_files"

usage() {
    echo "$0: /local/Directory date_string"
    echo "/local/Directory must be writable"
    echo "date_string = YYYY.MM.DD"
    echo "Issue is $1"
    exit 2
}

if [ ! -d $(dirname $LOG_FILE) ]
then
    usage "Log directory $(dirname $LOG_FILE) does not exist.  Aborting"
    exit 1
fi

log() {
    echo $* >> $LOG_FILE
}

log "=================== $0 run at $(date)  ====================="

if [ $# -lt 2 ]
then
    usage "num_args"
fi

if [ ! -d "$1" ]
then
    usage "directory $1 down not exist"
else
    LOCAL_DIR=$1
fi

TEST_DATE=$(echo $2 | sed 's,\.,/,g')
# validate the passed in date format
# RJW _ Mac only version
# date -j -f "%Y.%m.%d" "$2" > /dev/null 2>&1
# another version
# date  -d $TEST_DATE > /dev/null 2>&1
date --date="$TEST_DATE"  > /dev/null 2>&1
if [ $? == 1 ]
then
    log "date_format failed - $2"
    usage "date_format"
else
    DATE_STR=$2
    DATE_PATH=$(echo $DATE_STR | sed -e "s,\.,/,g")
fi

if [ "$3" = "force" ]
then
    rm -rf ${LOCAL_DIR}/${DATE_PATH}
    rm -f ${LOCAL_DIR}/${LOAD_DIR_NAME}/*${DATE_STR}*
elif [ "$3" = "PROCESS" ]
then
    SKIP_D=false
elif [ "$3" = "LOAD" ]
then
    SKIP_D=false
    SKIP_P=false
fi

# test to see if we have run this date before - abort unless force flag is on

log "Running from $LOCAL_DIR using $DATE_STR"
$SKIP_D && ./download_uaa.sh $LOCAL_DIR $DATE_STR "$3"
if [ $? -ne 0 ] && $SKIP_D
then
    log "DOWNLOAD $LOCAL_DIR $DATE_STR FAILED"
    usage "DOWNLOAD $LOCAL_DIR $DATE_STR FAILED"
else
    log "DOWNLOAD $LOCAL_DIR $DATE_STR SUCCESS"
fi
$SKIP_P && ./process_uaa.sh $LOCAL_DIR $DATE_STR "$3"
if [ $? -ne 0 ]
then
    log "PROCESS $LOCAL_DIR $DATE_STR FAILED"
    usage "PROCESS $LOCAL_DIR $DATE_STR FAILED"
else
    log "PROCESS $LOCAL_DIR $DATE_STR SUCCESS"
fi
./load_uaa.sh $LOCAL_DIR $DATE_STR "$3"
if [ $? -ne 0 ]
then
    log "LOAD $LOCAL_DIR $DATE_STR FAILED"
    usage "LOAD $LOCAL_DIR $DATE_STR FAILED"
else
    log "LOAD $LOCAL_DIR $DATE_STR SUCCESS"
fi
log "======================== analyzedb start $(date) ============================"
analyzedb -d pwssec
status=$?
if [ $status -ne 0 ]
then
    log "======================== analyzedb problem status: $status ========================"
else
    log "======================== analyzedb stop $(date) ============================"
fi
log "======================== $0 FINISH $DATE_STR $(date) ============================"
exit 0
