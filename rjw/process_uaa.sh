#!/bin/bash

if [ "$DEBUG_SCRIPT" = "True" ]
then
    set -x
fi

clean_up() {
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

PROCESS_LOG=~/logs/download_files
# ../pwssec/2016/06/22/2016.06.22.19-eba2b0b7a1ef828907d293a006424822.log.gz
LOG_FILE=~/logs/process_uaa.out

if [ ! -d $(dirname $LOG_FILE) ]
then
    echo "Logs directory does not exist.  Aborting"
    exit 1
fi

log() {
    echo $* >> $LOG_FILE
}

log "========================== $0 run $(date) =========================="

# presumably this script and the parser are in the same dir when we start
if [ -f parse_uaa.py ]
then
	PARSER=$(pwd)/parse_uaa.py
else
	log "parse_uaa.py not found"
	usage "parse_uaa.py not found"
fi

if [ $# -lt 1 ]
then
    usage "num_args"
fi

if [ ! -d "$1" ]
then
    usage "process directory must exist"
else
    LOCAL_DIR=$1
fi

log "Local_DIR is $LOCAL_DIR"
if [ ! -d $LOCAL_DIR/load_files ]
then
    mkdir $LOCAL_DIR/load_files > /dev/null 2>&1
    if [ $? != 0 ]
    then
        log "could not make ${LOCAL_DIR}/load_files dir"
        usage "could not make ${LOCAL_DIR}/load_files dir"
    fi
fi

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
    log "date_format $2"
    usage "date_format"
else
    DATE_STR=$2
fi

# before we start make sure we have NOT processed this DATE_STR
grep "$DATE_STR SUCCESS" $LOG_FILE > /dev/null 2>&1
if [ $? -eq 0 ]
then
    # process was a success previously
    log "$DATE_STR already processed - PROCEEDING"
    log "===================================================="
    exit 0
fi

log $LOCAL_DIR
log $DATE_STR

# slice and dice the date string
DATE_PATH=$(echo $DATE_STR | sed -e "s,\.,/,g")
DATE_PATH_PARTS=$(echo $DATE_STR | awk -F"." '{ printf("%s %s %s", $1, $2, $3 );}' )
LOCAL_PATH=$LOCAL_DIR
CUR_PATH=$(pwd)


# go to where the data is supposed to be
cd $LOCAL_PATH/$DATE_PATH
if [ $? != 0 ]
then
    log "$LOCAL_PATH/$DATE_PATH is not accessible.  Are you sure about the path and date?"
    usage "$LOCAL_PATH/$DATE_PATH is not accessible.  Are you sure about the path and date?"
fi

cnt=1
file_no=1
if [ -f  $LOCAL_DIR/load_files/load_uaa${file_no}-${DATE_STR} ]
then
    > $LOCAL_DIR/load_files/load_uaa${file_no}-${DATE_STR}
fi
# clear out the holding files
> $LOCAL_DIR/load_files/load_uaaONE
> $LOCAL_DIR/load_files/load_uaaTWO

for fil in *.gz
do
    if [ $cnt -gt 100 ]
    then
        if [ -f $LOCAL_DIR/load_files/load_uaa${file_no}-${DATE_STR}.gz ]
        then
            rm -rf $LOCAL_DIR/load_files/load_uaa${file_no}-${DATE_STR}.gz 
        fi
        # wait until the last one is processed
        wait $py_proc
        cat $LOCAL_DIR/load_files/load_uaaONE $LOCAL_DIR/load_files/load_uaaTWO > $LOCAL_DIR/load_files/load_uaa${file_no}-${DATE_STR} 
        log "zipping $LOCAL_DIR/load_files/load_uaa${file_no}-${DATE_STR}"
        gzip $LOCAL_DIR/load_files/load_uaa${file_no}-${DATE_STR} &
        # clear out the holding files
        > $LOCAL_DIR/load_files/load_uaaONE
        > $LOCAL_DIR/load_files/load_uaaTWO
        let "file_no += 1"
        # zero out file if needed
        if [ -f $LOCAL_DIR/load_files/load_uaa${file_no}-${DATE_STR} ]
        then
            > $LOCAL_DIR/load_files/load_uaa${file_no}-${DATE_STR}
        fi
        cnt=0
    fi
    filename=$(basename $fil)
    x=$((cnt%2))
    if [ $x == 1 ]
    then
    # echo "processing $filename"
        wait $py_proc > /dev/null 2>&1 # there may not be a process running, but if there is we need to wait
        python $PARSER $fil >> $LOCAL_DIR/load_files/load_uaaONE &
        py_proc=$!
    else
        python $PARSER $fil >> $LOCAL_DIR/load_files/load_uaaTWO
    fi
    x=0
    let "cnt+=1"
done
# wait $py_proc
# compress last file and wait for it to complete
if [ -f $LOCAL_DIR/load_files/load_uaa${file_no}-${DATE_STR}.gz ]
then
    rm -rf $LOCAL_DIR/load_files/load_uaa${file_no}-${DATE_STR}.gz
fi
# wait for any remaining processing
wait $py_proc > /dev/null 2>&1
# combine the working files
cat $LOCAL_DIR/load_files/load_uaaONE $LOCAL_DIR/load_files/load_uaaTWO > $LOCAL_DIR/load_files/load_uaa${file_no}-${DATE_STR} 
# compress
gzip $LOCAL_DIR/load_files/load_uaa${file_no}-${DATE_STR} 
if [ $? != 0 ]
then
    log "PROCESS $DATE_STR FAILED"
    usage "PROCESS $DATE_STR FAILED"
fi

echo "PROCESS $DATE_STR SUCCESS"
log "PROCESS $DATE_STR SUCCESS"
log "=========================== $0 FINISH $(date) ========================="
exit 0
