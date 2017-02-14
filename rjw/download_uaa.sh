#!/bin/bash

if [ "$DEBUG_SCRIPT" = "True" ]
then
    set -x
fi

PROCESS_LOG=~/logs/download_files
LOG_FILE=~/logs/download_uaa.out

clean_up() {
    rm -f /tmp/$$local_files /tmp/$$aws_files > /dev/null 2>&1
}

usage() {
    echo "$0: /local/Directory date_string"
    echo "/local/Directory must be writable"
    echo "date_string = YYYY.MM.DD"
    echo "Issue is $1"
    clean_up
    exit 2
}

> $PROCESS_LOG > /dev/null 2>&1
if [ $? != 0 ]
then
    usage "cannot truncate $PROCESS_LOG"
fi

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

if [ "$3" != "force" ]
then
    # before we start make sure we have NOT processed this DATE_STR
    grep "$DATE_STR SUCCESS" $LOG_FILE > /dev/null 2>&1
    if [ $? -eq 0 ]
    then
        log "$DATE_STR already processed - PROCEEDING"
        log "===================================================="
        exit 0
        # download was a success previously
        # usage "$DATE_STR already processed"
    fi
fi

log $LOCAL_DIR
log $DATE_STR
echo $LOCAL_DIR
echo $DATE_STR


# start processing them as they appear
DATE_PATH=$(echo $DATE_STR | sed -e "s,\.,/,g")
DATE_PATH_PARTS=$(echo $DATE_STR | awk -F"." '{ printf("%s %s %s", $1, $2, $3 );}' )
LOCAL_PATH=$LOCAL_DIR
CUR_PATH=$(pwd)
for part in $DATE_PATH_PARTS
do
    cd $LOCAL_PATH
    mkdir $part > /dev/null 2>&1
    LOCAL_PATH="${LOCAL_PATH}/$part"
done

# make sure directory creation all worked
if [ -d ${LOCAL_DIR}/${DATE_PATH} ]
then
    cd $CUR_PATH
else
    log "path creation problem $LOCAL_DIR $DATE_PATH"
    usage "path creation problem $LOCAL_DIR $DATE_PATH"
fi

DIR_EXISTS=false
log "DIR EXISTS $DIR_EXISTS"

# process was already run it appears
if [ "$DIR_EXISTS" = true ]
then
# RJW - Start here
# 2016-07-01 23:15:37  163675409 prod-logsearch/2016/07/01/2016.07.01.22-e09820c2c576a58e82a7a47130952576.log.gz
# 2016-07-01 23:13:38  156657813 prod-logsearch/2016/07/01/2016.07.01.22-ede2e3ce19bc0c604250d782fd54966d.log.gz
# 2016-07-01 12:21:19      92321 prod-logsearch/2016/07/01/uaa-2016.07.01.11-1472f67f81e7c7d74ad7d54d14ebc2b2.log.gz
# 2016-07-01 12:26:51      89628 prod-logsearch/2016/07/01/uaa-2016.07.01.11-173f2d65685301e948b88e87e7ae8f42.log.gz

    # check to be sure files already downloaded
    ls ${LOCAL_DIR}/${DATE_PATH} > /tmp/$$local_files
    # how many? 
    local_num_files=$(ls /tmp/$$local_files | wc -l | awk '{ printf("%d",$1);}')
    aws s3 ls s3://pivotal-cloudops-prod-log-archive/prod-logsearch/${DATE_PATH} --recursive | grep 'uaa' > /tmp/$$aws_files
    aws_num_files=$(ls /tmp/$$aws_files | wc -l | awk '{ printf("%d",$1);}')
    # there are 2 directories after a successful run.  Take those into account
    # if we have the same # of files then skip download else redownload all - too hard to figure out where to start
    if [ $(($local_num_file+2)) -eq $aws_num_files ]
    then
        log "files already downloaded. Skipping"
        AWS_COPY=0
    else
        # aws s3 cp s3://pivotal-cloudops-prod-log-archive/prod-logsearch/ $LOCAL_DIR --recursive --exclude '\*' --include "*uaa-${DATE_STR}*" | awk '{ print $NF }' > $PROCESS_LOG 2>&1
        aws s3 cp s3://pivotal-cloudops-prod-log-archive/prod-logsearch/${DATE_PATH} /var/vcap/store/gpadmin/pwssec/${DATE_PATH} --recursive --exclude="*" --include="*uaa-${DATE_STR}*" | awk '{ print $NF }' > $PROCESS_LOG 2>&1
        AWS_COPY=$?
    fi
# first time for this date string
else
    # get the remote files
    aws s3 cp s3://pivotal-cloudops-prod-log-archive/prod-logsearch/${DATE_PATH} /var/vcap/store/gpadmin/pwssec/${DATE_PATH} --recursive --exclude="*" --include="*uaa-${DATE_STR}*" > /dev/null 2>&1
    AWS_COPY=$?
fi

# RJW - need some sort of if to determine success/failure
if [ $AWS_COPY = 0 ]
then
    echo "DOWNLOAD $DATE_STR SUCCESS"
    log "$DATE_STR SUCCESS"
    # clean up the working files
    rm -f /tmp/$$local_files /tmp/$$aws_files > /dev/null 2>&1
else
    log "$DATE_STR FAILED"
    usage "DOWNLOAD $DATE_STR FAILED"
fi
log "======================== $0 FINISH $(date) ============================"
exit 0
