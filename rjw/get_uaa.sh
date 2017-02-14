#!/bin/bash

if [ "$DEBUG_SCRIPT" = "True" ]
then
    set -x
fi

LOG_FILE=~/logs/get_uaa.out
LOG_RESULT=~/logs/uaa_process.log

if [ ! -d $(dirname $LOG_FILE) ]
then
    echo "Log files/directory do not exist.  Aborting"
    exit 1
fi

log() {
    echo $* >> $LOG_FILE
}
log_result() {
    echo $* >> $LOG_RESULT
}

log "===================================================="
# date >> $LOG_FILE
log $(date)

usage() {
    echo "$0: /local/Directory date_string"
    echo "/local/Directory must be writable"
    echo "date_string = YYYY.MM.DD"
    echo "Issue is $1"
    echo "if SKIP_AWS_DOWNLOAD env variable is set to anything
aws downloads for that date string will be skipped"
    exit 2
}

check_process() {
    avail_logs=$( wc -l $1 | awk '{ printf("%d",$1); }' )
    processed_logs=$( wc -l $2 | awk '{ printf("%d",$1); }' )
    echo $avail_logs $processed_logs
    if [ $avail_logs -eq $processed_logs ]
    then
        return 0
    else
        return 1
    fi
}

check_size() {
    filename=$1
    SIZE1=$(ls -l $filename | awk '{ printf("%d",$5); }')
    sleep 1
    SIZE2=$(ls -l $filename | awk '{ printf("%d",$5); }')
    if [ $SIZE1 -ne $SIZE2 ]
    then
        return 1
    else
        return 0
    fi
}

# presumably this script and the parser are in the same dir when we start
if [ -f parse_uaa.py ]
then
	PARSER=$(pwd)/parse_uaa.py
else
	log "parse_uaa.py not found"
	usage "parse_uaa.py not found"
fi

if [ $# -lt 2 ]
then
    usage "num_args"
fi

if [ ! -d "$1" ]
then
    usage "directory"
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
    log "date_format $2"
    usage "date_format"
else
    DATE_STR=$2
fi
# before we start make sure we have NOT processed this DATE_STR
grep $DATE_STR $LOG_RESULT > /dev/null 2>&1
if [ $? -eq 0 ]
then
    if [ "$SKIP_AWS_DOWNLOAD" = "" ]
    then
	    log "$DATE_STR already processed"
	    log "===================================================="
	    usage "$DATE_STR already processed"
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
PROCESS_DIR=${LOCAL_DIR}/${DATE_PATH}/proc
if [ -d $PROCESS_DIR ]
then
    DIR_EXISTS=true
else
    mkdir $PROCESS_DIR > /dev/null 2>&1
    if [ $? != 0 ]
    then
        log "Cannot create process directory $PROCESS_DIR"
        usage "Cannot create process directory"
    fi
fi
UAA_DATA_DIR=${LOCAL_DIR}/${DATE_PATH}/uaa
if [ -d $UAA_DATA_DIR ]
then
    DIR_EXISTS=true
else 
    mkdir $UAA_DATA_DIR > /dev/null 2>&1
    if [ $? != 0 ]
    then
        log "Cannot create uaa directory $UAA_DATA_DIR"
        usage "Cannot create uaa directory"
    fi
fi

log "PROCESS DIR:" $PROCESS_DIR " UAA_DATA_DIR: " $UAA_DATA_DIR " DIR EXISTS $DIR_EXISTS"
if [ ! -d $UAA_DATA_DIR/meta ]
then
    mkdir $UAA_DATA_DIR/meta >> $LOG_FILE 2>&1
else
    echo > /dev/null
fi
if [ $? != 0 ]
then
    log "error creating meta directory"
    usage "error creating meta directory"
fi

# SKIP_AWS_DOWNLOAD env variable
if [ "$SKIP_AWS_DOWNLOAD" = "" ]
then
	# process was already run it appears
	if [ "$DIR_EXISTS" = true ]
	then

	    # check to be sure files already downloaded
	    num_file=$(ls ${LOCAL_DIR}/${DATE_PATH} | wc -l)
	    # there are 2 directories after a successful run.  Take those into account
	    if [ $num_file -gt 2 ]
	    then
		log "files already downloaded, proceeding from previous run"
	    else
		aws s3 cp s3://pivotal-cloudops-prod-log-archive/prod-logsearch/ $LOCAL_DIR --recursive --exclude '\*' --include "*uaa-${DATE_STR}*" &
		wait
	    fi
	# first time for this date string
	else
	    # get the remote files
	    # aws s3 cp s3://pivotal-cloudops-prod-log-archive/prod-logsearch/ $LOCAL_DIR --recursive --exclude "*" --include "*${DATE_STR}*" &
	    # new aws string per David on 7/5
	    aws s3 cp s3://pivotal-cloudops-prod-log-archive/prod-logsearch/ $LOCAL_DIR --recursive --exclude '\*' --include "*uaa-${DATE_STR}*" &
	    wait
	fi
fi

################
# Pre-processing before we start looping thru the files
################
# go to where the actual files will be placed
log "cd " ${LOCAL_DIR}/${DATE_PATH}
cd ${LOCAL_DIR}/${DATE_PATH}
# clear out files from previous runs if they exist
> avail_logs
if [ -f processed_logs ]
then
    # save the progress
    mv processed_logs ${UAA_DATA_DIR}/meta/${DATE_STR}.$$
fi
> processed_logs
loop_count=0
file_no=1
# create the first aggregation file
> ${UAA_DATA_DIR}/${DATE_STR}-${file_no}.uaap

while [ true ]
do
    # list all available logs in time order ascending (oldest first)
    ls -t -1c *.uaap.gz | sed -e 's/.uaap.gz//' > avail_logs
    for fil in $(cat avail_logs)
    do
        loop_count=$(($loop_count+1))
        if [ $loop_count -eq 100 ]
        then
            loop_count=0
            # compress the filled file, copy to S3 bucket - as process group in background
            ( gzip ${UAA_DATA_DIR}/${DATE_STR}-${file_no}.uaap && aws s3 cp ${UAA_DATA_DIR}/${DATE_STR}-${file_no}.uaap.gz s3://pivotal-cloudops-prod-log-archive-gpfdist-processed && log "${UAA_DATA_DIR}/${DATE_STR}-${file_no}.uaap.gz copied to s3://pivotal-cloudops-prod-log-archive-gpfdist-processed") &
            # increment file_no counter
            file_no=$(($file_no+1))
            # start the new file
            > ${UAA_DATA_DIR}/${DATE_STR}-$file_no.uaap
            # DEBUG log "file_no file counter is now $file_no " 
        fi
        grep "$fil" processed_logs > /dev/null 2>&1
        # hasn't been processed
        if [ $? -ne 0 ]
        then
            # check to see if program failed while processing this file
            if [ -f /tmp/${fil} ]
            then
                # clear the existing file so we don't duplicate, then proceed
                > ${UAA_DATA_DIR}/${fil}.uaa
            fi
            # create a file semaphore
            touch /tmp/${fil}
            # process the file and compress the output
                # zgrep 'uaa-audit' ${fil}.uaap.gz | gzip -c - > ${UAA_DATA_DIR}/${fil}.uaa.gz
                # rjw - removing compress step from here.  bundling data into larger files then compressing
                # python $PARSER ${fil}.uaap.gz | gzip -c - > ${UAA_DATA_DIR}/${fil}.uaap.gz
            python $PARSER ${fil}.uaap.gz > ${UAA_DATA_DIR}/${file}.uaa
            cat ${UAA_DATA_DIR}/${file}.uaa >> ${UAA_DATA_DIR}/${DATE_STR}-${file_no}.uaap
            # remove the semaphore
            rm /tmp/${fil}

            # add file name to the processed logs file so it doesn't get re-processed
            echo $fil >> processed_logs
            mv ${fil}.uaap.gz $PROCESS_DIR
        # file has been processed, mv it so we don't try again, then get the next
        else
            # process interrupted before completion
            if [ -f /tmp/$fil ]
            then
                # allow it to be processed again
                grep -v $fil processed_logs > $$
                mv $$ processed_logs
                # finally remove file semaphore
                rm /tmp/$fil
            else
                mv ${fil}.uaap.gz $PROCESS_DIR
                continue
            fi
        fi
    done

    # break out of enclosing while loop - processing is complete - if the aws process is done and all are processed
    if check_process avail_logs processed_logs
    then
        # break when all logs are processed
        break
    fi
done
# compress and copy the last file up
( gzip ${UAA_DATA_DIR}/${DATE_STR}-${file_no}.uaap && aws s3 cp ${UAA_DATA_DIR}/${DATE_STR}-${file_no}.uaap.gz s3://pivotal-cloudops-prod-log-archive-gpfdist-processed && log "${UAA_DATA_DIR}/${DATE_STR}-${file_no}.uaap.gz copied to s3://pivotal-cloudops-prod-log-archive-gpfdist-processed") 
if [ $? -eq 0 ]
then
    rm $PROCESS_DIR/*
    log "Process completed successfully.  proc files cleaned up"
    log_result "$DATE_STR SUCCESS"
fi
log $(date)
log "===================================================="
