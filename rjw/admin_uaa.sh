#!/bin/bash

# script to administer the pws sec environment
# originally built to admin the uaa logs.  
# Can and should be extended to cover additional data sources

if [ "$DEBUG_SCRIPT" = "True" ]
then
    set -x
fi

PROCESS_LOG=~/logs/download_files
RUN_LOG=/var/vcap/store/gpadmin/logs/run_all_uaa.out
ADMIN_LOG_FILE=~/logs/admin_uaa.out
declare -a MONTHS=('' 'January' 'February' 'March' 'April' 'May' 'June' 'July' 'August' 'September' 'October' 'November' 'December')

usage() {
    echo "$0: /local/Directory"
    echo "Issue is $1"
    exit 2
}

if [ ! -d $(dirname $LOG_FILE) ]
then
    usage "Log directory $(dirname $LOG_FILE) does not exist.  Aborting"
    exit 1
fi

if [ ! -d $1 ]
then
    usage "Directory path not valid"
else
    FILE_PATH=$1
fi
t_lines=$(tput lines)

log() {
    echo $* >> $ADMIN_LOG_FILE
}

log "=================== $0 run at $(date)  ====================="

if [ $# -lt 1 ]
then
    usage "num_args"
fi

recent_jobs() {
    TMP_RUN_LOG=/tmp/run_log$$
    # make sure jobs have not been cleaned before presenting them
    for fil_date in $( grep 'FINISH' $RUN_LOG | awk '{ print $4 }' )
    do
        DATE_PATH=$(echo $fil_date | sed -e "s,\.,/,g")
        file_count=$( ls $FILE_PATH/$DATE_PATH/*.gz 2> /dev/null | wc -l | awk '{ print $1 }' )
        # if there are still files in the directory add it to the recent jobs file
        if [ $file_count -ne 0 ]
        then
            echo "================" >> $TMP_RUN_LOG
            grep $fil_date $RUN_LOG >> $TMP_RUN_LOG
        fi
    done

    awk 'BEGIN { first=1; } {
        if( $1 ~ /====/ && $3 != "FINISH" ) {
            if( first )
                first=0;
            else {
                if( download == "SUCCESS" && process = "SUCCESS" && load == "SUCCESS" ) {
                    success="SUCCESS";
                    reason="NONE";
                }
                else {
                    success="FAILED";
                    if( download != "SUCCESS" )
                        reason="DOWNLOAD";
                    else if( process != "SUCCESS" )
                        reason="PROCESS";
                    else if( load != "SUCCESS" )
                        reason="LOAD";
                }
                printf("%s on %s: %s(%s)\n", date_str, run_date, success, reason )
                download="X";
                process="X";
                load="X";
                success="X";
                reason="X";
            }
            program=$2;
            run_date=$6" "$7" "$8;
        }
        else if( $1 == "Running" ) {
            dir=$3;
            date_str=$5
        }
        else if( $1 == "DOWNLOAD" )
            download=$NF;
        else if( $1 == "PROCESS" )
            process=$NF;
        else if( $1 == "LOAD" )
            load=$NF;
    }' $TMP_RUN_LOG
           
    rm $TMP_RUN_LOG
}
# =================== ./run_batch.sh run at Wed Sep 7 00:48:52 UTC 2016 =====================
# Running from /var/vcap/store/gpadmin/pwssec/ using 2016.07.05
# DOWNLOAD /var/vcap/store/gpadmin/pwssec/ 2016.07.05 FAILED
# =================== ./run_batch.sh run at Wed Sep 7 00:49:25 UTC 2016 =====================
# Running from /var/vcap/store/gpadmin/pwssec/ using 2016.07.05
# DOWNLOAD /var/vcap/store/gpadmin/pwssec/ 2016.07.05 SUCCESS
# PROCESS /var/vcap/store/gpadmin/pwssec/ 2016.07.05 SUCCESS
# LOAD /var/vcap/store/gpadmin/pwssec/ 2016.07.05 SUCCESS
# ======================== ./run_batch.sh FINISH 2016.07.05 Wed Sep 7 01:00:34 UTC 2016 ============================

rm_files() {
    rm ${FILE_PATH}/${2}/uaa*.log.gz > /dev/null 2>&1
    if [ $? != 0 ]
    then
        echo "No files removed, listing directory"
        ls ${FILE_PATH}/${2}/
    else
        echo "load files $2 removed"
    fi
    rm ${FILE_PATH}/load_files/*${1}* > /dev/null 2>&1
    if [ $? != 0 ]
    then
        echo "No files removed, listing directory"
        ls ${FILE_PATH}/load_files/*${1}*
    else
        echo "raw files $1 removed"
    fi
}

do_batch() {
    DATE_STR=$1
    DATE_PATH=$(echo $DATE_STR | sed -e "s,\.,/,g")
    num_raw_files=$(ls ${FILE_PATH}/$DATE_PATH/uaa*.log.gz | wc -l | awk '{ print $1 }')
    num_load_files=$(ls ${FILE_PATH}/load_files/*${DATE_STR}*.gz | wc -l | awk '{ print $1 }')
    echo "There are $num_raw_files raw files for batch $DATE_STR"
    echo "There are $num_load_files load files for batch $DATE_STR"
    echo -n "Would you like to remove these files? "
    read resp
    if [ "$resp" = "y" ]  || [ "$resp" = "Y" ]
    then
        rm_files $DATE_STR $DATE_PATH
    fi
}
val_month() {
    month=$1
    # RJW - Start here
}

clean_mo() {
    # get a listing of the raw files
    echo "Completed Jobs by Month"
    cnt=0
    for mo_str in $(grep "FINISH" $RUN_LOG | awk '{ x=split($4,dt,"."); print dt[2]; }' | sort -u)
    do
    # 2016.10.01
        cnt=$((cnt+1))
        mo_val=$(echo $mo_str | awk '{ print $1 + 0; }')
        # echo $mo_val # debug
        # echo "${cnt}. $mo_str"
        echo ${mo_str}. ${MONTHS[$mo_val]}
    done
    echo 'enter q or x to skip'
    echo -n "Select a month: "
    read month
    run_val=$(val_month "$month")
    if [ $run_val != "x" ]
    then
        do_month $run_val
    else
        echo "Bad Selection"
    fi
}

clean_up() {
    # YR=$(date +%Y)
    # MO=$(date +%m)
    # DY=$(date +%d)
    # get a listing of the raw files
    echo "Completed Jobs by Date"
    cnt=0
    declare -a BATCH
    for date_str in $(grep "FINISH" $RUN_LOG | awk '{ print $4 }')
    do
        cnt=$((cnt+1))
        echo "${cnt}. Load $date_str"
        BATCH[$cnt]=$date_str
    done
    echo -n "Select a batch by number: "
    read batch
    if [ "$batch" != "" ] && [ ${#BATCH[$batch]} = 10 ]
    then
        do_batch ${BATCH[$batch]}
    else
        echo "Bad Selection"
    fi
}

run_batch() {
    echo -n "Enter the path to the download directory (return for default: $FILE_PATH): "
    read dir
    if [ "$dir" = "" ]
    then
        dir=$FILE_PATH
    elif [ ! -d "$dir" ]
    then
        echo "download directory is not a directory"
        return
    fi
    if [ -z "$date_str" ]
    then
        date_str="YYYY.MM.DD"
    fi
    echo -n "Enter the date string ($date_str): "
    read date_str
    # RJW - has to be adjusted on every platform.  several included here
    TEST_DATE=$(echo $date_str | sed 's,\.,/,g')
    # validate the passed in date format
    # RJW _ Mac only version
    # date -j -f "%Y.%m.%d" "$2" > /dev/null 2>&1
    # another version
    # date  -d $TEST_DATE > /dev/null 2>&1
    date --date="$TEST_DATE"  > /dev/null 2>&1
    if [ $? == 1 ]
    then
        echo "invalid date_format $2"
        return
    else
        DATE_STR=$date_str
    fi

    echo -n "./run_batch.sh $dir $date_str  Run? : "
    read go
    if [ "$go" = "y" ] || [ "$go" = "Y" ]
    then
        ./run_batch.sh $dir $DATE_STR 
        if [ $? != 0 ]
        then
            echo "run_batch.sh $dir $DATE_STR failed"
        fi
    else
        echo "skipping run_batch"
    fi
}

menu() {
    clear
    echo "Options"
    echo "1. Review Recent Jobs"
    echo "2. Database Status"
    echo "3. Clean Up Data"
    echo "4. Run Batch"
    echo "5. Catalog Clean"
    echo "6. Clean Up by Month"
    echo "7. Quit"
    read choice
    get_choice $choice
    return 1
}

get_choice() {
    case $1 in
    # recent jobs
    1 )
        recent_jobs
        echo 'hit return to continue'
        read x
    ;;
    # db status
    2 )
        gpstate
        echo 'hit return to continue'
        read x
    ;;
    # clean up data
    3)
        clean_up
        echo 'hit return to continue'
        read x
    ;;
    4)
        run_batch
        echo 'hit return to continue'
        read x
    ;;
    5)
        num_jobs=$( psql -t -c "select count(*) from pg_stat_activity;" | awk '{ print $1; exit 0 }' )
        if [ $num_jobs -ne 1 ]
        then
            echo "database is currently in use.  Try again later"
            continue
        fi
        if [ -f catalog_clean.sh ]
        then 
            ./catalog_clean.sh 
        else
            echo "Catalog script missing"
        fi
        echo 'hit return to continue'
        read x
    ;;
    # clean up by month
    6)
        clean_mo
        echo 'hit return to continue'
        read x
    ;;
    # quit
    7|q|Q)
        rm /tmp/clean_files$$ > /dev/null 2>&1
        exit 0
    ;;
    "*" )
        menu
    ;;
    esac
}
while true
do
    menu
done
