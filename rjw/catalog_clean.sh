#!/bin/bash 

if [ "$PGDATABASE" = "" ]
then
        if [ "$1" = "" ]
        then
                echo "Either set PGDATABASE or pass in a database name"
                exit 1
        else
                DBNAME=$1
        fi
else
        DBNAME=$PGDATABASE
fi

# DBNAME="<database_name>" 
VCOMMAND="VACUUM ANALYZE"
psql -tc "select '$VCOMMAND' || ' pg_catalog.' || relname || ';' from pg_class a,pg_namespace b where a.relnamespace=b.oid and b.nspname= 'pg_catalog' and a.relkind='r'" $DBNAME | psql -a $DBNAME
