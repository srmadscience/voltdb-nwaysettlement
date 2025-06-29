#!/bin/sh

cd /home/ubuntu
. ./.profile

cd voltdb-nwaysettlement/scripts

sleep 120
cd ../ddl/
sqlcmd --servers=`cat $HOME/.vdbhostnames` < voltdb-nwaysettlement-createDB.sql

cd ../scripts
java -jar ${JVMOPTS} $HOME/bin/addtodeploymentdotxml.jar `cat $HOME/.vdbhostnames` deployment topics.xml

$HOME/bin/reload_dashboards.sh voltdb-nwaysettlement.json
