#!/bin/sh

cd /home/ubuntu
. ./.profile

cd voltdb-nwaysettlement/scripts

sleep 120
cd ../ddl/
sqlcmd --servers=`cat $HOME/.vdbhostnames` < voltdb-nwaysettlement-createDB.sql
cd ../scripts
java -jar $HOME/bin/addtodeploymentdotxml.jar `cat $HOME/.vdbhostnames` deployment topics.xml
