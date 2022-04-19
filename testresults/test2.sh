#!/bin/sh

SETSIZE=2
for i in 36 38 40 42 44 46 48 50 52 54 56 58 60 62 64 66 68 70
do
        CT=`expr $i / ${SETSIZE}`

	java -jar ../../jars/TestClient.jar `cat /home/ubuntu/.vdbhostnames` $CT 100000 9000000 15 15 2 0 > ${i}_a.lst &
	java -jar ../../jars/TestClient.jar `cat /home/ubuntu/.vdbhostnames` $CT 100000 9000000 15 15 2 0 > ${i}_b.lst 
	wait
	sleep 120 
done



