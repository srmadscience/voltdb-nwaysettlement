#!/bin/sh

for i in 116 118 120 ; do java -Xmx10048m -jar ../../jars/TestClient.jar `cat /home/ubuntu/.vdbhostnames` $i 100000 9000000 15 15 2 0 > $i.lst ; sleep 120; done



