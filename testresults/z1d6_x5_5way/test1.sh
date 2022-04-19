#!/bin/sh

for i in 24 26 28 30 32 34 36 38 40  ; do java -jar ../jars/TestClient.jar `cat /home/ubuntu/.vdbhostnames` $i 100000 9000000 15 15 5 0 > ${i}_5.lst ; sleep 120; done



