#!/bin/sh

for i in 1 5 10 20 22 24 26 28 30 32 34 36 38 40  ; do java -jar NWayClient.jar `cat /home/ubuntu/.vdbhostnames` $i 100000 9000000 15 15 2 0 > $i.lst ; sleep 120; done



