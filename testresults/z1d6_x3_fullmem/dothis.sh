#!/bin/sh

for usercount in 2 3 4 5; do for i in 20 22 24 26 28 30 32 34 36 38 40  ; do java -jar TestClient.jar vdb1,vdb2,vdb3  $i 100000 9000000 15 15 $usercount 0 > ${i}_${usercount}.lst ; sleep 120; done; done 
