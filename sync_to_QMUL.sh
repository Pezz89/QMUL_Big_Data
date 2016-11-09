#!/usr/bin/env bash

echo "Attempting to sync via SSHFS"
if  rsync -Pavzur ./ ~/bert/Big_Data/Assignment_1/ --delete && rsync -Pavzur ~/bert/Big_Data/Assignment_1/ ./ ; then
    echo "Synced via SSHFS"
else
    echo "QMUL SSHFS not mounted, syncing via ssh..."
    rsync -Pavzur ./
    sp319@bert.student.eecs.qmul.ac.uk:/homes/sp319/Big_Data/Assignment_1/ --delete
    rsync -Pavzur sp319@bert.student.eecs.qmul.ac.uk:/homes/sp319/Big_Data/Assignment_1/ ./  
fi 
