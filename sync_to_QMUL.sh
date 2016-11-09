#!/usr/bin/env bash

echo "Attempting to sync via SSHFS"
if  rsync -Pavzur ./ ~/bert/Big_Data/Assignment_1/ --exclude './build' --exclude './.git' --delete && rsync -Pavzur ~/bert/Big_Data/Assignment_1/ ./ --exclude './build' --exclude './.git' ; then
    echo "Synced via SSHFS"
else
    echo "QMUL SSHFS not mounted, syncing via ssh..."
    rsync -Pavzur ./ sp319@bert.student.eecs.qmul.ac.uk:/homes/sp319/Big_Data/Assignment_1/ --delete --exclude './build' --exclude './.git'
    rsync -Pavzur sp319@bert.student.eecs.qmul.ac.uk:/homes/sp319/Big_Data/Assignment_1/ ./ --exclude './build' --exclude './.git'
fi 
