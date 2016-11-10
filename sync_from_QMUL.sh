#!/usr/bin/env bash

FLAGS="-Pazur"
ARGS="--exclude './build' --exclude './.git' --exclude './classes' --exclude './out'"

echo "Attempting to sync via SSHFS"
eval "rsync $FLAGS ./ ~/bert/Big_Data/Assignment_1/ $ARGS"
rc1=$?
eval "rsync $FLAGS ~/bert/Big_Data/Assignment_1/ ./ $ARGS --delete"
rc2=$?

if (( $rc1 == 0 && $rc2 == 0 )) ; then
    echo "Synced via SSHFS"
elif (( $rc1 == 11 || $rc2 == 11 )) ; then
    echo "QMUL SSHFS not mounted, syncing via ssh..."
    eval "rsync $FLAGS ./ sp319@bert.student.eecs.qmul.ac.uk:/homes/sp319/Big_Data/Assignment_1/ $ARGS"
    eval "rsync $FLAGS sp319@bert.student.eecs.qmul.ac.uk:/homes/sp319/Big_Data/Assignment_1/ ./ $ARGS --delete"
else
    echo "sync ran with possible errors. Exit codes: $rc1, $rc2"
fi 
