#!/usr/bin/env bash
SCRIPT_PATH="`dirname \"$0\"`"

FLAGS="-Pazur"
ARGS="--exclude './build' --exclude './.git' --exclude './classes' --exclude './out'"

echo "Attempting to sync via SSHFS"
eval "rsync $FLAGS ~/bert/Big_Data/Assignment_1/ $SCRIPT_PATH/../ $ARGS --delete"
rc1=$?

if (( $rc1 == 0 )) ; then
    echo "Synced via SSHFS"
elif (( $rc1 == 11 )) ; then
    echo "QMUL SSHFS not mounted, syncing via ssh..."
    eval "rsync $FLAGS sp319@bert.student.eecs.qmul.ac.uk:/homes/sp319/Big_Data/Assignment_1/ $SCRIPT_PATH/../ $ARGS --delete"
else
    echo "sync ran with possible errors. Exit code: $rc1"
fi 