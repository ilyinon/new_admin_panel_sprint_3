#!/usr/bin/env bash

set -ev

run () {
    errorCode=$?

    echo "Verifying if db is available .."
    while !</dev/tcp/db/5432; do "Trying to connect to db .. "; sleep 3; done;

    echo "Verifying if elastic is available .."
    while !</dev/tcp/elastic/9200; do "Trying to connect to db .. "; sleep 3; done;

    echo "Verifying if redis is available .."
    while !</dev/tcp/redis/6379; do "Trying to connect to db .. "; sleep 3; done;

    echo "Running ETL"
    python main.py
    exit $errorCode
}

trap run ERR
sleep 5 && false