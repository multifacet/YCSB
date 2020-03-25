#!/bin/bash

if [ $# -eq 0 ] ; then
    docker run --rm -v `pwd`:/home/markm -i -t bluesheepimg /bin/bash
else
    docker run --rm -v $DIR:/home/markm -i -t bluesheepimg $@
fi
