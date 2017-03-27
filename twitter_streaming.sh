#!/usr/bin/env bash

PYTHONPATH=$PYTHONPATH:./twitter_stream python3 ./twitter_stream/twitter_streamer.py -o="/home/rnd1/data/twitter-crawler/twitter_stream/" -cmd=$1 -cc=$2