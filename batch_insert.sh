#!/usr/bin/env bash

#NAVER_BLOG="python3 -u ./cohort_converter.py"
#TWITTER_STREAM="./script/run_trainer_standalone"
#NAVER_NEWS="./script/run_tester"

function usage {
    echo "Usage: $0 {all|naver_blog|twitter_stream|naver_news}"
    exit 1
}

function naver_blog() {
    local date=$1
    for i in {5..34}
    do
        PYTHONPATH=$PYTHONPATH:./naver-blog-crawler python3 ./hbase_batch_input.py -c="$i" -p="/home/rnd1/data/" -d="${date}" -sdir="naver-blog-crawler" -tdir="blog_text"
    done
}

case "$1" in
    all)
        naver_blog
        ;;
    naver_blog)
        naver_blog $2
        ;;
    *)
        usage
esac