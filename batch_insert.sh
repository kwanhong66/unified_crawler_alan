#!/usr/bin/env bash

#NAVER_BLOG="python3 -u ./cohort_converter.py"
#TWITTER_STREAM="./script/run_trainer_standalone"
#NAVER_NEWS="./script/run_tester"

function usage {
    echo "Usage: $0 {all|naver_blog|twitter_stream|naver_news}"
    exit 1
}

function twitter_stream() {

echo "twitter stream"
}
function naver_blog() {
    local date=$1
    PYTHONPATH=$PYTHONPATH:./naver-blog-crawler python3 /home/rnd1/PycharmProjects/unified_crawler/hbase_batch_input.py -p="/home/rnd1/data/" -d="${date}" -sdir="naver-blog-crawler"
}

function naver_news() {
local date=$1
PYTHONPATH=$PYTHONPATH:./naver-blog-crawler python3 /home/rnd1/PycharmProjects/unified_crawler/hbase_batch_input.py -p="/home/rnd1/data/" -d="${date}" -sdir="nave-news-crawler"

}
case "$1" in
    all)
        twitter_stream
        naver_blog
        naver_news
        ;;
    naver_blog)
        naver_blog $2
        ;;
    naver_news)
        naver_news $2
    *)
        usage
esac