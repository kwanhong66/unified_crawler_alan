#!/usr/bin/env bash

#NAVER_BLOG="python3 -u ./cohort_converter.py"
#TWITTER_STREAM="./script/run_trainer_standalone"
#NAVER_NEWS="./script/run_tester"

yesterday=$(date +%Y-%m-%d -d "yesterday + 540 minutes")

function usage {
    echo "Usage: $0 {all|naver_blog|twitter_stream|naver_news}"
    exit 1
}

function twitter_stream() {
    PYTHONPATH=$PYTHONPATH:./naver-blog-crawler python3 /home/rnd1/PycharmProjects/unified_crawler/hbase_batch_insert.py -p="/home/rnd1/data/" -d="${yesterday}" -sdir="twitter-crawler"
}

function twitter_stream_archive() {
    local date=$1
    PYTHONPATH=$PYTHONPATH:./naver-blog-crawler python3 /home/rnd1/PycharmProjects/unified_crawler/hbase_batch_insert.py -p="/home/rnd1/data/" -d="${date}" -sdir="twitter-crawler"
}

function naver_blog() {
    PYTHONPATH=$PYTHONPATH:./naver-blog-crawler python3 /home/rnd1/PycharmProjects/unified_crawler/hbase_batch_insert.py -p="/home/rnd1/data/" -d="${yesterday}" -sdir="naver-blog-crawler"
}

function naver_blog_archive() {
    local date=$1
    PYTHONPATH=$PYTHONPATH:./naver-blog-crawler python3 /home/rnd1/PycharmProjects/unified_crawler/hbase_batch_insert.py -p="/home/rnd1/data/" -d="${date}" -sdir="naver-blog-crawler"
}

function naver_news() {
    PYTHONPATH=$PYTHONPATH:./naver-blog-crawler python3 /home/rnd1/PycharmProjects/unified_crawler/hbase_batch_insert.py -p="/home/rnd1/data/" -d="${yesterday}" -sdir="naver-news-crawler"
}

function naver_news_archive() {
    local date=$1
    PYTHONPATH=$PYTHONPATH:./naver-blog-crawler python3 /home/rnd1/PycharmProjects/unified_crawler/hbase_batch_insert.py -p="/home/rnd1/data/" -d="${date}" -sdir="naver-news-crawler"
}

case "$1" in
    all)
        twitter_stream
        naver_blog
        naver_news
        ;;
    all_archive)
        twitter_stream_archive $2
        naver_news_archive $2
        naver_blog_archive $2
        ;;
    twitter_stream)
        twitter_stream
        ;;
    twitter_stream_archive)
        twitter_stream_archive $2
        ;;
    naver)
        naver_blog
        naver_news
        ;;
    naver_blog)
        naver_blog
        ;;
    naver_blog_archive)
        naver_blog_archive $2
        ;;
    naver_news)
        naver_news
        ;;
    naver_news_archive)
        naver_news_archive $2
        ;;
    *)
        usage
esac