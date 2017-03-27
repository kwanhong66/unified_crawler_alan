#!/usr/bin/env bash

#year=$(date +%Y)
#month=$(date +%m)
#day=$(date +%d)

yesterday=$(date +%Y-%m-%d -d "yesterday + 540 minutes")

function all() {
    local date=$1
    for i in {5..34}
    do
        echo "$i category text crawling..."
        PYTHONPATH=$PYTHONPATH:./naver-blog-crawler python3 /home/rnd1/PycharmProjects/unified_crawler/naver_blog_crawler/blog_text_crawler.py -c="$i" -p="/home/rnd1/data/naver-blog-crawler" -d="${yesterday}"
    done
}

function all_archive() {
    local date=$1
    for i in {5..34}
    do
        echo "$i category text crawling..."
        PYTHONPATH=$PYTHONPATH:./naver-blog-crawler python3 /home/rnd1/PycharmProjects/unified_crawler/naver_blog_crawler/blog_text_crawler.py -c="$i" -p="/home/rnd1/data/naver-blog-crawler" -d="${date}"
    done
}


function category_select() {
    local category=$1
    local date=$2
    echo "${category} category text crawling..."
    PYTHONPATH=$PYTHONPATH:./naver-blog-crawler python3 /home/rnd1/PycharmProjects/unified_crawler/naver_blog_crawler/blog_text_crawler.py -c="${category}" -p="/home/rnd1/data/naver-blog-crawler" -d="${date}"
}

case "$1" in
    all)
        all $2
        ;;
    all_archive)
        all_archive $2
        ;;
    category_select)
        category_select $2 $3
        ;;
    *)
esac

