#!/usr/bin/env bash

yesterday=$(date +%Y%m%d -d "yesterday + 540 minutes")

function usage {
    echo "Usage: $0 {all|category_select category_number}"
    exit 1
}

function all {
    for i in {1..6}
    do
        echo "$i category list crawling..."
        Rscript /home/rnd1/PycharmProjects/unified_crawler/naver_news_crawler/crawl_naver_news.R $yesterday $i
    done
}

function category_select() {
    echo "$2 category list crawling..."
    Rscript /home/rnd1/PycharmProjects/unified_crawler/naver_news_crawler/crawl_naver_news.R $1 $2
}

case "$1" in
    all)
        all
        ;;
    category_select)
        category_select $2 $3
        ;;
    *)
        usage
esac