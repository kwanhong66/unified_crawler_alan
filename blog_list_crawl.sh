#!/usr/bin/env bash

function usage {
    echo "Usage: $0 {all|category_select category_number}"
    exit 1
}

function all {
    for i in {5..34}
    do
        echo "$i category list crawling..."
        PYTHONPATH=$PYTHONPATH:./naver-blog-crawler python3 /home/rnd1/PycharmProjects/unified_crawler/naver_blog_crawler/blog_list_crawler.py -c="$i" -p="/home/rnd1/data/naver-blog-crawler/url_list" -l=0
    done
}

function category_select() {
    local category=$1
    echo "${category} category list crawling..."
    PYTHONPATH=$PYTHONPATH:./naver-blog-crawler python3 /home/rnd1/PycharmProjects/unified_crawler/naver_blog_crawler/blog_list_crawler.py -c="${category}" -p="/home/rnd1/data/naver-blog-crawler/url_list" -l=0
}

case "$1" in
    all)
        all
        ;;
    category_select)
        category_select $2
        ;;
    *)
        usage
esac