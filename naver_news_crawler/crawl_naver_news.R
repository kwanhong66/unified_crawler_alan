#if (!require("devtools")) install.packages("devtools")
#devtools::install_github("kwanhong66/n2h4_modified")
library("stringr", lib.loc="~/R/x86_64-pc-linux-gnu-library/3.0")
library("rvest", lib.loc="~/R/x86_64-pc-linux-gnu-library/3.0")
library("xml2", lib.loc="~/R/x86_64-pc-linux-gnu-library/3.0")
library("jsonlite", lib.loc="~/R/x86_64-pc-linux-gnu-library/3.0")
library("RCurl", lib.loc="~/R/x86_64-pc-linux-gnu-library/3.0")
source('/home/rnd1/PycharmProjects/unified_crawler/naver_news_crawler/R/getContent.R')
source('/home/rnd1/PycharmProjects/unified_crawler/naver_news_crawler/R/getMainCategory.R')
source('/home/rnd1/PycharmProjects/unified_crawler/naver_news_crawler/R/getSubCategory.R')
source('/home/rnd1/PycharmProjects/unified_crawler/naver_news_crawler/R/getUrlListByCategory.R')
source('/home/rnd1/PycharmProjects/unified_crawler/naver_news_crawler/R/getMaxPageNum.R')

args = commandArgs(trailingOnly=TRUE)
strDate <- args[1]
endDate <- args[2]
categoryNum <- args[3]

# 메인 카테고리 id 가져옵니다.
cate<-getMainCategory()
tcate<-cate$sid1[strtoi(categoryNum)]

subCate<-cbind(sid1=tcate,getSubCategory(sid1=tcate))
# all sub-category
tscate<-subCate$sid2

mainDir <- "/home/rnd1/data/naver-news-crawler/news_text"
subDir <- tcate
#year <- substr(strDate, 1, 4)
#month <- substr(strDate, 5, 6)
#day <- substr(strDate, 7, 8)
#target_path <- file.path(mainDir, subDir, year, month, day)
#
#dir.create(target_path, showWarnings = FALSE, recursive = TRUE)
#setwd(target_path)

strTime<-Sys.time()
midTime<-Sys.time()

for (date in strDate:endDate){

  year <- substr(date, 1, 4)
  month <- substr(date, 5, 6)
  month_i <- strtoi(month)
  day <- substr(date, 7, 8)
  day_i <- strtoi(day)

  if (month == "08") {
    month_i = 8
  }

  if (month == "09") {
    month_i = 9
  }

  if (day == "08") {
    day_i = 8
  }

  if (day == "09") {
    day_i = 9
  }

  if (day_i == 0) {
    next
  } else {
    if ((month_i == 1) | (month_i == 3) | (month_i == 5) | (month_i == 7) | (month_i == 8) | (month_i == 10) | (month_i == 12))  {
      if (day_i > 31) {
        next
      }
    } else {
      if ((month_i == 2)) {
        if (day_i > 28) {
          next
        }
      } else {
        if (day_i > 30) {
          next
        }
      }
    }
  }

  target_path <- file.path(mainDir, subDir, year, month, day)

  dir.create(target_path, showWarnings = FALSE, recursive = TRUE)
  setwd(target_path)

  for (sid1 in tcate){
    for (sid2 in tscate){

      print(paste0(date," / ",sid1," / ",sid2 ," / start Time: ", strTime," / spent Time: ", Sys.time()-midTime," / spent Time at first: ", Sys.time()-strTime))
      midTime<-Sys.time()

      # 뉴스 리스트 페이지의 url을 sid1, sid2, date로 생성합니다.
      pageUrl<-paste0("http://news.naver.com/main/list.nhn?sid2=",sid2,"&sid1=",sid1,"&mid=shm&mode=LS2D&date=",date)
      # 리스트 페이지의 마지막 페이지수를 가져옵니다.
      max<-getMaxPageNum(pageUrl)

      for (pageNum in 1:max){
        print(paste0(date," / ",sid1," / ",sid2," / ",pageNum, " / start Time: ", strTime," / spent Time: ", Sys.time()-midTime," / spent Time at first: ", Sys.time()-strTime))
        midTime<-Sys.time()
        # 페이지넘버를 포함한 뉴스 리스트 페이지의 url을 생성합니다.
        pageUrl<-paste0("http://news.naver.com/main/list.nhn?sid2=",sid2,"&sid1=",sid1,"&mid=shm&mode=LS2D&date=",date,"&page=",pageNum)
        # 뉴스 리스트 페이지 내의 개별 네이버 뉴스 url들을 가져옵니다.
        newsList<-getUrlListByCategory(pageUrl)
        newsData<-c()

        # 가져온 url들의 정보를 가져옵니다.
        for (newslink in newsList$links){
          # 불러오기에 성공할 때 까지 반복합니다.
          # 성공할때 까지 반복하면 못나오는 문제가 있어서 5회로 제한합니다.
          tryi<-0
          tem<-try(getContent(newslink), silent = TRUE)
          while(tryi<=5&&class(tem)=="try-error"){
            tem<-try(getContent(newslink), silent = TRUE)
            tryi<-tryi+1
            print(paste0("try again: ",newslink))
          }
          if (class(tem)!="try-error"){
            if(class(tem$datetime)[1]=="POSIXct"){
              if(tem$content == "") {
                next
              }
              newsData<-rbind(newsData,tem)
            }
          }
        }
        # 가져온 뉴스들(보통 한 페이지에 20개의 뉴스가 있습니다.)
        file_path <- paste0(sid1,"_",sid2,"_",date,"_",pageNum,".json")
        write_json(newsData, path = file_path, pretty = T)
      }
    }
  }
}