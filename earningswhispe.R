library(rvest)
library(stringr)
library(dplyr)

whispers = function(from=0){
  to = ifelse(weekdays(Sys.Date()+from)=="Friday",from+3,from+1)
  p1 = read_html(paste("https://www.earningswhispers.com/calendar?sb=c&d=",from,"&t=all",sep=""))
  p2 = read_html(paste("https://www.earningswhispers.com/calendar?sb=c&d=",to,"&t=all",sep=""))
  
  amc = p1 %>% html_nodes(".amc div") %>% html_attr("id") %>% na.omit() %>% 
    data.frame(row.names=NULL) %>% filter(!str_detect(.,"[:punct:]")) %>% mutate(Date=Sys.Date()+from,Time="AMC")
  bmo = p2 %>% html_nodes(".bmo div") %>% html_attr("id") %>% na.omit() %>% 
    data.frame(row.names=NULL) %>% filter(!str_detect(.,"[:punct:]")) %>% mutate(Date=Sys.Date()+to,Time="BMO")
  
  t1 = bind_rows(amc,bmo) %>% rename("Ticker"=1) %>% arrange(Ticker)
  return(t1)
}

todays_earnings = whispers()

db.whisper$remove("{}")
db.whisper$insert(todays_earnings)
