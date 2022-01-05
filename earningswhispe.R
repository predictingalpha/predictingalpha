
# Libraries ----

library(doParallel)
library(lubridate)
library(mongolite)
library(rvest)
library(tidyverse)



# Connections/Settings ----

Sys.setenv(TZ = "EST")
URL <- "mongodb+srv://jordan:libraryblackwell7@cluster0.skqv5.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
OPTIONS_SSL <- ssl_options(weak_cert_validation = TRUE)
cores <- detectCores()-1
registerDoParallel(cores = cores)

data_ertoday <- mongo(collection = "whisper", db = "pa", url = URL, options = OPTIONS_SSL)
data_erdates <- mongo(collection = "NextErDates", db = "pa", url = URL, options = OPTIONS_SSL)



# Phase 1 ----

whispers <- function(from = 0){
  
  today = weekdays(Sys.Date() + from)
  to = ifelse(today == "Friday", from + 3, ifelse(today == "Saturday", from + 2, from + 1))
  
  if(today %in% c("Saturday", "Sunday")){
    
    link = read_html(paste("https://www.earningswhispers.com/calendar?sb=c&d=",to,"&t=all",sep=""))
    
    dates <- tryCatch({
      data.frame(
        Ticker = html_nodes(link, ".bmo") %>%
          html_node("div") %>%
          html_attr("id") %>%
          str_remove(.,"T-"),
        Date = Sys.Date() + to,
        Time = "BMO",
        Confirmed = html_nodes(link, ".bmo") %>%
          html_node("div.confirm") %>%
          html_attr("title"),
        row.names = NULL
      )
    }, error = function(e){data.frame()})
    
    return(dates)
    
  } else {
    
    p1 = read_html(paste("https://www.earningswhispers.com/calendar?sb=c&d=",from,"&t=all",sep=""))
    p2 = read_html(paste("https://www.earningswhispers.com/calendar?sb=c&d=",to,"&t=all",sep=""))
    
    amc <- tryCatch({
      data.frame(
        Ticker = html_nodes(p1, ".amc") %>%
          html_node("div") %>%
          html_attr("id") %>%
          str_remove(.,"T-"),
        Date = Sys.Date() + from,
        Time = "AMC",
        Confirmed = html_nodes(p1, ".amc") %>%
          html_node("div.confirm") %>%
          html_attr("title"),
        row.names = NULL
      )}, error = function(e){data.frame()})
    
    bmo <- tryCatch({
      data.frame(
        Ticker = html_nodes(p2, ".bmo") %>%
          html_node("div") %>%
          html_attr("id") %>%
          str_remove(.,"T-"),
        Date = Sys.Date() + to,
        Time = "BMO",
        Confirmed = html_nodes(p2, ".bmo") %>%
          html_node("div.confirm") %>%
          html_attr("title"),
        row.names = NULL
      )}, error = function(e){data.frame()})
    
    dates = bind_rows(amc, bmo)
    return(dates)
  }
}



# Phase 2 ----

today <- whispers() 

if(nrow(today) > 0){
  
  today$Confirmed <- ifelse(is.na(today$Confirmed), "No", "Yes")
  today <- arrange(today, Date, Ticker)
}


next30 <- foreach(i = 0:30, .combine = "bind_rows", .packages = c("rvest", "stringr", "dplyr")) %dopar% {
  whispers(i)
}

next30 <- distinct(next30, Ticker, .keep_all = TRUE) %>%
  rename("ticker" = Ticker, "TimeOfDay" = Time, "ErDate" = Date) %>%
  mutate(Confirmed = ifelse(is.na(Confirmed), "No", "Yes"),
         DayToEr = as.numeric(ErDate - Sys.Date())) %>%
  arrange(ErDate, ticker)



# Mongo ----

data_ertoday$remove("{}")
data_ertoday$insert(today)

data_erdates$remove("{}")
data_erdates$insert(next30)


data_ertoday$disconnect(gc = TRUE)
data_erdates$disconnect(gc = TRUE)


q()



