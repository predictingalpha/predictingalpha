# 0.0 Libraries ----

library(mongolite)
library(tidyverse)
library(quantmod)
library(jsonlite)
library(rvest)
library(lubridate)
library(foreach)


#orats api url
Sys.setenv(TZ = "EST")
orats.url = 'https://api.orats.io/datav2/cores?token=28a2528a-34e8-448d-877d-c6eb709dc26e'
urlm = "mongodb+srv://jordan:libraryblackwell7@cluster0.skqv5.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
options_ssl = ssl_options(weak_cert_validation = TRUE)

db.earnings.dates = mongo(collection = "NextErDates", db = "pa", url = urlm, verbose = F, options = options_ssl)
db.whisper = mongo(collection = "whisper", db = "pa", url = urlm, verbose = F, options = options_ssl)


# 1.0 Earning Whispers Function ----

whispers = function(from = 0){
  
  # from = today, to = tomorrow - adjusted for weekends (ie. if saturday then tomorrow is monday)
  today = weekdays(Sys.Date() + from)
  to = ifelse(today=="Friday", from + 3,
              ifelse(today=="Saturday", from + 2, from + 1))
  
  # ifelse statement where if today = sat/sun, then just give me monday bmo
  if(today %in% c("Saturday","Sunday")){
    
    link = read_html(paste("https://www.earningswhispers.com/calendar?sb=c&d=",to,"&t=all",sep=""))
    
    dates <- data.frame(
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
    
    return(dates)
    
  } else {
    
    p1 = read_html(paste("https://www.earningswhispers.com/calendar?sb=c&d=",from,"&t=all",sep=""))
    p2 = read_html(paste("https://www.earningswhispers.com/calendar?sb=c&d=",to,"&t=all",sep=""))
    
    tryCatch({
      amc <- data.frame(
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
      )}, error = function(e){amc <<- data.frame()})
    
    bmo <- data.frame(
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
    )
    
    dates = bind_rows(amc,bmo)
    return(dates)
  }
  
}



# 2.0 Pulling today's earnings ----

today = whispers() %>%
  mutate(Confirmed = ifelse(is.na(Confirmed), "No", "Yes")) %>%
  arrange(Date, Ticker)



# 2.1 Pulling the next 30 day's earnings ----

next30 = foreach(i = 0:30, .combine = "bind_rows", .packages = c("rvest", "stringr", "purrr", "dplyr")) %do% {
  tryCatch({
    whispers(i)
  }, error = function(e){})
}

next30 = next30 %>% distinct() %>%
  rename("ticker" = Ticker, "ErDate" = Date, "TimeOfDay" = Time) %>%
  mutate(Confirmed = ifelse(is.na(Confirmed), "No", "Yes"),
         DayToEr = as.numeric(ErDate - Sys.Date())) %>%
  arrange(ErDate, ticker)



# 3.0 Inserting into database ----

db.whisper$remove("{}")
db.whisper$insert(today)

db.earnings.dates$remove("{}")
db.earnings.dates$insert(next30)


# 4.0 Disconnecting ----

db.whisper$disconnect(gc = TRUE)
db.earnings.dates$disconnect(gc = TRUE)

q()



