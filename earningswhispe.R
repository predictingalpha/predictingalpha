# 0.0 Libraries ----

source("setup.R")
registerDoParallel(cores = 4)


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

today = whispers()%>%
  mutate(Confirmed = ifelse(is.na(Confirmed), "No", "Yes")) %>%
  arrange(Date, Ticker)



# 2.1 Pulling the next 30 day's earnings ----

next30 = foreach(i = 0:30, .combine = "bind_rows", .packages = c("rvest", "stringr", "purrr", "dplyr", "rvest")) %dopar% {
  whispers(i)
}

next30 = next30 %>% distinct() %>%
  rename("ticker" = Ticker, "ErDate" = Date) %>%
  mutate(Confirmed = ifelse(is.na(Confirmed), "No", "Yes"),
         DayToEr = as.numeric(ErDate - Sys.Date())) %>%
  arrange(ErDate, ticker)



# 3.0 Inserting into database ----

db.whisper$remove("{}")
db.whisper$insert(today)

db.earnings.dates$remove("{}")
db.earnings.dates$insert(next30)








