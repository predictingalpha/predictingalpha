source("setup.R")
cal = create.calendar('mycal', weekdays = c("saturday", "sunday"))
calseq = bizdays::bizseq(Sys.Date(), Sys.Date()+30, cal)


finurl  = map(calseq, ~paste0("https://finnhub.io/api/v1/calendar/earnings?from=",.x,"&to=",.x,"&token=bpopumfrh5rb5khcpcug"))
earningsApi = map(finurl, ~jsonlite::fromJSON(.x)$earningsCalendar)
earningsfin = do.call(rbind, earningsApi)
earningsfin = earningsfin %>% dplyr::select(symbol, hour)


#confirmed earnings
urlyes = map(calseq,~paste0("https://api.earningscalendar.net/confirmed_earnings?date=",.x,"&api_key=RQkbfP-TYiCMjlrz7uC2Hg"))
l.yes = list()
for(i in 1:length(calseq)){
  l.yes[[i]] = jsonlite::fromJSON(urlyes[[i]])
  Sys.sleep(1)
}
yes = do.call(rbind, l.yes)
yes = yes %>% dplyr::select(ticker, date)
yes$Confirmed = T
yes = yes %>% rename("dc" = "date")

#not confirmed earnings
urlnot = map(calseq,~paste0("https://api.earningscalendar.net/earnings?date=",.x,"&api_key=RQkbfP-TYiCMjlrz7uC2Hg"))
l.no = list()
for(i in 1:length(calseq)){
  l.no[[i]] = jsonlite::fromJSON(urlnot[[i]])
  Sys.sleep(1)
}
no = do.call(rbind, l.no)
no = no %>% dplyr::select(ticker, date)

earningsData = left_join(no, yes, by = "ticker")
earningsData$ErDate = ifelse(is.na(earningsData$Confirmed),earningsData$date, earningsData$dc )

earningsData = left_join(earningsData, earningsfin, by = c("ticker" = "symbol"))
earningsData = earningsData %>%group_by(ticker)%>%slice(1)%>%ungroup()
earningsData = earningsData%>%drop_na(ErDate)
earningsData$Confirmed = ifelse(is.na(earningsData$Confirmed), F, T)
earningsData = earningsData %>%dplyr::select(ticker, Confirmed, ErDate, hour)
earningsData$DayToEr = as.numeric(as.Date(earningsData$ErDate) - Sys.Date())
tickers = tickers.db$find()
tickers = tickers %>% pull(Ticker)

earningsData = earningsData %>% filter(ticker %in% tickers)
earningsData = earningsData %>% rename(TimeOfDay = hour)

db.earnings.dates$remove("{}")
db.earnings.dates$insert(earningsData)


