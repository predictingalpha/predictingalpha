source("setup.R")
library(tidyverse)
library(BatchGetSymbols)
library(bizdays)
library(sqldf)
library(derivmkts)

future::plan(future::multisession, 
             workers = 6) 
tickers = tickers.db$find()

cal = create.calendar('mycal', weekdays = c('saturday', 'sunday'))


stock.tickers = tickers %>% filter(Type == "Stock")%>%pull(Ticker)

h.earnings.url = map(stock.tickers, ~sprintf("https://api.orats.io/datav2/hist/earnings?token=28a2528a-34e8-448d-877d-c6eb709dc26e&ticker=%s",.x))
h.earnings.dates = map(h.earnings.url, ~jsonlite::fromJSON(.x)$data)
h.earnings.df = do.call(bind_rows, h.earnings.dates)

l.out <- BatchGetSymbols(tickers = stock.tickers, 
                         first.date = Sys.Date()-1800, 
                         last.date = Sys.Date(),
                         thresh.bad.data = .01,
                         do.parallel = TRUE, 
                         do.cache = FALSE)
price.df = l.out$df.tickers

h.earnings.df$anncTod = as.numeric(h.earnings.df$anncTod)
h.earnings.df$earnDate = as.numeric(as.Date(h.earnings.df$earnDate))
h.earnings.df = h.earnings.df %>% mutate(Time = ifelse(anncTod > 1400, "AMC", "BMO"),
                                         data_date = ifelse(Time == "AMC", earnDate, offset(as.Date(earnDate, origin="1970-01-01"), -1, cal)))



h.earnings.df$data_date = as.Date(h.earnings.df$data_date, origin = "1970-01-01")
h.earnings.df$earnDate = as.Date(h.earnings.df$earnDate, origin = "1970-01-01")

h.earnings.df$intradayDate = as.Date(offset(h.earnings.df$data_date, 1, cal))
h.earnings.df$dayOfweek = weekdays(h.earnings.df$earnDate)

#get data from quandl
modern.er = h.earnings.df %>% filter(data_date > "2013-01-01")
strings = modern.er %>% dplyr::select(ticker, data_date, intradayDate, Time)%>%gather(key, value, -c(ticker, Time))

df1 = strings

df2 = price.df
names(df2)[7] = "refdate"

merged.df = sqldf("select df1.*, max(df2.refdate), df2.* from df1 
  left join df2 on df1.ticker = df2.ticker and df1.value >= df2.refdate
  group by df1.rowid
  order by df1.rowid")#[-3]xxxx

merged.df = merged.df[, -which(names(merged.df) == "ticker")[1]]

stringsquandl = merged.df %>% ungroup() %>% arrange(ticker, refdate)
#group_by(ticker) %>%
#summarise(string = refdate)%>%ungroup()%>%na.omit() %>% arrange(ticker, string)

strings.l = split(stringsquandl, stringsquandl$ticker)


df.data = map(strings.l, ~Quandl.datatable('ORATS/VOL', tradedate=as.character(.x$refdate), ticker= .x$ticker[1], qopts.columns=c("ticker", "tradedate", "pxatmiv", 
                                                                                                                                  "cvolu", "pvolu", "avgoptvolu20d", 
                                                                                                                                  "mktwidthvol", "borrow30", "iv30d", "iv60d", "iv90d", 
                                                                                                                                  "confidence", "impliedearningsmov")))

data.er.vols = do.call(bind_rows, df.data)

#merge that quandl data with price data
final = full_join(merged.df, data.er.vols, by = c("ticker" = "ticker", "refdate" =  "tradedate"))
final = final %>% drop_na(ticker)
final = final %>%arrange(ticker,refdate)
#final = final %>% dplyr::select(ticker, refdate, Time, key, value, price.open, price.close, pxatmiv:dtexm2)


###left off right here.
###pnl for every monthly



final = final%>% group_by(ticker) %>% mutate(date.type = lead(key),
                                                   t.price = lead(price.close),
                                                   t.open = lead(price.open),
                                                   t.iv30 = lead(iv30d)/100,
                                                   t.iv60 = lead(iv60d)/100,
                                                   Contango = t.iv60/t.iv30,
                                                   RelativeAction = (cvolu + pvolu)/avgoptvolu20d
                                                   )

final = final %>% filter(key == "data_date")

final = final %>% mutate(
  Move = (t.price  - price.close)/price.close,
  Jump = (t.open - price.close)/price.close,
  Intraday = Move - Jump,
  iv30d = iv30d/100, 
  callpre = bscall(price.close, price.close, iv30d, 0, 30/365, 0), 
  putpre = bsput(price.close, price.close, iv30d, 0,30/365, 0),
  frontStraddlePre = callpre + putpre,
  callpost = bscall(t.price, price.close, t.iv30, 0,29/365, 0),
  putpost = bsput(t.price, price.close, t.iv30, 0,29/365, 0),
  frontStraddlePost =  callpost + putpost,
  call_pnl = (callpost - callpre)/callpre,
  put_pnl = (putpost - putpre)/putpre, 
  straddle_pnl = (frontStraddlePost - frontStraddlePre)/frontStraddlePre
  )



## select earnings columns then upload that into database
erdates = modern.er %>% dplyr::select(ticker, earnDate, data_date)

f.df = inner_join(erdates, final, by = c("ticker"  = "ticker", "data_date" = "value"))
#f.df = f.df %>% dplyr::select(ticker, earnDate, Time, stkpxchng1m, impliedearningsmov,stkpxchng1wk, Jump, Move, Intraday, what2useIv1, t.iv1m, call_pre:straddle_pnl, dte1)
f.df = f.df %>% arrange(ticker, earnDate)%>%drop_na(call_pnl)%>%group_by(ticker)%>%mutate(CummulativeStraddle = cumsum(straddle_pnl),
                                                                                          CummulativeCall = cumsum(call_pnl),
                                                                                          CummulativePut = cumsum(put_pnl))

## rename this data frame and then ship it up. 
f.df = f.df %>% select(-refdate)%>%rename(Ticker = ticker,refdate = earnDate, CallPnL = call_pnl, PutPnL = put_pnl, 
                       Call_Cum = CummulativeCall, Put_Cum = CummulativePut, StraddlePnL = straddle_pnl,
                       Straddle_Cum = CummulativeStraddle, ImpErnMv = impliedearningsmov, IVPre = iv30d,
                       IVPost = t.iv30)

f.df$quarter = lubridate::quarter(f.df$refdate)
f.df = f.df %>% mutate(Move = Move *100, 
                       Jump = Jump * 100)
f.df = f.df %>% dplyr::select(Ticker, refdate, Time,pxatmiv:quarter, -c(date.type, `max(df2.refdate)`, key))
## also add new tickers to timeseries PA database

# upload this into datafram
h.imp.moves$remove("{}")
#should not fully remove but rather update
h.imp.moves$insert(f.df)
