
newERdates = newERdb$find()

newERdb$remove("{}")

#these next steps are to make sure we only pull data from not today as quandl does not update until later
backintoERdb = newERdates %>% filter(Date > Sys.Date() - 1)
newERdb$insert(backintoERdb)

newERdates = newERdates %>% filter(Date <= Sys.Date() - 1)


l.out <- BatchGetSymbols(tickers = stock.tickers, 
                         first.date = Sys.Date()-30, 
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


### i need to find the dates first
df.data = map(strings.l, ~Quandl.datatable('ORATS/VOL', tradedate=as.character(.x$refdate), ticker= .x$ticker[1]))

data.er.vols = do.call(bind_rows, df.data)

#merge that quandl data with price data
final = full_join(merged.df, data.er.vols, by = c("ticker" = "ticker", "refdate" =  "tradedate"))
final = final %>% drop_na(ticker)
final = final %>%arrange(ticker,refdate)
final = final %>% dplyr::select(ticker, refdate, Time, key, value, price.open, price.close, pxatmiv:dtexm2)


###left off right here.
###pnl for every monthly



final = earningsRaw%>% group_by(ticker) %>% mutate(date.type = lead(key),
                                                   t.price = lead(price.close),
                                                   t.open = lead(price.open),
                                                   t.dte1 = lead(dtexm1),
                                                   t.dte2 = lead(dtexm2),
                                                   t.dte3 = lead(dtexm3),
                                                   t.dte4 = lead(dtexm4),
                                                   t.iv1m = lead(atmivm1),
                                                   t.iv2m = lead(atmivm2),
                                                   t.iv3m = lead(atmivm3),
                                                   t.iv4m = lead(atmivm4),
                                                   expDif1 = dtexm1 - t.dte1,
                                                   expDif2 = dtexm2 - t.dte2,
                                                   expDif3 = dtexm3 - t.dte3,
                                                   expDif4 = dtexm4 - t.dte4
)
final$what2useIv1 = ifelse(abs(final$expDif) > 4, final$atmivm2, final$atmivm1)
final$dte1 = ifelse(abs(final$expDif) > 4, final$dtexm2, final$dtexm1)
test = final %>% filter(key == "data_date")
final = final %>% filter(key == "data_date")
final = test %>% filter(abs(expDif1) < 5, abs(expDif2) < 5, abs(expDif3) < 5, abs(expDif4) < 5 )
final = final %>% filter(abs(expDif1) > 0, abs(expDif2) > 0, abs(expDif3) > 0, abs(expDif4) > 0 )
final = final %>% mutate(
  Move = (t.price  - price.close)/price.close,
  Jump = (t.open - price.close)/price.close,
  Intraday = Move - Jump,
  dtexm1 = dtexm1 - 1,
  dtexm2 = dtexm2 - 1,
  dtexm3 = dtexm3 - 1,
  dtexm4 = dtexm4 - 1,
  t.dte1 = t.dte1 - 1,
  t.dte2 = t.dte2 - 1,
  t.dte3 = t.dte3 - 1, 
  t.dte4 = t.dte4 - 1,
  atmivm1 = atmivm1/100,
  atmivm2 = atmivm2/100,
  atmivm3 = atmivm3/100,
  atmivm4 = atmivm4/100,
  t.iv1m = t.iv1m/100,
  t.iv2m = t.iv2m/100,
  t.iv3m = t.iv3m/100, 
  t.iv4m = t.iv4m/100,
  frontStraddlePre = bscall(price.close, price.close, atmivm1, 0, dtexm1/365, 0)+bsput(price.close, price.close, atmivm1, 0, dtexm1/365, 0),
  frontStraddlePost =  bscall(t.price, price.close, t.iv1m, 0,t.dte1/365, 0)+bsput(t.price, price.close, t.iv1m, 0,t.dte1/365, 0),
  SecStraddlePre = bscall(price.close, price.close, atmiv2m, 0, dtexm2/365, 0)+bsput(price.close, price.close, atmiv2m, 0, dtexm2/365, 0),
  SecStraddlePost =  bscall(t.price, price.close, t.iv2m, 0,t.dte2/365, 0)+bsput(t.price, price.close, t.iv2m, 0,t.dte2/365, 0),
  ThirdStraddlePre= bscall(price.close, price.close, atmiv3m, 0, dtexm3/365, 0)+bsput(price.close, price.close, atmiv3m, 0, dtexm3/365, 0),
  ThirdStraddlePost =  bscall(t.price, price.close, t.iv3m, 0,t.dte3/365, 0)+bsput(t.price, price.close, t.iv3m, 0,t.dte3/365, 0),
  fourthStraddlePre = bscall(price.close, price.close, atmiv4m, 0, dtexm4/365, 0)+bsput(price.close, price.close, atmiv4m, 0, dtexm4/365, 0),
  fourthStraddlePost = bscall(t.price, price.close, t.iv4m, 0,t.dte4/365, 0)+bsput(t.price, price.close, t.iv4m, 0,t.dte4/365, 0),
  frontPnl = (frontStraddlePost - frontStraddlePre)/frontStraddlePre,
  secondPnl = (SecStraddlePost - SecStraddlePre)/SecStraddlePre,
  ThirdStraddlePnL =(ThirdStraddlePost - ThirdStraddlePre )/ThirdStraddlePre,
  FourthStraddlePnL = (fourthStraddlePost - fourthStraddlePost)/fourthStraddlePre,
  DollarFirst = frontStraddlePost - frontStraddlePre,
  DollarSecond = SecStraddlePost - SecStraddlePre,
  DollarThird = ThirdStraddlePost - ThirdStraddlePre,
  Dollar4th  = fourthStraddlePost - fourthStraddlePre,
  forwardvol = forward_vol(atmivm1, atmivm2, dtexm1, dtexm2),
  eventVol = event_full(forwardvol, atmivm1, dtexm1)
  #impliedMove
  #contangof1f2
  #contangof2f3
  #contangof3f4
  #
)


## select earnings columns then upload that into database
erdates = modern.er %>% dplyr::select(ticker, earnDate, data_date)

f.df = inner_join(erdates, final, by = c("ticker"  = "ticker", "data_date" = "value"))
f.df = f.df %>% dplyr::select(ticker, earnDate, Time, stkpxchng1m, impliedearningsmov,stkpxchng1wk, Jump, Move, Intraday, what2useIv1, t.iv1m, call_pre:straddle_pnl, dte1)
f.df = f.df %>% arrange(ticker, earnDate)%>%drop_na(call_pnl)%>%group_by(ticker)%>%mutate(CummulativeStraddle = cumsum(straddle_pnl),
                                                                                          CummulativeCall = cumsum(call_pnl),
                                                                                          CummulativePut = cumsum(put_pnl))

## rename this data frame and then ship it up. 
f.df = f.df %>% rename(Ticker = ticker,refdate = earnDate, CallPnL = call_pnl, PutPnL = put_pnl, 
                       Call_Cum = CummulativeCall, Put_Cum = CummulativePut, StraddlePnL = straddle_pnl,
                       Straddle_Cum = CummulativeStraddle, ImpErnMv = impliedearningsmov, IVPre = what2useIv1,
                       IVPost = t.iv1m, Dte = dte1)

f.df$quarter = lubridate::quarter(f.df$refdate)
f.df = f.df %>% mutate(Move = Move *100, 
                       Jump = Jump * 100)

#should not fully remove but rather update
h.imp.moves$insert(f.df)
