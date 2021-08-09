tickers = db.newTickers$find()
tickers = tickers$Ticker
cal = create.calendar('mycal', weekdays = c("saturday", "sunday"))
bdays = as.character(rev(bizseq(Sys.Date()-320, Sys.Date(), cal)))


df = map_df(tickers, ~Quandl.datatable('ORATS/VOL', tradedate=bdays, ticker = .x,  qopts.columns=c("ticker", "tradedate", "pxatmiv", "clshv20d","orhv10d", "orhv20d", "orhv60d", "orhvxern10d",
                                                                                                   "orhvxern20d", "orhvxern60d", "orhvxern120d", "orhvxern252d", "cvolu", "pvolu", 
                                                                                                   "coi", "poi", "avgoptvolu20d", "borrow30", 
                                                                                                   "slope",  "impliedearningsmov",
                                                                                                   "dlt5iv10d", "dlt25iv10d", "dlt75iv10d", "dlt95iv10d",
                                                                                                   "dlt5iv20d", "dlt5iv30d", "dlt5iv60d", "dlt5iv90d", "dlt5iv6m", "dlt5iv1y",
                                                                                                   "dlt25iv20d", "dlt25iv30d", "dlt25iv60d", "dlt25iv90d", "dlt25iv6m", "dlt25iv1y",
                                                                                                   "dlt75iv20d", "dlt75iv30d", "dlt75iv60d", "dlt75iv90d", "dlt75iv6m", "dlt75iv1y",
                                                                                                   "dlt95iv20d", "dlt95iv30d", "dlt95iv60d", "dlt95iv90d", "dlt95iv6m", "dlt95iv1y",
                                                                                                   "iv10d", "iv20d", "iv30d", "iv60d", "iv90d", "iv6m", "iv1yr", "exerniv10d", 
                                                                                                   "exerniv20d", "exerniv30d", "exerniv60d", "exerniv90d", "exerniv6m", "exerniv1yr",
                                                                                                   "fwd90_30")))


df$PCR = df$pvolu/df$cvolu
df$OptVolu = df$pvolu + df$cvolu
df$Oi = df$coi + df$poi
df$PCRoi = df$coi/df$poi
df$RelativeAction = df$OptVolu/df$avgoptvolu20d
df$Contango = df$iv90d/df$iv30d
l.out <- BatchGetSymbols(tickers = unique(df$ticker), 
                         first.date = Sys.Date()-365, 
                         last.date = Sys.Date(),
                         thresh.bad.data = .01,
                         do.parallel = TRUE, 
                         do.cache = FALSE)
price.df = l.out$df.tickers
price.df = price.df %>% rename(Ticker = ticker, Price = price.close, SpChng = ret.closing.prices, Date = ref.date)%>% dplyr::select(Ticker, Date, Price, SpChng)


df = df %>% rename(Ticker = ticker, Date = tradedate, AvgOptVolu = avgoptvolu20d, CallOi = coi, CallVolu = cvolu, Cl2ClVol20d = clshv20d, 
                   FWDV3090 = fwd90_30,  IV10d = iv10d, IV20d = iv20d, IV30d = iv30d, IV60d = iv60d, IV90d = iv90d, 
                   IV120d = iv6m, IV252d = iv1yr,  Skew = slope, ImpErnMv = impliedearningsmov,
                   RV10d = orhv10d, RV30d = orhv20d, NEvol10d=exerniv10d, NEvol20d = exerniv20d, NEvol30d=exerniv30d, NEvol60d = exerniv60d,
                   NEvol90d = exerniv90d, NEvol120d = exerniv6m, NEvol252d = exerniv1yr, PutOi = poi, PutVolu = pvolu
)

df = left_join(df, price.df, by = c("Ticker", "Date"))

df = df %>% group_by(Ticker)%>% mutate(IvChng = log(IV30d/lead(IV30d)))


#append the dataframe
today.core$insert(df)