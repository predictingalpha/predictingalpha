# setup ----

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


today.core = mongo(collection = "PAtime", db = "pa", url = urlm, verbose = F, options = options_ssl)
tickers.db = mongo(collection = "Tickers", db = "pa", url = urlm, verbose = F, options = options_ssl)
db.earnings.dates = mongo(collection = "NextErDates", db = "pa", url = urlm, verbose = F, options = options_ssl)
db.earnings.summaries = mongo(collection = "Summaries", db = "pa", url = urlm, verbose = F, options = options_ssl)
db.timeNow = mongo(collection = "update", db = "pa", url = urlm, verbose = F, options = options_ssl)


# code starts here ----

#pull in databases
summaries = db.earnings.summaries$find()
earningsestimates = db.earnings.dates$find()
tickers = tickers.db$find()

# weekly tickers from cboe
weekly_tickers = html_table(read_html("https://www.cboe.com/us/options/symboldir/weeklys_options"))[[1]] %>%
  pull(STOCKSYMBOL) %>%
  sort()


# pull orats data
x = jsonlite::fromJSON(orats.url)
x = x$data
x = x%>% filter(ticker %in% tickers$Ticker)
names(x) = tolower(names(x))
x$updatedat = as.character(Sys.time() + 60*60) 

db.timeNow$remove("{}")
db.timeNow$insert(data.frame(updatedat = x$updatedat[1]))

#pull yesterday orats data
y.date = as.Date(today.core$find(
  '{"Ticker": "AAPL"}',
  sort = '{"Date":-1}',
  limit = 1
)$Date)

if(y.date != Sys.Date()){
  x.date = y.date
}else{
  x.date = Sys.Date()-1
}

#be careful with this as we cant use this since we update intraday
yesterday = today.core$find(
  sprintf('{"Date":"%s"}', x.date)
)

if(nrow(yesterday)<1){
  x.date = Sys.Date()-2
  yesterday = today.core$find(
    sprintf('{"Date":"%s"}', x.date)
  )
}

n  = names(yesterday)
yesterday = yesterday %>% dplyr::select(Ticker, IV30d)%>%rename(Iv30Yest = IV30d)

df = left_join(x, yesterday, by = c("ticker" = "Ticker"))%>%mutate(IvChng = log(iv30d/Iv30Yest))%>%dplyr::select(-Iv30Yest)
#rename variables
df1 = left_join(df, summaries, by = c("ticker" = "Ticker"))%>%left_join(earningsestimates, by = "ticker")
df= df1 %>% mutate(PCR = pvolu/cvolu, 
                   FV30 = (orhv5d + clshvxern60d + iv30d + orfcst20d)/4,
                   ImpMvVsAvgMv = impliedearningsmove/AvgMv, 
                   ImpMvVsAvgImpmv = impliedearningsmove/AvgImpMv,
                   IVvsFV = iv30d/FV30, 
                   OptVolu = cvolu + pvolu, 
                   Type = ifelse(assettype %in% c(0,3), "Stock", "ETF"),
                   ImpMv1d = iv10d/20,
                   RelativeAction = OptVolu/avgoptvolu20d,
                   RV10d = orhvxern10d,
                   RV30d = orhvxern20d, 
                   Oi = poi + coi, 
                   PCRoi = poi/coi,
                   Contango = iv60d/iv30d,
                   Event = ifelse(iv30d==exerniv30d,"No","Yes"),
                   Weekly = ifelse(ticker %in% weekly_tickers, "Yes", "No"),
                   MktType = ifelse(mktcap < 300, "Micro",
                                    ifelse(mktcap < 2000, "Small",
                                           ifelse(mktcap < 10000, "Mid",
                                                  ifelse(mktcap < 200000, "Large", "Mega"))))
)

df$ImpMvVsAvgImpmv = ifelse(is.infinite(df$ImpMvVsAvgImpmv), 0, df$ImpMvVsAvgImpmv)
df$ImpMvVsAvgMv = ifelse(is.infinite(df$ImpMvVsAvgMv), 0, df$ImpMvVsAvgMv)
df$PCR = ifelse(is.infinte(df$PCR), 0, df$PCR)

df = df%>% left_join(tickers%>%select(-Type), by = c("ticker" = "Ticker"))
#will need to add sectors to database here
#....

df = df %>% rename(
  Ticker = ticker, 
  Date = tradedate, 
  AvgOptVolu = avgoptvolu20d,
  CallOi = coi, 
  PutOi  = poi, 
  Cl2ClVol20d = clshvxern20d, 
  Curvature = deriv,
  FWDV3090 = fwd90_30, 
  IV10d = iv10d, 
  IV20d = iv20d, 
  IV30d = iv30d, 
  IV60d = iv60d, 
  IV90d = iv90d, 
  IV120d = iv6m, 
  IV252d = iv1yr,
  ImpErnMv = impliedearningsmove,
  IvRank = ivpctile1y, 
  IvRankVsSPY = ivpctilespy,
  MktCap = mktcap, 
  NEvol10d = exerniv10d, 
  NEvol20d = exerniv20d,
  NEvol30d = exerniv30d,
  NEvol60d = exerniv60d,
  NEvol90d = exerniv90d,
  NEvol120d = exerniv6m,
  NEvol252d = exerniv1yr,
  Skew = slope,
  SkewRank = slopepctile,
  CallVolu = cvolu, 
  PutVolu = pvolu,
  
  
)

#https://ca.finance.yahoo.com/quote/AAPL/profile?p=AAPL
metrics <- yahooQF(c("Name", "Last Trade (Price Only)", "Change in Percent"))
d = getQuote(tickers$Ticker, what = metrics)
d$Ticker = rownames(d)
rownames(d) = NULL
names(d) = c("TradeTime", "Name", "Price", "SpChng", "Ticker")
d = d %>%dplyr::select(Name, Price, SpChng, Ticker)
d$Name = ifelse(is.na(d$Name), d$Ticker, d$Name)

df.f = left_join(df, d, by = "Ticker")

names.for.selection = intersect(names(df.f), n)
#n[!(n %in% names.for.selection)]

#run once with all names then will be good after
final = df.f %>% dplyr::select(all_of(c(names.for.selection, "updatedat", "IvChng", "ImpMv1d", 
                                        "Name", "Type", "IVvsFV", "ImpMvVsAvgImpmv", "ImpMvVsAvgMv", 
                                        "FV30",  "TimeOfDay",  "DayToEr", "Confirmed",  "ErDate", 'orhvxern90d',
                                        "AvgImpMv", "StraddlePnL", "PutPnL", "CallPnL" , "MedMv", "SpChng", 
                                        "AvgJump", "AvgMv", "Price", "MktCap", "SkewRank", "IvRank", "Secotr", "Industry", "Event", "Weekly", "MktType")))

#lets tap into data frame and calc some zscores for different ivols

patime4z = today.core$find(
  fields = '{"Ticker" : true, "Date" : true, "IV30d" : true,  "IV120d": true, "_id" : false}'
 
)

patimezAdd = final %>% dplyr::select(Ticker, Date, IV30d, IV120d)

timz = bind_rows(patimezAdd, patime4z) 
timz = timz %>% group_by(Ticker)%>% arrange(Date)%>%mutate(IV30zScore = as.numeric(scale(IV30d)), 
                                                           IV120zScore = as.numeric(scale(IV120d)))%>% slice(n())%>%ungroup()%>%dplyr::select(Ticker, IV30zScore, IV120zScore)
final = final %>% left_join(timz, by = "Ticker")%>% mutate_if(is.numeric, round, 2)
final$SpChng = final$SpChng/100
dims= dim(final)


#### upload to database
### if still an issue i can add a parallel feature
s <- split(final, 1:nrow(final))
json_strings <- lapply(s, rjson::toJSON)


if(dims[1] > 1200 && dims[2] == 102){
 
for (val in 1:nrow(final)){
  this_date <- final$Date[val]
  this_ticker <- final$Ticker[val]
  
  query <- sprintf('{"Date": "%s", "Ticker":"%s"}', this_date, this_ticker)
  
  set_query <- paste('{"$set":', json_strings[val], '}')
  today.core$update(query, set_query, upsert = T)
}
}

# disconnect from mongo ----

today.core$disconnect(gc = TRUE)
tickers.db$disconnect(gc = TRUE)
db.earnings.dates$disconnect(gc = TRUE)
db.earnings.summaries$disconnect(gc = TRUE)
db.timeNow$disconnect(gc = TRUE)

q()


