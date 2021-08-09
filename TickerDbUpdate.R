source("setup.R")
registerDoParallel(6)
future::plan(future::multisession, 
             workers = 8) 

#remove new ticker DB
db.newTickers$remove("{}")
currentTicks = tickers.db$find()
currentTickers = currentTicks %>% pull(Ticker)
x = jsonlite::fromJSON(orats.url)
x = x$data
x = x%>% filter(avgOptVolu20d > 1000, pxAtmIv > 9) #this will be some filter


tickersdb  = x %>% dplyr::select(ticker, assetType)
tickersdb$Type = ifelse(tickersdb$assetType %in% c(0,3), "Stock", "ETF")
tickersdb = tickersdb[,-2]
names(tickersdb)[1] = "Ticker"

tickersdb = tickersdb %>% filter(!(Ticker %in% currentTickers))
### get sector + industry data
get.sector.data = function(x){tryCatch({
  url = paste0("https://ca.finance.yahoo.com/quote/",x,"/profile?p=", x)
  a = read_html(url)
  sector = a %>%  html_nodes(xpath = "/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[1]/div/div/section/div[1]/div/div/p[2]/span[2]")%>% html_text()
  industry =  a %>% html_nodes(xpath = "/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[1]/div/div/section/div[1]/div/div/p[2]/span[4]")%>%html_text()
  
  d = data.frame(Ticker = x, Industry = industry, Secotr = sector)
  
  d}, error = function(e){NA})
}

noEtfs = tickersdb %>% filter(Type == "Stock")%>%pull(Ticker)

noEtfsdb = foreach(r = noEtfs, .combine = 'rbind', .packages=c("httr", "rvest", "tidyverse")) %dopar%{
  get.sector.data(r)
}

### combine database
tickersdb = left_join(tickersdb, noEtfsdb, by = "Ticker")

#tickers.db
data = bind_rows(currentTicks, tickersdb)

tickers.db$remove("{}")
tickers.db$insert(data)


db.newTickers$insert(tickersdb)
