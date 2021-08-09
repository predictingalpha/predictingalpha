library(mongolite)
library(Quandl)

#orats api url
Sys.setenv(TZ = "EST")
orats.url = 'https://api.orats.io/datav2/cores?token=28a2528a-34e8-448d-877d-c6eb709dc26e'

#mongo api url
urlold = "mongodb://usesr:pass@jordandb-shard-00-00-ykcna.mongodb.net:27017,jordandb-shard-00-01-ykcna.mongodb.net:27017,jordandb-shard-00-02-ykcna.mongodb.net:27017/test?ssl=true&replicaSet=JordanDB-shard-0&authSource=admin&retryWrites=false"
urlm = "mongodb+srv://jordan:libraryblackwell7@cluster0.skqv5.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"

#quandl url
Quandl.api_key('j76JNRpoVX5LF_s7-wWR')


today.core = mongo(collection = "PAtime", db = "pa", url = urlm, verbose = F)
h.imp.moves = mongo(collection = "ErMoves", db = "pa", url = urlm)
tickers.db = mongo(collection = "Tickers", db = "pa", url = urlm, verbose = F)
db.earnings.dates = mongo(collection = "NextErDates", db = "pa", url = urlm, verbose = F)
db.earnings.summaries = mongo(collection = "Summaries", db = "pa", url = urlm, verbose = F)
db.newTickers = mongo(collection = "newTickers", db = "pa", url = urlm, verbose = F)
db.readyUploadEr = mongo(collection = "readyUploadEr", db = "pa", url = urlm, verbose = F)
db.whisper = mongo(collection = "whisper", db = "pa", url = urlm, verbose = F)

#patime
#historical moves
#tickers
#dbearningsdates
#dbearningsmovessummaries
#earningstoUpload
#newTickers
