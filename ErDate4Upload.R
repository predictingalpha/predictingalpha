source("setup.R")
db = db.earnings.dates$find()
currentReady = db.readyUploadEr$find()
#currentReady =ccurrentReady %>% filter(ErDate > Sys.Date()-7)

todayEr = db %>% filter(Confirmed == T, ErDate == Sys.Date())

newER = bind_rows(currentReady, todayEr)

db.readytoUpload$remove("{}")
db.readyUploadEr$insert(newER)
