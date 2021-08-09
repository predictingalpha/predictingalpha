source("setup.R")

f.df = h.imp.moves$find()
summaries = f.df %>%drop_na(StraddlePnL)%>% group_by(Ticker)%>%dplyr::summarise(AvgMv = mean(abs(Move), na.rm = T), 
                                                                                AvgJump = mean(abs(Jump), na.rm = T), 
                                                                                MedMv = median(abs(Move), na.rm = T), 
                                                                                CallPnL = last(Call_Cum), 
                                                                                PutPnL = last(Put_Cum), 
                                                                                StraddlePnL = last(StraddlePnL),
                                                                                AvgImpMv = mean(ImpErnMv, na.rm = T)
)
db.earnings.summaries$remove("{}")
db.earnings.summaries$insert(summaries)


