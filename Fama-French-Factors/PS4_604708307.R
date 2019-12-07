library(data.table)
library(readxl)
library(base)
library(zoo)
library(dplyr)
library(xts)
library(readxl)
library(RPostgres)
rm(list=ls())

#load the CRSP data and clean it--------------------------------

setwd("~/Desktop/UCLA Quarter3/QAM/hw4")
CRSP<-read.csv("CRSP.csv") %>% as.data.table()
CRSP <- CRSP[CRSP$EXCHCD==1|CRSP$EXCHCD==2|CRSP$EXCHCD==3]
CRSP <- CRSP[CRSP$SHRCD==10|CRSP$SHRCD==11]

# set the date
CRSP$date<-as.Date(as.character(CRSP$date),format = "%Y%m%d")
CRSP$year<-year(CRSP$date)
CRSP$month<-month(CRSP$date)

#if return is -99.0, -88.0, -77.0, -66.0,  B, C for Return 
CRSP[RET %in% c("B","C","-99","-88","-77","-66",""," "), RET:=NA]
#if delist retun is -99.0, -88.0,-77.0, -66.0, P, S, T
CRSP[DLRET %in% c("A","P","S","T","-99","-88","-77","-66",""," "),DLRET := NA]

#calculate the cum-Dividend returns
CRSP[is.na(RET),CRET:=DLRET]
CRSP[is.na(DLRET),CRET:=RET]
CRSP[!is.na(RET) && !is.na(DLRET),CRET:=(1+RET)*(1+DRET)-1]

# find the market cap 
CRSP[,PRC:=abs(PRC)]
CRSP[,MV:=PRC*SHROUT/1000]

#limit data to that with full avaiablity 
CRSP[,YrMo:=year(date)*12 + month(date)]
CRSP[,prev_YrMo:=shift(YrMo),by=PERMNO]
CRSP[,valid_lag:= YrMo == (prev_YrMo + 1)]
#CRSP<-CRSP[valid_lag == T &  lagMV >0 & !is.na(CRET)]

#previous month's market cap is multiple PERMNOs per PERMCO----
#CRSP[,PERMCO_weight:=lagMV/sum(lagMV),by=.(PERMCO,date)]
CRSP[,lagMV:=shift(MV),by=.(PERMNO)]
CRSP[,MKT_Cap:=sum(lagMV),by=.(PERMCO,date)]

CRSP<-CRSP[,.(PERMCO,PERMNO,EXCHCD,date,year,month,PRC,SHROUT,CRET,MV,MKT_Cap)]


#load the ComputStat data and clean it-------------------------
#ComputStat from 1960-12 to 2017-12
ACCT<-read.csv("~/Desktop/ACCT.csv") %>% as.data.table
#select non-financial firms
ACCT<-ACCT[ACCT$sic > 6199 |ACCT$sic < 6190 ]
# set the date
ACCT$date<-as.Date(as.character(ACCT$datadate),format = "%Y%m%d")
setkey(ACCT)
ACCT<-ACCT[,.(gvkey,date,fyear,at,ceq,itcb,lt,mib,pstk,pstkl,pstkrv,seq,txdb,txditc)]

PRBA<-read.csv("~/Desktop/PRBA.csv") %>% as.data.table
# set the date
PRBA$date<-as.Date(as.character(PRBA$datadate),format = "%Y%m%d")
PRBA<-PRBA[,.(gvkey,date,prba)]
CPST<-merge(ACCT,PRBA,by.x=c("gvkey","date"),by.y=c("gvkey","date"),all.x = TRUE)

#define fundamental factors
CPST[,SHE:=coalesce(seq,ceq+pstk,at-lt-mib,at-lt)]
CPST[,DT:=coalesce(txditc,itcb+txdb,itcb,txdb)]
CPST[,PS:=coalesce(pstkrv,pstkl,pstk)]
CPST[,PS_n:=-PS]; CPST[,prba_n:=-prba]
CPST[,BE:=ifelse(is.na(SHE),NA,apply(.SD,1,sum,na.rm=TRUE)),.SDcols=c("SHE","PS_n","DT","prba_n")]
setkey(CPST)
CPST[,gvkey:=as.integer(gvkey)]
CPST<-CPST[,.(gvkey,date,fyear,SHE,DT,PS,prba,at,BE)]

#get the link table-----------------------------------------------
wrds <- dbConnect(Postgres(),
                  host = 'wrds-pgdata.wharton.upenn.edu',
                  port = 9737,
                  user = 'meix',
                  password = 'Mei19960503',
                  sslmode = 'require',
                  dbname = 'wrds')

#import Compustat data 
res <- dbSendQuery(wrds, 
                   "select gvkey, linkprim, liid, linktype, lpermno, lpermco, USEDFLAG, 
                   linkdt, linkenddt from CRSPA.CCMXPF_LINKTABLE where linktype = 'LC' or linktype = 'LU'")
#prepare the link table
lkt <- data.table(dbFetch(res))
lkt[,gvkey:=as.integer(gvkey)]
lkt<-lkt[linkprim=="P" | linkprim =="C"]

#merge CPST and the linktable---------------------------------------
merged<-merge(CPST, lkt, by.x = "gvkey", by.y="gvkey",allow.cartesian = T)
setkey(merged)
# make sure the gvkey date is within the link date range
merged<-merged[(is.na(linkdt) | date >=linkdt) & (is.na(linkenddt) | date <= linkenddt)]
setorder(merged, gvkey,date)
names(merged)[2]= "datadate"

#merge CRSP with CompuStat
full<-merge(CRSP,merged,by.x=c("PERMCO","year"),by.y=c("lpermco","fyear")) %>% as.data.table()
full<-full[,.(date,PERMNO,PERMCO,EXCHCD,year,gvkey,CRET,MV,MKT_Cap,SHE,at,DT,PS,BE,linktype,linkprim,liid,linkdt,linkenddt)]  
# make sure the PERMCO date is within the link date range
full<-full[(is.na(linkdt) | date >=linkdt) & (is.na(linkenddt) | date <= linkenddt)]
setorder(full,PERMCO,date)

#multiple gvkeys per PERMCO
#if LC not LC linktype, only keep LC
full[,prob:=.N>1,by=.(PERMCO,date)] #if not one-to-one match, problem=1
full[,Good_match := sum(linktype=="LC"),by=.(PERMCO,date)]
full<-full[!(prob == T & Good_match == T & linktype != 'LC')]
#if P and not P linkprim, only keep P
full[,prob:=.N>1,by=.(PERMCO,date)]
full[,Good_match:=sum(linkprim=="P"),by=.(PERMCO,date)]
full<-full[!(prob == T & Good_match == T & linkprim != 'P')]
# if 1 and not 1 liid, only keep 1
full[,prob:=.N>1,by=.(PERMCO,date)]
full[,Good_match:=sum(liid==1),by=.(PERMCO,date)]
full<-full[!(prob==T & Good_match == T & liid != 1)]
#use the link that is current
full[,prob:=.N>1,by=.(PERMCO,date)]
full[,Good_match:=sum(is.na(linkenddt)),by=.(PERMCO,date)]
full<-full[!(prob==T & Good_match == T & is.na(linkenddt))]
#use the link has been around the longest
full[,prob:=.N>1,by=.(PERMCO,date)]
full[,Good_match:=NULL]
full[is.na(linkenddt),linkenddt:=as.Date("2017-01-31","%Y-%m-%d")]
full[,Date_diff := as.integer(linkenddt-linkdt)]
setorder(full,PERMCO, date, Date_diff)
full[prob == T, Good_Match := Date_diff == Date_diff[.N], by = .(PERMCO,date)]
full=full[!(prob == T & Good_Match != T)]
#use the gvkey that has been around the longest
full[,prob:=.N>1, by=.(PERMCO,date)]
full[, Good_Match := NULL]
setorder(full,gvkey,linkdt)
full[prob == T, start_date:= linkdt[1],by=.(gvkey)]
setorder(full,gvkey,linkenddt)
full[prob == T, end_date := linkenddt[.N], by = .(gvkey)]
full[,Date_diff:=as.integer(end_date-start_date)]
setorder(full,PERMCO,date,Date_diff)
full= full[prob == T, Good_match:=Date_diff == Date_diff[.N],by=.(PERMCO,date)]
full=full[!(prob==T&Good_match!=T)]
#use the smaller gvkey
setorder(full,PERMCO,date,gvkey)
full=unique(full,by=c("PERMCO","date"))

#clean up
if(nrow(unique(full, by = c('gvkey','date'))) != nrow(full) |
   nrow(unique(full, by = c('PERMCO','date'))) != nrow(full)){
  stop('1. Monthly Firm-level returns.R: There is an issue with the merge!')
}
full<-full[,.(date,PERMNO,PERMCO,EXCHCD,year,gvkey,CRET,MV,MKT_Cap,SHE,at,DT,PS,BE,linktype,linkprim,liid,linkdt,linkenddt)] 
full<-na.omit(full)

# get year and month
full[,month:=month(date)]
full[,year:=year(date)]

full[,CRET:=as.numeric(as.character(CRET))]

###################################################################################################
#build portfolio 
#find the size portfolio return based on every June------------------------
full1<-full
full_size = full1[month == 6, .(PERMCO,EXCHCD,year,month,MKT_Cap)]
full_size[, ME_decile := as.numeric(cut(MKT_Cap,breaks = quantile(.SD[EXCHCD == 1,MKT_Cap],
                      probs = seq(from=0,to=1,by=0.1),type=7),include.lowest = T)),.(year)]
full_size[is.na(ME_decile) & (MKT_Cap <= min(full_size[EXCHCD == 1,]$MKT_Cap)), ME_decile := 1]
full_size[is.na(ME_decile) & (MKT_Cap >= max(full_size[EXCHCD == 1,]$MKT_Cap)), ME_decile := 10]
full_size = full_size[,list(PERMCO,year,ME_decile)]
full1 = merge(full1,full_size,by = c('PERMCO','year'), all.x = T)
full1[, keep_size := ifelse(!is.na(shift(ME_decile,6)) & !is.na(shift(MV)),1,0),by = PERMNO]
full1 <- full1[keep_size == 1,]
#size portfolio
size_port = full1[, (Size_Ret = weighted.mean(CRET,MV,na.rm = T)*100),.(year,month,ME_decile)]
size_port <- size_port[year>1972]
colnames(size_port)<-c("year","month","ME_decile","Size_Ret")

#compare with Fama-French data------------------------------------------------------
FF_size<-read.csv("Portfolios_Formed_on_ME.CSV") %>% as.data.table()
colnames(FF_size)<-c("date",paste("Decile",c(1:10),sep=""))
FF_size[,date:=as.numeric(as.character(date))]
FF_size[,date:=date*100+1]
FF_size[,date := as.Date(as.character(date),format = "%Y%m%d")]
FF_size[,year:=format(date,"%Y")]
FF_size[,month:=format(date,"%m")]
FF_size<-FF_size[year>1972 & year<2019,]
FF_size<-FF_size[c(1:552)]
FF_size<-as.data.frame(FF_size)
my_size<-dcast(size_port,year+month~ME_decile,value.var = "Size_Ret")[,c(1:12)] %>% as.data.frame()
cor_size<-c()
for (i in 1:10){cor_size[i]<-cor(my_size[,(i+2)],as.numeric(as.character(FF_size[,(i+1)])))}
##############################################################################################

#find the BM portfolio return based on every December--------------------------------
# if total asset is NA, assign BE to be NA as well, if BE is 0, assign BE to be NA
full2<-full
full2[BE == 0 | (is.na(at)), BE:=NA]
full2[,BEME:=BE/MKT_Cap]
full2<-full2[!is.na(BEME) & BEME>=0]

full_BM = full2[month == 12]
full_BM[,BM := BE/MKT_Cap]
full_BM[,BM_decile := as.numeric(cut(BM,breaks = quantile(.SD[EXCHCD==1,BM],probs = seq(0,1,0.1), type = 7),
                                                    include.lowest = T)),.(year)]
full_BM[is.na(BM_decile) & (BM <= min(full_BM[EXCHCD == 1,]$BM)), BM_decile := 1]
full_BM[is.na(BM_decile) & (BM >= max(full_BM[EXCHCD == 1,]$BM)), BM_decile := 10]

full2 = merge(full2,full_BM[,.(PERMCO,year,BM_decile)],
                            by = c("PERMCO","year"),all.x = T)
full2[, keep_BM := ifelse(!is.na(shift(BM_decile,6))&
                                          !is.na(shift(MV)),1,0), by = PERMNO]
full2 = full2[keep_BM == 1]
#BM portfolio
BM_port = full2[,(BM_RET = weighted.mean(CRET,MKT_Cap, na.rm = TRUE)*100),.(year,month,BM_decile)]
BM_port <- BM_port[year>1972]
colnames(BM_port)<-c("year","month","BM_decile","BM_Ret")

#compare with Fama-French Data-------------------------------------------------
FF_BM<-read.csv("Portfolios_Formed_on_BE-ME.CSV") %>% as.data.table()
colnames(FF_BM)<-c("date",paste("Decile",c(1:10),sep=""))
FF_BM[,date:=as.numeric(as.character(date))]
FF_BM[,date:=date*100+1]
FF_BM[,date := as.Date(as.character(date),format = "%Y%m%d")]
FF_BM[,year:=format(date,"%Y")]
FF_BM[,month:=format(date,"%m")]
FF_BM<-FF_BM[year>1972 & year<2019,]
FF_BM<-FF_BM[c(1:552)]
FF_BM<-as.data.frame(FF_BM)

my_BM<-dcast(BM_port,year+month~BM_decile,value.var = "BM_Ret")[,c(1:12)] %>% as.data.frame()
cor_BM<-c()
for (i in 1:10){cor_BM[i]<-cor(my_BM[,(i+2)],as.numeric(as.character(FF_BM[,(i+1)])))}

#find SMB portfolio return -----------------------------------------
#construct the SMB portfolio
full3<-full
full_SMB = full3[month == 6, .(PERMCO,EXCHCD,year,month,MKT_Cap)]
full_SMB[,SMB_decile:=as.numeric(cut(MKT_Cap,breaks = quantile(MKT_Cap, 
                                                               breaks = quantile(.SD[EXCHCD == 1,MKT_Cap],
  probs = c(0,0.3,0.7,1),type=7),include.lowest = T))),.(year)]
full_SMB[is.na(SMB_decile) & (MKT_Cap <= min(full_SMB[EXCHCD == 1,]$MKT_Cap)), SMB_decile := 1]
full_SMB[is.na(SMB_decile) & (MKT_Cap >= max(full_SMB[EXCHCD == 1,]$MKT_Cap)), SMB_decile := 3]

full3 = merge(full3,full_SMB[,.(PERMCO,year,SMB_decile)],
              by = c("PERMCO","year"),all.x = T)
full3[, keep_SMB := ifelse(!is.na(shift(SMB_decile,6)) & !is.na(shift(MV)),1,0),by = PERMNO]
full3 = full3[keep_SMB == 1,]
#SMB portfolio
SMB_port = full3[, (SMB_Ret = weighted.mean(CRET,MV,na.rm = T)*100),.(year,month,SMB_decile)]
SMB_port <- SMB_port[year>1972]
my_SMB_Ret <- SMB_port[SMB_decile==1]$V1 - SMB_port[SMB_decile==3]$V1

#compare with FF data 
FF_SMB<-read.csv("F-F_Research_Data_Factors.CSV") %>% as.data.table()
FF_SMB[,date:=as.numeric(as.character(X))]
FF_SMB[,date:=date*100+1]
FF_SMB[,date := as.Date(as.character(date),format = "%Y%m%d")]
FF_SMB[,year:=format(date,"%Y")]
FF_SMB[,month:=format(date,"%m")]
FF_SMB<-FF_SMB[year>1972 & year<2019,]
FF_SMB_Ret<-FF_SMB$SMB

corr_SMB<-cor(my_SMB_Ret,FF_SMB_Ret)


FF_research = read.csv("F-F_Research_Data_Factors.csv") %>% as.data.table()
FF_research[,`:=`(Year = as.integer(X/100), Month = X%%100)]
setorder(FF_research,Year,Month)
FF_rf = FF_research[Year>=1973&Year<=2018,list(Year,Month,RF)] %>% as.data.frame()
FF_SMB = FF_research[Year>=1973&Year<=2018]$SMB
FF_HML = FF_research[Year>=1973&Year<=2018]$HML

Return_size = c()
Return_bm = c()
std_size = c()
std_bm = c()
skew_size = c()
skew_bm = c()
correlation_size = c()
correlation_bm = c()
for(i in 1:10){
  Return_size[i] = mean(test_size[size_decile==i]$V1*100-FF_rf$RF)*12
  Return_bm[i] = mean(test_bm[BM_decile==i]$V1*100-FF_rf$RF)*12
  std_size[i] = sd(test_size[size_decile==i]$V1*100-FF_rf$RF)*sqrt(12)
  std_bm[i] = sd(test_bm[BM_decile==i]$V1*100-FF_rf$RF)*sqrt(12)
  skew_size[i] = skewness(test_size[size_decile==i]$V1)
  skew_bm[i] = skewness(test_bm[BM_decile==i]$V1)
  correlation_size[i] = cor(test_size[size_decile==i]$V1*100-FF_rf$RF,FF_size[,10+i])
  correlation_bm[i] = cor(test_bm[BM_decile==i]$V1*100-FF_rf$RF,FF_bm[,10+i])
}
Return_size[11] = mean(test_size[size_decile==10]$V1*100-test_size[size_decile==1]$V1*100)*12
Return_bm[11] = mean(test_bm[BM_decile==10]$V1*100-test_bm[BM_decile==1]$V1*100)*12
std_size[11] = sd(test_size[size_decile==10]$V1*100-test_size[size_decile==1]$V1*100)*sqrt(12)
std_bm[11] = sd(test_bm[BM_decile==10]$V1*100-test_bm[BM_decile==1]$V1*100)*sqrt(12)
skew_size[11] = skewness(test_size[size_decile==10]$V1*100-test_size[size_decile==1]$V1*100)
skew_bm[11] = skewness(test_bm[BM_decile==10]$V1*100-test_bm[BM_decile==1]$V1*100)
correlation_size[11] = cor(test_size[size_decile==1]$V1*100-test_size[size_decile==10]$V1*100,FF_SMB)
correlation_bm[11] = cor(test_bm[BM_decile==10]$V1*100-test_bm[BM_decile==1]$V1*100,FF_HML)

SR_size = Return_size/std_size
SR_bm = Return_bm/std_bm

Size = rbind(Return_size,std_size,SR_size,skew_size,correlation_size) %>% as.data.frame()
BM = rbind(Return_bm,std_bm,SR_bm,skew_bm,correlation_bm) %>% as.data.frame()
for(i in 1:10){
  colnames(Size)[i] = paste0("decile",i)
  colnames(BM)[i] = paste0("decile",i)
}
colnames(Size)[11] = "LongShort"; colnames(BM)[11] = "LongShort"

Return_SMB = mean(SMB_Port*100-FF_rf$RF)*12
Return_HML = mean(HML_Port*100-FF_rf$RF)*12
std_SMB = sd(SMB_Port*100-FF_rf$RF)*sqrt(12)
std_HML = sd(HML_Port*100-FF_rf$RF)*sqrt(12)
SR_SMB = Return_SMB/std_SMB
SR_HML = Return_HML/std_HML

#Q4
FF_size<-as.data.frame(FF_size)
FF_size$LS<-as.numeric(as.character(FF_size$Decile10))-as.numeric(as.character(FF_size$Decile1))
LS_size<-FF_size$LS[492:552]/100

FF_BM<-as.data.frame(FF_BM)
FF_BM$LS<-as.numeric(as.character(FF_BM$Decile10))-as.numeric(as.character(FF_BM$Decile1))
LS_BM<-FF_BM$LS[492:552]/100

plot(LS_size,type="l",col="steelblue",main="Return for Size and BM(Long minus Short)",
     xlab="time",ylab="return",lty=1,ylim=(c(-0.1,0.2)))
lines(LS_BM,type="l",col="green",lty=1)
legend("bottomright",legend=c("size", "BM"),
       col=c("steelblue", "green"),lty=1,cex=0.5)






























