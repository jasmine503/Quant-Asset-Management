library(data.table)
library(readxl)
library(zoo)
library(dplyr)
library(xts)
library(readxl)
rm(list=ls())

#Q1
#-----------loading the data-------------------#
setwd("~/Desktop/UCLA Quarter3/QAM/hw3")
CRSP_stock<-read.csv("CRSP_stock.csv") %>% as.data.table()
raw<-CRSP_stock
raw <- raw[raw$EXCHCD==1|raw$EXCHCD==2|raw$EXCHCD==3]
raw <- raw[raw$SHRCD==10|raw$SHRCD==11]
# set the date
raw$date<-as.Date(as.character(raw$date),format = "%Y%m%d")
# create year and month for CRSP_stock
raw[,Year:=format(date,"%Y")]
raw[,Month:=format(date,"%m")]
raw[,YrMo:=as.numeric(Year)*12+as.numeric(Month)]
# change the data type to numeric 
raw[,DLRET:=as.numeric(as.character(DLRET))]
raw[,RET:=as.numeric(as.character(RET))]
#raw<-raw[!is.na(raw$PRC),]
#raw<-raw[!is.na(raw$SHROUT),]
setkey(raw,PERMNO)

#-------get firms ranking returns------------#
# create cum-dividend returns 
raw[is.na(RET),CRET:=DLRET]
raw[is.na(DLRET),CRET:=RET]
raw[!is.na(RET)&&!is.na(DLRET),CRET:=(1+RET)*(1+DRET)-1]

# get the lagged market cap 
raw[,MV:=abs(PRC)*SHROUT]
raw[,lag_Mkt_Cap:=shift(MV),by=PERMNO]

# select restrictions from notes price(t-13), ret(t-2), mv(t-1)
raw[,`:=`(PRC_T=shift(PRC,13),RET_T=shift(CRET,2)), by = PERMNO]
raw[,jud:=ifelse(is.na(raw$PRC_T)|is.na(raw$RET_T)|is.na(raw$lag_Mkt_Cap),0,1)]

# select at least 8 months data
raw[,prev_YrMo:=shift(YrMo,12),by=PERMNO]
# check whether minimum of 8 monthly return for past 11 months
raw[,valid_lag:=ifelse((shift(YrMo,2)-prev_YrMo)>=8,1,0)]

raw[,prev_RET:=shift(CRET,2),by=PERMNO]
# get the cumulative sum of return from t-12 to t-2
raw[,Ranking_Ret:=as.numeric(rollapplyr(log(1+prev_RET),11,sum,align="right",fill=NA)),by=PERMNO]
raw<-raw[valid_lag==1,]
output1<-raw[,.(Year,Month,PERMNO,EXCHCD,lag_Mkt_Cap,Ret=CRET,Ranking_Ret)]
output1<-output1[Year>=1927 & Year <=2018,]
mean(exp(raw$Ranking_Ret)-1,na.rm=TRUE)

#Q2
raw1<-output1 %>% as.data.table()
#delete ranking returns which is NA
raw1<-na.omit(raw1)
raw1[,DM_decile:=as.numeric(cut(Ranking_Ret,
                                breaks = quantile(Ranking_Ret,probs=seq(0,1,by=0.1),type = 7),
                                include.lowest = T)),.(Year,Month)]

raw1[,KRF_decile:=as.numeric(cut(Ranking_Ret,
                                breaks = quantile(.SD[EXCHCD==1,Ranking_Ret],probs=seq(0,1,by=0.1),type = 7),
                                include.lowest = T)),.(Year,Month)]
#since the quantile for NYSE (KRF_decile) may not fully cover stocks
#make KRF_decile which is NA equals to DM_decile
raw1<-raw1[is.na(KRF_decile),KRF_decile:=DM_decile]
output2<-raw1[,.(Year,Month,PERMNO,lag_Mkt_Cap,Ret,DM_decile,KRF_decile)]


#Q3
#-----------loading the data-------------------#
raw3<-output2
FF<-read_excel("~/Desktop/FF_mkt.xlsx") %>% as.data.table()
FF[,Date:=as.Date(as.character(as.numeric(Date)*100+1),format="%Y%m%d")]
FF[,Year:=format(Date,"%Y")]
FF[,Month:=format(Date,"%m")]
FF_mkt<-FF[,.(Year,Month,Market_minus_Rf=`Mkt-RF`/100,SMB=SMB/100,HML=HML/100,Rf=RF/100)]

#-------- get the decile return----------------#
raw3[,DM_weight:=lag_Mkt_Cap/sum(lag_Mkt_Cap),.(Year,Month,DM_decile)]
temp_DM = raw3[,.(DM_Ret=weighted.mean(Ret,DM_weight)),.(Year,Month,DM_decile)]
setorder(temp_DM)

raw3[,KRF_weight:=lag_Mkt_Cap/sum(lag_Mkt_Cap),.(Year,Month,KRF_decile)]
temp_KRF = raw3[,.(KRF_Ret=weighted.mean(Ret,KRF_weight)),.(Year,Month,KRF_decile)]
setorder(temp_KRF)

output3 <- cbind(temp_DM,temp_KRF)[,c(-5,-6)]
output3<-merge(output3,FF_mkt,by=c("Year","Month"))[,c(-7,-8,-9)]

#Q4
raw4<-output3
mean_DM<-c();SD_DM<-c();SR_DM<-c();Sk_DM<-c()
#get the exceess return for DM decile return and KRF decile return
raw4[,exc_DM_Ret:=DM_Ret-Rf]
#find the winner minus loser
Ret_wml=raw4[DM_decile==10,]$DM_Ret-raw4[DM_decile==1,]$DM_Ret
mean_wml=mean(Ret_wml)*12*100 ; sigma_wml=sd(Ret_wml)*sqrt(12)*100
SR_wml=mean_wml/sigma_wml ; Sk_wml=skewness(log(1+Ret_wml))
WML<-cbind(mean_wml,sigma_wml,SR_wml,Sk_wml)

raw4[,`:=`(r_rf_DM=12*mean(exc_DM_Ret),simga_DM=sqrt(12)*sd(DM_Ret),
           SR_DM=sqrt(12)*mean(exc_DM_Ret)/sd(DM_Ret),Sk_DM=skewness(log(1+DM_Ret)),
           .(DM_decile))]

for (i in 1:10){
   mean_DM[i]<-raw4[DM_decile==i,]$r_rf_DM[1]*100
   SD_DM[i]<-raw4[DM_decile==i,]$simga_DM[1]*100
   SR_DM[i]<-raw4[DM_decile==i,]$SR_DM[1]
   Sk_DM[i]<-raw4[DM_decile==i,]$Sk_DM[1]
   }
output4<-rbind(mean_DM,SD_DM,SR_DM,Sk_DM)
output4<-cbind(output4,t(WML))
colnames(output4)<-c(paste("Decile",c(1:10),sep=""),"WML")

#Q5
DM_return<-read.csv("~/Desktop/10_Portfolios_Prior_12_2.csv") %>% as.data.table()
DM_return[,date := as.integer(as.character(X))]
DM_return[,`:=`(Year=as.integer(date/100),Month=as.integer(date%%100))]
DM_return=DM_return[,.(Year,Month,Decile1=Lo.PRIOR,Decile2=PRIOR.2,Decile3=PRIOR.3,
                         Decile4=PRIOR.4,Decile5=PRIOR.5,Decile6=PRIOR.6,Decile7=PRIOR.7,
                         Decile8=PRIOR.8,Decile9=PRIOR.9,Decile10=Hi.PRIOR)]
DM_return$WML=DM_return$Decile10-DM_return$Decile1
DM_return=DM_returns[c(-1105,-1106,-1107),]
DM_return=as.data.frame(DM_return)


KRF_returns<-read_excel("~/Desktop/m_m_pt_tot.xlsx")[,c(-4,-5)] %>% as.data.table()
KRF_returns[,date := as.Date(as.character(date),format = "%Y%m%d")]
KRF_returns[,Year:=format(date,"%Y")]
KRF_returns[,Month:=format(date,"%m")]
KRF_returns = KRF_returns[,.(Year,Month,decile,KRF_Ret = Ret)]
setkey(KRF_returns,decile)
KRF_return<-matrix(nrow=1035,ncol=12) %>% as.data.table()
KRF_return[,1]<-KRF_returns[.(1)]$Year
KRF_return[,2]<-KRF_returns[.(2)]$Month
for( i in 1:10){
  KRF_return[,i+2]<-100*KRF_returns[.(i)]$KRF_Ret
}
colnames(KRF_return)<-c("Year","Month",paste("Decile",c(1:10),sep=""))
KRF_return[,WML:=Decile10-Decile1]
KRF_return=as.data.frame(KRF_return)

my_DM=dcast(temp_DM,Year+Month~DM_decile,value.var = "DM_Ret")
colnames(my_DM)<-c("Year","Month",paste("Decile",c(1:10),sep=""))
my_DM[,WML:=Decile10-Decile1]
my_DM<-as.data.frame(my_DM)

my_KRF=dcast(temp_KRF,Year+Month~KRF_decile,value.var = "KRF_Ret")
colnames(my_KRF)<-c("Year","Month",paste("Decile",c(1:10),sep=""))
my_KRF[,WML:=Decile10-Decile1]
my_KRF<-my_KRF[-c(1036:1104),]
my_KRF<-as.data.frame(my_KRF)

cor_DM<-c(); cor_KRF<-c()
for (i in 1:11){
  cor_DM[i]<-cor(DM_return[,(i+2)],my_DM[,(i+2)])
  cor_KRF[i]<-cor(KRF_return[,(i+2)],my_KRF[,(i+2)])
}
output5<-rbind(cor_DM,cor_KRF)
colnames(output5)<-c(paste("Decile",c(1:10),sep=""),"HML")

#Q6
DM_return<-as.data.table(DM_return)
setkey(DM_return,Year)
plot(DM_return[.(2015:2018)]$Decile1,type="l",col="blue",main="Return for Decile1, 10 and WML",
     xlab="time (2015-2018)",ylab="return",lty=2)
lines(DM_return[.(2015:2018)]$Decile10,type="l",col="steelblue",lty=2)
lines(DM_return[.(2015:2018)]$WML,typ="l",col="red")
legend(35,-10,legend=c("WML", "Decile1","Decile10"),
       col=c("red", "blue","steelblue"),lty=1,cex=0.5)





