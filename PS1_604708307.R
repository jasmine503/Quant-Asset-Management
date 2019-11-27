library(data.table)
library(readxl)
library(zoo)
library(dplyr)
rm(list=ls())

#Q1
#-----------loading the data-------------------#
raw<-read.csv("QAM_11.csv") %>% as.data.table()
raw <- raw[raw$EXCHCD==1|raw$EXCHCD==2|raw$EXCHCD==3]
raw <- raw[raw$SHRCD==10|raw$SHRCD==11]
# change the data type
raw[,DLRET:=as.numeric(as.character(DLRET))]
raw[,RET:=as.numeric(as.character(RET))]
raw<-raw[!is.na(raw$PRC),]
# set the date
raw$date<-as.Date(as.character(raw$date),format = "%Y%m%d")
setkey(raw)
CRSP_Stocks<-raw

PS1_Q1<-function(raw){
#-----------market return----------------------#
  # create cum-dividend returns 
  raw[is.na(RET),CRET:=DLRET]
  raw[is.na(DLRET),CRET:=RET]
  raw[!is.na(RET)&&!is.na(DLRET),CRET:=(1+RET)*(1+DRET)-1]
  raw<-raw[!is.na(raw$CRET),]
  
  # find the market cap and weight
  setorder(raw,PERMNO,date)
  raw[,PRC:=abs(PRC)]
  raw[,MV:=PRC*SHROUT]
  raw[,lagMV:=shift(MV),by=PERMNO]
  raw<-raw[!is.na(raw$lagMV),]
  raw[,sumMV:=sum(lagMV),by=date]
  
  #get sum of weighted return 
  raw[,weiRET:=CRET*(lagMV/sumMV)]
  raw[,mktRET:=sum(weiRET),by=date]
  
  #get equal wighted return
  raw[,q:=length(unique(PERMNO)),by=date]
  raw[,equRET:=sum(CRET/q)]
  
  #summary the output
  output<-raw[,list(Year = head(format(date,"%Y"),1),
                    Month = head(format(date,"%m"),1),
                    Stock_lag_MV = head(sumMV,1),
                    Stock_Ew_Ret = head(equRET,1),
                    Stock_Vw_Ret = head(mktRET,1)), by = date]
  setkey(output,Year,Month)
  return(output)
}
Monthly_CRSP_Stocks<-PS1_Q1(CRSP_Stocks)



#Q2
#-----------loading the data-------------------#
FF<-as.data.table(read_excel("~/Desktop/QAM_FF.xlsx"))
FF[,Date:=as.Date(as.character(as.numeric(Date)*100+1),format="%Y%m%d")]
FF[,Year:=format(Date,"%Y")]
FF[,Month:=format(Date,"%m")]
FF_mkt<-FF[,list(Year=Year,Month=Month,Market_minus_Rf=`Mkt-RF`,SMB=SMB,HML=HML,Rf=RF)]

PS1_Q2<-function(Monthly_CRSP_Stocks,FF_mkt){
#-----------excess return----------------------#
    raw2<-merge(Monthly_CRSP_Stocks,FF_mkt,by=c("Year","Month"))
    raw2[,exc_Ret:=Stock_Vw_Ret-(Rf/100)]
    library(moments)
    output2<-raw2[,list(Annualized_Mean =c(mean(exc_Ret)*12,mean(Market_minus_Rf/100)*12),
                      Annualized_Std = c(sd(exc_Ret)*sqrt(12),sd(Market_minus_Rf/100)*sqrt(12)),
                      Annualized_Sharpe_Ratio=c(mean(exc_Ret)*12/(sd(exc_Ret)*sqrt(12)),
                                                mean(Market_minus_Rf/100)*12/(sd(exc_Ret)*sqrt(12))),
                      Skewness= c(skewness(exc_Ret),skewness(Market_minus_Rf/100)),
                      excess_kurtosis= c(kurtosis(exc_Ret)-3,kurtosis(Market_minus_Rf/100)-3))]
    rownames(output2)<-c("Estimated FF Market Excess Return","Actual FF Market Excess Return")
    return(output2)
}
Numeric_Matrix<-PS1_Q2(Monthly_CRSP_Stocks,FF_mkt)



#Q3
PS1_Q3<-function(Monthly_CRSP_Stocks,FF_mkt){
  raw2<-merge(Monthly_CRSP_Stocks,FF_mkt,by=c("Year","Month"))
  raw2[,exc_Ret:=Stock_Vw_Ret-(Rf/100)]
  raw2[,abs_diff:=abs(Market_minus_Rf/100-exc_Ret)]
  output3<-raw2[,list(Correlation=cor(Market_minus_Rf,exc_Ret),
                      max_abs_diff=max(abs_diff))]
  return(output3)
}
Correlation<-PS1_Q3(Monthly_CRSP_Stocks,FF_mkt)

