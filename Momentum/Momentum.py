#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec  5 17:21:25 2019

@author: meixiangui
"""

import numpy as np
import pandas as pd
import datetime as dt
from scipy.stats import skew
import matplotlib.pyplot as plt


def PS3_Q1(dat):
    #### STEP One: define the universe of monthly returns and ranking returns ####
    ##############################################################################

    # Look at the data types
    dat.dtypes

    # Fill na with -99, then transform the datatype into int
    dat[["SHRCD", "EXCHCD", "SHROUT"]] = dat[["SHRCD", "EXCHCD", "SHROUT"]].fillna(-99)

    dat[["PERMNO", "SHRCD", "EXCHCD", "SHROUT"]] = dat[["PERMNO", "SHRCD", "EXCHCD", "SHROUT"]].astype(int)

    ## Replace -99 with NaN
    dat.replace([-99], np.nan, inplace = True)

    # Convert "date" to datetime
    dat["date"] = pd.to_datetime(dat["date"], format = "%Y%m%d")

    # Push the "last trading date of a month 7-29" to the "last calender date of a month 7-31" 
    dat["date"] = dat["date"] + pd.offsets.MonthEnd(0)

    # Add explicit year and month as new columns
    dat.loc[:,"Year"] = dat["date"].dt.year
    dat.loc[:,"Month"] = dat["date"].dt.month


    ################# Slicing the data and Do computation #########
    # subset the data by Share Code and Exchange Code, time
    dat = dat.query("SHRCD == [10, 11] and EXCHCD == [1, 2, 3]")

    # calculate market cap t
    dat["mktval"] = np.abs(dat["PRC"])*dat["SHROUT"]

    # Groupby each stock, take lag on its market value
    dat["lag_Mkt_Cap"] = dat.groupby(["PERMNO"])["mktval"].shift(1)

    # Convert to million
    dat["lag_Mkt_Cap"] = dat["lag_Mkt_Cap"]/1000

    ## Replace characters with NaN
    dat.replace(["A", "S", "C", "T", "P"], np.nan, inplace = True)

    # convert datatype to float
    dat["RET"] = dat["RET"].astype(float)

    ##### orignal method
    # ## calculate real return: Aggregate delisting return and HPR
    # Ret = np.where(dat["DLRET"].notna(), dat["DLRET"].astype(float), 
    #                                          dat["RET"])

    ##### ignore delisting return
    Ret = dat["RET"]

    Ret = np.where(dat["RET"].notna() & dat["DLRET"].notna(), 
                (dat["DLRET"].astype(float) + 1)*(dat["RET"] + 1) - 1, Ret)

    # create new real return
    dat["Ret"] = Ret


    ################# Create Ranking Return #########
    # create a subset to work on with
    dat_temp = dat[['PERMNO','date','RET']].sort_values(['PERMNO','date']).set_index('date')

    # create log return at period s
    dat_temp["logret"] = np.log(1 + dat_temp.RET)

    # shift return by 2 
    dat_temp["shift_log_ret"] = dat_temp.groupby(["PERMNO"])["logret"].shift(2)

    # cumulative log return
    cum_ret = dat_temp.groupby(["PERMNO"])["shift_log_ret"].rolling(11).sum()

    # reset index and rename column, merge ranking return
    cum_ret = cum_ret.reset_index().rename(columns = {"shift_log_ret" : "Ranking_Ret"})
    dat = pd.merge(dat, cum_ret, on = ["PERMNO", "date"])



    ################# Apply Data Restriction #################
    def data_restriction(df):

        tmp_ret = df.Ranking_Ret

        # initiate an indicator vector, length equal to the rows of input df
        valid_indicator = np.ones(df.shape[0])

        # check price t-13 is not missing 
        valid_indicator[np.isnan(df.PRC.shift(13))] = np.nan

        # check ret(t-2) is not missing 
        valid_indicator[np.isnan(df.RET.shift(2))] = np.nan    

        # check me(t-1) is not missing
        valid_indicator[np.isnan(df.lag_Mkt_Cap)] = np.nan

        # check a minimum of eight monthly return over past 11 months
        valid_indicator[np.isnan(df.Ret.shift(1)).rolling(11).sum() > 3] = np.nan

        # update valid return
        valid_return = valid_indicator*tmp_ret

        return valid_return

    out2 = dat.groupby(["PERMNO"]).apply(data_restriction)

    # updata the Ranking_Ret with restricted ranking return
    dat.Ranking_Ret = out2.reset_index(0, drop=True)


    # subset the output dataframe
    out_df = dat[["Year", "Month", "PERMNO", "EXCHCD", "lag_Mkt_Cap", "Ret", "Ranking_Ret"]]
    out_df = out_df.sort_values(['Year', 'Month'])
    out_df = out_df[out_df.Year >= 1927]
    
    return out_df


# In[3]:


def PS3_Q2(dat):
    #### STEP Two: define the monthly momentum portfolio decile ####
    ################################################################

    #### DM CUT - based on all stocks ####
    # cut into decile
    dat["DM_decile"] = dat.groupby(["Year", "Month"])["Ranking_Ret"].transform(lambda x: pd.qcut(x, 10, labels=False) + 1)

    #### NYSE CUT - based on nyse stocks ####
    # subset stocks in NYSE
    nyse = dat[dat['EXCHCD']==1]

    # cut nyse by percentiles
    nyse_break = nyse.groupby(["Year", "Month"])["Ranking_Ret"]                    .describe(percentiles=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9])                    .reset_index()

    # get nyse break points
    nyse_break = nyse_break[["Year", "Month", "10%", "20%", "30%", "40%",                                    "50%", "60%", "70%", "80%", "90%"]]

    def nyse_cut(df, nyse_break):

        year = df.Year.min()
        month = df.Month.min()

        # EACH POINT OF TIME get nyse break points
        bk = nyse_break[(nyse_break.Year == year) & (nyse_break.Month == month)]
        bk = np.array(bk).flatten()[2:]
        bk = np.insert(np.append(bk, np.inf), 0, -np.inf)

        # cut all firms into nyse bins
        rank_labels = pd.cut(df.Ranking_Ret, bk, labels = False) + 1

        return rank_labels

    out = dat.groupby(["Year", "Month"]).apply(nyse_cut, nyse_break = (nyse_break))

    # record KRF_decile to new column 
    dat["KRF_decile"] = out.reset_index([0,1]).Ranking_Ret

    return dat


# In[4]:


def PS3_Q3(dat, ff_mkt):

    def decile_weighted_return(df):

        lag_MV = df.lag_Mkt_Cap.sum()

        weights = df.lag_Mkt_Cap/lag_MV

        weighted_ret = weights*df.Ret

        return weighted_ret.sum()


    # calculate return within decile 
    out = dat.groupby(["DM_decile", "Year", "Month"]).apply(decile_weighted_return)
    out = out.reset_index([0,1,2]).rename(columns = {0 : "DM_Ret"})

    out2 = dat.groupby(["KRF_decile", "Year", "Month"]).apply(decile_weighted_return)
    out2 = out2.reset_index([0,1,2]).rename(columns = {0 : "KRF_Ret"})

    # join data frame
    out_df3 = out.join(out2.KRF_Ret) 

    # add risk free column
    out_df3 = pd.merge(out_df3, ff_mkt[["Year", "Month", "RF"]], how='outer')

    out_df3 = out_df3.rename(columns = {'DM_decile' : 'decile'})

    return out_df3


# In[5]:


def PS3_Q4(dat):
    # subset data to 2013.3
    dat = dat.query("Year <= 2013")
    dat = dat[:len(dat)-90]

    # calculate excess return
    dat['DM_exe_ret'] = dat['DM_Ret'] - dat['RF']
    dat['KRF_exe_ret'] = dat['KRF_Ret'] - dat['RF']
    
    # compute summary statistic
    r_rf = dat.groupby(['decile'])['DM_exe_ret'].apply(np.mean)*12
    decile_vol = dat.groupby(['decile'])['DM_Ret'].apply(np.std)*np.sqrt(12)
    decile_sr = r_rf/decile_vol
    decile_skew = dat.groupby(['decile'])['DM_Ret'].apply(lambda x: skew(np.log(1+x)))
    
    ## compute WML portfolio
    # get winner group and loser group
    loser_decile = dat.groupby(['decile']).get_group(1)
    winner_decile = dat.groupby(['decile']).get_group(10)

    WML = np.array(winner_decile.DM_Ret) - np.array(loser_decile.DM_Ret) + np.array(winner_decile.RF)
    WML_r = np.mean(WML - np.array(winner_decile.RF))*12
    WML_vol = WML.std()*np.sqrt(12)
    WML_SR = WML_r/WML_vol
    WML_skew = skew(np.log(1+WML))
    
    r_rf = np.append(np.array(r_rf), WML_r)
    decile_vol = np.append(np.array(decile_vol), WML_vol)
    decile_sr = np.append(np.array(decile_sr), WML_SR)
    decile_skew = np.append(np.array(decile_skew), WML_skew)
    num_array = np.round(np.array([r_rf*100, decile_vol*100, decile_sr, decile_skew]), 2)
    
    # put into data frame
    colnames = ["Decile 1", "Decile 2", "Decile 3", "Decile 4", "Decile 5",                "Decile 6", "Decile 7", "Decile 8", "Decile 9", "Decile 10", "WML"]
    
    out_df4 = pd.DataFrame(num_array, columns= colnames, index=['r-rf', 'vol', 'SR', 'sk(m)'])
    
    return out_df4


# In[9]:


def PS3_Q5(dat, DM_returns, KRF_returns):

    # compute correlation for KRF decile 1 - 10 
    def compute_KRF_corr(df, KRF_returns):
    
        decile_number = int(df.name)

        decile_return = KRF_returns.iloc[:, decile_number - 1]

        corr = np.corrcoef(np.array(decile_return), np.array(df.KRF_Ret))[0, 1]
    
        return corr
    
    KRF_corr = dat.groupby(["decile"]).apply(compute_KRF_corr, KRF_returns)
    
    # compute correlation for WML
    loser_decile = dat.groupby(['decile']).get_group(1)
    winner_decile = dat.groupby(['decile']).get_group(10)
    
    WML_KRF = np.array(winner_decile.KRF_Ret) - np.array(loser_decile.KRF_Ret) + np.array(winner_decile.RF)

    WML_KRF2 = np.array(KRF_returns.iloc[:, 9]) - np.array(KRF_returns.iloc[:, 0]) + np.array(winner_decile.RF)

    WML_KRF_corr = np.corrcoef(WML_KRF, WML_KRF2)[0, 1]

    KRF_corr = np.append(np.array(KRF_corr), WML_KRF_corr)
    
    ### DM
    # subset data to 2016
    dat = dat.query("Year <= 2016")
    
    dat = pd.merge(dat, DM_returns, on = ["Year", "Month", "decile"])
    
    # compute correlation for decile 1 - 10
    def compute_corr(df):
        return df.DM_Ret.corr(df.DM_Ret_2)

    DM_corr = dat.groupby(["decile"]).apply(compute_corr)
    
    # compute correlation for WML
    loser_decile = dat.groupby(['decile']).get_group(1)
    winner_decile = dat.groupby(['decile']).get_group(10)

    WML = np.array(winner_decile.DM_Ret) - np.array(loser_decile.DM_Ret)  + np.array(winner_decile.RF)

    WML2 = np.array(winner_decile.DM_Ret_2) - np.array(loser_decile.DM_Ret_2) + np.array(winner_decile.RF)

    WML_corr = np.corrcoef(WML, WML2)[0, 1]

    DM_corr = np.append(np.array(DM_corr), WML_corr)
    
    
    # put into data frame
    out_df5 = np.vstack([DM_corr, KRF_corr])
    
    colnames = ["Decile 1", "Decile 2", "Decile 3", "Decile 4", "Decile 5",                "Decile 6", "Decile 7", "Decile 8", "Decile 9", "Decile 10", "WML"]

    out_df5 = pd.DataFrame(out_df5, columns = [colnames], index = ["DM correlation", "KRF correlation"]).round(4)
    
    return out_df5



################ Uncommend to Read and Process data ##################################

# # Read Stock Data
# dat = pd.read_csv("CRSP_Stocks.csv")

# # Read ffmkt Data
# ff_mkt = pd.read_csv("F-F_Research_Data_Factors.csv", names = ["date", "Market_minus_Rf", "SMB", "HML", "RF"], skiprows = 1)

# ## Convert data time, push the last day of month to calender last day of month
# ff_mkt["date"] = pd.to_datetime(ff_mkt["date"], format = "%Y%m")

# ff_mkt["date"] = ff_mkt["date"] + pd.offsets.MonthEnd(0)

# ## Extract year and month
# ff_mkt["Year"] = ff_mkt.date.dt.year

# ff_mkt["Month"] = ff_mkt.date.dt.month

# ## Convert Percent to decimal
# ff_mkt[["Market_minus_Rf", "SMB", "HML", "RF"]] = ff_mkt[["Market_minus_Rf", "SMB", "HML", "RF"]]/100

# ff_mkt = ff_mkt.query("Year >= 1927 and Year <=2018")

# # Read DM return Data
# DM_returns = pd.read_table("m_m_pt_tot.txt", header = None, names = ["date", "decile", "DM_Ret_2", "d", "e"], sep='\s+')

# # Convert "date" to datetime
# DM_returns["date"] = pd.to_datetime(DM_returns["date"], format = "%Y%m%d")

# # Add explicit year and month as new columns
# DM_returns.loc[:,"Year"] = DM_returns["date"].dt.year
# DM_returns.loc[:,"Month"] = DM_returns["date"].dt.month

# # drop extra columns
# DM_returns.drop(columns=["date", "d", "e"], inplace = True)

# # Return KRF returns
# KRF_returns = pd.read_csv("10_Portfolios_Prior_12_2.csv", names =             ["date", "Decile 1", "Decile 2", "Decile 3", "Decile 4", "Decile 5",         "Decile 6", "Decile 7","Decile 8", "Decile 9", "Decile 10"], skiprows = 1)

# ## Convert data time, push the last day of month to calender last day of month
# KRF_returns["date"] = pd.to_datetime(KRF_returns["date"], format = "%Y%m")

# ## Extract year and month
# KRF_returns["Year"] = KRF_returns.date.dt.year

# KRF_returns["Month"] = KRF_returns.date.dt.month

# KRF_returns = KRF_returns.query("Year <= 2018")

# KRF_returns.drop(columns = ["date"], inplace = True)

# KRF_returns[["Decile 1", "Decile 2", "Decile 3", "Decile 4", "Decile 5",                "Decile 6", "Decile 7", "Decile 8", "Decile 9", "Decile 10"]] =             KRF_returns[["Decile 1", "Decile 2", "Decile 3", "Decile 4", "Decile 5",                "Decile 6", "Decile 7", "Decile 8", "Decile 9", "Decile 10"]]/100


# # In[11]:


# pd.set_option('mode.chained_assignment', None)

# ################ run functions ##################################
# CRSP_Stocks_Momentum = PS3_Q1(dat)

# CRSP_Stocks_Momentum_decile = PS3_Q2(CRSP_Stocks_Momentum.copy())

# CRSP_Stocks_Momentum_returns = PS3_Q3(CRSP_Stocks_Momentum_decile.copy(), ff_mkt)

# table1 = PS3_Q4(CRSP_Stocks_Momentum_returns.copy())

# table2 = PS3_Q5(CRSP_Stocks_Momentum_returns.copy(), DM_returns, KRF_returns)


# # In[12]:


# table1


# # In[13]:


# table2


# # In[14]:


# ## Empirical Analysis

# dat = CRSP_Stocks_Momentum_returns.copy()

# # compute return from WML
# loser_decile = dat.groupby(['decile']).get_group(1)
# winner_decile = dat.groupby(['decile']).get_group(10)

# WML = np.array(winner_decile.DM_Ret) - np.array(loser_decile.DM_Ret)  + np.array(winner_decile.RF)


# # In[15]:


# date = ff_mkt.date

# winner_return = np.array(winner_decile.DM_Ret)
# loser_return = np.array(loser_decile.DM_Ret)
# rf = np.array(winner_decile.RF)

# fig, ax = plt.subplots(figsize = (14,10))
# ax.plot(date, np.cumsum(np.log(1 + WML)), linestyle = '--', label = "WML", color = "red")
# ax.plot(date, np.cumsum(np.log(1 + winner_return)), label = "Winner", color = "c")
# ax.plot(date, np.cumsum(np.log(1 + loser_return)), label = "Loser")
# ax.plot(date, np.cumsum(np.log(1 + rf)), linestyle = 'dotted', label = "Risk Free", color = "grey")

# ax.set_ylabel("Cumulative Return")
# ax.set_xlabel("Year 1927.01 - 2018.12")
# ax.set_yticklabels(["{:.0e}".format(x) for x in np.exp(plt.yticks()[0]) -1])

# ax.set_title("Cumulative monthly returns 1927 - 2018")

# legend = ax.legend(fontsize = "x-large")


# # In[16]:


# date_sub = date[len(date) - 72 : len(date)]

# WML_sub = WML[len(date) - 72 : len(date)]
# winner_return_sub = np.array(winner_decile.DM_Ret)[len(date) - 72 : len(date)]
# loser_return_sub = np.array(loser_decile.DM_Ret)[len(date) - 72 : len(date)]
# rf_sub = np.array(winner_decile.RF)[len(date) - 72 : len(date)]

# fig, ax = plt.subplots(figsize = (14,10))
# ax.plot(date_sub, np.cumsum(np.log(1 + WML_sub)), linestyle = '--', label = "WML", color = "red")
# ax.plot(date_sub, np.cumsum(np.log(1 + winner_return_sub)), label = "Winner", color = "c", alpha = 0.5)
# ax.plot(date_sub, np.cumsum(np.log(1 + loser_return_sub)), label = "Loser", alpha = 0.5)
# ax.plot(date_sub, np.cumsum(np.log(1 + rf_sub)), linestyle = 'dotted', label = "Risk Free", color = "grey")

# ax.set_ylabel("Cumulative Return")
# ax.set_xlabel("Year 2013.1 - 2018.12")
# ax.set_yticklabels(["{:.2f}".format(x) for x in np.exp(plt.yticks()[0])-1])

# ax.set_title("Cumulative monthly returns 2013 - 2018")

# legend = ax.legend(fontsize = "large")

