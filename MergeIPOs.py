# -*- coding: utf-8 -*-
"""
@author: mabboud
"""
import pandas as pd
import numpy as np

def MergeIPOs(Clean_TrackerData,IPOsDF):

    NotNewList = IPOsDF['isin'].isin(Clean_TrackerData['isin']).astype(bool)
    IPOsDF=IPOsDF[~NotNewList]  
    
    
    df_prime = pd.concat([IPOsDF, Clean_TrackerData], axis=0, sort=False).reset_index(drop = True) 
    Clean_TrackerData_cols=Clean_TrackerData.columns
    df_prime = df_prime.loc[:,Clean_TrackerData_cols]
    
    InIPOsFlag = df_prime['sedol_next_day'].isin(IPOsDF['sedol_next_day']).astype(bool)
    DMCountries = Clean_TrackerData.loc[Clean_TrackerData['DM_universe_flag']==1,'ISO_country_symbol_next_day'].unique()
    EMCountries = Clean_TrackerData.loc[Clean_TrackerData['EM_universe_flag']==1,'ISO_country_symbol_next_day'].unique()
    FMCountries = Clean_TrackerData.loc[Clean_TrackerData['fm_universe_flag']==1,'ISO_country_symbol_next_day'].unique()
    GCCCountries = Clean_TrackerData.loc[Clean_TrackerData['gcc_universe_flag']==1,'ISO_country_symbol_next_day'].unique()
    
    
        
    df_prime.loc[InIPOsFlag,'calc_date']= df_prime['calc_date'].unique()[1]
    df_prime.loc[InIPOsFlag,'msci_timeseries_code']=df_prime.loc[InIPOsFlag,'sedol_next_day']
    df_prime.loc[InIPOsFlag,'msci_issuer_code']=df_prime.loc[InIPOsFlag,'sedol_next_day']
    df_prime.loc[InIPOsFlag,'msci_security_code']=df_prime.loc[InIPOsFlag,'sedol_next_day']
    df_prime.loc[InIPOsFlag,'DM_universe_flag']= df_prime.loc[InIPOsFlag,'ISO_country_symbol_next_day'].isin(DMCountries).astype(int)
    df_prime.loc[InIPOsFlag,'EM_universe_flag']= df_prime.loc[InIPOsFlag,'ISO_country_symbol_next_day'].isin(EMCountries).astype(int)
    df_prime.loc[InIPOsFlag,'non_local_listing_flag']= 0
    df_prime.loc[InIPOsFlag,'domestic_inc_factor_next_day']= df_prime.loc[InIPOsFlag,'foreign_inc_factor_next_day']
    df_prime.loc[InIPOsFlag,'limited_investability_factor']=1
    df_prime.loc[InIPOsFlag,'foreign_ownership_limit']=1
    df_prime.loc[InIPOsFlag,'unadj_market_cap_today_usdol']=df_prime.loc[InIPOsFlag,'initial_mkt_cap_usd_next_day']
    df_prime.loc[InIPOsFlag,'adj_market_cap_usdol']=df_prime.loc[InIPOsFlag,'initial_mkt_cap_usd_next_day']
    df_prime.loc[InIPOsFlag,'fm_universe_flag']=df_prime.loc[InIPOsFlag,'ISO_country_symbol_next_day'].isin(FMCountries).astype(int)
    df_prime.loc[InIPOsFlag,'gcc_universe_flag']=df_prime.loc[InIPOsFlag,'ISO_country_symbol_next_day'].isin(GCCCountries).astype(int)
    df_prime.loc[InIPOsFlag,'RIC']=df_prime.loc[InIPOsFlag,'sedol_next_day']
    df_prime.loc[InIPOsFlag,'share_type']=''
    df_prime.loc[InIPOsFlag,'alternate_listing']=''
    df_prime.loc[InIPOsFlag,'Status']='NEW'
    df_prime.loc[InIPOsFlag,'Size_Cap']='NEW'
    df_prime.loc[InIPOsFlag,'Dom_Flag']=0
    df_prime.loc[InIPOsFlag,'Pro_Status']='IPO'
    df_prime.loc[InIPOsFlag,'IIF']=1
    df_prime.loc[InIPOsFlag,'InTF']=0
    
    Clean_TrackerData = df_prime
    
    return Clean_TrackerData
    

    
    
    
    
    
    
    
    
    
    