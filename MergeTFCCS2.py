# -*- coding: utf-8 -*-
"""
@author: mabboud
"""
import pandas as pd
import numpy as np

def MergeTFCCS2 (Clean_TrackerData,CCS_Data,CntryMap):


    # Add Barra ID if exists to TF   
    Temp_Clean_TrackerData = Clean_TrackerData.merge(CCS_Data[['MSCI Security Code','Barra Id']],how='left',left_on='msci_security_code',right_on='MSCI Security Code')
    Temp_Clean_TrackerData.drop(['MSCI Security Code'], inplace=True, axis=1)
    Temp_Clean_TrackerData.rename(columns={'Barra Id': 'Barra_Id'},inplace=True)
    Temp_Clean_TrackerData['InTF']=1
    
    
    # Fill the columns that exist in CCS

    
    AllData = Temp_Clean_TrackerData
    AllData = AllData.merge(CntryMap[['ISO_Country','Market','Region']],how='left',left_on='ISO_country_symbol_next_day',right_on='ISO_Country')
    AllData.drop(['ISO_Country'], inplace=True, axis=1)
    
    AllData[['initial_mkt_cap_usd_next_day','foreign_inc_factor_next_day']]=AllData[['initial_mkt_cap_usd_next_day','foreign_inc_factor_next_day']].astype(float)
    AllData['FF_MktCap_usd']= AllData['initial_mkt_cap_usd_next_day'] * AllData['foreign_inc_factor_next_day']
    
    
    return  AllData