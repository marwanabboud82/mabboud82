# -*- coding: utf-8 -*-
"""
@author: mabboud
"""
import pandas as pd
import numpy as np

def MergeTFCCS_old (Clean_TrackerData,CCS_Data,CntryMap):


    # Add Barra ID if exists to TF   
    Temp_Clean_TrackerData = Clean_TrackerData.merge(CCS_Data[['MSCI Security Code','Barra Id']],how='left',left_on='msci_security_code',right_on='MSCI Security Code')
    Temp_Clean_TrackerData.drop(['MSCI Security Code'], inplace=True, axis=1)
    Temp_Clean_TrackerData.rename(columns={'Barra Id': 'Barra_Id'},inplace=True)
    Temp_Clean_TrackerData['InTF']=1
    
    
    # Fill the columns that exist in CCS
    TempDF= pd.DataFrame(columns=Temp_Clean_TrackerData.columns, index=CCS_Data.index)
    TempDF['calc_date']=CCS_Data['Calculation Date']
    TempDF['security_name']=CCS_Data['Security Name']
    TempDF['msci_security_code']=CCS_Data['MSCI Security Code']
    TempDF['sedol']=CCS_Data['Sedol code']
    TempDF['isin']=CCS_Data['Isin']
    TempDF['bb_ticker']=CCS_Data['Bloomberg code']
    TempDF['ISO_country_symbol_next_day']=CCS_Data['ISO Country Symbol']
    TempDF['Barra_Id']= CCS_Data['Barra Id']
    TempDF['InTF']= 0
    
    InCCSNotInTF = ~ TempDF['msci_security_code'].isin(Temp_Clean_TrackerData['msci_security_code'])
    # InTFNotInCCS = Temp_Clean_TrackerData['msci_security_code'].isin(TempDF['msci_security_code'])
    
    
    AllData = Temp_Clean_TrackerData.append(TempDF[InCCSNotInTF])


    AllData = AllData.merge(CntryMap[['ISO_Country','Market','Region']],how='left',left_on='ISO_country_symbol_next_day',right_on='ISO_Country')
    AllData.drop(['ISO_Country'], inplace=True, axis=1)

    #AllData = AllData.replace('nan', np.nan)
    #AllData['initial_mkt_cap_usd_next_day'] = pd.to_numeric(AllData['initial_mkt_cap_usd_next_day'])
    #AllData['foreign_inc_factor_next_day'] = pd.to_numeric(AllData['foreign_inc_factor_next_day'])

  #  AllData['initial_mkt_cap_usd_next_day'][1685:1690] * AllData['foreign_inc_factor_next_day'][1685:1690]




    AllData[['initial_mkt_cap_usd_next_day','foreign_inc_factor_next_day']]=AllData[['initial_mkt_cap_usd_next_day','foreign_inc_factor_next_day']].astype(float)
    AllData['FF_MktCap_usd']= AllData['initial_mkt_cap_usd_next_day'] * AllData['foreign_inc_factor_next_day']
    
    
    return  AllData