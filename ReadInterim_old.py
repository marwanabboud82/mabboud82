# -*- coding: utf-8 -*-
"""
@author: mabboud
"""
import pandas as pd

def ReadInterim2 (TrackerFilePath,All_SecurityData,All_CompanyData):

    # DM Interim File
    InterimPath = (TrackerFilePath.loc['DMSTDPath'] + 'Interim_File_draft.csv')

    Interim_Data = pd.read_csv(InterimPath[0],sep=',') # read interim
    
    All_SecurityData['Interim_MSSC']=All_SecurityData['company_full_mktcap']*0 
    for mkt in range(0,len(Interim_Data['Market'])):
        All_SecurityData.loc[All_SecurityData['Market']==Interim_Data.iloc[mkt,0],'Interim_MSSC']= Interim_Data.iloc[mkt,2]
    
    All_CompanyData['Interim_MSSC']=All_CompanyData['company_full_mktcap']*0  
    for mkt in range(0,len(Interim_Data['Market'])):
        All_CompanyData.loc[All_CompanyData['Market']==Interim_Data.iloc[mkt,0],'Interim_MSSC']= Interim_Data.iloc[mkt,2]
       
    return  Interim_Data,All_SecurityData,All_CompanyData