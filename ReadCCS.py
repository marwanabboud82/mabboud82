# -*- coding: utf-8 -*-
"""
@author: mabboud
"""
import pandas as pd

def ReadCCS (InputPath):

    # DM STD Tracker File
    CCSPath = InputPath

    CCS_Data = pd.read_csv(CCSPath,sep=',',skiprows=15, header=None) # read securities
    
    CCS_Data.drop(CCS_Data.columns[0],axis=1,inplace=True)
    CCS_Data.drop(CCS_Data.tail(5).index,inplace=True)
    
    colspecs=[(0, 3), (3, 5), (5, 39)]
    CCS_Data_Cols = pd.read_fwf(CCSPath,colspecs,nrows=8,skiprows=3) # read column names  
    CCS_Data.columns = CCS_Data_Cols.iloc[0:8,2]
    
    CCS_Data= CCS_Data.applymap(lambda x: x.strip() if isinstance(x, str) else x) #remove trailing leading spaces
    del colspecs, CCS_Data_Cols, CCSPath


    return  CCS_Data