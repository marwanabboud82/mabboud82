# -*- coding: utf-8 -*-
"""
@author: mabboud
"""
import pandas as pd

def ReadTrackerFiles (CalcDate,TrackerFilePath):

    # DM STD Tracker File
    DMSTDPath = (TrackerFilePath.loc['DMSTDPath'] + CalcDate[0:2] + CalcDate[3:5] + 
    'd_tdrif\\' + CalcDate[0:2] + CalcDate[3:5] + 'd15d.txt')

    Tracker_DMSTD = pd.read_csv(DMSTDPath[0],sep='|',skiprows=95, header=None) # read securities
    Tracker_DMSTD.drop(Tracker_DMSTD.columns[0],axis=1,inplace=True)
    Tracker_DMSTD.drop(Tracker_DMSTD.tail(2).index,inplace=True)
    colspecs=[(0, 2), (2, 5),(5,39),(39,70),(70,72),(72,75),(75,79)]
    Tracker_DMSTD_Cols = pd.read_fwf(DMSTDPath[0],colspecs,nrows=91,skiprows=1) # read column names
    Tracker_DMSTD.columns = Tracker_DMSTD_Cols.iloc[0:90,3]
    Tracker_DMSTD= Tracker_DMSTD.applymap(lambda x: x.strip() if isinstance(x, str) else x) #remove trailing leading spaces
    del colspecs, Tracker_DMSTD_Cols, DMSTDPath

    # DM SML Tracker File
    DMSMLPath = (TrackerFilePath.loc['DMSMLPath'] + CalcDate[0:2] + CalcDate[3:5] + 
    'dstdrif\\' + CalcDate[0:2] + CalcDate[3:5] + 'd17d.txt')
    
    Tracker_DMSML = pd.read_csv(DMSMLPath[0],sep='|',skiprows=95, header=None)
    Tracker_DMSML.drop(Tracker_DMSML.columns[0],axis=1,inplace=True)
    Tracker_DMSML.drop(Tracker_DMSML.tail(2).index,inplace=True)
    colspecs=[(0, 2), (2, 5),(5,39),(39,70),(70,72),(72,75),(75,79)]
    Tracker_DMSML_Cols = pd.read_fwf(DMSMLPath[0],colspecs,nrows=91,skiprows=1)
    Tracker_DMSML.columns = Tracker_DMSML_Cols.iloc[0:90,3]
    Tracker_DMSML= Tracker_DMSML.applymap(lambda x: x.strip() if isinstance(x, str) else x) #remove trailing leading spaces
    del colspecs, Tracker_DMSML_Cols,DMSMLPath


    return  Tracker_DMSTD,Tracker_DMSML