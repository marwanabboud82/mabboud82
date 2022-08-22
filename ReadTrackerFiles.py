# -*- coding: utf-8 -*-
"""
@author: mabboud
"""
import pandas as pd
import os
import shutil



def ReadTrackerFiles(CalcDate, TrackerFilePath):
    # DM STD Tracker File
    dmstdzip = (TrackerFilePath.loc['DMSTDPath'] + CalcDate[0:2] + CalcDate[3:5] + 'd_tdrif.zip')
    TmpFile = UnZipFile(dmstdzip[0])    
    DMSTDPath = (TmpFile + '\\'+CalcDate[0:2] + CalcDate[3:5] + 'D15D.RIF')
    Tracker_DMSTD = ReadFile(DMSTDPath)
    
    # DM SML Tracker File
    dmsmlzip = (TrackerFilePath.loc['DMSMLPath'] + CalcDate[0:2] + CalcDate[3:5] + 'dstdrif.zip')
    TmpFile = UnZipFile(dmsmlzip[0])    
    DMSMLPath = (TmpFile + '\\'+CalcDate[0:2] + CalcDate[3:5] + 'D17D.RIF')
    Tracker_DMSML = ReadFile(DMSMLPath)
    
    # EM STD Tracker File
    emstdzip = (TrackerFilePath.loc['EMSTDPath'] + CalcDate[0:2] + CalcDate[3:5] + 'd_terif.zip')
    TmpFile = UnZipFile(emstdzip[0])    
    EMSTDPath = (TmpFile + '\\'+CalcDate[0:2] + CalcDate[3:5] + 'D15E.RIF')
    Tracker_EMSTD = ReadFile(EMSTDPath)
    
    # EM SML Tracker File
    emsmlzip = (TrackerFilePath.loc['EMSMLPath'] + CalcDate[0:2] + CalcDate[3:5] + 'dsterif.zip')
    TmpFile = UnZipFile(emsmlzip[0])    
    EMSMLPath = (TmpFile + '\\'+CalcDate[0:2] + CalcDate[3:5] + 'D17E.RIF')
    Tracker_EMSML = ReadFile(EMSMLPath)
        
    return Tracker_DMSTD, Tracker_DMSML,Tracker_EMSTD, Tracker_EMSML

def ReadFile(filename):
    nrowheader=95
    headercolspecs = [(0, 2), (2, 5), (5, 39), (39, 70), (70, 72), (72, 75), (75, 79)]
    df = pd.read_csv(filename, sep='|', skiprows=nrowheader, header=None,low_memory=False)
    df = df.iloc[:-2] #* and #EOD
    cols = pd.read_fwf(filename,headercolspecs, nrows=nrowheader-4, skiprows=1)
    cols.columns=['#','idx','nicename','name','type','length','length2']
    renamedict = cols.set_index('idx')['name'].to_dict()
    df=df.rename(columns=renamedict)
    df= df.applymap(lambda x: x.strip() if isinstance(x, str) else x) #remove trailing leading spaces
    floatcols=cols[cols['type']=='N']['name'].tolist()
    for colname in floatcols:
        df[colname]=pd.to_numeric(df[colname],errors='coerce')
    stringcols = cols[cols['type'] == 'S']['name'].tolist()
    for colname in stringcols:
        df[colname]=df[colname].apply(lambda x: x.strip() if isinstance(x, str) else x)
    df.drop(df.columns[0], axis=1, inplace=True)
    return df

def get_fullname(x): 
    return os.path.join('C:\\Users\\mabboud\\PycharmProjects\\Python\\MSCI\\Input\\',x)

def UnZipFile(fname):
    basefname=os.path.basename(fname)
    targetdir = get_fullname('tmp')
    try:
        os.mkdir(targetdir)
    except Exception as e:
        pass
    target=os.path.join(targetdir,basefname)
    try:
        shutil.copy(fname, target)
    except Exception as e:
        pass    
    shutil.unpack_archive(target, extract_dir=target.replace('.zip', ''), format='zip')
    return target.replace('.zip', '')


   