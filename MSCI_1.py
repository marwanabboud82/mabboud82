"""
@author: mabboud
"""

# libraries
import pandas as pd
import numpy as np
from KDBData.bbo import BBODownload


"""
#####################
# BBO
#####################
"""
sDate = '2022-08-18'

bbo = BBODownload(sDate = sDate, eDate = sDate, region='world').get()

InputPath = 'C:\\Users\\mabboud\\PycharmProjects\\Python\\MSCI\\input\\'
CalcDate='08/18/2022'
IntCalcDate = '08/19/2022'

"""
#####################
# READ TRACKER FILES
#####################
"""

#DMSTDPath = 'C:\\Users\\mabboud\\PycharmProjects\\Python\\MSCI\\input\\'
#EMSTDPath = 'C:\\Users\\mabboud\\PycharmProjects\\Python\\MSCI\\input\\'
#DMSMLPath = 'C:\\Users\\mabboud\\PycharmProjects\\Python\\MSCI\\input\\'
#EMSMLPath = 'C:\\Users\\mabboud\\PycharmProjects\\Python\\MSCI\\input\\'

DMSTDPath = 'P:\\daily\\2022\\08\\'
EMSTDPath = 'S:\\daily\\2022\\08\\'
DMSMLPath = 'Q:\\daily\\2022\\08\\'
EMSMLPath = 'T:\\daily\\2022\\08\\'

TrackerFilePathList = [DMSTDPath,EMSTDPath,DMSMLPath,EMSMLPath]

TrackerFilePath = pd.DataFrame(TrackerFilePathList) # Create DataFrame

del DMSTDPath,EMSTDPath,DMSMLPath,EMSMLPath,TrackerFilePathList

TrackerFilePath.index = ['DMSTDPath','EMSTDPath','DMSMLPath','EMSMLPath'] # change row names

"""
#####################
# READ TRACKER FILES
#####################
"""
from ReadTrackerFiles import ReadTrackerFiles
Tracker_DMSTD, Tracker_DMSML,Tracker_EMSTD, Tracker_EMSML = ReadTrackerFiles (CalcDate,TrackerFilePath)

#from mmsci_rules import MSCIRules 
#CalcDate = pd.to_datetime('2020-09-09')
#m = MSCIRules(asofdate=CalcDate)
#m.dmcoreonly=False
#m.dmonly=False
#Tracker_DMSTD_2, Tracker_DMSML_2,Tracker_EMSTD_2, Tracker_EMSML_2  =m.read()

"""
#####################
# CLEAN TRACKER FILES
#####################
"""
from CleanTrackerFiles import CleanTrackerFiles
Clean_Tracker_DMSTD,Clean_Tracker_DMSML,Clean_Tracker_EMSTD,Clean_Tracker_EMSML = CleanTrackerFiles (Tracker_DMSTD, Tracker_DMSML,Tracker_EMSTD, Tracker_EMSML)
Clean_TrackerData = [Clean_Tracker_DMSTD,Clean_Tracker_DMSML,Clean_Tracker_EMSTD,Clean_Tracker_EMSML]
Clean_TrackerData = pd.concat(Clean_TrackerData,axis=0,sort=False)
del Clean_Tracker_DMSTD,Clean_Tracker_DMSML,Clean_Tracker_EMSTD,Clean_Tracker_EMSML

"""
#####################
# GET IPOs & Merge
#####################
"""
from GetIPOs import GetIPOs
InceptionDate = '2021-06-01'
LastInceptionDate = '2022-05-31'
IPOsDF = GetIPOs(bbo,InceptionDate,LastInceptionDate)

from MergeIPOs import MergeIPOs
Clean_TrackerData = MergeIPOs(Clean_TrackerData,IPOsDF)


"""
#####################
# READ CCS
#####################
"""
from ReadCCS import ReadCCS
InputPathCCS= (InputPath + '0606d_36.59_')
CCS_Data= ReadCCS (InputPathCCS)


"""
#####################
# MERGE CCS                 TO BE CHANGED AFTER RECEIVING FILE
#####################
"""
from MergeTFCCS import MergeTFCCS
from CntryMappings import GetCntryMap
CntryMap=GetCntryMap()
#All_SecurityData= MergeTFCCS (Clean_TrackerData,CCS_Data,CntryMap)

All_SecurityData= MergeTFCCS (bbo,TrackerFilePath,Clean_TrackerData,CCS_Data,CntryMap,sDate)
ColumnsToUse = All_SecurityData.columns

from mmsci_rules import MSCIRules 
CalcDate2 = pd.to_datetime(sDate)
m = MSCIRules(asofdate=CalcDate2)
m.dmcoreonly=False
m.dmonly=False
Tracker_DMSTD_2, Tracker_DMSML_2,Tracker_EMSTD_2, Tracker_EMSML_2  =m.read()
sachadf_ = pd.concat([Tracker_DMSTD_2, Tracker_DMSML_2,Tracker_EMSTD_2, Tracker_EMSML_2],axis=0,sort=False)
sachadf=sachadf_[['msci_security_code','sedol']]
sachadf['msci_security_code']=sachadf['msci_security_code'].astype(int).astype(str)
All_SecurityData=All_SecurityData.merge(sachadf,on='msci_security_code',how='left')
All_SecurityData['sedol']=All_SecurityData['sedol'].fillna(All_SecurityData['sedol_next_day'])
All_SecurityData['sedol_next_day']=All_SecurityData['sedol']
All_SecurityData = All_SecurityData.drop('sedol',axis=1)

"""
#####################
# Add ATVR/FoTR
#####################
"""
tmpfilename= '20191101_20191129'
ATVRDMSTDPath = 'P:\\history\\2019\\' + tmpfilename 
ATVREMSTDPath = 'S:\\history\\2019\\'+ tmpfilename 
ATVRDMSMLPath = 'Q:\\history\\2019\\'+ tmpfilename 
ATVREMSMLPath = 'T:\\history\\2019\\'+ tmpfilename 


ATVRTrackerFilePathList = [ATVRDMSTDPath,ATVREMSTDPath,ATVRDMSMLPath,ATVREMSMLPath]
ATVRTrackerFilePath = pd.DataFrame(ATVRTrackerFilePathList) # Create DataFrame
del ATVRDMSTDPath,ATVREMSTDPath,ATVRDMSMLPath,ATVREMSMLPath,ATVRTrackerFilePathList
ATVRTrackerFilePath.index = ['ATVRDMSTDPath','ATVREMSTDPath','ATVRDMSMLPath','ATVREMSMLPath'] # change row names

#from GetATVRData import GetATVRData
#All_SecurityData= GetATVRData(All_SecurityData, CalcDate, ATVRTrackerFilePath,tmpfilename)
from IndexRules.mmsci_rules import MSCIRules
m = MSCIRules(todaydate=pd.to_datetime(CalcDate))
All_SecurityData = m.get_atvr_data(All_SecurityData)

NewColumnsToUse=ColumnsToUse.append(All_SecurityData.columns[1:6])
All_SecurityData=All_SecurityData.reindex(columns=NewColumnsToUse)

#import IndexRules.mmsci_rules as ss
#import importlib
#importlib.reload(ss)
#m = ss.MSCIRules(todaydate=pd.to_datetime(CalcDate))
"""
#####################
# Manual Override
#####################
"""
from ManualOverride import ManualOverride
All_SecurityData= ManualOverride(All_SecurityData, CalcDate, InputPath)

"""
#####################
# GET Company Universe
#####################
"""
from GetCompanyUniverse import GetCompanyUniverse
AllData_TEST = All_SecurityData[(~All_SecurityData['price'].isnull()) & ~(All_SecurityData['ISO_country_symbol_next_day']=='')]
All_CompanyData= GetCompanyUniverse (AllData_TEST)

"""
#####################
# Read Interim
#####################
"""
# MAKE SURE INTERIM IS NOT IN m$
######################################
from ReadInterim import ReadInterim
DMEMInterim,PublishedGMS,All_SecurityData,All_CompanyData = ReadInterim(TrackerFilePath,IntCalcDate,All_SecurityData,All_CompanyData)


"""
#####################
# Basic Investability
#####################
"""
MSRRankPrevSAIR = 6945
from UpdateInvestableEquityUniverse import UpdateInvestableEquityUniverse
NewGIEUMSR, All_SecurityData, All_CompanyData= UpdateInvestableEquityUniverse(All_SecurityData,All_CompanyData,MSRRankPrevSAIR)

GIEU_SecurityData = All_SecurityData.loc[All_SecurityData['All_BasicTest']==1]
GIEU_CompanyData = All_CompanyData.loc[All_CompanyData['All_BasicTest']==1]

"""
#####################
# Update GMS
#####################
"""
GMSRankPrevSAIR = 1497
from UpdateGMS import UpdateGMS
NewGIEUGMS = UpdateGMS(GIEU_CompanyData,GMSRankPrevSAIR)

"""
#####################
# Update the Market Segment Parameters
#####################
"""
from UpdateMarketSegmentParams import UpdateMarketSegmentParams
OutputUpdateSegmNbComp = UpdateMarketSegmentParams(DMEMInterim,PublishedGMS,GIEU_CompanyData)


"""
#####################
# Size Segment Assignment
#####################
"""
from SizeSegmentAssignment import SizeSegmentAssignment
OutputSizeSegmentAssignment = SizeSegmentAssignment(OutputUpdateSegmNbComp,All_CompanyData,All_SecurityData)


"""
#####################
# Final Investability
#####################
"""
from AssessFinalInvestability import AssessFinalInvestability
OutputFinalInvestability = AssessFinalInvestability(OutputUpdateSegmNbComp,OutputSizeSegmentAssignment)

OutputFinalInvestability['AUSTRALIA']['Market_SecurityData']

"""
#####################
# Generate Output Monitor
#####################
"""
from GenerateForecastMonitor import GenerateForecastMonitor
OutputFinalMonitor = GenerateForecastMonitor(OutputUpdateSegmNbComp,OutputFinalInvestability,CalcDate2)

""" #################################
# Add Book positions
#################################
""" 
from CommonLib.npositions import read_pos
import pandas as pd

posdf=read_pos(book='All')
posdf=posdf[posdf['Qty'].abs()>0]
posdf_ = posdf[['Sedol','Notional']]\
    .rename(columns={'Sedol':'sedol_next_day','Notional':'bookPos'})
posdf_ = posdf_.drop_duplicates(subset=['sedol_next_day']) # just in case
OutputFinalMonitor = OutputFinalMonitor.merge(posdf_,on='sedol_next_day',how='left')


"""
#####################
# Print CSV Output
#####################
"""
from PrintOutputMonitor import PrintOutputMonitor
PrintOutput = PrintOutputMonitor(DMEMInterim,OutputFinalMonitor,OutputUpdateSegmNbComp,OutputFinalInvestability,CalcDate)
