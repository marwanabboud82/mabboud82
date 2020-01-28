"""
@author: mabboud
"""

# libraries
import pandas as pd
import numpy as np

"""
#####################
# READ TRACKER FILES
#####################
"""


DMSTDPath = 'C:\\Users\\mabboud\\PycharmProjects\\Python\\MSCI\\input\\'
EMSTDPath = 'C:\\Users\\mabboud\\PycharmProjects\\Python\\MSCI\\input\\'
DMSMLPath = 'C:\\Users\\mabboud\\PycharmProjects\\Python\\MSCI\\input\\'
EMSMLPath = 'C:\\Users\\mabboud\\PycharmProjects\\Python\\MSCI\\input\\'

CalcDate='01/14/2020'
IntCalcDate = '01/15/2020'

TrackerFilePathList = [DMSTDPath,DMSTDPath,DMSTDPath,DMSTDPath]

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

"""
#####################
# CLEAN TRACKER FILES
#####################
"""
from CleanTrackerFiles import CleanTrackerFiles
Clean_Tracker_DMSTD,Clean_Tracker_DMSML,Clean_Tracker_EMSTD,Clean_Tracker_EMSML = CleanTrackerFiles (Tracker_DMSTD, Tracker_DMSML,Tracker_EMSTD, Tracker_EMSML)
Clean_TrackerData = [Clean_Tracker_DMSTD,Clean_Tracker_DMSML,Clean_Tracker_EMSTD,Clean_Tracker_EMSML]
Clean_TrackerData = pd.concat(Clean_TrackerData,axis=0,sort=False)


"""
#####################
# READ CCS
#####################
"""
from ReadCCS import ReadCCS
CCS_Data= ReadCCS (TrackerFilePath)


"""
#####################
# MERGE CCS                 TO BE CHANGED AFTER RECEIVING FILE
#####################
"""
from MergeTFCCS2 import MergeTFCCS2
from CntryMappings import GetCntryMap
CntryMap=GetCntryMap()
#All_SecurityData= MergeTFCCS (Clean_TrackerData,CCS_Data,CntryMap)

All_SecurityData= MergeTFCCS2 (Clean_TrackerData,CCS_Data,CntryMap)
xxx = CCS_Data.columns

"""
#####################
# Add ATVR/FoTR
#####################
"""
from GetATVRData import GetATVRData
All_SecurityData= GetATVRData (All_SecurityData)

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
OutputUpdateSegmNbComp = UpdateMarketSegmentParams(DMEMInterim,NewGIEUGMS,GIEU_CompanyData)


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



