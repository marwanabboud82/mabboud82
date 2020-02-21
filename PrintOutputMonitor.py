# -*- coding: utf-8 -*-
"""
@author: mabboud
"""

import pandas as pd
import xlsxwriter
import os

def  PrintOutputMonitor(DMEMInterim,OutputFinalMonitor,OutputUpdateSegmNbComp,OutputFinalInvestability,CalcDate):

    dirpath = os.getcwd()
    OutputPath = dirpath +'\\Output\\' + CalcDate[6:10]+CalcDate[3:5]+CalcDate[0:2]
    
    try:
        os.mkdir(OutputPath)
        print("Directory " , OutputPath ,  " Created ") 
    except FileExistsError:
        print("Directory " , OutputPath ,  " already exists")
    
   
    MarketCutoffs = DMEMInterim
    
    
    for mkt in OutputUpdateSegmNbComp.keys():
        MarketCutoffs.loc[mkt,'InterimMSnbC']=OutputUpdateSegmNbComp[mkt]['InterimData'].loc['InterimMSnbC',1]
        MarketCutoffs.loc[mkt,'InterimMSSC']=OutputUpdateSegmNbComp[mkt]['InterimData'].loc['InterimMSSC',1]
        MarketCutoffs.loc[mkt,'InterimPctCoverage']=OutputUpdateSegmNbComp[mkt]['InterimData'].loc['InterimPctCoverage',1]
        MarketCutoffs.loc[mkt,'InitialMSnbC']=OutputUpdateSegmNbComp[mkt]['InitialData'].loc['InitialMSnbC',1]
        MarketCutoffs.loc[mkt,'InitialMSSC']=OutputUpdateSegmNbComp[mkt]['InitialData'].loc['InitialMSSC',1]/1e+6
        MarketCutoffs.loc[mkt,'InitialPctCoverage']=OutputUpdateSegmNbComp[mkt]['InitialData'].loc['InitialPctCoverage',1]
        MarketCutoffs.loc[mkt,'RevisedMSnbC']=OutputUpdateSegmNbComp[mkt]['RevisedData'].loc['RevisedMSnbC',1]
        MarketCutoffs.loc[mkt,'RevisedMSSC']=OutputUpdateSegmNbComp[mkt]['RevisedData'].loc['RevisedMSSC',1]/1e+6
        MarketCutoffs.loc[mkt,'RevisedPctCoverage']=OutputUpdateSegmNbComp[mkt]['RevisedData'].loc['RevisedPctCoverage',1]
        MarketCutoffs.loc[mkt,'FinalMSnbC']=OutputUpdateSegmNbComp[mkt]['FinalData'].loc['FinalMSnbC',1]
        MarketCutoffs.loc[mkt,'FinalMSSC']=OutputUpdateSegmNbComp[mkt]['FinalData'].loc['FinalMSSC',1]/1e+6
        MarketCutoffs.loc[mkt,'FinalPctCoverage']=OutputUpdateSegmNbComp[mkt]['FinalData'].loc['FinalPctCoverage',1]
        
        MarketCutoffs.loc[mkt,'blnSizeCheck']=OutputUpdateSegmNbComp[mkt]['AuditCheckData'].loc['blnSizeCheck',1].bool()
        MarketCutoffs.loc[mkt,'blnCoverageCheck']=OutputUpdateSegmNbComp[mkt]['AuditCheckData'].loc['blnCoverageCheck',1].bool()
        MarketCutoffs.loc[mkt,'blnSizeCoverageCheck']=OutputUpdateSegmNbComp[mkt]['AuditCheckData'].loc['blnSizeCoverageCheck',1].bool()
        MarketCutoffs.loc[mkt,'blnIncreaseRequired']=OutputUpdateSegmNbComp[mkt]['AuditCheckData'].loc['blnIncreaseRequired',1]
        MarketCutoffs.loc[mkt,'blnDecreaseRequired']=OutputUpdateSegmNbComp[mkt]['AuditCheckData'].loc['blnDecreaseRequired',1]
        
        
    
    
    DMGMSLimits = OutputUpdateSegmNbComp['USA']['GMSLimits']
    EMGMSLimits = OutputUpdateSegmNbComp['SAUDI ARABIA']['GMSLimits']
    
    # MarketCutoff
    MarketCutoffWriter = pd.ExcelWriter(OutputPath + '\\DMEM_MarketCutoff_' + CalcDate[6:10]+CalcDate[3:5]+CalcDate[0:2] + '.xlsx', engine='xlsxwriter')
    MarketCutoffs.to_excel(MarketCutoffWriter, sheet_name='DMEM_MarketCutoff')
    DMGMSLimits.to_excel(MarketCutoffWriter, sheet_name='DMGMSLimits')
    EMGMSLimits.to_excel(MarketCutoffWriter, sheet_name='EMGMSLimits')
    MarketCutoffWriter.save()
    

    # Universe
    for mkt in OutputFinalInvestability.keys():
        print(mkt)
        Market_SecurityData =  OutputFinalInvestability[mkt]['Market_SecurityData']
        Market_CompanyData =  OutputFinalInvestability[mkt]['Market_CompanyData']
        MktMonitor = OutputFinalMonitor.loc[OutputFinalMonitor['Market']==mkt]
        
        Security_CompanyDetails = pd.ExcelWriter(OutputPath + '\\' + mkt + '_' + CalcDate[6:10]+CalcDate[3:5]+CalcDate[0:2] + '.xlsx', engine='xlsxwriter')
        Market_SecurityData.to_excel(Security_CompanyDetails, sheet_name='Security')
        Market_CompanyData.to_excel(Security_CompanyDetails, sheet_name='Company')
        MktMonitor.to_excel(Security_CompanyDetails, sheet_name='Monitor')

        Security_CompanyDetails.save()
        
    # MarketCutoff
    AllMonitorWriter = pd.ExcelWriter(OutputPath + '\\DMEM_OutputMonitor_' + CalcDate[6:10]+CalcDate[3:5]+CalcDate[0:2] + '.xlsx', engine='xlsxwriter')
    OutputFinalMonitor.to_excel(AllMonitorWriter, sheet_name='DMEM_OutputMonitor_')
    AllMonitorWriter.save()    
    

    
    PrintOutput = 'DONE'
    
    return PrintOutput