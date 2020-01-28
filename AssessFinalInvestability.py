# -*- coding: utf-8 -*-
"""
@author: mabboud
"""
import pandas as pd
import numpy as np

def AssessFinalInvestability(OutputUpdateSegmNbComp,OutputSizeSegmentAssignment):
    
    
    OutputFinalInvestability= OutputSizeSegmentAssignment 
    
    for mkt in OutputFinalInvestability.keys():
    
        Market_SecurityData =  OutputFinalInvestability[mkt]['Market_SecurityData']
        SAIR_MSSC = OutputUpdateSegmNbComp[mkt]['FinalData'].loc['FinalMSSC'].iloc[0]
        Market_SecurityData['Final_FFmktCap']=Market_SecurityData['FF_MktCap_usd']*0
        Market_SecurityData['Final_Test']=Market_SecurityData['FF_MktCap_usd']*0
        
        # Minimum Free Float Market Cap: 
        # New: FFMktCap > 50% x SAIR MSSC, Existing: FFMktCap > 2/3*50% x SAIR MSSC
        
        idxNewSec = ( (Market_SecurityData['Status']!='STD') & (Market_SecurityData['Status']!='SML') ).tolist()
        idxExistingSec  = ( (Market_SecurityData['Status']=='STD') | (Market_SecurityData['Status']=='SML') ).tolist()
        
        Market_SecurityData.loc[((Market_SecurityData['FF_MktCap_usd'] >= 0.5* SAIR_MSSC) & idxNewSec), 'Final_FFmktCap'] = 1
        Market_SecurityData.loc[((Market_SecurityData['FF_MktCap_usd'] >= 0.5* (2/3)* SAIR_MSSC) & idxExistingSec), 'Final_FFmktCap'] = 1
        
        
        # Minimum FIF: 
        # TO BE DONE
        
        # Minimum Foreign Room Requirement
        # TO BE DONE
        
        Market_SecurityData['Final_Test'] = Market_SecurityData['Final_FFmktCap']
    
        OutputFinalInvestability[mkt]['Market_SecurityData'] = Market_SecurityData
    
   
    return OutputFinalInvestability

