# -*- coding: utf-8 -*-
"""
@author: mabboud
"""
import pandas as pd
import numpy as np

def SizeSegmentAssignment(OutputUpdateSegmNbComp,All_CompanyData,All_SecurityData):
    
    OutputSizeSegmentAssignment={}
    
    for mkt in OutputUpdateSegmNbComp.keys():
        print(mkt)
        
        Market_SecurityData = All_SecurityData.loc[All_SecurityData['Market']==mkt]
        Market_CompanyData = All_CompanyData.loc[All_CompanyData['Market']==mkt]
        
        MIEU = OutputUpdateSegmNbComp[mkt]['MIEU']   
        SAIR_MSSC = OutputUpdateSegmNbComp[mkt]['FinalData'].loc['FinalMSSC'].iloc[0]
        MIEU['Iter']=float("NaN")
    

        
        # Step1: STD & NEITHER > 100%  x SAIR MSSC
        Iter1_List = (((MIEU['Status']=='STD') | (MIEU['Status']=='STD_SHADOW')) & (MIEU['company_full_mktcap']>=SAIR_MSSC)).tolist()
        MIEU.loc[Iter1_List,'Iter']=1
        
        # Step2: NEW > 100% x SAIR MSSC               
        Iter2_List = ( (MIEU['Status']=='NEW')  & (MIEU['company_full_mktcap']>=SAIR_MSSC) ).tolist()
        MIEU.loc[Iter2_List,'Iter']=2
        
        # Step3: SC  > 150%  x SAIR MSSC
        Iter3_List = ( (MIEU['Status']=='SML')  & (MIEU['company_full_mktcap']>= 1.5 * SAIR_MSSC) ).tolist()
        MIEU.loc[Iter3_List,'Iter']=3
        
        # Step4: STD & NEITHER > 66.67%  x SAIR MSSC
        Iter4_List = (((MIEU['Status']=='STD') | (MIEU['Status']=='STD_SHADOW')) & (MIEU['company_full_mktcap']>= 2/3 * SAIR_MSSC) & (MIEU['company_full_mktcap'] <  SAIR_MSSC) ).tolist()
        MIEU.loc[Iter4_List,'Iter']=4
            
        # Step5: SC > 100%  x SAIR MSSC
        Iter5_List = ( (MIEU['Status']=='SML')  & (MIEU['company_full_mktcap']>= SAIR_MSSC) & (MIEU['company_full_mktcap'] < 1.5 * SAIR_MSSC) ).tolist()
        MIEU.loc[Iter5_List,'Iter']=5
        
        MIEU= MIEU.sort_values(['Iter', 'company_full_mktcap'], ascending=[True, False])
        
        MIEU['Iter_Rank'] = [i for i in range(1,MIEU.shape[0]+1)] 
        MIEU.loc[MIEU['Iter'].isna(),'Iter_Rank']=float("NAN")
        
        Market_SecurityData = pd.merge(Market_SecurityData,MIEU[['msci_issuer_code','Iter']],on='msci_issuer_code',how='left')
        Market_CompanyData = pd.merge(Market_CompanyData,MIEU[['msci_issuer_code','Iter']],on='msci_issuer_code',how='left')
        Market_SecurityData = pd.merge(Market_SecurityData,MIEU[['msci_issuer_code','Iter_Rank']],on='msci_issuer_code',how='left')
        Market_CompanyData = pd.merge(Market_CompanyData,MIEU[['msci_issuer_code','Iter_Rank']],on='msci_issuer_code',how='left')
        
        
        # Restructure the Output Dictionaries
        OutputSizeSegmentAssignment[mkt]={}
        OutputSizeSegmentAssignment[mkt]['MIEU']= MIEU
        OutputSizeSegmentAssignment[mkt]['Market_CompanyData']=Market_CompanyData
        OutputSizeSegmentAssignment[mkt]['Market_SecurityData']=Market_SecurityData
        
              
   
    return OutputSizeSegmentAssignment

