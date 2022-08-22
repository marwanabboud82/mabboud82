# -*- coding: utf-8 -*-
"""
@author: mabboud
"""
import pandas as pd
import numpy as np
from scipy.stats import norm

def SizeSegmentAssignment(OutputUpdateSegmNbComp,All_CompanyData,All_SecurityData):
    
    OutputSizeSegmentAssignment={}
    
    for mkt in OutputUpdateSegmNbComp.keys():
        print(mkt)
        
        Market_SecurityData = All_SecurityData.loc[All_SecurityData['Market']==mkt]
        Market_CompanyData = All_CompanyData.loc[All_CompanyData['Market']==mkt]
        
        MIEU = OutputUpdateSegmNbComp[mkt]['MIEU']
        
        MIEU= MIEU.sort_values('company_full_mktcap', ascending=False)
        
        MIEU['SAIR_Rank'] = [i for i in range(1,MIEU.shape[0]+1)] 
            
        
        
        SAIR_MSSC = OutputUpdateSegmNbComp[mkt]['FinalData'].loc['FinalMSSC'].iloc[0]
        SAIR_MSnbC = OutputUpdateSegmNbComp[mkt]['FinalData'].loc['FinalMSnbC'].iloc[0]
        
        MIEU['SAIR_MSSC']=SAIR_MSSC
        MIEU['SAIR_MSnbC']=SAIR_MSnbC
        MIEU['Iter']=float("NaN")
        MIEU['Dist_1']=float("NaN")
        MIEU['Dist_2']=float("NaN")
    
     
        # Step1: STD & NEITHER > 100%  x SAIR MSSC
        Iter1_List = (((MIEU['Status']=='STD') | (MIEU['Status']=='STD_SHADOW')) & (MIEU['company_full_mktcap']>=SAIR_MSSC)).tolist()
        MIEU.loc[Iter1_List,'Iter']=1
        Iter1b_List = ((MIEU['Status']=='STD') | (MIEU['Status']=='STD_SHADOW')).tolist()
        MIEU.loc[Iter1b_List ,'Dist_1']= SAIR_MSSC / MIEU.loc[Iter1b_List,'company_full_mktcap']-1
        
        # Step2: NEW > 100% x SAIR MSSC               
        Iter2_List = ( (MIEU['Status']=='NEW')  & (MIEU['company_full_mktcap']>=SAIR_MSSC) ).tolist()
        MIEU.loc[Iter2_List,'Iter']=2
        Iter2b_List = ((MIEU['Status']=='NEW')).tolist()
        MIEU.loc[Iter2b_List ,'Dist_1'] = SAIR_MSSC / MIEU.loc[Iter2b_List,'company_full_mktcap']-1
        MIEU.loc[Iter2b_List ,'Dist_2'] = SAIR_MSSC / MIEU.loc[Iter2b_List,'company_full_mktcap']-1
        
        # Step3: SC  > 150%  x SAIR MSSC
        Iter3_List = ( (MIEU['Status']=='SML')  & (MIEU['company_full_mktcap']>= 1.5 * SAIR_MSSC) ).tolist()
        MIEU.loc[Iter3_List,'Iter']=3
        Iter3b_List = ((MIEU['Status']=='SML') ).tolist()
        MIEU.loc[Iter3b_List ,'Dist_1'] = ((1.5 * SAIR_MSSC) / MIEU.loc[Iter3b_List,'company_full_mktcap']) -1
        
        # Step4: STD & NEITHER > 66.67%  x SAIR MSSC
        Iter4_List = (((MIEU['Status']=='STD') | (MIEU['Status']=='STD_SHADOW')) & (MIEU['company_full_mktcap']>= 2/3 * SAIR_MSSC) & (MIEU['company_full_mktcap'] <  SAIR_MSSC) ).tolist()
        MIEU.loc[Iter4_List,'Iter']=4
        Iter4b_List = ((MIEU['Status']=='STD') | (MIEU['Status']=='STD_SHADOW')).tolist()
        MIEU.loc[Iter4b_List ,'Dist_2'] = ((2/3 * SAIR_MSSC) / MIEU.loc[Iter4b_List,'company_full_mktcap']) -1
            
        # Step5: SC > 100%  x SAIR MSSC
        Iter5_List = ( (MIEU['Status']=='SML')  & (MIEU['company_full_mktcap']>= SAIR_MSSC) & (MIEU['company_full_mktcap'] < 1.5 * SAIR_MSSC) ).tolist()
        MIEU.loc[Iter5_List,'Iter']=5
        Iter5b_List = ((MIEU['Status']=='SML') ).tolist()
        MIEU.loc[Iter5b_List ,'Dist_2'] = (SAIR_MSSC / MIEU.loc[Iter5b_List,'company_full_mktcap']) -1
        
        MIEU= MIEU.sort_values(['Iter', 'company_full_mktcap'], ascending=[True, False])
        
        
        MIEU['Iter_Rank'] = [i for i in range(1,MIEU.shape[0]+1)] 
        MIEU.loc[MIEU['Iter'].isna(),'Iter_Rank']=float("NAN")
        
        Market_SecurityData = pd.merge(Market_SecurityData,MIEU[['msci_issuer_code','SAIR_MSSC']],on='msci_issuer_code',how='left')
        Market_CompanyData = pd.merge(Market_CompanyData,MIEU[['msci_issuer_code','SAIR_MSSC']],on='msci_issuer_code',how='left')

        Market_SecurityData = pd.merge(Market_SecurityData,MIEU[['msci_issuer_code','SAIR_MSnbC']],on='msci_issuer_code',how='left')
        Market_CompanyData = pd.merge(Market_CompanyData,MIEU[['msci_issuer_code','SAIR_MSnbC']],on='msci_issuer_code',how='left')

        
        Market_SecurityData = pd.merge(Market_SecurityData,MIEU[['msci_issuer_code','SAIR_Rank']],on='msci_issuer_code',how='left')
        Market_CompanyData = pd.merge(Market_CompanyData,MIEU[['msci_issuer_code','SAIR_Rank']],on='msci_issuer_code',how='left')

        Market_SecurityData = pd.merge(Market_SecurityData,MIEU[['msci_issuer_code','CoverageFFMktCap']],on='msci_issuer_code',how='left')
        Market_CompanyData = pd.merge(Market_CompanyData,MIEU[['msci_issuer_code','CoverageFFMktCap']],on='msci_issuer_code',how='left')
        
        Market_SecurityData = pd.merge(Market_SecurityData,MIEU[['msci_issuer_code','Iter']],on='msci_issuer_code',how='left')
        Market_CompanyData = pd.merge(Market_CompanyData,MIEU[['msci_issuer_code','Iter']],on='msci_issuer_code',how='left')
                
        Market_SecurityData = pd.merge(Market_SecurityData,MIEU[['msci_issuer_code','Iter_Rank']],on='msci_issuer_code',how='left')
        Market_CompanyData = pd.merge(Market_CompanyData,MIEU[['msci_issuer_code','Iter_Rank']],on='msci_issuer_code',how='left')
        
        Market_SecurityData = pd.merge(Market_SecurityData,MIEU[['msci_issuer_code','Dist_1']],on='msci_issuer_code',how='left')
        Market_CompanyData = pd.merge(Market_CompanyData,MIEU[['msci_issuer_code','Dist_1']],on='msci_issuer_code',how='left')
        
        Market_SecurityData = pd.merge(Market_SecurityData,MIEU[['msci_issuer_code','Dist_2']],on='msci_issuer_code',how='left')
        Market_CompanyData = pd.merge(Market_CompanyData,MIEU[['msci_issuer_code','Dist_2']],on='msci_issuer_code',how='left')
        
        # Restructure the Output Dictionaries
        OutputSizeSegmentAssignment[mkt]={}
        OutputSizeSegmentAssignment[mkt]['MIEU']= MIEU
        OutputSizeSegmentAssignment[mkt]['Market_CompanyData']=Market_CompanyData
        OutputSizeSegmentAssignment[mkt]['Market_SecurityData']=Market_SecurityData
        

    return OutputSizeSegmentAssignment

