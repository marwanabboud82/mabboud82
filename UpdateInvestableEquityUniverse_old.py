# -*- coding: utf-8 -*-
"""
Created on Mon Dec  9 09:22:10 2019

@author: mabboud
"""

import pandas as pd

def  UpdateInvestableEquityUniverse(All_SecurityData,All_CompanyData,MSRRankPrevSAIR):
    
    # 1-Updating the Equity Universe Minimum Size Requirement (MSR)
    DMCompanyData = All_CompanyData[All_CompanyData['Region']=='DM']
    DMCompanyData=DMCompanyData.sort_values('company_full_mktcap', ascending=False)
    
    CumFFMktCap = DMCompanyData['FFCompMktCap'].cumsum()
    xxx=CumFFMktCap.tail(1)
    
    CoverageFFMktCap = pd.DataFrame(CumFFMktCap / xxx.iloc[0])
    #CoverageFFMktCap['RowNb']=  np.arange(len(CoverageFFMktCap))
    
    if (CoverageFFMktCap.iloc[MSRRankPrevSAIR] < 0.99).bool():
        MSRRankPrevSAIR = CoverageFFMktCap[CoverageFFMktCap['FFCompMktCap']<0.99].count()+1
    elif (CoverageFFMktCap.iloc[MSRRankPrevSAIR] > 0.9925).bool():
        MSRRankPrevSAIR = CoverageFFMktCap[CoverageFFMktCap['FFCompMktCap']<0.9925].count()+1

    #NewGIEUMSR = pd.DataFrame({'Rank':[(MSRRankPrevSAIR).iloc[0]],'MktCap':[DMCompanyData.iloc[MSRRankPrevSAIR,DMCompanyData.columns.get_loc('company_full_mktcap')].iloc[0]],'Coverage':[CoverageFFMktCap.iloc[MSRRankPrevSAIR].iloc[0].iloc[0]]})
    NewGIEUMSR = pd.DataFrame({'Rank':[(MSRRankPrevSAIR)],'MktCap':[DMCompanyData.iloc[MSRRankPrevSAIR,DMCompanyData.columns.get_loc('company_full_mktcap')]],'Coverage':[CoverageFFMktCap.iloc[MSRRankPrevSAIR].iloc[0]]})
    
    
    # 2- Updating the Equity Universe Minimum Free Float–Adjusted Market Capitalization
    NewGIEUMSR['MinFFMCR']=0.5 * NewGIEUMSR['MktCap']
    
    # 3- Evaluate Companies versus the MSR
    TestvsMSRComp = All_CompanyData['company_full_mktcap']*0
    TestvsMSRComp[ All_CompanyData['company_full_mktcap']>NewGIEUMSR['MktCap'].iloc[0]] =1
    TestvsMSRComp[ All_CompanyData['InTF']==1] =1
    All_CompanyData['TestvsMSRComp']=TestvsMSRComp
    
    TestvsMSRSec = All_SecurityData['company_full_mktcap']*0
    TestvsMSRSec[ All_SecurityData['company_full_mktcap']>NewGIEUMSR['MktCap'].iloc[0]] =1
    TestvsMSRSec[ All_SecurityData['InTF']==1] =1
    All_SecurityData['TestvsMSRSec']=TestvsMSRSec
    
    del TestvsMSRComp,TestvsMSRSec
    
    # 4- Evaluate Companies versus the MinFFMCR
    TestvsFFMCRComp = All_CompanyData['FFCompMktCap']*0
    TestvsFFMCRComp[ All_CompanyData['FFCompMktCap']>NewGIEUMSR['MinFFMCR'].iloc[0]] =1
    TestvsFFMCRComp[ All_CompanyData['InTF']==1] =1
    All_CompanyData['TestvsFFMCRComp']=TestvsFFMCRComp
      
    All_SecurityData[['company_full_mktcap','foreign_inc_factor_next_day']]=All_SecurityData[['company_full_mktcap','foreign_inc_factor_next_day']].astype(float)
    All_SecurityDataCompFFMktCap= All_SecurityData['company_full_mktcap'] * All_SecurityData['foreign_inc_factor_next_day']
    TestvsFFMCRSec = All_SecurityDataCompFFMktCap*0
    TestvsFFMCRSec[ All_SecurityDataCompFFMktCap>NewGIEUMSR['MinFFMCR'].iloc[0]] =1
    TestvsFFMCRSec[ All_SecurityData['InTF']==1] =1
    All_SecurityData['TestvsFFMCRSec']=TestvsFFMCRSec
    
    del TestvsFFMCRComp,TestvsFFMCRSec

    # 5- Evaluate Minimum Liquidity Requirements (security level)
    # 12mATVR New: DM(20%), EM(15%). Existing: DM(13.3%), EM(10%)
    # 3mATVR. New: DM(20%), EM(15%). Existing: DM(5%), EM(5%)
    # 3mFoTR. New: DM(90%), EM(80%). Existing: DM(80%), EM(70%) (MISSING FOR NOW)
    
    # 12mATVR New: DM(20%), EM(15%). Existing: DM(13.3%), EM(10%)
    All_SecurityData['12mATVR_Test']= All_SecurityData['12mATVR']*0
    DMExisting = (All_SecurityData['12mATVR']>=2/3*0.2) & (All_SecurityData['Region']=='DM') & ((All_SecurityData['Status']=='STD') | (All_SecurityData['Status']=='SML'))
    EMExisting = (All_SecurityData['12mATVR']>=2/3*0.15) & (All_SecurityData['Region']=='EM') & ((All_SecurityData['Status']=='STD') | (All_SecurityData['Status']=='SML'))
    DMNew = (All_SecurityData['12mATVR']>=0.2) & (All_SecurityData['Region']=='DM') & ((All_SecurityData['Status']!='STD') & (All_SecurityData['Status']!='SML'))
    EMNew = (All_SecurityData['12mATVR']>=0.15) & (All_SecurityData['Region']=='EM') & ((All_SecurityData['Status']!='STD') & (All_SecurityData['Status']!='SML'))
    
    All_SecurityData.loc[DMExisting,'12mATVR_Test']= 1
    All_SecurityData.loc[EMExisting,'12mATVR_Test']= 1
    All_SecurityData.loc[DMNew,'12mATVR_Test']= 1
    All_SecurityData.loc[EMNew,'12mATVR_Test']= 1
    
    # 3mATVR. New: DM(20%), EM(15%). Existing: DM(5%), EM(5%)
    All_SecurityData['3m_Q-4_Test']= All_SecurityData['3m_Q-4']*0
    DMExisting = (All_SecurityData['3m_Q-4']>=0.05) & (All_SecurityData['Region']=='DM') & ((All_SecurityData['Status']=='STD') | (All_SecurityData['Status']=='SML'))
    EMExisting = (All_SecurityData['3m_Q-4']>=0.05) & (All_SecurityData['Region']=='EM') & ((All_SecurityData['Status']=='STD') | (All_SecurityData['Status']=='SML'))
    DMNew = (All_SecurityData['3m_Q-4']>=0.2) & (All_SecurityData['Region']=='DM') & ((All_SecurityData['Status']!='STD') & (All_SecurityData['Status']!='SML'))
    EMNew = (All_SecurityData['3m_Q-4']>=0.15) & (All_SecurityData['Region']=='EM') & ((All_SecurityData['Status']!='STD') & (All_SecurityData['Status']!='SML'))
    
    All_SecurityData.loc[DMExisting,'3m_Q-4_Test']= 1
    All_SecurityData.loc[EMExisting,'3m_Q-4_Test']= 1
    All_SecurityData.loc[DMNew,'3m_Q-4_Test']= 1
    All_SecurityData.loc[EMNew,'3m_Q-4_Test']= 1
    
    All_SecurityData['3m_Q-3_Test']= All_SecurityData['3m_Q-3']*0
    DMExisting = (All_SecurityData['3m_Q-3']>=0.05) & (All_SecurityData['Region']=='DM') & ((All_SecurityData['Status']=='STD') | (All_SecurityData['Status']=='SML'))
    EMExisting = (All_SecurityData['3m_Q-3']>=0.05) & (All_SecurityData['Region']=='EM') & ((All_SecurityData['Status']=='STD') | (All_SecurityData['Status']=='SML'))
    DMNew = (All_SecurityData['3m_Q-3']>=0.2) & (All_SecurityData['Region']=='DM') & ((All_SecurityData['Status']!='STD') & (All_SecurityData['Status']!='SML'))
    EMNew = (All_SecurityData['3m_Q-3']>=0.15) & (All_SecurityData['Region']=='EM') & ((All_SecurityData['Status']!='STD') & (All_SecurityData['Status']!='SML'))
    All_SecurityData.loc[DMExisting,'3m_Q-3_Test']= 1
    All_SecurityData.loc[EMExisting,'3m_Q-3_Test']= 1
    All_SecurityData.loc[DMNew,'3m_Q-3_Test']= 1
    All_SecurityData.loc[EMNew,'3m_Q-3_Test']= 1    
    
    All_SecurityData['3m_Q-3_Test']= All_SecurityData['3m_Q-3']*0
    DMExisting = (All_SecurityData['3m_Q-3']>=0.05) & (All_SecurityData['Region']=='DM') & ((All_SecurityData['Status']=='STD') | (All_SecurityData['Status']=='SML'))
    EMExisting = (All_SecurityData['3m_Q-3']>=0.05) & (All_SecurityData['Region']=='EM') & ((All_SecurityData['Status']=='STD') | (All_SecurityData['Status']=='SML'))
    DMNew = (All_SecurityData['3m_Q-3']>=0.2) & (All_SecurityData['Region']=='DM') & ((All_SecurityData['Status']!='STD') & (All_SecurityData['Status']!='SML'))
    EMNew = (All_SecurityData['3m_Q-3']>=0.15) & (All_SecurityData['Region']=='EM') & ((All_SecurityData['Status']!='STD') & (All_SecurityData['Status']!='SML'))
    All_SecurityData.loc[DMExisting,'3m_Q-3_Test']= 1
    All_SecurityData.loc[EMExisting,'3m_Q-3_Test']= 1
    All_SecurityData.loc[DMNew,'3m_Q-3_Test']= 1
    All_SecurityData.loc[EMNew,'3m_Q-3_Test']= 1    
    
    All_SecurityData['3m_Q-2_Test']= All_SecurityData['3m_Q-2']*0
    DMExisting = (All_SecurityData['3m_Q-2']>=0.05) & (All_SecurityData['Region']=='DM') & ((All_SecurityData['Status']=='STD') | (All_SecurityData['Status']=='SML'))
    EMExisting = (All_SecurityData['3m_Q-2']>=0.05) & (All_SecurityData['Region']=='EM') & ((All_SecurityData['Status']=='STD') | (All_SecurityData['Status']=='SML'))
    DMNew = (All_SecurityData['3m_Q-2']>=0.2) & (All_SecurityData['Region']=='DM') & ((All_SecurityData['Status']!='STD') & (All_SecurityData['Status']!='SML'))
    EMNew = (All_SecurityData['3m_Q-2']>=0.15) & (All_SecurityData['Region']=='EM') & ((All_SecurityData['Status']!='STD') & (All_SecurityData['Status']!='SML'))
    All_SecurityData.loc[DMExisting,'3m_Q-2_Test']= 1
    All_SecurityData.loc[EMExisting,'3m_Q-2_Test']= 1
    All_SecurityData.loc[DMNew,'3m_Q-2_Test']= 1
    All_SecurityData.loc[EMNew,'3m_Q-2_Test']= 1  
    
    All_SecurityData['3m_Q-1_Test']= All_SecurityData['3m_Q-1']*0
    DMExisting = (All_SecurityData['3m_Q-1']>=0.05) & (All_SecurityData['Region']=='DM') & ((All_SecurityData['Status']=='STD') | (All_SecurityData['Status']=='SML'))
    EMExisting = (All_SecurityData['3m_Q-1']>=0.05) & (All_SecurityData['Region']=='EM') & ((All_SecurityData['Status']=='STD') | (All_SecurityData['Status']=='SML'))
    DMNew = (All_SecurityData['3m_Q-1']>=0.2) & (All_SecurityData['Region']=='DM') & ((All_SecurityData['Status']!='STD') & (All_SecurityData['Status']!='SML'))
    EMNew = (All_SecurityData['3m_Q-1']>=0.15) & (All_SecurityData['Region']=='EM') & ((All_SecurityData['Status']!='STD') & (All_SecurityData['Status']!='SML'))
    All_SecurityData.loc[DMExisting,'3m_Q-1_Test']= 1
    All_SecurityData.loc[EMExisting,'3m_Q-1_Test']= 1
    All_SecurityData.loc[DMNew,'3m_Q-1_Test']= 1
    All_SecurityData.loc[EMNew,'3m_Q-1_Test']= 1    
    
    All_SecurityData['BasicATVRTest']= All_SecurityData['12mATVR_Test']* All_SecurityData['3m_Q-4_Test'] * \
    All_SecurityData['3m_Q-3_Test']*All_SecurityData['3m_Q-2_Test']* All_SecurityData['3m_Q-1_Test']
    
    #All_SecurityData.loc[All_SecurityData['msci_security_code']==1268204,'3m_Q-3_Test']= 0.01
    # AA_Comp_TempSort.loc[AA_Comp_TempSort['msci_issuer_code']==12682,'BasicATVRTest']
    # xxx.loc[xxx['msci_issuer_code']==12682,'BasicATVRTest']
    
    # Take ATVR from security to company. if one security in a company passes ATVR, then we consider company would pass ATVR.
    AA_Sec_TempSort = All_SecurityData.sort_values(['msci_issuer_code', 'BasicATVRTest','FF_MktCap_usd'], ascending=[True, False,False])
    AA_Comp_TempSort = AA_Sec_TempSort.drop_duplicates(subset=['msci_issuer_code'], keep='first')
    AA_Comp_TempSort= AA_Comp_TempSort[['msci_issuer_code','BasicATVRTest']]
    
    All_CompanyData = All_CompanyData.merge(AA_Comp_TempSort[['msci_issuer_code','BasicATVRTest']],how='left',left_on='msci_issuer_code',right_on='msci_issuer_code')
    
    

    del DMExisting, EMExisting, DMNew, EMNew, AA_Sec_TempSort, AA_Comp_TempSort
    
    # 6- Global Minimum Foreign Inclusion Factor Requirement
    # FIF > 0.15 or  FF Market Cap > 1.8*0.5 * MSSC For New Securities
    # Existing Securities are not tested
    NewSec = ((All_SecurityData['foreign_inc_factor_next_day']>=0.15)  | (All_SecurityData['FF_MktCap_usd']>= 0.5 * 1.8 * 1e+6 * All_SecurityData['Interim_MSSC']) ) \
    & ((All_SecurityData['Status']!='STD') & (All_SecurityData['Status']!='SML'))
    All_SecurityData['TestMinFIF']= All_SecurityData['foreign_inc_factor_next_day']*0
    All_SecurityData.loc[NewSec,'TestMinFIF']= 1  
    ExistingSec = ((All_SecurityData['Status']=='STD') | (All_SecurityData['Status']=='SML'))
    All_SecurityData.loc[ExistingSec,'TestMinFIF']= 1  
        
    # Take TestMinFIF from security to company. if one security in a company passes test, then we consider company would pass as well.
    AA_Sec_TempSort = All_SecurityData.sort_values(['msci_issuer_code', 'TestMinFIF','FF_MktCap_usd'], ascending=[True, False,False])
    AA_Comp_TempSort = AA_Sec_TempSort.drop_duplicates(subset=['msci_issuer_code'], keep='first')
    AA_Comp_TempSort= AA_Comp_TempSort[['msci_issuer_code','TestMinFIF']]
        
    All_CompanyData = All_CompanyData.merge(AA_Comp_TempSort[['msci_issuer_code','TestMinFIF']],how='left',left_on='msci_issuer_code',right_on='msci_issuer_code')
        
    del NewSec, ExistingSec, AA_Sec_TempSort, AA_Comp_TempSort
    
    # 7- Minimum Foreign Room Requirement (TO BE DONE)

    # 8- Minimum Length of Trading Requirement (TO BE DONE)
    
    # GIEU Security / Company
    AllBasicTest =   All_SecurityData['TestvsMSRSec'] * All_SecurityData['TestvsFFMCRSec'] * \
    All_SecurityData['BasicATVRTest'] * All_SecurityData['TestMinFIF']
    All_SecurityData['All_BasicTest']=AllBasicTest
    
    AllBasicTestComp =   All_CompanyData['TestvsMSRComp'] * All_CompanyData['TestvsFFMCRComp'] * \
    All_CompanyData['BasicATVRTest'] * All_CompanyData['TestMinFIF']
    All_CompanyData['All_BasicTest']= AllBasicTestComp
    
    
    
    
    return NewGIEUMSR, All_SecurityData, All_CompanyData