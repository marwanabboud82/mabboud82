# -*- coding: utf-8 -*-
"""
@author: mabboud
"""
import pandas as pd
import os
import shutil



def GetATVRData (All_SecurityData, CalcDate, ATVRTrackerFilePath,tmpfilename):
    
    All_Out_Data = All_SecurityData.assign(**{'12mATVR':1000, '3m_Q-4':1000,'3m_Q-3':1000,'3m_Q-2':1000,'3m_Q-1':1000})
    
    
    """
    ##################
    # 12m ATVR and Q-1
    ##################
    """
    
    # DM STD Monthly File
    dmstdzip = (ATVRTrackerFilePath.loc['ATVRDMSTDPath'] +'_m15d_rif.zip')
    TmpFile = UnZipFile(dmstdzip[0])    
    DMSTDPath = (TmpFile + '\\' + tmpfilename +'_M15D_RIF')
    Monthly_DMSTD = ReadMonthlyFile(DMSTDPath)     
    Monthly_DMSTD['msci_security_code']= Monthly_DMSTD['msci_security_code'].astype(str) 
    Monthly_DMSTD['msci_security_code']= Monthly_DMSTD['msci_security_code'].str[:-2]
    All_Out_Data = All_Out_Data.merge(Monthly_DMSTD[['msci_security_code','aggr_atvr_fif_adj']],how='left',left_on='msci_security_code',right_on='msci_security_code')
    All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj']),'12mATVR']=All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj']),'aggr_atvr_fif_adj']
    All_Out_Data=All_Out_Data.drop(columns='aggr_atvr_fif_adj') 
    All_Out_Data = All_Out_Data.merge(Monthly_DMSTD[['msci_security_code','aggr_atvr_fif_adj_3m']],how='left',left_on='msci_security_code',right_on='msci_security_code')
    All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj_3m']),'3m_Q-1']=All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj_3m']),'aggr_atvr_fif_adj_3m']
    All_Out_Data=All_Out_Data.drop(columns='aggr_atvr_fif_adj_3m')
        
    # DM SML Monthly File
    dmsmlzip = (ATVRTrackerFilePath.loc['ATVRDMSMLPath'] + '_m17d_rif.zip')
    TmpFile = UnZipFile(dmsmlzip[0])    
    DMSMLPath = (TmpFile + '\\' + tmpfilename +'_M17D_RIF')
    Monthly_DMSML = ReadMonthlyFile(DMSMLPath)
    Monthly_DMSML['msci_security_code']= Monthly_DMSML['msci_security_code'].astype(str)
    Monthly_DMSML['msci_security_code']= Monthly_DMSML['msci_security_code'].str[:-2]
    All_Out_Data = All_Out_Data.merge(Monthly_DMSML[['msci_security_code','aggr_atvr_fif_adj']],how='left',left_on='msci_security_code',right_on='msci_security_code')
    All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj']),'12mATVR']=All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj']),'aggr_atvr_fif_adj']
    All_Out_Data=All_Out_Data.drop(columns='aggr_atvr_fif_adj')
    All_Out_Data = All_Out_Data.merge(Monthly_DMSML[['msci_security_code','aggr_atvr_fif_adj_3m']],how='left',left_on='msci_security_code',right_on='msci_security_code')
    All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj_3m']),'3m_Q-1']=All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj_3m']),'aggr_atvr_fif_adj_3m']
    All_Out_Data=All_Out_Data.drop(columns='aggr_atvr_fif_adj_3m')
    
    # EM STD Monthly File
    emstdzip = (ATVRTrackerFilePath.loc['ATVREMSTDPath'] + '_m15e_rif.zip')
    TmpFile = UnZipFile(emstdzip[0])    
    EMSTDPath = (TmpFile + '\\' + tmpfilename +'_M15E_RIF')
    Monthly_EMSTD = ReadMonthlyFile(EMSTDPath)     
    Monthly_EMSTD['msci_security_code']= Monthly_EMSTD['msci_security_code'].astype(str)
    Monthly_EMSTD['msci_security_code']= Monthly_EMSTD['msci_security_code'].str[:-2]
    All_Out_Data = All_Out_Data.merge(Monthly_EMSTD[['msci_security_code','aggr_atvr_fif_adj']],how='left',left_on='msci_security_code',right_on='msci_security_code')
    All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj']),'12mATVR']=All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj']),'aggr_atvr_fif_adj']
    All_Out_Data=All_Out_Data.drop(columns='aggr_atvr_fif_adj') 
    All_Out_Data = All_Out_Data.merge(Monthly_EMSTD[['msci_security_code','aggr_atvr_fif_adj_3m']],how='left',left_on='msci_security_code',right_on='msci_security_code')
    All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj_3m']),'3m_Q-1']=All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj_3m']),'aggr_atvr_fif_adj_3m']
    All_Out_Data=All_Out_Data.drop(columns='aggr_atvr_fif_adj_3m')
    
    # EM SML Monthly File
    emsmlzip = (ATVRTrackerFilePath.loc['ATVREMSMLPath'] + '_m17e_rif.zip')
    TmpFile = UnZipFile(emsmlzip[0])    
    EMSMLPath = (TmpFile + '\\' + tmpfilename +'_M17E_RIF')
    Monthly_EMSML = ReadMonthlyFile(EMSMLPath)
    Monthly_EMSML['msci_security_code']= Monthly_EMSML['msci_security_code'].astype(str)
    Monthly_EMSML['msci_security_code']= Monthly_EMSML['msci_security_code'].str[:-2]  
    All_Out_Data = All_Out_Data.merge(Monthly_EMSML[['msci_security_code','aggr_atvr_fif_adj']],how='left',left_on='msci_security_code',right_on='msci_security_code')
    All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj']),'12mATVR']=All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj']),'aggr_atvr_fif_adj']
    All_Out_Data=All_Out_Data.drop(columns='aggr_atvr_fif_adj')
    All_Out_Data = All_Out_Data.merge(Monthly_EMSML[['msci_security_code','aggr_atvr_fif_adj_3m']],how='left',left_on='msci_security_code',right_on='msci_security_code')
    All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj_3m']),'3m_Q-1']=All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj_3m']),'aggr_atvr_fif_adj_3m']
    All_Out_Data=All_Out_Data.drop(columns='aggr_atvr_fif_adj_3m')
    
    """
    ##################
    #  Q-2
    ##################
    """
    # DM STD Monthly File
    dmstdzip = (ATVRTrackerFilePath.loc['ATVRDMSTDPath'] + '_m15d_rif.zip')
    TmpFile = UnZipFile(dmstdzip[0])    
    DMSTDPath = (TmpFile + '\\' + tmpfilename +'_M15D_RIF')
    Monthly_DMSTD = ReadMonthlyFile(DMSTDPath)     
    Monthly_DMSTD['msci_security_code']= Monthly_DMSTD['msci_security_code'].astype(str)
    Monthly_DMSTD['msci_security_code']= Monthly_DMSTD['msci_security_code'].str[:-2]
    All_Out_Data = All_Out_Data.merge(Monthly_DMSTD[['msci_security_code','aggr_atvr_fif_adj_3m']],how='left',left_on='msci_security_code',right_on='msci_security_code')
    All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj_3m']),'3m_Q-2']=All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj_3m']),'aggr_atvr_fif_adj_3m']
    All_Out_Data=All_Out_Data.drop(columns='aggr_atvr_fif_adj_3m')
        
    # DM SML Monthly File
    dmsmlzip = (ATVRTrackerFilePath.loc['ATVRDMSMLPath'] + '_m17d_rif.zip')
    TmpFile = UnZipFile(dmsmlzip[0])    
    DMSMLPath = (TmpFile + '\\' + tmpfilename +'_M17D_RIF')
    Monthly_DMSML = ReadMonthlyFile(DMSMLPath)
    Monthly_DMSML['msci_security_code']= Monthly_DMSML['msci_security_code'].astype(str)  
    Monthly_DMSML['msci_security_code']= Monthly_DMSML['msci_security_code'].str[:-2]
    All_Out_Data = All_Out_Data.merge(Monthly_DMSML[['msci_security_code','aggr_atvr_fif_adj_3m']],how='left',left_on='msci_security_code',right_on='msci_security_code')
    All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj_3m']),'3m_Q-2']=All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj_3m']),'aggr_atvr_fif_adj_3m']
    All_Out_Data=All_Out_Data.drop(columns='aggr_atvr_fif_adj_3m')
    
    # EM STD Monthly File
    emstdzip = (ATVRTrackerFilePath.loc['ATVREMSTDPath'] + '_m15e_rif.zip')
    TmpFile = UnZipFile(emstdzip[0])    
    EMSTDPath = (TmpFile + '\\' + tmpfilename +'_M15E_RIF')
    Monthly_EMSTD = ReadMonthlyFile(EMSTDPath)     
    Monthly_EMSTD['msci_security_code']= Monthly_EMSTD['msci_security_code'].astype(str)   
    Monthly_EMSTD['msci_security_code']= Monthly_EMSTD['msci_security_code'].str[:-2]
    All_Out_Data = All_Out_Data.merge(Monthly_EMSTD[['msci_security_code','aggr_atvr_fif_adj_3m']],how='left',left_on='msci_security_code',right_on='msci_security_code')
    All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj_3m']),'3m_Q-1']=All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj_3m']),'aggr_atvr_fif_adj_3m']
    All_Out_Data=All_Out_Data.drop(columns='aggr_atvr_fif_adj_3m')
    
    # EM SML Monthly File
    emsmlzip = (ATVRTrackerFilePath.loc['ATVREMSMLPath'] + '_m17e_rif.zip')
    TmpFile = UnZipFile(emsmlzip[0])    
    EMSMLPath = (TmpFile + '\\' + tmpfilename +'_M17E_RIF')
    Monthly_EMSML = ReadMonthlyFile(EMSMLPath)
    Monthly_EMSML['msci_security_code']= Monthly_EMSML['msci_security_code'].astype(str)   
    Monthly_EMSML['msci_security_code']= Monthly_EMSML['msci_security_code'].str[:-2]  
    All_Out_Data = All_Out_Data.merge(Monthly_EMSML[['msci_security_code','aggr_atvr_fif_adj_3m']],how='left',left_on='msci_security_code',right_on='msci_security_code')
    All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj_3m']),'3m_Q-2']=All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj_3m']),'aggr_atvr_fif_adj_3m']
    All_Out_Data=All_Out_Data.drop(columns='aggr_atvr_fif_adj_3m')
    
    """
    ##################
    #  Q-2
    ##################
    """
    # DM STD Monthly File
    dmstdzip = (ATVRTrackerFilePath.loc['ATVRDMSTDPath'] + '_m15d_rif.zip')
    TmpFile = UnZipFile(dmstdzip[0])    
    DMSTDPath = (TmpFile + '\\' + tmpfilename +'_M15D_RIF')
    Monthly_DMSTD = ReadMonthlyFile(DMSTDPath)     
    Monthly_DMSTD['msci_security_code']= Monthly_DMSTD['msci_security_code'].astype(str)  
    Monthly_DMSTD['msci_security_code']= Monthly_DMSTD['msci_security_code'].str[:-2]
    All_Out_Data = All_Out_Data.merge(Monthly_DMSTD[['msci_security_code','aggr_atvr_fif_adj_3m']],how='left',left_on='msci_security_code',right_on='msci_security_code')
    All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj_3m']),'3m_Q-2']=All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj_3m']),'aggr_atvr_fif_adj_3m']
    All_Out_Data=All_Out_Data.drop(columns='aggr_atvr_fif_adj_3m')
        
    # DM SML Monthly File
    dmsmlzip = (ATVRTrackerFilePath.loc['ATVRDMSMLPath'] + '_m17d_rif.zip')
    TmpFile = UnZipFile(dmsmlzip[0])    
    DMSMLPath = (TmpFile + '\\' + tmpfilename +'_M17D_RIF')
    Monthly_DMSML = ReadMonthlyFile(DMSMLPath)
    Monthly_DMSML['msci_security_code']= Monthly_DMSML['msci_security_code'].astype(str)  
    Monthly_DMSML['msci_security_code']= Monthly_DMSML['msci_security_code'].str[:-2]
    All_Out_Data = All_Out_Data.merge(Monthly_DMSML[['msci_security_code','aggr_atvr_fif_adj_3m']],how='left',left_on='msci_security_code',right_on='msci_security_code')
    All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj_3m']),'3m_Q-2']=All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj_3m']),'aggr_atvr_fif_adj_3m']
    All_Out_Data=All_Out_Data.drop(columns='aggr_atvr_fif_adj_3m')
    
    # EM STD Monthly File
    emstdzip = (ATVRTrackerFilePath.loc['ATVREMSTDPath'] + '_m15e_rif.zip')
    TmpFile = UnZipFile(emstdzip[0])    
    EMSTDPath = (TmpFile + '\\' + tmpfilename +'_M15E_RIF')
    Monthly_EMSTD = ReadMonthlyFile(EMSTDPath)     
    Monthly_EMSTD['msci_security_code']= Monthly_EMSTD['msci_security_code'].astype(str) 
    Monthly_EMSTD['msci_security_code']= Monthly_EMSTD['msci_security_code'].str[:-2]
    All_Out_Data = All_Out_Data.merge(Monthly_EMSTD[['msci_security_code','aggr_atvr_fif_adj_3m']],how='left',left_on='msci_security_code',right_on='msci_security_code')
    All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj_3m']),'3m_Q-2']=All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj_3m']),'aggr_atvr_fif_adj_3m']
    All_Out_Data=All_Out_Data.drop(columns='aggr_atvr_fif_adj_3m')
    
    # EM SML Monthly File
    emsmlzip = (ATVRTrackerFilePath.loc['ATVREMSMLPath'] + '_m17e_rif.zip')
    TmpFile = UnZipFile(emsmlzip[0])    
    EMSMLPath = (TmpFile + '\\' + tmpfilename +'_M17E_RIF')
    Monthly_EMSML = ReadMonthlyFile(EMSMLPath)
    Monthly_EMSML['msci_security_code']= Monthly_EMSML['msci_security_code'].astype(str) 
    Monthly_EMSML['msci_security_code']= Monthly_EMSML['msci_security_code'].str[:-2]  
    All_Out_Data = All_Out_Data.merge(Monthly_EMSML[['msci_security_code','aggr_atvr_fif_adj_3m']],how='left',left_on='msci_security_code',right_on='msci_security_code')
    All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj_3m']),'3m_Q-2']=All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj_3m']),'aggr_atvr_fif_adj_3m']
    All_Out_Data=All_Out_Data.drop(columns='aggr_atvr_fif_adj_3m')
    
    
    """
    ##################
    #  Q-3
    ##################
    """
    # DM STD Monthly File
    dmstdzip = (ATVRTrackerFilePath.loc['ATVRDMSTDPath'] + '_m15d_rif.zip')
    TmpFile = UnZipFile(dmstdzip[0])    
    DMSTDPath = (TmpFile + '\\' + tmpfilename +'_M15D_RIF')
    Monthly_DMSTD = ReadMonthlyFile(DMSTDPath)     
    Monthly_DMSTD['msci_security_code']= Monthly_DMSTD['msci_security_code'].astype(str)  
    Monthly_DMSTD['msci_security_code']= Monthly_DMSTD['msci_security_code'].str[:-2]
    All_Out_Data = All_Out_Data.merge(Monthly_DMSTD[['msci_security_code','aggr_atvr_fif_adj_3m']],how='left',left_on='msci_security_code',right_on='msci_security_code')
    All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj_3m']),'3m_Q-3']=All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj_3m']),'aggr_atvr_fif_adj_3m']
    All_Out_Data=All_Out_Data.drop(columns='aggr_atvr_fif_adj_3m')
        
    # DM SML Monthly File
    dmsmlzip = (ATVRTrackerFilePath.loc['ATVRDMSMLPath'] + '_m17d_rif.zip')
    TmpFile = UnZipFile(dmsmlzip[0])    
    DMSMLPath = (TmpFile + '\\' + tmpfilename +'_M17D_RIF')
    Monthly_DMSML = ReadMonthlyFile(DMSMLPath)
    Monthly_DMSML['msci_security_code']= Monthly_DMSML['msci_security_code'].astype(str) 
    Monthly_DMSML['msci_security_code']= Monthly_DMSML['msci_security_code'].str[:-2]
    All_Out_Data = All_Out_Data.merge(Monthly_DMSML[['msci_security_code','aggr_atvr_fif_adj_3m']],how='left',left_on='msci_security_code',right_on='msci_security_code')
    All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj_3m']),'3m_Q-3']=All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj_3m']),'aggr_atvr_fif_adj_3m']
    All_Out_Data=All_Out_Data.drop(columns='aggr_atvr_fif_adj_3m')
    
    # EM STD Monthly File
    emstdzip = (ATVRTrackerFilePath.loc['ATVREMSTDPath'] + '_m15e_rif.zip')
    TmpFile = UnZipFile(emstdzip[0])    
    EMSTDPath = (TmpFile + '\\' + tmpfilename +'_M15E_RIF')
    Monthly_EMSTD = ReadMonthlyFile(EMSTDPath)     
    Monthly_EMSTD['msci_security_code']= Monthly_EMSTD['msci_security_code'].astype(str)   
    Monthly_EMSTD['msci_security_code']= Monthly_EMSTD['msci_security_code'].str[:-2]
    All_Out_Data = All_Out_Data.merge(Monthly_EMSTD[['msci_security_code','aggr_atvr_fif_adj_3m']],how='left',left_on='msci_security_code',right_on='msci_security_code')
    All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj_3m']),'3m_Q-3']=All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj_3m']),'aggr_atvr_fif_adj_3m']
    All_Out_Data=All_Out_Data.drop(columns='aggr_atvr_fif_adj_3m')
    
    # EM SML Monthly File
    emsmlzip = (ATVRTrackerFilePath.loc['ATVREMSMLPath'] + '_m17e_rif.zip')
    TmpFile = UnZipFile(emsmlzip[0])    
    EMSMLPath = (TmpFile + '\\' + tmpfilename +'_M17E_RIF')
    Monthly_EMSML = ReadMonthlyFile(EMSMLPath)
    Monthly_EMSML['msci_security_code']= Monthly_EMSML['msci_security_code'].astype(str)  
    Monthly_EMSML['msci_security_code']= Monthly_EMSML['msci_security_code'].str[:-2]  
    All_Out_Data = All_Out_Data.merge(Monthly_EMSML[['msci_security_code','aggr_atvr_fif_adj_3m']],how='left',left_on='msci_security_code',right_on='msci_security_code')
    All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj_3m']),'3m_Q-3']=All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj_3m']),'aggr_atvr_fif_adj_3m']
    All_Out_Data=All_Out_Data.drop(columns='aggr_atvr_fif_adj_3m')
    
    
    """
    ##################
    #  Q-4
    ##################
    """
    # DM STD Monthly File
    dmstdzip = (ATVRTrackerFilePath.loc['ATVRDMSTDPath'] + '_m15d_rif.zip')
    TmpFile = UnZipFile(dmstdzip[0])    
    DMSTDPath = (TmpFile + '\\' + tmpfilename +'_M15D_RIF')
    Monthly_DMSTD = ReadMonthlyFile(DMSTDPath)     
    Monthly_DMSTD['msci_security_code']= Monthly_DMSTD['msci_security_code'].astype(str)   
    Monthly_DMSTD['msci_security_code']= Monthly_DMSTD['msci_security_code'].str[:-2]
    All_Out_Data = All_Out_Data.merge(Monthly_DMSTD[['msci_security_code','aggr_atvr_fif_adj_3m']],how='left',left_on='msci_security_code',right_on='msci_security_code')
    All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj_3m']),'3m_Q-4']=All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj_3m']),'aggr_atvr_fif_adj_3m']
    All_Out_Data=All_Out_Data.drop(columns='aggr_atvr_fif_adj_3m')
        
    # DM SML Monthly File
    dmsmlzip = (ATVRTrackerFilePath.loc['ATVRDMSMLPath'] + '_m17d_rif.zip')
    TmpFile = UnZipFile(dmsmlzip[0])    
    DMSMLPath = (TmpFile + '\\' + tmpfilename +'_M17D_RIF')
    Monthly_DMSML = ReadMonthlyFile(DMSMLPath)
    Monthly_DMSML['msci_security_code']= Monthly_DMSML['msci_security_code'].astype(str)  
    Monthly_DMSML['msci_security_code']= Monthly_DMSML['msci_security_code'].str[:-2]
    All_Out_Data = All_Out_Data.merge(Monthly_DMSML[['msci_security_code','aggr_atvr_fif_adj_3m']],how='left',left_on='msci_security_code',right_on='msci_security_code')
    All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj_3m']),'3m_Q-4']=All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj_3m']),'aggr_atvr_fif_adj_3m']
    All_Out_Data=All_Out_Data.drop(columns='aggr_atvr_fif_adj_3m')
    
    # EM STD Monthly File
    emstdzip = (ATVRTrackerFilePath.loc['ATVREMSTDPath'] + '_m15e_rif.zip')
    TmpFile = UnZipFile(emstdzip[0])    
    EMSTDPath = (TmpFile + '\\' + tmpfilename +'_M15E_RIF')
    Monthly_EMSTD = ReadMonthlyFile(EMSTDPath)     
    Monthly_EMSTD['msci_security_code']= Monthly_EMSTD['msci_security_code'].astype(str)
    Monthly_EMSTD['msci_security_code']= Monthly_EMSTD['msci_security_code'].str[:-2]
    All_Out_Data = All_Out_Data.merge(Monthly_EMSTD[['msci_security_code','aggr_atvr_fif_adj_3m']],how='left',left_on='msci_security_code',right_on='msci_security_code')
    All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj_3m']),'3m_Q-4']=All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj_3m']),'aggr_atvr_fif_adj_3m']
    All_Out_Data=All_Out_Data.drop(columns='aggr_atvr_fif_adj_3m')
    
    # EM SML Monthly File
    emsmlzip = (ATVRTrackerFilePath.loc['ATVREMSMLPath'] + '_m17e_rif.zip')
    TmpFile = UnZipFile(emsmlzip[0])    
    EMSMLPath = (TmpFile + '\\' + tmpfilename +'_M17E_RIF')
    Monthly_EMSML = ReadMonthlyFile(EMSMLPath)
    Monthly_EMSML['msci_security_code']= Monthly_EMSML['msci_security_code'].astype(str)   
    Monthly_EMSML['msci_security_code']= Monthly_EMSML['msci_security_code'].str[:-2]  
    All_Out_Data = All_Out_Data.merge(Monthly_EMSML[['msci_security_code','aggr_atvr_fif_adj_3m']],how='left',left_on='msci_security_code',right_on='msci_security_code')
    All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj_3m']),'3m_Q-4']=All_Out_Data.loc[~pd.isna(All_Out_Data['aggr_atvr_fif_adj_3m']),'aggr_atvr_fif_adj_3m']
    All_Out_Data=All_Out_Data.drop(columns='aggr_atvr_fif_adj_3m')
    
       
    
    All_Out_Data[['12mATVR','3m_Q-4','3m_Q-3','3m_Q-2','3m_Q-1']]=All_Out_Data[['12mATVR','3m_Q-4','3m_Q-3','3m_Q-2','3m_Q-1']]/100 
    
    return  All_Out_Data


def ReadMonthlyFile(filename):
    nrowheader=152
    headercolspecs = [(0, 1), (1, 5), (5, 39), (39, 70), (70, 72), (72, 75), (75, 79)]
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