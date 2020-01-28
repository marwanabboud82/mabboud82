# -*- coding: utf-8 -*-
"""
@author: mabboud
"""
import pandas as pd

def CleanTrackerFiles (Tracker_DMSTD, Tracker_DMSML,Tracker_EMSTD, Tracker_EMSML):

    # clean DM STD
    Tracker_DMSTD['Status'] = 'STD_SHADOW'
    Tracker_DMSTD.loc[Tracker_DMSTD.family_std_flag_next_day==1, 'Status'] = 'STD'
    
    Tracker_DMSTD['Size_Cap'] = 'STD_None'
    Tracker_DMSTD.loc[Tracker_DMSTD.family_large_flag_next_day==1, 'Size_Cap'] = 'Large'
    Tracker_DMSTD.loc[Tracker_DMSTD.family_mid_flag_next_day==1, 'Size_Cap'] = 'Mid'
    
    Tracker_DMSTD[ 'Dom_Flag'] = 0
    Tracker_DMSTD.loc[Tracker_DMSTD.family_std_dom_flag_next_day==1, 'Dom_Flag'] = 1
    
    Tracker_DMSTD['Pro_Status'] = 'Pro_STD_SHADOW'
    Tracker_DMSTD.loc[Tracker_DMSTD.pro_forma_family_std_flag==1, 'Pro_Status'] = 'Pro_STD'
    
    Tracker_DMSTD['IIF'] = Tracker_DMSTD.std_IIF_next_day
    
    from CheckFieldsToDrop import CheckFieldsToDrop
    FieldsToDrop = CheckFieldsToDrop()
    
    Clean_Tracker_DMSTD=Tracker_DMSTD.drop(Tracker_DMSTD.columns[FieldsToDrop.iloc[:,0]], axis=1)
    
    # clean DM SML
    Tracker_DMSML['Status'] = 'SML_SHADOW'
    Tracker_DMSML.loc[Tracker_DMSML.family_micro_flag_next_day==1, 'Status'] = 'MICRO'
    Tracker_DMSML.loc[Tracker_DMSML.family_scap_flag_next_day==1, 'Status'] = 'SML'
    
    Tracker_DMSML['Size_Cap'] = 'SML_None'
    Tracker_DMSML.loc[Tracker_DMSML.family_scap_flag_next_day==1, 'Size_Cap'] = 'Small'
    Tracker_DMSML.loc[Tracker_DMSML.family_micro_flag_next_day==1, 'Size_Cap'] = 'Micro'
    
    Tracker_DMSML[ 'Dom_Flag'] = 0
    Tracker_DMSML.loc[Tracker_DMSML.family_small_dom_flag_next_day==1, 'Dom_Flag'] = 1
    
    Tracker_DMSML['Pro_Status'] = 'Pro_SML_SHADOW'
    Tracker_DMSML.loc[Tracker_DMSML.pro_forma_family_scap_flag==1, 'Pro_Status'] = 'Pro_SML'
    Tracker_DMSML.loc[Tracker_DMSML.pro_forma_family_micro_flag==1, 'Pro_Status'] = 'Pro_MICRO'
    
    Tracker_DMSML['IIF'] = Tracker_DMSML.scap_IIF_next_day
    
    Clean_Tracker_DMSML=Tracker_DMSML.drop(Tracker_DMSML.columns[FieldsToDrop.iloc[:,2]], axis=1)
    
    
     # clean EM STD
    Tracker_EMSTD['Status'] = 'STD_SHADOW'
    Tracker_EMSTD.loc[Tracker_EMSTD.family_std_flag_next_day==1, 'Status'] = 'STD'
    
    Tracker_EMSTD['Size_Cap'] = 'STD_None'
    Tracker_EMSTD.loc[Tracker_EMSTD.family_large_flag_next_day==1, 'Size_Cap'] = 'Large'
    Tracker_EMSTD.loc[Tracker_EMSTD.family_mid_flag_next_day==1, 'Size_Cap'] = 'Mid'
    
    Tracker_EMSTD[ 'Dom_Flag'] = 0
    Tracker_EMSTD.loc[Tracker_EMSTD.family_std_dom_flag_next_day==1, 'Dom_Flag'] = 1
    
    Tracker_EMSTD['Pro_Status'] = 'Pro_STD_SHADOW'
    Tracker_EMSTD.loc[Tracker_EMSTD.pro_forma_family_std_flag==1, 'Pro_Status'] = 'Pro_STD'
    
    Tracker_EMSTD['IIF'] = Tracker_EMSTD.std_IIF_next_day
       
    Clean_Tracker_EMSTD=Tracker_EMSTD.drop(Tracker_EMSTD.columns[FieldsToDrop.iloc[:,0]], axis=1)
    
    # clean DM SML
    Tracker_EMSML['Status'] = 'SML_SHADOW'
    Tracker_EMSML.loc[Tracker_EMSML.family_micro_flag_next_day==1, 'Status'] = 'MICRO'
    Tracker_EMSML.loc[Tracker_EMSML.family_scap_flag_next_day==1, 'Status'] = 'SML'
    
    Tracker_EMSML['Size_Cap'] = 'SML_None'
    Tracker_EMSML.loc[Tracker_EMSML.family_scap_flag_next_day==1, 'Size_Cap'] = 'Small'
    Tracker_EMSML.loc[Tracker_EMSML.family_micro_flag_next_day==1, 'Size_Cap'] = 'Micro'
    
    Tracker_EMSML[ 'Dom_Flag'] = 0
    Tracker_EMSML.loc[Tracker_EMSML.family_small_dom_flag_next_day==1, 'Dom_Flag'] = 1
    
    Tracker_EMSML['Pro_Status'] = 'Pro_SML_SHADOW'
    Tracker_EMSML.loc[Tracker_EMSML.pro_forma_family_scap_flag==1, 'Pro_Status'] = 'Pro_SML'
    Tracker_EMSML.loc[Tracker_EMSML.pro_forma_family_micro_flag==1, 'Pro_Status'] = 'Pro_MICRO'
    
    Tracker_EMSML['IIF'] = Tracker_EMSML.scap_IIF_next_day
    
    Clean_Tracker_EMSML=Tracker_EMSML.drop(Tracker_DMSML.columns[FieldsToDrop.iloc[:,2]], axis=1)

    return  Clean_Tracker_DMSTD,Clean_Tracker_DMSML,Clean_Tracker_EMSTD,Clean_Tracker_EMSML