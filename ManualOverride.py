# -*- coding: utf-8 -*-
"""
@author: mabboud
"""
import pandas as pd
import numpy as np



def ManualOverride (All_SecurityData, CalcDate, InputPath):
        
    ManualOverridePath = (InputPath + 'Manual_Override.csv')
    ManualOverride = pd.read_csv(ManualOverridePath,sep=',') 
    ManualOverride['msci_security_code']=ManualOverride['msci_security_code'].astype(str)
    
    All_SecurityData['Manual_Override']=All_SecurityData['foreign_inc_factor_next_day']*0
    
    for isec in ManualOverride['msci_security_code']:
        RowOverride = (ManualOverride.loc[ManualOverride['msci_security_code']==isec])
        for icol in RowOverride.columns[4:]:
            if not(RowOverride[icol].isna().bool()):
                print(RowOverride[icol])
                print(All_SecurityData.loc[All_SecurityData['msci_security_code']==isec,icol])
                OldValue = All_SecurityData.loc[All_SecurityData['msci_security_code']==isec,icol] 
                NewValue = RowOverride[icol].iloc[0]
            
                if(icol=='eod_number_of_shares_next_day'):
                    tempRatio = NewValue / OldValue
                    All_SecurityData.loc[All_SecurityData['msci_security_code']==isec,'initial_mkt_cap_usd_next_day']  = All_SecurityData.loc[All_SecurityData['msci_security_code']==isec,'initial_mkt_cap_usd_next_day']  * tempRatio
                    All_SecurityData.loc[All_SecurityData['msci_security_code']==isec,'eod_number_of_shares_next_day']  = NewValue
                
                if(icol=='foreign_inc_factor_next_day'):
                    tempRatio = NewValue / OldValue
                    All_SecurityData.loc[All_SecurityData['msci_security_code']==isec,'FF_MktCap_usd']  = All_SecurityData.loc[All_SecurityData['msci_security_code']==isec,'FF_MktCap_usd']  * tempRatio
                    All_SecurityData.loc[All_SecurityData['msci_security_code']==isec,'foreign_inc_factor_next_day']  = NewValue
                    
                if((icol=='12mATVR') | (icol=='3m_Q-4') | (icol=='3m_Q-3') | (icol=='3m_Q-2') | (icol=='3m_Q-1')):
                    All_SecurityData.loc[All_SecurityData['msci_security_code']==isec,icol]  = NewValue
                    
        if ( (~(RowOverride['manual_exclude'].isna()) & (RowOverride['manual_exclude']==1)).bool() ):
            All_SecurityData = All_SecurityData.loc[All_SecurityData['msci_security_code']!=isec]
            
        All_SecurityData.loc[All_SecurityData['msci_security_code']==isec,'Manual_Override'] = 1
   
                
    return  All_SecurityData

