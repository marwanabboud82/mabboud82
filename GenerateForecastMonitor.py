# -*- coding: utf-8 -*-
"""
@author: mabboud
"""
import pandas as pd
import numpy as np

def GenerateForecastMonitor(OutputUpdateSegmNbComp,OutputFinalInvestability):
    
    
    OutputFinalMonitor= [] 
    
    for mkt in OutputFinalInvestability.keys():
    
        Market_SecurityData =  OutputFinalInvestability[mkt]['Market_SecurityData']
    
        SAIR_MSnbC = OutputUpdateSegmNbComp[mkt]['FinalData'].loc['FinalMSnbC'].iloc[0]
        
        Market_SecurityData['Final_Monitor1']=''
        Market_SecurityData['Final_Monitor2']=''
        
        BufferLimit = 0.15
    
        #Monitor1
        BufferAdds_List = ( (Market_SecurityData['Status']!='STD')  & (Market_SecurityData['Dist_1']<=BufferLimit) & (Market_SecurityData['Final_FF_Dist']<=BufferLimit) ).tolist()
        Market_SecurityData.loc[BufferAdds_List,'Final_Monitor1']='Buffer_Add'
        
        BufferDels_List = ( (Market_SecurityData['Status']=='STD')  & ( (Market_SecurityData['Dist_2']>=-BufferLimit) | (Market_SecurityData['Final_FF_Dist']>=-BufferLimit) )).tolist()
        Market_SecurityData.loc[BufferDels_List,'Final_Monitor1']='Buffer_Del'
        
        Adds_List = ( (Market_SecurityData['Status']!='STD')  & (Market_SecurityData['Iter_Rank']<=SAIR_MSnbC) & (Market_SecurityData['Final_Test']==1) & (Market_SecurityData['All_BasicTest']==1)).tolist()
        Market_SecurityData.loc[Adds_List,'Final_Monitor1']='Add'
        
        Dels_List = ( (Market_SecurityData['Status']=='STD')  & ((np.isnan(Market_SecurityData['Iter_Rank'])) | (Market_SecurityData['Iter_Rank']> SAIR_MSnbC) | (Market_SecurityData['Final_Test']==0)) ).tolist()
        Market_SecurityData.loc[Dels_List,'Final_Monitor1']='Del'
        
        #Monitor2
        CloseAutoAdds_List = ( (Market_SecurityData['Status']!='STD')  & (Market_SecurityData['Dist_1']<=BufferLimit) & (Market_SecurityData['Final_FF_Dist']<=BufferLimit) & (Market_SecurityData['All_BasicTest']==1) ).tolist()
        Market_SecurityData.loc[CloseAutoAdds_List,'Final_Monitor2']='Auto_Add'
        
        CloseAutoDels_List = ( (Market_SecurityData['Status']=='STD')  & (Market_SecurityData['Dist_2']>=-BufferLimit) ).tolist()
        Market_SecurityData.loc[CloseAutoDels_List,'Final_Monitor2']='Auto_Del'
        
        FinalDels_List = ( (Market_SecurityData['Status']=='STD')  & (Market_SecurityData['Final_FF_Dist']>=-BufferLimit) ).tolist()
        Market_SecurityData.loc[FinalDels_List,'Final_Monitor2']= Market_SecurityData.loc[FinalDels_List,'Final_Monitor2'] + ',' + 'Final_Del'
        
        FinalAdds_List = ( (Market_SecurityData['Status']!='STD')  & (Market_SecurityData['Final_FF_Dist']<=BufferLimit) & (Market_SecurityData['All_BasicTest']==1) ).tolist()
        Market_SecurityData.loc[FinalAdds_List,'Final_Monitor2']= Market_SecurityData.loc[FinalAdds_List,'Final_Monitor2'] + ',' + 'Final_Add'
        
        BasicAdds_List = ( (Market_SecurityData['Status']!='STD')  & (Market_SecurityData['Dist_1']<=BufferLimit) & (Market_SecurityData['Final_FF_Dist']<=BufferLimit) & (Market_SecurityData['All_BasicTest']==0) ).tolist()
        Market_SecurityData.loc[BasicAdds_List,'Final_Monitor2']= Market_SecurityData.loc[BasicAdds_List,'Final_Monitor2'] + ',' + 'Fail_Basic'  
              
        Market_SecurityData =  Market_SecurityData[['security_name','msci_security_code','isin','eod_number_of_shares_next_day','foreign_inc_factor_next_day','ISO_country_symbol_next_day','initial_mkt_cap_usd_next_day','RIC','bb_ticker','sedol_next_day','Status','Size_Cap','Market','Region','FF_MktCap_usd','Interim_MSSC','Interim_MSnbC','Iter','Iter_Rank','Dist_1','Dist_2','Final_FF_Dist','Final_Monitor1','Final_Monitor2']]
    
        Market_SecurityData = Market_SecurityData[Market_SecurityData['Final_Monitor1']!='']
        
        OutputFinalMonitor.append(Market_SecurityData)
    
    OutputFinalMonitorDF = pd.concat(OutputFinalMonitor)
    
    
        
   
    return OutputFinalMonitorDF

