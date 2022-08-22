# -*- coding: utf-8 -*-
"""
@author: mabboud
"""
import pandas as pd
import numpy as np
import pandas as pd
from KDBData.data import DataQuery
from pandas.tseries.offsets import BDay
from scipy.stats import norm

def GenerateForecastMonitor(OutputUpdateSegmNbComp,OutputFinalInvestability,CalcDate2):
    
    
    OutputFinalMonitor= [] 
    
    for mkt in OutputFinalInvestability.keys():
        print(mkt)
        Market_SecurityData =  OutputFinalInvestability[mkt]['Market_SecurityData']
        Market_SecurityData = Market_SecurityData.sort_values('company_full_mktcap', ascending=False)
        Market_SecurityData= Market_SecurityData.drop_duplicates(subset=['msci_security_code']) 
    
        SAIR_MSnbC = OutputUpdateSegmNbComp[mkt]['FinalData'].loc['FinalMSnbC'].iloc[0]
        
        Market_SecurityData['Final_Monitor1']=''
        Market_SecurityData['Final_Monitor2']=''
        Market_SecurityData['Dist1_Conf']=''
        Market_SecurityData['Dist2_Conf']=''
        Market_SecurityData['Dist_Conf']=''        
        Market_SecurityData['FinalFF_Conf']=''
        Market_SecurityData['BalanceAdd_Conf']=''        
        Market_SecurityData['BalanceDel_Conf']=''
        Market_SecurityData['Final_Conf']=''
        Market_SecurityData['FinalFFAdj_Conf']=''
        
        BufferLimit = 0.2
    
        #Monitor1
        BufferAdds_List = ( (Market_SecurityData['Status']!='STD')  & (Market_SecurityData['Dist_1']<=BufferLimit) & (Market_SecurityData['Final_FF_Dist']<=BufferLimit) & (Market_SecurityData['China_Final_test']==1)).tolist()
        Market_SecurityData.loc[BufferAdds_List,'Final_Monitor1']='Buffer_Add'
        
        BufferDels_List = ( (Market_SecurityData['Status']=='STD')  & ( (Market_SecurityData['Dist_2']>=-BufferLimit) | (Market_SecurityData['Final_FF_Dist']>=-BufferLimit) )).tolist()
        Market_SecurityData.loc[BufferDels_List,'Final_Monitor1']='Buffer_Del'
        
        Adds_List = ( (Market_SecurityData['Status']!='STD')  & (Market_SecurityData['Iter_Rank']<=SAIR_MSnbC) & (Market_SecurityData['Final_Test']==1) & (Market_SecurityData['All_BasicTest']==1)).tolist()
        Market_SecurityData.loc[Adds_List,'Final_Monitor1']='Add'
        
        Dels_List = ( (Market_SecurityData['Status']=='STD')  & ((np.isnan(Market_SecurityData['Iter_Rank'])) | (Market_SecurityData['Iter_Rank']> SAIR_MSnbC) | (Market_SecurityData['Final_Test']==0)) ).tolist()
        Market_SecurityData.loc[Dels_List,'Final_Monitor1']='Del'
        
        #Monitor2
        CloseAutoAdds_List = ( (Market_SecurityData['Status']!='STD')  & (Market_SecurityData['Dist_1']<=BufferLimit) & (Market_SecurityData['Final_FF_Dist']<=BufferLimit) & (Market_SecurityData['All_BasicTest']==1) & (Market_SecurityData['China_Final_test']==1) ).tolist()
        Market_SecurityData.loc[CloseAutoAdds_List,'Final_Monitor2']='Auto_Add'
        
        CloseAutoDels_List = ( (Market_SecurityData['Status']=='STD')  & (Market_SecurityData['Dist_2']>=-BufferLimit) ).tolist()
        Market_SecurityData.loc[CloseAutoDels_List,'Final_Monitor2']='Auto_Del'
        
        FinalDels_List = ( (Market_SecurityData['Status']=='STD')  & (Market_SecurityData['Final_FF_Dist']>=-BufferLimit) ).tolist()
        Market_SecurityData.loc[FinalDels_List,'Final_Monitor2']= Market_SecurityData.loc[FinalDels_List,'Final_Monitor2'] + ',' + 'Final_Del'
        
        FinalAdds_List = ( (Market_SecurityData['Status']!='STD')  & (Market_SecurityData['Final_FF_Dist']<=BufferLimit) & (Market_SecurityData['All_BasicTest']==1) ).tolist()
        Market_SecurityData.loc[FinalAdds_List,'Final_Monitor2']= Market_SecurityData.loc[FinalAdds_List,'Final_Monitor2'] + ',' + 'Final_Add'
        
        BasicAdds_List = ( (Market_SecurityData['Status']!='STD')  & (Market_SecurityData['Dist_1']<=BufferLimit) & (Market_SecurityData['Final_FF_Dist']<=BufferLimit) & (Market_SecurityData['All_BasicTest']==0) ).tolist()
        Market_SecurityData.loc[BasicAdds_List,'Final_Monitor2']= Market_SecurityData.loc[BasicAdds_List,'Final_Monitor2'] + ',' + 'Fail_Basic'  
              
        
        #Calculate confidence (03/08/22)
        #########################################################################################
        #1-calulate dist conf from distance to cutoff
        #2 caluclate balance add/del confidence based on excel sheet in folder input
        #3 Calculate Final Conf based on Dist and Balancing
        #4 Finally Adjust for confidence of FreeFloat
        
        
        
        
        Market_SecurityData['Dist1_Conf'] = norm.cdf(Market_SecurityData['Dist_1'],0,0.2)
        Market_SecurityData['Dist2_Conf'] = norm.cdf(Market_SecurityData['Dist_2'],0,0.2)
        Market_SecurityData['FinalFF_Conf'] = norm.cdf(Market_SecurityData['Final_FF_Dist'],0,0.2)
        
        Market_SecurityData.loc[(Market_SecurityData['Status']!='STD') ,'Dist_Conf']= 1 - Market_SecurityData.loc[(Market_SecurityData['Status']!='STD') ,'Dist1_Conf'] 
        Market_SecurityData.loc[(Market_SecurityData['Status']=='STD') ,'Dist_Conf']= Market_SecurityData.loc[(Market_SecurityData['Status']=='STD') ,'Dist2_Conf']
        Market_SecurityData.loc[(Market_SecurityData['Status']!='STD') ,'FinalFF_Conf']= 1 - Market_SecurityData.loc[(Market_SecurityData['Status']!='STD') ,'FinalFF_Conf']
        Market_SecurityData.loc[(Market_SecurityData['Status']=='STD') ,'FinalFF_Conf']= Market_SecurityData.loc[(Market_SecurityData['Status']=='STD') ,'FinalFF_Conf']

        # Balancing Confidence      
        Market_SecurityData = Market_SecurityData.groupby(['Status'], as_index=False, sort=False, group_keys=False) \
        .apply(lambda y:y\
               .assign(RankSTD=lambda x:x['company_full_mktcap'].rank(method='dense',ascending=True))\
               .assign(RankSML=lambda x:x['company_full_mktcap'].rank(method='dense',ascending=False))
                       )
        
        Market_SecurityData['BalanceAdd_Conf']=Market_SecurityData['Dist_Conf']     
        Market_SecurityData['BalanceDel_Conf']=Market_SecurityData['Dist_Conf']  
        Market_SecurityData['Final_Conf']=Market_SecurityData['Dist_Conf']  
        
        
        #BalancingNumber = np.maximum(np.sum((Market_SecurityData['Status']=='SML')  & (Market_SecurityData['Dist_1']<=BufferLimit) ),np.sum((Market_SecurityData['Status']=='STD')  & (Market_SecurityData['Dist_2']>=-BufferLimit) ))
        #minNb = np.minimum(np.sum((Market_SecurityData['Status']=='SML')),np.sum((Market_SecurityData['Status']=='STD')))
        #if minNb < BalancingNumber:
        #    BalancingNumber=minNb
        BalancingNumber = int(np.minimum(Market_SecurityData.loc[(Market_SecurityData['Status']=='STD') ,'RankSTD'].iloc[0],Market_SecurityData.loc[(Market_SecurityData['Status']=='SML') ,'RankSML'].iloc[-1]))
                
        
        if BalancingNumber>=1:
            Market_SecurityData.loc[(Market_SecurityData['Status']=='SML') & (Market_SecurityData['RankSML']==1),'BalanceAdd_Conf'] = \
                Market_SecurityData.loc[(Market_SecurityData['Status']=='STD') & (Market_SecurityData['RankSTD']==1),'Dist_Conf'].iloc[0]
            Market_SecurityData.loc[(Market_SecurityData['Status']=='STD') & (Market_SecurityData['RankSTD']==1),'BalanceDel_Conf'] = \
                Market_SecurityData.loc[(Market_SecurityData['Status']=='SML') & (Market_SecurityData['RankSML']==1),'Dist_Conf'].iloc[0]
                                         
            # dirty fix for russia for now
            toprow1sml = Market_SecurityData.loc[(Market_SecurityData['Status']=='SML') & (Market_SecurityData['RankSML']==1),'Dist_Conf'].iloc[0]
            toprow1std = Market_SecurityData.loc[(Market_SecurityData['Status']=='STD') & (Market_SecurityData['RankSTD']==1),'Dist_Conf'].iloc[0]
            
            if (( BalancingNumber>=2) & (toprow1sml>0) & (toprow1std >0)):    
                for i in range(2,BalancingNumber+1):
                    #print(i)
                    Market_SecurityData.loc[(Market_SecurityData['Status']=='SML') & (Market_SecurityData['RankSML']==i),'BalanceAdd_Conf'] = \
                             np.maximum(Market_SecurityData.loc[(Market_SecurityData['Status']=='STD') & (Market_SecurityData['RankSTD']==i),'Dist_Conf'].iloc[0] , \
                                                  (Market_SecurityData.loc[(Market_SecurityData['Status']=='SML') & (Market_SecurityData['RankSML']==i),'Dist_Conf'].iloc[0] / \
                                                   Market_SecurityData.loc[(Market_SecurityData['Status']=='SML') & (Market_SecurityData['RankSML']==1),'Dist_Conf'].iloc[0] * \
                                                   Market_SecurityData.loc[(Market_SecurityData['Status']=='SML') & (Market_SecurityData['RankSML']==1),'BalanceAdd_Conf'].iloc[0]))
                         
                    Market_SecurityData.loc[(Market_SecurityData['Status']=='STD') & (Market_SecurityData['RankSTD']==i),'BalanceDel_Conf'] = \
                             np.maximum(Market_SecurityData.loc[(Market_SecurityData['Status']=='SML') & (Market_SecurityData['RankSML']==i),'Dist_Conf'].iloc[0] , \
                                                  (Market_SecurityData.loc[(Market_SecurityData['Status']=='STD') & (Market_SecurityData['RankSTD']==i),'Dist_Conf'].iloc[0] / \
                                                   Market_SecurityData.loc[(Market_SecurityData['Status']=='STD') & (Market_SecurityData['RankSTD']==1),'Dist_Conf'].iloc[0] * \
                                                   Market_SecurityData.loc[(Market_SecurityData['Status']=='STD') & (Market_SecurityData['RankSTD']==1),'BalanceDel_Conf'].iloc[0]))
                            
            for i in range(1,BalancingNumber+1):
               
                if (Market_SecurityData.loc[(Market_SecurityData['Status']=='SML') & (Market_SecurityData['RankSML']==i),'Dist_Conf'].iloc[0] > \
                                            Market_SecurityData.loc[(Market_SecurityData['Status']=='SML') & (Market_SecurityData['RankSML']==i),'BalanceAdd_Conf'].iloc[0]):
                    Market_SecurityData.loc[(Market_SecurityData['Status']=='SML') & (Market_SecurityData['RankSML']==i),'Final_Conf'] = \
                    Market_SecurityData.loc[(Market_SecurityData['Status']=='SML') & (Market_SecurityData['RankSML']==i),'Dist_Conf'].iloc[0]
                else:
                    Market_SecurityData.loc[(Market_SecurityData['Status']=='SML') & (Market_SecurityData['RankSML']==i),'Final_Conf'] = \
                                                (Market_SecurityData.loc[(Market_SecurityData['Status']=='SML') & (Market_SecurityData['RankSML']==i),'Dist_Conf'].iloc[0] + \
                                                        Market_SecurityData.loc[(Market_SecurityData['Status']=='SML') & (Market_SecurityData['RankSML']==i),'BalanceAdd_Conf'].iloc[0]) / 2
                                            
                if (Market_SecurityData.loc[(Market_SecurityData['Status']=='STD') & (Market_SecurityData['RankSTD']==i),'Dist_Conf'].iloc[0] > \
                                            Market_SecurityData.loc[(Market_SecurityData['Status']=='STD') & (Market_SecurityData['RankSTD']==i),'BalanceDel_Conf'].iloc[0]):
                    Market_SecurityData.loc[(Market_SecurityData['Status']=='STD') & (Market_SecurityData['RankSTD']==i),'Final_Conf'] = \
                    Market_SecurityData.loc[(Market_SecurityData['Status']=='STD') & (Market_SecurityData['RankSTD']==i),'Dist_Conf'].iloc[0]
                else:
                    Market_SecurityData.loc[(Market_SecurityData['Status']=='STD') & (Market_SecurityData['RankSTD']==i),'Final_Conf'] = \
                                              (Market_SecurityData.loc[(Market_SecurityData['Status']=='STD') & (Market_SecurityData['RankSTD']==i),'Dist_Conf'].iloc[0] + \
                                                        Market_SecurityData.loc[(Market_SecurityData['Status']=='STD') & (Market_SecurityData['RankSTD']==i),'BalanceDel_Conf'].iloc[0])  /2                           
        # for basic fails force to 10% confidence        
        BasicAdds_List = ( (Market_SecurityData['Status']!='STD')  & (Market_SecurityData['Dist_1']<=BufferLimit) & (Market_SecurityData['Final_FF_Dist']<=BufferLimit) & (Market_SecurityData['All_BasicTest']==0) ).tolist()       
        Market_SecurityData.loc[ BasicAdds_List,'Final_Conf'] = 0.1
        
        Market_SecurityData.loc[(Market_SecurityData['Status']!='STD') ,'FinalFFAdj_Conf']= np.minimum(Market_SecurityData.loc[(Market_SecurityData['Status']!='STD') ,'Final_Conf'],Market_SecurityData.loc[(Market_SecurityData['Status']!='STD') ,'FinalFF_Conf'])
        Market_SecurityData.loc[(Market_SecurityData['Status']=='STD') ,'FinalFFAdj_Conf']= np.maximum(Market_SecurityData.loc[(Market_SecurityData['Status']=='STD') ,'Final_Conf'],Market_SecurityData.loc[(Market_SecurityData['Status']=='STD') ,'FinalFF_Conf'])
    
    ######################################################################################### 
        
        #Add fx
        Market_SecurityData['FX']= Market_SecurityData['initial_mkt_cap_usd_next_day']/Market_SecurityData['initial_mkt_cap_loc_next_day']
        
        OutputFinalInvestability[mkt]['Market_SecurityData'] = Market_SecurityData 
        
        Market_SecurityData =  Market_SecurityData[['security_name','msci_security_code','isin','eod_number_of_shares_next_day','foreign_inc_factor_next_day','ISO_country_symbol_next_day','price_ISO_ccy_symbol_next_day','initial_mkt_cap_usd_next_day','RIC','bb_ticker','sedol_next_day','Status','Size_Cap','Market','Region','FF_MktCap_usd','FX','Interim_MSSC','Interim_MSnbC','Iter','Iter_Rank','Dist_1','Dist_2','Final_FF_Dist','Final_Monitor1','Final_Monitor2','Dist_Conf','FinalFF_Conf','BalanceAdd_Conf','BalanceDel_Conf','Final_Conf','FinalFFAdj_Conf']]
        Market_SecurityData = Market_SecurityData[(Market_SecurityData['Final_Monitor1']!='') | (Market_SecurityData['Final_Conf']>0.05)]
                
        OutputFinalMonitor.append(Market_SecurityData)
    
    OutputFinalMonitorDF = pd.concat(OutputFinalMonitor)
    
    # adjusting for volatility. only 20% weight for TGT
    DF_BARRA = get_barra_data(CalcDate2,OutputFinalMonitorDF['sedol_next_day'])
    DF_BARRA = DF_BARRA[['sedol','specific_risk']]
    DF_BARRA['specific_risk']=DF_BARRA['specific_risk'].fillna(DF_BARRA['specific_risk'].mean()) 
    DF_BARRA['sedol_next_day']=DF_BARRA['sedol']
    df_specrisk = 1/((DF_BARRA['specific_risk']) / np.std(DF_BARRA['specific_risk']))
    risk_adj = norm.cdf(df_specrisk,df_specrisk.mean(),0.1)
    risk_adj = 0.8 + 0.1 * risk_adj
    DF_BARRA['RiskAdjFactor20%'] =  risk_adj   
    OutputFinalMonitorDF =  OutputFinalMonitorDF.merge(DF_BARRA,on='sedol_next_day',how='left')   
    OutputFinalMonitorDF=OutputFinalMonitorDF.drop('sedol',axis=1)
      
    
    DF_ADV = get_fast_adv(OutputFinalMonitorDF['sedol_next_day'],CalcDate2)
    DF_ADV=DF_ADV[['idSedol1','adtM']]
    DF_ADV['sedol_next_day']=DF_ADV['idSedol1']
    OutputFinalMonitorDF =  OutputFinalMonitorDF.merge(DF_ADV,on='sedol_next_day',how='left')   
    OutputFinalMonitorDF=OutputFinalMonitorDF.drop('idSedol1',axis=1)
    OutputFinalMonitorDF['adtM$']=OutputFinalMonitorDF['adtM']*OutputFinalMonitorDF['FX']

    penceList = (OutputFinalMonitorDF['ISO_country_symbol_next_day']=='GB') | \
    (OutputFinalMonitorDF['price_ISO_ccy_symbol_next_day']=='ILS') | \
    (OutputFinalMonitorDF['ISO_country_symbol_next_day']=='ZA') | \
    (OutputFinalMonitorDF['ISO_country_symbol_next_day']=='KW')
    OutputFinalMonitorDF.loc[penceList,'adtM$'] = OutputFinalMonitorDF.loc[penceList,'adtM$']/100
    OutputFinalMonitorDF['TrackingAss']=0.06
    EuropeIMI = (OutputFinalMonitorDF['Market']=='EUROPE') & ((OutputFinalMonitorDF['Status']=='STD') | (OutputFinalMonitorDF['Status']=='SML'))
    OutputFinalMonitorDF.loc[EuropeIMI,'TrackingAss']=0.045
    AmericaIMI = (OutputFinalMonitorDF['Market']=='USA') | (OutputFinalMonitorDF['Market']=='CANADA')
    OutputFinalMonitorDF.loc[AmericaIMI,'TrackingAss']=0.02
    OutputFinalMonitorDF['VTT']=OutputFinalMonitorDF['FF_MktCap_usd']*OutputFinalMonitorDF['TrackingAss']/1e+6
    OutputFinalMonitorDF['DTT']=OutputFinalMonitorDF['VTT']/OutputFinalMonitorDF['adtM$']
    OutputFinalMonitorDF['TGT']=OutputFinalMonitorDF['VTT'] * 0.03 * OutputFinalMonitorDF['FinalFFAdj_Conf']
    OutputFinalMonitorDF.loc[(OutputFinalMonitorDF['Status']!='STD') ,'TGT'] =  0.03 * OutputFinalMonitorDF.loc[(OutputFinalMonitorDF['Status']!='STD') ,'VTT'] * \
                                                                                OutputFinalMonitorDF.loc[(OutputFinalMonitorDF['Status']!='STD') ,'FinalFFAdj_Conf']
    
    OutputFinalMonitorDF.loc[(OutputFinalMonitorDF['Status']=='STD') ,'TGT'] =  -0.01 * OutputFinalMonitorDF.loc[(OutputFinalMonitorDF['Status']=='STD') ,'VTT'] * \
                                                                                OutputFinalMonitorDF.loc[(OutputFinalMonitorDF['Status']=='STD') ,'FinalFFAdj_Conf']
    OutputFinalMonitorDF['TGTAdj']=OutputFinalMonitorDF['TGT'] * OutputFinalMonitorDF['RiskAdjFactor20%']
    
    
    OutputFinalMonitorDF = OutputFinalMonitorDF[(OutputFinalMonitorDF['FinalFFAdj_Conf']>0.05) | (OutputFinalMonitorDF['TGT']>0.15)]
    
    return OutputFinalMonitorDF




def get_fast_adv(ids,eDate):
    m = DataQuery()
    advStart=pd.to_datetime(eDate)-BDay(20)
    context = {
        'sdate': m.tokdbdate(advStart),
        'edate': m.tokdbdate(eDate),
        'sedols':m.joinsymbols(ids),
    }
    tquery = (
            '0!select last date,adtM:avg primVolume,vol:15.8*dev (pxLast- prev pxLast)%pxLast,last pxLast,last tickerAndExchCode,last crncy by idSedol1 from '
             '0!select primVolume:first volumeLoc,first pxLast,first tickerAndExchCode,first crncy by date,idSedol1 from '+
              '`isPrimary`volumeLoc xdesc '+
              'select date,idSedol1,pxLast,volumeLoc:(pxVolume*pxLast)%1e6,tickerAndExchCode,crncy,'+
              'isPrimary:1.0*(idBbPrimSecurityFlag="Y") '+
              'from bboEquityPricingData_dl '+
              'where date within {{sdate}} {{edate}}'+
              ',idSedol1 in {{sedols|safe}},lastUpdate>0t')
    df = m.contquery(context, tquery,debug=True)
    df['idSedol1']=df['idSedol1'].astype(str)
    return df

def get_barra_data(date=pd.to_datetime('2022-08-09'),sedols=['7101069','BH4HKS3']):
    """universe is merging many barra tables using the barra_id"""
    m = DataQuery()
    context = {
        'edate': m.tokdbdate(date),
        'sdate': m.tokdbdate(date),
        'model': 'gem3s',
        'sedols' :m.joinsymbols(sedols),
        'filter' :sedols is not None,
    }
    tquery = (
            'masterdf:select from estu_{{model}} where date within {{sdate}} {{edate}}{%if filter%},sedol in {{sedols|safe}}{%endif%}; '+
            '{%if filter%}barraids:exec distinct barra_id from masterdf;{%endif%}'
            'pricedf:select from asset_price_{{model}} where date within {{sdate}} {{edate}}{%if filter%},barra_id in barraids{%endif%}; '+
            'assetdf:select from asset_data_{{model}} where date within {{sdate}} {{edate}}{%if filter%},barra_id in barraids{%endif%}; '+
            'res0:0!(`date`barra_id xkey masterdf) lj (`date`barra_id xkey pricedf); '+
            'res1:0!(`date`barra_id xkey res0) lj (`date`barra_id xkey assetdf); '+
            'res1')
    df = m.contquery(context,tquery)
    df =df.rename(columns={'sym' :'ticker' ,'ccy' :'crncy'})
    return df



def GetTrackingAss ():
    TrackinAss = pd.DataFrame({'ISO_Country':['AU','CN','HK','ID','IN','JP','KR','MY','NZ','PH','PK','SG','TH','TW','CA','AT','BE','CH','CZ','DE','DK','ES','FI','FR','GB','GR','HU','IE','IT','LU','NL','NO','PL','PT','RU','SE','AR','BR','CL','CO','MX','PE','VE','EG','IL','JO','LK','MA','TR','ZA','US','AE','BH','KW','OM','QA','SA','XA','BG','EE','HR','KE','KZ','LB','MU','NG','RO','SI','TN','UA','VN','BA','BD','BW','GH','JM','LT','RS','TT','ZW','SN','PS','BF','PS','CI','BF','CI','PS','PS','BJ','NA','MK'], \
            
            'Market': ['AUSTRALIA','CHINA','HONG KONG','INDONESIA','INDIA','JAPAN','KOREA','MALAYSIA','NEW ZEALAND','PHILIPPINES','PAKISTAN','SINGAPORE','THAILAND','TAIWAN','CANADA','EUROPE','EUROPE','EUROPE','CZECH REPUBLIC','EUROPE','EUROPE','EUROPE','EUROPE','EUROPE','EUROPE','GREECE','HUNGARY','EUROPE','EUROPE','OTHER','EUROPE','EUROPE','POLAND','EUROPE','RUSSIA','EUROPE','ARGENTINA','BRAZIL','CHILE','COLOMBIA','MEXICO','PERU','OTHER','EGYPT','ISRAEL','OTHER','OTHER','OTHER','TURKEY','SOUTH AFRICA','USA','UNITED ARAB EMIRATES','OTHER','OTHER','OTHER','QATAR','SAUDI ARABIA','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER'], \
            
            'Tracking': [0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0,0.05,0.05,0,0,0,0.05,0.05,0.05,0.05,0,0,0,0.05,0.05,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0], \
            
            'Region': ['DM','EM','DM','EM','EM','DM','EM','EM','DM','EM','EM','DM','EM','EM','DM','DM','DM','DM','EM','DM','DM','DM','DM','DM','DM','EM','EM','DM','DM','NONE','DM','DM','EM','DM','EM','DM','EM','EM','EM','EM','EM','EM','NONE','EM','DM','NONE','NONE','NONE','EM','EM','DM','EM','NONE','NONE','NONE','EM','EM','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE'] })

    return TrackinAss

