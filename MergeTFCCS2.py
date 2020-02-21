# -*- coding: utf-8 -*-
"""
@author: mabboud
"""
import pandas as pd
import numpy as np

def MergeTFCCS2 (bbo,TrackerFilePath,Clean_TrackerData,CCS_Data,CntryMap):

    # Add Barra ID if exists to TF   
  
    Clean_TrackerData['msci_security_code'] =     Clean_TrackerData['msci_security_code'].astype(str)
    CCS_Data['MSCI Security Code'] =     CCS_Data['MSCI Security Code'].astype(str)
    
    Temp_Clean_TrackerData = Clean_TrackerData.merge(CCS_Data[['MSCI Security Code','Barra Id']],how='left',left_on='msci_security_code',right_on='MSCI Security Code')
    Temp_Clean_TrackerData.drop(['MSCI Security Code'], inplace=True, axis=1)
    Temp_Clean_TrackerData.rename(columns={'Barra Id': 'Barra_Id'},inplace=True)
    Temp_Clean_TrackerData['InTF']=1
    
    
    # Fill the columns that exist in CCS
    TempDF= pd.DataFrame(columns=Temp_Clean_TrackerData.columns, index=CCS_Data.index)
    TempDF['calc_date']=CCS_Data['Calculation Date']
    TempDF['security_name']=CCS_Data['Security Name']
    TempDF['msci_security_code']=CCS_Data['MSCI Security Code']
    TempDF['sedol_next_day']=CCS_Data['Sedol code']
    TempDF['isin']=CCS_Data['Isin']
    TempDF['bb_ticker']=CCS_Data['Bloomberg code']
    TempDF['ISO_country_symbol_next_day']=CCS_Data['ISO Country Symbol']
    TempDF['Barra_Id']= CCS_Data['Barra Id']
    TempDF['InTF']= 0
    
    InCCSNotInTF = ~ TempDF['msci_security_code'].isin(Temp_Clean_TrackerData['msci_security_code'])
    
    TempDF = TempDF[InCCSNotInTF]
    ccsstartnb = TempDF.shape[0]
    print('CCS Start',ccsstartnb)
    
    ### CLEAN CCS
    
    # barra master for CrossList
    filebarra = (TrackerFilePath.loc['DMSTDPath'] + 'CCS\\barra_master_gemtr_20200204.pkl')
    barramaster= pd.read_pickle(filebarra.iloc[0],compression='zip') 
    tmpbarra=barramaster.loc[(barramaster['barra_id'].isin(TempDF['Barra_Id']).astype(bool))]
    CrossListChk = tmpbarra.loc[tmpbarra['instrument']=='CROSS_LIST','barra_id']
    CrossListChk = CrossListChk.loc[CrossListChk!=""].tolist()
    BadCrossList = (TempDF['Barra_Id'].apply(lambda x: any([k in x for k in CrossListChk])))
    print('CrossList', sum(BadCrossList))
    TempDF = TempDF.loc[~(BadCrossList)]
    
    # tmptfbarra=barramaster.loc[(barramaster['barra_id'].isin(Temp_Clean_TrackerData['Barra_Id']).astype(bool))]
    
    # barra master for GDR and ADR
    GDRListChk = tmpbarra.loc[((tmpbarra['instrument']=='ADR') | (tmpbarra['instrument']=='GDR')),'barra_id']
    GDRListChk = GDRListChk.loc[GDRListChk!=""].tolist()
    BadGDRList = (TempDF['Barra_Id'].apply(lambda x: any([k in x for k in GDRListChk])))
    AllowedForeign = ['AR','IL','RU','CN','NL','HK']
    AllowedForeignBool = (TempDF['ISO_country_symbol_next_day'].apply(lambda x: any([k in x for k in AllowedForeign]))) 
    print('ADR_GDR_1', sum((BadGDRList & ~(AllowedForeignBool))))
    TempDF = TempDF.loc[~(BadGDRList & ~(AllowedForeignBool))]
    
    # barra country to iso country match
    GoodMappingCheck = ['AT_AUT','AU_AUS','BE_BEL','BE_NLD','CA_CAN','CH_CHE','DE_DEU','DK_DNK','ES_ESP','FI_FIN','FR_FRA','FR_NLD','GB_GBR','HK_HKG','HK_NOR','HK_SGP','HK_USA','IE_IRL','IL_AUS','IL_BEL','IL_HKG','IL_ISR','IL_SGP','IL_SWE','IL_GBR','IL_USA','IT_ITA','JP_JPN','NL_NLD','NL_USA','NO_NOR','NZ_NZL','PT_PRT','SE_SWE','SG_SGP','US_USA','AE_ARE','AR_USA','BR_BRA','CL_CHL','CN_CHN','CN_HKG','CN_SGP','CN_USA','CO_COL','CZ_CZE','EG_EGY','GR_GRC','HU_HUN','ID_IDN','IN_IND','KR_KOR','MX_MEX','MY_MYS','PE_USA','PH_PHL','PK_PAK','PL_POL','QA_QAT','RU_RUS','RU_GBR','RU_USA','SA_SAU','TH_THA','TR_TUR','TW_TWN','ZA_ZAF']    
    Temp_CCS_BarraCntry = TempDF.merge(tmpbarra[['barra_id','country']],how='left',left_on='Barra_Id',right_on='barra_id')
    # remove the ones not in barra they re either delisted aquired etc....
    xxx = ((Temp_CCS_BarraCntry['country']!="") & (~(Temp_CCS_BarraCntry['country'].isna())))
    Temp_CCS_BarraCntry=Temp_CCS_BarraCntry.loc[xxx]
    TempDF = Temp_CCS_BarraCntry.loc[xxx]    
    Temp_CCS_BarraCntryStr = Temp_CCS_BarraCntry['ISO_country_symbol_next_day'] +'_' +  Temp_CCS_BarraCntry['country']
    BadMappingCntry = ~(Temp_CCS_BarraCntryStr.apply(lambda x: any([k in x for k in GoodMappingCheck])))
    print('BadMappingCntrys' , sum(BadMappingCntry))
    TempDF = TempDF.loc[~BadMappingCntry]

    
    # only markets we need
    mktsDMEM = CntryMap.loc[CntryMap['Market']!='OTHER']
    BadMarkets=~(TempDF['ISO_country_symbol_next_day'].apply(lambda x: any([k in x for k in mktsDMEM['ISO_Country']])))
    print('MktWeNeed' , sum(BadMarkets))
    TempDF = TempDF.loc[~BadMarkets]
    
    #missing BBgs
    TempDF=TempDF.loc[TempDF['bb_ticker']!='']
    print('MissingBBs' , sum(TempDF['bb_ticker']==''))
    
    #missing barra
    TempDF=TempDF.loc[TempDF['Barra_Id']!='']
    print('MissingBarra' , sum(TempDF['Barra_Id']==''))
    
    # name bad string
    CheckCharacters=['%','PFD/ISSUE',' ETF',' ETC',' S&P',' FTSE',' ISHARES','ISHARES',' STOXX','SHRT',' SHORT',' INDX',' SHARES',' LYXOR', '-RTS', ' RTS', ' ETF ', ' RIGHT', ' RIGHTS', '-RIGHT', '-RIGHTS','DETACHED' ]
    BadCharacters = (TempDF['security_name'].apply(lambda x: any([k in x for k in CheckCharacters])))
    print('BadCharacters' , sum(BadCharacters))
    TempDF = TempDF.loc[~BadCharacters]
        
    #Chines Stocks with not usual exchanges 
    ChinesBadExch = ['AT Equity','UR Equity','MF Equity','LN Equity','KQ Equity','LI Equity','UA Equity','TT Equity','GY Equity','SE Equity']
    BadCNExch = (TempDF['bb_ticker'].apply(lambda x: any([k in x for k in ChinesBadExch])))
    print('BadCNExch' , sum(BadCNExch & (TempDF['ISO_country_symbol_next_day']=='CN')))
    TempDF = TempDF.loc[~(BadCNExch & (TempDF['ISO_country_symbol_next_day']=='CN'))]
    # Remove China A shares from CCS (note that they exist as shadow in TF)
    ChinaAInNames = [' A']
    IncludeChinaAInName = (TempDF['security_name'].str[-2:].apply(lambda x: any([k in x for k in ChinaAInNames]))) 
    print('ChinaANames' , sum(IncludeChinaAInName & (TempDF['ISO_country_symbol_next_day']=='CN')))
    TempDF = TempDF.loc[~(IncludeChinaAInName & (TempDF['ISO_country_symbol_next_day']=='CN'))]
    
    #Exchanges not in TF Exchanges
    CheckExch = ['AF Equity','PE Equity','BW Equity','IB Equity','SSALC1 PE','KZ Equity','RR Equity','LX Equity',' I UN Pfd',' J UN Pfd',' G UN Pfd',' A UN Pfd',' C UN Pfd',' Y UN Pfd',' X UN Pfd',' B UN Pfd','OO UQ Pfd',' E UN Pfd',' H UN Pfd','PP SJ Pfd']
    BadExch = (TempDF['bb_ticker'].apply(lambda x: any([k in x for k in CheckExch])))
    print('BadExch' , sum(BadExch))
    TempDF = TempDF.loc[~(BadExch)]
        
    # foreign listing
    ForeignExchInNames = ['(AE)','(AR)','(AU)','(BE)','(CA)','(CH)','(CL)','(CN)','(CO)','(DE)','(EG)','(ES)','(FR)','(GB)','(GR)','(HK)','(ID)','(IE)','(IL)','(IS)','(IT)','(JP)','(KR)','(KW)','(KZ)','(MX)','(MY)','(NG)','(NL)','(NO)','(NZ)','(PE)','(PH)','(PK)','(RU)','(SE)','(SG)','(US)','(ZA)']
    AllowedForeign = ['AR','IL','RU','CN','NL','HK']
    IncludeForeignExchangeInName = (TempDF['security_name'].str[-5:].apply(lambda x: any([k in x for k in ForeignExchInNames]))) 
    AllowedForeignBool = (TempDF['ISO_country_symbol_next_day'].apply(lambda x: any([k in x for k in AllowedForeign]))) 
    print('BadExch' , sum((IncludeForeignExchangeInName & ~(AllowedForeignBool))))
    TempDF = TempDF.loc[~(IncludeForeignExchangeInName & ~(AllowedForeignBool))]
    
    # any stock with marketcap from barra <100m$. lets remove
    Temp_CCS_Barra_MktCap = TempDF.merge(tmpbarra[['barra_id','capt','xrate']],how='left',left_on='Barra_Id',right_on='barra_id',left_index=True)
    Temp_CCS_Barra_MktCap['indexnb']= TempDF.index.tolist()
    Temp_CCS_Barra_MktCap.set_index('indexnb', inplace=True)
    Temp_CCS_Barra_MktCap.index.name = None
    Temp_CCS_Barra_MktCap['mktcap_m_usd'] = Temp_CCS_Barra_MktCap['capt'] * Temp_CCS_Barra_MktCap['xrate'] / 1e+6
    print('Lessthan100m$' , sum((Temp_CCS_Barra_MktCap['mktcap_m_usd']<100)))
    BadSmallMktCap =  ((Temp_CCS_Barra_MktCap['mktcap_m_usd']<100))
    BadSmallMktCap =BadSmallMktCap.rename('Barra_Id')
    TempDF = TempDF.loc[~BadSmallMktCap]
    
    
    
    # Any stock already in TF and have ADR in CCS. for now lets not consider the ADR. although ADR could replace sometimes if its more liquid
       
    
    # Temp_CCS_Barra_MktCap = TempDF.merge(tmpbarra[['barra_id','capt','xrate']],how='left',left_on='Barra_Id',right_on='barra_id')
    # latestbarra=barramaster.loc[(barramaster['barra_id'].isin(Temp_CCS_Barra_MktCap['Barra_Id']).astype(bool))]
    
    
    #royaldutch = ['KORACL1','KORACL3','KORZBT1','KORZCW1']
    #allowedroyaldtch = (barramaster['barra_id'].apply(lambda x: any([k in x for k in royaldutch]))) 
    #xxx=barramaster.loc[allowedroyaldtch]
        
    
    ccscleanednb = ccsstartnb - TempDF.shape[0]
    
    
    #### USE BBO to fill fields
    # Get Sedols from Barra if not in CCS
    MissingSedols = TempDF['sedol_next_day']==''    
    Temp_CCS_Barra_Sedols = TempDF.loc[MissingSedols].merge(tmpbarra[['barra_id','sedol']],how='left',left_on='Barra_Id',right_on='barra_id',left_index=True)
    Temp_CCS_Barra_Sedols['indexnb']= TempDF.index.tolist()
    Temp_CCS_Barra_Sedols.set_index('indexnb', inplace=True)
    Temp_CCS_Barra_Sedols.index.name = None
    TempDF.loc[MissingSedols,'sedol_next_day'] = Temp_CCS_Barra_Sedols['sedol']
    
    
    
    
    
    # Append to TF
    AllData = Temp_Clean_TrackerData.append(TempDF)


    
    #AllData = Temp_Clean_TrackerData
    AllData = AllData.merge(CntryMap[['ISO_Country','Market','Region']],how='left',left_on='ISO_country_symbol_next_day',right_on='ISO_Country')
    AllData.drop(['ISO_Country'], inplace=True, axis=1)
    
    AllData[['initial_mkt_cap_usd_next_day','foreign_inc_factor_next_day']]=AllData[['initial_mkt_cap_usd_next_day','foreign_inc_factor_next_day']].astype(float)
    AllData['FF_MktCap_usd']= AllData['initial_mkt_cap_usd_next_day'] * AllData['foreign_inc_factor_next_day']
    
    
    return  AllData

def GetADRGDR ():

    ADRAllowed = pd.DataFrame({'ADR':['HK','IL','AR','CN','PE','RU']}) 
    GDRAllowed = pd.DataFrame({'GDR':['KZ','RU','UA']}) 
       
    return ADRAllowed,GDRAllowed 


def FillFieldsBBO(bbo,TTTDF):
    InbboInCCS = bbo['idSedol1'].isin(TTTDF['sedol_next_day'])
    df = bbo[InbboInCCS]
    
    InCCSInBBO = TTTDF['sedol_next_day'].isin(bbo['idSedol1'])
    TTTDF = TTTDF[InCCSInBBO]
    
        
    df['secMktCapLoc'] = df['secMktCap'] / df['fxrate']
    renamedict = {
        'name': 'security_name',
        'idIsin': 'isin',
        'industrySubgroup': 'sub_industry_next_day',  # not the same
        'pxLast': 'price',
        'crncy': 'price_ISO_ccy_symbol_next_day',
        'eqyShOut': 'eod_number_of_shares_next_day',  # *1e6
        'eqyFloat':'foreign_inc_factor_next_day',
        'sedol1CountryIso': 'ISO_country_symbol_next_day',
        'secMktCap': 'initial_mkt_cap_usd_next_day',
        'secMktCapLoc': 'initial_mkt_cap_loc_next_day',
        'compMktCap': 'company_full_mktcap',
        # 'RIC'
        'tickerAndExchCode': 'bb_ticker',
        'idSedol1': 'sedol_next_day',
        'securityTyp': 'share_class',
        # share_type
    }
    newdf=df[renamedict.keys()].rename(columns=renamedict)
    
    newdf['eod_number_of_shares_next_day']=newdf['eod_number_of_shares_next_day']*1e6
    newdf['foreign_inc_factor_next_day']=newdf['foreign_inc_factor_next_day']/newdf['eod_number_of_shares_next_day']*1e6
    
   
    FloatsToRound = newdf['foreign_inc_factor_next_day']>0.15
    newdf.loc[FloatsToRound,'foreign_inc_factor_next_day']= roundup(newdf.loc[FloatsToRound,'foreign_inc_factor_next_day'])
    
    ColumnsExisting = ['security_name','isin','ISO_country_symbol_next_day','bb_ticker','Barra_Id']
    newdf = newdf.loc[:,~(newdf.columns.isin(ColumnsExisting))]
    
    
        
    return newdf


def MergeCCSBBO(newdf,TTTDF,Temp_Clean_TrackerData):
   
    for col in newdf.columns:
        if (col!='sedol_next_day') & (sum(TTTDF.columns==col)>0):     
            TTTDF[col]=TTTDF.merge(newdf, how='left', on='sedol_next_day')[col+'_y']
            
    DMCountries = Temp_Clean_TrackerData.loc[Temp_Clean_TrackerData['DM_universe_flag']==1,'ISO_country_symbol_next_day'].unique()
    EMCountries = Temp_Clean_TrackerData.loc[Temp_Clean_TrackerData['EM_universe_flag']==1,'ISO_country_symbol_next_day'].unique()
    FMCountries = Temp_Clean_TrackerData.loc[Temp_Clean_TrackerData['fm_universe_flag']==1,'ISO_country_symbol_next_day'].unique()
    GCCCountries = Temp_Clean_TrackerData.loc[Temp_Clean_TrackerData['gcc_universe_flag']==1,'ISO_country_symbol_next_day'].unique()
    
    
        
    TTTDF.loc['msci_timeseries_code']=df_prime.loc[InIPOsFlag,'sedol_next_day']
    df_prime.loc[InIPOsFlag,'msci_issuer_code']=df_prime.loc[InIPOsFlag,'sedol_next_day']
    df_prime.loc[InIPOsFlag,'msci_security_code']=df_prime.loc[InIPOsFlag,'sedol_next_day']
    df_prime.loc[InIPOsFlag,'DM_universe_flag']= df_prime.loc[InIPOsFlag,'ISO_country_symbol_next_day'].isin(DMCountries).astype(int)
    df_prime.loc[InIPOsFlag,'EM_universe_flag']= df_prime.loc[InIPOsFlag,'ISO_country_symbol_next_day'].isin(EMCountries).astype(int)
    df_prime.loc[InIPOsFlag,'non_local_listing_flag']= 0
    df_prime.loc[InIPOsFlag,'domestic_inc_factor_next_day']= df_prime.loc[InIPOsFlag,'foreign_inc_factor_next_day']
    df_prime.loc[InIPOsFlag,'limited_investability_factor']=1
    df_prime.loc[InIPOsFlag,'foreign_ownership_limit']=1
    df_prime.loc[InIPOsFlag,'unadj_market_cap_today_usdol']=df_prime.loc[InIPOsFlag,'initial_mkt_cap_usd_next_day']
    df_prime.loc[InIPOsFlag,'adj_market_cap_usdol']=df_prime.loc[InIPOsFlag,'initial_mkt_cap_usd_next_day']
    df_prime.loc[InIPOsFlag,'fm_universe_flag']=df_prime.loc[InIPOsFlag,'ISO_country_symbol_next_day'].isin(FMCountries).astype(int)
    df_prime.loc[InIPOsFlag,'gcc_universe_flag']=df_prime.loc[InIPOsFlag,'ISO_country_symbol_next_day'].isin(GCCCountries).astype(int)
    df_prime.loc[InIPOsFlag,'RIC']=df_prime.loc[InIPOsFlag,'sedol_next_day']
    df_prime.loc[InIPOsFlag,'share_type']=''
    df_prime.loc[InIPOsFlag,'alternate_listing']=''
    df_prime.loc[InIPOsFlag,'Status']='NEW'
    df_prime.loc[InIPOsFlag,'Size_Cap']='NEW'
    df_prime.loc[InIPOsFlag,'Dom_Flag']=0
    df_prime.loc[InIPOsFlag,'Pro_Status']='PRO_NEW'
    df_prime.loc[InIPOsFlag,'IIF']=1
    
    Clean_TrackerData = df_prime
    
    return Clean_TrackerData

def roundup(x):
    x=x/0.05
    x=(x.apply(np.ceil))*0.05
    return x