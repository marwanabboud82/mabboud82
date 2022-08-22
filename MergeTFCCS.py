# -*- coding: utf-8 -*-
"""
@author: mabboud
"""
import pandas as pd
import numpy as np

def MergeTFCCS (bbo,TrackerFilePath,Clean_TrackerData,CCS_Data,CntryMap,sDate):

    #filebarra = ('C:\\Users\\mabboud\\PycharmProjects\\Python\\MSCI\\input\\barra_master_gemtr_20201208.csv')
    
    
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
    
    InCCSANDInIPOS=  (Temp_Clean_TrackerData['sedol_next_day'].isin(TempDF['sedol_next_day'])) \
    & (Temp_Clean_TrackerData['Pro_Status']=='IPO')
    Temp_Clean_TrackerData=Temp_Clean_TrackerData[~InCCSANDInIPOS]

    InCCSNotInTF = ~ TempDF['msci_security_code'].isin(Temp_Clean_TrackerData['msci_security_code'])
    TempDF = TempDF[InCCSNotInTF]
    
  
    
    ccsstartnb = TempDF.shape[0]
    print('CCS Start',ccsstartnb)
    
    ### CLEAN CCS
    
    # barra master for CrossList

    from KDBData.barra import BarraMasterDownload ##to put on top with all other imports
    b = BarraMasterDownload(sDate=sDate,eDate=sDate,model='gemtr')
    barramaster=b.get()
    #barramaster= pd.read_csv(filebarra) 

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
    #GoodMappingCheck = ['AT_AUT','AU_AUS','BE_BEL','BE_NLD','CA_CAN','CH_CHE','DE_DEU','DK_DNK','ES_ESP','FI_FIN','FR_FRA','FR_NLD','GB_GBR','HK_HKG','HK_NOR','HK_SGP','HK_USA','IE_IRL','IL_AUS','IL_BEL','IL_HKG','IL_ISR','IL_SGP','IL_SWE','IL_GBR','IL_USA','IT_ITA','JP_JPN','NL_NLD','NL_USA','NO_NOR','NZ_NZL','PT_PRT','SE_SWE','SG_SGP','US_USA','AE_ARE','AR_USA','BR_BRA','CL_CHL','CN_CHN','CN_HKG','CN_SGP','CN_USA','CO_COL','CZ_CZE','EG_EGY','GR_GRC','HU_HUN','ID_IDN','IN_IND','KR_KOR','MX_MEX','MY_MYS','PE_USA','PH_PHL','PK_PAK','PL_POL','QA_QAT','RU_RUS','RU_GBR','RU_USA','SA_SAU','TH_THA','TR_TUR','TW_TWN','ZA_ZAF']    
    GoodMappingCheck = ['AE_ARE','AR_USA','AT_AUT','AU_AUS','BA_BIH','BD_BGD','BE_BEL','BE_NLD','BG_BGR','BH_BHR','BH_KWT','BR_BRA','BW_BWA','CA_CAN','CH_CHE','CL_CHL','CN_CHN','CN_HKG','CN_SGP','CN_USA','CO_COL','CZ_CZE','DE_DEU','DK_DNK','EE_EST','EG_EGY','ES_ESP','FI_FIN','FR_FRA','FR_NLD','GB_GBR','GR_GRC','HK_HKG','HK_SGP','HK_USA','HR_HRV','HU_HUN','ID_IDN','IE_IRL','IL_HKG','IL_ISR','IL_USA','IN_IND','IS_DNK','IS_ISL','IT_ITA','JM_JAM','JO_JOR','JP_JPN','KE_KEN','KR_KOR','KW_KWT','LB_LBN','LK_LKA','LT_LTU','MA_MAR','MU_MUS','MU_ZAF','MX_MEX','MY_MYS','NG_NGA','NL_NLD','NL_USA','NO_NOR','NZ_NZL','OM_OMN','PE_USA','PH_PHL','PK_PAK','PL_POL','PS_PSE','PT_PRT','QA_QAT','RO_ROU','RS_SRB','RU_GBR','RU_RUS','RU_USA','SA_SAU','SE_SWE','SG_HKG','SG_SGP','SG_USA','SI_SVN','TH_THA','TN_TUN','TR_TUR','TT_TTO','TW_TWN','UA_POL','US_USA','VN_VNM','ZA_ZAF']    
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
    

    
    # BDCs in US
    # Get Sedols from Barra if not in CCS
    MissingSedols = TempDF['sedol_next_day']==''    
    Temp_CCS_Barra_Sedols = TempDF.loc[MissingSedols].merge(tmpbarra[['barra_id','sedol']],how='left',left_on='Barra_Id',right_on='barra_id',left_index=True)
    Temp_CCS_Barra_Sedols['indexnb']= TempDF.index.tolist()
    Temp_CCS_Barra_Sedols.set_index('indexnb', inplace=True)
    Temp_CCS_Barra_Sedols.index.name = None
    TempDF.loc[MissingSedols,'sedol_next_day'] = Temp_CCS_Barra_Sedols['sedol']
    
    
    # CHANGE TO PULL BICS_LEVEL_4_SUB_INDUSTRY_NAME from bloomberg where it shows BDCs
    # at the moment got this list from here: https://www.bdcinvestor.com/business-development-company-list/
    BDCsList =['BGLP5H7','B032FN0','BFZ4N57','B1VRDC9','BJH07C6','BBHX291','2174583','B3RV2F5','BLRVST7','B0C1G46','B8JVQ34','B60K6F8','2793331','BWC8Y36','B3T78R5','B8W8DM0','B5BD5P2','B07LT08','B28BNR6','B451X51','B7X6W41','2593218','BRWZXQ8','B61WWF5','BF4WCJ9','BF4WC11','B7L7HC9','BRGCNF4','BK0P8V1','BFN6VQ2','B61FCS7','B1W5VY0','B020VX7','BJ7JPR4','2722959','B1VPWH1','B8NW3Y3','B61FRC6','B4085D9','BF4WVH0','B64XDW9','BK8G322','BKRVMQ5','B86YGD6']
    #BadBDCs = (TempDF['sedol_next_day'].apply(lambda x: any([k in x for k in BDCsList])))
    BadBDCs = (TempDF['sedol_next_day'].isin(BDCsList))
    print('BadBDCs' , sum(BadBDCs))
    TempDF = TempDF.loc[~BadBDCs]
    
    
    
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
    
    
    #royaldutch = ['SWIAED2','SWIAED1']
    #allowedroyaldtch = (barramaster['barra_id'].apply(lambda x: any([k in x for k in royaldutch]))) 
    #xxx=barramaster.loc[allowedroyaldtch]
        
    
    ccscleanednb = ccsstartnb - TempDF.shape[0]
    

    
    #MissingSedols = Temp_Clean_TrackerData['sedol_next_day']==''    
    #Temp_TF_Barra_Sedols = Temp_Clean_TrackerData.loc[MissingSedols].merge(barramaster[['barra_id','sedol']],how='left',left_on='Barra_Id',right_on='barra_id',left_index=True)
    #Temp_TF_Barra_Sedols['indexnb']= Temp_Clean_TrackerData.index.tolist()
    #Temp_TF_Barra_Sedols.set_index('indexnb', inplace=True)
    #Temp_TF_Barra_Sedols.index.name = None
    #Temp_Clean_TrackerData.loc[MissingSedols,'sedol_next_day'] = Temp_TF_Barra_Sedols['sedol']
    
    
    #### USE BBO to fill fields
    TempDF = FillFieldsBBO(bbo,TempDF)
    
        
    
    ### Merge with TF     
    
    AllData = MergeCCSBBO(TempDF,Temp_Clean_TrackerData)
 
    AllData = AllData.merge(CntryMap[['ISO_Country','Market','Region']],how='left',left_on='ISO_country_symbol_next_day',right_on='ISO_Country')
    AllData.drop(['ISO_Country'], inplace=True, axis=1)
    
    ### Include the FOL concept.
    IsInCCS =  AllData['Pro_Status']=='CCS'
    AllData.loc[IsInCCS,'foreign_inc_factor_next_day'] = np.minimum(AllData.loc[IsInCCS,'foreign_inc_factor_next_day'],AllData.loc[IsInCCS,'foreign_ownership_limit'])
    AllData.loc[IsInCCS,'foreign_inc_factor_next_day'] = np.minimum(AllData.loc[IsInCCS,'foreign_inc_factor_next_day'],1)       
    
        
    AllData[['initial_mkt_cap_usd_next_day','foreign_inc_factor_next_day']]=AllData[['initial_mkt_cap_usd_next_day','foreign_inc_factor_next_day']].astype(float)
    AllData['FF_MktCap_usd']= AllData['initial_mkt_cap_usd_next_day'] * AllData['foreign_inc_factor_next_day']
    
    AllData = AllData.drop(columns=['origin', 'barra_id','country'])
    
    InIPOandInCCS  = AllData.loc[AllData['Pro_Status']=='IPO','sedol_next_day' ].isin(AllData.loc[AllData['Pro_Status']=='CCS','sedol_next_day' ]) 
    IPOAllData = AllData.loc[AllData['Pro_Status']=='IPO']
    IPOsDuplicateofCCS = (AllData['Pro_Status']=='IPO') &  AllData['sedol_next_day'].isin(IPOAllData.loc[InIPOandInCCS,'sedol_next_day']) 
    AllData = AllData.loc[~IPOsDuplicateofCCS]
    
        # exclued the local listings of the current IMI ADRs in MSCI China (BABA JD etc ...)
    InChinaIMIADR = (AllData['alternate_listing']=='ADR') & (AllData['ISO_country_symbol_next_day']=='CN') 
    MSCICompCodes = AllData.loc[InChinaIMIADR,'msci_issuer_code']
    DupCodes = (AllData['msci_issuer_code'].isin(MSCICompCodes))
    AllData=AllData.loc[~((DupCodes) & (AllData['Status']=='NEW'))]
    
    #AllData['msci_security_code']=np.where(AllData['Pro_Status']!='IPO',pd.to_numeric(NonIPOData['msci_security_code']).astype(np.int64).astype(str),AllData['msci_security_code'])
    #NonIPOData = AllData.loc[AllData['Pro_Status']!='IPO'].copy()#,'msci_security_code'].str[:-2]
    #NonIPOData['msci_security_code']=pd.to_numeric(NonIPOData['msci_security_code']).astype(np.int64)
    
    # We call df your dataframe you want to sort
    orderdf =pd.DataFrame({'Status':['STD','SML','STD_SHADOW','SML_SHADOW','MICRO','NEW'],'Order':[1,2,3,4,5,6]})
    AllData=AllData.merge(orderdf,on='Status',how='left')
    AllData=AllData.sort_values('Order')
    AllData=AllData.drop_duplicates(subset=['msci_security_code'])
    AllData = AllData.drop(columns=['Order'])
    
    
    return  AllData

def GetADRGDR ():

    ADRAllowed = pd.DataFrame({'ADR':['HK','IL','AR','CN','PE','RU']}) 
    GDRAllowed = pd.DataFrame({'GDR':['KZ','RU','UA']}) 
       
    return ADRAllowed,GDRAllowed 



def FillFieldsBBO(bbo,TTTDF):
    InbboInCCS = bbo['idSedol1'].isin(TTTDF['sedol_next_day'])
    df = bbo[InbboInCCS]
    
    quarterly = TTTDF.dropna(axis=1,how='all')
    
           
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
        'compMktCapBBG': 'company_full_mktcap',
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
    
    mkey = 'sedol_next_day'
    commoncols = [x for x in quarterly.columns if (x in newdf.columns) and (x!=mkey)]
    newdf = quarterly.merge(newdf.drop(commoncols,axis=1),on=mkey,how='left')
            
    return newdf


def MergeCCSBBO(TempDF,Temp_Clean_TrackerData):
    # Concat
    TmpAllData = pd.concat([Temp_Clean_TrackerData.assign(origin='msci'),TempDF.assign(origin='bbo')],axis=0,sort=False).reset_index(drop=True)
    
    TmpAllData.loc[TmpAllData['Pro_Status']!='IPO','msci_security_code']=TmpAllData.loc[TmpAllData['Pro_Status']!='IPO','msci_security_code'].str[:-2]
    
    InCCSFlag = TmpAllData['origin']=='bbo'
    InIPOFlag = TmpAllData['Pro_Status']=='IPO'
    
    DMCountries = Temp_Clean_TrackerData.loc[Temp_Clean_TrackerData['DM_universe_flag']==1,'ISO_country_symbol_next_day'].unique()
    EMCountries = Temp_Clean_TrackerData.loc[Temp_Clean_TrackerData['EM_universe_flag']==1,'ISO_country_symbol_next_day'].unique()
    FMCountries = Temp_Clean_TrackerData.loc[Temp_Clean_TrackerData['fm_universe_flag']==1,'ISO_country_symbol_next_day'].unique()
    GCCCountries = Temp_Clean_TrackerData.loc[Temp_Clean_TrackerData['gcc_universe_flag']==1,'ISO_country_symbol_next_day'].unique()
    
    
    TmpAllData.loc[InCCSFlag,'msci_timeseries_code']=TmpAllData.loc[InCCSFlag,'sedol_next_day']
    #TmpAllData['msci_issuer_code']=TmpAllData['msci_issuer_code'].astype(str)
    TmpAllData['msci_issuer_code'] = ''
    #TmpAllData['msci_security_code']=TmpAllData['msci_security_code'].astype(str)
    #TmpAllData['msci_issuer_code']=TmpAllData['msci_security_code'].str[:-2]
    TmpAllData.loc[~InIPOFlag,'msci_issuer_code']=TmpAllData.loc[~InIPOFlag,'msci_security_code'].str[:-2]
    TmpAllData.loc[InIPOFlag,'msci_issuer_code']=TmpAllData.loc[InIPOFlag,'msci_security_code']
    #TmpAllData.loc[InCCSFlag,'msci_issuer_code']=TmpAllData.loc[InCCSFlag,'msci_security_code'].apply(lambda x:x[:-2] if isinstance(x,str) else np.nan)
    
    TmpAllData.loc[InCCSFlag,'DM_universe_flag']= TmpAllData.loc[InCCSFlag,'ISO_country_symbol_next_day'].isin(DMCountries).astype(int)
    TmpAllData.loc[InCCSFlag,'EM_universe_flag']= TmpAllData.loc[InCCSFlag,'ISO_country_symbol_next_day'].isin(EMCountries).astype(int)
    TmpAllData.loc[InCCSFlag,'non_local_listing_flag']= 0
    TmpAllData.loc[InCCSFlag,'domestic_inc_factor_next_day']= TmpAllData.loc[InCCSFlag,'foreign_inc_factor_next_day']
    TmpAllData.loc[InCCSFlag,'limited_investability_factor']=1
    TmpAllData.loc[InCCSFlag,'foreign_ownership_limit']=1
    TmpAllData.loc[InCCSFlag,'unadj_market_cap_today_usdol']=TmpAllData.loc[InCCSFlag,'initial_mkt_cap_usd_next_day']
    TmpAllData.loc[InCCSFlag,'adj_market_cap_usdol']=TmpAllData.loc[InCCSFlag,'initial_mkt_cap_usd_next_day']
    TmpAllData.loc[InCCSFlag,'fm_universe_flag']=TmpAllData.loc[InCCSFlag,'ISO_country_symbol_next_day'].isin(FMCountries).astype(int)
    TmpAllData.loc[InCCSFlag,'gcc_universe_flag']=TmpAllData.loc[InCCSFlag,'ISO_country_symbol_next_day'].isin(GCCCountries).astype(int)
    TmpAllData.loc[InCCSFlag,'RIC']=TmpAllData.loc[InCCSFlag,'sedol_next_day']
    TmpAllData.loc[InCCSFlag,'share_type']=''
    TmpAllData.loc[InCCSFlag,'alternate_listing']=''
    TmpAllData.loc[InCCSFlag,'Status']='NEW'
    TmpAllData.loc[InCCSFlag,'Size_Cap']='NEW'
    TmpAllData.loc[InCCSFlag,'Dom_Flag']=0
    TmpAllData.loc[InCCSFlag,'Pro_Status']='CCS'
    TmpAllData.loc[InCCSFlag,'IIF']=1
    TmpAllData.loc[InCCSFlag,'InTF']=0
    
    return TmpAllData

def roundup(x):
    x=x/0.05
    x=(x.apply(np.ceil))*0.05
    return x