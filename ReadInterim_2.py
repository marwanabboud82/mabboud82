# -*- coding: utf-8 -*-
"""
@author: mabboud
"""
import pandas as pd

def ReadInterim_2 (TrackerFilePath,IntCalcDate,All_SecurityData,All_CompanyData):
    
    # DM STD Interim File
    DMSTDInterimPath = (TrackerFilePath.loc['DMSTDPath'] + IntCalcDate[0:2] + IntCalcDate[3:5]  +
                 'd_tdrif\\' + IntCalcDate[0:2] + IntCalcDate[3:5] + 'd51d')
    
    Interim_DMSTD = ReadIntFile(DMSTDInterimPath[0])
    Interim_DMSTD.loc[Interim_DMSTD['index_name'] == 'EUROPE MID CAP','index_name'] ='NOT TO BE USED'
    Interim_DMSTD.loc[Interim_DMSTD['index_name'] == 'FRANCE MID CAP','index_name'] ='EUROPE MID CAP'
    # EM STD Interim File
    EMSTDInterimPath = (TrackerFilePath.loc['DMSTDPath'] + IntCalcDate[0:2] + IntCalcDate[3:5]  +
                 'd_terif\\' + IntCalcDate[0:2] + IntCalcDate[3:5] + 'd51e')
    Interim_EMSTD = ReadIntFile(EMSTDInterimPath[0])
    
    # Fill MSSC and MSNbC
    unqmkts = All_CompanyData['Market'].unique()
    cols=['Market','DM/EM','MSnbC','MSSC','Ranked_MSSC','Diff','Capped_MSSC','Capped_Diff','FF_Coverage']
    DMEMInterim=  pd.DataFrame(index=unqmkts,columns=cols)
    DMEMInterim['Market'] = unqmkts
    DMEMInterim=DMEMInterim.drop(index='OTHER')
    
    for colmkt in DMEMInterim['Market']:
        DMEMInterim.loc[colmkt,'Market']= colmkt
        DMEMInterim.loc[colmkt,'DM/EM'] = All_CompanyData.loc[All_CompanyData['Market']==colmkt,'Region'].unique()[0]
        if (DMEMInterim.loc[colmkt,'DM/EM']=='DM'):
            DMEMInterim.loc[colmkt,'MSSC'] =  Interim_DMSTD.loc[Interim_DMSTD['index_name'] == colmkt + ' MID CAP','full_issuer_mkt_cap_cutoff_usd'].iloc[0]
            DMEMInterim.loc[colmkt,'MSnbC'] =  Interim_DMSTD.loc[Interim_DMSTD['index_name'] == colmkt ,'target_nb_companies'].iloc[0]
        elif(DMEMInterim.loc[colmkt,'DM/EM']=='EM'):
            DMEMInterim.loc[colmkt,'MSSC'] =  Interim_EMSTD.loc[Interim_EMSTD['index_name'] == colmkt + ' MID CAP','full_issuer_mkt_cap_cutoff_usd'].iloc[0]
            DMEMInterim.loc[colmkt,'MSnbC'] =  Interim_EMSTD.loc[Interim_EMSTD['index_name'] == colmkt ,'target_nb_companies'].iloc[0]
    # GET the GMS from Published Data
    EMMktCaps = DMEMInterim.loc[DMEMInterim['DM/EM']=='EM',['Market','MSSC']]
    EMMktCaps=EMMktCaps.sort_values('MSSC',ascending=True)
   
    EMDuplicatesNb = EMMktCaps.pivot_table(index=['MSSC'], aggfunc='size')
   
    if(EMDuplicatesNb.tail(1)>1).bool():
        EMGMSHigh= EMMktCaps.tail(1)
        EMGMS = EMGMSHigh['MSSC']/1.15
        DMGMS = 2 * EMGMS
    elif((EMDuplicatesNb.tail(1)==1) & (EMDuplicatesNb.head(1)>1)).bool():
        EMGMSLow = EMMktCaps.head(1)
        EMGMS = EMGMSLow['MSSC'] * 2
        DMGMS = 2 * EMGMS
   
    gmsindex = ['DMGMSLow','DMGMS','DMGMSHigh','EMGMSLow','EMGMS','EMGMSHigh'] 
    PublishedGMS = {'$m':[DMGMS[0]*0.5,DMGMS[0],DMGMS[0]*1.15,EMGMS[0]*0.5,EMGMS[0],EMGMS[0]*1.15]}
    PublishedGMS=  pd.DataFrame(PublishedGMS,index=gmsindex)
  
    # Include Them in the Security and Company Universe
    All_SecurityData['Interim_MSSC']=All_SecurityData['company_full_mktcap']*0 
    for mkt in DMEMInterim['Market']:
        All_SecurityData.loc[All_SecurityData['Market']==DMEMInterim.loc[mkt,'Market'],'Interim_MSSC']= DMEMInterim.loc[mkt,'MSSC']
    All_SecurityData['Interim_MSnbC']=All_SecurityData['company_full_mktcap']*0 
    for mkt in DMEMInterim['Market']:
        All_SecurityData.loc[All_SecurityData['Market']==DMEMInterim.loc[mkt,'Market'],'Interim_MSnbC']= DMEMInterim.loc[mkt,'MSnbC']
    
    All_CompanyData['Interim_MSSC']=All_CompanyData['company_full_mktcap']*0 
    for mkt in DMEMInterim['Market']:
        All_CompanyData.loc[All_CompanyData['Market']==mkt,'Interim_MSSC']= DMEMInterim.loc[mkt,'MSSC']
    All_CompanyData['Interim_MSnbC']=All_CompanyData['company_full_mktcap']*0 
    for mkt in DMEMInterim['Market']:
        All_CompanyData.loc[All_CompanyData['Market']==mkt,'Interim_MSnbC']= DMEMInterim.loc[mkt,'MSnbC']
    
    # Get the FFCoverage
    for mkt in DMEMInterim['Market']:
        tmpcomp = All_CompanyData[All_CompanyData['Market']==mkt]
        tmpcomp = tmpcomp.sort_values('company_full_mktcap', ascending=False)
        CumFFMktCap = tmpcomp['FFCompMktCap'].cumsum()
        xxx=CumFFMktCap.tail(1)
        CoverageFFMktCap = CumFFMktCap / xxx.iloc[0]
        DMEMInterim.loc[mkt,'FF_Coverage']= CoverageFFMktCap.iloc[int(DMEMInterim.loc[mkt,'MSnbC'])-1]




    
     # Check Published vs Ranked
    for mkt in DMEMInterim['Market']:
        tmpcomp = All_CompanyData[All_CompanyData['Market']==mkt]
        tmpcomp = tmpcomp.sort_values('company_full_mktcap', ascending=False)
        DMEMInterim.loc[mkt,'Ranked_MSSC'] = tmpcomp.iloc[int(DMEMInterim.loc[mkt,'MSnbC'])-1,tmpcomp.columns.get_loc("company_full_mktcap")]/1e+6
        DMEMInterim.loc[mkt,'Diff'] = int(DMEMInterim.loc[mkt,'Ranked_MSSC'] - DMEMInterim.loc[mkt,'MSSC'] )
        if ((DMEMInterim.loc[mkt,'Diff']!=0) & (int(DMEMInterim.loc[mkt,'MSSC'])==int(PublishedGMS.loc[DMEMInterim.loc[mkt,'DM/EM']+'GMSHigh']) )):
            DMEMInterim.loc[mkt,'Capped_MSSC'] = PublishedGMS.loc[DMEMInterim.loc[mkt,'DM/EM']+'GMSHigh'][0]
        elif ((DMEMInterim.loc[mkt,'Diff']!=0) & (int(DMEMInterim.loc[mkt,'MSSC'])==int(PublishedGMS.loc[DMEMInterim.loc[mkt,'DM/EM']+'GMSLow']) )):
            DMEMInterim.loc[mkt,'Capped_MSSC'] = PublishedGMS.loc[DMEMInterim.loc[mkt,'DM/EM']+'GMSLow'][0]
        else:
            DMEMInterim.loc[mkt,'Capped_MSSC'] = DMEMInterim.loc[mkt,'Ranked_MSSC']
        DMEMInterim.loc[mkt,'Capped_Diff'] = int(DMEMInterim.loc[mkt,'Capped_MSSC'] - DMEMInterim.loc[mkt,'MSSC'] )
            

    return DMEMInterim,PublishedGMS,All_SecurityData,All_CompanyData


def ReadIntFile(filename):
    nrowheader = 129
    headercolspecs = [(0, 1), (1, 5), (5, 39), (39, 70), (70, 72), (72, 75), (75, 79)]
    df = pd.read_csv(filename, sep='|', skiprows=nrowheader, header=None)
    df = df.iloc[:-2]  # * and #EOD
    cols = pd.read_fwf(filename, headercolspecs, nrows=nrowheader - 4, skiprows=1)
    cols.columns = ['#', 'idx', 'nicename', 'name', 'type', 'length', 'length2']
    renamedict = cols.set_index('idx')['name'].to_dict()
    df = df.rename(columns=renamedict)
    df= df.applymap(lambda x: x.strip() if isinstance(x, str) else x) #remove trailing leading spaces
    floatcols = cols[cols['type'] == 'N']['name'].tolist()
    for colname in floatcols:
        df[colname] = pd.to_numeric(df[colname], errors='ignore')
    stringcols = cols[cols['type'] == 'S']['name'].tolist()
    for colname in stringcols:
        df[colname] = df[colname].apply(lambda x: x.strip() if isinstance(x, str) else x)
    df.drop(df.columns[0], axis=1, inplace=True)
    return df