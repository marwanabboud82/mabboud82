# -*- coding: utf-8 -*-
"""
@author: mabboud
"""
import pandas as pd

def GetCompanyUniverse (All_Data):
   
    #All_Data= AllData_TEST
    #Sort STD, SC then Others to get correct flag for companies
    All_Data_Ranked = pd.concat( [All_Data[All_Data['Status']=='STD'], All_Data[All_Data['Status']=='SML'], \
    All_Data[All_Data['Status']=='MICRO'], All_Data[All_Data['Status']=='STD_SHADOW'], \
    All_Data[All_Data['Status']=='SML_SHADOW'],All_Data[All_Data['Status']=='NEW']])
       
    All_Data_Ranked['num_sec_in_comp']= pd.DataFrame(All_Data_Ranked.groupby('msci_issuer_code')['msci_issuer_code'].transform('count'))
    All_Data_Ranked['Sum_FullMktCap_USD']=pd.DataFrame(All_Data_Ranked.groupby('msci_issuer_code')['initial_mkt_cap_usd_next_day'].transform('sum'))
    All_Data_Ranked['Unlisted_FullMktCap_USD']= All_Data_Ranked['company_full_mktcap']- All_Data_Ranked['Sum_FullMktCap_USD']
    
    All_Data_Ranked['FFCompMktCap']=pd.DataFrame(All_Data_Ranked.groupby('msci_issuer_code')['FF_MktCap_usd'].transform('sum'))
    
    # Get unique company codes
    UnqCompCodes = All_Data['msci_issuer_code'].unique()
    #Get Company Universe
    Company_Data = All_Data_Ranked.drop_duplicates(subset=['msci_issuer_code'], keep='first')
    Company_Data = Company_Data[['msci_issuer_code','bb_ticker','security_name','company_full_mktcap','Status','Size_Cap','ISO_country_symbol_next_day','Region','Market','InTF','foreign_inc_factor_next_day','eod_number_of_shares_next_day','num_sec_in_comp','Sum_FullMktCap_USD','Unlisted_FullMktCap_USD','FFCompMktCap']]
    Company_Data=Company_Data.sort_values('company_full_mktcap', ascending=False)
    
    
    
    
    
    return  Company_Data