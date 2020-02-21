# -*- coding: utf-8 -*-
"""
Created on Tue Jan 28 18:06:25 2020

@author: mabboud
"""


import pandas as pd
import numpy as np



def GetIPOs(bbo,InceptionDate):


    df = bbo[bbo['eqyInitPoDt'] > pd.to_datetime(InceptionDate)]
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
        
    return newdf

def roundup(x):
    x=x/0.05
    x=(x.apply(np.ceil))*0.05
    return x