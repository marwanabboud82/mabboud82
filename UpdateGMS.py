# -*- coding: utf-8 -*-
"""
@author: mabboud
"""
import pandas as pd

def UpdateGMS(GIEU_CompanyData,GMSRankPrevSAIR):
    
    NewGIEUGMS={}

    NewGIEUGMSRank = GMSRankPrevSAIR

    DM_GIEU_CompanyData = GIEU_CompanyData.loc[GIEU_CompanyData['Region']=='DM']
    DM_GIEU_CompanyData = DM_GIEU_CompanyData.sort_values('company_full_mktcap', ascending=False)
    CumFFMktCap = DM_GIEU_CompanyData['FFCompMktCap'].cumsum()
    xxx=CumFFMktCap.tail(1)
    CoverageFFMktCap = pd.DataFrame(CumFFMktCap / xxx.iloc[0])

    if (CoverageFFMktCap.iloc[GMSRankPrevSAIR] < 0.85).bool():
        NewGIEUGMSRank = CoverageFFMktCap[CoverageFFMktCap['FFCompMktCap']<0.85].count()
    elif (CoverageFFMktCap.iloc[GMSRankPrevSAIR] > 0.87).bool():
        NewGIEUGMSRank = CoverageFFMktCap[CoverageFFMktCap['FFCompMktCap']<0.87].count()

    NewGIEUGMS['Rank'] = NewGIEUGMSRank
    NewGIEUGMS['Value'] = DM_GIEU_CompanyData.iloc[NewGIEUGMSRank+1,DM_GIEU_CompanyData.columns.get_loc("company_full_mktcap")] 


    return  NewGIEUGMS