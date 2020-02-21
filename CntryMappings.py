# -*- coding: utf-8 -*-
"""
Created on Thu Nov 28 19:15:21 2019

@author: mabboud
"""
import pandas as pd

def GetCntryMap ():
    CntryMap = pd.DataFrame({'ISO_Country':['AU','CN','HK','ID','IN','JP','KR','MY','NZ','PH','PK','SG','TH','TW','CA','AT','BE','CH','CZ','DE','DK','ES','FI','FR','GB','GR','HU','IE','IT','LU','NL','NO','PL','PT','RU','SE','AR','BR','CL','CO','MX','PE','VE','EG','IL','JO','LK','MA','TR','ZA','US','AE','BH','KW','OM','QA','SA','XA','BG','EE','HR','KE','KZ','LB','MU','NG','RO','SI','TN','UA','VN','BA','BD','BW','GH','JM','LT','RS','TT','ZW','SN','PS','BF','PS','CI','BF','CI','PS','PS','BJ','NA','MK'], \
            
            'Market': ['AUSTRALIA','CHINA','HONG KONG','INDONESIA','INDIA','JAPAN','KOREA','MALAYSIA','NEW ZEALAND','PHILIPPINES','PAKISTAN','SINGAPORE','THAILAND','TAIWAN','CANADA','EUROPE','EUROPE','EUROPE','CZECH REPUBLIC','EUROPE','EUROPE','EUROPE','EUROPE','EUROPE','EUROPE','GREECE','HUNGARY','EUROPE','EUROPE','OTHER','EUROPE','EUROPE','POLAND','EUROPE','RUSSIA','EUROPE','ARGENTINA','BRAZIL','CHILE','COLOMBIA','MEXICO','PERU','OTHER','EGYPT','ISRAEL','OTHER','OTHER','OTHER','TURKEY','SOUTH AFRICA','USA','UNITED ARAB EMIRATES','OTHER','OTHER','OTHER','QATAR','SAUDI ARABIA','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER','OTHER'], \
            
            'Region': ['DM','EM','DM','EM','EM','DM','EM','EM','DM','EM','EM','DM','EM','EM','DM','DM','DM','DM','EM','DM','DM','DM','DM','DM','DM','EM','EM','DM','DM','NONE','DM','DM','EM','DM','EM','DM','EM','EM','EM','EM','EM','EM','NONE','EM','DM','NONE','NONE','NONE','EM','EM','DM','EM','NONE','NONE','NONE','EM','EM','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE'] })

    return CntryMap


