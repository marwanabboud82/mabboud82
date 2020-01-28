# -*- coding: utf-8 -*-
"""
@author: mabboud
"""
import pandas as pd

def GetATVRData (All_In_Data):
   
    All_Out_Data = All_In_Data.assign(**{'12mATVR':1000, '3m_Q-4':1000,'3m_Q-3':1000,'3m_Q-2':1000,'3m_Q-1':1000})
    
    del All_In_Data
    return  All_Out_Data