


import pandas as pd
import xlsxwriter
import os

dirpath = os.getcwd()

Allmktcutoff=pd.DataFrame()
AllMonitor=pd.DataFrame()

for datettt in ('18','19','20','21'):
        
    InputputPath = dirpath +'\\Output\\' + '2022' + datettt + '04'
    tempmktcutoff = pd.read_excel(InputputPath +'\\DMEM_MarketCutoff_' + '2022' + datettt + '04.xlsx', sheet_name='DMEM_MarketCutoff')
    tempmktcutoff['datettt'] = '2022' + '04' + datettt    
    
    tempmonitor = pd.read_excel(InputputPath +'\\DMEM_OutputMonitor_' + '2022' + datettt + '04.xlsx', sheet_name='DMEM_OutputMonitor_')
    tempmonitor['datettt'] = '2022' + '04' + datettt
    
    for mktttt in tempmktcutoff['Market'].unique():
        tempmktcutoff.loc[tempmktcutoff['Market']==mktttt,'Adds'] = tempmonitor.loc[(tempmonitor['Market']==mktttt) & (tempmonitor['Final_Monitor1']=='Add'),'Final_Monitor1'].count()
        tempmktcutoff.loc[tempmktcutoff['Market']==mktttt,'Dels'] = tempmonitor.loc[(tempmonitor['Market']==mktttt) & (tempmonitor['Final_Monitor1']=='Del'),'Final_Monitor1'].count()
    
    Allmktcutoff = pd.concat([Allmktcutoff, tempmktcutoff], ignore_index=True)
    AllMonitor = pd.concat([AllMonitor, tempmonitor], ignore_index=True)
   
    
    


CleanAllmktcutoff = pd.pivot_table(Allmktcutoff,index=["Market"],columns=['datettt'],values=['MSnbC','FinalMSnbC','MSSC','Ranked_MSSC','FinalMSSC','Adds','Dels'])

CleanAllMonitor = pd.pivot_table(AllMonitor,index=["msci_security_code"],columns=['datettt'],values=['Final_Monitor1'],aggfunc='sum')
CleanAllMonitor = CleanAllMonitor.merge(AllMonitor.iloc[:,1:15], how='left', on='msci_security_code')
CleanAllMonitor=CleanAllMonitor.drop_duplicates(subset='msci_security_code', keep='first')





# MarketCutoff
OutPath2 = InputputPath = dirpath +'\\Output\\multi'
MultiDateReader = pd.ExcelWriter(OutPath2 + '\\MultiDates_End__' + datettt + '.xlsx', engine='xlsxwriter')
CleanAllmktcutoff.to_excel(MultiDateReader, sheet_name='MultiCutoffs')
CleanAllMonitor.to_excel(MultiDateReader, sheet_name='MultiMonitor')
MultiDateReader.save()