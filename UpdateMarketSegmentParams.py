# -*- coding: utf-8 -*-
"""
Created on Wed Jan  1 12:11:57 2020

@author: mabboud
"""



def   UpdateMarketSegmentParams(DMEMInterim,PublishedGMS,GIEU_CompanyData):

    import pandas as pd
    import math
    
    # InterimPublished %NewGIEUGMS %SAIRGieuCompany
    # Constants Limits
    LowerGMScst=0.5;
    UpperGMScst=1.15;
    LowerProxGMScst = 0.575;
    UpperProxGMScst = 1.0;
    MinCoveragecst = 0.8;
    MaxCoveragecst = 0.9;
    
    AllMarkets = DMEMInterim['Market']
    AllDMEM =  DMEMInterim['DM/EM']
    
    MIEU = {}
    GMSLimits={}
    InterimData={}
    InitialData={}
    RevisedData={}
    FinalData = {}
    AuditCheckData={}
    OutputUpdateSegmNbComp = {}
    
    for marketN in AllMarkets:
   
        # 1- Rank MIEU
        TempMarketGIEU = GIEU_CompanyData.loc[GIEU_CompanyData['Market']==marketN]
        TempSortedMarketGIEU = TempMarketGIEU.sort_values('company_full_mktcap', ascending=False)
        CumFFMktCap = TempSortedMarketGIEU['FFCompMktCap'].cumsum()
        xxx=CumFFMktCap.tail(1)
        CoverageFFMktCap = pd.DataFrame(CumFFMktCap / xxx.iloc[0])
        TempSortedMarketGIEU['CumFFMktCap']=CumFFMktCap
        TempSortedMarketGIEU['CoverageFFMktCap']=CoverageFFMktCap
        TempSortedMarketGIEU=TempSortedMarketGIEU.reset_index(drop=True)
        
        MIEU[marketN]=pd.DataFrame(TempSortedMarketGIEU) 
        
        
        if (DMEMInterim.loc[DMEMInterim['Market']==marketN,'DM/EM']=='DM').bool():
            TmpGIEUGMS = PublishedGMS.loc['DMGMS','$m']*1e+6
        elif(DMEMInterim.loc[DMEMInterim['Market']==marketN,'DM/EM']=='EM').bool():
            TmpGIEUGMS = PublishedGMS.loc['EMGMS','$m']*1e+6         
        LowerGMS = LowerGMScst*TmpGIEUGMS
        UpperGMS = UpperGMScst*TmpGIEUGMS
        LowerProxGMS = LowerProxGMScst*TmpGIEUGMS
        UpperProxGMS = UpperProxGMScst*TmpGIEUGMS
        TmpGMSLimits = [['LowerGMS',LowerGMS],['UpperGMS',UpperGMS],['LowerProxGMS',LowerProxGMS],['UpperProxGMS',UpperProxGMS]]
        TmpGMSLimits = pd.DataFrame(TmpGMSLimits) 
        TmpGMSLimits.index = TmpGMSLimits[0]
        TmpGMSLimits.drop(0, inplace=True, axis=1)
        
        GMSLimits[marketN]=pd.DataFrame(TmpGMSLimits) 
        
        
        # 2- Get Interim Values
    
        InterimMSnbC = DMEMInterim.loc[DMEMInterim['Market']==marketN,'MSnbC']
        InterimMSSC = DMEMInterim.loc[DMEMInterim['Market']==marketN,'MSSC']
        InterimPctCoverage =  MIEU[marketN].CoverageFFMktCap[InterimMSnbC-1]
        TmpInterimData = [['InterimMSnbC',InterimMSnbC.iloc[0]],['InterimMSSC',InterimMSSC.iloc[0]],['InterimPctCoverage',InterimPctCoverage.iloc[0]]]
        TmpInterimData =pd.DataFrame(TmpInterimData)
        TmpInterimData.index = TmpInterimData[0]
        TmpInterimData.drop(0, inplace=True, axis=1)
        
        InterimData[marketN] = pd.DataFrame(TmpInterimData) 
    
        # 3- Determine Initial MS#C
   
        ###WORTH CHECKING IT AGAIN ONCE VALUES
        ### ROUND all market caps
        
        MIEU[marketN].company_full_mktcap=round(MIEU[marketN].company_full_mktcap)
        InterimMSSC_in_Millions = round(DMEMInterim.loc[marketN,'MSSC'] * 1e+6)
        GMSLimits[marketN]= round(GMSLimits[marketN])
    
    
        # Check vs Low GMS
        
        
        if(InterimMSSC_in_Millions >=  GMSLimits[marketN].loc['LowerGMS']).bool():
            ChosenCompanyRank = MIEU[marketN].company_full_mktcap[MIEU[marketN].company_full_mktcap >= InterimMSSC_in_Millions ].count()
        else:
            pos1 = MIEU[marketN].company_full_mktcap[MIEU[marketN].company_full_mktcap >= GMSLimits[marketN].loc['LowerGMS'].iloc[0]].count()
                
            IMI_Tmp_MIEU = MIEU[marketN][(MIEU[marketN].Status=='STD') | (MIEU[marketN].Status=='SML')]
            pos2 = IMI_Tmp_MIEU.company_full_mktcap[(IMI_Tmp_MIEU.company_full_mktcap <= GMSLimits[marketN].loc['LowerGMS'].iloc[0]) & \
                                                 (IMI_Tmp_MIEU.company_full_mktcap >= InterimMSSC_in_Millions )].count()
            ChosenCompanyRank = pos1 + pos2
        InitialMSnbC = ChosenCompanyRank     
        InitialMSSC =  MIEU[marketN].iloc[ChosenCompanyRank-1, MIEU[marketN].columns.get_loc("company_full_mktcap")]         
        InitialPctCoverage =  MIEU[marketN].iloc[ChosenCompanyRank-1, MIEU[marketN].columns.get_loc("CoverageFFMktCap")]  
        TmpInitialData = [['InitialMSnbC',InitialMSnbC],['InitialMSSC',InitialMSSC],['InitialPctCoverage',InitialPctCoverage]]
        TmpInitialData = pd.DataFrame(TmpInitialData) 
        TmpInitialData.index = TmpInitialData[0]
        TmpInitialData.drop(0, inplace=True, axis=1)
        
        InitialData[marketN]=pd.DataFrame(TmpInitialData)     
            
        # 4-  Determine if Changes in the Segment Number of Companies are Requireds
    
        blnSizeCheck=False
        blnCoverageCheck=False
        blnSizeCoverageCheck=False
        blnIncreaseRequired=False
        blnDecreaseRequired=False
            
        # Size check
        blnSizeCheck = (InitialData[marketN].loc['InitialMSSC'] >=  GMSLimits[marketN].loc['LowerGMS']) & \
        (InitialData[marketN].loc['InitialMSSC'] <=  GMSLimits[marketN].loc['UpperGMS'])
        # Coverage check
        blnCoverageCheck = (InitialData[marketN].loc['InitialPctCoverage'] >= MinCoveragecst) & \
        (InitialData[marketN].loc['InitialPctCoverage'] <= MaxCoveragecst)
        # Proximity check
        blnProximityCheck = (blnSizeCheck & (~blnCoverageCheck)) & \
        ( (InitialData[marketN].loc['InitialMSSC'] >=  GMSLimits[marketN].loc['LowerGMS']) & (InitialData[marketN].loc['InitialMSSC'] <=  GMSLimits[marketN].loc['LowerProxGMS']) ) | \
        ( (InitialData[marketN].loc['InitialMSSC'] <=  GMSLimits[marketN].loc['UpperGMS']) & (InitialData[marketN].loc['InitialMSSC'] >=  GMSLimits[marketN].loc['UpperProxGMS']) ) 
        # Size and Coverage check
        blnSizeCoverageCheck = (blnSizeCheck & blnCoverageCheck) | blnProximityCheck
    
        # 5-  Calculate Changes in the Segment Number of Companies
        
        print(marketN)
   
        RevisedMSnbC = InitialData[marketN].loc['InitialMSnbC'].iloc[0]
        RevisedMSSC = InitialData[marketN].loc['InitialMSSC'].iloc[0]
        RevisedPctCoverage =  InitialData[marketN].loc['InitialPctCoverage'].iloc[0]
        
        if (~blnSizeCoverageCheck).bool():
            # Test if Increase Required ( initialMSSC > 0.575GMS AND initialCoverage<80%) OR (initialMSSC > 1.15GMS)
            blnIncreaseRequired = (((InitialData[marketN].loc['InitialMSSC'] > GMSLimits[marketN].loc['LowerProxGMS']) & \
            (InitialData[marketN].loc['InitialPctCoverage']< MinCoveragecst)) | \
            (InitialData[marketN].loc['InitialMSSC']  > GMSLimits[marketN].loc['UpperGMS'])).bool()
            # Test if Decrease Required ( initialMSSC < 1.0GMS AND initialCoverage>90%) OR (initialMSSC < 0.5GMS)
            blnDecreaseRequired = (((InitialData[marketN].loc['InitialMSSC'] < GMSLimits[marketN].loc['UpperProxGMS']) & \
            (InitialData[marketN].loc['InitialPctCoverage'] > MaxCoveragecst)) | \
            (InitialData[marketN].loc['InitialMSSC']  <  GMSLimits[marketN].loc['LowerGMS'])).bool()
            
            # If Increase Required
            if(blnIncreaseRequired):
                print(marketN)
                print('Increase')
                # if initialMSSC > 1.15GMS
                if(InitialData[marketN].loc['InitialMSSC']  > GMSLimits[marketN].loc['UpperGMS']).bool():
                    RevisedMSnbC = MIEU[marketN].company_full_mktcap[MIEU[marketN].company_full_mktcap >= GMSLimits[marketN].loc['UpperGMS'].iloc[0]].count()             
                # if initialCoverage < 80%
                elif (InitialData[marketN].loc['InitialPctCoverage']< MinCoveragecst).bool():
                    # Check last company <80%
                    Rank1 = MIEU[marketN].company_full_mktcap[MIEU[marketN].CoverageFFMktCap < MinCoveragecst].count()
                    Rank2 = MIEU[marketN].company_full_mktcap[MIEU[marketN].company_full_mktcap >= GMSLimits[marketN].loc['LowerProxGMS'].iloc[0]].count()
                    if(Rank2 > Rank1):
                        RevisedMSnbC=Rank1 +1
                    else:
                        RevisedMSnbC=Rank2
            # end If Inccrease
                    
            # If Decrease Required
            if(blnDecreaseRequired):
                print(marketN)
                print('Decrease')
                # 5% max nb of companied to be deleted
                Max5PcntDeleteCompaniesNb = max(2,math.floor(0.05*InitialData[marketN].loc['InitialMSnbC']))
                SumFFMktCap=0
                # if initialMSSC < 0.5GMS
                if(InitialData[marketN].loc['InitialMSSC'] < GMSLimits[marketN].loc['LowerGMS']).bool():
                    for rankT in range(int(InitialData[marketN].loc['InitialMSnbC']),int(InitialData[marketN].loc['InitialMSnbC']-Max5PcntDeleteCompaniesNb),-1):
                        print(rankT)
                        if(MIEU[marketN].company_full_mktcap[rankT-1] >= GMSLimits[marketN].loc['LowerGMS']).bool():
                            break
                        RevisedMSnbC = rankT
                        SumFFMktCap = SumFFMktCap + MIEU[marketN].FFCompMktCap[rankT-1]
                # if initialCoverage > 90%                   
                elif (InitialData[marketN].loc['InitialPctCoverage'] > MaxCoveragecst).bool():
                    for rankT in range(int(InitialData[marketN].loc['InitialMSnbC']),int(InitialData[marketN].loc['InitialMSnbC']-Max5PcntDeleteCompaniesNb),-1):
                        print(rankT)
                        if( (MIEU[marketN].CoverageFFMktCap[rankT-1] <= MaxCoveragecst) | \
                                (MIEU[marketN].company_full_mktcap[rankT-1] >= GMSLimits[marketN].loc['UpperProxGMS']) ).bool():
                            if (MIEU[marketN].CoverageFFMktCap[rankT-1] <= MaxCoveragecst):
                                SumFFMktCap = SumFFMktCap + MIEU[marketN].FFCompMktCap[rankT-1]
                                RevisedMSnbC = rankT
                            
                            break
                        
                        RevisedMSnbC = rankT
                        SumFFMktCap = SumFFMktCap + MIEU[marketN].FFCompMktCap[rankT-1]
                
                # Check if we removed at least half the free float-adjusted market capitalization that lies
                # between the smallest company before the adjustment of the Initial MSnbC and the GMS Low
                RevisedMSSC = MIEU[marketN].company_full_mktcap[RevisedMSnbC-1]
                if (RevisedMSSC < GMSLimits[marketN].loc['LowerGMS'].iloc[0]):
                    GMSLowRank = MIEU[marketN].company_full_mktcap[MIEU[marketN].company_full_mktcap >= GMSLimits[marketN].loc['LowerGMS'].iloc[0]].count()
                    
                    if ( GMSLowRank > InitialData[marketN].loc['InitialMSnbC']).bool():
                        print('Invalid')
                       
                    
                    MaxSumFFMktCap = MIEU[marketN].FFCompMktCap[InitialData[marketN].loc['InitialMSnbC']-1] - \
                                     MIEU[marketN].FFCompMktCap[GMSLowRank-1]
                    
                    if( ~((RevisedMSSC> GMSLimits[marketN].loc['LowerGMS'].iloc[0]) | (SumFFMktCap >= 0.5*MaxSumFFMktCap).bool()) ):
                        MaxNbCompaniesToDelete = math.floor(0.2*InitialData[marketN].loc['InitialMSnbC'])
                        SumFFMktCapToDelete = 0
                        for rankT in range(int(InitialData[marketN].loc['InitialMSnbC']),int(InitialData[marketN].loc['InitialMSnbC']-MaxNbCompaniesToDelete),-1):
                            if (SumFFMktCapToDelete > 0.5*MaxSumFFMktCap).bool():
                                break
                            
                            SumFFMktCapToDelete = SumFFMktCapToDelete +   MIEU[marketN].FFCompMktCap[rankT-1]
                            RevisedMSnbC = rankT
            # end If Decrease
                        
            RevisedMSSC = MIEU[marketN].company_full_mktcap[RevisedMSnbC-1]
            RevisedPctCoverage = MIEU[marketN].CoverageFFMktCap[RevisedMSnbC-1]
            
            
            # end if not SizeCoverage
            
        TmpRevisedData = [['RevisedMSnbC',RevisedMSnbC],['RevisedMSSC',RevisedMSSC],['RevisedPctCoverage',RevisedPctCoverage]]
        TmpRevisedData = pd.DataFrame(TmpRevisedData) 
        TmpRevisedData.index = TmpRevisedData[0]
        TmpRevisedData.drop(0, inplace=True, axis=1)
        
        RevisedData[marketN]=pd.DataFrame(TmpRevisedData)
            
            # In market segments with a small number of companies, the deletion of the first two companies is not subject to the limits described above.
            # If the Size requirement has been met then the post SAIR MS#C is set to the new reduced MS#C, and the post SAIR MSSC is set to the Full Company Market Cap of the company ranked at this new MS#C. If not and the 5% or 20% limit on the reduction of companies has been invoked, the Full Market Cap of the smallest company in the index will remain below the lower boundary of the GMS Range. In this case, the post SAIR MSSC is set to 0.5*GMS. 

        # Check Capping
        FinalMSSC =  RevisedData[marketN].loc['RevisedMSSC']
        if(RevisedData[marketN].loc['RevisedMSSC'] < GMSLimits[marketN].loc['LowerGMS']).bool():
            FinalMSSC = GMSLimits[marketN].loc['LowerGMS']
        
        if(RevisedData[marketN].loc['RevisedMSSC'] > GMSLimits[marketN].loc['UpperGMS']).bool():
            FinalMSSC = GMSLimits[marketN].loc['UpperGMS']
        
        FinalMSnbC = RevisedData[marketN].loc['RevisedMSnbC']
        FinalPctCoverage = RevisedData[marketN].loc['RevisedPctCoverage']
        
        TmpFinalData = [['FinalMSnbC',FinalMSnbC.iloc[0]],['FinalMSSC',FinalMSSC.iloc[0]],['FinalPctCoverage',FinalPctCoverage.iloc[0]]]
        TmpFinalData = pd.DataFrame(TmpFinalData) 
        TmpFinalData.index = TmpFinalData[0]
        TmpFinalData.drop(0, inplace=True, axis=1)
            
        FinalData[marketN]=pd.DataFrame(TmpFinalData)
                
        
        
        TmpAuditCheckData = [['blnSizeCheck',blnSizeCheck],['blnCoverageCheck',blnCoverageCheck],['blnSizeCoverageCheck',blnSizeCoverageCheck],['blnIncreaseRequired',blnIncreaseRequired],['blnDecreaseRequired',blnDecreaseRequired]]
        TmpAuditCheckData = pd.DataFrame(TmpAuditCheckData) 
        TmpAuditCheckData.index = TmpAuditCheckData[0]
        TmpAuditCheckData.drop(0, inplace=True, axis=1)
        
        AuditCheckData[marketN]=pd.DataFrame(TmpAuditCheckData)
        
        
        # Restructure the Output Dictionaries
        OutputUpdateSegmNbComp[marketN]={}
        OutputUpdateSegmNbComp[marketN]['MIEU']=MIEU[marketN]
        OutputUpdateSegmNbComp[marketN]['GMSLimits']=GMSLimits[marketN]
        OutputUpdateSegmNbComp[marketN]['InterimData']=InterimData[marketN]
        OutputUpdateSegmNbComp[marketN]['InitialData']=InitialData[marketN]
        OutputUpdateSegmNbComp[marketN]['RevisedData']=RevisedData[marketN]
        OutputUpdateSegmNbComp[marketN]['FinalData']=FinalData[marketN]
        OutputUpdateSegmNbComp[marketN]['AuditCheckData']=AuditCheckData[marketN]
        
        # END OF FOR MARKET LOOP
        
    
    return OutputUpdateSegmNbComp
    