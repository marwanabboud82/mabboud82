import os
import re
import shutil
import numpy as np
import pandas as pd
from pandas.tseries.offsets import BDay
from dateutil.relativedelta import relativedelta, FR, TU
from datetime import datetime
from CommonLib.path import get_datadir,get_mountdir
from CommonLib.filesys import get_fullname
from KDBData.data import DataQuery

g_data = get_datadir()

pd.options.display.max_rows = 1000
pd.options.display.max_colwidth = 200

def UnZipFile(fname):
    basefname = os.path.basename(fname)
    targetdir = get_fullname('tmp')
    try:
        os.mkdir(targetdir)
    except Exception as e:
        pass
    target = os.path.join(targetdir, basefname)
    shutil.copy(fname, target)
    os.chmod(target,0o775)
    shutil.unpack_archive(target, extract_dir=target.replace('.zip', ''), format='zip')
    return target.replace('.zip', '')

def ReadFile(filename,freq='daily'):
    """Reads MSCI file where the 95 first rows explain the content and then we have the content."""
    maxnrowheader = 250
    # We look for lines starting with *
    startsfound=[]
    f=open(filename,'rt')
    for iline in range(maxnrowheader):
        line = f.readline()
        if line.startswith('*'):
            startsfound+=[{'iline':iline,'line':line}]
    f.close()
    nrowheader = startsfound[1]['iline']
    headercolspecs = [(0, 1), (1, 5), (5, 39), (39, 70), (70, 72), (72, 75), (75, 79)]
    df = pd.read_csv(filename, sep='|', skiprows=nrowheader+3, header=None)
    df = df.iloc[:-2]  # * and #EOD
    cols = pd.read_fwf(filename, headercolspecs, nrows=nrowheader-2, skiprows=1)
    cols.columns = ['#', 'idx', 'nicename', 'name', 'type', 'length', 'length2']
    renamedict = cols.set_index('idx')['name'].to_dict()
    df = df.rename(columns=renamedict)
    strcols = cols[cols['type'] == 'S']['name'].tolist()
    for colname in strcols:
        try:
            df[colname] = df[colname].apply(lambda x: x.strip())
        except Exception :
            pass
    floatcols = cols[cols['type'] == 'N']['name'].tolist()
    for colname in floatcols:
        df[colname] = pd.to_numeric(df[colname], errors='coerce')
    return df.iloc[:,1:]

def diff_month(d1, d2):
    return (d1.year - d2.year) * 12 + d1.month - d2.month

class MSCIRules:
    """print methodology from page 39 to 70
    Europe:
     Standard 4.5% (+1.5%)
     IMI : 1.5%
     """
    asofdate = pd.to_datetime('2020-02-04')
    securityId = 'sedol_next_day'
    dmcoreonly=False
    dmonly=False
    cntrymap = None
    cfg ={
        'daily_files': {
            'dm_core': {
                'kind': 'std',
                'basezipname': lambda dt:'%sd_tdrif.zip' % dt.strftime('%m%d'),
                'filename':lambda dt:'%sD15D.RIF' % dt.strftime('%m%d'),
                'folder': 'MSCI_DM_CORE_INDEXES',
                'sedolbasezipname': lambda dt: '%sdxtdsed.zip' % dt.strftime('%m%d'),
                'sedolfilename':lambda dt:'%sD98D.SED' % dt.strftime('%m%d'),
                'comingactionsbasezipname': lambda dt: '%sdm_ace' % dt.strftime('%Y%m%d'),
            },
            'dm_small': {
                'kind': 'small',
                'basezipname': lambda dt: '%sdstdrif.zip' % dt.strftime('%m%d'),
                'filename': lambda dt: '%sD17D.RIF' % dt.strftime('%m%d'),
                'folder': 'MSCI_DM_SMALL_CAP_INDEXES',
                'sedolbasezipname': lambda dt: '%sdztdsed.zip' % dt.strftime('%m%d'),
                'sedolfilename': lambda dt: '%sD99D.SED' % dt.strftime('%m%d'),
                'comingactionsbasezipname': lambda dt: '%ssc_ace_rif' % dt.strftime('%Y%m%d'),
            },
            'em_core': {
                'kind': 'std',
                'basezipname': lambda dt: '%sd_terif.zip' % dt.strftime('%m%d'),
                'filename': lambda dt: '%sD15E.RIF' % dt.strftime('%m%d'),
                'folder': 'MSCI_EM_CORE_INDEXES',
                'sedolbasezipname': lambda dt: '%sd_tesed.zip' % dt.strftime('%m%d'),
                'sedolfilename': lambda dt: '%sD98E.SED' % dt.strftime('%m%d'),
                'actionsbasezipname': lambda dt: '%sscem_ace_rif' % dt.strftime('%m%d'),
                'comingactionsbasezipname': lambda dt: '%sem_ace_rif' % dt.strftime('%Y%m%d'),
            },
            'em_small': {
                'kind': 'small',
                'basezipname': lambda dt:'%sdsterif.zip' % dt.strftime('%m%d'),
                'filename': lambda dt: '%sD17E.RIF' % dt.strftime('%m%d'),
                'folder': 'MSCI_EM_SMALL_CAP_INDEXES',
                'sedolbasezipname': lambda dt: '%sdstesed.zip' % dt.strftime('%m%d'),
                'sedolfilename': lambda dt: '%sD99E.SED' % dt.strftime('%m%d'),
                'comingactionsbasezipname': lambda dt: '%sscem_ace_rif' % dt.strftime('%Y%m%d'),
            },
        },
        'history_files':{
            # Monthly files are base files and ATVR files
            'dm_core': {
                'kind': 'std',
                'freq':'daily',
                'basezipname': lambda sds, eds: '%s_%s_d15d_rif' % (sds, eds),
                'atvrfilename':lambda sds, eds: '%s_%s_m15d_rif' % (sds, eds),
                'folder': 'MSCI_DM_CORE_INDEXES',
            },
            'dm_small': {
                'kind': 'small',
                #'basezipname': lambda sds, eds: '%s_%s_d17d_rif' % (sds, eds),
                'basezipname': lambda sds, eds: '%s_%s_m17d_rif' % (sds, eds),
                'atvrfilename': lambda sds, eds: '%s_%s_m17d_rif' % (sds, eds),
                'folder': 'MSCI_DM_SMALL_CAP_INDEXES',
            },
            'em_core': {
                'kind': 'std',
                'basezipname': lambda sds, eds: '%s_%s_d15e_rif' % (sds, eds),
                'atvrfilename': lambda sds, eds: '%s_%s_m15e_rif' % (sds, eds),
                'folder': 'MSCI_EM_CORE_INDEXES',
            },
            'em_small': {
                'kind': 'small',
                'basezipname': lambda sds, eds: '%s_%s_d17e_rif' % (sds, eds),
                'atvrfilename': lambda sds, eds: '%s_%s_m17e_rif' % (sds, eds),
                'folder': 'MSCI_EM_SMALL_CAP_INDEXES',
            },
        }
    }
    def __init__(self,asofdate=None):
        self.asofdate=asofdate
        self.cntrymap = pd.read_csv(get_fullname('CntryRegionMap.csv'))
        ##self.histdts = pd.read_csv(get_fullname('msci_reviews_dates.csv'))
        #self.histdts['Ann'] = pd.to_datetime(self.histdts['Ann'])
        #self.histdts['Eff'] = pd.to_datetime(self.histdts['Eff'])
        self.cntrymap['ISO'] =self.cntrymap['ISO'].str.replace(' ','')

    def msci_rounding(self,x,base=5):
        if (x<15.0) and (base!=1):
            return self.msci_rounding(x,base=1)
        if x % base == 0.0:
            return x
        return (x // base + 1) * base

    def run(self):
        df = self.read_history()
        df['ISO_country_symbol'] = df['ISO_country_symbol'].str.replace(' ','')
        df = df.merge(self.cntrymap.rename(columns={'ISO':'ISO_country_symbol'}),on='ISO_country_symbol',how='left')
        return df

    def unzip_read(self,fullzipname,filename,delete=True):
        unzipfolder = UnZipFile(fullzipname)
        fullfilename = os.path.join(unzipfolder, filename)
        df_loc = ReadFile(fullfilename)
        if delete:
            shutil.rmtree(unzipfolder)
            os.remove(unzipfolder + '.zip')
        df_loc['fileFolder'] = os.path.basename(unzipfolder)
        return df_loc

    def read(s,withatvr=False):
        list_df = []
        for idx in s.cfg['daily_files'].keys():
            print(idx)
            if (s.dmcoreonly) and (idx!='dm_core'):
                continue
            if (s.dmonly) and ('em' in idx):
                continue
            # Opening MSCI Composition Files
            zipname = s.cfg['daily_files'][idx]['basezipname'](s.asofdate)
            folder = s.cfg['daily_files'][idx]['folder']
            fullzipname=os.path.join(*[get_mountdir(folder),'daily',s.asofdate.strftime('%Y'),s.asofdate.strftime('%m'), zipname])
            df_loc = s.unzip_read(fullzipname,s.cfg['daily_files'][idx]['filename'](s.asofdate))
            df_loc = s.cleanTrackerFiles(df_loc, s.cfg['daily_files'][idx]['kind'])

            # Opening Sedol file
            zipname = s.cfg['daily_files'][idx]['sedolbasezipname'](s.asofdate)
            fullzipname=os.path.join(*[get_mountdir(folder),'daily',s.asofdate.strftime('%Y'),s.asofdate.strftime('%m'), zipname])
            sed_loc = s.unzip_read(fullzipname, s.cfg['daily_files'][idx]['sedolfilename'](s.asofdate))

            if withatvr:
                # Opening ATVR last file
                # Security it takes them ten days to upload the new monthly file...
                startmonths,endmonths = s.get_sd_ed_file(s.asofdate-BDay(10),offset=1)
                basezipname = s.cfg['history_files'][idx]['atvrfilename'](startmonths,endmonths)
                fullzipname = os.path.join(*[get_mountdir(folder),'history',s.asofdate.strftime('%Y'),basezipname+'.zip'])
                atvr_loc = s.unzip_read(fullzipname, basezipname.upper())

            # Merging
            df_loc = df_loc.drop(['sedol', 'sedol_next_day', 'cusip'], axis=1)
            sed_loc = sed_loc.drop(['isin', 'bb_ticker','security_name'], axis=1)
            df_loc=df_loc.merge(sed_loc,on=['calc_date','msci_issuer_code','msci_timeseries_code','msci_security_code'],how='left')
            if withatvr:
                newcols = [x for x in atvr_loc.columns if not x in df_loc.columns]
                df_loc = df_loc.merge(atvr_loc[newcols+['msci_security_code']],on=['msci_security_code'],how='left')
            df_loc['fileFolder'] = idx
            list_df += [df_loc]
        df = pd.concat(list_df, axis=0, sort=False)
        df['ISO_country_symbol'] = df['ISO_country_symbol'].str.replace(' ', '')
        df['RIC'] = df['RIC'].str.replace(' ', '')
        df = df.merge(s.cntrymap.rename(columns={'ISO': 'ISO_country_symbol'}), on='ISO_country_symbol', how='left')
        return list_df

    def cleanTrackerFiles(self, df, name):
        """Cleaning Tracker Files"""
        if name == 'std':
            # clean STD
            df['Status'] = 'STD_SHADOW'
            df.loc[df.family_std_flag_next_day == 1, 'Status'] = 'STD'
            df['Size_Cap'] = 'STD_None'
            df.loc[df.family_large_flag_next_day == 1, 'Size_Cap'] = 'Large'
            df.loc[df.family_mid_flag_next_day == 1, 'Size_Cap'] = 'Mid'
            df['Dom_Flag'] = 0  # Domestic Market?
            df.loc[df.family_std_dom_flag_next_day == 1, 'Dom_Flag'] = 1
            df['Pro_Status'] = 'Pro_STD_SHADOW' # Pro means Proforma ie Next
            df.loc[df.pro_forma_family_std_flag == 1, 'Pro_Status'] = 'Pro_STD'
            df['IIF'] = df.std_IIF_next_day  # ??
        if name == 'small':
            # clean Small Cap files
            df['Status'] = 'SML_SHADOW'
            df.loc[df.family_micro_flag_next_day == 1, 'Status'] = 'MICRO'
            df.loc[df.family_scap_flag_next_day == 1, 'Status'] = 'SML'
            df['Size_Cap'] = 'SML_None'
            df.loc[df.family_scap_flag_next_day == 1, 'Size_Cap'] = 'Small'
            df.loc[df.family_micro_flag_next_day == 1, 'Size_Cap'] = 'Micro'
            df['Dom_Flag'] = 0
            df.loc[df.family_small_dom_flag_next_day == 1, 'Dom_Flag'] = 1
            df['Pro_Status'] = 'Pro_SML_SHADOW'
            df.loc[df.pro_forma_family_scap_flag == 1, 'Pro_Status'] = 'Pro_SML'
            df.loc[df.pro_forma_family_micro_flag == 1, 'Pro_Status'] = 'Pro_MICRO'
            df['IIF'] = df.scap_IIF_next_day
        return df


if __name__ == '__main__':
    CalcDate = pd.to_datetime('2020-09-08')
    m = MSCIRules(asofdate=CalcDate)
    m.dmcoreonly=False
    m.dmonly=False
    Tracker_DMSTD, Tracker_DMSML,Tracker_EMSTD, Tracker_EMSML =m.read()
    #print(df[['sedol','msci_security_code']].head(20))