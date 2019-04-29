#--------------------------------------------------------------------#
# File: data.py
#
# Description: 
#
#    Methods for reading and parsing data.
#
#--------------------------------------------------------------------#

#--------------------------------------------------------------------#
# libraries
#--------------------------------------------------------------------#
import re
import os
import pickle
import fnmatch

from datetime import datetime

import numpy  as np
import pandas as pd

#--------------------------------------------------------------------#
# list files method
#--------------------------------------------------------------------#
def list_files(directory, pattern):
    """alternate way to gather files matching a certain fname"""
    relevant_files = []
    all_files      = os.listdir(directory) 
    for entry in all_files:  
        if fnmatch.fnmatch(entry, pattern):
            relevant_files.append(entry)
    return relevant_files

#--------------------------------------------------------------------#
# read all files method
#--------------------------------------------------------------------#
def read_in_all_files(parent_folder, pattern):
    """bring in all inpatient files from synthetic_data folder"""
    inpatient_files = list_files(parent_folder, pattern)
    df              = pd.DataFrame()
    for d in inpatient_files:
        temp_df = pd.read_csv(parent_folder + d , compression='zip')
        temp_df['sample_number'] = re.sub('\.','',re.findall('\d{1}\.', d)[0])
        df = pd.concat([df, temp_df], axis = 0)
    return df

#--------------------------------------------------------------------#
# utility functions
#--------------------------------------------------------------------#
def grouping_helper(x, keep_list):
    if x in keep_list: return re.sub('[^A-Za-z0-9]+','',x).lower()
    else:              return 'Other'
def join_codes(row):
    return " ".join(list(set([str(v) for i, v in row.iteritems() if pd.notnull(v)])))
def join_group_codes(row):
    return " ".join(list(set([str(v)[0:3] for i, v in row.iteritems() if pd.notnull(v)])))

#--------------------------------------------------------------------#
# create inpatient dataframe
#--------------------------------------------------------------------#
def create_inpatient_core_df(df):
    """clean and create core dataset"""
    print(df.columns)
    df.columns = [c.lower() for c in df.columns]
    df = df.loc[(df['clm_from_dt'].notnull() & df['clm_thru_dt'].notnull()),:]

    df['clm_from_datetime'] = [datetime.strptime(str(int(a)),'%Y%m%d') for a in df['clm_from_dt']]
    df['clm_thru_datetime'] = [datetime.strptime(str(int(a)),'%Y%m%d') for a in df['clm_thru_dt']]
    df['clm_start_year']    = df['clm_from_datetime'].dt.year
    df['clm_start_month']   = df['clm_from_datetime'].dt.month
    df['clm_start_day']     = df['clm_from_datetime'].dt.day

    condition_list = [df['clm_utlztn_day_cnt'] <= 3,
                      (df['clm_utlztn_day_cnt'] > 3) & (df['clm_utlztn_day_cnt'] <= 7),
                      df['clm_utlztn_day_cnt'] > 7]
    choice_list = ['0-3 days', '4-7 days', 'Over 7 days']
    df['clm_utlztn_day_cnt_grouped'] = np.select(condition_list, choice_list, default = 'Other')

    # keep these key columns 
    claims_data_key_cols = ['clm_id','desynpuf_id','sample_number','clm_start_year',
                           'clm_start_month', 'clm_start_day','clm_from_datetime',
                            'clm_utlztn_day_cnt','clm_utlztn_day_cnt_grouped',
                            'prvdr_num','prvdr_num_grp','at_physn_npi','clm_drg_cd',
                            'clm_drg_cd_grp','clm_pmt_amt']

    # group major diagnosis codes
    keep_list = df.clm_drg_cd.value_counts().index[df.clm_drg_cd.value_counts().values > 100]
    df['clm_drg_cd_grp'] = [grouping_helper(r, keep_list) for r in df.clm_drg_cd]
    #df.clm_drg_cd_grp.value_counts()
    keep_list_prvdr = df.prvdr_num.value_counts().index[df.prvdr_num.value_counts().values > 100]
    df['prvdr_num_grp'] = [grouping_helper(r, keep_list_prvdr) for r in df.prvdr_num]


    icd9_dgns_cols  = [d for d in df.columns if d[:9] == ('icd9_dgns')]
    icd9_prcdr_cols = [p for p in df.columns if p[:10] == ('icd9_prcdr')]
    hcpcs_cols      = [h for h in df.columns if h[:8] == ('hcpcs_cd')]
    provider_cols   = [pv for pv in df.columns if 'physn_npi' in pv] 

    collapse_columns_list = [icd9_dgns_cols, icd9_prcdr_cols, 
                             hcpcs_cols, provider_cols]
    suffix_list = ['icd9_dgns','icd9_pcrdr','hcpcs_cd','physn_npi']

    core_df = df.loc[:,claims_data_key_cols]
    print(core_df.head())
    i = 0
    for i in range(len(collapse_columns_list)):
        print(suffix_list[i])
        print(collapse_columns_list[i])
        
        # create collapsed codes
        collapsed_codes = df.loc[:, collapse_columns_list[i]].apply(join_codes, axis = 1)
        core_df['collapsed_' + suffix_list[i]] = collapsed_codes
    
    # try and group icd9 dgns
    collapsed_icd9_dgns_group_codes = df.loc[:, icd9_dgns_cols].apply(join_group_codes, axis = 1)
    core_df['collapsed_icd9_dgns_group'] = collapsed_icd9_dgns_group_codes

    # try and group icd9 prcdr
    collapsed_icd9_prcdr_group_codes = df.loc[:, icd9_prcdr_cols].apply(join_group_codes, axis = 1)
    core_df['collapsed_icd9_prcdr_group'] = collapsed_icd9_prcdr_group_codes
        
        ## only need to uncomment if using original df as core_df
        #df.drop(columns = collapse_columns_list[i], inplace = True)
    
    return core_df

#--------------------------------------------------------------------#
# merge/link attribute methods
#--------------------------------------------------------------------#
def add_summary_info(df, data_path):
    """create keys master list"""
    filelist = list_files(directory = data_path, 
                          pattern = '*_Beneficiary_Summary_File_Sample_*')
    file_dict = dict(zip(filelist,[re.sub('\.','',re.findall('\d{1}\.', d)[0]) for d in filelist])) 
    print(file_dict)
    
    # gather and deduplicate key columns from all summary files
    k = pd.DataFrame(columns = ['desynpuf_id','bene_birth_dt', 'bene_sex_ident_cd', 'bene_race_cd', 'sample_number'])
    for sf in filelist:
        raw_df = pd.read_csv('synthetic_data/'+ sf , compression='zip')
        f = pd.DataFrame({'desynpuf_id' : raw_df['DESYNPUF_ID'],
                          'bene_birth_dt' : raw_df['BENE_BIRTH_DT'], 
                          'bene_sex_ident_cd' : raw_df['BENE_SEX_IDENT_CD'], 
                          'bene_race_cd' : raw_df['BENE_RACE_CD']})
        f['sample_number'] = re.sub('\.','',re.findall('\d{1}\.', sf)[0])
        k = pd.concat([k, f], axis = 0)
    print(k.shape)
    k.drop_duplicates(inplace = True)
    print(k.shape)
    print(k.head())

    # in a loop, clean each summary data frame associated with each sample number and attach to core keys
    rebuilt_df = pd.DataFrame()
    for n in list(set(file_dict.values())):
        filter_k = k.loc[k['sample_number']==n,:] # filter to dataframe for each sample number
        
        # iterate over the yearly summary files only relevant to the sample number n
        for s in [f for f in list(file_dict.keys()) if file_dict[f] == n]:
            raw_df = pd.read_csv('synthetic_data/'+ s , compression='zip')
            raw_df['sample_number'] = re.sub('\.','',re.findall('\d{1}\.', s)[0])
            # year specific column
            year_specific = raw_df[['SP_STATE_CODE', 'BENE_COUNTY_CD', 
                                    'BENE_DEATH_DT', 'BENE_ESRD_IND',
                                    'BENE_HI_CVRAGE_TOT_MONS', 'BENE_SMI_CVRAGE_TOT_MONS',
                                    'BENE_HMO_CVRAGE_TOT_MONS', 'PLAN_CVRG_MOS_NUM',
                                    'MEDREIMB_IP', 'BENRES_IP', 'PPPYMT_IP', 'MEDREIMB_OP', 'BENRES_OP',
                                    'PPPYMT_OP', 'MEDREIMB_CAR', 'BENRES_CAR', 'PPPYMT_CAR']]
            year_specific.columns = [(n + '_' + re.findall('\d{4}', s)[0]).lower() for n in year_specific]
            year_specific['desynpuf_id'] = raw_df['DESYNPUF_ID']

            chronic_condition_cols = [cc for cc in raw_df.columns if ((cc[:3] == ('SP_')) & (cc != 'SP_STATE_CODE'))]
            #new_chronic_condition_cols = [n + '_' + re.findall('\d{4}', s)[0] for n in chronic_condition_cols]
            for col in chronic_condition_cols:
                raw_df[col] = raw_df[col] - 1
            year_specific['chronic_condition_count_'+re.findall('\d{4}', s)[0]] = raw_df[chronic_condition_cols].sum(axis = 1)
            print(s.upper() + ' JOINER SHAPE', year_specific.shape)
            filter_k = filter_k.merge(year_specific, how='left', on='desynpuf_id')
            print(s.upper() + ' NEW K SHAPE', filter_k.shape)
                        
        # restack each portion
        rebuilt_df = pd.concat([rebuilt_df, filter_k], axis = 0)
        print('NEW REBUILT DF SHAPE: ', rebuilt_df.shape)
    
    k = rebuilt_df.drop_duplicates()
    print('DEDUPED REBUILT DF SHAPE: ', k.shape)
    
    collapsed_st = k.loc[:, [st for st in k.columns if (st[:13] == 'sp_state_code')]].apply(join_codes, axis = 1)
    k['collapsed_states'] = collapsed_st

    collapsed_ct = k.loc[:, [ct for ct in k.columns if (ct[:14] == 'bene_county_cd')]].apply(join_codes, axis = 1)
    k['collapsed_counties'] = collapsed_ct

    k['death_date'] = np.where(k['bene_death_dt_2008'].isnull(), 
                               np.where(k['bene_death_dt_2009'].isnull(), 
                                        k['bene_death_dt_2010'], 
                                        k['bene_death_dt_2009']),
                               k['bene_death_dt_2008'])
    k['death_date'] = pd.to_datetime(k['death_date'], errors = 'coerce', format = '%Y%m%d')
    k['death_month'] = k['death_date'].dt.month
    k['death_year'] = k['death_date'].dt.year
    k['death_day'] = k['death_date'].dt.day
      
    print('df shape - model df', df.shape)
    print('k shape - shape of keys df', k.shape)
    print('df head - model df', df.head())
    merged_df = df.merge(k, how='left', on=['desynpuf_id','sample_number'])
    print('merged df shape - join k to df',merged_df.shape)
    merged_df.drop_duplicates(inplace = True)
    print('merged df shape without dupes', merged_df.shape)
    
    # death logic resolves to true if claim starts after beneficiary died
    # separated just to keep things clean
    death_logic = ((merged_df['death_date'].isnull()==False) & # died
                   (merged_df['death_year'] >= merged_df['clm_start_year']) & # year of death > = year of claim
                   (merged_df['death_month'] >= merged_df['clm_start_month']) & # month of death > = month of claim
                   (merged_df['death_day'] > merged_df['clm_start_day'])) # day of death > than day of claim
    merged_df['death_before_claim_ind'] = np.where(death_logic,1,0)
    
    return merged_df