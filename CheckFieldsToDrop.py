# -*- coding: utf-8 -*-
"""
Created on Fri Jan 24 15:37:46 2020

@author: mabboud
"""

import pandas as pd

def CheckFieldsToDrop ():
    FieldsToDrop = pd.DataFrame({'DM_STD_col_nb':[5,6,8,11,12,13,14,15,16,17,20,22,23,24,26,27,28,29,30,31,33,34,36,40,41,42,43,44,45,46,47,48,49,50,51,53,54,55,56,57,58,59,60,61,62,63,64,66,69,70,71,72,73,74,75,76,77,78,79], \
            
            'DM_STD_col_name': ['sedol','cusip','ISO_country_symbol','sector','sector_next_day','industry_group','industry_group_next_day','industry','industry_next_day','sub_industry','publication_flag','value_of_quotation','price_marker','price_ISO_currency_symbol','sec_idx_eod00d_loc','price_adjustment_factor','adjustment_comments','prelim_price_adj_fact_nxt_day','adjustment_comments_next_day','eod_number_of_shares_today','closing_number_of_shares','foreign_inclusion_factor','domestic_inclusion_factor','family_std_flag','family_std_flag_next_day','family_std_dom_flag','family_std_dom_flag_next_day','reserved_12','reserved_13','family_large_flag','family_large_flag_next_day','pro_forma_family_large_flag','family_mid_flag','family_mid_flag_next_day','pro_forma_family_mid_flag','value_of_quotation_next_day','pro_forma_FIF','pro_forma_DIF','pro_forma_family_std_flag','pro_forma_family_std_dom_flag','price_return_loc','price_return_usd','gross_return_loc','gross_return_usd','net_return_intl_loc','net_return_intl_usd','initial_mkt_cap_usd','initial_mkt_cap_loc','std_IIF','std_IIF_next_day','pro_forma_std_IIF','family_large_dom_flag','family_large_dom_flag_next_day','pro_forma_family_large_dom_flg','pro_forma_sub_industry','pro_forma_eod_number_of_shares','family_mid_dom_flag','family_mid_dom_flag_next_day','pro_forma_family_mid_dom_flg'], \
            
            'DM_SML_col_nb': [5,6,8,11,12,13,14,15,16,17,20,22,23,24,26,27,28,29,30,31,33,34,36,40,41,42,43,44,45,46,47,48,49,50,51,53,54,55,56,57,58,59,60,61,62,63,64,66,69,70,71,72,73,74,75,76,77,78,79], \
            
            'DM_SML_col_name': ['sedol','cusip','ISO_country_symbol','sector','sector_next_day','industry_group','industry_group_next_day','industry','industry_next_day','sub_industry','publication_flag','value_of_quotation','price_marker','price_ISO_currency_symbol','sec_idx_eod00d_loc','price_adjustment_factor','adjustment_comments','prelim_price_adj_fact_nxt_day','adjustment_comments_next_day','eod_number_of_shares_today','closing_number_of_shares','foreign_inclusion_factor','domestic_inclusion_factor','reserved_20','reserved_21','family_scap_flag','family_scap_flag_next_day','family_small_dom_flag','family_small_dom_flag_next_day','pro_forma_family_small_dom_flg','family_micro_flag','family_micro_flag_next_day','pro_forma_family_micro_flag','reserved_1','reserved_2','value_of_quotation_next_day','pro_forma_FIF','pro_forma_DIF','pro_forma_family_scap_flag','reserved_55','price_return_loc','price_return_usd','gross_return_loc','gross_return_usd','net_return_intl_loc','net_return_intl_usd','initial_mkt_cap_usd','initial_mkt_cap_loc','scap_IIF','scap_IIF_next_day','pro_forma_scap_IIF','reserved_8','reserved_9','reserved_10','pro_forma_sub_industry','pro_forma_eod_number_of_shares','reserved_13','reserved_14','reserved_15'] })

    return FieldsToDrop