# Databricks notebook source
# commented out this markdown cell to avoid printing this in other notebooks when this notebook is called

# %md # CCU018_01-D01-parameters
 
# **Description** This notebook defines a set of parameters for use across CCU018_01. This notebook is called at the start of each notebook in the pipleine.
 
# **Author(s)** Tom Bolton (John Nolan, Elena Raffetti)

# COMMAND ----------

# MAGIC %run "/Workspaces/dars_nic_391419_j3w9t_collab/SHDS/common/functions"

# COMMAND ----------

import pyspark.sql.functions as f
import pandas as pd
import re

# COMMAND ----------

# -----------------------------------------------------------------------------
# Project
# -----------------------------------------------------------------------------
proj = 'ccu037_02'


# -----------------------------------------------------------------------------
# Databases
# -----------------------------------------------------------------------------
db = 'dars_nic_391419_j3w9t'
dbc = f'{db}_collab'


# -----------------------------------------------------------------------------
# Paths
# -----------------------------------------------------------------------------
# maternity files (to be incorporated into archive files when the archives are available - freezing for now)
# path_hes_apc_mat_1920 = f'{db}.hes_apc_mat_1920_{db}'
# path_hes_apc_mat_2021 = f'{db}.hes_apc_mat_2021_{db}'
# path_hes_apc_mat_2122 = f'{db}.hes_apc_mat_2122_{db}'

# path_hes_apc_otr_1920 = f'{db}.hes_apc_otr_1920_{db}'
# path_hes_apc_otr_2021 = f'{db}.hes_apc_otr_2021_{db}'
# path_hes_apc_otr_2122 = f'{db}.hes_apc_otr_2122_{db}'

# path_hes_apc_1920 = f'{db}.hes_apc_1920_{db}'
# path_hes_apc_2021 = f'{db}.hes_apc_2021_{db}'
# path_hes_apc_2122 = f'{db}.hes_apc_2122_{db}'

# archive tables
tmp_archive_date = '2022-03-31 00:00:00.000000'
data = [
    ['deaths',      dbc, f'deaths_{db}_archive',            tmp_archive_date,             'DEC_CONF_NHS_NUMBER_CLEAN_DEID', 'REG_DATE_OF_DEATH']
  , ['gdppr',       dbc, f'gdppr_{db}_archive',             tmp_archive_date,             'NHS_NUMBER_DEID',                'DATE']
  , ['hes_apc',     dbc, f'hes_apc_all_years_archive',      tmp_archive_date,             'PERSON_ID_DEID',                 'EPISTART'] 
  , ['hes_apc_mat', dbc, f'hes_apc_mat_all_years_archive',  tmp_archive_date,             'PERSON_ID_DEID',                 'EPISTART'] 
  , ['hes_apc_otr', dbc, f'hes_apc_otr_all_years_archive',  tmp_archive_date,             'PERSON_ID_DEID',                 'EPISTART']   
  , ['hes_op',      dbc, f'hes_op_all_years_archive',       tmp_archive_date,             'PERSON_ID_DEID',                 'APPTDATE'] 
  , ['hes_ae',      dbc, f'hes_ae_all_years_archive',       tmp_archive_date,             'PERSON_ID_DEID',                 'ARRIVALDATE'] 
  , ['pmeds',       dbc, f'primary_care_meds_{db}_archive', tmp_archive_date,             'Person_ID_DEID',                 'ProcessingPeriodDate']
  , ['hes_cc',      dbc, f'hes_cc_all_years_archive',       tmp_archive_date,             'PERSON_ID_DEID',                 'CCSTARTDATE']
  , ['chess',       dbc, f'chess_{db}_archive',             tmp_archive_date,             'PERSON_ID_DEID',                 'InfectionSwabDate']
  , ['sgss',        dbc, f'sgss_{db}_archive',              tmp_archive_date,             'PERSON_ID_DEID',                 'Specimen_Date']
  , ['sus',         dbc, f'sus_{db}_archive',               '2021-12-22 12:45:12.749112', 'NHS_NUMBER_DEID',                'EPISODE_START_DATE']
  , ['vacc',        dbc, f'vaccine_status_{db}_archive',    tmp_archive_date,             'PERSON_ID_DEID',                 'RECORDED_DATE']
]
df_archive = pd.DataFrame(data, columns = ['dataset', 'database', 'table', 'productionDate', 'idVar', 'dateVar'])
# df_archive
  # check isid dataset and table

  
# note: the below is largely listed in order of appearance within the pipeline:  

# reference tables
path_ref_bhf_phenotypes  = 'bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127'
path_ref_map_ctv3_snomed = 'dss_corporate.read_codes_map_ctv3_to_snomed'
path_ref_geog            = 'dss_corporate.ons_chd_geo_listings'
path_ref_imd             = 'dss_corporate.english_indices_of_dep_v02'
path_ref_ethnic_hes      = 'dss_corporate.hesf_ethnicity'
path_ref_ethnic_gdppr    = 'dss_corporate.gdppr_ethnicity'
path_ref_gp_refset       = 'dss_corporate.gpdata_snomed_refset_full'
path_ref_gdppr_refset    = 'dss_corporate.gdppr_cluster_refset'
path_ref_icd10           = 'dss_corporate.icd10_group_chapter_v01'

# temporary tables (1)
path_tmp_hes_apc_mat                = f'{dbc}.{proj}_tmp_hes_apc_mat'
path_tmp_gdppr_id                   = f'{dbc}.{proj}_tmp_gdppr_id'
path_tmp_hes_apc_mat_del            = f'{dbc}.{proj}_tmp_hes_apc_mat_del'
path_tmp_hes_apc_mat_del_clean      = f'{dbc}.{proj}_tmp_hes_apc_mat_del_clean'
path_tmp_flow_hes_apc_mat_del_clean = f'{dbc}.{proj}_tmp_flow_hes_apc_mat_del_clean'
path_tmp_cohort                     = f'{dbc}.{proj}_tmp_cohort'
path_tmp_cohort_del                 = f'{dbc}.{proj}_tmp_cohort_del'
path_tmp_flow_cohort                = f'{dbc}.{proj}_tmp_flow_cohort'

# in tables (available post table_freeze)
path_in_deaths        = f'{dbc}.{proj}_in_deaths_{db}_archive'
path_in_gdppr         = f'{dbc}.{proj}_in_gdppr_{db}_archive'
path_in_hes_apc       = f'{dbc}.{proj}_in_hes_apc_all_years_archive'
path_in_hes_apc_mat   = f'{dbc}.{proj}_in_hes_apc_mat_all_years_archive'
# path_in_hes_apc_otr = f'{dbc}.{proj}_in_hes_apc_otr_all_years_archive'
path_in_hes_op        = f'{dbc}.{proj}_in_hes_op_all_years_archive'
path_in_hes_ae        = f'{dbc}.{proj}_in_hes_ae_all_years_archive'
path_in_pmeds         = f'{dbc}.{proj}_in_primary_care_meds_{db}_archive'
path_in_hes_cc        = f'{dbc}.{proj}_in_hes_cc_all_years_archive'
path_in_chess         = f'{dbc}.{proj}_in_chess_{db}_archive'
path_in_sgss          = f'{dbc}.{proj}_in_sgss_{db}_archive'
path_in_sus           = f'{dbc}.{proj}_in_sus_{db}_archive'
path_in_vacc          = f'{dbc}.{proj}_in_vaccine_status_{db}_archive'

# curated tables
path_cur_hes_apc_long      = f'{dbc}.{proj}_cur_hes_apc_all_years_archive_long'
path_cur_hes_apc_oper_long = f'{dbc}.{proj}_cur_hes_apc_all_years_archive_oper_long'
path_cur_deaths_long       = f'{dbc}.{proj}_cur_deaths_{db}_archive_long'
path_cur_deaths_sing       = f'{dbc}.{proj}_cur_deaths_{db}_archive_sing'
path_cur_lsoa_region       = f'{dbc}.{proj}_cur_lsoa_region_lookup'
path_cur_lsoa_imd          = f'{dbc}.{proj}_cur_lsoa_imd_lookup'
path_cur_ethnic_desc_cat   = f'{dbc}.{proj}_cur_ethnic_desc_cat_lookup'
path_cur_covid             = f'{dbc}.{proj}_cur_covid'

# temporary tables (2)
path_tmp_skinny_unassembled = f'{dbc}.{proj}_tmp_skinny_unassembled'
path_tmp_skinny_assembled   = f'{dbc}.{proj}_tmp_skinny_assembled'
path_tmp_skinny             = f'{dbc}.{proj}_tmp_skinny'
path_tmp_quality_assurance  = f'{dbc}.{proj}_tmp_quality_assurance'
path_tmp_flow_inc_exc       = f'{dbc}.{proj}_tmp_flow_inc_exc'

# out tables
path_out_codelist           = f'{dbc}.{proj}_out_codelist'
path_out_cohort             = f'{dbc}.{proj}_out_cohort'
path_out_skinny             = f'{dbc}.{proj}_out_skinny'
path_out_covariates         = f'{dbc}.{proj}_out_covariates'
path_out_covariates_supp    = f'{dbc}.{proj}_out_covariates_supp'
path_out_exposures          = f'{dbc}.{proj}_out_exposures'
path_out_outcomes_dur_preg  = f'{dbc}.{proj}_out_outcomes_dur_preg'
path_out_outcomes_post_preg = f'{dbc}.{proj}_out_outcomes_post_preg'


# -----------------------------------------------------------------------------
# Dates
# -----------------------------------------------------------------------------
proj_preg_start_date_min = '2019-03-01' 
proj_preg_start_date_max = '2021-12-31'
proj_delivery_date_max   = '2022-10-01' # to confirm
proj_fu_date_max         = '2022-10-31' 


# -----------------------------------------------------------------------------
# Composite events
# -----------------------------------------------------------------------------
# Note: to be defined as columns within the codelist going forward
composite_events = {
  "arterial": ['AMI', 'stroke_IS', 'stroke_NOS', 'other_arterial_embolism'],
  "venous": ['PE', 'PVT', 'DVT', 'DVT_other', 'DVT_pregnancy', 'ICVT', 'ICVT_pregnancy'],
  "haematological": ['DIC', 'TTP', 'thrombocytopenia'],
  "other_cvd": ['stroke_SAH', 'stroke_HS', 'mesenteric_thrombus', 'artery_dissect', 'life_arrhythmias', 'cardiomyopathy', 'HF', 'angina', 'angina_unstable']
}


# -----------------------------------------------------------------------------
# out tables 
# -----------------------------------------------------------------------------
tmp_out_date = '20220524'
data = [
    ['codelist',           tmp_out_date]
  , ['cohort',             tmp_out_date]
  , ['skinny',             tmp_out_date]  
  , ['covariates',         tmp_out_date]  
  , ['covariates_supp',    tmp_out_date]    
  , ['exposures',          tmp_out_date]      
  , ['outcomes_dur_preg',  tmp_out_date]  
  , ['outcomes_post_preg', tmp_out_date]  
  , ['outcomes_at_birth',  tmp_out_date]  
]
df_out = pd.DataFrame(data, columns = ['dataset', 'out_date'])
# df_out

# COMMAND ----------

# function to get archive table at the specified productionDate
def get_archive_table(dataset):
  row = df_archive[df_archive['dataset'] == dataset]
  assert row.shape[0] == 1
  row = row.iloc[0]
  path = row['database'] + '.' + row['table']  
  productionDate = row['productionDate']  
  print(path + ' (' + productionDate + ')')
  tmp = spark.table(path)\
    .where(f.col('ProductionDate') == productionDate)  
  print(f'  {tmp.count():,}')
  # idVar = row['idVar']
  # count_var(tmp, idVar)
  return tmp

# COMMAND ----------

print(f'Project:')
print("  {0:<22}".format('proj') + " = " + f'{proj}') 
print(f'')
print(f'Databases:')
print("  {0:<22}".format('db') + " = " + f'{db}') 
print("  {0:<22}".format('dbc') + " = " + f'{dbc}') 
print(f'')
print(f'Paths:')
# with pd.option_context('display.max_rows', None, 'display.max_columns', None): 
#  print(df_paths_raw_data[['dataset', 'database', 'table']])
print(f'')
print(f'  df_archive')
print(df_archive[['dataset', 'database', 'table', 'productionDate']].to_string())
print(f'')
tmp = vars().copy()
for var in list(tmp.keys()):
  if(re.match('^path_.*$', var)):
    print("  {0:<22}".format(var) + " = " + tmp[var])    
print(f'')
print(f'Dates:')    
print("  {0:<22}".format('proj_preg_start_date_min') + " = " + f'{proj_preg_start_date_min}') 
print("  {0:<22}".format('proj_preg_start_date_max') + " = " + f'{proj_preg_start_date_max}') 
print("  {0:<22}".format('proj_delivery_date_max') + " = " + f'{proj_delivery_date_max}') 
print("  {0:<22}".format('proj_fu_date_max') + " = " + f'{proj_fu_date_max}') 
print(f'')
print(f'composite_events:')   
for i, c in enumerate(composite_events):
  print('  ', i, c, '=', composite_events[c])
print(f'')
print(f'Out dates:')
# with pd.option_context('display.max_rows', None, 'display.max_columns', None): 
#  print(df_paths_raw_data[['dataset', 'database', 'table']])
print(f'')
print(f'  df_out')
print(df_out[['dataset', 'out_date']].to_string())
print(f'')