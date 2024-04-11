# Databricks notebook source
# MAGIC %md # CCU037 - cohort to include pregnancy flag 
# MAGIC 
# MAGIC Code Obtained from CCU018_01-D03-cohort
# MAGIC  
# MAGIC **Description** This notebook creates a cohort table - identifying delivery records and extracts key information from the delivery record.
# MAGIC  
# MAGIC **Original Author(s)** Tom Bolton (John Nolan, Elena Raffetti)
# MAGIC 
# MAGIC **Modified by** Marta Pineda

# COMMAND ----------

# TODO:
# 20220402. Katie Harron mentioned MBL could lead to lower missingness for GESTAT

# COMMAND ----------

spark.sql('CLEAR CACHE')

# COMMAND ----------

# DBTITLE 1,Libraries
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Window

from functools import reduce

import databricks.koalas as ks
import pandas as pd
import numpy as np

import re
import io
import datetime

import matplotlib
import matplotlib.pyplot as plt
from matplotlib import dates as mdates
import seaborn as sns

print("Matplotlib version: ", matplotlib.__version__)
print("Seaborn version: ", sns.__version__)
_datetimenow = datetime.datetime.now() # .strftime("%Y%m%d")
print(f"_datetimenow:  {_datetimenow}")

# COMMAND ----------

# DBTITLE 1,Functions
# MAGIC %run "/Workspaces/dars_nic_391419_j3w9t_collab/SHDS/common/functions"

# COMMAND ----------

# MAGIC %md # 0 Parameters

# COMMAND ----------

# MAGIC %run "/Workspaces/dars_nic_391419_j3w9t_collab/CCU037_01/Pregnancy_flag_parameters/CCU018_01-D01-parameters (pregnancy flag)"

# COMMAND ----------

# MAGIC %md # 1 Data

# COMMAND ----------

# get archive tables at the ProductionDate specified in the parameters notebook (pre table freeze)
hes_apc     = get_archive_table('hes_apc')
hes_apc_mat = get_archive_table('hes_apc_mat')
gdppr       = get_archive_table('gdppr')
deaths      = get_archive_table('deaths')

# COMMAND ----------

# MAGIC %md # 2 Prepare

# COMMAND ----------

# MAGIC %md ## 2.1 hes_apc and hes_apc_mat

# COMMAND ----------

# check financial years
tmpt = tab(hes_apc, 'FYEAR'); print()
tmpt = tab(hes_apc_mat, 'FYEAR'); print()

# filter to relevant financial years
_list_fyears = ['1920', '2021', '2122']
_hes_apc = hes_apc\
  .where(f.col('FYEAR').isin(_list_fyears))
_hes_apc_mat = hes_apc_mat\
  .where(f.col('FYEAR').isin(_list_fyears))

# check financial years
tmpt = tab(_hes_apc, 'FYEAR'); print()
tmpt = tab(_hes_apc_mat, 'FYEAR'); print()

# restrict the number of columns in _hes_apc
# rename columns that are also contained in _hes_apc_mat prior to merging
vlist = ['FYEAR'] 
vlist.extend(['EPIKEY', 'PERSON_ID_DEID']) 
vlist.extend(['EPISTART', 'ADMIDATE', 'EPIEND', 'DISDATE'])
vlist.extend(['EPIORDER', 'EPISTAT', 'EPITYPE'])
vlist.extend(['SEX', 'MYDOB', 'STARTAGE', 'STARTAGE_CALC'])
vlist.extend(['DIAG_4_CONCAT', 'OPERTN_4_CONCAT'])
vlist.extend(['PROCODE3', 'PROCODE5', 'SITETRET'])
_hes_apc = _hes_apc\
  .select(vlist)\
  .withColumnRenamed('FYEAR', 'FYEAR_hes_apc')\
  .withColumnRenamed('PERSON_ID_DEID', 'PERSON_ID_DEID_hes_apc')

# check for missing and duplicate keys before merging
count_var(_hes_apc, 'EPIKEY'); print()
count_var(_hes_apc_mat, 'EPIKEY'); print()

# check for common vars before merging
tmp1 = pd.DataFrame({'varname':_hes_apc.columns})
tmp2 = pd.DataFrame({'varname':_hes_apc_mat.columns})
tmpm = pd.merge(tmp1, tmp2, on='varname', how='outer', validate="one_to_one", indicator=True)
vlist = tmpm[tmpm['_merge'] == 'both']['varname'].tolist()
assert vlist == ['EPIKEY']

# merge hes_apc_mat and hes_apc
tmp1 = merge(_hes_apc_mat, _hes_apc, ['EPIKEY']); print()
assert tmp1.count() == tmp1.where(f.col('_merge').isin(['both', 'right_only'])).count() 

# keep _merge == 'both' only (after having checked that there are no 'left_only')
tmp2 = tmp1\
  .where(f.col('_merge') == 'both')\
  .drop('_merge')

# check financial year agreement
tmpt = tab(tmp2, 'FYEAR', 'FYEAR_hes_apc', var2_unstyled=1); print()

# check PERSON_ID_DEID agreement
tmp2 = tmp2\
  .withColumn('_id_agree', udf_null_safe_equality('PERSON_ID_DEID', 'PERSON_ID_DEID_hes_apc').cast("integer"))
tmpt = tab(tmp2, '_id_agree'); print()
tmpt = tab(tmp2, '_id_agree', 'FYEAR', var2_unstyled=1); print()

# COMMAND ----------

# ----------------------------------------------------------------------------------
# save checkpoint
# ----------------------------------------------------------------------------------
# save name
outName = f'{proj}_tmp_hes_apc_mat'

# save previous version for comparison purposes
tmpt = spark.sql(f"""SHOW TABLES FROM {dbc}""")\
  .select('tableName')\
  .where(f.col('tableName') == outName)\
  .collect()
if(len(tmpt)>0):
  _datetimenow = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
  outName_pre = f'{outName}_pre{_datetimenow}'.lower()
  print(outName_pre)
  spark.table(f'{dbc}.{outName}').write.mode('overwrite').saveAsTable(f'{dbc}.{outName_pre}')
  spark.sql(f'ALTER TABLE {dbc}.{outName_pre} OWNER TO {dbc}')

# save
tmp2.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

# COMMAND ----------

# repoint
# rename ID (use hes_apc ID)
_hes_apc_mat = spark.table(f'{dbc}.{proj}_tmp_hes_apc_mat')\
  .withColumnRenamed('PERSON_ID_DEID', 'PERSON_ID_DEID_hes_apc_mat')\
  .withColumnRenamed('PERSON_ID_DEID_hes_apc', 'PERSON_ID_DEID')

# COMMAND ----------

# check
display(_hes_apc_mat)

# COMMAND ----------

# MAGIC %md ## 2.2 gdppr_id

# COMMAND ----------

# distinct ID
_gdppr_id = gdppr\
  .select('NHS_NUMBER_DEID')\
  .distinct()

# check
count_var(_gdppr_id, 'NHS_NUMBER_DEID')

# COMMAND ----------

# ----------------------------------------------------------------------------------
# save checkpoint
# ----------------------------------------------------------------------------------
# save name
outName = f'{proj}_tmp_gdppr_id'.lower()

# save previous version for comparison purposes
tmpt = spark.sql(f"""SHOW TABLES FROM {dbc}""")\
  .select('tableName')\
  .where(f.col('tableName') == outName)\
  .collect()
if(len(tmpt)>0):
  _datetimenow = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
  outName_pre = f'{outName}_pre{_datetimenow}'.lower()
  print(outName_pre)
  spark.table(f'{dbc}.{outName}').write.mode('overwrite').saveAsTable(f'{dbc}.{outName_pre}')
  spark.sql(f'ALTER TABLE {dbc}.{outName_pre} OWNER TO {dbc}')

# save
_gdppr_id.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

# COMMAND ----------

# repoint
# rename ID
_gdppr_id = spark.table(f'{dbc}.{proj}_tmp_gdppr_id')\
  .withColumnRenamed('NHS_NUMBER_DEID', 'PERSON_ID_DEID')

# check
print(_gdppr_id.limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md ## 2.3 deaths

# COMMAND ----------

# check
count_var(deaths, 'DEC_CONF_NHS_NUMBER_CLEAN_DEID'); print()
assert dict(deaths.dtypes)['REG_DATE'] == 'string'
assert dict(deaths.dtypes)['REG_DATE_OF_DEATH'] == 'string'

# define window for the purpose of creating a row number below as per the skinny patient table
_win = Window\
  .partitionBy('PERSON_ID')\
  .orderBy(f.desc('REG_DATE'), f.desc('REG_DATE_OF_DEATH'), f.desc('S_UNDERLYING_COD_ICD10'))

# rename ID
# remove records with missing IDs
# reformat dates
# reduce to a single row per individual as per the skinny patient table
# select columns required
# rename column ahead of reshape below
# sort by ID
_deaths = deaths\
  .withColumnRenamed('DEC_CONF_NHS_NUMBER_CLEAN_DEID', 'PERSON_ID')\
  .where(f.col('PERSON_ID').isNotNull())\
  .withColumn('REG_DATE', f.to_date(f.col('REG_DATE'), 'yyyyMMdd'))\
  .withColumn('REG_DATE_OF_DEATH', f.to_date(f.col('REG_DATE_OF_DEATH'), 'yyyyMMdd'))\
  .withColumn('_rownum', f.row_number().over(_win))\
  .where(f.col('_rownum') == 1)\
  .select(['PERSON_ID', 'REG_DATE', 'REG_DATE_OF_DEATH', 'S_UNDERLYING_COD_ICD10'] + [col for col in list(deaths.columns) if re.match(r'^S_COD_CODE_\d(\d)*$', col)])\
  .withColumnRenamed('S_UNDERLYING_COD_ICD10', 'S_COD_CODE_UNDERLYING')\
  .orderBy('PERSON_ID')\
  .select('PERSON_ID', 'REG_DATE_OF_DEATH')\
  .withColumnRenamed('REG_DATE_OF_DEATH', 'DOD')\
  .withColumn('death_flag', f.lit(1))

# check
count_var(_deaths, 'PERSON_ID'); print()
count_var(_deaths, 'DOD'); print()

# TODO missing DOD, but has a death record, exclude? QA?
# numbers so small (15428832 - 15428678 = 154 [0.001%]) that will not impact this cohort

# check
print(_deaths.limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md # 3 Filter deliveries

# COMMAND ----------

# ----------------------------------------------------------------------------------
# dictionaries
# ----------------------------------------------------------------------------------
# MATERNITY_EPISODE_TYPE
# 1 = Finished delivery episode
# 2 = Finished birth episode
# 3 = Finished other delivery episode
# 4 = Finished other birth episode
# 9 = Unfinished maternity episodes
# 99 = All other episodes

# EPITYPE
# 1 = General episode (anything that is not covered by the other codes)  
# 2 = Delivery episode  
# 3 = Birth episode  
# 4 = Formally detained under the provisions of mental health legislation ...
# 5 = Other delivery event 
# 6 = Other birth event


# ----------------------------------------------------------------------------------
# checks
# ----------------------------------------------------------------------------------
# check the distribution of MATERNITY_EPISODE_TYPE by FYEAR
tmpt = tab(_hes_apc_mat, 'MATERNITY_EPISODE_TYPE', 'FYEAR', var2_unstyled=1); print()

# check the distribution of MATERNITY_EPISODE_TYPE by EPITYPE
tmpt = tab(_hes_apc_mat, 'MATERNITY_EPISODE_TYPE', 'EPITYPE', var2_unstyled=1); print()


# ----------------------------------------------------------------------------------
# filter
# ----------------------------------------------------------------------------------
# exclude birth episodes (i.e., babies) and restrict to deliveries (i.e., mothers)
# ER to confirm the exclusion of unfinished maternity episodes
_hes_apc_mat_del = _hes_apc_mat\
  .where(f.col('MATERNITY_EPISODE_TYPE').isin([1]))


# ----------------------------------------------------------------------------------
# checks
# ----------------------------------------------------------------------------------
# check completeness and uniqueness of person ID (and spell ID)
# overall
count_var(_hes_apc_mat_del, 'PERSON_ID_DEID'); print()
# count_var(hes_apc_mat_del, 'SUSSPELLID')

# per financial year
fylist = ['1920', '2021', '2122']
for fy in fylist:
  print(fy)
  tmp = _hes_apc_mat_del\
    .where(f.col('FYEAR') == fy)  
  count_var(tmp, 'PERSON_ID_DEID'); print()
  # count_var(tmp, 'SUSSPELLID')

# COMMAND ----------

# check
display(_hes_apc_mat_del)

# COMMAND ----------

# ----------------------------------------------------------------------------------
# save checkpoint
# ----------------------------------------------------------------------------------
# save name
outName = f'{proj}_tmp_hes_apc_mat_del' # was _hes_apc_mat_del

# save previous version for comparison purposes
tmpt = spark.sql(f"""SHOW TABLES FROM {dbc}""")\
  .select('tableName')\
  .where(f.col('tableName') == outName)\
  .collect()
if(len(tmpt)>0):
  _datetimenow = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
  outName_pre = f'{outName}_pre{_datetimenow}'.lower()
  print(outName_pre)
  spark.table(f'{dbc}.{outName}').write.mode('overwrite').saveAsTable(f'{dbc}.{outName_pre}')
  spark.sql(f'ALTER TABLE {dbc}.{outName_pre} OWNER TO {dbc}')

# save
_hes_apc_mat_del.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

# COMMAND ----------

# MAGIC %md # 4 Clean deliveries

# COMMAND ----------

#Saved in ccu037_01 for "Pregnancy_flag_parameters". If it works, modify the location!

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU037_01/Pregnancy_flag_parameters/CCU018_01-D03a-cohort_deliveries_clean (pregnancy flag)

# COMMAND ----------

# load
spark.sql(f"""REFRESH TABLE {path_tmp_hes_apc_mat_del_clean}""")
hes_apc_mat_del_clean = spark.table(path_tmp_hes_apc_mat_del_clean)

# check
count_var(hes_apc_mat_del_clean, 'PERSON_ID_DEID'); print()
tmpt = tab(hes_apc_mat_del_clean.where(f.col('_rownum') == 1), '_rownummax'); print() #, '_delivery_date_diff_min_ge_210', var2_unstyled=1); print()

# restrict columns
vlist = ['PERSON_ID_DEID'
  , 'PERSON_ID_DEID_hes_apc_mat'
  , '_id_agree'       
  , 'EPIKEY'
  , 'FYEAR'
  , '_rownum'
  , '_rownummax'
  , 'delivery_date'
  , 'GESTAT_1'
  , 'BIRWEIT_1'
  , 'NUMBABY'
  , 'NUMTAILB'
  , 'NUMPREG'
  , 'DELONSET'
  ]
tmp1 = hes_apc_mat_del_clean\
  .select(vlist)

# COMMAND ----------

# MAGIC %md # 5 Add dates

# COMMAND ----------

print('---------------------------------------------------------------------------------')
print('add Date of Death (DOD)')
print('---------------------------------------------------------------------------------')
# check
count_var(tmp1, 'PERSON_ID_DEID'); print()

# merge in deaths 
tmp1 = merge(tmp1, _deaths.withColumnRenamed('PERSON_ID', 'PERSON_ID_DEID'), ['PERSON_ID_DEID']); print()

# filter to left_only and both (equivalent to left join)
tmp1 = tmp1\
  .where(f.col('_merge').isin(['left_only', 'both']))\
  .drop('_merge')

# check
count_var(tmp1, 'PERSON_ID_DEID'); print()

# update vlist
vlist = vlist + ['death_flag', 'DOD']


print('---------------------------------------------------------------------------------')
print('add pregnancy start date')
print('---------------------------------------------------------------------------------')
# check GESTAT_1 missingness (by financial year)
tmpt = tab(hes_apc_mat_del_clean, 'GESTAT_1'); print()
tmpt = tab(hes_apc_mat_del_clean, 'GESTAT_1', 'FYEAR', var2_unstyled=1); print()

# To consider: can we use references to improve estimation based on the pregnancy type (c.f., 40 weeks for all)?
tmp1 = tmp1\
  .withColumn('preg_start_date_estimated',\
    f.when((f.col('GESTAT_1').isNull()) | (f.col('GESTAT_1') == 99), 1).otherwise(0)\
  )\
  .withColumn('GESTAT_1_tmp',\
    f.when(f.col('preg_start_date_estimated') == 1, 40).otherwise(f.col('GESTAT_1'))\
  )\
  .withColumn('preg_start_date', f.expr('date_add(delivery_date, -7*GESTAT_1_tmp)'))

# check consistency with previous delivery date for individuals with multiple delivery events, noting
#   "You can get pregnant as little as 3 weeks after the birth of a baby, even if you're breastfeeding and your periods haven't started again." 
#   https://www.nhs.uk/conditions/baby/support-and-services/sex-and-contraception-after-birth/
#   Using 3 weeks as cut off

# define window
_win = Window\
  .partitionBy('PERSON_ID_DEID')\
  .orderBy('_rownum')

# add lagged delivery date plus 3 weeks 
# add indicator for when pregnancy start is earlier than the lagged delivery date plus 3 weeks
# add days difference between pregnancy start and lagged delivery date plus 3 weeks, where indicator is 1
tmp1 = tmp1\
  .withColumn('_delivery_date_lag1', f.lag(f.col('delivery_date')).over(_win))\
  .withColumn('_delivery_date_lag1_p21d', f.expr('date_add(_delivery_date_lag1, 21)'))\
  .withColumn('_tmp_ind', f.when(f.col('preg_start_date') < f.col('_delivery_date_lag1_p21d'), 1).otherwise(0))\
  .withColumn('_tmp_diff', f.when(f.col('_tmp_ind') == 1, f.datediff(f.col('preg_start_date'), f.col('_delivery_date_lag1_p21d'))).otherwise(0))

# check
tmpt = tab(tmp1, '_rownum', '_tmp_ind', var2_unstyled=1); print()
tmpt = tab(tmp1, 'preg_start_date_estimated', '_tmp_ind', var2_unstyled=1); print()
tmpt = tabstat(tmp1.where(f.col('_tmp_ind') == 1), '_tmp_diff'); print()
tmpt = tab(tmp1, '_tmp_diff', '_tmp_ind', var2_unstyled=1); print()
tmpt = tab(tmp1, '_tmp_diff', 'preg_start_date_estimated', var2_unstyled=1); print()

# correct pregnancy start date and create indicator 
# where the pregnancy start is earlier than the preceeding delivery date plus 3 weeks
# reduce columns
vlist = vlist + ['preg_start_date', 'preg_start_date_estimated', 'preg_start_date_corrected']
tmp1 = tmp1\
  .withColumn('preg_start_date', f.when(f.col('_tmp_ind') == 1, f.col('_delivery_date_lag1_p21d')).otherwise(f.col('preg_start_date')))\
  .withColumnRenamed('_tmp_ind', 'preg_start_date_corrected')\
  .select(vlist)

# check
tmpt = tab(tmp1, 'preg_start_date_estimated', 'preg_start_date_corrected', var2_unstyled=1); print()


print('---------------------------------------------------------------------------------')
print('add follow up (fu) date')
print('---------------------------------------------------------------------------------')
print(f'proj_fu_date_max = {proj_fu_date_max}'); print()
# add follow up date, censoring for subsequent pregnancy start date for individuals with multiple delivery events

# update vlist
vlist = vlist + ['fu_end_date', 'fu_days']

# add delivery date plus 365 days
# add follow up date max from parameters
# add subsequent pregnancy start date
# take rowwise minimum of the above
# add days difference between rowwise minimum and delivery date
# reduce columns
tmp1 = tmp1\
  .withColumn('_delivery_date_p365', f.expr('date_add(delivery_date, 365)'))\
  .withColumn('_proj_fu_end_date', f.to_date(f.lit(proj_fu_date_max), 'yyyy-MM-dd'))\
  .withColumn('_preg_start_date_lagm1', f.lag(f.col('preg_start_date'), -1).over(_win))\
  .withColumn('fu_end_date',\
    f.least(
      '_delivery_date_p365'
      , '_proj_fu_end_date'
      , '_preg_start_date_lagm1' 
    )\
  )\
  .withColumn('fu_days', f.datediff(f.col('fu_end_date'), f.col('delivery_date')))\
  .select(vlist)

# check
tmpt = tabstat(tmp1, 'fu_days'); print()

# cache for compare_files below
tmp1.cache()
print(f'{tmp1.count():,}'); print()


print('---------------------------------------------------------------------------------')
print('censor for deaths')
print('---------------------------------------------------------------------------------')
# add indicator for when DOD is null
# add indicator for when DOD < delivery date
# add indicator for when DOD < follow up end date
tmp2 = tmp1\
  .withColumn('_dod_null', f.when((f.col('death_flag') == 1) & (f.col('DOD').isNull()), 1))\
  .withColumn('_dod_lt_delivery_date', f.when(f.col('DOD') < f.col('delivery_date'), 1))\
  .withColumn('_dod_lt_fu_end_date', f.when(f.col('DOD') < f.col('fu_end_date'), 1))

# check
tmpt = tab(tmp2, '_dod_null', 'death_flag', var2_unstyled=1); print()
assert tmp2.select('_dod_null').where(f.col('_dod_null') == 1).count() == 0

tmpt = tab(tmp2, '_dod_lt_delivery_date', 'death_flag', var2_unstyled=1); print()
assert tmp2.select('_dod_lt_delivery_date').where(f.col('_dod_lt_delivery_date') == 1).count() == 2

tmpt = tab(tmp2, '_dod_lt_fu_end_date', 'death_flag', var2_unstyled=1); print()

count_var(tmp2, 'DOD'); print()

# set DOD to missing where this was before the delivery date
tmp2 = tmp2\
  .withColumn('DOD', f.when(f.col('_dod_lt_delivery_date') == 1, f.lit(None)).otherwise(f.col('DOD')))\
  .drop('_dod_null', '_dod_lt_delivery_date', '_dod_lt_fu_end_date')

# check
count_var(tmp2, 'DOD'); print()

# recalculate follow up end date with DOD included
tmp2 = tmp2\
  .withColumn('_delivery_date_p365', f.expr('date_add(delivery_date, 365)'))\
  .withColumn('_proj_fu_end_date', f.to_date(f.lit(proj_fu_date_max), 'yyyy-MM-dd'))\
  .withColumn('_preg_start_date_lagm1', f.lag(f.col('preg_start_date'), -1).over(_win))\
  .withColumn('fu_end_date',\
    f.least(
      '_delivery_date_p365'
      , '_proj_fu_end_date'
      , '_preg_start_date_lagm1' 
      , 'DOD' 
    )\
  )\
  .withColumn('fu_days', f.datediff(f.col('fu_end_date'), f.col('delivery_date')))\
  .drop('_delivery_date_p365', '_proj_fu_end_date', '_preg_start_date_lagm1')

# cache for compare_files below
tmp2.cache()
print(f'{tmp2.count():,}'); print()


print('---------------------------------------------------------------------------------')
print('compare files - impact of censoring for death')
print('---------------------------------------------------------------------------------')
old = tmp1
new = tmp2
file1, file2, file3, file3_differences = compare_files(old, new, ['PERSON_ID_DEID', '_rownum'], warningError=0); print()

# check
tmp = file3_differences\
  .where(f.col('varname') == 'fu_days')\
  .withColumn('_diff', f.col('_TWO') - f.col('_ONE'))
tmpt = tabstat(tmp, '_diff'); print()

# temporary step to avoid having to change code below
tmp1 = tmp2

# COMMAND ----------

# check
display(tmp1)

# COMMAND ----------

# MAGIC %md # 6 Project inclusion criteria

# COMMAND ----------

print('---------------------------------------------------------------------------------')
print('delivery date <= proj_fu_date_max (pre20220808 was project delivery date maximum)')
print('---------------------------------------------------------------------------------')
print(f'proj_delivery_date_max = {proj_delivery_date_max}'); print()
print(f'proj_fu_date_max = {proj_fu_date_max}'); print()

# check
tmpc = count_var(tmp1, 'PERSON_ID_DEID', ret=1, df_desc='original', indx=7); print()

# filter delivery_date
tmp1a = tmp1\
  .where(f.col('delivery_date') <= f.to_date(f.lit(proj_fu_date_max), 'yyyy-MM-dd'))

# check
tmpt = count_var(tmp1a, 'PERSON_ID_DEID', ret=1, df_desc='post delivery date filter', indx=8); print()
tmpc = tmpc.unionByName(tmpt)
tmpt = tabstat(tmp1a, 'delivery_date', date=1); print()
tmpt = tabstat(tmp1a, 'fu_days'); print()


print('---------------------------------------------------------------------------------')
print('project pregnancy start date min <= pregnancy start date <= project pregnancy start date max')
print('---------------------------------------------------------------------------------')
print(f'proj_preg_start_date_min = {proj_preg_start_date_min}, proj_preg_start_date_max = {proj_preg_start_date_max}'); print()

# filter preg_start_date
tmp2 = tmp1a\
  .where((f.col('preg_start_date') >= proj_preg_start_date_min) & (f.col('preg_start_date') <= proj_preg_start_date_max))

# check
tmpt = count_var(tmp2, 'PERSON_ID_DEID', ret=1, df_desc='post pregnancy start date filter', indx=9); print()
tmpc = tmpc.unionByName(tmpt)
tmpt = tabstat(tmp2, 'preg_start_date', date=1); print()


print('---------------------------------------------------------------------------------')
print('anchor on gdppr')
print('---------------------------------------------------------------------------------')
# only include patients with records in GDPPR

# check
count_var(_gdppr_id, 'PERSON_ID_DEID'); print()

# merge
tmp3 = merge(tmp2, _gdppr_id, ['PERSON_ID_DEID']); print()

# check
tmpt = tabstat(tmp3, var='delivery_date', byvar='_merge', date=1); print()

# filter to patients that are in GDPPR
tmp3 = tmp3\
  .where(f.col('_merge') == 'both')\
  .drop('_merge')

# check
tmpt = count_var(tmp3, 'PERSON_ID_DEID', ret=1, df_desc='post gdppr anchor', indx=10); print()
tmpc = tmpc.unionByName(tmpt)


print('---------------------------------------------------------------------------------')
print('first delivery only')
print('---------------------------------------------------------------------------------')
# Note: filter for first pregnancy after censoring follow-up for subsequent pregnancy above)
# Recreate a row number variable to identify multiple records by ID
# Note: no longer need "'EPISTART', 'ADMIDATE', 'EPIEND', 'DISDATE', 'EPIORDER', 'EPIKEY'", since delivery_date is well-defined with intervals of >210 days

# define windows for row number and row number max
_win_rownum = Window\
  .partitionBy('PERSON_ID_DEID')\
  .orderBy(['delivery_date'])
_win_rownummax = Window\
  .partitionBy('PERSON_ID_DEID')

# create _rownum and _rownummax
tmp3 = tmp3\
  .withColumn('_rownum', f.row_number().over(_win_rownum))\
  .withColumn('_rownummax', f.count('PERSON_ID_DEID').over(_win_rownummax))\
  .withColumnRenamed('PERSON_ID_DEID', 'PERSON_ID')\
  .withColumnRenamed('PERSON_ID_DEID_hes_apc_mat', 'PERSON_ID_hes_apc_mat')\
  .orderBy('PERSON_ID', '_rownum')

# check
tmpt = tab(tmp3.where(f.col('_rownum') == 1), '_rownummax'); print()
tmpt = tab(tmp3, '_rownum'); print()

# filter to first delivery
tmp4 = tmp3\
  .where(f.col('_rownum') == 1)

# check
tmpt = count_var(tmp4, 'PERSON_ID', ret=1, df_desc='post first delivery', indx=11); print()
tmpc = tmpc.unionByName(tmpt)

# COMMAND ----------

# MAGIC %md # 7 Flow diagram

# COMMAND ----------

# combine with deliveries clean flow
spark.sql(f"""REFRESH TABLE {path_tmp_flow_hes_apc_mat_del_clean}""")
tmpc1 = spark.table(path_tmp_flow_hes_apc_mat_del_clean)\
  .unionByName(tmpc)\
  .withColumn('indx', f.col('indx').cast(t.IntegerType()))\
  .orderBy('indx')\
  .where(~((f.col('indx') == 7) & (f.col('df_desc') == 'original')))

# check
print(tmpc1.toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md # 8 Save

# COMMAND ----------

vlist_cohort = ['PERSON_ID'
  , 'PERSON_ID_hes_apc_mat'
  , '_id_agree'
  , 'EPIKEY'
  , 'preg_start_date'
  , 'preg_start_date_estimated'
  , 'preg_start_date_corrected'
  , 'delivery_date'
  , 'fu_end_date'
  , 'fu_days'
  ]
vlist_delivery = ['PERSON_ID', 'PERSON_ID_hes_apc_mat', '_id_agree', 'EPIKEY'] + [v for v in tmp4.columns if v not in vlist_cohort + ['_rownum', '_rownummax']]

# check
print('vlist_cohort: ', vlist_cohort)
print('vlist_delivery: ', vlist_delivery)

# cohort
tmp4a = tmp4\
  .select(vlist_cohort)

# additional delivery variables
tmp4b = tmp4\
  .select(vlist_delivery)

# COMMAND ----------

# check final
display(tmp4a)

# COMMAND ----------

# check final
display(tmp4b)

# COMMAND ----------

# MAGIC %md ## 8.1 First delivery cohort

# COMMAND ----------

# save name
# 20220807 changed from _out_cohort to _tmp_cohort (_out_cohort will be created after inclusion exclusion)
outName = f'{proj}_tmp_cohort'.lower()

# save previous version for comparison purposes
_datetimenow = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
outName_pre = f'{outName}_pre{_datetimenow}'.lower()
#silence if not working:
print(outName_pre)
#spark.table(f'{dbc}.{outName}').write.mode('overwrite').saveAsTable(f'{dbc}.{outName_pre}')
#spark.sql(f'ALTER TABLE {dbc}.{outName_pre} OWNER TO {dbc}')

# save
print(outName)
tmp4a.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

# COMMAND ----------

# save name
outName = f'{proj}_tmp_cohort_del'.lower()

# save previous version for comparison purposes
tmpt = spark.sql(f"""SHOW TABLES FROM {dbc}""")\
  .select('tableName')\
  .where(f.col('tableName') == outName)\
  .collect()
if(len(tmpt)>0):
  _datetimenow = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
  outName_pre = f'{outName}_pre{_datetimenow}'.lower()
  print(outName_pre)
  spark.table(f'{dbc}.{outName}').write.mode('overwrite').saveAsTable(f'{dbc}.{outName_pre}')
  spark.sql(f'ALTER TABLE {dbc}.{outName_pre} OWNER TO {dbc}')
  
# save
tmp4b.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

# COMMAND ----------

outName = f'{proj}_tmp_cohort_del'.lower()
print(outName)

# COMMAND ----------

# MAGIC %md ## 8.2 Flow

# COMMAND ----------

# save name
outName = f'{proj}_tmp_flow_cohort'.lower()

# save previous version for comparison purposes
tmpt = spark.sql(f"""SHOW TABLES FROM {dbc}""")\
  .select('tableName')\
  .where(f.col('tableName') == outName)\
  .collect()
if(len(tmpt)>0):
  _datetimenow = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
  outName_pre = f'{outName}_pre{_datetimenow}'.lower()
  print(outName_pre)
  spark.table(f'{dbc}.{outName}').write.mode('overwrite').saveAsTable(f'{dbc}.{outName_pre}')
  spark.sql(f'ALTER TABLE {dbc}.{outName_pre} OWNER TO {dbc}')

# save
tmpc1.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

# COMMAND ----------

outName = f'{proj}_tmp_flow_del'.lower()
print(outName)

# COMMAND ----------

# MAGIC %md ## 8.3 Multiple deliveries cohort

# COMMAND ----------

# data for JN webinar plots (also reproduced below)
tmp3a = tmp3\
  .select(vlist_cohort + ['_rownum', '_rownummax'])

# check
tmpt = tab(tmp3a.where(f.col('_rownum') == 1), '_rownummax'); print()
tmpt = tabstat(tmp3a, var='delivery_date', byvar='_rownum', date=1); print()

# COMMAND ----------

# check
display(tmp3a)

# COMMAND ----------

# check 
tmp3b = tmp3a\
  .withColumn('date_ym', f.date_format(f.col('preg_start_date'), 'yyyy-MM'))\
  .groupBy('date_ym', '_rownum')\
  .agg(\
    f.count(f.lit(1)).alias('n')\
  )\
  .orderBy('date_ym', f.desc('_rownum'))

display(tmp3b)

# COMMAND ----------

# # save name
# outName = f'{proj}_tmp_cohort_multiple'

# # save previous version for comparison purposes
# _datetimenow = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
# outName_pre = f'{outName}_pre{_datetimenow}'.lower()
# print(outName_pre)
# spark.table(f'{dbc}.{outName}').write.mode('overwrite').saveAsTable(f'{dbc}.{outName_pre}')
# spark.sql(f'ALTER TABLE {dbc}.{outName_pre} OWNER TO {dbc}')

# # save
# tmp3a.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
# spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

# COMMAND ----------

# MAGIC %md # 9 Inner join with GDPPR patients from ccu037_02 cohort

# COMMAND ----------

# MAGIC %md ## 9.1 Load tables
# MAGIC - cohort   = baseline cohort with individuals diagnosed with COVID-19 as defined in ccu037_02 objective
# MAGIC - cohort02 = patients from cohort including date of covid-19 diagnosis 
# MAGIC - preg = pregnancy table including preg_start_date and delivery_date

# COMMAND ----------

#cohort = spark.table("dars_nic_391419_j3w9t_collab.ccu037_02_cohort")
cohort02 = spark.table("dars_nic_391419_j3w9t_collab.ccu037_follow_up_availability")
preg = spark.table("dars_nic_391419_j3w9t_collab.ccu037_02_tmp_cohort")

# COMMAND ----------

display(cohort02)

# COMMAND ----------

display(preg)

# COMMAND ----------

# MAGIC %md ## 9.2 Inner join
# MAGIC 
# MAGIC Extract individuals from our cohort02 that are found in preg table. 

# COMMAND ----------

# repoint
# rename ID from preg
preg2 = preg\
  .withColumnRenamed('PERSON_ID', 'NHS_NUMBER_DEID')

# repoint
# rename ID directly from stored tables (bdc and proj are defined in study parameters).
#preg2 = spark.table(f'{dbc}.{proj}_tmp_cohort_del')\
#  .withColumnRenamed('PERSON_ID', 'NHS_NUMBER_DEID')

preg3 = merge(cohort02,preg2,['NHS_NUMBER_DEID']);print()

# COMMAND ----------

# filter to both (equivalent to inner join)
preg4 = preg3\
  .where(f.col('_merge').isin(['both']))\
  .drop('_merge')

preg4.count()
# filter to left_only and both (equivalent to left join)
#preg4 = preg3\
#  .where(f.col('_merge').isin(['left_only', 'both']))\
#  .drop('_merge')

# COMMAND ----------

display(preg4)

# COMMAND ----------

# MAGIC %md ## 9.3 Incorporate pregnancy flag
# MAGIC 
# MAGIC Pregnancy flag: Defined as being pregnant when positive for COVID-19. (i.e., individuals from cohort02 who were pregnant at the moment of COVID-19 diagnosis)

# COMMAND ----------

# filter to being pregnant when diagnosed for covid
preg5 = preg4\
  .where((f.col('covid_date') > f.col('preg_start_date')) & (f.col('covid_date')<f.col('delivery_date')))

preg5.count()

# COMMAND ----------

preg6 = preg5\
  .withColumn('preg_flag', f.lit(1))\
  .drop('follow_up_90d','follow_up_180d','follow_up_1y','follow_up_2y') 

display(preg6)

# COMMAND ----------

# MAGIC %md ## 9.4 Save table with pregnancy flag

# COMMAND ----------

# save name
# 20220807 changed from _out_cohort to _tmp_cohort (_out_cohort will be created after inclusion exclusion)
outName = f'{proj}_pregnancy_flag'.lower()

# save previous version for comparison purposes
_datetimenow = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
outName_pre = f'{outName}_pre{_datetimenow}'.lower()
#silence if not working:
print(outName_pre)
#spark.table(f'{dbc}.{outName}').write.mode('overwrite').saveAsTable(f'{dbc}.{outName_pre}')
#spark.sql(f'ALTER TABLE {dbc}.{outName_pre} OWNER TO {dbc}')

# save
print(outName)
preg6.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')