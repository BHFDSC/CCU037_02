# Databricks notebook source
# commented out this markdown cell to avoid printing this in other notebooks when this notebook is called

# %md # CCU018_01-D03a-cohort_deliveries_clean
 
# **Description** This notebook ...
 
# **Author(s)** Tom Bolton (John Nolan, Elena Raffetti)

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

# MAGIC %run "/Workspaces/dars_nic_391419_j3w9t_collab/CCU037_01/delete/CCU018_01-D01-parameters (pregnancy flag)"

# COMMAND ----------

# MAGIC %md # 1 Data

# COMMAND ----------

spark.sql(f"""REFRESH TABLE {path_tmp_hes_apc_mat_del}""")
hes_apc_mat_del = spark.table(f'{path_tmp_hes_apc_mat_del}')

# COMMAND ----------

# MAGIC %md # 2 Preparation

# COMMAND ----------

# check
tmpc = count_var(hes_apc_mat_del, 'PERSON_ID_DEID', ret=1, df_desc='original', indx=1)

# COMMAND ----------

# MAGIC %md ## 2.1 Reduce columns

# COMMAND ----------

# NOTE
#   For simplicity, we have used _1 in the baby tail - ignoring _2 to _9 after exploratory analysis revealed that these 
#     higher indices did not provide additional evidence of delivery 
#   More cleaning to be done, but try to leave as is for mulitple research projects
# TODO
#   Investigate higher tail indices for conflicts or additional info, but as this relates to <1% of records, ignore for now.

tmp = hes_apc_mat_del

# reduce columns
vlist_drop = [col for col in tmp.columns if re.match('^(BIRESUS|BIRORDR|BIRSTAT|BIRWEIT|DELMETH|DELPLAC|DELSTAT|GESTAT|SEXBABY)_[2-9]$', col)]
vlist_drop.extend(['PARTYEAR', 'ProductionDate_TB', 'FYEAR_hes_apc'])
vlist_drop.extend(['MATERNITY_EPISODE_TYPE', 'SEX', 'MYDOB', 'STARTAGE', 'STARTAGE_CALC']) 
# , 'NHSNOIND'
tmp = tmp\
  .drop(*vlist_drop)

# reorder columns
vlist_ordered = [
  'PERSON_ID_DEID'
  , 'PERSON_ID_DEID_hes_apc_mat'  
  , '_id_agree'    
  , 'EPIKEY'
  , 'FYEAR'
  , 'ADMIDATE'
  , 'EPISTART'
  , 'EPIEND'
  , 'DISDATE'
  , 'EPIORDER'
  , 'EPISTAT'
  , 'EPITYPE' 
]
# , 'SUSSPELLID'
vlist_unordered = [v for v in tmp.columns if v not in vlist_ordered]
vlist_all = vlist_ordered + vlist_unordered
tmp1 = tmp\
  .select(*vlist_all)

# check
display(tmp1)

# COMMAND ----------

# MAGIC %md ## 2.2 Create row total

# COMMAND ----------

# Create indicators for evidence of delivery for each maternity variable and create row total

# NOTE
#   Excluding NUMTAILB - default is 1, therefore not useful for providing evidence of delivery
# TODO
#   Consider excluding dummy and implausible dates (e.g., ANASDATE = 1801-01-01 (1.2%)) 

tmp2 = tmp1\
  .withColumn('_ind_ANAGEST',   f.when(f.col('ANAGEST').isNotNull(), 1).otherwise(0))\
  .withColumn('_ind_ANASDATE',  f.when(f.col('ANASDATE').isNotNull(), 1).otherwise(0))\
  .withColumn('_ind_ANTEDUR',   f.when(f.col('ANTEDUR').isNotNull(), 1).otherwise(0))\
  .withColumn('_ind_BIRESUS_1', f.when(f.col('BIRESUS_1').isin(['1','2','3','4','5','6','8']), 1).otherwise(0))\
  .withColumn('_ind_BIRORDR_1', f.when(f.col('BIRORDR_1').isin(['1','2','3','4','5','6','7','8']), 1).otherwise(0))\
  .withColumn('_ind_BIRSTAT_1', f.when(f.col('BIRSTAT_1').isin(['1','2','3','4']), 1).otherwise(0))\
  .withColumn('_ind_BIRWEIT_1', f.when((f.col('BIRWEIT_1') >= 10) & (f.col('BIRWEIT_1') <= 7000), 1).otherwise(0))\
  .withColumn('_ind_DELCHANG',  f.when(f.col('DELCHANG').isin(['1','2','3','4','5','6','8']), 1).otherwise(0))\
  .withColumn('_ind_DELINTEN',  f.when(f.col('DELINTEN').isin(['0','1','2','3','4','5','6','7','8']), 1).otherwise(0))\
  .withColumn('_ind_DELMETH_1', f.when(f.col('DELMETH_1').isin(['0','1','2','3','4','5','6','7','8','9']), 1).otherwise(0))\
  .withColumn('_ind_DELMETH_D', f.when(f.col('DELMETH_D').isin(['01','02','03','04','05','06','07','08','09','10']), 1).otherwise(0))\
  .withColumn('_ind_DELONSET',  f.when(f.col('DELONSET').isin(['1','2','3','4','5','8']), 1).otherwise(0))\
  .withColumn('_ind_DELPLAC_1', f.when(f.col('DELPLAC_1').isin(['0','1','2','3','4','5','6','7','8']), 1).otherwise(0))\
  .withColumn('_ind_DELPOSAN',  f.when(f.col('DELPOSAN').isin(['0','1','2','3','4','5','6','7','8']), 1).otherwise(0))\
  .withColumn('_ind_DELPREAN',  f.when(f.col('DELPREAN').isin(['0','1','2','3','4','5','6','7','8']), 1).otherwise(0))\
  .withColumn('_ind_DELSTAT_1', f.when(f.col('DELSTAT_1').isin(['1','2','3','8']), 1).otherwise(0))\
  .withColumn('_ind_GESTAT_1',  f.when((f.col('GESTAT_1') >= 10) & (f.col('GESTAT_1') <= 49), 1).otherwise(0))\
  .withColumn('_ind_MATAGE',    f.when((f.col('MATAGE') >= 10) & (f.col('MATAGE') <= 70), 1).otherwise(0))\
  .withColumn('_ind_NUMBABY',   f.when(f.col('NUMBABY').isin(['1','2','3','4','5','6']), 1).otherwise(0))\
  .withColumn('_ind_NUMPREG',   f.when((f.col('NUMPREG') >= 0) & (f.col('NUMPREG') <= 19), 1).otherwise(0))\
  .withColumn('_ind_SEXBABY_1', f.when(f.col('SEXBABY_1').isin(['1','2']), 1).otherwise(0))\
  .withColumn('_ind_POSTDUR',   f.when(f.col('POSTDUR').isNotNull(), 1).otherwise(0))

# separate data step for the below as need columns of tmp2
tmp2 = tmp2\
  .withColumn('_rowtotal', sum([f.col(col) for col in tmp2.columns if re.match('^_ind_.*$', col)]))

# check
tmpt = tab(tmp2, '_rowtotal')

# COMMAND ----------

# MAGIC %md ## 2.3 Create indicators

# COMMAND ----------

# Used initially (pre20220321)
#   .withColumn('delivery_opcs4', f.when(f.col('OPERTN_4_CONCAT').rlike('R(1[7-9]|2[0-5])'), 1).otherwise(0))\
#   .withColumn('abomisc_opcs4', f.when(f.col('OPERTN_4_CONCAT').rlike('Q14'), 1).otherwise(0))\

tmp2 = tmp2\
  .withColumn('delivery_icd10',     f.when(f.col('DIAG_4_CONCAT').rlike('O8[0-4]|Z3(7|8)'), 1).otherwise(0))\
  .withColumn('delivery_opcs4',     f.when(f.col('OPERTN_4_CONCAT').rlike('P14[1-3]|R(1[4-9]|2[0-9]|3[0-2])'), 1).otherwise(0))\
  .withColumn('delivery_code',      f.greatest('delivery_icd10', 'delivery_opcs4'))\
  .withColumn('delivery_concat',    f.concat('delivery_icd10', 'delivery_opcs4'))\
  .withColumn('abomisc_lt21',       f.when(f.col('GESTAT_1') < 21, 1).otherwise(0))\
  .withColumn('abomisc_lt24_still', f.when((f.col('GESTAT_1') < 24) & (f.col('BIRSTAT_1').isin([2,3,4])), 1).otherwise(0))\
  .withColumn('abomisc_icd10',      f.when(f.col('DIAG_4_CONCAT').rlike('O0[0-8]'), 1).otherwise(0))\
  .withColumn('abomisc_opcs4',      f.when(f.col('OPERTN_4_CONCAT').rlike('Q10[12]|Q11[12356]|Q14|R03[1289]'), 1).otherwise(0))\
  .withColumn('abomisc_code',       f.greatest('abomisc_lt21', 'abomisc_lt24_still', 'abomisc_icd10', 'abomisc_opcs4'))\
  .withColumn('abomisc_concat',     f.concat('abomisc_lt21', 'abomisc_lt24_still', 'abomisc_icd10', 'abomisc_opcs4'))\
  .withColumn('care',               f.when(f.col('DIAG_4_CONCAT').rlike('O1[0-6]|O[234]|O6|O7[0-5]|O[89]|P|Z3[34569]'), 1).otherwise(0))\
  .withColumn('y951',               f.when(f.col('OPERTN_4_CONCAT').rlike('Y951'), 1).otherwise(0))\
  .withColumn('y952',               f.when(f.col('OPERTN_4_CONCAT').rlike('Y952'), 1).otherwise(0))\
  .withColumn('y953',               f.when(f.col('OPERTN_4_CONCAT').rlike('Y953'), 1).otherwise(0))\
  .withColumn('y954',               f.when(f.col('OPERTN_4_CONCAT').rlike('Y954'), 1).otherwise(0))\
  .withColumn('y95_concat',         f.concat('y951', 'y952', 'y953', 'y954'))\
  .withColumn('delivery_any',       f.when((f.col('delivery_code') == 1) | (f.col('abomisc_code') == 1) | (f.col('_rowtotal') > 0), 1).otherwise(0))

# check
tmpt = tab(tmp2, 'delivery_any'); print()
tmpt = tab(tmp2, 'delivery_icd10', 'delivery_opcs4', var2_unstyled=1); print()
tmpt = tab(tmp2, 'delivery_code'); print()
tmpt = tab(tmp2, 'delivery_code', 'abomisc_code', var2_unstyled=1); print()
tmpt = tab(tmp2, 'abomisc_concat', 'delivery_concat', var2_unstyled=1); print()

# COMMAND ----------

# MAGIC %md ## 2.4 Create delivery date

# COMMAND ----------

# check ANTEDUR and POSTDUR
tmpt = tab(tmp2, 'ANTEDUR'); print()
tmpt = tab(tmp2, 'POSTDUR'); print()
tmpt = tab(tmp2, '_ind_ANTEDUR', '_ind_POSTDUR', var2_unstyled=1); print()

tmpp = tmp2\
  .withColumn('_tmp_ANTEDUR_POSTDUR', f.concat('_ind_ANTEDUR', '_ind_POSTDUR'))
tmpt = tab(tmpp, '_tmp_ANTEDUR_POSTDUR', 'FYEAR', var2_unstyled=1); print()
# Note: ANTEDUR and POSTDUR are not available for the latest financial year (2122)

tmpp = tmpp\
  .withColumn('delivery_date_ante', f.when(f.col('ANTEDUR').isNotNull(), f.expr('date_add(EPISTART, ANTEDUR)')))\
  .withColumn('delivery_date_post', f.when(f.col('POSTDUR').isNotNull(), f.expr('date_add(EPIEND, -POSTDUR)')))\
  .withColumn('standard_equality', f.col('delivery_date_ante') == f.col('delivery_date_post'))\
  .withColumn('null_safe_equality', udf_null_safe_equality('delivery_date_ante', 'delivery_date_post'))

# check
tmpt = tab(tmpp, 'standard_equality', 'null_safe_equality', var2_unstyled=1); print()
# => (ANTEDUR, EPISTART) and (POSTDUR, EPIEND) are consistent


# TODO
#   Use operation dates to get more precise delivery date? For the majority only result in a small difference of a few days...
tmp2 = tmp2\
  .withColumn('delivery_date_prov', f.greatest('_ind_ANTEDUR', '_ind_POSTDUR'))\
  .withColumn('delivery_date',\
    f.when(f.col('ANTEDUR').isNotNull(), f.expr('date_add(EPISTART, ANTEDUR)'))\
     .when(f.col('POSTDUR').isNotNull(), f.expr('date_add(EPIEND, -POSTDUR)'))\
     .otherwise(f.col('EPISTART'))\
  )

# check
tmpt = tab(tmp2, 'delivery_date_prov'); print()
tmpt = tabstat(tmp2, 'delivery_date', date=1)

# COMMAND ----------

# MAGIC %md # 3 Exclusion

# COMMAND ----------

# MAGIC %md ## 3.1 Drop duplicate records

# COMMAND ----------

# check
count_var(tmp2, 'PERSON_ID_DEID'); print() # returned above

# drop
vlist = [col for col in tmp2.columns if col not in ['EPIKEY']]
# , 'SUSSPELLID'

# commented out - random on EPIKEY - use window ordered by EPIKEY instead
# tmp3 = tmp2\
#   .dropDuplicates(vlist)

# define window
_win_rownum = Window\
  .partitionBy(vlist)\
  .orderBy('EPIKEY')

# create row number and filter
tmp3 = tmp2\
  .withColumn('_rownum', f.row_number().over(_win_rownum))\
  .where(f.col('_rownum') == 1)

# check
tmpt = count_var(tmp3, 'PERSON_ID_DEID', ret=1, df_desc='post drop duplicates', indx=2); print()
tmpc = tmpc.unionByName(tmpt)

# COMMAND ----------

# ----------------------------------------------------------------------------------
# save checkpoint
# ----------------------------------------------------------------------------------
outName = f'{proj}_tmp3'
tmp3.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
tmp3 = spark.table(f'{dbc}.{outName}')

# COMMAND ----------

# MAGIC %md ## 3.2 Drop records with no evidence of delivery

# COMMAND ----------

# check
tmpt = tab(tmp3, 'delivery_any'); print()

# drop
tmp4 = tmp3\
  .where(f.col('delivery_any') == 1)

# check
tmpt = count_var(tmp4, 'PERSON_ID_DEID', ret=1, df_desc='post drop records with no evidence of delivery', indx=3); print()
tmpc = tmpc.unionByName(tmpt)

tmpt = tab(tmp4, 'delivery_any'); print()

# cache
tmp4.cache().count()

# COMMAND ----------

# MAGIC %md ## 3.3 Drop records failing quality assurance

# COMMAND ----------

# DBTITLE 1,Suspect relationship between GESTAT and BIRWEIT for RAE and RJ6 providers
tmpp = tmp4\
  .select('PERSON_ID_DEID', 'EPIKEY', 'PROCODE3', 'GESTAT_1', 'BIRWEIT_1')\
  .where((f.col('GESTAT_1').isNotNull()) & (f.col('BIRWEIT_1').isNotNull()))\
  .where((f.col('GESTAT_1') != 99) & (f.col('BIRWEIT_1') != 9999))\
  .withColumn('PROCODE3_TB', f.when(f.col('PROCODE3').isin(['RAE', 'RJ6']), f.col('PROCODE3')).otherwise('Other'))\
  .withColumn('GESTAT_1_TB', f.col('GESTAT_1') + f.rand() - 0.5)\
  .toPandas()
g = sns.relplot(data=tmpp, x='GESTAT_1_TB', y="BIRWEIT_1", col="PROCODE3_TB", kind="scatter", col_wrap=3, palette=sns.color_palette("tab10", 1), facet_kws = dict(sharey = True), s=2, linewidth=0)
display(g.fig)

# COMMAND ----------

# check
tmpt = tab(tmp4, 'PROCODE3'); print()
tmpp = tmp4\
  .withColumn('_tmp', f.when(f.col('PROCODE3').isin(['RAE', 'RJ6']), 1).otherwise(0))
tmpt = tab(tmpp.where(f.col('_tmp') == 1), 'SITETRET'); print()
tmpt = tab(tmpp, 'abomisc_code', '_tmp', var2_unstyled=1); print()
tmpt = tab(tmpp, 'abomisc_concat', '_tmp', var2_unstyled=1); print()

# drop provider codes with severe departures from the expected GESTAT vs BIRWEIT relationship (see Stata scatterplots)
tmp5 = tmp4\
  .where(~f.col('PROCODE3').isin(['RAE', 'RJ6']))

# check
tmpt = count_var(tmp5, 'PERSON_ID_DEID', ret=1, df_desc='post drop records failing quality assurance', indx=4); print()
tmpc = tmpc.unionByName(tmpt)
tmpt = tab(tmp5, 'PROCODE3'); print()
tmpt = tab(tmp5, 'abomisc_concat', 'delivery_concat', var2_unstyled=1); print()

# cache
tmp5.cache().count()

# COMMAND ----------

# MAGIC %md # 4 Cleaning

# COMMAND ----------

# create a row number variable to identify multiple records by ID
# note: add EPIKEY to orderBy to ensure results can be reproduced in the event of ties

# define windows
vlist = ['delivery_date', 'EPISTART', 'ADMIDATE', 'EPIEND', 'DISDATE', 'EPIORDER', 'EPIKEY']
_win_rownum = Window\
  .partitionBy('PERSON_ID_DEID')\
  .orderBy(vlist)
_win_rownummax = Window\
  .partitionBy('PERSON_ID_DEID')

# create _rownum and _rownummax
tmp6 = tmp5\
  .withColumn('_rownum', f.row_number().over(_win_rownum))\
  .withColumn('_rownummax', f.count('PERSON_ID_DEID').over(_win_rownummax))\
  .orderBy('PERSON_ID_DEID', '_rownum')

# check
tmpt = tab(tmp6.where(f.col('_rownum') == 1), '_rownummax')

# COMMAND ----------

# MAGIC %md ## 4.1 Combine records within 28 days

# COMMAND ----------

# define windows
vlist = ['delivery_date', 'EPISTART', 'ADMIDATE', 'EPIEND', 'DISDATE', 'EPIORDER', 'EPIKEY']
_win = Window\
  .partitionBy('PERSON_ID_DEID')\
  .orderBy(vlist)
_win_egen = Window\
  .partitionBy('PERSON_ID_DEID')\
  .orderBy(vlist)\
  .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
_win_cumsum = Window\
  .partitionBy('PERSON_ID_DEID')\
  .orderBy(vlist)\
  .rowsBetween(Window.unboundedPreceding, Window.currentRow)
_win_grp_egen = Window\
  .partitionBy('PERSON_ID_DEID', '_grp')\
  .orderBy(vlist)\
  .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
_win_28days_grp_rownum = Window\
  .partitionBy('PERSON_ID_DEID', '_28days_grp')\
  .orderBy(f.desc('_rowtotal'), f.desc('delivery_date_prov'), f.desc('delivery_code'), f.desc('abomisc_code'), *vlist)
_win_28days_grp_rownummax = Window\
  .partitionBy('PERSON_ID_DEID', '_28days_grp')


# create date difference between delivery date and previous delivery date within individuals
# create minimum date difference within individuals
# create indicator for when the minimum date difference is >= 210 days (assumed to be the minimum legitimate interval between deliveries)

# create indicator for when delivery date difference is: (i) <= 28 days; (ii) > 28 and < 210 days; (iii) >= 210 days; (for rows).
# create maximum indicators for (i), (ii), (iii) (for individuals)
# create concatenation of maximum indicators, to allow us to see interval categories (i), (ii), (iii) for each indvidual

# create indicator for when delivery date difference is > 28 days (for rows)
# create cumulative sum of this indicator to identify groups of records that are within 28 days for each indvidual
tmp6 = tmp6\
  .withColumn('_diff', f.datediff(f.col('delivery_date'), f.lag(f.col('delivery_date'), 1).over(_win)))\
  .withColumn('_diff_min', f.min(f.col('_diff')).over(_win_egen))\
  .withColumn('_diff_min_ge_210', f.when(f.col('_diff_min') >= 210, 1).otherwise(0))\
  .withColumn('_flag_le28', f.when(f.col('_diff') <= 28, 1).otherwise(0))\
  .withColumn('_flag_gt28_lt210', f.when((f.col('_diff') > 28) & (f.col('_diff') < 210), 1).otherwise(0))\
  .withColumn('_flag_ge210', f.when(f.col('_diff') >= 210, 1).otherwise(0))\
  .withColumn('_flag_le28_max', f.max(f.col('_flag_le28')).over(_win_egen))\
  .withColumn('_flag_gt28_lt210_max', f.max(f.col('_flag_gt28_lt210')).over(_win_egen))\
  .withColumn('_flag_ge210_max', f.max(f.col('_flag_ge210')).over(_win_egen))\
  .withColumn('_flag_max_concat', f.concat('_flag_le28_max', '_flag_gt28_lt210_max', '_flag_ge210_max'))\
  .withColumn('_flag_gt28', f.when(f.col('_diff') > 28, 1).otherwise(0))\
  .withColumn('_grp', f.sum(f.col('_flag_gt28')).over(_win_cumsum))


# create minimum delivery date within groups of records that are within 28 days
# note: possible to have chains of 28-day separated records, so we identify these with grp_2
# note: upon inspection, combining chains of 28-day separated records is more appropriate (impacts 32 records [i.e., _flag_gt28_2])
#       last line commented out
tmp6 = tmp6\
  .withColumn('_grp_delivery_date_min', f.min(f.col('delivery_date')).over(_win_grp_egen))\
  .withColumn('_diff_2', f.datediff(f.col('delivery_date'), f.col('_grp_delivery_date_min')))\
  .withColumn('_flag_gt28_2', f.when(f.col('_diff_2') > 28, 1).otherwise(0))\
  .withColumn('_grp_2', f.sum(f.col('_flag_gt28_2')).over(_win_cumsum))
# .withColumn('_28days_grp', f.col('_grp') + f.col('_grp_2'))


# create row number and maximum row number for the above groups for each indvidual
# create indicator for when maximum row number is > 1, to highlight the rows to combine/collapse
tmp6 = tmp6\
  .withColumnRenamed('_grp', '_28days_grp')\
  .withColumn('_28days_rownum', f.row_number().over(_win_28days_grp_rownum))\
  .withColumn('_28days_rownummax', f.count('PERSON_ID_DEID').over(_win_28days_grp_rownummax))\
  .withColumn('_28days_combine', f.when(f.col('_28days_rownummax') > 1, 1).otherwise(0))
  
  
# check
tmpt = tab(tmp6.where(f.col('_rownum') == 1), '_rownummax', '_diff_min_ge_210', var2_unstyled=1); print()
tmpp = tmp6\
  .where(\
    (f.col('_rownum') == 1)\
    & (f.col('_rownummax') > 1)\
    & (f.col('_diff_min_ge_210') == 0)\
  )
tmpt = tab(tmpp, '_flag_max_concat'); print()
tmpt = tab(tmp6, '_flag_gt28_2'); print()
tmpt = tab(tmp6, '_28days_grp'); print()
tmpt = tab(tmp6.where(f.col('_28days_rownum') == 1), '_28days_rownummax'); print()

# COMMAND ----------

# MAGIC %md ### 4.1.1 Flag conflicts

# COMMAND ----------

# conflicts

# note: for MATAGE, <None> did not work with isin, so, as a quick fix, used <99> as an artificial dummy value to avoid code rewrite
_dict = {
  'BIRWEIT_1': 9999,
  'GESTAT_1': 99,
  'MATAGE': 99,
  'NUMBABY': ['9', 'X'],
  'SEXBABY_1': ['0', '9']  
}

# define windows
_win_conflict = Window\
  .partitionBy('PERSON_ID_DEID', '_28days_grp')\
  .orderBy('_28days_rownum')\
  .rowsBetween(Window.unboundedPreceding, Window.currentRow)
  # .rowsBetween(-sys.maxsize, 0))
_win_egen = Window\
  .partitionBy('PERSON_ID_DEID', '_28days_grp')\
  .orderBy('_28days_rownum')\
  .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)            

tmpm = []
for i, key in enumerate(_dict):
  print(i, key, _dict[key])
  
  # take value of variable where 28 day rownum equals 1
  # forward fill this value within the 28 day group
  # identify conflicts between the above and the value on later rows, allowing for missing values
  # flag conflict over the 28 day group (rather than only on the individual row)
  tmp6 = tmp6\
    .withColumn('_tmp', f.when(f.col('_28days_rownum') == 1, f.col(key)).otherwise(None))\
    .withColumn('_tmpm', f.last('_tmp', True).over(_win_conflict))\
    .withColumn('_tmpc', f.when((f.col(key).isNotNull()) & (~f.col(key).isin(_dict[key])) & (~udf_null_safe_equality(key, '_tmpm')), 1).otherwise(0))\
    .withColumn(f'_conflict_{key}', f.max(f.col('_tmpc')).over(_win_egen))

  # check
  tmpt = tab(tmp6, '_tmpc'); print()
  
  # collect succinct summary across variables
  i = i + 1
  tmpv = tmp6.where(f.col('_tmpc') == 1).count()
  tmps = spark.createDataFrame(
      [
        (f'{i}', f'{key}', f'{tmpv}'),
      ],
      ['indx', 'var', 'n_conflicts']  
    )
  if(i == 1): tmpm = tmps
  else: tmpm = tmpm.unionByName(tmps)

# check    
print(tmpm.toPandas().to_string()); print() 

# COMMAND ----------

# MAGIC %md ### 4.1.2 Combine

# COMMAND ----------

# define windows
_win = Window\
  .partitionBy('PERSON_ID_DEID', '_28days_grp')\
  .orderBy('_28days_rownum')
_win_egen = Window\
  .partitionBy('PERSON_ID_DEID', '_28days_grp')\
  .orderBy('_28days_rownum')\
  .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

# filter to multiple records that are within 28 days that we will collapse/combine
tmp7 = tmp6\
  .where(f.col('_28days_combine') == 1)

# collect all values for key columns that are in 28 day group in an array
varlist = ['EPIKEY', 'EPISTART', '_rowtotal', 'BIRWEIT_1', 'GESTAT_1', 'MATAGE', 'NUMBABY', 'SEXBABY_1', 'DIAG_4_CONCAT', 'OPERTN_4_CONCAT', 'delivery_date', 'delivery_date_prov', 'delivery_code', 'abomisc_code', 'care']
for var in varlist:
  print(var)
  tmp7 = tmp7\
    .withColumn(f'_28days_combine_{var}', f.flatten(f.collect_list(f.array(var)).over(_win)))\
    .withColumn(f'_28days_combine_{var}', f.max(f.col(f'_28days_combine_{var}')).over(_win_egen))
print()
  
# filter to first record, retaining values from other records that are in the 28 day group in an array   
tmp7 = tmp7\
  .where(f.col('_28days_rownum') == 1)\
  .select(['PERSON_ID_DEID', '_28days_grp'] + [f'_28days_combine_{var}' for var in varlist])

# check original
count_var(tmp6, 'PERSON_ID_DEID'); print()

# filter original to first record
tmp6a = tmp6\
  .where(f.col('_28days_rownum') == 1)

# check
count_var(tmp6a, 'PERSON_ID_DEID'); print()

# merge filtered original and collected values above
tmp8 = merge(tmp6a, tmp7, ['PERSON_ID_DEID', '_28days_grp']); print()

# check
assert tmp8.select('_merge').where(f.col('_merge') == 'right_only').count() == 0

# tidy
tmp8 = tmp8\
  .drop('_merge')

# check
tmpt = count_var(tmp8, 'PERSON_ID_DEID', ret=1, df_desc='post combining records within 28 days', indx=5); print()
tmpc = tmpc.unionByName(tmpt)

# COMMAND ----------

# MAGIC %md ### 4.1.3 Recreate

# COMMAND ----------

# REcreate a row number variable to identify multiple records by person ID
# note: add EPIKEY to orderBy to ensure results can be reproduced in the event of ties

# define windows
vlist = ['delivery_date', 'EPISTART', 'ADMIDATE', 'EPIEND', 'DISDATE', 'EPIORDER', 'EPIKEY']
_win_rownum = Window\
  .partitionBy('PERSON_ID_DEID')\
  .orderBy(vlist)
_win_rownummax = Window\
  .partitionBy('PERSON_ID_DEID')

# REcreate _rownum and _rownummax
tmp8a = tmp8\
  .withColumn('_rownum', f.row_number().over(_win_rownum))\
  .withColumn('_rownummax', f.count('PERSON_ID_DEID').over(_win_rownummax))\
  .orderBy('PERSON_ID_DEID', '_rownum')

# check
tmpt = tab(tmp8a.where(f.col('_rownum') == 1), '_rownummax'); print()

# define windows
_win = Window\
  .partitionBy('PERSON_ID_DEID')\
  .orderBy(vlist)
_win_egen = Window\
  .partitionBy('PERSON_ID_DEID')\
  .orderBy(vlist)\
  .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

# REcreate date difference between delivery date and previous delivery date within individuals
# REcreate minimum date difference within individuals
# REcreate indicator for when the minimum date difference is >= 210 days (assumed to be the minimum legitimate interval between deliveries)
tmp8a = tmp8a\
  .withColumn('_diff', f.datediff(f.col('delivery_date'), f.lag(f.col('delivery_date'), 1).over(_win)))\
  .withColumn('_diff_min', f.min(f.col('_diff')).over(_win_egen))\
  .withColumn('_diff_min_ge_210', f.when(f.col('_diff_min') >= 210, 1).otherwise(0))\

# check
tmpt = tab(tmp8a.where(f.col('_rownum') == 1), '_rownummax', '_diff_min_ge_210', var2_unstyled=1)

# COMMAND ----------

# MAGIC %md # 5 Inclusion

# COMMAND ----------

# MAGIC %md ## 5.1 Keep records with an interval of at least 210 days

# COMMAND ----------

# keep records for individuals with only 1 record or with multiple records that all have an interval of at least 210 days
tmp9 = tmp8a\
  .where(\
    (f.col('_rownummax') == 1)\
    | (f.col('_diff_min_ge_210') == 1)\
  )
 
# check records for individuals not satisfying the above  
tmp9a = tmp8a\
  .where(~(\
    (f.col('_rownummax') == 1)\
    | (f.col('_diff_min_ge_210') == 1)\
  ))

# check
tmpt = count_var(tmp9, 'PERSON_ID_DEID', ret=1, df_desc='post dropping records with an interval < 210 days', indx=6); print()
tmpc = tmpc.unionByName(tmpt)
tmpt = tab(tmp9.where(f.col('_rownum') == 1), '_rownummax'); print()
count_var(tmp9a, 'PERSON_ID_DEID'); print()
tmpt = tab(tmp9a.where(f.col('_rownum') == 1), '_rownummax'); print()

# COMMAND ----------

# check
display(tmp9.where(f.col('_rownummax') == 4))

# COMMAND ----------

# check
display(tmp9a)

# COMMAND ----------

# MAGIC %md # 6 Flow diagram

# COMMAND ----------

# check flow
print(tmpc.toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md # 7 Save

# COMMAND ----------

# select variables to keep and reorder
tmp9b = tmp9\
  .select(\
     'PERSON_ID_DEID',
     'PERSON_ID_DEID_hes_apc_mat',
     '_id_agree',
     '_rownum',
     '_rownummax',
     'delivery_date_prov',
     'delivery_date',
     '_diff',
     '_diff_min',
     '_diff_min_ge_210',
     '_rowtotal',
     'delivery_code',
     'abomisc_code',
     'care',                            
     'DIAG_4_CONCAT',
     'OPERTN_4_CONCAT',          
  
     'BIRWEIT_1',                   
     'GESTAT_1',               
             
     'EPIKEY',
     'FYEAR',
     'PROCODE3',        
     'PROCODE5',         
     'SITETRET',         
     'ADMIDATE',
     'EPISTART',
     'EPIEND',
     'DISDATE',
     'EPIORDER',
     'EPISTAT',
     'EPITYPE',
          
     'ANAGEST',
     'ANASDATE',
     'ANTEDUR',
     'BIRESUS_1',
     'BIRORDR_1',
     'BIRSTAT_1',

     'DELCHANG',
     'DELINTEN',
     'DELMETH_1',
     'DELMETH_D',
     'DELONSET',
     'DELPLAC_1',
     'DELPOSAN',
     'DELPREAN',
     'DELSTAT_1',

     'MATAGE',
     'NUMBABY',
     'NUMPREG',
     'NUMTAILB',
     'POSTDUR',
     'SEXBABY_1',
     
     'delivery_icd10',
     'delivery_opcs4',     
     'delivery_concat',
          
     'abomisc_lt21',
     'abomisc_lt24_still',
     'abomisc_icd10',
     'abomisc_opcs4',
     'abomisc_concat',
     
     'y951',
     'y952',
     'y953',
     'y954',
     'y95_concat',
          
     'delivery_any',
          
     '_conflict_BIRWEIT_1',
     '_conflict_GESTAT_1',
     '_conflict_MATAGE',
     '_conflict_NUMBABY',
     '_conflict_SEXBABY_1',
          
     '_28days_combine_EPIKEY',
     '_28days_combine_EPISTART',
     '_28days_combine__rowtotal',
     '_28days_combine_BIRWEIT_1',
     '_28days_combine_GESTAT_1',
     '_28days_combine_MATAGE',
     '_28days_combine_NUMBABY',
     '_28days_combine_SEXBABY_1',
     '_28days_combine_DIAG_4_CONCAT',
     '_28days_combine_OPERTN_4_CONCAT',
     '_28days_combine_delivery_date',
     '_28days_combine_delivery_date_prov',
     '_28days_combine_delivery_code',
     '_28days_combine_abomisc_code',
     '_28days_combine_care'
  )
#   'SUSSPELLID',

# check
display(tmp9b)

# COMMAND ----------

# spark.conf.set('spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation', 'true')

# save name
outName = f'{proj}_tmp_hes_apc_mat_del_clean'.lower() # was _tmp_hes_apc_mat_clean

# save previous version for comparison purposes
_datetimenow = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
outName_pre = f'{outName}_pre{_datetimenow}'.lower()
print(outName_pre)
# If code note working, silence following two lines:
spark.table(f'{dbc}.{outName}').write.mode('overwrite').saveAsTable(f'{dbc}.{outName_pre}')
spark.sql(f'ALTER TABLE {dbc}.{outName_pre} OWNER TO {dbc}')

# save
print(outName)
tmp9b.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

# COMMAND ----------

# save name
outName = f'{proj}_tmp_flow_hes_apc_mat_del_clean'.lower() 

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
tmpc.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

# COMMAND ----------

