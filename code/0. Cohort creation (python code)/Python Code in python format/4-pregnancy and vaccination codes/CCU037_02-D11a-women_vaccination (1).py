# Databricks notebook source
# MAGIC %md # ccu037_02-D11a-women_vaccination
# MAGIC  
# MAGIC **Description** This notebook determines exposures for the analysis.
# MAGIC 
# MAGIC **Author(s)** Marta Pineda - Adapted from the work of Venexia Walker, Sam Ip for ccu002_06 and ccu036_02-D11a
# MAGIC 
# MAGIC SI added: when there are conflicting records at any stage, we take the individual's first record after ordering by D1 D2 Db and D3, with NULLS defined as ordered last

# COMMAND ----------

# MAGIC %md ## Clear cache

# COMMAND ----------

# MAGIC %sql
# MAGIC CLEAR CACHE

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

# DBTITLE 1,Parameters ccu037_02
#Project:
proj = "ccu037_02"

#Databases:
db                     = "dars_nic_391419_j3w9t"
dbc                    = "dars_nic_391419_j3w9t_collab"
  
#Dates:
proj_preg_start_date   = "2020-01-23"
proj_preg_end_date     = "2022-06-29"
proj_fu_end_date       = "2022-06-29"

# Params
# production_date = "2022-05-30 14:32:44.253705" # Notebook CCU03_01_create_table_aliases   Cell 8
# ProductionDate =  "2022-06-29 00:00:00.000000"

# COMMAND ----------

# DBTITLE 1,Parameters including freezzing date 
#%run "/Workspaces/dars_nic_391419_j3w9t_collab/CCU036_02/CCU036_02-D01-parameters"

# -----------------------------------------------------------------------------
# Paths
# -----------------------------------------------------------------------------
# maternity files (to be incorporated into archive files when the archives are available - freezing for now)
path_hes_apc_mat_1920 = f'{db}.hes_apc_mat_1920_{db}'
path_hes_apc_mat_2021 = f'{db}.hes_apc_mat_2021_{db}'
path_hes_apc_mat_2122 = f'{db}.hes_apc_mat_2122_{db}'

path_hes_apc_otr_1920 = f'{db}.hes_apc_otr_1920_{db}'
path_hes_apc_otr_2021 = f'{db}.hes_apc_otr_2021_{db}'
path_hes_apc_otr_2122 = f'{db}.hes_apc_otr_2122_{db}'

path_hes_apc_1920 = f'{db}.hes_apc_1920_{db}'
path_hes_apc_2021 = f'{db}.hes_apc_2021_{db}'
path_hes_apc_2122 = f'{db}.hes_apc_2122_{db}'
tmp_archive_date = '2022-06-29 00:00:00.000000'

# archive tables
data = [
    ['deaths',  dbc, f'deaths_{db}_archive',            '2022-06-29 00:00:00.000000', 'DEC_CONF_NHS_NUMBER_CLEAN_DEID', 'REG_DATE_OF_DEATH']
  , ['gdppr',   dbc, f'gdppr_{db}_archive',             '2022-06-29 00:00:00.000000', 'NHS_NUMBER_DEID',                'DATE']
  , ['hes_apc', dbc, f'hes_apc_all_years_archive',      tmp_archive_date, 'PERSON_ID_DEID',                 'EPISTART'] 
  , ['hes_apc_mat', dbc, f'hes_apc_mat_all_years_archive',  tmp_archive_date, 'PERSON_ID_DEID',                 'EPISTART'] 
  , ['hes_op',  dbc, f'hes_op_all_years_archive',       '2022-06-29 00:00:00.000000', 'PERSON_ID_DEID',                 'APPTDATE'] 
  , ['hes_ae',  dbc, f'hes_ae_all_years_archive',       '2022-06-29 00:00:00.000000', 'PERSON_ID_DEID',                 'ARRIVALDATE'] 
  , ['pmeds',   dbc, f'primary_care_meds_{db}_archive', '2022-06-29 00:00:00.000000', 'Person_ID_DEID',                 'ProcessingPeriodDate']
  , ['hes_cc',  dbc, f'hes_cc_all_years_archive',       '2022-06-29 00:00:00.000000', 'PERSON_ID_DEID',                 'CCSTARTDATE']
  , ['chess',   dbc, f'chess_{db}_archive',             '2022-06-29 00:00:00.000000', 'PERSON_ID_DEID',                 'InfectionSwabDate']
  , ['sgss',    dbc, f'sgss_{db}_archive',              '2022-06-29 00:00:00.000000', 'PERSON_ID_DEID',                 'Specimen_Date']
  , ['sus',     dbc, f'sus_{db}_archive',               '2022-06-29 00:00:00.000000', 'NHS_NUMBER_DEID',                'EPISODE_START_DATE']
  , ['vacc',        dbc, f'vaccine_status_{db}_archive',    tmp_archive_date, 'PERSON_ID_DEID',                 'RECORDED_DATE']
]
df_archive = pd.DataFrame(data, columns = ['dataset', 'database', 'table', 'productionDate', 'idVar', 'dateVar'])
df_archive
  # check isid dataset and table

# project in tables (available post table_freeze)
path_hes_apc_mat     = f'{dbc}.{proj}_in_hes_apc_mat'
path_gdppr_id        = f'{dbc}.{proj}_in_gdppr_id'
path_deaths          = f'{dbc}.{proj}_in_deaths_{db}_archive'
path_gdppr           = f'{dbc}.{proj}_in_gdppr_{db}_archive'
path_hes_apc         = f'{dbc}.{proj}_in_hes_apc_all_years_archive'
path_hes_op          = f'{dbc}.{proj}_in_hes_op_all_years_archive'
path_hes_ae          = f'{dbc}.{proj}_in_hes_ae_all_years_archive'
path_pmeds           = f'{dbc}.{proj}_in_primary_care_meds_{db}_archive'
path_hes_cc          = f'{dbc}.{proj}_in_hes_cc_all_years_archive'
path_chess           = f'{dbc}.{proj}_in_chess_{db}_archive'
path_sgss            = f'{dbc}.{proj}_in_sgss_{db}_archive'
path_sus             = f'{dbc}.{proj}_in_sus_{db}_archive'
path_vacc            = f'{dbc}.{proj}_in_vaccine_status_{db}_archive'
path_skinny          = f'{dbc}.{proj}_out_skinny'

# project in curated tables
path_hes_apc_long      = f'{dbc}.{proj}_in_hes_apc_all_years_archive_long'
path_hes_apc_oper_long = f'{dbc}.{proj}_in_hes_apc_all_years_archive_oper_long'
path_hes_op_long       = f'{dbc}.{proj}_in_hes_op_all_years_archive_long'
path_deaths_long       = f'{dbc}.{proj}_in_deaths_{db}_archive_long'
path_deaths_sing       = f'{dbc}.{proj}_in_deaths_{db}_archive_sing'
path_lsoa_region       = f'{dbc}.{proj}_in_lsoa_region_lookup'
path_covid             = f'{dbc}.{proj}_in_covid'

# project temporary tables
path_delivery_record    = f'{dbc}.{proj}_tmp_delivery_record'
path_vaccine           = f'{dbc}.{proj}_vaccination'

# project out tables
path_codelist          = f'{dbc}.{proj}_out_codelist'
path_cohort            = f'{dbc}.{proj}_out_cohort'
path_exposure          = f'{dbc}.{proj}_out_exposure'


# reference files
path_bhf_phenotypes  = 'bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127'
path_map_ctv3_snomed = 'dss_corporate.read_codes_map_ctv3_to_snomed'
path_gdppr_snomed    = 'dss_corporate.gpdata_snomed_refset_full'
path_icd10           = 'dss_corporate.icd10_group_chapter_v01'
path_geog            = 'dss_corporate.ons_chd_geo_listings'
path_imd             = 'dss_corporate.english_indices_of_dep_v02'
path_ethnic_hes      = 'dss_corporate.hesf_ethnicity'
path_ethnic_gdppr    = 'dss_corporate.gdppr_ethnicity'


# COMMAND ----------

# MAGIC %md  ## Define functions

# COMMAND ----------

# Define create table function by Sam Hollings
# Source: Workspaces/dars_nic_391419_j3w9t_collab/DATA_CURATION_wrang000_functions

def create_table(table_name:str, database_name:str='dars_nic_391419_j3w9t_collab', select_sql_script:str=None) -> None:
  """Will save to table from a global_temp view of the same name as the supplied table name (if no SQL script is supplied)
  Otherwise, can supply a SQL script and this will be used to make the table with the specificed name, in the specifcied database."""
  
  spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")
  
  if select_sql_script is None:
    select_sql_script = f"SELECT * FROM global_temp.{table_name}"
  
  spark.sql(f"""CREATE TABLE {database_name}.{table_name} AS
                {select_sql_script}
             """)
  spark.sql(f"ALTER TABLE {database_name}.{table_name} OWNER TO {database_name}")
  
def drop_table(table_name:str, database_name:str='dars_nic_391419_j3w9t_collab', if_exists=True):
  if if_exists:
    IF_EXISTS = 'IF EXISTS'
  else: 
    IF_EXISTS = ''
  spark.sql(f"DROP TABLE {IF_EXISTS} {database_name}.{table_name}")

# COMMAND ----------

# MAGIC %md ## Create annotated vaccination table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu037_02_codelist_vaccine_products AS
# MAGIC SELECT *
# MAGIC FROM VALUES
# MAGIC ('39114911000001105','SNOMED','COVID19_vaccine_AstraZeneca'),
# MAGIC ('39115011000001105','SNOMED','COVID19_vaccine_AstraZeneca'),
# MAGIC ('39115111000001106','SNOMED','COVID19_vaccine_AstraZeneca'),
# MAGIC ('39115711000001107','SNOMED','COVID19_vaccine_Pfizer'),
# MAGIC ('39115611000001103','SNOMED','COVID19_vaccine_Pfizer'),
# MAGIC ('39326911000001101','SNOMED','COVID19_vaccine_Moderna'), 
# MAGIC ('39375411000001104','SNOMED','COVID19_vaccine_Moderna'),
# MAGIC ('1324681000000101','SNOMED','COVID19_vaccine_dose1'),
# MAGIC ('1324691000000104','SNOMED','COVID19_vaccine_dose2'),
# MAGIC ('1324741000000101','SNOMED','COVID19_vaccine_dose1_declined'),
# MAGIC ('1324751000000103','SNOMED','COVID19_vaccine_dose2_declined'),
# MAGIC ('1362591000000103','SNOMED','COVID19_vaccine_dose_booster'),
# MAGIC ('1324671000000103','SNOMED','COVID19_vaccine_dose3'),
# MAGIC ('61396006','','COVID19_vaccine_site_left_thigh'),
# MAGIC ('368209003','','COVID19_vaccine_site_right_upper_arm'),
# MAGIC ('368208006','','COVID19_vaccine_site_left_upper_arm'),
# MAGIC ('723980000','','COVID19_vaccine_site_right_buttock'),
# MAGIC ('723979003','','COVID19_vaccine_site_left_buttock'),
# MAGIC ('11207009','','COVID19_vaccine_site_right_thigh'),
# MAGIC ('413294000','','COVID19_vaccine_care_setting_community_health_services'),
# MAGIC ('310065000','','COVID19_vaccine_care_setting_open_access_service'),
# MAGIC ('788007007','','COVID19_vaccine_care_setting_general_practice_service')
# MAGIC AS tab(code, terminology, name)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM global_temp.ccu037_02_codelist_vaccine_products

# COMMAND ----------

drop_table("ccu037_02_codelist_vaccine_products")
create_table("ccu037_02_codelist_vaccine_products")

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- temp codelists created for only vaccine/exposure-related medcodes -- to be added to ccu037_02_codelists for all medcodes in D02
# MAGIC select * from  dars_nic_391419_j3w9t_collab.ccu037_02_codelist_vaccine_products 

# COMMAND ----------

# DBTITLE 1,Vaccination source table in TRE
# MAGIC %sql 
# MAGIC select COUNT(distinct PERSON_ID_DEID) from dars_nic_391419_j3w9t_collab._vaccine_status_dars_nic_391419_j3w9t --Similar number than the total amount of individuals who had been vacinated in England

# COMMAND ----------

#Comparison to ccu036 frozen
#%sql
#select count(DISTINCT PERSON_ID_DEID) from dars_nic_391419_j3w9t_collab.ccu036_01_in_vaccine_status_dars_nic_391419_j3w9t_archive_pre20220522 -- this n is much lower
#%sql
#select count(DISTINCT PERSON_ID_DEID) from dars_nic_391419_j3w9t_collab.ccu036_01_in_vaccine_status_dars_nic_391419_j3w9t_archive -- this n is much lower

# COMMAND ----------

# DBTITLE 1,Using "ccu036_01_in_vaccine_status_dars_nic_391419_j3w9t_archive_pre20220522" as archive frozen table as it works for us
# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu037_02_vaccination_raw AS
# MAGIC SELECT vaccination_raw.PERSON_ID_DEID,
# MAGIC        to_date(cast(vaccination_raw.RECORDED_DATE as string), 'yyyyMMdd') AS RECORDED_DATE,
# MAGIC        REPLACE(product.name, 'COVID19_vaccine_', '') AS VACCINE_PRODUCT,
# MAGIC        REPLACE(procedure.name,'COVID19_vaccine_','') AS VACCINATION_PROCEDURE,
# MAGIC        REPLACE(situation.name,'COVID19_vaccine_','') AS VACCINATION_SITUATION_CODE
# MAGIC FROM dars_nic_391419_j3w9t_collab._vaccine_status_dars_nic_391419_j3w9t AS vaccination_raw
# MAGIC -- FROM dars_nic_391419_j3w9t_collab.ccu036_01_in_vaccine_status_dars_nic_391419_j3w9t_archive_pre20220522 AS vaccination_raw
# MAGIC -- LEFT JOIN (SELECT code, name FROM dars_nic_391419_j3w9t_collab.ccu002_06_codelists WHERE left(name,16)="COVID19_vaccine_") AS product ON vaccination_raw.VACCINE_PRODUCT_CODE = product.code
# MAGIC -- LEFT JOIN (SELECT * FROM dars_nic_391419_j3w9t_collab.ccu002_06_codelists WHERE left(name,16)="COVID19_vaccine_") AS procedure ON vaccination_raw.VACCINATION_PROCEDURE_CODE = procedure.code
# MAGIC -- LEFT JOIN (SELECT code, name FROM dars_nic_391419_j3w9t_collab.ccu002_06_codelists WHERE left(name,16)="COVID19_vaccine_") AS situation ON vaccination_raw.VACCINATION_SITUATION_CODE = situation.code
# MAGIC LEFT JOIN (SELECT code, name FROM dars_nic_391419_j3w9t_collab.ccu037_02_codelist_vaccine_products WHERE left(name,16)="COVID19_vaccine_") AS product ON vaccination_raw.VACCINE_PRODUCT_CODE = product.code
# MAGIC LEFT JOIN (SELECT * FROM dars_nic_391419_j3w9t_collab.ccu037_02_codelist_vaccine_products WHERE left(name,16)="COVID19_vaccine_") AS procedure ON vaccination_raw.VACCINATION_PROCEDURE_CODE = procedure.code
# MAGIC LEFT JOIN (SELECT code, name FROM dars_nic_391419_j3w9t_collab.ccu037_02_codelist_vaccine_products WHERE left(name,16)="COVID19_vaccine_") AS situation ON vaccination_raw.VACCINATION_SITUATION_CODE = situation.code;
# MAGIC 
# MAGIC SELECT * from global_temp.ccu037_02_vaccination_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT VACCINATION_SITUATION_CODE,COUNT(*) AS count
# MAGIC FROM global_temp.ccu037_02_vaccination_raw 
# MAGIC GROUP BY VACCINATION_SITUATION_CODE;

# COMMAND ----------

# MAGIC %md ## Convert vaccination data to wide format

# COMMAND ----------

# Create dose specific vaccination tables
for dose in ["dose1","dose2", "dose3", "dose_booster"]:
   sql("""CREATE OR REPLACE GLOBAL TEMP VIEW ccu037_02_vaccination_""" + dose + """ AS
   SELECT PERSON_ID_DEID, RECORDED_DATE AS """ + dose + """_date,
   VACCINE_PRODUCT AS """ + dose + """_product, 
   VACCINATION_SITUATION_CODE AS """ + dose + """_situation
   FROM global_temp.ccu037_02_vaccination_raw
   WHERE (VACCINATION_PROCEDURE='""" + dose + """' )""")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Join dose specific vaccination tables to create wide format vaccination table
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu037_02_vaccination_wide AS
# MAGIC SELECT dose1.PERSON_ID_DEID,
# MAGIC        dose1.dose1_date,
# MAGIC        dose1.dose1_product,
# MAGIC        dose1.dose1_situation,
# MAGIC        dose2.dose2_date,
# MAGIC        dose2.dose2_product,
# MAGIC        dose2.dose2_situation,
# MAGIC        dose3.dose3_date,
# MAGIC        dose3.dose3_product,
# MAGIC        dose3.dose3_situation,
# MAGIC        dose_booster.dose_booster_date,
# MAGIC        dose_booster.dose_booster_product,
# MAGIC        dose_booster.dose_booster_situation
# MAGIC FROM global_temp.ccu037_02_vaccination_dose1 AS dose1
# MAGIC FULL JOIN global_temp.ccu037_02_vaccination_dose2 AS dose2 on dose1.PERSON_ID_DEID = dose2.PERSON_ID_DEID
# MAGIC FULL JOIN global_temp.ccu037_02_vaccination_dose_booster AS dose_booster on dose1.PERSON_ID_DEID = dose_booster.PERSON_ID_DEID 
# MAGIC FULL JOIN global_temp.ccu037_02_vaccination_dose3 AS dose3 on dose1.PERSON_ID_DEID = dose3.PERSON_ID_DEID;
# MAGIC 
# MAGIC select * from global_temp.ccu037_02_vaccination_wide

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.ccu037_02_vaccination_wide
# MAGIC where PERSON_ID_DEID = "Y302ETF1P90O09H"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Identify people with multiple records and mark as conflicted
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu037_02_vaccination_conflicted AS 
# MAGIC SELECT *
# MAGIC FROM (SELECT PERSON_ID_DEID, 
# MAGIC              CASE WHEN Records_per_Patient>1 THEN 1 ELSE 0 END AS conflicted_vax_record
# MAGIC       FROM (SELECT PERSON_ID_DEID, count(PERSON_ID_DEID) AS Records_per_Patient
# MAGIC             FROM global_temp.ccu037_02_vaccination_wide
# MAGIC             GROUP BY PERSON_ID_DEID))
# MAGIC WHERE conflicted_vax_record==1;
# MAGIC 
# MAGIC select * from global_temp.ccu037_02_vaccination_conflicted

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct(*))
# MAGIC from global_temp.ccu037_02_vaccination_conflicted;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Restrict to one record per person with indicator for a conflicted record
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu037_02_vaccination AS 
# MAGIC SELECT vaccination.PERSON_ID_DEID AS NHS_NUMBER_DEID,
# MAGIC        vaccination.dose1_date AS vaccination_dose1_date,
# MAGIC        vaccination.dose1_product AS vaccination_dose1_product,
# MAGIC        vaccination.dose1_situation AS vaccination_dose1_situation,
# MAGIC        vaccination.dose2_date AS vaccination_dose2_date,
# MAGIC        vaccination.dose2_product AS vaccination_dose2_product,
# MAGIC        vaccination.dose2_situation AS vaccination_dose2_situation,
# MAGIC        vaccination.dose3_date AS vaccination_dose3_date,
# MAGIC        vaccination.dose3_product AS vaccination_dose3_product,
# MAGIC        vaccination.dose3_situation AS vaccination_dose3_situation,
# MAGIC        vaccination.dose_booster_date AS vaccination_dose_booster_date,
# MAGIC        vaccination.dose_booster_product AS vaccination_dose_booster_product,
# MAGIC        vaccination.dose_booster_situation AS vaccination_dose_booster_situation,
# MAGIC        conflict.conflicted_vax_record AS vaccination_conflicted
# MAGIC FROM (SELECT * -- ccu037_02_vaccination_wide group by ID, pick record with earliest D1 date (otherwise random)
# MAGIC       FROM (SELECT *, row_number() OVER (PARTITION BY PERSON_ID_DEID ORDER BY dose1_date asc, dose2_date asc NULLS LAST, dose_booster_date asc NULLS LAST, dose3_date asc NULLS LAST) AS record_number -- want any other choosing rules for conflicted records? 
# MAGIC             FROM (SELECT * 
# MAGIC                   FROM global_temp.ccu037_02_vaccination_wide))
# MAGIC       WHERE record_number=1) AS vaccination
# MAGIC LEFT JOIN global_temp.ccu037_02_vaccination_conflicted AS conflict ON conflict.PERSON_ID_DEID = vaccination.PERSON_ID_DEID;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *, row_number() OVER (PARTITION BY PERSON_ID_DEID ORDER BY dose1_date asc) AS record_number
# MAGIC             FROM global_temp.ccu037_02_vaccination_wide
# MAGIC             where  PERSON_ID_DEID = "Y302ETF1P90O09H"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.ccu037_02_vaccination
# MAGIC where NHS_NUMBER_DEID= "Y302ETF1P90O09H"

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(DISTINCT NHS_NUMBER_DEID) from global_temp.ccu037_02_vaccination;

# COMMAND ----------

# MAGIC %md ## Select women only from ccu037_02 that had been vacinated

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Restrict ccu037_02_vaccination to individuals present in dars_nic_391419_j3w9t_collab.ccu037_02_cohort
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu037_02_vaccination_gdppr AS 
# MAGIC SELECT * -- AS NHS_NUMBER_DEID,
# MAGIC FROM (SELECT NHS_NUMBER_DEID as ID, age, sex FROM dars_nic_391419_j3w9t_collab.ccu037_02_cohort) as cohort
# MAGIC    INNER JOIN (SELECT * FROM global_temp.ccu037_02_vaccination) AS vac ON cohort.ID = vac.NHS_NUMBER_DEID
# MAGIC 
# MAGIC --- Change to LEFT JOIN FOR INCLUDING ALL ID from ccu037_02 plus vaccination info regardless of ccu037_02 women where or not vacinated

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Restrict ccu037_02_vaccination to WOMEN present in dars_nic_391419_j3w9t_collab.ccu037_02_cohort
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu037_02_vaccination_gdppr_women AS 
# MAGIC SELECT * FROM global_temp.ccu037_02_vaccination_gdppr WHERE sex == 2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.ccu037_02_vaccination_gdppr_women;

# COMMAND ----------

# MAGIC %sql  --- ditinct n = non distict
# MAGIC select count(distinct ID) from global_temp.ccu037_02_vaccination_gdppr;

# COMMAND ----------

# MAGIC %sql  --- ditinct n = non distict
# MAGIC select count(distinct ID) from global_temp.ccu037_02_vaccination_gdppr_women

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(ID) from global_temp.ccu037_02_vaccination_gdppr_women where sex == "2" AND age >= 30 AND age <= 100

# COMMAND ----------

# MAGIC %md ## Save

# COMMAND ----------

#drop_table('ccu037_01_vaccination')
#create_table('ccu037_01_vaccination')
# outName = f'{proj}_in_vaccination'.lower()
# spark.sql(f'DROP TABLE IF EXISTS {dbc}.{outName}')
# tmp.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
# spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

# COMMAND ----------

# save name
outName = f'{proj}_vaccination_gdppr'.lower()
# save
drop_table('ccu037_02_vaccination_gdppr')
create_table(outName, select_sql_script=f"SELECT * FROM global_temp.{outName}") 

# COMMAND ----------

# save name
outName = f'{proj}_vaccination_gdppr_women'.lower()

# save previous version for comparison purposes
#_datetimenow = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
#outName_pre = f'{outName}_pre{_datetimenow}'.lower()
#print(outName_pre)
#spark.table(f'{dbc}.{outName}').write.mode('overwrite').saveAsTable(f'{dbc}.{outName_pre}')
#spark.sql(f'ALTER TABLE {dbc}.{outName_pre} OWNER TO {dbc}')

# save
drop_table('ccu037_02_vaccination_gdppr_women')
create_table(outName, select_sql_script=f"SELECT * FROM global_temp.{outName}") 

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended dars_nic_391419_j3w9t_collab.ccu037_02_vaccination_gdppr_women
