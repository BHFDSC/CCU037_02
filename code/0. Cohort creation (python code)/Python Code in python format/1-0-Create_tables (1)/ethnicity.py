# Databricks notebook source
# MAGIC %md
# MAGIC # Ethnicity table
# MAGIC  
# MAGIC **Description** 
# MAGIC 
# MAGIC This notebook runs a list of `SQL` queries to:  
# MAGIC   
# MAGIC 1. Extract latest Primary and SNOMED ethnicity codes from GDPPR
# MAGIC 2. Extract latest ethnicity code from HES
# MAGIC   
# MAGIC   
# MAGIC **Project(s)** CCU037
# MAGIC  
# MAGIC **Author(s)** Freya Allery 
# MAGIC  
# MAGIC **Reviewer(s)** 
# MAGIC  
# MAGIC **Date last updated** 2022-06-20
# MAGIC  
# MAGIC **Date last reviewed** *NA*
# MAGIC  
# MAGIC **Date last run** 2022-06-20
# MAGIC 
# MAGIC **Changelog**   
# MAGIC  
# MAGIC **Data input**   
# MAGIC * gdppr_data =  `dars_nic_391419_j3w9t_collab.ccu037_gdppr_primary_and_snomed_fa` 
# MAGIC * hes_data = `dars_nic_391419_j3w9t_collab.hes_apc_all_years`  
# MAGIC     
# MAGIC **Data output**  
# MAGIC * `dars_nic_391419_j3w9t_collab.ccu037_ethnicity` 1 row per patient
# MAGIC 
# MAGIC **Software and versions** `SQL`, `Python`
# MAGIC 
# MAGIC **Packages and versions** `pyspark`

# COMMAND ----------

# MAGIC %md
# MAGIC # 0. Define functions, import libraries

# COMMAND ----------

# Define create table function by Sam Hollings
# Source: Workspaces/dars_nic_391419_j3w9t_collab/DATA_CURATION_wrang000_functions
# Second source were pasted from: https://db.core.data.digital.nhs.uk/#notebook/2317231/command/2452227

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

gdppr_data = "dars_nic_391419_j3w9t_collab.gdppr_dars_nic_391419"
hes_data = "dars_nic_391419_j3w9t_collab.hes_apc_all_years_archive"

output = "ccu037_ethnicity"

prod_date = "2022-01-20 14:58:52.353312"

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. GDPPR

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dars_nic_391419_j3w9t_collab.gdppr_dars_nic_391419_j3w9t_archive

# COMMAND ----------

gdppr_prod_date = SELECT max(distinct ProductionDate) FROM dars_nic_391419_j3w9t_collab.gdppr_dars_nic_391419_j3w9t_archive

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Primary codes

# COMMAND ----------

# Primary codes
spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW ccu037_gdppr_ethnicity AS 
       SELECT NHS_NUMBER_DEID, 
              gdppr.ETHNIC, REPORTING_PERIOD_END_DATE as RECORD_DATE, 
              'GDPPR' as dataset, 
              2 as eth_rank 
       FROM dars_nic_391419_j3w9t_collab.gdppr_dars_nic_391419_j3w9t_archive as gdppr 
       WHERE ProductionDate = "2022-05-30 14:32:44.253705" """)

# MAKE VARIABLES FOR PROD_DATE_GDPPR AND PROD_DATE_HES

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM global_temp.ccu037_gdppr_ethnicity

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2. SNOMED codes

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dss_corporate.gdppr_ethnicity_mappings

# COMMAND ----------

#SNOMED codes
spark.sql(f"""CREATE OR REPLACE GLOBAL TEMP VIEW ccu037_gdppr_ethnicity_SNOMED AS 
               SELECT NHS_NUMBER_DEID,
                      eth.ConceptId as SNOMED_ETHNIC,
                      eth.PrimaryCode as ETHNIC,
                      DATE as RECORD_DATE,
                      'GDPPR_snomed' as dataset,
                      1 as eth_rank
                FROM dars_nic_391419_j3w9t_collab.gdppr_dars_nic_391419_j3w9t_archive as gdppr
                      INNER JOIN dss_corporate.gdppr_ethnicity_mappings eth on gdppr.CODE = eth.ConceptId
                WHERE gdppr.ProductionDate = "2022-05-30 14:32:44.253705" """)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM global_temp.ccu037_gdppr_ethnicity_SNOMED

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. HES

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT(ProductionDate) FROM dars_nic_391419_j3w9t_collab.hes_apc_all_years_archive

# COMMAND ----------

spark.sql(f"""
 CREATE OR REPLACE GLOBAL TEMP VIEW ccu037_all_hes_apc_ethnicity AS
 SELECT DISTINCT PERSON_ID_DEID as NHS_NUMBER_DEID, 
      ETHNOS as ETHNIC, 
      EPISTART as RECORD_DATE, 
      "hes_apc" as dataset,
      3 as eth_rank
  FROM dars_nic_391419_j3w9t_collab.hes_apc_all_years_archive 
  WHERE ProductionDate = "2022-03-31 00:00:00.000000" """)

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM global_temp.ccu037_all_hes_apc_ethnicity

# COMMAND ----------

# MAGIC %md
# MAGIC Recommendation from the data wranglers is to rmove OP and AE HES for not as they are not well used and don't add much info ==> use APC

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Output table - all codes
# MAGIC ### GDPPR (SNOMED and Primary Codes), HES Primary codes

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu037_ethnicity_all_codes as
# MAGIC SELECT NHS_NUMBER_DEID, ETHNIC, NULL AS SNOMED_ETHNIC, RECORD_DATE, dataset, eth_rank 
# MAGIC FROM global_temp.ccu037_gdppr_ethnicity
# MAGIC UNION ALL
# MAGIC SELECT NHS_NUMBER_DEID, ETHNIC, SNOMED_ETHNIC, RECORD_DATE, dataset, eth_rank 
# MAGIC FROM global_temp.ccu037_gdppr_ethnicity_SNOMED
# MAGIC UNION ALL
# MAGIC SELECT NHS_NUMBER_DEID, ETHNIC, null AS SNOMED_ETHNIC, RECORD_DATE, dataset, eth_rank 
# MAGIC FROM global_temp.ccu037_all_hes_apc_ethnicity

# COMMAND ----------

drop_table('ccu037_ethnicity_all_codes')
create_table('ccu037_ethnicity_all_codes') 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dars_nic_391419_j3w9t_collab.ccu037_ethnicity_all_codes

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Output table - prioritised codes

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1. Get most recent record for each source

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM global_temp.ccu037_gdppr_ethnicity 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(DISTINCT NHS_NUMBER_DEID) FROM global_temp.ccu037_gdppr_ethnicity

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu037_gdppr_ethnicity_latest as
# MAGIC SELECT NHS_NUMBER_DEID,
# MAGIC        CASE 
# MAGIC           WHEN ETHNIC IS NOT NULL OR TRIM(ETHNIC) NOT IN ("","9", "99", "X" , "Z") THEN MAX(RECORD_DATE)
# MAGIC           ELSE MAX(RECORD_DATE)
# MAGIC        END AS RECORD_DATE,
# MAGIC        ETHNIC, 
# MAGIC        dataset, 
# MAGIC        eth_rank
# MAGIC       FROM global_temp.ccu037_gdppr_ethnicity
# MAGIC       GROUP BY NHS_NUMBER_DEID, ETHNIC, dataset, eth_rank

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM global_temp.ccu037_gdppr_ethnicity_latest

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM global_temp.ccu037_gdppr_ethnicity_latest

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu037_gdppr_ethnicity_SNOMED_latest as
# MAGIC SELECT NHS_NUMBER_DEID,
# MAGIC        CASE 
# MAGIC           WHEN ETHNIC IS NOT NULL OR TRIM(ETHNIC) NOT IN ("","9", "99", "X" , "Z") THEN MAX(RECORD_DATE)
# MAGIC           ELSE MAX(RECORD_DATE)
# MAGIC        END AS RECORD_DATE,
# MAGIC        ETHNIC, 
# MAGIC        SNOMED_ETHNIC, 
# MAGIC        dataset, 
# MAGIC        eth_rank
# MAGIC                 from global_temp.ccu037_gdppr_ethnicity_SNOMED
# MAGIC                 WHERE ETHNIC IS NOT NULL OR TRIM(ETHNIC) NOT IN ("","9", "99", "X" , "Z")
# MAGIC                 group by NHS_NUMBER_DEID, ETHNIC, SNOMED_ETHNIC, dataset, eth_rank

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(DISTINCT NHS_NUMBER_DEID) FROM global_temp.ccu037_gdppr_ethnicity_SNOMED_latest

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM global_temp.ccu037_gdppr_ethnicity_SNOMED_latest

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu037_all_hes_apc_ethnicity_latest as
# MAGIC SELECT NHS_NUMBER_DEID,
# MAGIC        CASE 
# MAGIC           WHEN ETHNIC IS NOT NULL OR TRIM(ETHNIC) NOT IN ("","9", "99", "X" , "Z") THEN MAX(RECORD_DATE)
# MAGIC           ELSE MAX(RECORD_DATE)
# MAGIC        END AS RECORD_DATE,
# MAGIC        ETHNIC, 
# MAGIC        dataset, 
# MAGIC        eth_rank
# MAGIC                 from global_temp.ccu037_all_hes_apc_ethnicity
# MAGIC                 WHERE ETHNIC IS NOT NULL OR TRIM(ETHNIC) NOT IN ("","9", "99", "X" , "Z")
# MAGIC                 group by NHS_NUMBER_DEID, ETHNIC, dataset, eth_rank

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(DISTINCT NHS_NUMBER_DEID) FROM global_temp.ccu037_all_hes_apc_ethnicity_latest

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu037_ethnicity_all_codes_latest as
# MAGIC SELECT NHS_NUMBER_DEID, ETHNIC, NULL AS SNOMED_ETHNIC, RECORD_DATE, dataset, eth_rank 
# MAGIC FROM global_temp.ccu037_gdppr_ethnicity_latest
# MAGIC UNION ALL
# MAGIC SELECT NHS_NUMBER_DEID, ETHNIC, SNOMED_ETHNIC, RECORD_DATE, dataset, eth_rank 
# MAGIC FROM global_temp.ccu037_gdppr_ethnicity_SNOMED_latest
# MAGIC UNION ALL
# MAGIC SELECT NHS_NUMBER_DEID, ETHNIC, null AS SNOMED_ETHNIC, RECORD_DATE, dataset, eth_rank 
# MAGIC FROM global_temp.ccu037_all_hes_apc_ethnicity_latest

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM global_temp.ccu037_ethnicity_all_codes_latest WHERE SNOMED_ETHNIC IS NOT null

# COMMAND ----------

drop_table("ccu037_ethnicity_all_codes_latest")
create_table("ccu037_ethnicity_all_codes_latest")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2. Select the highest ranked ethnicty for each patient

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu037_ethnicity_all_codes_latest_null as
# MAGIC SELECT *,
# MAGIC      CASE WHEN ETHNIC IS NULL or TRIM(ETHNIC) IN ("","9", "99", "X" , "Z") THEN 1 ELSE 0 END as ethnic_null,
# MAGIC      CASE WHEN SNOMED_ETHNIC IS NULL THEN 1 ELSE 0 END as SNOMED_ethnic_null
# MAGIC      FROM global_temp.ccu037_ethnicity_all_codes_latest

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM global_temp.ccu037_ethnicity_all_codes_latest_null

# COMMAND ----------

# MAGIC %sql
# MAGIC --CREATE OR REPLACE GLOBAL TEMP VIEW ccu037_ethnicity_selected AS
# MAGIC SELECT NHS_NUMBER_DEID, RECORD_DATE, dataset,
# MAGIC        CASE
# MAGIC          WHEN SNOMED_ethnic_null = 0 THEN ETHNIC 
# MAGIC          WHEN (eth_rank = 2 AND ethnic_null = 0) THEN ETHNIC
# MAGIC          WHEN (eth_rank = 3 AND ethnic_null = 0) THEN ETHNIC
# MAGIC          ELSE null
# MAGIC        END AS ETHNICITY,
# MAGIC        CASE
# MAGIC          WHEN SNOMED_ethnic_null = 0 THEN SNOMED_ETHNIC 
# MAGIC          ELSE null
# MAGIC        END AS SNOMED_ETHNICITY
# MAGIC        FROM global_temp.ccu037_ethnicity_all_codes_latest_null
# MAGIC GROUP BY NHS_NUMBER_DEID, RECORD_DATE, dataset, ETHNIC, SNOMED_ETHNIC, SNOMED_ethnic_null, ethnic_null, eth_rank

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM global_temp.ccu037_ethnicity_selected

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM global_temp.ccu037_ethnicity_selected

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(DISTINCT NHS_NUMBER_DEID) FROM global_temp.ccu037_ethnicity_selected 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu037_ethnicity_selected_trial AS
# MAGIC SELECT NHS_NUMBER_DEID, RECORD_DATE, dataset, ETHNIC, SNOMED_ETHNIC,
# MAGIC        CASE
# MAGIC          WHEN SNOMED_ethnic_null = 0 THEN 3 
# MAGIC          WHEN (eth_rank = 2 AND ethnic_null = 0) THEN 2
# MAGIC          WHEN (eth_rank = 3 AND ethnic_null = 0) THEN 1
# MAGIC          ELSE 0
# MAGIC        END AS ethnicity_rank
# MAGIC        FROM global_temp.ccu037_ethnicity_all_codes_latest_null

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM global_temp.ccu037_ethnicity_selected_trial

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu037_ethnicity_selected_trial_2 AS
# MAGIC SELECT DISTINCT(NHS_NUMBER_DEID), RECORD_DATE, dataset, ETHNIC, SNOMED_ETHNIC, MAX(ethnicity_rank)
# MAGIC FROM   global_temp.ccu037_ethnicity_selected_trial 
# MAGIC GROUP BY NHS_NUMBER_DEID, RECORD_DATE, dataset, ETHNIC, SNOMED_ETHNIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(DISTINCT NHS_NUMBER_DEID) FROM global_temp.ccu037_ethnicity_selected_trial_2

# COMMAND ----------

