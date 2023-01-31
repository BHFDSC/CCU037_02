# Databricks notebook source
# MAGIC %md
# MAGIC # Inclusion criteria
# MAGIC  
# MAGIC **Description** 
# MAGIC 
# MAGIC This notebook runs a list of `SQL` queries to:  
# MAGIC   
# MAGIC 1. Apply the exclusion critieria:
# MAGIC    a) over 18
# MAGIC    b) positive covid test
# MAGIC    c) at least one year of records available prior to covid infection
# MAGIC    
# MAGIC 2. Create table with final cohort demographics
# MAGIC 
# MAGIC Carrried out for study end date: "2022-06-29 00:00:00.000000"
# MAGIC   
# MAGIC   
# MAGIC **Project(s)** CCU037
# MAGIC  
# MAGIC **Author(s)** Freya Allery 
# MAGIC  
# MAGIC **Reviewer(s)** 
# MAGIC  
# MAGIC **Date last updated** 2023-01-17
# MAGIC  
# MAGIC **Date last reviewed** *NA*
# MAGIC  
# MAGIC **Date last run** 2022-01-17
# MAGIC 
# MAGIC **Changelog**   
# MAGIC  
# MAGIC **Data input**   
# MAGIC * gdppr_data =  `dars_nic_391419_j3w9t_collab.gdppr_dars_nic_391419_j3w9t_archive` 
# MAGIC * hes_data = `dars_nic_391419_j3w9t_collab.hes_apc_all_years`
# MAGIC * covid_data = `dars_nic_391419_j3w9t_collab.ccu037_covid_events`
# MAGIC     
# MAGIC **Data output**  
# MAGIC * `dars_nic_391419_j3w9t_collab.ccu037_ethnicity_assembled` 1 row per patient
# MAGIC 
# MAGIC **Software and versions** `SQL`, `Python`
# MAGIC 
# MAGIC **Packages and versions** `pyspark`

# COMMAND ----------

# MAGIC %md
# MAGIC # 1.Define functions

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

# MAGIC %md
# MAGIC # 2. Join tables

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW ccu037_cohort_unfiltered AS
SELECT *, *, *
FROM ((dars_nic_391419_j3w9t_collab.ccu037_ethnicity_assembled_multiple_granularities as ethnicity
INNER JOIN dars_nic_391419_j3w9t_collab.ccu037_demographics as demographics ON ethnicity.NHS_NUMBER_DEID = demographics.person_id_deid)
LEFT JOIN dars_nic_391419_j3w9t_collab.ccu037_covid_events as covid_events ON ethnicity.NHS_NUMBER_DEID = covid_events.person_id_deid) """)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM global_temp.ccu037_cohort_unfiltered

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM global_temp.ccu037_cohort_unfiltered

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Criteria

# COMMAND ----------

gdppr_table = "dars_nic_391419_j3w9t_collab.gdppr_dars_nic_391419_j3w9t_archive"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1. GP record

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW ccu037_cohort_filter_gp AS
SELECT 
DISTINCT b.NHS_NUMBER_DEID, 
b.dob,
b.sex, 
b.lsoa, 
b.IMD_quintile,
b.ethnicity_5_group,
b.ethnicity_11_group,
b.PrimaryCode_ethnicity, 
b.SNOMED_ethnicity, 
b.first_event as covid_first_event,
b.date_first as date_first_covid_event,
b.death,
b.date_death,
b.death_covid,
b.01_Covid_positive_test,
b.01_GP_covid_diagnosis,
b.02_Covid_admission,
b.03_ECMO_treatment,
b.03_ICU_admission,
b.03_IMV_treatment,
b.03_NIV_treatment,
b.04_Covid_inpatient_death,
b.04_Fatal_with_covid_diagnosis,
b.04_Fatal_without_covid_diagnosis,
b.0_Covid_infection,
b.severity as covid_severity,
b.ventilatory_support,
a.ProductionDate
FROM global_temp.ccu037_cohort_unfiltered b
LEFT JOIN {gdppr_table} a 
ON b.NHS_NUMBER_DEID == a.NHS_NUMBER_DEID
WHERE ProductionDate = "2022-06-29 00:00:00.000000" """)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.ccu037_cohort_filter_gp

# COMMAND ----------

cohort = "global_temp.ccu037_cohort_filter_gp"

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE GLOBAL TEMP VIEW earliest_gdppr_record as
WITH added_row_number AS (
  SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY NHS_NUMBER_DEID ORDER BY RECORD_DATE ASC) AS row_number
  FROM {gdppr_table} WHERE ProductionDate = "2022-06-29 00:00:00.000000"
)
SELECT
  NHS_NUMBER_DEID, RECORD_DATE as min_record_date
FROM added_row_number
WHERE row_number = 1
""")

# COMMAND ----------

# Joining  the earliest gp record date to date of first covid event for cohort, subtracting by 365 days and filtering for null and less than 365 days records
spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW record_date_check AS
    SELECT 
    *
    FROM
    (SELECT
    b.NHS_NUMBER_DEID, 
b.dob,
b.sex, 
b.lsoa, 
b.IMD_quintile,
b.ethnicity_5_group,
b.ethnicity_11_group,
b.PrimaryCode_ethnicity, 
b.SNOMED_ethnicity, 
b.covid_first_event,
b.date_first_covid_event,
b.death,
b.date_death,
b.death_covid,
b.01_Covid_positive_test,
b.01_GP_covid_diagnosis,
b.02_Covid_admission,
b.03_ECMO_treatment,
b.03_ICU_admission,
b.03_IMV_treatment,
b.03_NIV_treatment,
b.04_Covid_inpatient_death,
b.04_Fatal_with_covid_diagnosis,
b.04_Fatal_without_covid_diagnosis,
b.0_Covid_infection,
b.covid_severity,
b.ventilatory_support,
a.min_record_date
    FROM {cohort} as b
    LEFT JOIN global_temp.earliest_gdppr_record as a
    ON b.NHS_NUMBER_DEID = a.NHS_NUMBER_DEID)
    WHERE min_record_date IS NOT null AND min_record_date < (date_first_covid_event - INTERVAL 365 DAY)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2. Positive COVID-19 test before 31st Dec

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW ccu037_cohort_filter1 AS
SELECT 
NHS_NUMBER_DEID, 
dob,
FLOOR(datediff(date_first_covid_event, dob)/365.25) as age,
sex, 
lsoa, 
IMD_quintile,
ethnicity_5_group,
ethnicity_11_group,
PrimaryCode_ethnicity, 
SNOMED_ethnicity, 
covid_first_event,
date_first_covid_event,
death,
date_death,
death_covid,
01_Covid_positive_test,
01_GP_covid_diagnosis,
02_Covid_admission,
03_ECMO_treatment,
03_ICU_admission,
03_IMV_treatment,
03_NIV_treatment,
04_Covid_inpatient_death,
04_Fatal_with_covid_diagnosis,
04_Fatal_without_covid_diagnosis,
0_Covid_infection,
covid_severity,
ventilatory_support
FROM  global_temp.record_date_check
WHERE (01_Covid_positive_test == 1 OR
01_GP_covid_diagnosis ==1)
AND date_first_covid_event < to_date("2022-12-31")
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3. Over 18

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW ccu037_inclusion_criteria_updated AS
SELECT 
NHS_NUMBER_DEID, 
dob,
age,
sex, 
lsoa, 
IMD_quintile,
ethnicity_5_group,
ethnicity_11_group,
PrimaryCode_ethnicity, 
SNOMED_ethnicity, 
covid_first_event,
date_first_covid_event,
death,
date_death,
death_covid,
01_Covid_positive_test,
01_GP_covid_diagnosis,
02_Covid_admission,
03_ECMO_treatment,
03_ICU_admission,
03_IMV_treatment,
03_NIV_treatment,
04_Covid_inpatient_death,
04_Fatal_with_covid_diagnosis,
04_Fatal_without_covid_diagnosis,
0_Covid_infection,
covid_severity,
ventilatory_support
FROM  global_temp.ccu037_cohort_filter1
WHERE age > 18
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from global_temp.ccu037_inclusion_criteria_updated

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Commit

# COMMAND ----------

drop_table("ccu037_inclusion_criteria_updated")
create_table("ccu037_inclusion_criteria_updated")