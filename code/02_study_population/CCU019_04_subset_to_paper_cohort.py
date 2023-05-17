# Databricks notebook source
# MAGIC %md
# MAGIC # CCU019_04_subset_to_paper_chohort
# MAGIC  
# MAGIC **Description** 
# MAGIC 
# MAGIC This notebook specifies the population cohort for project CCU019 and subsets the rare_disease_sample and covid trajectory data to this definition.
# MAGIC 
# MAGIC **Project(s)** CCU0019
# MAGIC  
# MAGIC **Author(s)** Johan Thygesen
# MAGIC  
# MAGIC **Reviewer(s)** 
# MAGIC  
# MAGIC **Date last updated** 2022-07-21
# MAGIC  
# MAGIC **Date last reviewed** 
# MAGIC  
# MAGIC **Date last run** 2022-07-21
# MAGIC  
# MAGIC **Data input**  
# MAGIC   1. `gddpr_data`
# MAGIC   1. `ccu013_covid_trajectory`
# MAGIC   2. `ccu019_rare_disease`
# MAGIC 
# MAGIC **Data output**
# MAGIC 1. `ccu019_covid_trajectory_paper_cohort`
# MAGIC 2. `ccu019_rare_disease_paper_cohort`
# MAGIC 
# MAGIC **Software and versions** SQL, python
# MAGIC  
# MAGIC **Packages and versions** See cell below:
# MAGIC 
# MAGIC **TODO**
# MAGIC * Implement Longcovid search

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU019/rare_diseases/CCU019_00_helper_functions

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Rare disease list
# MAGIC SELECT count (DISTINCT person_id_deid) FROM dars_nic_391419_j3w9t_collab.ccu019_rare_disease 

# COMMAND ----------

# MAGIC %md
# MAGIC ## GDPPR

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Only accept individuals who are alive an in GDPPR at the begining of the pandemic
# MAGIC -------------------------------------------------------------------------------------
# MAGIC --- All counts are from  @ 02/08/2022 - prodcution: 2021-11-26 14:02:40.645948
# MAGIC 
# MAGIC --- n = 61,072,202  | Total ids 
# MAGIC -- SELECT count (DISTINCT person_id_deid) FROM dars_nic_391419_j3w9t_collab.ccu019_tmp_gdppr
# MAGIC 
# MAGIC ---n = 58,321,682  | Total ids where date is before pandemic 2020-01-23
# MAGIC -- SELECT count (DISTINCT person_id_deid) FROM dars_nic_391419_j3w9t_collab.ccu019_tmp_gdppr WHERE date <= "2020-01-23"
# MAGIC 
# MAGIC ---n = 58,303,340   | Total ids where date is after 1990-01-01 and before pandemic 2020-01-23
# MAGIC SELECT count (DISTINCT person_id_deid) FROM dars_nic_391419_j3w9t_collab.ccu019_tmp_gdppr WHERE (date >= "1990-01-01") and (date <= "2020-01-23")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Single death per patient
# MAGIC In the deaths table (Civil registration deaths), some unfortunate people are down as dying twice. Let's take the most recent death date. 

# COMMAND ----------

production_date = "2021-11-26 14:02:40.645948"
deaths = spark.sql(f'''
    SELECT * FROM dars_nic_391419_j3w9t_collab.deaths_dars_nic_391419_j3w9t_archive WHERE ProductionDate == "{production_date}"''')
deaths.createOrReplaceGlobalTempView('ccu019_dp_deaths_tmp') 

# COMMAND ----------

death_single = spark.sql(f'''
SELECT * 
FROM 
  (SELECT * , row_number() OVER (PARTITION BY DEC_CONF_NHS_NUMBER_CLEAN_DEID 
                                      ORDER BY REG_DATE desc, REG_DATE_OF_DEATH desc) as death_rank
    FROM dars_nic_391419_j3w9t_collab.deaths_dars_nic_391419_j3w9t_archive WHERE ProductionDate == "{production_date}"
    ) cte
WHERE death_rank = 1
AND DEC_CONF_NHS_NUMBER_CLEAN_DEID IS NOT NULL
and TO_DATE(REG_DATE_OF_DEATH, "yyyyMMdd") > '1900-01-01'
AND TO_DATE(REG_DATE_OF_DEATH, "yyyyMMdd") <= current_date()
''')
death_single.createOrReplaceGlobalTempView('ccu019_dp_single_patient_death')

# COMMAND ----------

# MAGIC %sql
# MAGIC --- All counts are from  @ 02/08/2022 - prodcution: 2021-11-26 14:02:40.645948
# MAGIC 
# MAGIC --- n = 15,199,230 | Total IDs 
# MAGIC --SELECT count (distinct DEC_CONF_NHS_NUMBER_CLEAN_DEID) FROM global_temp.ccu019_dp_single_patient_death
# MAGIC --- n = 14,204,440 | Total IDs where date is <= 2020-01-23
# MAGIC SELECT count (distinct DEC_CONF_NHS_NUMBER_CLEAN_DEID) FROM global_temp.ccu019_dp_single_patient_death WHERE REG_DATE_OF_DEATH <= "20200123"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu019_dp_single_patient_death_death_before_pandemic as
# MAGIC SELECT distinct DEC_CONF_NHS_NUMBER_CLEAN_DEID FROM global_temp.ccu019_dp_single_patient_death WHERE REG_DATE_OF_DEATH <= "20200123"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Subset to population

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu019_paper_population_cohort as
# MAGIC SELECT DISTINCT person_id_deid FROM (select person_id_deid, date FROM dars_nic_391419_j3w9t_collab.ccu019_tmp_gdppr where date >= "1990-01-01" AND date <= "2020-01-23") gdppr
# MAGIC LEFT ANTI JOIN (select DEC_CONF_NHS_NUMBER_CLEAN_DEID FROM global_temp.ccu019_dp_single_patient_death_death_before_pandemic) death
# MAGIC ON gdppr.person_id_deid = death.DEC_CONF_NHS_NUMBER_CLEAN_DEID

# COMMAND ----------

drop_table("ccu019_paper_population_cohort")
create_table("ccu019_paper_population_cohort")

# COMMAND ----------

# MAGIC %sql
# MAGIC --- 58162316 population size @ 03/08/2022
# MAGIC SELECT count(distinct person_id_deid) FROM dars_nic_391419_j3w9t_collab.ccu019_paper_population_cohort

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Test if anyone in the new population cohort is in the death table and if so when do they die
# MAGIC select min(REG_DATE_OF_DEATH), max(REG_DATE_OF_DEATH) FROM global_temp.ccu019_dp_single_patient_death a
# MAGIC INNER JOIN dars_nic_391419_j3w9t_collab.ccu019_paper_population_cohort b
# MAGIC ON a.DEC_CONF_NHS_NUMBER_CLEAN_DEID = b.person_id_deid

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rare disease patients in the population

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(distinct person_id_deid) FROM dars_nic_391419_j3w9t_collab.ccu019_rare_disease

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Take backup of ccu019_paper_population_cohort
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu019_paper_cohort_rare_disease_220822 as 
# MAGIC SELECT * FROM dars_nic_391419_j3w9t_collab.ccu019_paper_cohort_rare_disease

# COMMAND ----------

drop_table("ccu019_paper_cohort_rare_disease_220822")
create_table("ccu019_paper_cohort_rare_disease_220822")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(distinct person_id_deid) FROM dars_nic_391419_j3w9t_collab.ccu019_paper_cohort_rare_disease

# COMMAND ----------

# MAGIC %sql
# MAGIC -- All rare disease IDs found
# MAGIC -- n = 1,523,204
# MAGIC -- SELECT count (distinct person_id_deid) FROM dars_nic_391419_j3w9t_collab.dars_nic_391419_j3w9t_collab.dars_nic_391419_j3w9t_collab.ccu019_rare_disease 
# MAGIC 
# MAGIC -- All rare disease patients in the population
# MAGIC -- n = 997,357
# MAGIC 
# MAGIC -- All rare disease patients in the population with onset earlier than the start of the pandmic
# MAGIC -- n = 894,396
# MAGIC SELECT count(distinct a.person_id_deid) FROM dars_nic_391419_j3w9t_collab.ccu019_rare_disease a
# MAGIC INNER JOIN dars_nic_391419_j3w9t_collab.ccu019_paper_population_cohort b
# MAGIC ON a.person_id_deid = b.person_id_deid
# MAGIC WHERE date < '2020-01-23'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu019_paper_cohort_rare_disease as
# MAGIC SELECT a.* FROM dars_nic_391419_j3w9t_collab.ccu019_rare_disease a
# MAGIC INNER JOIN dars_nic_391419_j3w9t_collab.ccu019_paper_population_cohort b
# MAGIC ON a.person_id_deid = b.person_id_deid
# MAGIC WHERE date < '2020-01-23'

# COMMAND ----------

drop_table("ccu019_paper_cohort_rare_disease")
create_table("ccu019_paper_cohort_rare_disease")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count (*) as obs, count(distinct person_id_deid) as n_ids, count(distinct phenotype) as rare_disease
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu019_paper_cohort_rare_disease
